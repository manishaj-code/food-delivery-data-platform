"""DAG integrity (FR-070 – FR-076, AC-040, AC-041, AC-046).

The structural tests need Airflow and run inside the Airflow image::

    docker compose run --rm -w /opt/project airflow-init \
        python -m pytest -p no:cacheprovider tests/unit/test_dag_integrity.py

The orchestration-only check (AC-046) reads the DAG file's imports and runs everywhere.
"""

from __future__ import annotations

import ast
from datetime import timedelta
from pathlib import Path

import pytest

from src.common.constants import DATASETS

pytestmark = pytest.mark.unit

DAG_FILE = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "food_delivery_pipeline.py"
DAG_ID = "food_delivery_pipeline"
EXPECTED_ORDER = [
    "start",
    "prepare_source_data",
    *[f"ingest_{dataset}" for dataset in DATASETS],
    "validate_data",
    "transform_data",
    "publish_processed",
    "load_warehouse",
    "run_dq_checks",
    "pipeline_summary",
    "success",
]


def test_dag_file_is_orchestration_only() -> None:
    """AC-046: the DAG imports Airflow, the step API, and constants — no business logic."""
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    allowed = ("airflow", "src.pipeline", "src.common.constants", "datetime", "__future__")
    assert all(module.startswith(allowed) for module in modules), modules
    source = DAG_FILE.read_text(encoding="utf-8").lower()
    assert "select " not in source and "spark" not in source


@pytest.fixture(scope="module")
def dag():
    # airflow.sdk, not airflow: the project's own airflow/ folder is importable as a
    # namespace package even where Airflow is not installed.
    pytest.importorskip("airflow.sdk", reason="Airflow is only installed in the Airflow image")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAG_FILE.parent))
    assert bag.import_errors == {}
    return bag.dags[DAG_ID]


@pytest.mark.airflow
def test_task_ids_and_chain(dag) -> None:
    """AC-040: the exact FR-070 chain, each task feeding only the next one."""
    assert sorted(dag.task_ids) == sorted(EXPECTED_ORDER)
    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:], strict=False):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}, upstream
    assert dag.get_task("start").upstream_task_ids == set()


@pytest.mark.airflow
def test_retries_and_failure_callback(dag) -> None:
    """AC-041 / FR-072 / FR-074."""
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
        assert task.retry_delay == timedelta(minutes=5), task.task_id
    assert dag.get_task("ingest_orders").on_failure_callback


@pytest.mark.airflow
def test_schedule_and_run_limits(dag) -> None:
    """AC-041 / FR-076."""
    assert dag.schedule == "@daily"
    assert type(dag.timetable).__name__ == "CronTriggerTimetable"  # Airflow 3 default
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    # Must not be after the historical run date, or that run gets no tasks.
    assert dag.start_date.date().isoformat() == "2026-08-31"


@pytest.mark.airflow
def test_load_type_param(dag) -> None:
    param = dag.params.get_param("load_type")
    assert param.value == "incremental"
    assert param.schema["enum"] == ["historical", "incremental"]
