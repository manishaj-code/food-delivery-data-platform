"""The whole pipeline through the CLI runner on the sample data (FR-075, FR-080, FR-081, FR-101).

Historical load (2026-08-31) -> incremental run (2026-09-01) -> an incremental run with a
broken source header (2026-09-02), which must stop at ingest_orders and be audited.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import date

import pytest

from src.cli import run_pipeline
from src.common.config import load_settings
from src.common.logging_config import LOG_FORMAT, RunContextFilter
from src.pipeline import steps
from tests.integration.conftest import Warehouse, fresh_warehouse
from tests.sample_lake import SAMPLE_DIR

pytestmark = [pytest.mark.integration, pytest.mark.spark]

SCHEMA = "test_cli_pipeline"
RUNS = {
    "historical": (date(2026, 8, 31), "historical", "cli__test_historical"),
    "daily": (date(2026, 9, 1), "incremental", "cli__test_daily"),
    "broken": (date(2026, 9, 2), "incremental", "cli__test_broken"),
}

# Spec 12 §2 (dataset-specific ones checked for orders).
REQUIRED_MESSAGES = (
    "Starting orders ingestion",
    "Records received: ",
    "Records written to raw: ",
    "Starting validation for orders",
    "Dataset: orders",
    "Total Records: ",
    "Valid Records: ",
    "Invalid Records: ",
    "Quality Score: ",
    "Transformation completed",
    "Redshift load completed",
    "Post-load checks passed",
    "Pipeline completed successfully",
)


class _Collect(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.setFormatter(logging.Formatter(LOG_FORMAT))
        self.addFilter(RunContextFilter())
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


@pytest.fixture(scope="module")
def runs(spark, tmp_path_factory):
    source = tmp_path_factory.mktemp("cli_source")
    shutil.copytree(SAMPLE_DIR, source, dirs_exist_ok=True)
    header = source / "orders" / "orders_2026-09-02.csv"
    lines = header.read_text(encoding="utf-8").splitlines()
    header.write_text("\n".join(["order_id,oops", *lines[1:]]) + "\n", encoding="utf-8")

    with fresh_warehouse(SCHEMA) as warehouse:
        settings = load_settings(
            {
                **os.environ,
                "WAREHOUSE_TYPE": "postgres",
                "REDSHIFT_SCHEMA": SCHEMA,
                "LOCAL_LAKE_PATH": str(tmp_path_factory.mktemp("cli_lake")),
                "SOURCE_DATA_PATH": str(source),
            }
        )
        patch = pytest.MonkeyPatch()
        patch.setattr(steps, "_settings", lambda: settings)
        collect = _Collect()
        root = logging.getLogger()
        root.addHandler(collect)
        level = root.level
        root.setLevel(logging.INFO)
        try:
            exit_codes = {name: run_pipeline(*run) for name, run in RUNS.items()}
        finally:
            root.removeHandler(collect)
            root.setLevel(level)
            patch.undo()
        yield warehouse, exit_codes, collect.lines


def test_historical_and_daily_runs_succeed_and_the_broken_run_fails(runs) -> None:
    _warehouse, exit_codes, _lines = runs
    assert exit_codes == {"historical": 0, "daily": 0, "broken": 1}


def test_required_log_messages_with_run_context(runs) -> None:
    """FR-101 / FR-073: required messages, each line carrying run_id= and run_date=."""
    _warehouse, _codes, lines = runs
    daily = [line for line in lines if "run_id=cli__test_daily run_date=2026-09-01" in line]
    for message in REQUIRED_MESSAGES:
        assert any(message in line for line in daily), message
    failure = [line for line in lines if "Pipeline failed at task ingest_orders" in line]
    assert failure and " - ERROR - " in failure[0]
    assert "run_id=cli__test_broken run_date=2026-09-02" in failure[0]


def test_audit_rows_for_every_run(runs) -> None:
    """FR-075 / FR-102: one row per run x dataset x stage, including the failed run."""
    warehouse: Warehouse = runs[0]
    pipeline = warehouse.query(
        "SELECT run_id, status, error_message FROM {schema}.pipeline_run_audit "
        "WHERE stage = 'pipeline' ORDER BY run_date"
    )
    assert [(run_id, status) for run_id, status, _ in pipeline] == [
        ("cli__test_historical", "SUCCESS"),
        ("cli__test_daily", "SUCCESS"),
        ("cli__test_broken", "FAILED"),
    ]
    assert pipeline[2][2].startswith("ingest_orders: Header does not match")

    [(duplicates,)] = warehouse.query(
        "SELECT COUNT(*) FROM (SELECT run_id, dataset, stage FROM {schema}.pipeline_run_audit "
        "GROUP BY run_id, dataset, stage HAVING COUNT(*) > 1) AS d"
    )
    assert duplicates == 0

    stages = warehouse.query(
        "SELECT DISTINCT stage FROM {schema}.pipeline_run_audit WHERE run_id = %s",
        ("cli__test_daily",),
    )
    assert {stage for (stage,) in stages} == {
        "prepare_source",
        "ingestion",
        "validation",
        "transformation",
        "publish",
        "warehouse_load",
        "post_load_checks",
        "pipeline",
    }
    broken = dict(
        warehouse.query(
            "SELECT dataset, status FROM {schema}.pipeline_run_audit "
            "WHERE run_id = 'cli__test_broken' AND stage = 'ingestion'"
        )
    )
    assert broken == {
        "customers": "SUCCESS",
        "restaurants": "SUCCESS",
        "delivery_partners": "NO_DATA",  # no new partners on 2026-09-02
        "orders": "FAILED",
    }


def test_the_daily_run_processes_only_its_own_date(runs) -> None:
    """FR-080 / AC-043: records read = the daily file, not the full history."""
    warehouse: Warehouse = runs[0]
    rows = dict(
        warehouse.query(
            "SELECT run_id, records_in FROM {schema}.pipeline_run_audit "
            "WHERE stage = 'ingestion' AND dataset = 'orders' AND status = 'SUCCESS'"
        )
    )
    daily_file = SAMPLE_DIR / "orders" / "orders_2026-09-01.csv"
    historical_file = SAMPLE_DIR / "orders" / "orders_historical.csv"
    assert rows["cli__test_daily"] == len(daily_file.read_text().splitlines()) - 1
    assert rows["cli__test_historical"] == len(historical_file.read_text().splitlines()) - 1
    [(orders,)] = warehouse.query("SELECT COUNT(*) FROM {schema}.fact_order")
    assert orders > rows["cli__test_historical"] * 0.9
