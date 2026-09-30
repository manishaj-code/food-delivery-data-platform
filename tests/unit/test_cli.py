"""CLI: argument handling and the in-process pipeline runner (spec 07 §3)."""

from __future__ import annotations

from datetime import date

import pytest

from src import cli
from src.common.constants import DATASETS
from src.common.exceptions import SourceFileError

pytestmark = pytest.mark.unit


def test_pipeline_plan_matches_the_dag_order() -> None:
    task_ids = [task_id for task_id, _step, _kwargs in cli.pipeline_plan()]
    assert task_ids == [
        "prepare_source_data",
        *[f"ingest_{dataset}" for dataset in DATASETS],
        "validate_data",
        "transform_data",
        "publish_processed",
        "load_warehouse",
        "run_dq_checks",
        "pipeline_summary",
    ]


def test_run_pipeline_stops_at_the_first_failure(monkeypatch) -> None:
    calls, failures = [], []

    def ok(**kwargs):
        calls.append(kwargs.get("dataset", "step"))
        return {}

    def fail(**kwargs):
        raise SourceFileError("Source file not found", dataset=kwargs["dataset"])

    plan = [("prepare_source_data", ok, {}), ("ingest_orders", fail, {"dataset": "orders"})]
    plan.append(("validate_data", ok, {}))
    monkeypatch.setattr(cli, "pipeline_plan", lambda: plan)
    monkeypatch.setattr(cli.steps, "record_failure", lambda *args: failures.append(args))

    assert cli.run_pipeline(date(2026, 9, 1), "incremental", "cli__1") == 1

    assert calls == ["step"]  # validate_data never ran
    assert failures == [
        (
            date(2026, 9, 1),
            "cli__1",
            "incremental",
            "ingest_orders",
            "Source file not found (dataset=orders)",
        )
    ]


def test_run_pipeline_returns_zero_when_every_step_succeeds(monkeypatch) -> None:
    monkeypatch.setattr(cli, "pipeline_plan", lambda: [("a", lambda **kw: {}, {})])
    assert cli.run_pipeline(date(2026, 9, 1), "incremental", "cli__1") == 0


def test_run_step_calls_the_named_step(monkeypatch) -> None:
    seen = {}
    monkeypatch.setitem(cli.STEPS, "ingest", lambda **kwargs: seen.update(kwargs) or {})

    exit_code = cli.main(
        ["run-step", "ingest", "--dataset", "orders", "--run-date", "2026-09-01", "--run-id", "x"]
    )

    assert exit_code == 0
    assert seen == {
        "run_date": date(2026, 9, 1),
        "run_id": "x",
        "load_type": "incremental",
        "dataset": "orders",
    }


def test_run_step_ingest_requires_a_dataset() -> None:
    with pytest.raises(SystemExit):
        cli.main(["run-step", "ingest", "--run-date", "2026-09-01"])


def test_run_step_returns_one_on_failure(monkeypatch) -> None:
    def fail(**kwargs):
        raise SourceFileError("missing")

    monkeypatch.setitem(cli.STEPS, "validate", fail)
    assert cli.main(["run-step", "validate", "--run-date", "2026-09-01"]) == 1


def test_bad_run_date_is_rejected() -> None:
    with pytest.raises(SystemExit):
        cli.main(["run-pipeline", "--run-date", "01-09-2026"])


def test_generate_data_and_run_analytics_delegate(monkeypatch) -> None:
    import scripts.generate_data
    import src.warehouse.analytics

    seen = []
    monkeypatch.setattr(scripts.generate_data, "main", lambda argv: seen.append(argv) or 0)
    monkeypatch.setattr(src.warehouse.analytics, "main", lambda argv: seen.append(argv) or 0)

    assert cli.main(["generate-data", "--mode", "historical"]) == 0
    assert cli.main(["run-analytics", "--all"]) == 0
    assert seen == [["--mode", "historical"], ["--all"]]


def test_default_run_id_is_marked_as_cli() -> None:
    assert cli.default_run_id().startswith("cli__")
