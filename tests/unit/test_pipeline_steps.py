"""Pipeline step API and Airflow glue (FR-071 – FR-075, spec 07 §9–10).

Heavy dependencies (Spark jobs, warehouse) are replaced by fakes; the lake is a temp dir.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.common.config import load_settings
from src.common.constants import DATASETS
from src.common.exceptions import (
    DataQualityThresholdError,
    PipelineError,
    SourceFileError,
    StorageError,
    TransformationError,
    WarehouseConnectionError,
    WarehouseLoadError,
)
from src.common.logging_config import configure_logging
from src.common.paths import audit_path
from src.common.storage import LocalStorage
from src.pipeline import callbacks, steps
from src.warehouse.audit import AuditRecord
from src.warehouse.loader import TableLoadResult
from src.warehouse.post_load_checks import CheckResult

pytestmark = pytest.mark.unit

SAMPLE_DIR = Path(__file__).resolve().parents[2] / "data" / "sample"
DAY = date(2026, 8, 31)
RUN_ID = "manual__2026-08-31T00:00:00+00:00"


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Settings on a temp lake + temp source dir (a copy of the sample data)."""
    source = tmp_path / "source"
    shutil.copytree(SAMPLE_DIR, source)
    settings = load_settings(
        {"LOCAL_LAKE_PATH": str(tmp_path / "lake"), "SOURCE_DATA_PATH": str(source)}
    )
    monkeypatch.setattr(steps, "_settings", lambda: settings)
    return SimpleNamespace(
        settings=settings, storage=LocalStorage(settings.local_lake_path), source=source
    )


def audit(env, stage: str, dataset: str, day: date = DAY) -> dict:
    return json.loads(env.storage.read_bytes(audit_path(stage, dataset, day)))


def run_kwargs(load_type: str = "historical", day: date = DAY) -> dict:
    return {"run_date": day.isoformat(), "run_id": RUN_ID, "load_type": load_type}


# --- error classification ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "retryable"),
    [
        (StorageError("s3 down"), True),
        (TransformationError("spark oom"), True),
        (WarehouseConnectionError("timeout"), True),
        (SourceFileError("missing"), False),
        (DataQualityThresholdError("gate"), False),
        (WarehouseLoadError("constraint"), False),
        (RuntimeError("unexpected"), True),
    ],
)
def test_is_retryable_follows_spec_07_section_9(exc: Exception, retryable: bool) -> None:
    assert steps.is_retryable(exc) is retryable


def test_error_message_keeps_context_of_pipeline_errors() -> None:
    assert steps.error_message(SourceFileError("Missing", dataset="orders")) == (
        "Missing (dataset=orders)"
    )
    assert steps.error_message(ValueError("bad")) == "ValueError: bad"


# --- prepare_source_data -------------------------------------------------------------------


def test_prepare_uses_existing_historical_files(env) -> None:
    result = steps.prepare_source_data(**run_kwargs())

    assert result["generated"] is False
    assert set(result["files"]) == set(DATASETS)
    assert audit(env, "prepare_source", "_pipeline")["status"] == "SUCCESS"


def test_prepare_fails_without_retry_when_historical_files_are_missing(env) -> None:
    (env.source / "orders" / "orders_historical.csv").unlink()

    with pytest.raises(SourceFileError, match="missing") as caught:
        steps.prepare_source_data(**run_kwargs())

    assert not steps.is_retryable(caught.value)
    record = audit(env, "prepare_source", "_pipeline")
    assert record["status"] == "FAILED"
    assert "orders" in record["error_message"]


def test_prepare_generates_a_missing_daily_increment(env) -> None:
    day = date(2026, 9, 5)

    result = steps.prepare_source_data(**run_kwargs("incremental", day))

    assert result["generated"] is True
    assert all(Path(path).is_file() for path in result["files"].values())


def test_prepare_refuses_a_partial_daily_file_set(env) -> None:
    (env.source / "payments" / "payments_2026-09-01.csv").unlink()

    with pytest.raises(SourceFileError, match="Some source files"):
        steps.prepare_source_data(**run_kwargs("incremental", date(2026, 9, 1)))


# --- ingest_dataset ------------------------------------------------------------------------


def test_ingest_writes_raw_audit_and_logs_run_context(env, capsys) -> None:
    configure_logging("INFO")

    result = steps.ingest_dataset("customers", **run_kwargs())

    assert result["status"] == "SUCCESS"
    assert result["records_written"] > 0
    record = audit(env, "ingestion", "customers")
    assert (record["records_in"], record["records_out"]) == (
        result["records_read"],
        result["records_written"],
    )
    assert record["run_id"] == RUN_ID and record["load_type"] == "historical"
    out = capsys.readouterr().out
    assert f"[run_id={RUN_ID} run_date=2026-08-31 dataset=customers] Starting customers" in out


def test_unknown_load_type_is_rejected(env) -> None:
    with pytest.raises(PipelineError, match="load_type"):
        steps.ingest_dataset("customers", DAY, RUN_ID, "weekly")


# --- validate_data -------------------------------------------------------------------------


def _report(dataset: str, total: int, valid: int, status: str) -> dict:
    return {
        "dataset": dataset,
        "total_records": total,
        "valid_records": valid,
        "invalid_records": total - valid,
        "quality_score": round(valid / total * 100, 2) if total else 100.0,
        "status": status,
    }


def _fake_validation(monkeypatch: pytest.MonkeyPatch, reports: list[dict]) -> None:
    import src.common.spark
    import src.validation.run_validation

    monkeypatch.setattr(src.common.spark, "get_spark", lambda settings: None)
    monkeypatch.setattr(
        src.validation.run_validation, "validate_all", lambda *args, **kwargs: reports
    )


def test_validate_records_every_dataset_before_the_quality_gate_fails(env, monkeypatch) -> None:
    _fake_validation(
        monkeypatch,
        [_report("customers", 100, 100, "PASSED"), _report("orders", 100, 80, "FAILED")],
    )

    with pytest.raises(DataQualityThresholdError) as caught:
        steps.validate_data(**run_kwargs())

    assert not steps.is_retryable(caught.value)
    assert audit(env, "validation", "customers")["status"] == "SUCCESS"
    orders = audit(env, "validation", "orders")
    assert (orders["status"], orders["records_rejected"], orders["quality_score"]) == (
        "FAILED",
        20,
        80.0,
    )
    assert audit(env, "validation", "_pipeline")["status"] == "FAILED"


def test_a_successful_retry_removes_the_failed_step_record(env, monkeypatch) -> None:
    _fake_validation(monkeypatch, [_report("orders", 100, 80, "FAILED")])
    with pytest.raises(DataQualityThresholdError):
        steps.validate_data(**run_kwargs())

    _fake_validation(monkeypatch, [_report("orders", 100, 100, "PASSED")])
    result = steps.validate_data(**run_kwargs())

    assert result["orders"]["quality_score"] == 100.0
    assert not env.storage.exists(audit_path("validation", "_pipeline", DAY))
    assert audit(env, "validation", "orders")["status"] == "SUCCESS"


# --- load_warehouse / run_dq_checks --------------------------------------------------------


def test_load_warehouse_records_each_table(env, monkeypatch) -> None:
    import src.warehouse.loader

    results = [
        TableLoadResult("dim_customer", "customers", "SUCCESS", 10, 10, 2, 8, 0.5),
        TableLoadResult("fact_order", "orders", "NO_DATA", 0, 0, 0, 0, 0.1),
    ]
    monkeypatch.setattr(src.warehouse.loader, "load_warehouse", lambda *args: results)

    summary = steps.load_warehouse(**run_kwargs())

    assert summary["dim_customer"] == {"staged": 10, "updated": 2, "inserted": 8}
    assert audit(env, "warehouse_load", "orders")["status"] == "NO_DATA"
    assert audit(env, "warehouse_load", "customers")["records_out"] == 10


def test_connection_errors_during_load_are_retryable_and_audited(env, monkeypatch) -> None:
    import src.warehouse.loader

    def fail(*args):
        raise WarehouseConnectionError("Could not connect", host="postgres")

    monkeypatch.setattr(src.warehouse.loader, "load_warehouse", fail)

    with pytest.raises(WarehouseConnectionError) as caught:
        steps.load_warehouse(**run_kwargs())

    assert steps.is_retryable(caught.value)
    assert "Could not connect" in audit(env, "warehouse_load", "_pipeline")["error_message"]


def test_dq_checks_return_pass_flags(env, monkeypatch) -> None:
    import src.warehouse.post_load_checks

    results = [
        CheckResult("WQ-002", "ERROR", "keys"),
        CheckResult("WQ-005", "WARN", "aov", {"x": 1}),
    ]
    monkeypatch.setattr(
        src.warehouse.post_load_checks, "run_post_load_checks", lambda *args: results
    )

    assert steps.run_dq_checks(**run_kwargs()) == {"WQ-002": True, "WQ-005": False}
    record = audit(env, "post_load_checks", "_pipeline")
    assert (record["records_in"], record["records_rejected"]) == (2, 1)


# --- pipeline_summary / record_failure -----------------------------------------------------


def _stage(stage: str, dataset: str, status: str = "SUCCESS", **counts) -> AuditRecord:
    return AuditRecord(
        run_id=RUN_ID,
        run_date=DAY,
        load_type="historical",
        dataset=dataset,
        stage=stage,
        status=status,
        started_at=datetime(2026, 9, 29, 10, 0, 0),
        finished_at=datetime(2026, 9, 29, 10, 0, 5),
        duration_seconds=5.0,
        **counts,
    )


@pytest.fixture
def written(monkeypatch):
    rows: list[AuditRecord] = []

    def fake_write(settings, records):
        rows.extend(records)
        return len(records)

    monkeypatch.setattr(steps, "_write_run_audit", fake_write)
    return rows


def test_pipeline_summary_persists_audit_and_logs_success(env, written, capsys) -> None:
    configure_logging("INFO")
    steps.save_audit(env.storage, _stage("ingestion", "orders", records_in=100, records_out=100))
    steps.save_audit(env.storage, _stage("validation", "orders", records_rejected=3))
    steps.save_audit(env.storage, _stage("warehouse_load", "orders", records_out=97))
    other_run = _stage("ingestion", "customers")
    other_run.run_id = "an-older-run"
    steps.save_audit(env.storage, other_run)

    result = steps.pipeline_summary(**run_kwargs())

    assert result["status"] == "SUCCESS" and result["audit_rows"] == 4
    pipeline = written[-1]
    assert (pipeline.stage, pipeline.dataset, pipeline.status) == (
        "pipeline",
        "_pipeline",
        "SUCCESS",
    )
    assert (pipeline.records_in, pipeline.records_out, pipeline.records_rejected) == (100, 97, 3)
    assert {record.run_id for record in written} == {RUN_ID}
    out = capsys.readouterr().out
    assert "Pipeline completed successfully" in out
    assert "METRIC PipelineSuccess=1" in out


def test_pipeline_summary_refuses_a_run_with_failed_stages(env, written) -> None:
    steps.save_audit(env.storage, _stage("ingestion", "orders", status="FAILED"))

    with pytest.raises(PipelineError, match="failed stages"):
        steps.pipeline_summary(**run_kwargs())


def test_record_failure_logs_audits_and_emits_metric(env, written, capsys) -> None:
    configure_logging("INFO")
    steps.save_audit(env.storage, _stage("ingestion", "orders", "FAILED"))

    steps.record_failure(DAY, RUN_ID, "historical", "ingest_orders", "Source file not found")

    out = capsys.readouterr().out
    assert "ERROR" in out and "Pipeline failed at task ingest_orders: Source file not found" in out
    assert "METRIC PipelineFailure=1" in out
    record = audit(env, "pipeline", "_pipeline")
    assert record["status"] == "FAILED"
    assert record["error_message"] == "ingest_orders: Source file not found"
    assert [r.stage for r in written] == ["ingestion", "pipeline"]


def test_record_failure_never_raises_when_the_warehouse_is_down(env, monkeypatch, capsys) -> None:
    configure_logging("INFO")

    def down(settings, records):
        raise WarehouseConnectionError("down")

    monkeypatch.setattr(steps, "_write_run_audit", down)

    steps.record_failure(DAY, RUN_ID, "historical", "load_warehouse", "boom")

    out = capsys.readouterr().out
    assert "Could not persist the failure audit" in out
    assert "METRIC PipelineFailure=1" in out


# --- Airflow glue (callbacks) --------------------------------------------------------------


def test_run_arguments_from_airflow_context() -> None:
    context = {
        "logical_date": datetime(2026, 9, 1, 0, 0),
        "run_id": "scheduled__2026-09-01",
        "params": {"load_type": "incremental"},
    }
    assert callbacks.run_arguments(context) == {
        "run_date": "2026-09-01",
        "run_id": "scheduled__2026-09-01",
        "load_type": "incremental",
    }


def test_run_arguments_without_logical_date_use_run_after() -> None:
    context = {
        "logical_date": None,
        "dag_run": SimpleNamespace(run_after=datetime(2026, 9, 29, 8, 30)),
        "run_id": "manual__x",
        "params": {},
    }
    assert callbacks.run_arguments(context)["run_date"] == "2026-09-29"
    assert callbacks.run_arguments(context)["load_type"] == "incremental"


def test_on_task_failure_reports_the_failed_task(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(steps, "record_failure", lambda **kwargs: calls.append(kwargs))
    context = {
        "logical_date": datetime(2026, 9, 1),
        "run_id": "r1",
        "params": {"load_type": "incremental"},
        "task_instance": SimpleNamespace(task_id="ingest_orders"),
        "exception": SourceFileError("Source file not found", dataset="orders"),
    }

    callbacks.on_task_failure(context)

    assert calls == [
        {
            "run_date": "2026-09-01",
            "run_id": "r1",
            "load_type": "incremental",
            "task_id": "ingest_orders",
            "error": "Source file not found (dataset=orders)",
        }
    ]


def test_on_task_failure_reports_the_original_error_not_the_airflow_wrapper(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(steps, "record_failure", lambda **kwargs: calls.append(kwargs))
    original = DataQualityThresholdError("Quality score below 95.00% threshold")
    wrapper = RuntimeError("AirflowFailException stand-in")
    wrapper.__cause__ = original
    context = {
        "logical_date": datetime(2026, 9, 1),
        "run_id": "r1",
        "params": {},
        "ti": SimpleNamespace(task_id="validate_data"),
        "exception": wrapper,
    }

    callbacks.on_task_failure(context)

    assert calls[0]["error"] == "Quality score below 95.00% threshold"


def test_on_task_failure_never_raises(monkeypatch, capsys) -> None:
    def broken(**kwargs):
        raise RuntimeError("callback bug")

    monkeypatch.setattr(steps, "record_failure", broken)
    context = {"logical_date": datetime(2026, 9, 1), "run_id": "r1", "params": {}}

    callbacks.on_task_failure(context)  # does not raise

    assert "Failure callback could not complete" in capsys.readouterr().out


def test_run_step_passes_the_run_arguments(monkeypatch) -> None:
    seen = {}

    def fake_step(**kwargs):
        seen.update(kwargs)
        return {"ok": True}

    context = {"logical_date": datetime(2026, 9, 2), "run_id": "r2", "params": {}}

    assert callbacks.run_step(fake_step, context, dataset="orders") == {"ok": True}
    assert seen == {
        "run_date": "2026-09-02",
        "run_id": "r2",
        "load_type": "incremental",
        "dataset": "orders",
    }
