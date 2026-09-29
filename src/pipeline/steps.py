"""Pipeline steps: the only API the Airflow DAG and the CLI call (FR-071, spec 07 §3).

Every step takes ``run_date``, ``run_id``, and ``load_type``, sets the logging context,
measures its duration, writes stage audit records to ``reports/audit/…`` in the lake,
and returns a small JSON-serialisable dict (Airflow XCom). A failing step logs the error
once, writes a ``FAILED`` audit record, and re-raises; ``is_retryable`` tells the caller
whether a retry can help (spec 07 §9). ``pipeline_summary`` inserts the run's audit
records into ``pipeline_run_audit``.

Spark, validation, and transformation modules are imported inside the steps, so
importing this module (e.g. when Airflow parses the DAG) stays fast.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any

from src.common.config import Settings, get_settings
from src.common.constants import DATASETS, LOAD_TYPE_HISTORICAL, LOAD_TYPES
from src.common.exceptions import ConfigError, PipelineError, SourceFileError
from src.common.logging_config import log_context
from src.common.metrics import publish_metric
from src.common.paths import Zone, audit_path, date_partition
from src.common.sources import CsvFileSource
from src.common.storage import Storage, get_storage
from src.warehouse.audit import AuditRecord

logger = logging.getLogger(__name__)

# Stages of pipeline_run_audit (spec 12 §4).
STAGE_PREPARE = "prepare_source"
STAGE_INGESTION = "ingestion"
STAGE_VALIDATION = "validation"
STAGE_TRANSFORMATION = "transformation"
STAGE_PUBLISH = "publish"
STAGE_WAREHOUSE_LOAD = "warehouse_load"
STAGE_POST_LOAD_CHECKS = "post_load_checks"
STAGE_PIPELINE = "pipeline"
PIPELINE_DATASET = "_pipeline"  # audit rows that are not about one dataset

SUCCESS, NO_DATA, FAILED = "SUCCESS", "NO_DATA", "FAILED"

# The source-system simulator creates a missing daily increment (spec 07 §3 stage 0).
GENERATOR_PROFILE = "full"
GENERATOR_SEED = 42


# --------------------------------------------------------------------------- helpers


def _settings() -> Settings:
    return get_settings()


def _now() -> datetime:
    """UTC now without tzinfo: warehouse TIMESTAMP columns hold UTC."""
    return datetime.now(UTC).replace(tzinfo=None)


def parse_run_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ConfigError("run_date must be YYYY-MM-DD", run_date=value) from exc


def _check_load_type(load_type: str) -> str:
    if load_type not in LOAD_TYPES:
        raise ConfigError(f"load_type must be one of {LOAD_TYPES}", load_type=load_type)
    return load_type


def is_retryable(exc: BaseException) -> bool:
    """Typed pipeline errors say whether retrying helps; unexpected errors are retried."""
    if isinstance(exc, PipelineError):
        return exc.retryable
    return True


def error_message(exc: BaseException) -> str:
    """Audit/log text of an error: our errors carry context, never secrets (spec 11 §11)."""
    return str(exc) if isinstance(exc, PipelineError) else f"{type(exc).__name__}: {exc}"


# --------------------------------------------------------------------------- audit files


def _audit_to_json(record: AuditRecord) -> str:
    values = {
        key: value.isoformat() if isinstance(value, date | datetime) else value
        for key, value in asdict(record).items()
    }
    return json.dumps(values, indent=2)


def _audit_from_json(data: bytes) -> AuditRecord:
    values = json.loads(data)
    values["run_date"] = date.fromisoformat(values["run_date"])
    for key in ("started_at", "finished_at"):
        if values[key]:
            values[key] = datetime.fromisoformat(values[key])
    return AuditRecord(**values)


def save_audit(storage: Storage, record: AuditRecord) -> None:
    """Write one stage record; a rerun of the same (stage, dataset, date) replaces it."""
    storage.write_text(
        audit_path(record.stage, record.dataset, record.run_date), _audit_to_json(record)
    )


def load_run_audits(storage: Storage, run_date: date, run_id: str) -> list[AuditRecord]:
    """Stage records of ``run_id`` for ``run_date`` (older runs of the date are ignored)."""
    prefix = f"{Zone.REPORTS}/audit/{date_partition(run_date)}/"
    records = [_audit_from_json(storage.read_bytes(key)) for key in storage.list(prefix)]
    return [record for record in records if record.run_id == run_id]


@dataclass
class StepRun:
    """What a running step knows about itself; builds its audit records."""

    stage: str
    run_date: date
    run_id: str
    load_type: str
    settings: Settings
    storage: Storage
    started_at: datetime
    started: float  # perf_counter

    def record(self, dataset: str, status: str, **counts: Any) -> AuditRecord:
        record = AuditRecord(
            run_id=self.run_id,
            run_date=self.run_date,
            load_type=self.load_type,
            dataset=dataset,
            stage=self.stage,
            status=status,
            started_at=self.started_at,
            finished_at=_now(),
            duration_seconds=round(time.perf_counter() - self.started, 2),
            **counts,
        )
        save_audit(self.storage, record)
        return record


@contextmanager
def _step(
    stage: str,
    run_date: date | str,
    run_id: str,
    load_type: str,
    dataset: str = PIPELINE_DATASET,
) -> Iterator[StepRun]:
    """Run context, timing, and failure handling shared by every step."""
    day = parse_run_date(run_date)
    with log_context(
        run_id=run_id,
        run_date=day.isoformat(),
        dataset="-" if dataset == PIPELINE_DATASET else dataset,
    ):
        _check_load_type(load_type)
        settings = _settings()
        storage = get_storage(settings)
        step = StepRun(
            stage, day, run_id, load_type, settings, storage, _now(), time.perf_counter()
        )
        logger.info("Starting step %s (load_type=%s)", stage, load_type)
        if dataset == PIPELINE_DATASET:
            # Drop the step-level record of an earlier failed attempt; this attempt
            # writes its own (per-dataset records are simply overwritten).
            with suppress(PipelineError):
                storage.delete_prefix(audit_path(stage, PIPELINE_DATASET, day))
        try:
            yield step
        except Exception as exc:
            logger.exception("Step %s failed: %s", stage, error_message(exc))
            try:
                step.record(dataset, FAILED, error_message=error_message(exc))
            except Exception:  # noqa: BLE001 — never hide the original error
                logger.warning("Could not write the FAILED audit record", exc_info=True)
            raise
        logger.info(
            "Step %s finished in %.2fs", stage, round(time.perf_counter() - step.started, 2)
        )


# --------------------------------------------------------------------------- steps


def prepare_source_data(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """Ensure the run's source CSVs exist; generate a missing daily increment (stage 0)."""
    with _step(STAGE_PREPARE, run_date, run_id, load_type) as step:
        root = step.settings.source_data_path
        files = {
            dataset: CsvFileSource.for_run(root, dataset, load_type, step.run_date).path
            for dataset in DATASETS
        }
        missing = [dataset for dataset, path in files.items() if not path.exists()]
        generated = False
        if missing and load_type == LOAD_TYPE_HISTORICAL:
            raise SourceFileError(
                "Historical source files missing; run generate-data --mode historical",
                missing=missing,
                path=str(root),
            )
        if missing and len(missing) < len(files):
            raise SourceFileError(
                "Some source files of this run are missing", missing=missing, path=str(root)
            )
        if missing:
            _generate_increment(step)
            generated = True
        step.record(PIPELINE_DATASET, SUCCESS, records_out=len(files))
        return {
            "generated": generated,
            "files": {dataset: str(path) for dataset, path in files.items()},
        }


def _generate_increment(step: StepRun) -> None:
    from scripts import generate_data

    profile = generate_data.PROFILES[GENERATOR_PROFILE]
    try:
        data = generate_data.generate_incremental(profile, GENERATOR_SEED, step.run_date)
    except ValueError as exc:
        raise SourceFileError(
            "No source files for this run date and no increment can be generated",
            error=exc,
        ) from exc
    generate_data.write_generated(
        data, step.settings.source_data_path, GENERATOR_PROFILE, GENERATOR_SEED
    )
    logger.info("Generated the daily increment for %s", step.run_date.isoformat())


def ingest_dataset(
    dataset: str, run_date: date | str, run_id: str, load_type: str
) -> dict[str, Any]:
    """Source CSV -> ``raw/`` for one dataset (stage 1)."""
    from src.ingestion import INGESTORS

    with _step(STAGE_INGESTION, run_date, run_id, load_type, dataset) as step:
        if dataset not in INGESTORS:
            raise ConfigError("Unknown dataset", dataset=dataset)
        ingestor = INGESTORS[dataset](step.storage, step.settings.source_data_path)
        result = ingestor.run(step.run_date, load_type, run_id)
        step.record(
            dataset,
            result.status,
            records_in=result.records_read,
            records_out=result.records_written,
        )
        return result.to_dict()


_REPORT_STATUS = {"PASSED": SUCCESS, "NO_DATA": NO_DATA, "FAILED": FAILED}


def validate_data(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """PySpark validation of all datasets, then the quality gate (stage 2).

    Reports and audit records are written for every dataset before the gate raises.
    """
    from src.common.spark import get_spark
    from src.validation.report import enforce_threshold
    from src.validation.run_validation import validate_all

    with _step(STAGE_VALIDATION, run_date, run_id, load_type) as step:
        spark = get_spark(step.settings)
        reports = validate_all(
            spark, step.storage, step.settings, step.run_date, run_id, enforce_gate=False
        )
        for report in reports:
            step.record(
                report["dataset"],
                _REPORT_STATUS[report["status"]],
                records_in=report["total_records"],
                records_out=report["valid_records"],
                records_rejected=report["invalid_records"],
                quality_score=report["quality_score"],
            )
        enforce_threshold(reports, step.settings.dq_min_quality_score)
        return {
            report["dataset"]: {
                "total": report["total_records"],
                "valid": report["valid_records"],
                "invalid": report["invalid_records"],
                "quality_score": report["quality_score"],
            }
            for report in reports
        }


def transform_data(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """Validated -> staged processed Parquet for all eight datasets (stage 3)."""
    from src.common.spark import get_spark
    from src.transformation.processed_writer import write_processed
    from src.transformation.transform_job import run_transformations

    with _step(STAGE_TRANSFORMATION, run_date, run_id, load_type) as step:
        spark = get_spark(step.settings)
        result = run_transformations(spark, step.storage, step.settings, step.run_date, run_id)
        try:
            write_processed(result, step.storage, step.settings)
        finally:
            result.release()
        for dataset, count in result.counts.items():
            step.record(dataset, SUCCESS if count else NO_DATA, records_out=count)
        return dict(result.counts)


def publish_processed(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """Verify the staged output and replace the ``processed/`` partitions (stage 4)."""
    from src.transformation import publish

    with _step(STAGE_PUBLISH, run_date, run_id, load_type) as step:
        manifests = publish.publish_processed(step.storage, step.run_date, run_id)
        for manifest in manifests:
            count = manifest["row_count"]
            step.record(manifest["dataset"], SUCCESS if count else NO_DATA, records_out=count)
        return {manifest["dataset"]: manifest["row_count"] for manifest in manifests}


def load_warehouse(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """``processed/`` partition -> staging -> dimensions and facts (stage 5)."""
    from src.warehouse import loader

    with _step(STAGE_WAREHOUSE_LOAD, run_date, run_id, load_type) as step:
        results = loader.load_warehouse(step.settings, step.storage, step.run_date, run_id)
        for result in results:
            step.record(
                result.dataset,
                result.status,
                records_in=result.records_in,
                records_out=result.records_staged,
            )
        return {
            result.table: {
                "staged": result.records_staged,
                "updated": result.rows_updated,
                "inserted": result.rows_inserted,
            }
            for result in results
        }


def run_dq_checks(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """Post-load warehouse checks WQ-001 – WQ-006 (stage 6)."""
    from src.warehouse.post_load_checks import run_post_load_checks

    with _step(STAGE_POST_LOAD_CHECKS, run_date, run_id, load_type) as step:
        results = run_post_load_checks(step.settings, step.storage, step.run_date, run_id)
        step.record(
            PIPELINE_DATASET,
            SUCCESS,
            records_in=len(results),
            records_rejected=sum(not result.passed for result in results),  # WARN only here
        )
        return {result.check_id: result.passed for result in results}


def _pipeline_record(
    records: list[AuditRecord],
    run_date: date,
    run_id: str,
    load_type: str,
    status: str,
    error: str | None = None,
) -> AuditRecord:
    """Run-level row: totals over the stage records, duration since the first stage."""
    finished = _now()
    started = min((r.started_at for r in records if r.started_at), default=finished)

    def total(stage: str, field: str) -> int | None:
        values = [getattr(r, field) for r in records if r.stage == stage]
        values = [value for value in values if value is not None]
        return sum(values) if values else None

    return AuditRecord(
        run_id=run_id,
        run_date=run_date,
        load_type=load_type,
        dataset=PIPELINE_DATASET,
        stage=STAGE_PIPELINE,
        status=status,
        records_in=total(STAGE_INGESTION, "records_in"),
        records_out=total(STAGE_WAREHOUSE_LOAD, "records_out"),
        records_rejected=total(STAGE_VALIDATION, "records_rejected"),
        started_at=started,
        finished_at=finished,
        duration_seconds=round((finished - started).total_seconds(), 2),
        error_message=error,
    )


def _write_run_audit(settings: Settings, records: list[AuditRecord]) -> int:
    from src.warehouse.audit import write_audit
    from src.warehouse.connection import warehouse_connection

    with warehouse_connection(settings) as conn:
        return write_audit(conn, settings.redshift_schema, records)


def _log_summary(records: list[AuditRecord]) -> None:
    for record in records:
        logger.info(
            "Summary %-16s %-20s %-8s in=%s out=%s rejected=%s score=%s duration=%ss",
            record.stage,
            record.dataset,
            record.status,
            record.records_in,
            record.records_out,
            record.records_rejected,
            record.quality_score,
            record.duration_seconds,
        )


def pipeline_summary(run_date: date | str, run_id: str, load_type: str) -> dict[str, Any]:
    """Persist the run's audit records, log the summary, emit metrics (stage 7, FR-075)."""
    with _step(STAGE_PIPELINE, run_date, run_id, load_type) as step:
        stages = [
            r
            for r in load_run_audits(step.storage, step.run_date, run_id)
            if r.stage != STAGE_PIPELINE
        ]
        failed = [f"{r.stage}/{r.dataset}" for r in stages if r.status == FAILED]
        if failed:
            raise PipelineError("Run has failed stages; not marking it successful", failed=failed)
        pipeline = _pipeline_record(stages, step.run_date, run_id, load_type, SUCCESS)
        save_audit(step.storage, pipeline)
        rows = _write_run_audit(step.settings, [*stages, pipeline])
        _log_summary([*stages, pipeline])
        publish_metric(step.settings, "PipelineSuccess", 1)
        publish_metric(
            step.settings, "PipelineDurationSeconds", pipeline.duration_seconds or 0, "Seconds"
        )
        logger.info(
            "Pipeline completed successfully (load_type=%s, duration=%.2fs, audit rows=%d)",
            load_type,
            pipeline.duration_seconds,
            rows,
        )
        return {
            "status": SUCCESS,
            "duration_seconds": pipeline.duration_seconds,
            "audit_rows": rows,
        }


def record_failure(
    run_date: date | str, run_id: str, load_type: str, task_id: str, error: str
) -> None:
    """Failure summary for a run (FR-074): log, FAILED pipeline audit, PipelineFailure metric.

    Called by the Airflow failure callback and the CLI. Best effort: never raises, so it
    cannot hide the original failure. Audit records are also flushed to the warehouse
    when it is reachable, so failed runs show up in ``pipeline_run_audit``.
    """
    day = parse_run_date(run_date)
    with log_context(run_id=run_id, run_date=day.isoformat(), dataset="-"):
        logger.error("Pipeline failed at task %s: %s", task_id, error)
        settings = _settings()
        try:
            storage = get_storage(settings)
            stages = [r for r in load_run_audits(storage, day, run_id) if r.stage != STAGE_PIPELINE]
            pipeline = _pipeline_record(
                stages, day, run_id, load_type, FAILED, error=f"{task_id}: {error}"
            )
            save_audit(storage, pipeline)
            _write_run_audit(settings, [*stages, pipeline])
        except Exception:  # noqa: BLE001 — reporting must not mask the task failure
            logger.warning("Could not persist the failure audit", exc_info=True)
        publish_metric(settings, "PipelineFailure", 1)
