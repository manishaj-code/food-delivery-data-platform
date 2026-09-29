"""Validate all datasets of one run in dependency order (FR-020 – FR-025).

Parents are validated first so child referential rules can use the parents' valid
records of this batch, plus keys already present in ``processed/`` from earlier run dates.
All outputs and reports are written before the quality gate is applied, so a failing
run can still be investigated.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.common.config import Settings
from src.common.constants import BUSINESS_KEYS, DATASETS
from src.common.exceptions import SourceFileError
from src.common.logging_config import log_context
from src.common.paths import raw_file_path, spark_uri
from src.common.storage import Storage
from src.transformation.processed_reader import read_processed_keys
from src.validation.quarantine import write_quarantine, write_validated
from src.validation.report import build_report, enforce_threshold, log_report, write_report
from src.validation.rule_catalog import parent_datasets
from src.validation.rules import RuleContext
from src.validation.validator import ValidationOutcome, read_raw, validate

logger = logging.getLogger(__name__)


def _parent_keys(
    spark: SparkSession,
    storage: Storage,
    settings: Settings,
    parent: str,
    batch: ValidationOutcome | None,
    run_date: date,
) -> DataFrame:
    key_column = BUSINESS_KEYS[parent]
    frames = [batch.valid.select(key_column)] if batch else []
    processed = read_processed_keys(spark, storage, settings, parent, before=run_date)
    if processed is not None:
        frames.append(processed)
    if not frames:
        return spark.createDataFrame([], f"{key_column} string")
    keys = frames[0]
    for frame in frames[1:]:
        keys = keys.unionByName(frame)
    return keys


def validate_dataset(
    spark: SparkSession,
    storage: Storage,
    settings: Settings,
    dataset: str,
    run_date: date,
    run_id: str,
    parents: dict[str, ValidationOutcome],
) -> tuple[ValidationOutcome, dict[str, Any]]:
    """Validate one dataset, write validated/quarantine/report, return outcome and report."""
    raw_key = raw_file_path(dataset, run_date)
    if not storage.exists(raw_key):
        raise SourceFileError(
            "Raw partition not found; run ingestion first", dataset=dataset, key=raw_key
        )

    context = RuleContext(
        run_date=run_date,
        parent_keys={
            parent: _parent_keys(spark, storage, settings, parent, parents.get(parent), run_date)
            for parent in parent_datasets(dataset)
        },
    )
    raw = read_raw(spark, spark_uri(raw_key, settings), dataset)
    outcome = validate(raw, dataset, context, validated_at=datetime.now(UTC))

    write_validated(outcome.valid, storage, settings, dataset, run_date, run_id)
    write_quarantine(outcome.invalid, storage, settings, dataset, run_date, run_id)
    report = build_report(outcome, run_id, run_date, settings.dq_min_quality_score)
    log_report(report)
    write_report(storage, report)
    return outcome, report


def validate_all(
    spark: SparkSession,
    storage: Storage,
    settings: Settings,
    run_date: date,
    run_id: str,
    datasets: Sequence[str] = DATASETS,
    enforce_gate: bool = True,
) -> list[dict[str, Any]]:
    """Validate ``datasets`` (parents first) and apply the quality gate. Returns the reports."""
    ordered = [name for name in DATASETS if name in datasets]
    outcomes: dict[str, ValidationOutcome] = {}
    reports: list[dict[str, Any]] = []
    try:
        for dataset in ordered:
            with log_context(dataset=dataset, run_id=run_id, run_date=run_date.isoformat()):
                outcome, report = validate_dataset(
                    spark, storage, settings, dataset, run_date, run_id, outcomes
                )
                outcomes[dataset] = outcome
                reports.append(report)
    finally:
        for outcome in outcomes.values():
            outcome.release()

    if enforce_gate:
        enforce_threshold(reports, settings.dq_min_quality_score)
    return reports
