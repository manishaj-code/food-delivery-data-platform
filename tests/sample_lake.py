"""Shared end-to-end fixture: sample data ingested and validated into a temporary lake.

Historical sample -> run date 2026-08-31: ingest, validate, transform, stage, publish
``processed/``. Then the 2026-09-01 increment goes through the same steps against
those earlier parents.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.common.config import Settings, load_settings
from src.common.storage import LocalStorage
from src.ingestion import INGESTORS
from src.transformation.processed_writer import write_processed
from src.transformation.publish import publish_processed
from src.transformation.transform_job import TransformationResult, run_transformations
from src.validation.run_validation import validate_all

SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"
HISTORICAL_RUN = date(2026, 8, 31)
DAILY_RUN = date(2026, 9, 1)


@dataclass
class ValidatedLake:
    storage: LocalStorage
    settings: Settings
    historical: dict[str, dict[str, Any]]  # dataset -> report
    daily: dict[str, dict[str, Any]]
    historical_transform: TransformationResult  # both published to processed/
    daily_transform: TransformationResult


def manifest(label: str) -> dict[str, int]:
    path = SAMPLE_DIR / f"_bad_records_manifest_{label}.json"
    return json.loads(path.read_text(encoding="utf-8"))["expected_failures"]


def make_settings(lake: Path) -> Settings:
    return load_settings({"LOCAL_LAKE_PATH": str(lake), "SOURCE_DATA_PATH": str(SAMPLE_DIR)})


def ingest(storage: LocalStorage, run_date: date, load_type: str, run_id: str) -> None:
    for ingestor in INGESTORS.values():
        ingestor(storage, SAMPLE_DIR).run(run_date, load_type, run_id)


def transform_and_publish(
    spark, storage: LocalStorage, settings: Settings, run_date: date, run_id: str
) -> TransformationResult:
    """Transform validated/, stage the outputs, and publish them to processed/."""
    result = run_transformations(spark, storage, settings, run_date, run_id)
    write_processed(result, storage, settings)
    publish_processed(storage, run_date, run_id)
    return result


@pytest.fixture(scope="session")
def validated_lake(spark, tmp_path_factory: pytest.TempPathFactory) -> ValidatedLake:
    root = tmp_path_factory.mktemp("lake")
    storage, settings = LocalStorage(root), make_settings(root)

    ingest(storage, HISTORICAL_RUN, "historical", "test__historical")
    historical = validate_all(spark, storage, settings, HISTORICAL_RUN, "test__historical")
    historical_transform = transform_and_publish(
        spark, storage, settings, HISTORICAL_RUN, "test__historical"
    )

    ingest(storage, DAILY_RUN, "incremental", "test__daily")
    daily = validate_all(spark, storage, settings, DAILY_RUN, "test__daily")
    daily_transform = transform_and_publish(spark, storage, settings, DAILY_RUN, "test__daily")

    return ValidatedLake(
        storage,
        settings,
        {report["dataset"]: report for report in historical},
        {report["dataset"]: report for report in daily},
        historical_transform,
        daily_transform,
    )
