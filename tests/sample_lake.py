"""Shared end-to-end fixture: sample data ingested and validated into a temporary lake.

Historical sample -> run date 2026-08-31; its validated output is then "promoted" to
``processed/`` (standing in for Phase 6), and the 2026-09-01 increment is validated
against those earlier parents.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.common.config import Settings, load_settings
from src.common.constants import DATASETS
from src.common.paths import Zone, partition_path
from src.common.storage import LocalStorage
from src.ingestion import INGESTORS
from src.validation.run_validation import validate_all

SAMPLE_DIR = Path(__file__).resolve().parents[2] / "data" / "sample"
HISTORICAL_RUN = date(2026, 8, 31)
DAILY_RUN = date(2026, 9, 1)


@dataclass
class ValidatedLake:
    storage: LocalStorage
    settings: Settings
    historical: dict[str, dict[str, Any]]  # dataset -> report
    daily: dict[str, dict[str, Any]]


def manifest(label: str) -> dict[str, int]:
    path = SAMPLE_DIR / f"_bad_records_manifest_{label}.json"
    return json.loads(path.read_text(encoding="utf-8"))["expected_failures"]


def make_settings(lake: Path) -> Settings:
    return load_settings({"LOCAL_LAKE_PATH": str(lake), "SOURCE_DATA_PATH": str(SAMPLE_DIR)})


def ingest(storage: LocalStorage, run_date: date, load_type: str, run_id: str) -> None:
    for ingestor in INGESTORS.values():
        ingestor(storage, SAMPLE_DIR).run(run_date, load_type, run_id)


def promote_to_processed(storage: LocalStorage, run_date: date) -> None:
    for dataset in DATASETS:
        source = partition_path(Zone.VALIDATED, dataset, run_date)
        target = partition_path(Zone.PROCESSED, dataset, run_date)
        for key in storage.list(source):
            storage.copy(key, target + key[len(source) :])


@pytest.fixture(scope="session")
def validated_lake(spark, tmp_path_factory: pytest.TempPathFactory) -> ValidatedLake:
    root = tmp_path_factory.mktemp("lake")
    storage, settings = LocalStorage(root), make_settings(root)

    ingest(storage, HISTORICAL_RUN, "historical", "test__historical")
    historical = validate_all(spark, storage, settings, HISTORICAL_RUN, "test__historical")
    promote_to_processed(storage, HISTORICAL_RUN)

    ingest(storage, DAILY_RUN, "incremental", "test__daily")
    daily = validate_all(spark, storage, settings, DAILY_RUN, "test__daily")

    return ValidatedLake(
        storage,
        settings,
        {report["dataset"]: report for report in historical},
        {report["dataset"]: report for report in daily},
    )
