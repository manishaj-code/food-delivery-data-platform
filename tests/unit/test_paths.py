"""Tests for src/common/paths.py (FR-040, FR-041)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.common.config import load_settings
from src.common.paths import (
    Zone,
    audit_path,
    partition_path,
    raw_file_path,
    report_path,
    spark_uri,
    temp_path,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 9, 29)


@pytest.mark.parametrize("zone", list(Zone))
def test_every_zone_is_partitioned_by_run_date(zone: Zone) -> None:
    assert partition_path(zone, "orders", RUN_DATE) == (
        f"{zone.value}/orders/year=2026/month=09/day=29/"
    )


def test_partition_values_are_zero_padded() -> None:
    assert partition_path("processed", "customers", date(2026, 1, 5)) == (
        "processed/customers/year=2026/month=01/day=05/"
    )


def test_unknown_zone_is_rejected() -> None:
    with pytest.raises(ValueError):
        partition_path("bronze", "orders", RUN_DATE)


def test_file_paths() -> None:
    assert raw_file_path("orders", RUN_DATE) == "raw/orders/year=2026/month=09/day=29/orders.csv"
    assert report_path("orders", RUN_DATE) == (
        "reports/data_quality/year=2026/month=09/day=29/orders.json"
    )
    assert audit_path("ingestion", "orders", RUN_DATE) == (
        "reports/audit/year=2026/month=09/day=29/ingestion__orders.json"
    )


def test_temp_path_is_run_scoped_and_key_safe() -> None:
    assert temp_path(Zone.RAW, "orders", "scheduled__2026-09-29T00:00:00+00:00") == (
        "_tmp/scheduled__2026-09-29T00_00_00_00_00/raw/orders/"
    )
    assert temp_path(Zone.RAW, "orders", "a") != temp_path(Zone.RAW, "orders", "b")


def test_spark_uri_local_and_s3(tmp_path: Path) -> None:
    key = partition_path(Zone.PROCESSED, "orders", RUN_DATE)
    local = load_settings({"LOCAL_LAKE_PATH": str(tmp_path)})
    s3 = load_settings({"STORAGE_MODE": "s3", "S3_BUCKET": "food-delivery-data-dev"})

    assert spark_uri(key, local) == (
        f"file://{tmp_path.resolve().as_posix()}/processed/orders/year=2026/month=09/day=29"
    )
    assert spark_uri(key, s3) == (
        "s3a://food-delivery-data-dev/processed/orders/year=2026/month=09/day=29/"
    )
