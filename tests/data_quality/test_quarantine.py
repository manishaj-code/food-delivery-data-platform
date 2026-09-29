"""Validated and quarantine outputs (FR-023, FR-091, AC-022, AC-023)."""

from __future__ import annotations

import pytest

from src.common.constants import DATASETS, INGESTION_METADATA_COLUMNS, SOURCE_COLUMNS
from src.common.paths import Zone, partition_path, spark_uri
from src.validation.run_validation import validate_all
from tests.sample_lake import HISTORICAL_RUN, ValidatedLake

pytestmark = pytest.mark.spark


def _read(spark, lake: ValidatedLake, zone: Zone, dataset: str):
    uri = spark_uri(partition_path(zone, dataset, HISTORICAL_RUN), lake.settings)
    return spark.read.parquet(uri)


@pytest.mark.parametrize("dataset", DATASETS)
def test_valid_plus_quarantined_equals_total(spark, validated_lake: ValidatedLake, dataset) -> None:
    report = validated_lake.historical[dataset]

    valid = _read(spark, validated_lake, Zone.VALIDATED, dataset).count()
    quarantined = _read(spark, validated_lake, Zone.QUARANTINE, dataset).count()

    assert valid == report["valid_records"]
    assert quarantined == report["invalid_records"]
    assert valid + quarantined == report["total_records"]


def test_quarantine_keeps_original_values_and_failed_rules(
    spark, validated_lake: ValidatedLake
) -> None:
    quarantine = _read(spark, validated_lake, Zone.QUARANTINE, "orders")

    assert quarantine.columns == [
        *SOURCE_COLUMNS["orders"],
        *INGESTION_METADATA_COLUMNS,
        "_dq_failed_rules",
        "_dq_validated_at",
    ]
    assert dict(quarantine.dtypes)["order_amount"] == "string"  # original value, not cast
    rows = {row.order_id: row for row in quarantine.collect()}
    negative = [row for row in rows.values() if (row.order_amount or "").startswith("-")]
    null_customer = [row for row in rows.values() if row.customer_id is None]

    assert negative and all("DQ-ORD-006" in row._dq_failed_rules for row in negative)
    assert null_customer
    assert all(row._dq_failed_rules.startswith("DQ-ORD-003;DQ-ORD-004") for row in null_customer)
    assert all(row._dq_validated_at is not None and row._run_id for row in rows.values())


def test_validated_output_is_typed(spark, validated_lake: ValidatedLake) -> None:
    types = dict(_read(spark, validated_lake, Zone.VALIDATED, "orders").dtypes)

    assert types["order_amount"] == "decimal(10,2)"
    assert types["order_date"] == "timestamp"
    assert types["_ingestion_date"] == "date"
    assert types["_source_row_number"] == "int"


def test_rerun_overwrites_partitions(spark, validated_lake: ValidatedLake) -> None:
    storage = validated_lake.storage
    prefixes = [
        partition_path(zone, dataset, HISTORICAL_RUN)
        for zone in (Zone.VALIDATED, Zone.QUARANTINE)
        for dataset in ("customers", "restaurants")
    ]
    before = {prefix: storage.list(prefix) for prefix in prefixes}

    validate_all(
        spark,
        storage,
        validated_lake.settings,
        HISTORICAL_RUN,
        "test__rerun",
        datasets=("customers", "restaurants"),
    )

    after = {prefix: storage.list(prefix) for prefix in prefixes}
    assert {p: len(keys) for p, keys in after.items()} == {
        p: len(keys) for p, keys in before.items()
    }
    assert all(len(keys) >= 1 for keys in after.values())
    assert storage.list("_tmp/") == []
    reran = _read(spark, validated_lake, Zone.VALIDATED, "customers")
    assert {row._run_id for row in reran.select("_run_id").distinct().collect()} == {
        "test__historical"  # raw metadata is carried through; the raw file was not re-ingested
    }
