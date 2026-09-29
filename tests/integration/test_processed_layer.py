"""Processed layer: stage -> verify -> publish (FR-040 â€“ FR-044, FR-090, AC-009, AC-012).

The shared ``validated_lake`` fixture has published both sample run dates. Tests that
change the lake work on a copy of it.
"""

from __future__ import annotations

import dataclasses
import json
import shutil
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.common.constants import CUSTOMERS, ORDERS, PAYMENTS
from src.common.exceptions import TransformationError
from src.common.paths import TEMP_ROOT, Zone, partition_path, spark_uri
from src.common.storage import LocalStorage, S3Storage
from src.transformation.processed_reader import read_processed, read_processed_keys
from src.transformation.processed_writer import STAGED_MARKER, staging_prefix, write_processed
from src.transformation.publish import (
    MANIFEST_NAME,
    publish_processed,
    read_manifest,
    verify_staged,
)
from src.transformation.schemas import PROCESSED_DATASETS, PROCESSED_SCHEMAS
from tests.conftest import TEST_BUCKET
from tests.sample_lake import DAILY_RUN, HISTORICAL_RUN, ValidatedLake, make_settings

pytestmark = [pytest.mark.integration, pytest.mark.spark]

RUNS = {HISTORICAL_RUN: "historical_transform", DAILY_RUN: "daily_transform"}


def _parts(storage, dataset: str, run_date: date) -> list[str]:
    return [
        key
        for key in storage.list(partition_path(Zone.PROCESSED, dataset, run_date))
        if key.endswith(".parquet")
    ]


def _snapshot(storage: LocalStorage, run_date: date) -> dict[str, bytes]:
    """Every file of every processed partition for ``run_date`` (key -> content)."""
    return {
        key: storage.read_bytes(key)
        for dataset in PROCESSED_DATASETS
        for key in storage.list(partition_path(Zone.PROCESSED, dataset, run_date))
    }


@pytest.fixture
def lake_copy(validated_lake: ValidatedLake, tmp_path: Path):
    root = tmp_path / "lake"
    shutil.copytree(validated_lake.storage.root, root)
    return LocalStorage(root), make_settings(root)


@pytest.mark.parametrize("run_date", RUNS)
@pytest.mark.parametrize("dataset", PROCESSED_DATASETS)
def test_every_partition_has_a_manifest(validated_lake: ValidatedLake, dataset, run_date) -> None:
    """AC-012: row count, run_id, schema, files, and source partitions."""
    storage = validated_lake.storage
    result = getattr(validated_lake, RUNS[run_date])

    manifest = read_manifest(storage, dataset, run_date)

    assert manifest["run_id"] == result.run_id
    assert manifest["run_date"] == run_date.isoformat()
    assert manifest["row_count"] == result.counts[dataset]
    assert [column["name"] for column in manifest["schema"]] == PROCESSED_SCHEMAS[dataset].names
    assert [file["name"] for file in manifest["files"]] == [
        key.rsplit("/", 1)[1] for key in _parts(storage, dataset, run_date)
    ]
    assert sum(file["row_count"] for file in manifest["files"]) == manifest["row_count"]
    assert all(storage.list(prefix) for prefix in manifest["source_partitions"])


def test_partitions_hold_only_parquet_and_the_manifest(validated_lake: ValidatedLake) -> None:
    storage = validated_lake.storage
    for dataset in PROCESSED_DATASETS:
        prefix = partition_path(Zone.PROCESSED, dataset, HISTORICAL_RUN)
        names = {key.rsplit("/", 1)[1] for key in storage.list(prefix)}
        assert MANIFEST_NAME in names
        assert all(name.endswith(".parquet") for name in names - {MANIFEST_NAME})


def test_staging_is_empty_after_publish(validated_lake: ValidatedLake) -> None:
    assert validated_lake.storage.list(f"{TEMP_ROOT}/") == []


def test_parquet_is_readable_without_spark(validated_lake: ValidatedLake) -> None:
    """Portability: pyarrow reads the partition (skipping ``_manifest.json``) with COPY types."""
    storage = validated_lake.storage
    directory = storage.root / partition_path(Zone.PROCESSED, ORDERS, HISTORICAL_RUN)

    table = pq.read_table(directory)

    assert table.num_rows == validated_lake.historical_transform.counts[ORDERS]
    assert table.schema.field("order_timestamp").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("order_date").type == pa.date32()
    assert table.schema.field("order_amount").type == pa.decimal128(10, 2)
    assert "year" not in table.schema.names  # partition values live in the path only


def test_reader_combines_run_dates_and_ignores_manifests(
    spark, validated_lake: ValidatedLake
) -> None:
    """Keys from both published dates are readable; only earlier dates are visible."""
    lake = validated_lake
    counts = {run_date: getattr(lake, attr).counts[ORDERS] for run_date, attr in RUNS.items()}

    everything = read_processed(spark, lake.storage, lake.settings, ORDERS, date(2026, 9, 2))
    before_daily = read_processed(spark, lake.storage, lake.settings, ORDERS, DAILY_RUN)
    keys = read_processed_keys(spark, lake.storage, lake.settings, CUSTOMERS, date(2026, 9, 2))

    assert everything.count() == sum(counts.values())
    assert before_daily.count() == counts[HISTORICAL_RUN]
    assert keys.count() == keys.distinct().count()


def test_rerun_replaces_the_partition(lake_copy, validated_lake: ValidatedLake) -> None:
    """FR-090: publishing a run date again gives the same files and counts, new run_id."""
    storage, settings = lake_copy
    before = {dataset: _parts(storage, dataset, HISTORICAL_RUN) for dataset in PROCESSED_DATASETS}
    rerun = dataclasses.replace(validated_lake.historical_transform, run_id="test__rerun")

    write_processed(rerun, storage, settings)
    manifests = publish_processed(storage, HISTORICAL_RUN, "test__rerun")

    for manifest in manifests:
        dataset = manifest["dataset"]
        assert manifest["run_id"] == "test__rerun"
        assert manifest["row_count"] == rerun.counts[dataset]
        assert len(_parts(storage, dataset, HISTORICAL_RUN)) == len(before[dataset])
    assert storage.list(f"{TEMP_ROOT}/") == []


def test_publish_retry_after_success_is_a_no_op(lake_copy) -> None:
    storage, _ = lake_copy
    before = _snapshot(storage, DAILY_RUN)

    manifests = publish_processed(storage, DAILY_RUN, "test__daily")

    assert {manifest["run_id"] for manifest in manifests} == {"test__daily"}
    assert _snapshot(storage, DAILY_RUN) == before


def test_failed_verification_publishes_nothing(lake_copy, validated_lake: ValidatedLake) -> None:
    """TR-06: one bad staged dataset -> every existing partition stays as it was."""
    storage, settings = lake_copy
    before = _snapshot(storage, HISTORICAL_RUN)
    rerun = dataclasses.replace(validated_lake.historical_transform, run_id="test__broken")
    write_processed(rerun, storage, settings)
    marker_key = staging_prefix(PAYMENTS, "test__broken") + STAGED_MARKER
    marker = json.loads(storage.read_bytes(marker_key))
    storage.write_text(marker_key, json.dumps(marker | {"row_count": marker["row_count"] + 1}))

    with pytest.raises(TransformationError, match="row count") as excinfo:
        publish_processed(storage, HISTORICAL_RUN, "test__broken")

    assert excinfo.value.context["dataset"] == PAYMENTS
    assert _snapshot(storage, HISTORICAL_RUN) == before
    assert storage.list(staging_prefix(CUSTOMERS, "test__broken"))  # kept for investigation


def test_missing_staged_output_is_rejected(lake_copy) -> None:
    storage, _ = lake_copy

    with pytest.raises(TransformationError, match="rerun the transformation"):
        publish_processed(storage, date(2026, 9, 3), "never_transformed")


def test_interrupted_staging_is_rejected(spark, lake_copy, validated_lake) -> None:
    """Parquet written but no ``_staged.json`` (the writer died) -> not publishable."""
    storage, settings = lake_copy
    df = validated_lake.historical_transform.datasets[CUSTOMERS]
    prefix = staging_prefix(CUSTOMERS, "test__interrupted")
    df.write.parquet(spark_uri(prefix, settings))

    with pytest.raises(TransformationError, match="incomplete"):
        verify_staged(storage, CUSTOMERS, "test__interrupted")


def test_schema_drift_is_rejected(spark, lake_copy, validated_lake) -> None:
    storage, settings = lake_copy
    customers = validated_lake.historical_transform.datasets[CUSTOMERS]
    drifted = customers.withColumn("signup_date", customers.signup_date.cast("string"))
    result = dataclasses.replace(
        validated_lake.historical_transform,
        run_id="test__drift",
        datasets={**validated_lake.historical_transform.datasets, CUSTOMERS: drifted},
    )
    write_processed(result, storage, settings)

    with pytest.raises(TransformationError, match="schema") as excinfo:
        verify_staged(storage, CUSTOMERS, "test__drift")

    assert excinfo.value.context["differences"] == ["signup_date: expected date32[day], got string"]


def test_publish_to_s3(s3_client, validated_lake: ValidatedLake) -> None:
    """Publish is plain Python + pyarrow, so it runs against S3 without Spark (moto)."""
    local, s3 = validated_lake.storage, S3Storage(TEST_BUCKET, client=s3_client)
    target = partition_path(Zone.PROCESSED, CUSTOMERS, HISTORICAL_RUN)
    prefix = staging_prefix(CUSTOMERS, "test__s3")
    for key in _parts(local, CUSTOMERS, HISTORICAL_RUN):
        s3.write_bytes(prefix + key.rsplit("/", 1)[1], local.read_bytes(key))
    count = validated_lake.historical_transform.counts[CUSTOMERS]
    s3.write_text(prefix + STAGED_MARKER, json.dumps({"row_count": count}))

    [manifest] = publish_processed(s3, HISTORICAL_RUN, "test__s3", datasets=(CUSTOMERS,))

    assert manifest["row_count"] == count
    assert s3.exists(target + MANIFEST_NAME)
    assert s3.list(f"{TEMP_ROOT}/") == []
