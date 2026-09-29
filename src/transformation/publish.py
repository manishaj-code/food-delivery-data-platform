"""Publish staged processed outputs (FR-040 – FR-044, spec 07 §3 stage 4, §8).

Runs without Spark. Every staged dataset is verified first with pyarrow, reading only
the Parquet footers: the row count must equal the count the transform recorded and the
schema must equal the spec schema. Only when **all** datasets pass are the run-date
partitions replaced, one dataset at a time:

    delete target -> copy part files -> write ``_manifest.json`` (last) -> delete staging

A partition without ``_manifest.json`` is incomplete. Rerunning publish is safe: a dataset
whose staging is gone but whose manifest carries this run_id is already published.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from src.common.constants import CUSTOMERS, DELIVERY, ORDERS, PAYMENTS, RESTAURANTS
from src.common.exceptions import TransformationError
from src.common.logging_config import log_context
from src.common.paths import Zone, partition_path
from src.common.storage import Storage
from src.transformation.processed_writer import STAGED_MARKER, staging_prefix
from src.transformation.schemas import (
    DAILY_ORDER_METRICS,
    ORDER_ANALYTICS,
    PROCESSED_DATASETS,
    PROCESSED_SCHEMAS,
)

logger = logging.getLogger(__name__)

MANIFEST_NAME = "_manifest.json"

# Raw partitions each processed dataset is built from (lineage in the manifest).
_ANALYTICS_SOURCES = (ORDERS, PAYMENTS, DELIVERY, CUSTOMERS, RESTAURANTS)
SOURCE_DATASETS: dict[str, tuple[str, ...]] = {
    ORDER_ANALYTICS: _ANALYTICS_SOURCES,
    DAILY_ORDER_METRICS: _ANALYTICS_SOURCES,
}


def arrow_type(dtype: DataType) -> pa.DataType:
    """Arrow type a Spark column is stored as in the lake's Parquet files."""
    if isinstance(dtype, DecimalType):
        return pa.decimal128(dtype.precision, dtype.scale)
    if isinstance(dtype, TimestampType):
        return pa.timestamp("us", tz="UTC")  # TIMESTAMP_MICROS, adjusted to UTC
    simple = {
        StringType: pa.string(),
        DateType: pa.date32(),
        IntegerType: pa.int32(),
        LongType: pa.int64(),
        BooleanType: pa.bool_(),
    }
    return simple[type(dtype)]


@dataclass(frozen=True)
class StagedOutput:
    dataset: str
    prefix: str
    files: dict[str, int]  # part file key -> row count
    row_count: int


def verify_staged(storage: Storage, dataset: str, run_id: str) -> StagedOutput:
    """Check one staged dataset: complete, expected row count, spec schema."""
    prefix = staging_prefix(dataset, run_id)
    keys = storage.list(prefix)
    if prefix + STAGED_MARKER not in keys:
        raise TransformationError(
            "Staged output missing or incomplete; rerun the transformation",
            dataset=dataset,
            key=prefix,
        )
    expected_rows = json.loads(storage.read_bytes(prefix + STAGED_MARKER))["row_count"]
    expected_schema = [
        (field.name, arrow_type(field.dataType)) for field in PROCESSED_SCHEMAS[dataset].fields
    ]

    files = {}
    for key in (key for key in keys if key.endswith(".parquet")):
        parquet = pq.ParquetFile(pa.BufferReader(storage.read_bytes(key)))
        actual_schema = [(field.name, field.type) for field in parquet.schema_arrow]
        if actual_schema != expected_schema:
            raise TransformationError(
                "Staged Parquet schema does not match the processed schema",
                dataset=dataset,
                key=key,
                differences=_differences(expected_schema, actual_schema),
            )
        files[key] = parquet.metadata.num_rows

    row_count = sum(files.values())
    if not files or row_count != expected_rows:
        raise TransformationError(
            "Staged row count does not match the transformation count",
            dataset=dataset,
            key=prefix,
            expected=expected_rows,
            actual=row_count,
        )
    return StagedOutput(dataset, prefix, files, row_count)


def _differences(expected: list[tuple[str, Any]], actual: list[tuple[str, Any]]) -> list[str]:
    expected_map, actual_map = dict(expected), dict(actual)
    names = list(dict.fromkeys([name for name, _ in expected] + [name for name, _ in actual]))
    return [
        f"{name}: expected {expected_map.get(name)}, got {actual_map.get(name)}"
        for name in names
        if expected_map.get(name) != actual_map.get(name)
    ] or ["column order differs"]


def build_manifest(staged: StagedOutput, run_date: date, run_id: str) -> dict[str, Any]:
    sources = SOURCE_DATASETS.get(staged.dataset, (staged.dataset,))
    return {
        "dataset": staged.dataset,
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "row_count": staged.row_count,
        "files": [
            {"name": key.rsplit("/", 1)[1], "row_count": rows} for key, rows in staged.files.items()
        ],
        "schema": [
            {"name": field.name, "type": field.dataType.simpleString()}
            for field in PROCESSED_SCHEMAS[staged.dataset].fields
        ],
        "source_partitions": [partition_path(Zone.RAW, source, run_date) for source in sources],
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def read_manifest(storage: Storage, dataset: str, run_date: date) -> dict[str, Any] | None:
    key = partition_path(Zone.PROCESSED, dataset, run_date) + MANIFEST_NAME
    if not storage.exists(key):
        return None
    return json.loads(storage.read_bytes(key))


def _publish(storage: Storage, staged: StagedOutput, run_date: date, run_id: str) -> dict:
    target = partition_path(Zone.PROCESSED, staged.dataset, run_date)
    storage.delete_prefix(target)
    for key in staged.files:
        storage.copy(key, target + key[len(staged.prefix) :])
    manifest = build_manifest(staged, run_date, run_id)
    storage.write_text(target + MANIFEST_NAME, json.dumps(manifest, indent=2))
    storage.delete_prefix(staged.prefix)
    logger.info(
        "Published %s: %d records to %s", staged.dataset, staged.row_count, storage.uri_for(target)
    )
    return manifest


def publish_processed(
    storage: Storage,
    run_date: date,
    run_id: str,
    datasets: tuple[str, ...] = PROCESSED_DATASETS,
) -> list[dict[str, Any]]:
    """Verify all staged datasets, then replace their run-date partitions; returns manifests."""
    with log_context(run_id=run_id, run_date=run_date.isoformat()):
        logger.info("Starting publish of processed data")
        manifests, pending = [], []
        for dataset in datasets:
            with log_context(dataset=dataset):
                existing = read_manifest(storage, dataset, run_date)
                staged_marker = staging_prefix(dataset, run_id) + STAGED_MARKER
                if existing and existing["run_id"] == run_id and not storage.exists(staged_marker):
                    logger.info("%s already published by this run", dataset)
                    manifests.append(existing)
                else:
                    pending.append(verify_staged(storage, dataset, run_id))
        for staged in pending:  # nothing is replaced unless every dataset verified
            with log_context(dataset=staged.dataset):
                manifests.append(_publish(storage, staged, run_date, run_id))
        logger.info("Publish completed: %d dataset(s)", len(manifests))
        return manifests
