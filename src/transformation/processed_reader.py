"""Readers for data already in ``processed/`` (FR-022, FR-082).

Used for referential checks (validation) and lookups (transformation) in incremental
runs, where parents arrived on an earlier day. Only partitions **before** the run date
are read, so rerunning an older date gives the same result even after later loads.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Settings
from src.common.constants import BUSINESS_KEYS
from src.common.paths import Zone, spark_uri
from src.common.storage import Storage
from src.transformation.cleaning import latest_by_key

PARTITION_COLUMNS = ("year", "month", "day")


def read_processed(
    spark: SparkSession, storage: Storage, settings: Settings, dataset: str, before: date
) -> DataFrame | None:
    """All rows of ``processed/<dataset>/`` from partitions dated before ``before``.

    Returns ``None`` when nothing has been processed yet. Partition columns
    (year/month/day) come from the folder names and are dropped from the result.
    """
    prefix = f"{Zone.PROCESSED}/{dataset}/"
    if not any(key.endswith(".parquet") for key in storage.list(prefix)):
        return None
    df = spark.read.parquet(spark_uri(prefix, settings))
    partition_date = F.make_date(F.col("year"), F.col("month"), F.col("day"))
    return df.where(partition_date < F.lit(before)).drop(*PARTITION_COLUMNS)


def read_processed_keys(
    spark: SparkSession, storage: Storage, settings: Settings, dataset: str, before: date
) -> DataFrame | None:
    """Distinct business keys of ``dataset`` in processed partitions before ``before``."""
    df = read_processed(spark, storage, settings, dataset, before)
    if df is None:
        return None
    return df.select(BUSINESS_KEYS[dataset]).distinct()


def current_state(batch: DataFrame, earlier: DataFrame | None, key: str) -> DataFrame:
    """Newest row per key: this batch wins, otherwise the latest earlier partition."""
    batch = batch.withColumn("_priority", F.lit(1))
    if earlier is None:
        return batch.drop("_priority")
    earlier = earlier.select(*batch.columns[:-1]).withColumn("_priority", F.lit(0))
    combined = batch.unionByName(earlier)
    return latest_by_key(
        combined, key, F.col("_priority").desc(), F.col("source_ingestion_date").desc()
    ).drop("_priority")
