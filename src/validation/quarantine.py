"""Write validated and quarantined records to the lake (FR-023, FR-091).

Both outputs replace their run-date partition through ``replace_partition``: Spark
writes to a run-scoped staging prefix, which is then published.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame

from src.common.config import Settings
from src.common.paths import Zone, spark_uri
from src.common.storage import Storage, replace_partition


def write_zone(
    df: DataFrame,
    storage: Storage,
    settings: Settings,
    zone: Zone,
    dataset: str,
    run_date: date,
    run_id: str,
) -> list[str]:
    """Replace ``<zone>/<dataset>/year=/month=/day=/`` with ``df`` as Parquet (Snappy).

    Daily volumes are small (<= ~100k rows), so one file per partition is written.
    """

    def write(staging: str) -> None:
        df.coalesce(1).write.mode("overwrite").parquet(spark_uri(staging, settings))

    return replace_partition(storage, zone, dataset, run_date, run_id, write)


def write_validated(
    df: DataFrame, storage: Storage, settings: Settings, dataset: str, run_date: date, run_id: str
) -> list[str]:
    return write_zone(df, storage, settings, Zone.VALIDATED, dataset, run_date, run_id)


def write_quarantine(
    df: DataFrame, storage: Storage, settings: Settings, dataset: str, run_date: date, run_id: str
) -> list[str]:
    return write_zone(df, storage, settings, Zone.QUARANTINE, dataset, run_date, run_id)
