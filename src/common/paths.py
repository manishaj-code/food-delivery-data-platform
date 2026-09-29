"""Lake path construction (docs/spec/07-data-pipeline-specification.md §4, FR-040, FR-041).

All lake keys are built here so local and S3 layouts stay identical. Keys are
relative and ``/``-separated; every zone is partitioned by run date.
"""

from __future__ import annotations

import re
from datetime import date
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.common.config import Settings


class Zone(StrEnum):
    RAW = "raw"
    VALIDATED = "validated"
    QUARANTINE = "quarantine"
    PROCESSED = "processed"
    REPORTS = "reports"


# Run-scoped staging area for partition replacement (expired by the S3 lifecycle rule).
TEMP_ROOT = "_tmp"
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_.=-]")


def date_partition(run_date: date) -> str:
    """``year=2026/month=09/day=29``."""
    return f"year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}"


def partition_path(zone: Zone | str, dataset: str, run_date: date) -> str:
    """Partition prefix, e.g. ``raw/orders/year=2026/month=09/day=29/``."""
    return f"{Zone(zone)}/{dataset}/{date_partition(run_date)}/"


def raw_file_path(dataset: str, run_date: date) -> str:
    """Raw file key, e.g. ``raw/orders/year=2026/month=09/day=29/orders.csv`` (FR-014)."""
    return f"{partition_path(Zone.RAW, dataset, run_date)}{dataset}.csv"


def report_path(dataset: str, run_date: date) -> str:
    """Data quality report, e.g. ``reports/data_quality/year=2026/month=09/day=29/orders.json``."""
    return f"{Zone.REPORTS}/data_quality/{date_partition(run_date)}/{dataset}.json"


def audit_path(stage: str, dataset: str, run_date: date) -> str:
    """Stage result, e.g. ``reports/audit/year=2026/month=09/day=29/ingestion__orders.json``."""
    return f"{Zone.REPORTS}/audit/{date_partition(run_date)}/{stage}__{dataset}.json"


def temp_path(zone: Zone | str, dataset: str, run_id: str) -> str:
    """Run-scoped staging prefix, e.g. ``_tmp/scheduled__2026-09-29T00_00_00_00_00/raw/orders/``.

    Characters that are awkward in S3 keys or Hadoop paths (``:``, ``+``) are replaced.
    """
    safe_run_id = _UNSAFE_CHARS.sub("_", run_id)
    return f"{TEMP_ROOT}/{safe_run_id}/{Zone(zone)}/{dataset}/"


def spark_uri(key: str, settings: Settings) -> str:
    """URI Spark uses for a lake key: ``file:///…`` locally, ``s3a://bucket/…`` on AWS."""
    if settings.storage_mode == "s3":
        return f"s3a://{settings.s3_bucket}/{key}"
    return f"file://{(settings.local_lake_path / key).resolve().as_posix()}"
