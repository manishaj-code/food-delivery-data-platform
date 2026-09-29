"""Lake path construction (docs/spec/07-data-pipeline-specification.md §4).

All lake keys are built here so local and S3 layouts stay identical. Keys are
relative, ``/``-separated, and partitioned by run date. Phase 3 adds the other
zones, report/audit paths, and Spark URIs.
"""

from __future__ import annotations

from datetime import date

RAW_ZONE = "raw"


def partition_path(zone: str, dataset: str, run_date: date) -> str:
    """Partition prefix, e.g. ``raw/orders/year=2026/month=09/day=29/``."""
    return f"{zone}/{dataset}/year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}/"


def raw_file_path(dataset: str, run_date: date) -> str:
    """Raw file key, e.g. ``raw/orders/year=2026/month=09/day=29/orders.csv`` (FR-014)."""
    return f"{partition_path(RAW_ZONE, dataset, run_date)}{dataset}.csv"
