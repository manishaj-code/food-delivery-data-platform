"""Stage processed outputs for publishing (FR-040 – FR-044, spec 07 §3 stage 3, §8).

The transform task writes each dataset as Parquet (Snappy) to its run-scoped staging
prefix ``_tmp/<run_id>/processed/<dataset>/`` and then records the row count Spark saw
in ``_staged.json``. The marker is written last, so an interrupted Spark write is never
mistaken for a complete one. ``publish.publish_processed`` verifies and publishes.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from src.common.config import Settings
from src.common.logging_config import log_context
from src.common.paths import Zone, spark_uri, temp_path
from src.common.storage import Storage
from src.transformation.schemas import PROCESSED_DATASETS
from src.transformation.transform_job import TransformationResult

logger = logging.getLogger(__name__)

STAGED_MARKER = "_staged.json"


def staging_prefix(dataset: str, run_id: str) -> str:
    return temp_path(Zone.PROCESSED, dataset, run_id)


def stage_dataset(
    result: TransformationResult, dataset: str, storage: Storage, settings: Settings
) -> str:
    """Write one dataset to its staging prefix, then its marker; returns the prefix.

    Daily volumes are small (<= ~100k rows), so one file per dataset is written.
    """
    prefix = staging_prefix(dataset, result.run_id)
    storage.delete_prefix(prefix)  # leftovers from an earlier failed attempt
    df = result.datasets[dataset]
    df.coalesce(1).write.mode("overwrite").parquet(spark_uri(prefix, settings))
    marker = {
        "dataset": dataset,
        "run_id": result.run_id,
        "run_date": result.run_date.isoformat(),
        "row_count": result.counts[dataset],
        "staged_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    storage.write_text(prefix + STAGED_MARKER, json.dumps(marker, indent=2))
    logger.info("Staged %s: %d records", dataset, result.counts[dataset])
    return prefix


def write_processed(
    result: TransformationResult, storage: Storage, settings: Settings
) -> dict[str, str]:
    """Stage every processed dataset; returns dataset -> staging prefix."""
    staged = {}
    run_date = result.run_date.isoformat()
    for dataset in PROCESSED_DATASETS:
        with log_context(dataset=dataset, run_id=result.run_id, run_date=run_date):
            staged[dataset] = stage_dataset(result, dataset, storage, settings)
    return staged
