"""Join validation (FR-035): every child record must match a parent key.

Validation already quarantines orphans, so a failure here means a bug or inconsistent
lake state; the transformation stops instead of loading orphan facts.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.exceptions import TransformationError

SAMPLE_SIZE = 5


def assert_no_orphans(
    child: DataFrame,
    parent_keys: DataFrame,
    child_column: str,
    parent_column: str,
    dataset: str,
    parent: str,
) -> None:
    """Raise ``TransformationError`` with count and sample keys if any child has no parent."""
    parent_side = parent_keys.select(F.col(parent_column).alias("_parent_key")).distinct()
    orphans = child.join(
        parent_side, child[child_column] == parent_side["_parent_key"], "left_anti"
    ).select(child_column)
    sample = [row[0] for row in orphans.limit(SAMPLE_SIZE).collect()]
    if not sample:
        return
    raise TransformationError(
        f"{dataset} has records without a matching {parent}",
        dataset=dataset,
        parent=parent,
        column=child_column,
        orphan_count=orphans.count(),
        sample_keys=sample,
    )
