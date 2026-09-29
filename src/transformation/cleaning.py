"""Reusable DataFrame -> DataFrame cleaning helpers (FR-031 – FR-034).

Validation already trims, nullifies, and upper-cases values; these helpers are applied
again so transforms are correct on their own (idempotent on clean data).
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType


def trim_and_nullify(df: DataFrame, columns: Iterable[str] | None = None) -> DataFrame:
    """Trim string columns and turn empty strings into NULL (all string columns by default)."""
    targets = (
        set(columns)
        if columns is not None
        else {field.name for field in df.schema.fields if isinstance(field.dataType, StringType)}
    )
    return df.select(
        *(
            F.when(F.trim(F.col(name)) == "", None).otherwise(F.trim(F.col(name))).alias(name)
            if name in targets
            else F.col(name)
            for name in df.columns
        )
    )


def normalise_upper(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Upper-case (and trim) status/method columns: ``" delivered "`` -> ``DELIVERED``."""
    targets = [name for name in columns if name in df.columns]
    return df.withColumns({name: F.upper(F.trim(F.col(name))) for name in targets})


def deduplicate(df: DataFrame, key: str, order_by: str = "_source_row_number") -> DataFrame:
    """Keep one row per ``key``: the first by ``order_by`` (FR-031, consistent with DQ rules)."""
    window = Window.partitionBy(key).orderBy(F.col(order_by).asc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def latest_by_key(df: DataFrame, key: str, *order_by: Column) -> DataFrame:
    """Keep the newest row per ``key`` according to ``order_by`` (descending priority)."""
    window = Window.partitionBy(key).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def round_decimal(column: Column, precision: int, scale: int) -> Column:
    """Round half-up to ``scale`` decimals and cast to ``decimal(precision, scale)``."""
    return F.round(column.cast(f"decimal(38,{max(scale, 6)})"), scale).cast(
        f"decimal({precision},{scale})"
    )


def minutes_between(start: Column, end: Column) -> Column:
    """Exact minutes between two timestamps, as a decimal (NULL if either is NULL)."""
    seconds = F.unix_timestamp(end) - F.unix_timestamp(start)
    return seconds.cast("decimal(20,0)") / F.lit(60)


def conform(df: DataFrame, schema: StructType) -> DataFrame:
    """Select exactly the schema's columns, in order, cast to the schema's types."""
    return df.select(
        *(F.col(field.name).cast(field.dataType).alias(field.name) for field in schema)
    )
