"""PySpark validator: tag every raw record with the rules it fails (FR-021 – FR-023).

Flow for one dataset: read raw CSV as strings -> normalise (trim, empty -> NULL,
upper-case enums) -> evaluate all rules in one pass -> split into valid records
(typed) and invalid records (original strings + failed rule IDs).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.common.constants import (
    ENUM_COLUMNS,
    INGESTION_METADATA_COLUMNS,
    SOURCE_COLUMN_TYPES,
    SOURCE_COLUMNS,
)
from src.validation.rule_catalog import rules_for
from src.validation.rules import ERROR, Rule, RuleContext, failed, parse_value, prepare

logger = logging.getLogger(__name__)

FAILED_RULES_COLUMN = "_dq_failed_rules"
VALIDATED_AT_COLUMN = "_dq_validated_at"
_ERROR_RULES = "_dq_error_rules"
_ALL_RULES = "_dq_all_rules"

METADATA_TYPES = {
    "_ingestion_timestamp": "timestamp",
    "_ingestion_date": "date",
    "_source_row_number": "int",
}


@dataclass
class ValidationOutcome:
    """Result of validating one dataset. ``valid``/``invalid`` share one cached DataFrame."""

    dataset: str
    valid: DataFrame
    invalid: DataFrame
    total_records: int
    valid_records: int
    rule_failures: dict[str, int]
    rules: tuple[Rule, ...]
    validated_at: datetime
    _cached: DataFrame | None = field(default=None, repr=False)

    @property
    def invalid_records(self) -> int:
        return self.total_records - self.valid_records

    def release(self) -> None:
        if self._cached is not None:
            self._cached.unpersist()


def raw_schema(dataset: str) -> StructType:
    columns = (*SOURCE_COLUMNS[dataset], *INGESTION_METADATA_COLUMNS)
    return StructType([StructField(name, StringType(), True) for name in columns])


def read_raw(spark: SparkSession, uri: str, dataset: str) -> DataFrame:
    """Read a raw CSV written by ingestion; every value stays a string."""
    return (
        spark.read.option("header", True)
        .option("escape", '"')  # Python's csv module escapes quotes by doubling them
        .option("multiLine", True)  # quoted values may contain newlines
        .option("enforceSchema", False)  # fail if the header differs from the schema
        .option("mode", "FAILFAST")
        .schema(raw_schema(dataset))
        .csv(uri)
    )


def normalise(df: DataFrame, dataset: str) -> DataFrame:
    """Trim source values, turn empty strings into NULL, upper-case enumerated columns."""

    def clean(name: str) -> Column:
        trimmed = F.trim(F.col(name))
        value = F.when(trimmed == "", None).otherwise(trimmed)
        return (F.upper(value) if name in ENUM_COLUMNS else value).alias(name)

    source = SOURCE_COLUMNS[dataset]
    return df.select(*(clean(name) for name in source), *INGESTION_METADATA_COLUMNS)


def to_typed(df: DataFrame, dataset: str) -> DataFrame:
    """Cast a validated record set to its logical types (safe after validation)."""
    types = SOURCE_COLUMN_TYPES[dataset] | METADATA_TYPES
    columns = (*SOURCE_COLUMNS[dataset], *INGESTION_METADATA_COLUMNS)

    def typed(name: str) -> Column:
        logical_type = types.get(name, "string")
        if logical_type == "int":
            return F.col(name).try_cast("int").alias(name)
        return parse_value(F.col(name), logical_type).alias(name)

    return df.select(*(typed(name) for name in columns))


def _rule_ids(rules: tuple[Rule, ...], flags: dict[str, str]) -> Column:
    return F.array_compact(
        F.array(*(F.when(F.col(flags[rule.rule_id]), F.lit(rule.rule_id)) for rule in rules))
    )


def validate(
    raw: DataFrame, dataset: str, context: RuleContext, validated_at: datetime
) -> ValidationOutcome:
    """Evaluate every rule of ``dataset`` on ``raw`` and split valid from invalid records."""
    logger.info("Starting validation for %s", dataset)
    rules = rules_for(dataset)
    columns = (*SOURCE_COLUMNS[dataset], *INGESTION_METADATA_COLUMNS)

    df = normalise(raw, dataset)
    for rule in rules:
        df = prepare(rule, df, context)
    flags = {rule.rule_id: f"_f{index}" for index, rule in enumerate(rules)}
    df = df.select(*columns, *(failed(rule, context).alias(flags[rule.rule_id]) for rule in rules))
    error_rules = tuple(rule for rule in rules if rule.severity == ERROR)
    tagged = (
        df.withColumn(_ERROR_RULES, _rule_ids(error_rules, flags))
        .withColumn(_ALL_RULES, _rule_ids(rules, flags))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    counts = tagged.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.size(_ERROR_RULES) == 0, 1).otherwise(0)).alias("valid"),
        *(F.sum(F.col(flags[rule.rule_id]).cast("int")).alias(rule.rule_id) for rule in rules),
    ).first()

    is_valid = F.size(_ERROR_RULES) == 0
    valid = to_typed(tagged.where(is_valid), dataset)
    invalid = tagged.where(~is_valid).select(
        *columns,
        F.concat_ws(";", _ALL_RULES).alias(FAILED_RULES_COLUMN),
        F.lit(validated_at).cast("timestamp").alias(VALIDATED_AT_COLUMN),
    )
    return ValidationOutcome(
        dataset=dataset,
        valid=valid,
        invalid=invalid,
        total_records=counts["total"] or 0,
        valid_records=counts["valid"] or 0,
        rule_failures={rule.rule_id: counts[rule.rule_id] or 0 for rule in rules},
        rules=rules,
        validated_at=validated_at,
        _cached=tagged,
    )
