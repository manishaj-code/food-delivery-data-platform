"""Rule model and check builders for the PySpark data-quality engine (spec 06).

Each check turns a ``Rule`` into a boolean Spark Column that is TRUE when a record
FAILS the rule. Set-level checks (``unique``, ``exists_in``) first add a helper
column to the DataFrame (window / join) and then compare against it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.common.constants import BUSINESS_KEYS, SEVERITY_ERROR, SEVERITY_WARN, SOURCE_COLUMN_TYPES

ERROR = SEVERITY_ERROR
WARN = SEVERITY_WARN

NOT_NULL = "not_null"
UNIQUE = "unique"
IN_SET = "in_set"
NUMERIC_RANGE = "numeric_range"
VALID_DATE = "valid_date"
VALID_TIMESTAMP = "valid_timestamp"
COLUMN_LTE = "column_lte"
EXISTS_IN = "exists_in"
REQUIRED_WHEN = "required_when"
CHECK_TYPES = (
    NOT_NULL,
    UNIQUE,
    IN_SET,
    NUMERIC_RANGE,
    VALID_DATE,
    VALID_TIMESTAMP,
    COLUMN_LTE,
    EXISTS_IN,
    REQUIRED_WHEN,
)

SPARK_DATE_FORMAT = "yyyy-MM-dd"
SPARK_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


@dataclass(frozen=True)
class Rule:
    rule_id: str
    dataset: str
    columns: tuple[str, ...]
    check: str
    severity: str = ERROR
    params: Mapping[str, Any] = field(default_factory=dict, hash=False)
    description: str = ""

    @property
    def column(self) -> str:
        return self.columns[0]


@dataclass(frozen=True)
class RuleContext:
    """Inputs needed by some checks: the run date and parent key sets."""

    run_date: date
    parent_keys: Mapping[str, DataFrame] = field(default_factory=dict, hash=False)


def parse_value(column: Column, logical_type: str) -> Column:
    """Safe parse of a string column: NULL when the value is not valid for the type.

    ``try_*`` functions are used because Spark 4 runs in ANSI mode, where a plain cast
    of a bad value raises instead of returning NULL.
    """
    if logical_type == "date":
        return F.to_date(F.try_to_timestamp(column, F.lit(SPARK_DATE_FORMAT)))
    if logical_type == "timestamp":
        return F.try_to_timestamp(column, F.lit(SPARK_TIMESTAMP_FORMAT))
    if logical_type.startswith("decimal"):
        return column.try_cast(logical_type)
    return column


def _type_of(rule: Rule, column: str) -> str:
    return SOURCE_COLUMN_TYPES.get(rule.dataset, {}).get(column, "string")


def _invalid(rule: Rule, column: str) -> Column:
    """Non-null value that does not parse as the column's type."""
    return F.col(column).isNotNull() & parse_value(F.col(column), _type_of(rule, column)).isNull()


def _missing_or_invalid(rule: Rule, column: str) -> Column:
    if rule.params.get("allow_null", False):
        return _invalid(rule, column)
    return F.col(column).isNull() | _invalid(rule, column)


def _after_run_date(rule: Rule, column: str, context: RuleContext) -> Column:
    """Value later than the end of the run date (i.e. in the future for this batch)."""
    if not rule.params.get("not_after_run_date", False):
        return F.lit(False)
    parsed = parse_value(F.col(column), _type_of(rule, column))
    if _type_of(rule, column) == "date":
        return parsed > F.lit(context.run_date)
    next_day = context.run_date + timedelta(days=1)
    return parsed >= F.to_timestamp(F.lit(next_day.isoformat()))


def helper_column(rule: Rule) -> str:
    return f"_h_{rule.rule_id.replace('-', '_').lower()}"


def prepare(rule: Rule, df: DataFrame, context: RuleContext) -> DataFrame:
    """Add the helper column a set-level check needs; other checks return ``df`` unchanged."""
    if rule.check == UNIQUE:
        window = Window.partitionBy(rule.column).orderBy(
            F.col("_source_row_number").cast("int"), F.col("_source_row_number")
        )
        return df.withColumn(helper_column(rule), F.row_number().over(window))
    if rule.check == EXISTS_IN:
        parent = rule.params["parent"]
        keys = context.parent_keys.get(parent)
        marker = helper_column(rule)
        if keys is None:
            return df.withColumn(marker, F.lit(None).cast("boolean"))
        parent_keys = (
            keys.select(F.col(BUSINESS_KEYS[parent]).alias(f"{marker}_key"))
            .where(F.col(f"{marker}_key").isNotNull())
            .distinct()
            .withColumn(marker, F.lit(True))
        )
        joined = df.join(parent_keys, df[rule.column] == parent_keys[f"{marker}_key"], "left")
        return joined.drop(f"{marker}_key")
    return df


def failed(rule: Rule, context: RuleContext) -> Column:
    """Boolean Column: TRUE when the record fails ``rule`` (never NULL)."""
    column = F.col(rule.column)
    check = rule.check
    if check == NOT_NULL:
        condition = column.isNull()
    elif check == UNIQUE:
        condition = column.isNotNull() & (F.col(helper_column(rule)) > 1)
    elif check == IN_SET:
        condition = column.isNull() | ~column.isin(*rule.params["values"])
    elif check == NUMERIC_RANGE:
        value = parse_value(column, _type_of(rule, rule.column))
        condition = _missing_or_invalid(rule, rule.column)
        if "min" in rule.params:
            condition = condition | (value < F.lit(rule.params["min"]))
        if "max" in rule.params:
            condition = condition | (value > F.lit(rule.params["max"]))
    elif check in (VALID_DATE, VALID_TIMESTAMP):
        condition = F.lit(False)
        for name in rule.columns:
            condition = (
                condition | _missing_or_invalid(rule, name) | _after_run_date(rule, name, context)
            )
    elif check == COLUMN_LTE:
        left, right = (parse_value(F.col(c), _type_of(rule, c)) for c in rule.columns)
        condition = left.isNotNull() & right.isNotNull() & (left > right)
    elif check == EXISTS_IN:
        condition = column.isNull() | F.col(helper_column(rule)).isNull()
    elif check == REQUIRED_WHEN:
        when_column, when_value = rule.params["when_column"], rule.params["when_value"]
        condition = (F.col(when_column) == F.lit(when_value)) & column.isNull()
    else:
        raise ValueError(f"Unknown check type {check!r} in {rule.rule_id}")
    return F.coalesce(condition, F.lit(False))
