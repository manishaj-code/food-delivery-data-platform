"""Fact transforms: orders, payments, delivery (FR-031 – FR-036).

Timestamps arrive typed (UTC) from ``validated/``; dates are derived with the Spark
session time zone fixed to UTC, so a 23:59:59 order stays on its own day.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.constants import BUSINESS_KEYS, ENUM_COLUMNS, ORDERS, PAYMENTS
from src.common.constants import DELIVERY as DELIVERY_DATASET
from src.transformation.cleaning import (
    conform,
    deduplicate,
    minutes_between,
    normalise_upper,
    round_decimal,
    trim_and_nullify,
)
from src.transformation.schemas import PROCESSED_SCHEMAS


def _standardise(df: DataFrame, dataset: str, run_id: str) -> DataFrame:
    df = normalise_upper(trim_and_nullify(df), ENUM_COLUMNS)
    return (
        deduplicate(df, BUSINESS_KEYS[dataset])
        .withColumn("source_ingestion_date", F.col("_ingestion_date"))
        .withColumn("_run_id", F.lit(run_id))
    )


def transform_orders(validated: DataFrame, run_id: str) -> DataFrame:
    df = _standardise(validated, ORDERS, run_id).withColumns(
        {
            "order_timestamp": F.col("order_date"),
            "order_date": F.to_date(F.col("order_date")),
            "order_amount": round_decimal(F.col("order_amount"), 10, 2),
        }
    )
    return conform(df, PROCESSED_SCHEMAS[ORDERS])


def transform_payments(validated: DataFrame, run_id: str) -> DataFrame:
    df = _standardise(validated, PAYMENTS, run_id).withColumns(
        {
            "payment_timestamp": F.col("payment_date"),
            "payment_date": F.to_date(F.col("payment_date")),
            "payment_amount": round_decimal(F.col("payment_amount"), 10, 2),
        }
    )
    return conform(df, PROCESSED_SCHEMAS[PAYMENTS])


def delivery_duration_minutes(pickup: str = "pickup_time", delivery: str = "delivery_time"):
    """(delivery_time - pickup_time) in minutes, 2 decimals half-up; NULL if either is NULL."""
    return round_decimal(minutes_between(F.col(pickup), F.col(delivery)), 8, 2)


def transform_delivery(validated: DataFrame, order_dates: DataFrame, run_id: str) -> DataFrame:
    """``order_dates`` (order_id, order_date) comes from this batch's orders ∪ processed orders."""
    lookup = order_dates.select("order_id", "order_date").dropDuplicates(["order_id"])
    df = (
        _standardise(validated, DELIVERY_DATASET, run_id)
        .withColumn("delivery_duration_minutes", delivery_duration_minutes())
        .join(lookup, "order_id", "left")
    )
    return conform(df, PROCESSED_SCHEMAS[DELIVERY_DATASET])
