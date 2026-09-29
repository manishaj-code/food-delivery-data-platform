"""Processed-layer schemas (docs/spec/07-data-pipeline-specification.md §6, FR-033).

The final ``select`` of every transform enforces these column names, order, and types.
Types are Redshift COPY compatible (string, date, timestamp without time zone, decimal).
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.constants import (
    CUSTOMERS,
    DELIVERY,
    DELIVERY_PARTNERS,
    ORDERS,
    PAYMENTS,
    RESTAURANTS,
)

ORDER_ANALYTICS = "order_analytics"
DAILY_ORDER_METRICS = "daily_order_metrics"
PROCESSED_DATASETS: tuple[str, ...] = (
    CUSTOMERS,
    RESTAURANTS,
    DELIVERY_PARTNERS,
    ORDERS,
    PAYMENTS,
    DELIVERY,
    ORDER_ANALYTICS,
    DAILY_ORDER_METRICS,
)

AMOUNT = DecimalType(10, 2)
DURATION = DecimalType(8, 2)

_LINEAGE = (("source_ingestion_date", DateType()), ("_run_id", StringType()))


def _schema(*columns: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in (*columns, *_LINEAGE)])


def columns_and_types(schema: StructType) -> list[tuple[str, DataType]]:
    """Schema identity used for checks: names, order, and types (Parquet ignores nullability)."""
    return [(field.name, field.dataType) for field in schema.fields]


S = StringType()
TS = TimestampType()
D = DateType()

PROCESSED_SCHEMAS: dict[str, StructType] = {
    CUSTOMERS: _schema(
        ("customer_id", S), ("customer_name", S), ("email", S), ("city", S), ("signup_date", D)
    ),
    RESTAURANTS: _schema(
        ("restaurant_id", S),
        ("restaurant_name", S),
        ("city", S),
        ("cuisine", S),
        ("rating", DecimalType(2, 1)),
    ),
    DELIVERY_PARTNERS: _schema(
        ("delivery_partner_id", S), ("partner_name", S), ("city", S), ("joining_date", D)
    ),
    ORDERS: _schema(
        ("order_id", S),
        ("customer_id", S),
        ("restaurant_id", S),
        ("order_timestamp", TS),
        ("order_date", D),
        ("order_amount", AMOUNT),
        ("order_status", S),
    ),
    PAYMENTS: _schema(
        ("payment_id", S),
        ("order_id", S),
        ("payment_method", S),
        ("payment_amount", AMOUNT),
        ("payment_status", S),
        ("payment_timestamp", TS),
        ("payment_date", D),
    ),
    DELIVERY: _schema(
        ("delivery_id", S),
        ("order_id", S),
        ("delivery_partner_id", S),
        ("pickup_time", TS),
        ("delivery_time", TS),
        ("delivery_duration_minutes", DURATION),
        ("delivery_status", S),
        ("order_date", D),
    ),
    ORDER_ANALYTICS: _schema(
        ("order_id", S),
        ("order_date", D),
        ("order_hour", IntegerType()),
        ("customer_id", S),
        ("customer_city", S),
        ("restaurant_id", S),
        ("restaurant_city", S),
        ("cuisine", S),
        ("order_amount", AMOUNT),
        ("order_status", S),
        ("payment_status", S),
        ("paid_amount", AMOUNT),
        ("delivery_status", S),
        ("delivery_duration_minutes", DURATION),
        ("is_late", BooleanType()),
    ),
    DAILY_ORDER_METRICS: _schema(
        ("order_date", D),
        ("total_orders", LongType()),
        ("paid_orders", LongType()),
        ("total_revenue", DecimalType(14, 2)),
        ("average_order_value", AMOUNT),
        ("cancelled_orders", LongType()),
        ("cancellation_rate_pct", DecimalType(5, 2)),
    ),
}
