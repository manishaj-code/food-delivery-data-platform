"""Analytical datasets: order_analytics and daily_order_metrics (FR-037, spec 02 §5–6).

Metric conventions (spec 02 §5):
- Revenue = SUCCESS payments only, attributed to the order's date.
- Paid order = an order with at least one SUCCESS payment; AOV = revenue / paid orders.
- Late delivery = completed delivery taking more than 45 minutes.
- Rates are NULL when the denominator is 0.
``daily_order_metrics`` covers this batch's orders only; the warehouse recomputes the
full-history figures (post-load check WQ-005 reconciles them as a warning).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common.constants import LATE_DELIVERY_THRESHOLD_MINUTES
from src.transformation.cleaning import conform, latest_by_key, round_decimal
from src.transformation.schemas import DAILY_ORDER_METRICS, ORDER_ANALYTICS, PROCESSED_SCHEMAS

SUCCESS = "SUCCESS"
DELIVERED = "DELIVERED"
CANCELLED = "CANCELLED"


def _payments_per_order(payments: DataFrame) -> DataFrame:
    is_success = F.col("payment_status") == SUCCESS
    return payments.groupBy("order_id").agg(
        F.coalesce(F.sum(F.when(is_success, F.col("payment_amount"))), F.lit(0)).alias(
            "paid_amount"
        ),
        F.max(is_success.cast("int")).alias("_has_success"),
        F.max_by("payment_status", F.struct("payment_timestamp", "payment_id")).alias(
            "_latest_payment_status"
        ),
    )


def is_late(status: Column, duration: Column) -> Column:
    """TRUE/FALSE for completed deliveries, NULL otherwise."""
    completed = (status == DELIVERED) & duration.isNotNull()
    return F.when(completed, duration > F.lit(LATE_DELIVERY_THRESHOLD_MINUTES))


def build_order_analytics(
    orders: DataFrame,
    payments: DataFrame,
    delivery: DataFrame,
    customers: DataFrame,
    restaurants: DataFrame,
    run_id: str,
) -> DataFrame:
    """One row per order of the batch, enriched with customer, restaurant, payment, delivery.

    ``customers``/``restaurants`` must hold one current row per key (batch ∪ processed).
    """
    payment = _payments_per_order(payments)
    latest_delivery = latest_by_key(
        delivery,
        "order_id",
        F.col("delivery_time").desc_nulls_last(),
        F.col("delivery_id").desc(),
    ).select("order_id", "delivery_status", "delivery_duration_minutes")
    customer = customers.select("customer_id", F.col("city").alias("customer_city"))
    restaurant = restaurants.select(
        "restaurant_id", F.col("city").alias("restaurant_city"), "cuisine"
    )

    df = (
        orders.join(payment, "order_id", "left")
        .join(latest_delivery, "order_id", "left")
        .join(customer, "customer_id", "left")
        .join(restaurant, "restaurant_id", "left")
        .withColumns(
            {
                "order_hour": F.hour("order_timestamp"),
                "paid_amount": F.coalesce(F.col("paid_amount"), F.lit(0)),
                "payment_status": F.when(F.col("_has_success") == 1, F.lit(SUCCESS)).otherwise(
                    F.col("_latest_payment_status")
                ),
                "is_late": is_late(F.col("delivery_status"), F.col("delivery_duration_minutes")),
                "_run_id": F.lit(run_id),
            }
        )
    )
    return conform(df, PROCESSED_SCHEMAS[ORDER_ANALYTICS])


def _rate_pct(numerator: Column, denominator: Column) -> Column:
    return round_decimal(numerator * F.lit(100) / F.nullif(denominator, F.lit(0)), 5, 2)


def build_daily_order_metrics(order_analytics: DataFrame, run_id: str) -> DataFrame:
    """Per order date: total/paid/cancelled orders, revenue, AOV, cancellation rate (M-01–M-04)."""
    is_paid = F.col("payment_status") == SUCCESS
    df = (
        order_analytics.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(F.when(is_paid, F.col("order_id"))).alias("paid_orders"),
            F.coalesce(F.sum(F.when(is_paid, F.col("paid_amount"))), F.lit(0)).alias(
                "total_revenue"
            ),
            F.countDistinct(F.when(F.col("order_status") == CANCELLED, F.col("order_id"))).alias(
                "cancelled_orders"
            ),
            F.max("source_ingestion_date").alias("source_ingestion_date"),
        )
        .withColumns(
            {
                "total_revenue": round_decimal(F.col("total_revenue"), 14, 2),
                "average_order_value": round_decimal(
                    F.col("total_revenue") / F.nullif(F.col("paid_orders"), F.lit(0)), 10, 2
                ),
                "cancellation_rate_pct": _rate_pct(
                    F.col("cancelled_orders"), F.col("total_orders")
                ),
                "_run_id": F.lit(run_id),
            }
        )
    )
    return conform(df, PROCESSED_SCHEMAS[DAILY_ORDER_METRICS])
