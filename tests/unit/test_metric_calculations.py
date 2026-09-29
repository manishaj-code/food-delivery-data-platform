"""Metric calculations on hand-computed fixtures (FR-036, FR-037, AC-011)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from src.transformation.analytics import build_daily_order_metrics, build_order_analytics
from src.transformation.facts import delivery_duration_minutes
from src.transformation.schemas import PROCESSED_SCHEMAS, columns_and_types

pytestmark = pytest.mark.spark

RUN_ID = "test__metrics"
DAY = date(2026, 9, 29)
INGESTED = date(2026, 9, 29)


def _ts(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


@pytest.mark.parametrize(
    ("pickup", "delivered", "minutes"),
    [
        ("2026-09-29 12:00:00", "2026-09-29 12:31:30", Decimal("31.50")),
        ("2026-09-29 12:00:00", "2026-09-29 12:45:00", Decimal("45.00")),
        ("2026-09-29 12:00:00", "2026-09-29 12:45:01", Decimal("45.02")),  # 45.0166.. half-up
        ("2026-09-29 23:50:00", "2026-09-30 00:20:00", Decimal("30.00")),  # across midnight
        ("2026-09-29 12:00:00", None, None),
        (None, None, None),
    ],
)
def test_delivery_duration_minutes(spark, pickup, delivered, minutes) -> None:
    df = spark.createDataFrame(
        [(_ts(pickup) if pickup else None, _ts(delivered) if delivered else None)],
        "pickup_time timestamp, delivery_time timestamp",
    )

    assert df.select(delivery_duration_minutes().alias("m")).first().m == minutes


def _analytics_inputs(spark):
    orders = spark.createDataFrame(
        [
            # order_id, customer, restaurant, timestamp, amount, status
            ("O1", "C1", "R1", _ts("2026-09-29 12:10:00"), Decimal("100.00"), "DELIVERED"),
            ("O2", "C2", "R1", _ts("2026-09-29 13:00:00"), Decimal("200.00"), "DELIVERED"),
            ("O3", "C1", "R2", _ts("2026-09-29 20:00:00"), Decimal("50.00"), "CANCELLED"),
            ("O4", "C2", "R2", _ts("2026-09-29 21:00:00"), Decimal("75.55"), "DELIVERED"),
        ],
        "order_id string, customer_id string, restaurant_id string, order_timestamp timestamp,"
        " order_amount decimal(10,2), order_status string",
    ).selectExpr(
        "*", "to_date(order_timestamp) AS order_date", f"DATE'{INGESTED}' AS source_ingestion_date"
    )
    payments = spark.createDataFrame(
        [
            ("P1", "O1", Decimal("100.00"), "SUCCESS", _ts("2026-09-29 12:11:00")),
            ("P2a", "O2", Decimal("200.00"), "FAILED", _ts("2026-09-29 13:01:00")),
            ("P2b", "O2", Decimal("200.00"), "SUCCESS", _ts("2026-09-29 13:02:00")),
            ("P3", "O3", Decimal("50.00"), "REFUNDED", _ts("2026-09-29 20:01:00")),
            ("P4", "O4", Decimal("75.55"), "SUCCESS", _ts("2026-09-29 21:01:00")),
        ],
        "payment_id string, order_id string, payment_amount decimal(10,2),"
        " payment_status string, payment_timestamp timestamp",
    )
    delivery = spark.createDataFrame(
        [
            ("D1", "O1", "DELIVERED", _ts("2026-09-29 12:55:00"), Decimal("45.00")),
            ("D2", "O2", "DELIVERED", _ts("2026-09-29 14:00:00"), Decimal("45.01")),
            ("D3", "O3", "FAILED", None, None),
            ("D4", "O4", "DELIVERED", _ts("2026-09-29 21:40:00"), Decimal("20.00")),
        ],
        "delivery_id string, order_id string, delivery_status string, delivery_time timestamp,"
        " delivery_duration_minutes decimal(8,2)",
    )
    customers = spark.createDataFrame(
        [("C1", "Pune"), ("C2", "Mumbai")], "customer_id string, city string"
    )
    restaurants = spark.createDataFrame(
        [("R1", "Pune", "Indian"), ("R2", "Delhi", "Chinese")],
        "restaurant_id string, city string, cuisine string",
    )
    return orders, payments, delivery, customers, restaurants


def test_order_analytics(spark) -> None:
    analytics = build_order_analytics(*_analytics_inputs(spark), run_id=RUN_ID)

    assert columns_and_types(analytics.schema) == columns_and_types(
        PROCESSED_SCHEMAS["order_analytics"]
    )
    rows = {row.order_id: row for row in analytics.collect()}
    assert rows["O1"].is_late is False  # exactly 45.00 minutes is not late
    assert rows["O2"].is_late is True  # 45.01 minutes
    assert rows["O3"].is_late is None  # not a completed delivery
    assert rows["O2"].payment_status == "SUCCESS"  # retry succeeded after a failure
    assert rows["O2"].paid_amount == Decimal("200.00")
    assert rows["O3"].payment_status == "REFUNDED"
    assert rows["O3"].paid_amount == Decimal("0.00")
    assert (rows["O1"].customer_city, rows["O1"].restaurant_city, rows["O1"].cuisine) == (
        "Pune",
        "Pune",
        "Indian",
    )
    assert rows["O4"].restaurant_city == "Delhi"  # revenue city = restaurant city
    assert rows["O1"].order_hour == 12


def test_daily_order_metrics_hand_calculated(spark) -> None:
    """4 orders; 3 paid (100 + 200 + 75.55 = 375.55); AOV 125.18; 1 of 4 cancelled = 25%."""
    analytics = build_order_analytics(*_analytics_inputs(spark), run_id=RUN_ID)

    metrics = build_daily_order_metrics(analytics, RUN_ID)

    assert columns_and_types(metrics.schema) == columns_and_types(
        PROCESSED_SCHEMAS["daily_order_metrics"]
    )
    row = metrics.first()
    assert row.order_date == DAY
    assert (row.total_orders, row.paid_orders, row.cancelled_orders) == (4, 3, 1)
    assert row.total_revenue == Decimal("375.55")
    assert row.average_order_value == Decimal("125.18")  # 125.1833.. rounded half-up
    assert row.cancellation_rate_pct == Decimal("25.00")
    assert row._run_id == RUN_ID


def test_aov_is_null_when_no_order_is_paid(spark) -> None:
    orders, payments, delivery, customers, restaurants = _analytics_inputs(spark)
    only_cancelled = orders.where("order_id = 'O3'")

    analytics = build_order_analytics(
        only_cancelled, payments, delivery, customers, restaurants, RUN_ID
    )
    row = build_daily_order_metrics(analytics, RUN_ID).first()

    assert (row.paid_orders, row.total_revenue, row.average_order_value) == (
        0,
        Decimal("0.00"),
        None,
    )
    assert row.cancellation_rate_pct == Decimal("100.00")
