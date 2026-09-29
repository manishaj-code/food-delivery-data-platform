"""Power BI views and the 15 analytics queries (FR-060 – FR-062, AC-080 – AC-083).

Metric tests run on ``fixtures/analytics_fixture.sql``: 9 orders over two days, whose
expected results are worked out by hand in the comments below (spec 02 §5–6).
"""

from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.warehouse.analytics import QUERIES, VIEWS, QueryResult, run_query
from src.warehouse.loader import load_warehouse
from tests.integration.conftest import Warehouse, fresh_warehouse
from tests.sample_lake import DAILY_RUN, HISTORICAL_RUN, ValidatedLake

pytestmark = pytest.mark.integration

FIXTURE_SQL = Path(__file__).parent / "fixtures" / "analytics_fixture.sql"
D1, D2 = date(2026, 9, 1), date(2026, 9, 2)

# Spec 08 §8, in column order.
VIEW_COLUMNS = {
    "vw_daily_orders": [
        "order_date",
        "total_orders",
        "delivered_orders",
        "cancelled_orders",
        "cancellation_rate_pct",
    ],
    "vw_daily_revenue": ["order_date", "paid_orders", "total_revenue", "average_order_value"],
    "vw_restaurant_performance": [
        "restaurant_id",
        "restaurant_name",
        "city",
        "cuisine",
        "rating",
        "total_orders",
        "total_revenue",
        "average_order_value",
        "cancellation_rate_pct",
        "avg_delivery_minutes",
    ],
    "vw_delivery_performance": [
        "order_date",
        "city",
        "total_deliveries",
        "completed_deliveries",
        "failed_deliveries",
        "avg_delivery_minutes",
        "late_deliveries",
        "late_delivery_rate_pct",
    ],
    "vw_customer_summary": [
        "customer_id",
        "customer_name",
        "city",
        "signup_date",
        "total_orders",
        "total_spend",
        "first_order_date",
        "last_order_date",
        "is_repeat_customer",
    ],
    "vw_payment_summary": [
        "payment_date",
        "payment_method",
        "payment_status",
        "payment_count",
        "total_amount",
    ],
}


def dec(value: str) -> Decimal:
    return Decimal(value)


@pytest.fixture(scope="module")
def fixture_wh(warehouse: Warehouse) -> Warehouse:
    lines = FIXTURE_SQL.read_text(encoding="utf-8").splitlines()
    sql = "\n".join(line for line in lines if not line.lstrip().startswith("--"))
    warehouse.execute(sql)
    warehouse.conn.commit()
    return warehouse


def query(warehouse: Warehouse, name: str) -> QueryResult:
    return run_query(warehouse.conn, warehouse.schema, name)


def view(warehouse: Warehouse, name: str, order_by: str) -> list[tuple]:
    return warehouse.query(f"SELECT * FROM {{schema}}.{name} ORDER BY {order_by}")


# --- Views: structure (AC-081) ---------------------------------------------------------


def test_the_six_views_exist_with_spec_columns(fixture_wh: Warehouse) -> None:
    assert set(VIEWS) == set(VIEW_COLUMNS)
    for name, expected in VIEW_COLUMNS.items():
        columns = fixture_wh.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (fixture_wh.schema, name),
        )
        assert [column for (column,) in columns] == expected, name


# --- Views: hand-computed metrics (AC-082) ---------------------------------------------


def test_vw_daily_orders(fixture_wh: Warehouse) -> None:
    """09-01: O1–O4, O3 cancelled (1/4). 09-02: O5–O9, O7 cancelled (1/5)."""
    assert view(fixture_wh, "vw_daily_orders", "order_date") == [
        (D1, 4, 3, 1, dec("25.00")),
        (D2, 5, 3, 1, dec("20.00")),
    ]


def test_vw_daily_revenue_uses_success_payments_on_the_order_date(fixture_wh: Warehouse) -> None:
    """09-01: O1 500 + O2 300 (FAILED attempt ignored) + O4 400; O3 REFUNDED -> 1200 / 3.
    09-02: O5 250 + O6 650 + O8 350 (paid on 09-03) + O9 100; O7 FAILED -> 1350 / 4."""
    assert view(fixture_wh, "vw_daily_revenue", "order_date") == [
        (D1, 3, dec("1200.00"), dec("400.00")),
        (D2, 4, dec("1350.00"), dec("337.50")),
    ]


def test_vw_restaurant_performance(fixture_wh: Warehouse) -> None:
    """R1: O1 O3 O5 O8, paid 500+250+350; D1 30, D5 40 (D3 failed, D8 open).
    R2: O2 O4 O7, paid 300+400; D2 50, D4 45. R3: O6 O9 paid 650+100; D6 55, D9 25.
    R4 has no orders: zero counts, NULL rates."""
    rows = view(fixture_wh, "vw_restaurant_performance", "restaurant_id")
    assert [row[5:] for row in rows] == [
        (4, dec("1100.00"), dec("366.67"), dec("25.00"), dec("35.00")),
        (3, dec("700.00"), dec("350.00"), dec("33.33"), dec("47.50")),
        (2, dec("750.00"), dec("375.00"), dec("0.00"), dec("40.00")),
        (0, 0, None, None, None),
    ]
    assert rows[0][:5] == ("R1", "Spice Route", "Pune", "Indian", dec("4.5"))


def test_vw_delivery_performance(fixture_wh: Warehouse) -> None:
    """Completed = DELIVERED with both times. D4 takes exactly 45 min: not late (> 45)."""
    assert view(fixture_wh, "vw_delivery_performance", "order_date, city") == [
        (D1, "Mumbai", 2, 2, 0, dec("47.50"), 1, dec("50.00")),  # D2 50 (late), D4 45
        (D1, "Pune", 2, 1, 1, dec("30.00"), 0, dec("0.00")),  # D1 30, D3 failed
        (D2, "Pune", 4, 3, 0, dec("40.00"), 1, dec("33.33")),  # D5 40, D6 55 (late), D9 25, D8
    ]


def test_vw_customer_summary(fixture_wh: Warehouse) -> None:
    """C1: O1 O2 O6; C2: O3 (refunded) O4 O8; C3: O5 O7 (failed); C4: O9; C5: none."""
    rows = view(fixture_wh, "vw_customer_summary", "customer_id")
    assert [(row[0], *row[4:]) for row in rows] == [
        ("C1", 3, dec("1450.00"), D1, D2, True),
        ("C2", 3, dec("750.00"), D1, D2, True),
        ("C3", 2, dec("250.00"), D2, D2, True),
        ("C4", 1, dec("100.00"), D2, D2, False),
        ("C5", 0, 0, None, None, False),
    ]


def test_vw_payment_summary_groups_by_payment_date(fixture_wh: Warehouse) -> None:
    rows = view(fixture_wh, "vw_payment_summary", "payment_date, payment_method, payment_status")
    assert sum(row[3] for row in rows) == 10
    assert (date(2026, 9, 3), "UPI", "SUCCESS", 1, dec("350.00")) in rows
    assert (D1, "CARD", "FAILED", 1, dec("300.00")) in rows


# --- Queries on the fixture (AC-082) ---------------------------------------------------


def test_q03_average_order_value_overall_and_daily(fixture_wh: Warehouse) -> None:
    """Overall: 2550 / 7 paid orders = 364.2857 -> 364.29."""
    assert query(fixture_wh, "03_average_order_value").rows == [
        ("overall", None, 7, dec("2550.00"), dec("364.29")),
        ("daily", D1, 3, dec("1200.00"), dec("400.00")),
        ("daily", D2, 4, dec("1350.00"), dec("337.50")),
    ]


def test_q04_cancellation_rate(fixture_wh: Warehouse) -> None:
    """Overall: O3 and O7 of 9 orders = 22.22 %."""
    assert query(fixture_wh, "04_cancellation_rate").rows[0] == (
        "overall",
        None,
        9,
        2,
        dec("22.22"),
    )


def test_q05_payment_success_rate_per_method(fixture_wh: Warehouse) -> None:
    """CARD: P2 F, P4 R, P7 S, P10 S. UPI: P1 S, P3 S, P8 F, P9 S. WALLET P5, CASH P6."""
    assert query(fixture_wh, "05_payment_success_rate").rows == [
        ("CARD", 4, 2, dec("50.00")),
        ("UPI", 4, 3, dec("75.00")),
        ("CASH", 1, 1, dec("100.00")),
        ("WALLET", 1, 1, dec("100.00")),
    ]
    overall = sum(row[2] for row in query(fixture_wh, "05_payment_success_rate").rows)
    assert overall * 100 / 10 == 70  # 7 of 10 attempts succeeded


def test_q06_revenue_by_restaurant_city(fixture_wh: Warehouse) -> None:
    """Pune = R1 + R3 (1100 + 750); Mumbai = R2; Delhi's R4 has no orders."""
    assert query(fixture_wh, "06_revenue_by_city").rows == [
        ("Pune", 6, dec("1850.00")),
        ("Mumbai", 3, dec("700.00")),
        ("Delhi", 0, 0),
    ]


def test_q07_and_q08_rank_restaurants(fixture_wh: Warehouse) -> None:
    by_orders = [row[0] for row in query(fixture_wh, "07_orders_by_restaurant").rows]
    top = query(fixture_wh, "08_top_restaurants")
    assert by_orders == ["R1", "R2", "R3", "R4"]
    assert [(row[0], row[6]) for row in top.rows] == [
        ("R1", dec("1100.00")),
        ("R3", dec("750.00")),
        ("R2", dec("700.00")),
        ("R4", 0),
    ]


def test_q09_orders_by_cuisine(fixture_wh: Warehouse) -> None:
    assert query(fixture_wh, "09_orders_by_cuisine").rows == [
        ("Indian", 4, dec("1100.00"), dec("44.44")),
        ("Chinese", 3, dec("700.00"), dec("33.33")),
        ("Italian", 2, dec("750.00"), dec("22.22")),
        ("Desserts", 0, 0, dec("0.00")),
    ]


def test_q10_average_delivery_time(fixture_wh: Warehouse) -> None:
    """(30 + 50 + 45 + 40 + 55 + 25) / 6 = 40.83; Mumbai (50 + 45) / 2; Pune 150 / 4."""
    assert query(fixture_wh, "10_average_delivery_time").rows == [
        ("overall", None, 6, dec("40.83")),
        ("city", "Mumbai", 2, dec("47.50")),
        ("city", "Pune", 4, dec("37.50")),
    ]


def test_q11_late_deliveries(fixture_wh: Warehouse) -> None:
    """D2 (50) and D6 (55) are late; D4 (exactly 45) is not."""
    assert query(fixture_wh, "11_late_deliveries").rows == [
        ("overall", None, 6, 2, dec("33.33")),
        ("city", "Mumbai", 2, 1, dec("50.00")),
        ("city", "Pune", 4, 1, dec("25.00")),
    ]


def test_q12_and_q13_customer_frequency_and_repeat_rate(fixture_wh: Warehouse) -> None:
    """Active customers C1 (3), C2 (3), C3 (2), C4 (1); C5 never ordered -> 3 of 4 repeat."""
    assert query(fixture_wh, "12_customer_order_frequency").rows == [
        (1, 1, dec("25.00")),
        (2, 1, dec("25.00")),
        (3, 2, dec("50.00")),
    ]
    assert query(fixture_wh, "13_repeat_customers").rows == [(4, 3, dec("75.00"))]


def test_q14_monthly_revenue(fixture_wh: Warehouse) -> None:
    assert query(fixture_wh, "14_monthly_revenue").rows == [
        ("2026-09", 7, dec("2550.00"), dec("364.29"))
    ]


def test_q15_peak_ordering_hours(fixture_wh: Warehouse) -> None:
    """O3 and O4 were both placed at 20h on Tuesday 09-01; every other slot has one order."""
    rows = query(fixture_wh, "15_peak_ordering_hours").rows
    assert rows[0] == (20, "Tuesday", 2, 1)
    assert sum(row[2] for row in rows) == 9
    assert {row[3] for row in rows[1:]} == {2}


def test_q01_q02_match_the_views(fixture_wh: Warehouse) -> None:
    assert query(fixture_wh, "01_daily_orders").rows == [(D1, 4, 3, 1), (D2, 5, 3, 1)]
    assert query(fixture_wh, "02_daily_revenue").rows == [
        (D1, 3, dec("1200.00")),
        (D2, 4, dec("1350.00")),
    ]


# --- Generated sample data (AC-080, AC-083) --------------------------------------------


@pytest.fixture(scope="module")
def sample_wh(validated_lake: ValidatedLake):
    with fresh_warehouse("test_analytics_sample") as warehouse:
        for run_date, run_id in ((HISTORICAL_RUN, "test__historical"), (DAILY_RUN, "test__daily")):
            load_warehouse(
                warehouse.settings, validated_lake.storage, run_date, run_id, conn=warehouse.conn
            )
        yield warehouse


@pytest.mark.spark
@pytest.mark.parametrize("name", QUERIES)
def test_every_query_returns_rows_on_generated_data(sample_wh: Warehouse, name: str) -> None:
    result = query(sample_wh, name)
    assert result.rows, name
    assert len(result.columns) == len(result.rows[0])


@pytest.mark.spark
def test_peak_hours_are_lunch_or_dinner_on_generated_data(sample_wh: Warehouse) -> None:
    """Spec 02 M-12: the generator puts peaks at 12–14h and 19–22h."""
    per_hour = Counter()
    for hour, _day, orders, _rank in query(sample_wh, "15_peak_ordering_hours").rows:
        per_hour[hour] += orders
    top_three = [hour for hour, _ in per_hour.most_common(3)]
    assert set(top_three) <= {12, 13, 14, 19, 20, 21, 22}


@pytest.mark.spark
def test_views_reconcile_with_the_fact_tables(sample_wh: Warehouse) -> None:
    """Additive view columns sum back to the fact tables (no join fan-out)."""
    [(orders, revenue)] = sample_wh.query(
        "SELECT (SELECT COUNT(*) FROM {schema}.fact_order), "
        "(SELECT SUM(payment_amount) FROM {schema}.fact_payment WHERE payment_status = 'SUCCESS')"
    )
    [(daily_orders,)] = sample_wh.query("SELECT SUM(total_orders) FROM {schema}.vw_daily_orders")
    [(restaurant_orders, restaurant_revenue)] = sample_wh.query(
        "SELECT SUM(total_orders), SUM(total_revenue) FROM {schema}.vw_restaurant_performance"
    )
    [(customer_orders, customer_spend)] = sample_wh.query(
        "SELECT SUM(total_orders), SUM(total_spend) FROM {schema}.vw_customer_summary"
    )
    [(daily_revenue,)] = sample_wh.query("SELECT SUM(total_revenue) FROM {schema}.vw_daily_revenue")
    assert daily_orders == restaurant_orders == customer_orders == orders
    assert daily_revenue == restaurant_revenue == customer_spend == revenue
