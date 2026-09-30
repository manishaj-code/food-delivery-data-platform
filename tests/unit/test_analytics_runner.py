"""Analytics runner and SQL files (FR-060, FR-061) — no database needed."""

from __future__ import annotations

import re
from datetime import date

import pytest

from src.common.exceptions import ConfigError
from src.warehouse import analytics
from src.warehouse.analytics import (
    MONITORING_QUERIES,
    QUERIES,
    VIEWS,
    QueryResult,
    format_result,
    run_query,
)
from src.warehouse.sql_runner import SQL_DIR, render_sql

pytestmark = pytest.mark.unit

# Spec 02 §4.
SPEC_QUERIES = (
    "01_daily_orders",
    "02_daily_revenue",
    "03_average_order_value",
    "04_cancellation_rate",
    "05_payment_success_rate",
    "06_revenue_by_city",
    "07_orders_by_restaurant",
    "08_top_restaurants",
    "09_orders_by_cuisine",
    "10_average_delivery_time",
    "11_late_deliveries",
    "12_customer_order_frequency",
    "13_repeat_customers",
    "14_monthly_revenue",
    "15_peak_ordering_hours",
)


def test_the_15_spec_queries_exist() -> None:
    assert QUERIES == SPEC_QUERIES


def test_the_monitoring_queries_exist() -> None:
    assert MONITORING_QUERIES == (
        "monitoring/quality_trend",
        "monitoring/recent_runs",
        "monitoring/slowest_stages",
    )


SQL_FILES = [f"analytics/{name}.sql" for name in QUERIES + MONITORING_QUERIES] + [
    f"analytics/views/{view}.sql" for view in VIEWS
]


@pytest.mark.parametrize("path", SQL_FILES)
def test_sql_renders_with_schema_and_no_leftover_placeholders(path: str) -> None:
    sql = render_sql(path, "food_delivery")
    assert "food_delivery." in sql
    assert "%(" not in sql  # the analytics SQL takes no parameters


def test_late_threshold_is_defined_in_one_view_only() -> None:
    """Spec 02 §5: the 45-minute threshold lives in vw_delivery_performance."""
    files = sorted((SQL_DIR / "analytics").rglob("*.sql"))
    with_threshold = [path.stem for path in files if re.search(r">\s*45\b", path.read_text())]
    assert with_threshold == ["vw_delivery_performance"]


def test_unknown_query_is_rejected_before_touching_the_database() -> None:
    with pytest.raises(ConfigError, match="Unknown analytics query"):
        run_query(conn=None, schema="food_delivery", name="../ddl/postgres/01_schema")


def test_format_result_prints_a_table_and_truncates() -> None:
    result = QueryResult(
        "01_daily_orders",
        ("order_date", "total_orders"),
        [(date(2026, 9, 1), 4), (date(2026, 9, 2), None), (date(2026, 9, 3), 7)],
    )

    text = format_result(result, limit=2)

    assert text.splitlines() == [
        "-- 01_daily_orders (3 rows)",
        "order_date  total_orders",
        "----------  ------------",
        "2026-09-01  4",
        "2026-09-02",
        "... 1 more rows",
    ]


def test_as_dicts() -> None:
    result = QueryResult("q", ("a", "b"), [(1, 2)])
    assert result.as_dicts() == [{"a": 1, "b": 2}]


def test_main_prints_the_selected_query(monkeypatch, capsys) -> None:
    calls = []

    def fake_run(settings, names, conn=None):
        calls.append(tuple(names))
        return [QueryResult(name, ("x",), [(1,)]) for name in names]

    monkeypatch.setattr(analytics, "run_analytics", fake_run)

    assert analytics.main(["--query", "08_top_restaurants"]) == 0
    assert analytics.main(["--all", "--limit", "1"]) == 0
    assert analytics.main(["--monitoring"]) == 0

    assert calls == [("08_top_restaurants",), QUERIES, MONITORING_QUERIES]
    assert "-- 08_top_restaurants (1 rows)" in capsys.readouterr().out


def test_main_rejects_an_unknown_query() -> None:
    with pytest.raises(SystemExit):
        analytics.main(["--query", "99_nope"])
