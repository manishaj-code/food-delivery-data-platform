"""Each check type on small crafted DataFrames (FR-021, FR-022, AC-026)."""

from __future__ import annotations

from datetime import date

import pytest

from src.validation.rule_catalog import RULES
from src.validation.rules import RuleContext, failed, prepare

pytestmark = pytest.mark.spark

RUN_DATE = date(2026, 9, 29)
RULES_BY_ID = {rule.rule_id: rule for rule in RULES}


def evaluate(spark, rule_id: str, rows: list[dict], parent_keys: dict | None = None) -> list[bool]:
    """Failed flag per input row, in input order. Missing columns are NULL strings."""
    rule = RULES_BY_ID[rule_id]
    columns = sorted({name for row in rows for name in row} | set(rule.columns))
    if "_source_row_number" not in columns:
        columns.append("_source_row_number")
    data = [
        tuple(str(index) if name == "_source_row_number" else row.get(name) for name in columns)
        for index, row in enumerate(rows, start=1)
    ]
    df = spark.createDataFrame(data, ", ".join(f"`{name}` string" for name in columns))
    context = RuleContext(RUN_DATE, parent_keys or {})
    result = prepare(rule, df, context).select(
        "_source_row_number", failed(rule, context).alias("failed")
    )
    return [row.failed for row in sorted(result.collect(), key=lambda r: int(r[0]))]


def test_not_null(spark) -> None:
    assert evaluate(spark, "DQ-CUS-001", [{"customer_id": "C1"}, {"customer_id": None}]) == [
        False,
        True,
    ]


def test_unique_keeps_first_occurrence_by_row_number(spark) -> None:
    rows = [{"order_id": "O1"}, {"order_id": "O2"}, {"order_id": "O1"}, {"order_id": None}]

    # NULL keys are left to the NOT NULL rule, not reported as duplicates.
    assert evaluate(spark, "DQ-ORD-002", rows) == [False, False, True, False]


def test_unique_orders_row_numbers_numerically(spark) -> None:
    rows = [{"order_id": "O1"}] * 10  # row "10" must not sort before row "2"

    assert evaluate(spark, "DQ-ORD-002", rows) == [False] + [True] * 9


def test_in_set(spark) -> None:
    rows = [{"order_status": s} for s in ("DELIVERED", "SHIPPED", None, "CANCELLED")]

    assert evaluate(spark, "DQ-ORD-007", rows) == [False, True, True, False]


@pytest.mark.parametrize(
    ("amount", "expected"),
    [("456.50", False), ("0", False), ("-10.00", True), ("abc", True), ("NaN", True), (None, True)],
)
def test_numeric_amount(spark, amount: str | None, expected: bool) -> None:
    assert evaluate(spark, "DQ-ORD-006", [{"order_amount": amount}]) == [expected]


@pytest.mark.parametrize(
    ("rating", "expected"),
    [("4.3", False), ("0", False), ("5.0", False), (None, False), ("7.5", True), ("-1", True)],
)
def test_numeric_range_allows_null_rating(spark, rating: str | None, expected: bool) -> None:
    assert evaluate(spark, "DQ-RES-003", [{"rating": rating}]) == [expected]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-01-10", False),
        ("2026-09-29", False),  # run date itself is allowed
        ("2026-09-30", True),  # future relative to run date
        ("2026-02-30", True),
        ("not-a-date", True),
        ("31/01/2026", True),
        (None, True),
    ],
)
def test_valid_date_not_after_run_date(spark, value: str | None, expected: bool) -> None:
    assert evaluate(spark, "DQ-CUS-004", [{"signup_date": value}]) == [expected]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-09-29 23:59:59", False),
        ("2026-09-30 00:00:00", True),
        ("2026-02-30 12:00:00", True),
        ("2026-13-01 10:00:00", True),
        ("2026-09-29", True),
        ("not-a-date", True),
    ],
)
def test_valid_timestamp_not_after_end_of_run_date(spark, value: str, expected: bool) -> None:
    assert evaluate(spark, "DQ-ORD-008", [{"order_date": value}]) == [expected]


def test_valid_timestamp_allows_nulls_when_configured(spark) -> None:
    rows = [
        {"pickup_time": None, "delivery_time": None},
        {"pickup_time": "2026-09-29 12:00:00", "delivery_time": None},
        {"pickup_time": "2026-09-29 12:00:00", "delivery_time": "bad"},
    ]

    assert evaluate(spark, "DQ-DEL-007", rows) == [False, False, True]


def test_column_lte(spark) -> None:
    rows = [
        {"pickup_time": "2026-09-29 12:00:00", "delivery_time": "2026-09-29 12:30:00"},
        {"pickup_time": "2026-09-29 12:30:00", "delivery_time": "2026-09-29 12:30:00"},
        {"pickup_time": "2026-09-29 12:30:00", "delivery_time": "2026-09-29 12:20:00"},
        {"pickup_time": "2026-09-29 12:30:00", "delivery_time": None},
        {"pickup_time": "bad", "delivery_time": "2026-09-29 12:20:00"},  # left to DQ-DEL-007
    ]

    assert evaluate(spark, "DQ-DEL-005", rows) == [False, False, True, False, False]


def test_required_when(spark) -> None:
    rows = [
        {"delivery_status": "DELIVERED", "delivery_time": "2026-09-29 12:30:00"},
        {"delivery_status": "DELIVERED", "delivery_time": None},
        {"delivery_status": "FAILED", "delivery_time": None},
    ]

    assert evaluate(spark, "DQ-DEL-008", rows) == [False, True, False]


def test_exists_in_uses_parent_keys_including_earlier_processed(spark) -> None:
    batch_customers = spark.createDataFrame([("C1",)], "customer_id string")
    processed_customers = spark.createDataFrame([("C2",), ("C2",)], "customer_id string")
    parents = {"customers": batch_customers.unionByName(processed_customers)}
    rows = [{"customer_id": c} for c in ("C1", "C2", "C3", None)]

    assert evaluate(spark, "DQ-ORD-004", rows, parents) == [False, False, True, True]


def test_exists_in_without_parents_fails_every_row(spark) -> None:
    empty = spark.createDataFrame([], "restaurant_id string")

    assert evaluate(spark, "DQ-ORD-005", [{"restaurant_id": "R1"}], {"restaurants": empty}) == [
        True
    ]
