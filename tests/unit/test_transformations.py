"""Tests for src/transformation (FR-030 – FR-035, FR-082, AC-009, AC-010)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from src.common.constants import BUSINESS_KEYS, DATASETS
from src.common.exceptions import TransformationError
from src.transformation.cleaning import deduplicate, normalise_upper, trim_and_nullify
from src.transformation.facts import transform_orders
from src.transformation.join_validation import assert_no_orphans
from src.transformation.processed_reader import current_state
from src.transformation.schemas import PROCESSED_DATASETS, PROCESSED_SCHEMAS, columns_and_types
from src.validation.validator import raw_schema, to_typed
from tests.sample_lake import ValidatedLake

pytestmark = pytest.mark.spark

RUN_ID = "test__transform"


def _validated_orders(spark, rows: list[tuple]):
    """Build a typed validated/ orders DataFrame from source strings + row numbers."""
    data = [
        (*row, "2026-09-29 01:00:00", "2026-09-29", "orders/orders.csv", str(number), RUN_ID)
        for number, row in enumerate(rows, start=1)
    ]
    return to_typed(spark.createDataFrame(data, raw_schema("orders")), "orders")


def test_trim_and_nullify(spark) -> None:
    df = spark.createDataFrame([("  Pune ", ""), ("   ", "x")], "city string, email string")

    assert [tuple(row) for row in trim_and_nullify(df).collect()] == [("Pune", None), (None, "x")]


def test_normalise_upper(spark) -> None:
    df = spark.createDataFrame([(" delivered ",)], "order_status string")

    assert normalise_upper(df, ["order_status", "missing"]).first().order_status == "DELIVERED"


def test_deduplicate_keeps_lowest_source_row_number(spark) -> None:
    df = spark.createDataFrame(
        [("O1", 3, "third"), ("O1", 1, "first"), ("O2", 2, "only")],
        "order_id string, _source_row_number int, note string",
    )

    kept = {row.order_id: row.note for row in deduplicate(df, "order_id").collect()}
    assert kept == {"O1": "first", "O2": "only"}


def test_transform_orders_schema_types_and_utc_dates(spark) -> None:
    validated = _validated_orders(
        spark,
        [
            ("ORD1", "C1", "R1", "2026-09-29 23:59:59", "456.50", "delivered"),
            ("ORD2", "C1", "R1", "2026-09-30 00:00:00", "10", "CANCELLED"),
            ("ORD1", "C1", "R1", "2026-09-29 10:00:00", "1.00", "PLACED"),  # extra copy
        ],
    )

    orders = transform_orders(validated, RUN_ID)

    assert columns_and_types(orders.schema) == columns_and_types(PROCESSED_SCHEMAS["orders"])
    rows = {row.order_id: row for row in orders.collect()}
    assert set(rows) == {"ORD1", "ORD2"}
    assert rows["ORD1"].order_timestamp == datetime(2026, 9, 29, 23, 59, 59)
    assert rows["ORD1"].order_date == date(2026, 9, 29)  # UTC: stays on its own day
    assert rows["ORD2"].order_date == date(2026, 9, 30)
    assert rows["ORD1"].order_amount == Decimal("456.50")
    assert rows["ORD1"].order_status == "DELIVERED"
    assert rows["ORD1"].source_ingestion_date == date(2026, 9, 29)
    assert rows["ORD1"]._run_id == RUN_ID


def test_assert_no_orphans_raises_with_count_and_sample(spark) -> None:
    child = spark.createDataFrame(
        [("P1", "O1"), ("P2", "O9"), ("P3", None)], "payment_id string, order_id string"
    )
    parents = spark.createDataFrame([("O1",)], "order_id string")

    with pytest.raises(TransformationError, match="without a matching orders") as excinfo:
        assert_no_orphans(child, parents, "order_id", "order_id", "payments", "orders")

    assert excinfo.value.context["orphan_count"] == 2
    assert "O9" in excinfo.value.context["sample_keys"]


def test_assert_no_orphans_passes_when_all_match(spark) -> None:
    child = spark.createDataFrame([("P1", "O1")], "payment_id string, order_id string")
    parents = spark.createDataFrame([("O1",), ("O1",)], "order_id string")

    assert_no_orphans(child, parents, "order_id", "order_id", "payments", "orders")


def test_current_state_prefers_batch_then_latest_earlier(spark) -> None:
    schema = "customer_id string, city string, source_ingestion_date date"
    batch = spark.createDataFrame([("C1", "Mumbai", date(2026, 9, 3))], schema)
    earlier = spark.createDataFrame(
        [
            ("C1", "Pune", date(2026, 9, 1)),
            ("C2", "Delhi", date(2026, 9, 1)),
            ("C2", "Chennai", date(2026, 9, 2)),
        ],
        schema,
    )

    state = {
        row.customer_id: row.city for row in current_state(batch, earlier, "customer_id").collect()
    }

    assert state == {"C1": "Mumbai", "C2": "Chennai"}
    assert current_state(batch, None, "customer_id").count() == 1


# --- end to end on the sample lake ------------------------------------------------------


@pytest.mark.parametrize("attribute", ["historical_transform", "daily_transform"])
def test_all_processed_datasets_match_spec_schemas(
    validated_lake: ValidatedLake, attribute
) -> None:
    """AC-009: six datasets + order_analytics + daily_order_metrics with spec 07 §6 schemas."""
    result = getattr(validated_lake, attribute)

    assert tuple(result.datasets) == PROCESSED_DATASETS
    for name, df in result.datasets.items():
        assert columns_and_types(df.schema) == columns_and_types(PROCESSED_SCHEMAS[name]), name


@pytest.mark.parametrize("attribute", ["historical_transform", "daily_transform"])
def test_no_duplicate_business_keys(validated_lake: ValidatedLake, attribute) -> None:
    """AC-010: one row per business key (and per order in order_analytics)."""
    result = getattr(validated_lake, attribute)
    keys = dict(BUSINESS_KEYS) | {
        "order_analytics": "order_id",
        "daily_order_metrics": "order_date",
    }

    for name, df in result.datasets.items():
        assert df.count() == df.select(keys[name]).distinct().count(), name


def test_counts_match_validated_records(validated_lake: ValidatedLake) -> None:
    counts = validated_lake.historical_transform.counts

    for dataset in DATASETS:
        assert counts[dataset] == validated_lake.historical[dataset]["valid_records"], dataset
    assert counts["order_analytics"] == counts["orders"]


def test_daily_run_resolves_parents_from_processed(validated_lake: ValidatedLake) -> None:
    """Daily orders get customer/restaurant cities from earlier processed partitions."""
    analytics = validated_lake.daily_transform.datasets["order_analytics"]

    assert analytics.count() > 0
    assert analytics.where("customer_city IS NULL OR restaurant_city IS NULL").count() == 0
    delivery = validated_lake.daily_transform.datasets["delivery"]
    assert delivery.where("order_date IS NULL").count() == 0
