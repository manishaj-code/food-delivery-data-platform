"""Warehouse DDL, dim_date, and loading one processed partition (FR-050 – FR-056, AC-030+)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from src.common.constants import CUSTOMERS, ORDERS
from src.common.exceptions import WarehouseLoadError
from src.warehouse import loader
from src.warehouse.audit import AuditRecord, write_audit
from src.warehouse.init_warehouse import init_warehouse
from src.warehouse.loader import LOAD_ORDER, load_warehouse, upsert_table
from tests.integration.conftest import Warehouse
from tests.sample_lake import HISTORICAL_RUN, ValidatedLake

pytestmark = [pytest.mark.integration, pytest.mark.spark]

TABLES = {table.target: table for table in LOAD_ORDER}


@pytest.fixture(scope="module")
def loaded(warehouse: Warehouse, validated_lake: ValidatedLake):
    return load_warehouse(
        warehouse.settings,
        validated_lake.storage,
        HISTORICAL_RUN,
        "test__historical",
        conn=warehouse.conn,
    )


def test_ddl_is_idempotent(warehouse: Warehouse) -> None:
    assert init_warehouse(warehouse.settings, warehouse.conn) == 0
    tables = warehouse.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
        (warehouse.schema,),
    )
    # 4 dimensions + 3 facts + 7 staging tables + audit
    assert len(tables) == 15


def test_dim_date_covers_2025_to_2027_without_gaps(warehouse: Warehouse) -> None:
    [(rows, first, last, days)] = warehouse.query(
        "SELECT COUNT(*), MIN(full_date), MAX(full_date), MAX(full_date) - MIN(full_date) + 1 "
        "FROM {schema}.dim_date"
    )
    assert (rows, first, last, days) == (1095, date(2025, 1, 1), date(2027, 12, 31), 1095)


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 9, 29), (20260929, 2, "Tuesday", 40, 9, "September", 3, 2026, False)),
        (date(2026, 10, 4), (20261004, 7, "Sunday", 40, 10, "October", 4, 2026, True)),
        (date(2025, 1, 1), (20250101, 3, "Wednesday", 1, 1, "January", 1, 2025, False)),
    ],
)
def test_dim_date_attributes(warehouse: Warehouse, day: date, expected: tuple) -> None:
    [row] = warehouse.query(
        "SELECT date_key, day_of_week, day_name, week_of_year, month, month_name, quarter, "
        "year, is_weekend FROM {schema}.dim_date WHERE full_date = %s",
        (day,),
    )
    assert row == expected


def test_counts_match_the_processed_manifests(
    warehouse: Warehouse, loaded, validated_lake: ValidatedLake
) -> None:
    """FR-052: staging = processed partition; first load inserts every row."""
    counts = validated_lake.historical_transform.counts
    for result in loaded:
        assert result.records_in == result.records_staged == counts[result.dataset]
        if TABLES[result.table].business_key:
            assert result.rows_inserted == counts[result.dataset]
            assert result.rows_updated == 0
            assert warehouse.count(result.table) == counts[result.dataset]


def test_fact_keys_resolve_to_the_right_dimension_rows(warehouse: Warehouse, loaded) -> None:
    """FR-054: every order's surrogate keys point at its own customer, restaurant, and date."""
    [(orders, matched)] = warehouse.query(
        "SELECT COUNT(*), SUM(CASE WHEN c.customer_id = s.customer_id "
        "AND r.restaurant_id = s.restaurant_id AND d.full_date = s.order_date THEN 1 ELSE 0 END) "
        "FROM {schema}.fact_order AS o "
        "JOIN {schema}.stg_order AS s ON s.order_id = o.order_id "
        "JOIN {schema}.dim_customer AS c ON c.customer_key = o.customer_key "
        "JOIN {schema}.dim_restaurant AS r ON r.restaurant_key = o.restaurant_key "
        "JOIN {schema}.dim_date AS d ON d.date_key = o.order_date_key"
    )
    assert orders == matched == warehouse.count("stg_order")


def test_values_round_trip_from_parquet(
    warehouse: Warehouse, loaded, validated_lake: ValidatedLake
) -> None:
    """Decimals, timestamps (UTC), and NULLs survive Parquet -> CSV -> PostgreSQL."""
    expected = validated_lake.historical_transform.datasets[ORDERS].orderBy("order_id").first()
    [row] = warehouse.query(
        "SELECT order_timestamp, order_amount, order_status, source_ingestion_date "
        "FROM {schema}.fact_order WHERE order_id = %s",
        (expected.order_id,),
    )
    assert row == (
        expected.order_timestamp,
        expected.order_amount,
        expected.order_status,
        expected.source_ingestion_date,
    )
    [(nulls,)] = warehouse.query("SELECT COUNT(*) FROM {schema}.dim_customer WHERE email IS NULL")
    customers = validated_lake.historical_transform.datasets[CUSTOMERS]
    assert nulls == customers.where("email IS NULL").count()


def test_failed_upsert_rolls_back_the_table(
    warehouse: Warehouse, loaded, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-055: the UPDATE already ran when the failure happens; the rollback undoes it."""
    before = warehouse.query("SELECT COUNT(*), MAX(updated_at) FROM {schema}.fact_order")

    def fail(*args, **kwargs):
        raise WarehouseLoadError("injected failure")

    monkeypatch.setattr(loader, "fetch_value", fail)
    with pytest.raises(WarehouseLoadError, match="injected"):
        upsert_table(warehouse.conn, warehouse.schema, TABLES["fact_order"], datetime(2030, 1, 1))

    assert warehouse.query("SELECT COUNT(*), MAX(updated_at) FROM {schema}.fact_order") == before


def test_audit_rows_are_replaced_on_rerun(warehouse: Warehouse) -> None:
    """FR-102/FR-093: one row per run_id x dataset x stage; a rerun replaces it."""

    def record(status: str, error: str | None = None) -> AuditRecord:
        return AuditRecord(
            run_id="test__audit",
            run_date=HISTORICAL_RUN,
            load_type="historical",
            dataset=ORDERS,
            stage="warehouse_load",
            status=status,
            records_in=406,
            error_message=error,
        )

    write_audit(warehouse.conn, warehouse.schema, [record("FAILED", "x" * 5000)])
    write_audit(warehouse.conn, warehouse.schema, [record("SUCCESS")])

    assert warehouse.query(
        "SELECT status, records_in, error_message FROM {schema}.pipeline_run_audit "
        "WHERE run_id = 'test__audit'"
    ) == [("SUCCESS", 406, None)]
    assert len(record("FAILED", "x" * 5000).error_message) == 1000


def test_unpublished_partition_is_rejected(
    warehouse: Warehouse, validated_lake: ValidatedLake
) -> None:
    with pytest.raises(WarehouseLoadError, match="not published") as excinfo:
        load_warehouse(
            warehouse.settings,
            validated_lake.storage,
            date(2026, 9, 3),
            "test__missing",
            conn=warehouse.conn,
        )
    assert not excinfo.value.retryable
