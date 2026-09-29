"""Daily batches on top of the historical load (FR-053, FR-083, AC-033, AC-036).

The sample 2026-09-01 increment only contains new keys, so changed records (a customer
moving city, an order status update, a late-arriving older batch) are staged by hand
and applied with the same upsert the pipeline uses.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from src.common.constants import DELIVERY_PARTNERS, ORDERS
from src.warehouse.loader import LOAD_ORDER, load_warehouse, upsert_table
from tests.integration.conftest import Warehouse
from tests.sample_lake import DAILY_RUN, HISTORICAL_RUN, ValidatedLake

pytestmark = [pytest.mark.integration, pytest.mark.spark]

TABLES = {table.target: table for table in LOAD_ORDER}
LATER_BATCH = date(2026, 9, 2)


@pytest.fixture(scope="module")
def daily_results(warehouse: Warehouse, validated_lake: ValidatedLake):
    for run_date, run_id in ((HISTORICAL_RUN, "test__historical"), (DAILY_RUN, "test__daily")):
        results = load_warehouse(
            warehouse.settings, validated_lake.storage, run_date, run_id, conn=warehouse.conn
        )
    return {result.dataset: result for result in results}


def _stage(warehouse: Warehouse, staging: str, row: dict) -> None:
    """Replace ``staging`` with one hand-made row."""
    columns = ", ".join(row)
    placeholders = ", ".join(f"%({name})s" for name in row)
    warehouse.execute(f"DELETE FROM {{schema}}.{staging}")
    warehouse.execute(f"INSERT INTO {{schema}}.{staging} ({columns}) VALUES ({placeholders})", row)
    warehouse.conn.commit()


def _order(warehouse: Warehouse, order_id: str) -> tuple:
    [row] = warehouse.query(
        "SELECT order_key, order_status, source_ingestion_date, created_at, updated_at "
        "FROM {schema}.fact_order WHERE order_id = %s",
        (order_id,),
    )
    return row


def test_daily_batch_adds_new_keys(
    warehouse: Warehouse, daily_results, validated_lake: ValidatedLake
) -> None:
    historical = validated_lake.historical_transform.counts
    daily = validated_lake.daily_transform.counts

    assert warehouse.count("fact_order") == historical[ORDERS] + daily[ORDERS]
    assert daily_results[ORDERS].rows_inserted == daily[ORDERS]
    # The sample day has no new partners: a header-only file loads as NO_DATA.
    assert daily_results[DELIVERY_PARTNERS].status == "NO_DATA"
    assert warehouse.count("dim_delivery_partner") == historical[DELIVERY_PARTNERS]


def test_customer_change_keeps_the_surrogate_key(warehouse: Warehouse, daily_results) -> None:
    """SCD Type 1: attributes overwritten in place; customer_key and created_at unchanged."""
    [(key, customer_id, name, email, signup, created)] = warehouse.query(
        "SELECT customer_key, customer_id, customer_name, email, signup_date, created_at "
        "FROM {schema}.dim_customer ORDER BY customer_id LIMIT 1"
    )
    _stage(
        warehouse,
        "stg_customer",
        {
            "customer_id": customer_id,
            "customer_name": name,
            "email": email,
            "city": "Mysuru",
            "signup_date": signup,
            "source_ingestion_date": LATER_BATCH,
            "_run_id": "test__city_change",
        },
    )

    updated, inserted = upsert_table(
        warehouse.conn, warehouse.schema, TABLES["dim_customer"], datetime(2026, 9, 2, 1)
    )

    assert (updated, inserted) == (1, 0)
    assert warehouse.query(
        "SELECT customer_key, city, created_at, updated_at FROM {schema}.dim_customer "
        "WHERE customer_id = %s",
        (customer_id,),
    ) == [(key, "Mysuru", created, datetime(2026, 9, 2, 1))]


def test_status_update_modifies_the_existing_order(warehouse: Warehouse, daily_results) -> None:
    [(order_id, customer_id, restaurant_id, timestamp, order_date, amount)] = warehouse.query(
        "SELECT o.order_id, c.customer_id, r.restaurant_id, o.order_timestamp, d.full_date, "
        "o.order_amount FROM {schema}.fact_order AS o "
        "JOIN {schema}.dim_customer AS c ON c.customer_key = o.customer_key "
        "JOIN {schema}.dim_restaurant AS r ON r.restaurant_key = o.restaurant_key "
        "JOIN {schema}.dim_date AS d ON d.date_key = o.order_date_key "
        "WHERE o.order_status = 'DELIVERED' ORDER BY o.order_id LIMIT 1"
    )
    key, _, _, created, _ = _order(warehouse, order_id)
    orders_before = warehouse.count("fact_order")
    row = {
        "order_id": order_id,
        "customer_id": customer_id,
        "restaurant_id": restaurant_id,
        "order_timestamp": timestamp,
        "order_date": order_date,
        "order_amount": amount,
        "order_status": "CANCELLED",
        "source_ingestion_date": LATER_BATCH,
        "_run_id": "test__status_update",
    }
    _stage(warehouse, "stg_order", row)
    upsert_table(warehouse.conn, warehouse.schema, TABLES["fact_order"], datetime(2026, 9, 2, 1))

    assert _order(warehouse, order_id)[:4] == (key, "CANCELLED", LATER_BATCH, created)
    assert warehouse.count("fact_order") == orders_before

    # FR-083: an older batch arriving afterwards (e.g. a rerun of 2026-08-31) is ignored.
    _stage(
        warehouse,
        "stg_order",
        row | {"order_status": "DELIVERED", "source_ingestion_date": HISTORICAL_RUN},
    )
    updated, inserted = upsert_table(
        warehouse.conn, warehouse.schema, TABLES["fact_order"], datetime(2026, 9, 3)
    )

    assert (updated, inserted) == (0, 0)
    assert _order(warehouse, order_id)[1:3] == ("CANCELLED", LATER_BATCH)
