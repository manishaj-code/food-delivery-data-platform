"""Post-load checks WQ-001 – WQ-006 (FR-026, AC-034).

Each check passes on the loaded sample and fails on crafted bad data. Bad data is
changed inside an open transaction and rolled back, so every test sees a clean load.
"""

from __future__ import annotations

import logging

import pytest

from src.common.exceptions import DataQualityThresholdError
from src.warehouse.loader import load_warehouse
from src.warehouse.post_load_checks import CHECKS, run_post_load_checks
from tests.integration.conftest import Warehouse
from tests.sample_lake import HISTORICAL_RUN, ValidatedLake

pytestmark = [pytest.mark.integration, pytest.mark.spark]


@pytest.fixture(scope="module")
def loaded(warehouse: Warehouse, validated_lake: ValidatedLake) -> Warehouse:
    load_warehouse(
        warehouse.settings,
        validated_lake.storage,
        HISTORICAL_RUN,
        "test__historical",
        conn=warehouse.conn,
    )
    return warehouse


@pytest.fixture
def run_checks(loaded: Warehouse, validated_lake: ValidatedLake):
    def run():
        return run_post_load_checks(
            loaded.settings,
            validated_lake.storage,
            HISTORICAL_RUN,
            "test__historical",
            conn=loaded.conn,
        )

    yield run
    loaded.conn.rollback()


def test_every_check_passes_on_the_loaded_sample(run_checks) -> None:
    results = run_checks()

    assert [result.check_id for result in results] == [check.check_id for check in CHECKS]
    assert all(result.passed for result in results)  # incl. WQ-005: SQL AOV == PySpark AOV


BAD_DATA = {
    "WQ-001": "DELETE FROM {schema}.stg_order "
    "WHERE order_id = (SELECT MIN(order_id) FROM {schema}.stg_order)",
    # PostgreSQL enforces UNIQUE; drop it to simulate Redshift, which does not.
    "WQ-002": "ALTER TABLE {schema}.dim_customer DROP CONSTRAINT dim_customer_customer_id_key; "
    "INSERT INTO {schema}.dim_customer (customer_id, customer_name, email, city, signup_date, "
    "source_ingestion_date, created_at, updated_at) SELECT customer_id, customer_name, email, "
    "city, signup_date, source_ingestion_date, created_at, updated_at "
    "FROM {schema}.dim_customer ORDER BY customer_id LIMIT 1",
    "WQ-003": "UPDATE {schema}.fact_order SET customer_key = -1 "
    "WHERE order_id = (SELECT MIN(order_id) FROM {schema}.fact_order)",
    "WQ-004": "DELETE FROM {schema}.fact_order "
    "WHERE order_id = (SELECT MIN(order_id) FROM {schema}.fact_payment)",
    "WQ-006": "UPDATE {schema}.fact_payment SET payment_amount = -1 "
    "WHERE payment_id = (SELECT MIN(payment_id) FROM {schema}.fact_payment)",
}


@pytest.mark.parametrize("check_id", BAD_DATA)
def test_error_check_fails_the_run(loaded: Warehouse, run_checks, check_id: str) -> None:
    loaded.execute(BAD_DATA[check_id])

    with pytest.raises(DataQualityThresholdError) as excinfo:
        run_checks()

    assert excinfo.value.context["checks"] == [check_id]
    assert not excinfo.value.retryable


def test_aov_mismatch_only_warns(
    loaded: Warehouse, run_checks, caplog: pytest.LogCaptureFixture
) -> None:
    loaded.execute(
        "UPDATE {schema}.stg_daily_order_metrics SET average_order_value = average_order_value + 1 "
        "WHERE order_date = (SELECT MIN(order_date) FROM {schema}.stg_daily_order_metrics)"
    )

    with caplog.at_level(logging.WARNING):
        results = {result.check_id: result for result in run_checks()}

    assert results["WQ-005"].failures == {"daily_order_metrics.average_order_value": 1}
    assert "Post-load check WQ-005 failed (WARN)" in caplog.text
