"""Loading the same run date twice changes nothing but ``updated_at`` (FR-092, AC-035)."""

from __future__ import annotations

import pytest

from src.warehouse.loader import LOAD_ORDER, load_warehouse
from tests.integration.conftest import Warehouse
from tests.sample_lake import HISTORICAL_RUN, ValidatedLake

pytestmark = [pytest.mark.integration, pytest.mark.spark]

TARGETS = [table for table in LOAD_ORDER if table.business_key]


def _snapshot(warehouse: Warehouse) -> dict[str, list[tuple]]:
    """Every dimension/fact row, all columns except ``updated_at``, ordered by business key."""
    snapshot = {}
    for table in TARGETS:
        columns = [
            name
            for (name,) in warehouse.query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (warehouse.schema, table.target),
            )
            if name != "updated_at"
        ]
        snapshot[table.target] = warehouse.query(
            f"SELECT {', '.join(columns)} FROM {{schema}}.{table.target} "
            f"ORDER BY {table.business_key}"
        )
    return snapshot


def test_same_run_date_twice_gives_identical_rows(
    warehouse: Warehouse, validated_lake: ValidatedLake
) -> None:
    def load(run_id: str):
        return load_warehouse(
            warehouse.settings, validated_lake.storage, HISTORICAL_RUN, run_id, conn=warehouse.conn
        )

    load("test__first")
    first = _snapshot(warehouse)
    second_results = load("test__second")
    second = _snapshot(warehouse)

    assert second == first  # same rows, same surrogate keys, same created_at
    for result in second_results:
        if result.table in first:
            assert result.rows_inserted == 0
            assert result.rows_updated == result.records_staged
            assert len(first[result.table]) == result.records_staged
