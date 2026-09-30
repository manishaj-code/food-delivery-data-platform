"""Rerunning the same date changes nothing but ``updated_at`` (FR-092, AC-033)."""

from __future__ import annotations

import pytest

from src.warehouse.loader import load_warehouse
from tests.integration.conftest import PipelineRuns, Warehouse, business_rows
from tests.sample_lake import HISTORICAL_RUN, ValidatedLake

pytestmark = [pytest.mark.integration, pytest.mark.spark]


def test_same_run_date_twice_gives_identical_rows(
    warehouse: Warehouse, validated_lake: ValidatedLake
) -> None:
    def load(run_id: str):
        return load_warehouse(
            warehouse.settings, validated_lake.storage, HISTORICAL_RUN, run_id, conn=warehouse.conn
        )

    load("test__first")
    first = business_rows(warehouse)
    second_results = load("test__second")
    second = business_rows(warehouse)

    assert second == first  # same rows, same surrogate keys, same created_at
    for result in second_results:
        if result.table in first:
            assert result.rows_inserted == 0
            assert result.rows_updated == result.records_staged
            assert len(first[result.table]) == result.records_staged


def test_full_pipeline_rerun_of_a_date_changes_nothing(pipeline_runs: PipelineRuns) -> None:
    """Every step again for 2026-09-01 (new run_id): same lake files, rows, and values."""
    first, rerun = pipeline_runs.after_daily, pipeline_runs.after_rerun

    assert pipeline_runs.exit_codes["rerun"] == 0
    assert rerun.lake_files == first.lake_files
    assert rerun.table_counts == first.table_counts
    assert rerun.rows == first.rows
