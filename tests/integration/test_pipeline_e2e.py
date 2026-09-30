"""The whole pipeline through the CLI runner on the sample data, without Airflow.

FR-075, FR-080, FR-081, FR-101; AC-005, AC-012, AC-021, AC-022, AC-032, AC-036, AC-070,
AC-071. Runs (fixture ``pipeline_runs``): historical load (2026-08-31) -> daily run
(2026-09-01) -> the same daily run again -> a daily run with a broken orders header
(2026-09-02), which must stop at ingest_orders and be audited.
"""

from __future__ import annotations

import json

import pytest

from src.common.constants import DATASETS
from src.common.paths import Zone, partition_path, raw_file_path, report_path
from src.transformation.publish import read_manifest
from src.transformation.schemas import PROCESSED_SCHEMAS
from src.warehouse.analytics import run_query
from src.warehouse.loader import LOAD_ORDER
from tests.integration.conftest import BROKEN_DATE, PipelineRuns
from tests.sample_lake import DAILY_RUN, HISTORICAL_RUN, manifest

pytestmark = [pytest.mark.integration, pytest.mark.spark]

# Successful run dates -> (bad-record manifest label, run_id that last processed the date)
RUN_DATES = {
    HISTORICAL_RUN: ("historical", "cli__test_historical"),
    DAILY_RUN: (DAILY_RUN.isoformat(), "cli__test_daily_rerun"),
}

# Spec 12 §2 (dataset-specific ones checked for orders).
REQUIRED_MESSAGES = (
    "Starting orders ingestion",
    "Records received: ",
    "Records written to raw: ",
    "Starting validation for orders",
    "Dataset: orders",
    "Total Records: ",
    "Valid Records: ",
    "Invalid Records: ",
    "Quality Score: ",
    "Transformation completed",
    "Redshift load completed",
    "Post-load checks passed",
    "Pipeline completed successfully",
)


def _report(runs: PipelineRuns, dataset: str, run_date) -> dict:
    return json.loads(runs.storage.read_bytes(report_path(dataset, run_date)))


def test_successful_runs_exit_0_and_the_broken_run_exits_1(pipeline_runs: PipelineRuns) -> None:
    assert pipeline_runs.exit_codes == {"historical": 0, "daily": 0, "rerun": 0, "broken": 1}


def test_required_log_messages_with_run_context(pipeline_runs: PipelineRuns) -> None:
    """FR-101 / AC-070 / FR-073: required messages, each line carrying run_id= and run_date=."""
    lines = pipeline_runs.log_lines
    daily = [line for line in lines if "run_id=cli__test_daily run_date=2026-09-01" in line]
    for message in REQUIRED_MESSAGES:
        assert any(message in line for line in daily), message
    failure = [line for line in lines if "Pipeline failed at task ingest_orders" in line]
    assert failure and " - ERROR - " in failure[0]
    assert "run_id=cli__test_broken run_date=2026-09-02" in failure[0]


def test_metrics_are_emitted_as_log_lines_in_local_mode(pipeline_runs: PipelineRuns) -> None:
    """Spec 12 §3 with METRICS_ENABLED=false: every metric of a run appears as a METRIC line."""
    lines = pipeline_runs.log_lines
    daily = [line for line in lines if "run_id=cli__test_daily " in line and "METRIC " in line]
    for expected in (
        "METRIC RecordsIngested=",
        "METRIC RecordsRejected=",
        "METRIC DataQualityScore=",
        "METRIC RecordsProcessed=",
        "METRIC PipelineSuccess=1 ",
        "METRIC PipelineDurationSeconds=",
    ):
        assert any(expected in line for line in daily), expected
    assert any("unit=Percent Environment=local Dataset=orders" in line for line in daily)
    broken = [line for line in lines if "run_id=cli__test_broken " in line]
    assert any("METRIC PipelineFailure=1" in line for line in broken)


def test_monitoring_queries_on_the_audit_table(pipeline_runs: PipelineRuns) -> None:
    """Spec 12 §4: recent runs, quality trend (latest attempt per date), slowest stages."""
    conn, schema = pipeline_runs.warehouse.conn, pipeline_runs.warehouse.schema

    recent = run_query(conn, schema, "monitoring/recent_runs").as_dicts()
    assert [row["run_id"] for row in recent][0] == "cli__test_broken"  # newest first
    assert sorted(row["status"] for row in recent) == ["FAILED", "SUCCESS", "SUCCESS", "SUCCESS"]

    trend = run_query(conn, schema, "monitoring/quality_trend").as_dicts()
    keys = [(row["dataset"], row["run_date"]) for row in trend]
    assert len(keys) == len(set(keys))  # one row per dataset and date
    orders = {row["run_date"]: row for row in trend if row["dataset"] == "orders"}
    assert orders[DAILY_RUN]["run_id"] == "cli__test_daily_rerun"  # the latest attempt
    assert 95 <= orders[HISTORICAL_RUN]["quality_score"] < 100

    stages = run_query(conn, schema, "monitoring/slowest_stages").as_dicts()
    runs = {row["stage"]: row["runs"] for row in stages}
    assert runs["ingestion"] == 4 and runs["validation"] == 3  # broken run stopped at ingestion
    assert [row["avg_seconds"] for row in stages] == sorted(
        (row["avg_seconds"] for row in stages), reverse=True
    )


def test_raw_zone_has_every_dataset_for_each_run(pipeline_runs: PipelineRuns) -> None:
    """AC-005; the broken run wrote nothing for orders and never reached processed/."""
    for run_date in RUN_DATES:
        for dataset in DATASETS:
            assert pipeline_runs.storage.exists(raw_file_path(dataset, run_date)), dataset
    assert not pipeline_runs.storage.exists(raw_file_path("orders", BROKEN_DATE))
    assert not pipeline_runs.storage.list(f"{Zone.PROCESSED}/orders/year=2026/month=09/day=02/")


def test_quality_reports_reconcile_and_detect_every_injected_defect(
    pipeline_runs: PipelineRuns, spark
) -> None:
    """AC-021 / AC-022: valid + quarantined = read; detected >= manifest per rule."""
    for run_date, (label, _run_id) in RUN_DATES.items():
        detected: dict[str, int] = {}
        for dataset in DATASETS:
            report = _report(pipeline_runs, dataset, run_date)
            assert report["valid_records"] + report["invalid_records"] == report["total_records"]
            quarantine = pipeline_runs.lake / partition_path(Zone.QUARANTINE, dataset, run_date)
            quarantined = spark.read.parquet(str(quarantine)).count() if quarantine.exists() else 0
            assert quarantined == report["invalid_records"], (run_date, dataset)
            for result in report["rule_results"] + report["warnings"]:
                detected[result["rule_id"]] = result["failed_records"]
        for rule_id, count in manifest(label).items():
            assert detected.get(rule_id, 0) >= count, (label, rule_id)


def test_processed_partitions_have_manifests_of_the_last_run(pipeline_runs: PipelineRuns) -> None:
    """AC-012: all eight processed datasets, manifest written by the run that published them."""
    for run_date, (_label, run_id) in RUN_DATES.items():
        for dataset in PROCESSED_SCHEMAS:
            published = read_manifest(pipeline_runs.storage, dataset, run_date)
            assert published is not None, (run_date, dataset)
            assert published["run_id"] == run_id


def test_warehouse_rows_equal_processed_keys(pipeline_runs: PipelineRuns, spark) -> None:
    """AC-032: one warehouse row per business key of the processed partitions.

    Keys, not rows: the daily sample also changes existing records (upserted in place).
    """
    for table in LOAD_ORDER:
        if not table.business_key:
            continue
        partitions = [
            str(pipeline_runs.lake / partition_path(Zone.PROCESSED, table.dataset, run_date))
            for run_date in RUN_DATES
        ]
        processed = spark.read.schema(PROCESSED_SCHEMAS[table.dataset]).parquet(*partitions)
        keys = processed.select(table.business_key).distinct().count()
        assert pipeline_runs.warehouse.count(table.target) == keys, table.target


def test_post_load_checks_pass_after_historical_and_daily_runs(
    pipeline_runs: PipelineRuns,
) -> None:
    """AC-036: every WQ check ran and none failed (not even the WARN-only ones)."""
    rows = pipeline_runs.warehouse.query(
        "SELECT run_id, status, records_in, records_rejected FROM {schema}.pipeline_run_audit "
        "WHERE stage = 'post_load_checks' ORDER BY run_id"
    )
    assert rows == [
        ("cli__test_daily", "SUCCESS", 6, 0),
        ("cli__test_daily_rerun", "SUCCESS", 6, 0),
        ("cli__test_historical", "SUCCESS", 6, 0),
    ]


def test_audit_rows_for_every_run(pipeline_runs: PipelineRuns) -> None:
    """FR-075 / AC-071: one row per run x dataset x stage, including the failed run."""
    warehouse = pipeline_runs.warehouse
    pipeline = {
        run_id: (status, error)
        for run_id, status, error in warehouse.query(
            "SELECT run_id, status, error_message FROM {schema}.pipeline_run_audit "
            "WHERE stage = 'pipeline'"
        )
    }
    assert {run_id: status for run_id, (status, _) in pipeline.items()} == {
        "cli__test_historical": "SUCCESS",
        "cli__test_daily": "SUCCESS",
        "cli__test_daily_rerun": "SUCCESS",
        "cli__test_broken": "FAILED",
    }
    assert pipeline["cli__test_broken"][1].startswith("ingest_orders: Header does not match")

    [(duplicates,)] = warehouse.query(
        "SELECT COUNT(*) FROM (SELECT run_id, dataset, stage FROM {schema}.pipeline_run_audit "
        "GROUP BY run_id, dataset, stage HAVING COUNT(*) > 1) AS d"
    )
    assert duplicates == 0

    stages = warehouse.query(
        "SELECT DISTINCT stage FROM {schema}.pipeline_run_audit WHERE run_id = %s",
        ("cli__test_daily",),
    )
    assert {stage for (stage,) in stages} == {
        "prepare_source",
        "ingestion",
        "validation",
        "transformation",
        "publish",
        "warehouse_load",
        "post_load_checks",
        "pipeline",
    }
    [(missing_duration,)] = warehouse.query(
        "SELECT COUNT(*) FROM {schema}.pipeline_run_audit "
        "WHERE run_id = 'cli__test_daily' AND duration_seconds IS NULL"
    )
    assert missing_duration == 0
    broken = dict(
        warehouse.query(
            "SELECT dataset, status FROM {schema}.pipeline_run_audit "
            "WHERE run_id = 'cli__test_broken' AND stage = 'ingestion'"
        )
    )
    assert broken == {
        "customers": "SUCCESS",
        "restaurants": "SUCCESS",
        "delivery_partners": "NO_DATA",  # no new partners on 2026-09-02
        "orders": "FAILED",
    }
