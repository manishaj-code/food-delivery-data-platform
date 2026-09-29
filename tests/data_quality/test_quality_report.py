"""Quality score, report JSON, log lines, and the gate (FR-024, FR-025, AC-024, AC-025)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime

import pytest

from src.common.exceptions import DataQualityThresholdError
from src.common.paths import report_path
from src.validation.report import (
    build_report,
    enforce_threshold,
    log_report,
    quality_score,
)
from src.validation.rule_catalog import rules_for
from src.validation.validator import ValidationOutcome
from tests.data_quality.conftest import DAILY_RUN, HISTORICAL_RUN, ValidatedLake

RUN_DATE = date(2026, 9, 29)


def _outcome(total: int, valid: int, failures: dict[str, int] | None = None) -> ValidationOutcome:
    rules = rules_for("orders")
    counts = dict.fromkeys((rule.rule_id for rule in rules), 0) | (failures or {})
    return ValidationOutcome(
        dataset="orders",
        valid=None,  # type: ignore[arg-type]  # not used by the report
        invalid=None,  # type: ignore[arg-type]
        total_records=total,
        valid_records=valid,
        rule_failures=counts,
        rules=rules,
        validated_at=datetime(2026, 9, 29, 1, 12, 3, tzinfo=UTC),
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("valid", "total", "score"),
    [(99870, 100000, 99.87), (0, 10, 0.0), (10, 10, 100.0), (2, 3, 66.67), (0, 0, 100.0)],
)
def test_quality_score(valid: int, total: int, score: float) -> None:
    assert quality_score(valid, total) == score


@pytest.mark.unit
def test_report_shape_matches_spec() -> None:
    report = build_report(
        _outcome(100000, 99870, {"DQ-ORD-006": 10, "DQ-ORD-003": 120}), "run-1", RUN_DATE, 95.0
    )

    assert report == {
        "run_id": "run-1",
        "run_date": "2026-09-29",
        "dataset": "orders",
        "total_records": 100000,
        "valid_records": 99870,
        "invalid_records": 130,
        "quality_score": 99.87,
        "threshold": 95.0,
        "passed": True,
        "status": "PASSED",
        "rule_results": [
            {"rule_id": "DQ-ORD-003", "severity": "ERROR", "failed_records": 120},
            {"rule_id": "DQ-ORD-006", "severity": "ERROR", "failed_records": 10},
        ],
        "warnings": [],
        "validated_at": "2026-09-29T01:12:03Z",
    }


@pytest.mark.unit
def test_empty_dataset_is_no_data_and_passes() -> None:
    report = build_report(_outcome(0, 0), "run-1", RUN_DATE, 95.0)

    assert (report["quality_score"], report["passed"], report["status"]) == (100.0, True, "NO_DATA")


@pytest.mark.unit
def test_gate_passes_at_exactly_threshold_and_fails_below() -> None:
    at_threshold = build_report(_outcome(10000, 9500), "run-1", RUN_DATE, 95.0)
    below = build_report(_outcome(10000, 9499), "run-1", RUN_DATE, 95.0)
    below["report_path"] = "file:///lake/reports/orders.json"

    enforce_threshold([at_threshold], 95.0)
    assert below["status"] == "FAILED"
    with pytest.raises(DataQualityThresholdError) as excinfo:
        enforce_threshold([at_threshold, below], 95.0)

    message = str(excinfo.value)
    assert "orders=94.99%" in message
    assert "95.00%" in message
    assert "file:///lake/reports/orders.json" in message
    assert not excinfo.value.retryable


@pytest.mark.unit
def test_required_log_lines(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="src.validation")
    report = build_report(_outcome(100000, 99870, {"DQ-ORD-006": 10}), "run-1", RUN_DATE, 95.0)

    log_report(report)

    messages = [record.getMessage() for record in caplog.records]
    assert messages[:5] == [
        "Dataset: orders",
        "Total Records: 100000",
        "Valid Records: 99870",
        "Invalid Records: 130",
        "Quality Score: 99.87%",
    ]
    assert "Rule DQ-ORD-006 failed for 10 record(s)" in messages


@pytest.mark.spark
def test_report_json_is_persisted_per_dataset(validated_lake: ValidatedLake) -> None:
    key = report_path("orders", HISTORICAL_RUN)
    saved = json.loads(validated_lake.storage.read_bytes(key))

    assert saved["dataset"] == "orders"
    assert saved["run_date"] == HISTORICAL_RUN.isoformat()
    assert saved["valid_records"] + saved["invalid_records"] == saved["total_records"]
    assert saved["quality_score"] == quality_score(saved["valid_records"], saved["total_records"])
    assert validated_lake.historical["orders"]["report_path"].endswith(key)


@pytest.mark.spark
def test_header_only_daily_file_reports_no_data(validated_lake: ValidatedLake) -> None:
    partners = validated_lake.daily["delivery_partners"]  # sample 2026-09-01 file is header-only

    assert DAILY_RUN.isoformat() == partners["run_date"]
    assert (partners["total_records"], partners["status"]) == (0, "NO_DATA")
