"""Quality report, required log lines, and the quality gate (FR-024, FR-025, spec 06 §5)."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from datetime import UTC, date
from typing import Any

from src.common.exceptions import DataQualityThresholdError
from src.common.paths import report_path
from src.common.storage import Storage
from src.validation.rules import ERROR, WARN
from src.validation.validator import ValidationOutcome

logger = logging.getLogger(__name__)

STATUS_PASSED = "PASSED"
STATUS_FAILED = "FAILED"
STATUS_NO_DATA = "NO_DATA"


def quality_score(valid_records: int, total_records: int) -> float:
    """valid / total x 100, rounded to 2 decimals; 100.0 when there are no records."""
    if total_records == 0:
        return 100.0
    return round(valid_records / total_records * 100, 2)


def _rule_results(outcome: ValidationOutcome, severity: str) -> list[dict[str, Any]]:
    return [
        {"rule_id": rule.rule_id, "severity": severity, "failed_records": count}
        for rule in outcome.rules
        if rule.severity == severity and (count := outcome.rule_failures[rule.rule_id]) > 0
    ]


def build_report(
    outcome: ValidationOutcome, run_id: str, run_date: date, threshold: float
) -> dict[str, Any]:
    score = quality_score(outcome.valid_records, outcome.total_records)
    passed = score >= threshold
    if outcome.total_records == 0:
        status = STATUS_NO_DATA
    else:
        status = STATUS_PASSED if passed else STATUS_FAILED
    validated_at = outcome.validated_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "dataset": outcome.dataset,
        "total_records": outcome.total_records,
        "valid_records": outcome.valid_records,
        "invalid_records": outcome.invalid_records,
        "quality_score": score,
        "threshold": threshold,
        "passed": passed,
        "status": status,
        "rule_results": _rule_results(outcome, ERROR),
        "warnings": _rule_results(outcome, WARN),
        "validated_at": validated_at,
    }


def log_report(report: dict[str, Any]) -> None:
    """Required log lines (spec 06 §5, spec 12 §2)."""
    logger.info("Dataset: %s", report["dataset"])
    logger.info("Total Records: %d", report["total_records"])
    logger.info("Valid Records: %d", report["valid_records"])
    logger.info("Invalid Records: %d", report["invalid_records"])
    logger.info("Quality Score: %.2f%%", report["quality_score"])
    for result in report["rule_results"]:
        logger.info("Rule %s failed for %d record(s)", result["rule_id"], result["failed_records"])
    for result in report["warnings"]:
        logger.warning(
            "WARN rule %s failed for %d record(s) (kept)",
            result["rule_id"],
            result["failed_records"],
        )


def write_report(storage: Storage, report: dict[str, Any]) -> str:
    """Persist the report as JSON; overwritten per (dataset, run date). Returns its URI."""
    key = report_path(report["dataset"], date.fromisoformat(report["run_date"]))
    storage.write_text(key, json.dumps(report, indent=2) + "\n")
    report["report_path"] = storage.uri_for(key)
    return report["report_path"]


def enforce_threshold(reports: Iterable[dict[str, Any]], threshold: float) -> None:
    """Raise ``DataQualityThresholdError`` if any dataset scored below ``threshold``."""
    failing = [report for report in reports if report["quality_score"] < threshold]
    if not failing:
        return
    details = "; ".join(
        f"{r['dataset']}={r['quality_score']:.2f}% ({r.get('report_path', 'report not written')})"
        for r in failing
    )
    raise DataQualityThresholdError(
        f"Quality score below {threshold:.2f}% threshold: {details}",
        datasets=",".join(r["dataset"] for r in failing),
        threshold=threshold,
    )
