"""Every injected bad record is detected (FR-004, FR-021, FR-022, FR-025, AC-021, AC-025, AC-026).

Counts are asserted with ``>=`` because of cascade quarantine (risk DR-01): a
quarantined order also makes its payment and delivery fail the referential rules.
"""

from __future__ import annotations

import pytest

from src.common.exceptions import DataQualityThresholdError
from src.common.storage import LocalStorage
from src.validation.run_validation import validate_all
from tests.sample_lake import (
    DAILY_RUN,
    ValidatedLake,
    ingest,
    make_settings,
    manifest,
)

pytestmark = pytest.mark.spark


def _detected(reports: dict[str, dict]) -> dict[str, int]:
    return {
        result["rule_id"]: result["failed_records"]
        for report in reports.values()
        for result in report["rule_results"] + report["warnings"]
    }


@pytest.mark.parametrize(
    ("label", "attribute"), [("historical", "historical"), ("2026-09-01", "daily")]
)
def test_every_manifest_rule_is_detected(
    validated_lake: ValidatedLake, label: str, attribute: str
) -> None:
    detected = _detected(getattr(validated_lake, attribute))
    expected = manifest(label)

    assert expected, "manifest should list injected defects"
    for rule_id, count in expected.items():
        assert detected.get(rule_id, 0) >= count, rule_id


def test_clean_rules_do_not_fire_on_generated_data(validated_lake: ValidatedLake) -> None:
    """Only injected defects (and their cascades) fail; clean generator output passes."""
    detected = _detected(validated_lake.historical)
    cascades = {"DQ-ORD-004", "DQ-PAY-003", "DQ-DEL-003"}  # NULL customer_id, quarantined orders

    assert set(detected) <= set(manifest("historical")) | cascades


def test_daily_run_accepts_parents_from_earlier_processed_partitions(
    validated_lake: ValidatedLake,
) -> None:
    """AC-026: daily orders reference customers/restaurants loaded by the historical run."""
    orders = validated_lake.daily["orders"]

    assert orders["passed"]
    assert orders["valid_records"] > 0
    assert _detected(validated_lake.daily).get("DQ-ORD-004", 0) == 0


def test_all_sample_datasets_pass_the_gate(validated_lake: ValidatedLake) -> None:
    for reports in (validated_lake.historical, validated_lake.daily):
        assert all(report["passed"] for report in reports.values())


def test_gate_blocks_run_below_threshold(spark, tmp_path) -> None:
    """AC-025: without earlier parents, daily orders fail DQ-ORD-004 and the gate stops the run.

    Reports and quarantine are still written so the failure can be investigated.
    """
    storage, settings = LocalStorage(tmp_path), make_settings(tmp_path)
    ingest(storage, DAILY_RUN, "incremental", "test__no_parents")

    with pytest.raises(DataQualityThresholdError, match="orders") as excinfo:
        validate_all(spark, storage, settings, DAILY_RUN, "test__no_parents")

    assert not excinfo.value.retryable
    assert "reports/data_quality/year=2026/month=09/day=01/orders.json" in str(excinfo.value)
    assert storage.exists("reports/data_quality/year=2026/month=09/day=01/delivery.json")
    assert storage.list("quarantine/orders/year=2026/month=09/day=01/")
