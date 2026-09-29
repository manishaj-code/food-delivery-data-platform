"""The rule catalogue matches spec 06 §2 one-to-one (FR-020, AC-020)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.common.constants import DATASETS, SOURCE_COLUMNS
from src.validation.rule_catalog import RULES, parent_datasets, rules_for
from src.validation.rules import CHECK_TYPES, ERROR, WARN

pytestmark = pytest.mark.unit

SPEC = Path(__file__).resolve().parents[2] / "docs" / "spec" / "06-data-quality-specification.md"
SPEC_ROW = re.compile(r"^\| (DQ-[A-Z]{3}-\d{3}) \|.*\| (ERROR|WARN) \|", re.MULTILINE)


def _spec_rules() -> dict[str, str]:
    return dict(SPEC_ROW.findall(SPEC.read_text(encoding="utf-8")))


def test_catalogue_matches_spec_ids_and_severities() -> None:
    spec = _spec_rules()

    assert len(spec) == 34
    assert {rule.rule_id: rule.severity for rule in RULES} == spec


def test_counts_32_error_and_2_warn() -> None:
    severities = [rule.severity for rule in RULES]
    assert severities.count(ERROR) == 32
    assert severities.count(WARN) == 2


def test_rule_ids_are_unique_and_well_formed() -> None:
    ids = [rule.rule_id for rule in RULES]
    assert len(ids) == len(set(ids))
    prefixes = dict(zip(("CUS", "RES", "DPT", "ORD", "PAY", "DEL"), DATASETS, strict=True))
    for rule in RULES:
        assert prefixes[rule.rule_id.split("-")[1]] == rule.dataset


def test_rules_reference_real_columns_and_checks() -> None:
    for rule in RULES:
        assert rule.check in CHECK_TYPES, rule.rule_id
        assert set(rule.columns) <= set(SOURCE_COLUMNS[rule.dataset]), rule.rule_id


def test_every_dataset_has_rules_and_parents_come_first() -> None:
    for dataset in DATASETS:
        assert rules_for(dataset)
        for parent in parent_datasets(dataset):
            assert DATASETS.index(parent) < DATASETS.index(dataset)
    assert parent_datasets("orders") == ("customers", "restaurants")
    assert parent_datasets("delivery") == ("delivery_partners", "orders")
