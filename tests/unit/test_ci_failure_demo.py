"""Deliberately broken (Phase 12, AC-053): CI must go red. Never merged."""

import os  # unused import: ruff F401


def test_ci_detects_a_failing_test() -> None:
    assert 1 + 1 == 2
