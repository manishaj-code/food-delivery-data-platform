"""Deliberately broken (Phase 12, AC-053): CI must go red. Never merged."""


def test_ci_detects_a_failing_test() -> None:
    assert 1 + 1 == 3
