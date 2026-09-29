"""Tests for src.common.logging_config (FR-100)."""

from __future__ import annotations

import logging

import pytest

from src.common.logging_config import configure_logging, current_context, log_context

pytestmark = pytest.mark.unit


def _our_handlers() -> list[logging.Handler]:
    return [h for h in logging.getLogger().handlers if getattr(h, "_food_delivery_handler", False)]


def test_configure_logging_is_idempotent() -> None:
    configure_logging()
    configure_logging()

    assert len(_our_handlers()) == 1


def test_log_line_contains_level_logger_and_run_context(capsys: pytest.CaptureFixture) -> None:
    configure_logging("INFO")
    logger = logging.getLogger("src.ingestion.test")

    with log_context(run_id="manual__1", run_date="2026-09-29", dataset="orders"):
        logger.info("Starting orders ingestion")

    line = capsys.readouterr().out.strip()
    assert " - INFO - src.ingestion.test - " in line
    assert "[run_id=manual__1 run_date=2026-09-29 dataset=orders] Starting orders ingestion" in line


def test_context_defaults_and_resets() -> None:
    assert current_context() == {"run_id": "-", "run_date": "-", "dataset": "-"}

    with log_context(run_id="r1"), log_context(dataset="payments"):
        assert current_context() == {"run_id": "r1", "run_date": "-", "dataset": "payments"}

    assert current_context()["run_id"] == "-"


def test_unknown_context_field_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown log context"), log_context(user="x"):
        pass
