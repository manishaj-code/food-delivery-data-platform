"""Tests for src.common.metrics (FR-104, AC-074). The CloudWatch publisher follows in Phase 14."""

from __future__ import annotations

import logging

import pytest

from src.common import metrics
from src.common.config import load_settings

pytestmark = pytest.mark.unit


def test_log_publisher_writes_a_metric_line(caplog: pytest.LogCaptureFixture) -> None:
    settings = load_settings({"PIPELINE_ENV": "test"})

    with caplog.at_level(logging.INFO, logger="src.common.metrics"):
        metrics.publish_metric(settings, "RecordsIngested", 42, Dataset="orders")

    assert caplog.messages == [
        "METRIC RecordsIngested=42 unit=Count Environment=test Dataset=orders"
    ]


def test_publishing_errors_never_fail_the_pipeline(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """AC-074: a failing publisher is logged as WARNING and swallowed."""

    class Broken:
        def put(self, *args, **kwargs) -> None:
            raise ConnectionError("CloudWatch unreachable")

    monkeypatch.setattr(metrics, "get_metrics_publisher", lambda settings: Broken())

    with caplog.at_level(logging.WARNING, logger="src.common.metrics"):
        metrics.publish_metric(load_settings({}), "PipelineFailure", 1)

    [record] = caplog.records
    assert record.levelname == "WARNING"
    assert record.getMessage() == "Failed to publish metric PipelineFailure"
