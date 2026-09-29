"""Pipeline metrics (spec 12 §3).

``LogMetricsPublisher`` writes ``METRIC name=value`` log lines; the CloudWatch publisher
is added in Phase 14 behind the same interface. Publishing never fails the pipeline:
errors are logged as WARNING.
"""

from __future__ import annotations

import logging
from typing import Protocol

from src.common.config import Settings

logger = logging.getLogger(__name__)

NAMESPACE = "FoodDelivery/Pipeline"


class MetricsPublisher(Protocol):
    def put(self, name: str, value: float, unit: str = "Count", **dimensions: str) -> None: ...


class LogMetricsPublisher:
    def __init__(self, environment: str) -> None:
        self.environment = environment

    def put(self, name: str, value: float, unit: str = "Count", **dimensions: str) -> None:
        dims = {"Environment": self.environment, **dimensions}
        details = " ".join(f"{key}={val}" for key, val in dims.items())
        logger.info("METRIC %s=%s unit=%s %s", name, value, unit, details)


def get_metrics_publisher(settings: Settings) -> MetricsPublisher:
    return LogMetricsPublisher(settings.pipeline_env)


def publish_metric(
    settings: Settings, name: str, value: float, unit: str = "Count", **dimensions: str
) -> None:
    """Best effort: a metrics failure is logged, never raised."""
    try:
        get_metrics_publisher(settings).put(name, value, unit, **dimensions)
    except Exception:  # noqa: BLE001 — metrics must never break the pipeline
        logger.warning("Failed to publish metric %s", name, exc_info=True)
