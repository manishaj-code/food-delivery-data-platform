"""Pipeline metrics (spec 12 §3).

``LogMetricsPublisher`` writes ``METRIC name=value`` log lines (local mode);
``CloudWatchMetricsPublisher`` sends them to CloudWatch namespace ``FoodDelivery/Pipeline``
(``METRICS_ENABLED=true``). Every metric carries the ``Environment`` dimension, per-dataset
metrics also ``Dataset`` (low cardinality, CR-04).

Publishing never fails the pipeline: ``publish_metrics`` logs errors as WARNING and
swallows them. This is the only place where the project swallows exceptions, by design
(AC-074) — metrics are observability, not part of the data's outcome.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Protocol

from src.common.config import Settings

logger = logging.getLogger(__name__)

NAMESPACE = "FoodDelivery/Pipeline"
MAX_METRICS_PER_CALL = 1000  # PutMetricData limit


@dataclass(frozen=True)
class Metric:
    name: str
    value: float
    unit: str = "Count"  # Count | Percent | Seconds
    dimensions: dict[str, str] = field(default_factory=dict)


def dataset_metric(name: str, value: float, dataset: str, unit: str = "Count") -> Metric:
    return Metric(name, value, unit, {"Dataset": dataset})


class MetricsPublisher(Protocol):
    def publish(self, metrics: Sequence[Metric]) -> None: ...


class LogMetricsPublisher:
    def __init__(self, environment: str) -> None:
        self.environment = environment

    def publish(self, metrics: Sequence[Metric]) -> None:
        for metric in metrics:
            dims = {"Environment": self.environment, **metric.dimensions}
            details = " ".join(f"{key}={val}" for key, val in dims.items())
            logger.info("METRIC %s=%s unit=%s %s", metric.name, metric.value, metric.unit, details)


@lru_cache(maxsize=4)
def _cloudwatch_client(region: str) -> Any:
    import boto3

    return boto3.client("cloudwatch", region_name=region)


def _batches(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


class CloudWatchMetricsPublisher:
    """``PutMetricData`` in batches; credentials come from the default AWS chain."""

    def __init__(self, environment: str, region: str, client: Any = None) -> None:
        self.environment = environment
        self.client = client or _cloudwatch_client(region)

    def _datum(self, metric: Metric) -> dict[str, Any]:
        dims = {"Environment": self.environment, **metric.dimensions}
        return {
            "MetricName": metric.name,
            "Dimensions": [{"Name": key, "Value": str(val)} for key, val in dims.items()],
            "Value": float(metric.value),
            "Unit": metric.unit,
        }

    def publish(self, metrics: Sequence[Metric]) -> None:
        data = [self._datum(metric) for metric in metrics]
        for batch in _batches(data, MAX_METRICS_PER_CALL):
            self.client.put_metric_data(Namespace=NAMESPACE, MetricData=list(batch))
        logger.info("Published %d metrics to CloudWatch %s", len(data), NAMESPACE)


def get_metrics_publisher(settings: Settings) -> MetricsPublisher:
    if settings.metrics_enabled:
        return CloudWatchMetricsPublisher(settings.pipeline_env, settings.aws_region)
    return LogMetricsPublisher(settings.pipeline_env)


def publish_metrics(settings: Settings, metrics: Sequence[Metric]) -> None:
    """Best effort: a metrics failure is logged as WARNING, never raised (AC-074)."""
    if not metrics:
        return
    try:
        get_metrics_publisher(settings).publish(metrics)
    except Exception:  # noqa: BLE001 — metrics must never break the pipeline
        names = sorted({metric.name for metric in metrics})
        logger.warning("Failed to publish metrics %s", ", ".join(names), exc_info=True)


def publish_metric(
    settings: Settings, name: str, value: float, unit: str = "Count", **dimensions: str
) -> None:
    """One metric, e.g. ``publish_metric(settings, "PipelineFailure", 1)``."""
    publish_metrics(settings, [Metric(name, value, unit, dimensions)])
