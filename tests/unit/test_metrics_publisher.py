"""Tests for src.common.metrics (FR-104, AC-072 wiring, AC-074). No AWS: botocore Stubber."""

from __future__ import annotations

import logging

import boto3
import pytest
from botocore.stub import Stubber

from src.common import metrics
from src.common.config import load_settings
from src.common.metrics import CloudWatchMetricsPublisher, Metric, dataset_metric

pytestmark = pytest.mark.unit


def test_log_publisher_writes_a_metric_line(caplog: pytest.LogCaptureFixture) -> None:
    settings = load_settings({"PIPELINE_ENV": "test"})

    with caplog.at_level(logging.INFO, logger="src.common.metrics"):
        metrics.publish_metric(settings, "RecordsIngested", 42, Dataset="orders")

    assert caplog.messages == [
        "METRIC RecordsIngested=42 unit=Count Environment=test Dataset=orders"
    ]


def test_factory_follows_metrics_enabled() -> None:
    local = metrics.get_metrics_publisher(load_settings({}))
    aws = metrics.get_metrics_publisher(
        load_settings({"METRICS_ENABLED": "true", "AWS_REGION": "us-east-1"})
    )

    assert isinstance(local, metrics.LogMetricsPublisher)
    assert isinstance(aws, CloudWatchMetricsPublisher)


def _stubbed_client():
    client = boto3.client(
        "cloudwatch",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    return client, Stubber(client)


def test_cloudwatch_publisher_namespace_dimensions_and_units() -> None:
    client, stub = _stubbed_client()
    stub.add_response(
        "put_metric_data",
        {},
        {
            "Namespace": "FoodDelivery/Pipeline",
            "MetricData": [
                {
                    "MetricName": "DataQualityScore",
                    "Dimensions": [
                        {"Name": "Environment", "Value": "dev"},
                        {"Name": "Dataset", "Value": "orders"},
                    ],
                    "Value": 99.26,
                    "Unit": "Percent",
                },
                {
                    "MetricName": "PipelineSuccess",
                    "Dimensions": [{"Name": "Environment", "Value": "dev"}],
                    "Value": 1.0,
                    "Unit": "Count",
                },
            ],
        },
    )

    with stub:
        CloudWatchMetricsPublisher("dev", "us-east-1", client).publish(
            [
                dataset_metric("DataQualityScore", 99.26, "orders", "Percent"),
                Metric("PipelineSuccess", 1),
            ]
        )

    stub.assert_no_pending_responses()


def test_cloudwatch_publisher_batches_at_the_api_limit() -> None:
    client, stub = _stubbed_client()
    sizes = []
    for _ in range(2):
        stub.add_response("put_metric_data", {})
    client.meta.events.register(
        "provide-client-params.cloudwatch.PutMetricData",
        lambda params, **kwargs: sizes.append(len(params["MetricData"])),
    )

    with stub:
        CloudWatchMetricsPublisher("dev", "us-east-1", client).publish(
            [Metric("RecordsIngested", i) for i in range(metrics.MAX_METRICS_PER_CALL + 5)]
        )

    assert sizes == [metrics.MAX_METRICS_PER_CALL, 5]


def test_publishing_errors_never_fail_the_pipeline(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """AC-074: a failing CloudWatch call is logged as WARNING and swallowed."""
    client, stub = _stubbed_client()
    stub.add_client_error("put_metric_data", "Throttling", "Rate exceeded")
    monkeypatch.setattr(
        metrics,
        "get_metrics_publisher",
        lambda settings: CloudWatchMetricsPublisher("dev", "us-east-1", client),
    )

    with stub, caplog.at_level(logging.WARNING, logger="src.common.metrics"):
        metrics.publish_metric(load_settings({}), "PipelineFailure", 1)

    [record] = caplog.records
    assert record.levelname == "WARNING"
    assert record.getMessage() == "Failed to publish metrics PipelineFailure"
    assert "Throttling" in caplog.text


def test_nothing_to_publish_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(settings):
        raise AssertionError("publisher must not be created")

    monkeypatch.setattr(metrics, "get_metrics_publisher", fail)

    metrics.publish_metrics(load_settings({}), [])
