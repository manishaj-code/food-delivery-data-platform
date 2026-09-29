"""Shared pytest fixtures."""

from __future__ import annotations

import logging
import os

import pytest


@pytest.fixture(autouse=True)
def _restore_root_logger():
    """Undo configure_logging() calls made by code under test.

    Otherwise a handler bound to pytest's captured stdout outlives the test and
    fails when libraries (e.g. py4j) log during interpreter shutdown.
    """
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


TEST_BUCKET = "food-delivery-data-test"
TEST_REGION = "ap-south-1"


@pytest.fixture
def s3_client(monkeypatch: pytest.MonkeyPatch):
    """moto-mocked S3 with an empty test bucket. Fake credentials; no network, no AWS account."""
    moto = pytest.importorskip("moto")
    import boto3

    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": TEST_REGION,
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    with moto.mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(
            Bucket=TEST_BUCKET, CreateBucketConfiguration={"LocationConstraint": TEST_REGION}
        )
        yield client


@pytest.fixture(scope="session")
def spark():
    """One small local SparkSession for the whole test session (UTC, 1 core)."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("food-delivery-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
    )
    s3a_jars = os.environ.get("SPARK_S3A_JARS_DIR")
    if s3a_jars and os.path.isdir(s3a_jars):  # installed in the container image
        builder = builder.config("spark.driver.extraClassPath", f"{s3a_jars}/*")
    session = builder.getOrCreate()
    yield session
    session.stop()
