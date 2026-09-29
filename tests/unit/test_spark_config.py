"""Tests for src/common/spark.py configuration (no JVM needed)."""

from __future__ import annotations

import pytest

from src.common.config import load_settings
from src.common.spark import S3A_CREDENTIALS_PROVIDER, spark_config

pytestmark = pytest.mark.unit


def test_local_mode_config() -> None:
    config = spark_config(load_settings({"SPARK_DRIVER_MEMORY": "2g"}))

    assert config["spark.sql.session.timeZone"] == "UTC"
    assert config["spark.driver.memory"] == "2g"
    assert not any(key.startswith("spark.hadoop.fs.s3a") for key in config)


def test_s3_mode_adds_s3a_jars_and_default_credential_chain(monkeypatch) -> None:
    monkeypatch.setenv("SPARK_S3A_JARS_DIR", "/opt/spark-jars")
    settings = load_settings(
        {"STORAGE_MODE": "s3", "S3_BUCKET": "bucket", "AWS_REGION": "eu-west-1"}
    )

    config = spark_config(settings)

    assert config["spark.driver.extraClassPath"] == "/opt/spark-jars/*"
    assert config["spark.hadoop.fs.s3a.aws.credentials.provider"] == S3A_CREDENTIALS_PROVIDER
    assert config["spark.hadoop.fs.s3a.endpoint.region"] == "eu-west-1"
    assert not any("secret" in key.lower() or "access.key" in key for key in config)
