"""Smoke test: PySpark starts inside the container (Java 21 present)."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.spark


def test_spark_session_runs_a_job(spark) -> None:
    df = spark.createDataFrame([("ORD0000001", 456.5), ("ORD0000002", 120.0)], ["order_id", "amt"])

    assert df.count() == 2
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_s3a_filesystem_is_on_the_classpath(spark) -> None:
    """hadoop-aws + AWS SDK match the Hadoop inside PySpark (risk TR-01)."""
    if not os.path.isdir(os.environ.get("SPARK_S3A_JARS_DIR", "")):
        pytest.skip("S3A jars not installed (image built with INSTALL_S3A_JARS=false)")
    jvm = spark.sparkContext._jvm
    hadoop_version = jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    s3a = jvm.java.lang.Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")
    sdk_client = jvm.java.lang.Class.forName("software.amazon.awssdk.services.s3.S3Client")

    assert hadoop_version == "3.5.0"
    assert s3a.getName() == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert sdk_client is not None
