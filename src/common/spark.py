"""SparkSession factory (docs/spec/07-data-pipeline-specification.md §7).

Spark runs in local mode inside the pipeline container. In ``STORAGE_MODE=s3`` the
s3a connector jars baked into the image are added and credentials come from the AWS
SDK v2 default chain (environment, named profile, container/instance role).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common.config import Settings

SHUFFLE_PARTITIONS = 8  # small data on one machine; the default 200 only adds overhead
S3A_CREDENTIALS_PROVIDER = "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"

# Lake file format. Shared with the test SparkSession so tests write what production writes.
LAKE_WRITE_CONFIG = {
    # Keep lake partitions free of Hadoop marker files.
    "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
    "spark.sql.parquet.compression.codec": "snappy",
    # INT64 microseconds instead of the legacy INT96 encoding: standard Parquet that
    # pyarrow and Redshift COPY read without special handling.
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
}


def spark_config(settings: Settings) -> dict[str, str]:
    """Spark settings for the configured environment (separate for testability)."""
    config = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.ui.enabled": "false",
        **LAKE_WRITE_CONFIG,
    }
    if settings.storage_mode == "s3":
        jars_dir = os.environ.get("SPARK_S3A_JARS_DIR", "/opt/spark-jars")
        config |= {
            "spark.driver.extraClassPath": f"{jars_dir}/*",
            "spark.hadoop.fs.s3a.aws.credentials.provider": S3A_CREDENTIALS_PROVIDER,
            "spark.hadoop.fs.s3a.endpoint.region": settings.aws_region,
        }
    return config


def get_spark(settings: Settings, app_name: str = "food-delivery-pipeline") -> SparkSession:
    """Create (or reuse) the process-wide local SparkSession."""
    builder = SparkSession.builder.master("local[*]").appName(app_name)
    for key, value in spark_config(settings).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
