"""Smoke test: PySpark starts inside the container (Java 21 present)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


def test_spark_session_runs_a_job(spark) -> None:
    df = spark.createDataFrame([("ORD0000001", 456.5), ("ORD0000002", 120.0)], ["order_id", "amt"])

    assert df.count() == 2
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
