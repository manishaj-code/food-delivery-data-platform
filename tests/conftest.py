"""Shared pytest fixtures."""

from __future__ import annotations

import logging

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


@pytest.fixture(scope="session")
def spark():
    """One small local SparkSession for the whole test session (UTC, 1 core)."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("food-delivery-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()
