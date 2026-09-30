"""Smoke tests against the real AWS dev environment (marker ``aws``; manual only).

Run in AWS mode after ``terraform apply`` and ``init-warehouse`` (``.env`` from
``terraform output -raw env_file``, sandbox profile mounted read-only)::

    docker compose -f docker-compose.yml -f docker-compose.aws.yml run --rm \
        --entrypoint pytest pipeline -m aws tests/integration/test_aws_smoke.py

Skipped unless STORAGE_MODE=s3 / WAREHOUSE_TYPE=redshift; CI deselects them (-m "not aws").
"""

from __future__ import annotations

import os
import uuid

import pytest

from src.common.config import Settings, load_settings
from src.common.storage import get_storage
from src.warehouse.connection import warehouse_connection
from src.warehouse.sql_runner import fetch_all

pytestmark = pytest.mark.aws


@pytest.fixture(scope="module")
def settings() -> Settings:
    return load_settings(os.environ)


def test_s3_write_read_delete(settings: Settings) -> None:
    """AC-008 path: the pipeline's storage layer reaches the real bucket."""
    if settings.storage_mode != "s3":
        pytest.skip("STORAGE_MODE is not s3")
    storage = get_storage(settings)
    key = f"raw/_smoke/{uuid.uuid4().hex}.txt"

    storage.write_text(key, "smoke")
    try:
        assert storage.read_bytes(key) == b"smoke"
        assert key in storage.list("raw/_smoke/")
    finally:
        storage.delete_prefix(key)
    assert not storage.exists(key)


def test_redshift_connection_and_dim_date(settings: Settings) -> None:
    """TLS connection with the Secrets Manager password; dim_date loaded by init-warehouse."""
    if settings.warehouse_type != "redshift":
        pytest.skip("WAREHOUSE_TYPE is not redshift")
    with warehouse_connection(settings) as conn:
        assert fetch_all(conn, "SELECT 1") == [(1,)]
        [(days,)] = fetch_all(conn, f"SELECT COUNT(*) FROM {settings.redshift_schema}.dim_date")
    assert days == 1095
