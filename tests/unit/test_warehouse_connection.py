"""Warehouse credentials and connection errors (FR-135, spec 09 §3.5, spec 11)."""

from __future__ import annotations

import json
import logging
import socket

import pytest

from src.common import secrets
from src.common.config import load_settings
from src.common.exceptions import ConfigError, WarehouseConnectionError
from src.warehouse.connection import connection_options, get_connection, resolve_credentials
from tests.conftest import TEST_REGION

pytestmark = pytest.mark.unit

PASSWORD = "Sup3r-Secret-Pa55"


@pytest.fixture
def secretsmanager(monkeypatch: pytest.MonkeyPatch):
    moto = pytest.importorskip("moto")
    import boto3

    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": TEST_REGION,
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    secrets.clear_cache()
    with moto.mock_aws():
        yield boto3.client("secretsmanager", region_name=TEST_REGION)
    secrets.clear_cache()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_password_from_secrets_manager(secretsmanager) -> None:
    arn = secretsmanager.create_secret(
        Name="redshift-admin",
        SecretString=json.dumps({"username": "admin", "password": PASSWORD}),
    )["ARN"]
    settings = load_settings(
        {"REDSHIFT_SECRET_ARN": arn, "AWS_REGION": TEST_REGION, "REDSHIFT_PASSWORD": ""}
    )

    user, password = resolve_credentials(settings)

    assert user == "admin"
    assert password.get_secret_value() == PASSWORD
    assert PASSWORD not in repr(password)


def test_malformed_secret_does_not_leak_its_value(secretsmanager) -> None:
    arn = secretsmanager.create_secret(Name="broken", SecretString=f"not json {PASSWORD}")["ARN"]
    settings = load_settings({"REDSHIFT_SECRET_ARN": arn, "AWS_REGION": TEST_REGION})

    with pytest.raises(ConfigError) as excinfo:
        resolve_credentials(settings)

    assert PASSWORD not in str(excinfo.value)
    assert excinfo.value.__cause__ is None and excinfo.value.__suppress_context__


def test_missing_secret_is_a_config_error(secretsmanager) -> None:
    settings = load_settings({"REDSHIFT_SECRET_ARN": "missing", "AWS_REGION": TEST_REGION})

    with pytest.raises(ConfigError, match="Could not read secret"):
        resolve_credentials(settings)


def test_no_password_configured() -> None:
    with pytest.raises(ConfigError, match="REDSHIFT_PASSWORD or REDSHIFT_SECRET_ARN"):
        resolve_credentials(load_settings({}))


def test_redshift_requires_tls() -> None:
    settings = load_settings(
        {"WAREHOUSE_TYPE": "redshift", "REDSHIFT_PASSWORD": "x", "REDSHIFT_COPY_AUTH": "session"}
    )
    options = connection_options(settings)

    assert options["sslmode"] == "require"
    assert "password" not in options


def test_connection_failure_never_exposes_the_password(caplog: pytest.LogCaptureFixture) -> None:
    settings = load_settings(
        {
            "REDSHIFT_HOST": "127.0.0.1",
            "REDSHIFT_PORT": str(_free_port()),  # nothing listens here
            "REDSHIFT_PASSWORD": PASSWORD,
        }
    )

    with caplog.at_level(logging.DEBUG), pytest.raises(WarehouseConnectionError) as excinfo:
        get_connection(settings)

    assert excinfo.value.retryable
    assert excinfo.value.context["host"] == "127.0.0.1"
    assert PASSWORD not in str(excinfo.value)
    assert PASSWORD not in repr(excinfo.value.context)
    assert PASSWORD not in caplog.text
