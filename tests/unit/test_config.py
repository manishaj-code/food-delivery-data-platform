"""Tests for src.common.config (FR-131, FR-135)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.common.config import Secret, load_settings
from src.common.exceptions import ConfigError

pytestmark = pytest.mark.unit

REDSHIFT_ENV = {
    "WAREHOUSE_TYPE": "redshift",
    "REDSHIFT_HOST": "wg.example.ap-south-1.redshift-serverless.amazonaws.com",
    "REDSHIFT_PORT": "5439",
    "REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:ap-south-1:123456789012:secret:rs",
    "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123456789012:role/copy",
}


def test_defaults_are_local_mode() -> None:
    settings = load_settings({})

    assert settings.storage_mode == "local"
    assert settings.warehouse_type == "postgres"
    assert settings.local_lake_path == Path("lake")
    assert settings.redshift_schema == "food_delivery"
    assert settings.dq_min_quality_score == 95.0
    assert settings.spark_driver_memory == "1g"
    assert settings.metrics_enabled is False
    assert settings.redshift_password is None


def test_blank_values_fall_back_to_defaults() -> None:
    assert load_settings({"STORAGE_MODE": "  ", "LOG_LEVEL": ""}).storage_mode == "local"


@pytest.mark.parametrize(
    ("env", "variable"),
    [
        ({"STORAGE_MODE": "ftp"}, "STORAGE_MODE"),
        ({"STORAGE_MODE": "s3"}, "S3_BUCKET"),
        ({"WAREHOUSE_TYPE": "snowflake"}, "WAREHOUSE_TYPE"),
        ({"REDSHIFT_PORT": "abc"}, "REDSHIFT_PORT"),
        ({"REDSHIFT_SCHEMA": "food-delivery;drop"}, "REDSHIFT_SCHEMA"),
        ({"DQ_MIN_QUALITY_SCORE": "120"}, "DQ_MIN_QUALITY_SCORE"),
        ({"SPARK_DRIVER_MEMORY": "lots"}, "SPARK_DRIVER_MEMORY"),
        ({"METRICS_ENABLED": "maybe"}, "METRICS_ENABLED"),
        ({"WAREHOUSE_TYPE": "redshift"}, "REDSHIFT_SECRET_ARN"),
    ],
)
def test_invalid_configuration_names_the_variable(env: dict[str, str], variable: str) -> None:
    with pytest.raises(ConfigError) as error:
        load_settings(env)

    assert error.value.context["variable"] == variable


def test_redshift_iam_role_required_for_iam_role_copy() -> None:
    env = {**REDSHIFT_ENV, "REDSHIFT_IAM_ROLE_ARN": ""}

    with pytest.raises(ConfigError, match="REDSHIFT_IAM_ROLE_ARN"):
        load_settings(env)


def test_sandbox_session_copy_auth_does_not_need_iam_role() -> None:
    env = {**REDSHIFT_ENV, "REDSHIFT_IAM_ROLE_ARN": "", "REDSHIFT_COPY_AUTH": "session"}

    settings = load_settings(env)

    assert settings.redshift_copy_auth == "session"
    assert settings.redshift_iam_role_arn is None


def test_s3_mode_with_bucket() -> None:
    settings = load_settings({"STORAGE_MODE": "S3", "S3_BUCKET": "food-delivery-data-dev"})

    assert settings.storage_mode == "s3"
    assert settings.s3_bucket == "food-delivery-data-dev"


def test_password_is_masked_everywhere() -> None:
    settings = load_settings({"REDSHIFT_PASSWORD": "super-secret-pw"})

    assert settings.redshift_password is not None
    assert settings.redshift_password.get_secret_value() == "super-secret-pw"
    assert "super-secret-pw" not in repr(settings)
    assert "super-secret-pw" not in str(settings.redshift_password)


def test_secret_equality() -> None:
    assert Secret("a") == Secret("a")
    assert Secret("a") != Secret("b")
