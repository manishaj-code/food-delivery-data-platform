"""Centralised, environment-based configuration (docs/spec/11-security-specification.md §4).

All settings come from environment variables. Secrets are wrapped so they never
appear in ``repr`` or log output.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from src.common.constants import DEFAULT_DQ_MIN_QUALITY_SCORE
from src.common.exceptions import ConfigError

STORAGE_MODES = ("local", "s3")
WAREHOUSE_TYPES = ("postgres", "redshift")
COPY_AUTH_MODES = ("iam_role", "session")
_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_MEMORY_PATTERN = re.compile(r"^\d+[mg]$")


class Secret:
    """Holds a sensitive value; masked in repr/str so it cannot leak into logs."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def get_secret_value(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('**********')"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)


@dataclass(frozen=True)
class Settings:
    pipeline_env: str
    log_level: str
    storage_mode: str
    local_lake_path: Path
    source_data_path: Path
    aws_region: str
    s3_bucket: str | None
    warehouse_type: str
    redshift_host: str
    redshift_port: int
    redshift_database: str
    redshift_schema: str
    redshift_user: str
    redshift_password: Secret | None
    redshift_secret_arn: str | None
    redshift_iam_role_arn: str | None
    redshift_copy_auth: str
    dq_min_quality_score: float
    spark_driver_memory: str
    metrics_enabled: bool


def _get(env: Mapping[str, str], name: str, default: str | None = None) -> str | None:
    value = env.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _choice(env: Mapping[str, str], name: str, default: str, allowed: tuple[str, ...]) -> str:
    value = (_get(env, name, default) or default).lower()
    if value not in allowed:
        raise ConfigError(f"{name} must be one of {allowed}", variable=name)
    return value


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _get(env, name, str(default))
    try:
        return int(raw)  # type: ignore[arg-type]
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer", variable=name) from exc


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = _get(env, name, str(default))
    try:
        return float(raw)  # type: ignore[arg-type]
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number", variable=name) from exc


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = (_get(env, name, str(default)) or "").lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"{name} must be a boolean", variable=name)


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build and validate settings from ``env`` (defaults to ``os.environ``)."""
    env = os.environ if env is None else env

    password = _get(env, "REDSHIFT_PASSWORD")
    settings = Settings(
        pipeline_env=_get(env, "PIPELINE_ENV", "local") or "local",
        log_level=(_get(env, "LOG_LEVEL", "INFO") or "INFO").upper(),
        storage_mode=_choice(env, "STORAGE_MODE", "local", STORAGE_MODES),
        local_lake_path=Path(_get(env, "LOCAL_LAKE_PATH", "lake") or "lake"),
        source_data_path=Path(_get(env, "SOURCE_DATA_PATH", "data/generated") or "data/generated"),
        aws_region=_get(env, "AWS_REGION", "ap-south-1") or "ap-south-1",
        s3_bucket=_get(env, "S3_BUCKET"),
        warehouse_type=_choice(env, "WAREHOUSE_TYPE", "postgres", WAREHOUSE_TYPES),
        redshift_host=_get(env, "REDSHIFT_HOST", "postgres") or "postgres",
        redshift_port=_int(env, "REDSHIFT_PORT", 5432),
        redshift_database=_get(env, "REDSHIFT_DATABASE", "warehouse") or "warehouse",
        redshift_schema=(_get(env, "REDSHIFT_SCHEMA", "food_delivery") or "").lower(),
        redshift_user=_get(env, "REDSHIFT_USER", "warehouse_user") or "warehouse_user",
        redshift_password=Secret(password) if password else None,
        redshift_secret_arn=_get(env, "REDSHIFT_SECRET_ARN"),
        redshift_iam_role_arn=_get(env, "REDSHIFT_IAM_ROLE_ARN"),
        redshift_copy_auth=_choice(env, "REDSHIFT_COPY_AUTH", "iam_role", COPY_AUTH_MODES),
        dq_min_quality_score=_float(env, "DQ_MIN_QUALITY_SCORE", DEFAULT_DQ_MIN_QUALITY_SCORE),
        spark_driver_memory=(_get(env, "SPARK_DRIVER_MEMORY", "1g") or "1g").lower(),
        metrics_enabled=_bool(env, "METRICS_ENABLED", False),
    )
    _validate(settings)
    return settings


def _validate(settings: Settings) -> None:
    if settings.storage_mode == "s3" and not settings.s3_bucket:
        raise ConfigError("S3_BUCKET is required when STORAGE_MODE=s3", variable="S3_BUCKET")
    if not _IDENTIFIER_PATTERN.match(settings.redshift_schema):
        raise ConfigError(
            "REDSHIFT_SCHEMA must be a lowercase SQL identifier", variable="REDSHIFT_SCHEMA"
        )
    if not 0.0 <= settings.dq_min_quality_score <= 100.0:
        raise ConfigError(
            "DQ_MIN_QUALITY_SCORE must be between 0 and 100", variable="DQ_MIN_QUALITY_SCORE"
        )
    if not _MEMORY_PATTERN.match(settings.spark_driver_memory):
        raise ConfigError(
            "SPARK_DRIVER_MEMORY must look like 512m or 1g", variable="SPARK_DRIVER_MEMORY"
        )
    if settings.warehouse_type == "redshift":
        if not (settings.redshift_password or settings.redshift_secret_arn):
            raise ConfigError(
                "REDSHIFT_PASSWORD or REDSHIFT_SECRET_ARN is required when WAREHOUSE_TYPE=redshift",
                variable="REDSHIFT_SECRET_ARN",
            )
        if settings.redshift_copy_auth == "iam_role" and not settings.redshift_iam_role_arn:
            raise ConfigError(
                "REDSHIFT_IAM_ROLE_ARN is required when REDSHIFT_COPY_AUTH=iam_role",
                variable="REDSHIFT_IAM_ROLE_ARN",
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings, loaded once from the environment."""
    return load_settings()
