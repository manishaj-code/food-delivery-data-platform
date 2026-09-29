"""AWS Secrets Manager access (docs/spec/09-aws-infrastructure-specification.md §3.5).

Used in AWS mode for the Redshift admin credentials that Redshift Serverless stores in
Secrets Manager (``manage_admin_password``). Each secret is fetched once per process and
cached in memory; its values are wrapped in ``Secret`` and never logged.
"""

from __future__ import annotations

import json
from functools import lru_cache

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.common.config import Secret
from src.common.exceptions import ConfigError


@lru_cache(maxsize=8)
def _secret_string(secret_arn: str, region: str) -> str:
    client = boto3.client("secretsmanager", region_name=region)
    try:
        return client.get_secret_value(SecretId=secret_arn)["SecretString"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        raise ConfigError("Could not read secret", secret_arn=secret_arn, error=code) from exc
    except BotoCoreError as exc:
        raise ConfigError(
            "Could not read secret", secret_arn=secret_arn, error=type(exc).__name__
        ) from exc


def get_database_credentials(secret_arn: str, region: str) -> tuple[str, Secret]:
    """``(username, password)`` from a JSON secret with ``username`` and ``password`` keys."""
    try:
        values = json.loads(_secret_string(secret_arn, region))
        return values["username"], Secret(values["password"])
    except (ValueError, KeyError, TypeError):
        # No exception chaining: a parse error can carry the secret text.
        raise ConfigError(
            "Secret must be JSON with 'username' and 'password'", secret_arn=secret_arn
        ) from None


def clear_cache() -> None:
    """Forget cached secrets (tests, or after a rotation)."""
    _secret_string.cache_clear()
