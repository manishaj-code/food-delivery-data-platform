"""Warehouse connections (spec 08, spec 09 §3.2, spec 11).

One psycopg2 code path for both engines: Redshift speaks the PostgreSQL wire protocol.
Redshift connections require TLS. The password comes from ``REDSHIFT_PASSWORD``
(local, untracked ``.env``) or from Secrets Manager (``REDSHIFT_SECRET_ARN``, AWS mode)
and is never logged or included in errors.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2

from src.common.config import Secret, Settings
from src.common.exceptions import ConfigError, WarehouseConnectionError
from src.common.secrets import get_database_credentials

CONNECT_TIMEOUT_SECONDS = 10


def resolve_credentials(settings: Settings) -> tuple[str, Secret]:
    """``(user, password)``: the explicit password wins; otherwise the Secrets Manager secret."""
    if settings.redshift_password:
        return settings.redshift_user, settings.redshift_password
    if settings.redshift_secret_arn:
        return get_database_credentials(settings.redshift_secret_arn, settings.aws_region)
    raise ConfigError(
        "No warehouse password configured; set REDSHIFT_PASSWORD or REDSHIFT_SECRET_ARN",
        variable="REDSHIFT_PASSWORD",
    )


def connection_options(settings: Settings) -> dict[str, Any]:
    """Connection parameters without credentials (safe to log)."""
    options: dict[str, Any] = {
        "host": settings.redshift_host,
        "port": settings.redshift_port,
        "dbname": settings.redshift_database,
        "connect_timeout": CONNECT_TIMEOUT_SECONDS,
        "application_name": "food-delivery-pipeline",
    }
    if settings.warehouse_type == "redshift":
        options["sslmode"] = "require"
    else:
        options["options"] = "-c timezone=UTC"  # Redshift sessions are UTC already
    return options


def get_connection(settings: Settings) -> Any:
    """Open a warehouse connection (transactions are managed by the caller)."""
    user, password = resolve_credentials(settings)
    options = connection_options(settings)
    try:
        return psycopg2.connect(user=user, password=password.get_secret_value(), **options)
    except psycopg2.OperationalError as exc:
        # libpq messages name host/user but never the password.
        message = " ".join(str(exc).split()) or type(exc).__name__
        raise WarehouseConnectionError(
            "Could not connect to the warehouse",
            host=options["host"],
            port=options["port"],
            database=options["dbname"],
            error=message,
        ) from None


@contextmanager
def warehouse_connection(settings: Settings, conn: Any = None) -> Iterator[Any]:
    """Use ``conn`` if given (tests, one shared connection), else open and close one."""
    if conn is not None:
        yield conn
        return
    conn = get_connection(settings)
    try:
        yield conn
    finally:
        conn.close()
