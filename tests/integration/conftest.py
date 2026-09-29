"""Warehouse fixtures: a fresh PostgreSQL schema per test module (WAREHOUSE_TYPE=postgres).

The Compose ``postgres`` service provides the database; credentials come from the
environment (``.env``). Tests are skipped with a clear message when it is not reachable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pytest

from src.common.config import Settings, load_settings
from src.common.exceptions import ConfigError, WarehouseConnectionError
from src.warehouse.connection import get_connection
from src.warehouse.init_warehouse import init_warehouse
from src.warehouse.sql_runner import fetch_all


@dataclass
class Warehouse:
    settings: Settings
    conn: Any

    @property
    def schema(self) -> str:
        return self.settings.redshift_schema

    def query(self, sql: str, params: Any = None) -> list[tuple]:
        """Run ``sql`` with ``{schema}`` filled in."""
        return fetch_all(self.conn, sql.replace("{schema}", self.schema), params)

    def count(self, table: str) -> int:
        return self.query(f"SELECT COUNT(*) FROM {{schema}}.{table}")[0][0]

    def execute(self, sql: str, params: Any = None) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(sql.replace("{schema}", self.schema), params)


def _drop_schema(conn: Any, schema: str) -> None:
    conn.rollback()
    with conn.cursor() as cursor:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.commit()


@pytest.fixture(scope="module")
def warehouse(request: pytest.FixtureRequest) -> Warehouse:
    """Initialised warehouse in schema ``test_<module>``, dropped afterwards."""
    module = request.module.__name__.rsplit(".", 1)[-1].removeprefix("test_")
    settings = load_settings(
        {**os.environ, "WAREHOUSE_TYPE": "postgres", "REDSHIFT_SCHEMA": f"test_{module}"}
    )
    try:
        conn = get_connection(settings)
    except (WarehouseConnectionError, ConfigError) as exc:
        pytest.skip(f"PostgreSQL warehouse not reachable ({exc}); start it with docker compose")
    _drop_schema(conn, settings.redshift_schema)
    init_warehouse(settings, conn)
    yield Warehouse(settings, conn)
    _drop_schema(conn, settings.redshift_schema)
    conn.close()
