"""Integration fixtures.

``warehouse``: a fresh PostgreSQL schema per test module (WAREHOUSE_TYPE=postgres). The
Compose ``postgres`` service (or the CI service container) provides the database;
credentials come from the environment (``.env``). Tests are skipped with a clear message
when it is not reachable.

``pipeline_runs``: the end-to-end CLI pipeline runs shared by the e2e, idempotency, and
incremental tests (the expensive part runs once per session).
"""

from __future__ import annotations

import logging
import os
import shutil
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.cli import run_pipeline
from src.common.config import Settings, load_settings
from src.common.exceptions import ConfigError, WarehouseConnectionError
from src.common.logging_config import LOG_FORMAT, RunContextFilter
from src.common.paths import TEMP_ROOT
from src.common.storage import LocalStorage
from src.pipeline import steps
from src.warehouse.connection import get_connection
from src.warehouse.init_warehouse import init_warehouse
from src.warehouse.loader import LOAD_ORDER
from src.warehouse.sql_runner import fetch_all
from tests.sample_lake import DAILY_RUN, HISTORICAL_RUN, SAMPLE_DIR


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


@contextmanager
def fresh_warehouse(schema: str) -> Iterator[Warehouse]:
    """Initialised warehouse in ``schema``, dropped afterwards (skips if Postgres is down)."""
    env = {**os.environ, "WAREHOUSE_TYPE": "postgres", "REDSHIFT_SCHEMA": schema}
    settings = load_settings(env)
    try:
        conn = get_connection(settings)
    except (WarehouseConnectionError, ConfigError) as exc:
        pytest.skip(f"PostgreSQL warehouse not reachable ({exc}); start it with docker compose")
    try:
        _drop_schema(conn, schema)
        init_warehouse(settings, conn)
        yield Warehouse(settings, conn)
        _drop_schema(conn, schema)
    finally:
        conn.close()


@pytest.fixture(scope="module")
def warehouse(request: pytest.FixtureRequest) -> Iterator[Warehouse]:
    """Initialised warehouse in schema ``test_<module>``, dropped afterwards."""
    module = request.module.__name__.rsplit(".", 1)[-1].removeprefix("test_")
    with fresh_warehouse(f"test_{module}") as warehouse:
        yield warehouse


def business_rows(warehouse: Warehouse) -> dict[str, list[tuple]]:
    """Every dimension/fact row, all columns except ``updated_at``, ordered by business key."""
    snapshot = {}
    for table in LOAD_ORDER:
        if not table.business_key:
            continue
        columns = [
            name
            for (name,) in warehouse.query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (warehouse.schema, table.target),
            )
            if name != "updated_at"
        ]
        snapshot[table.target] = warehouse.query(
            f"SELECT {', '.join(columns)} FROM {{schema}}.{table.target} "
            f"ORDER BY {table.business_key}"
        )
    return snapshot


# --------------------------------------------------------------------------- end to end


E2E_SCHEMA = "test_pipeline_e2e"
BROKEN_DATE = date(2026, 9, 2)
# name -> (run_date, load_type, run_id), run in this order through the CLI runner
E2E_RUNS = {
    "historical": (HISTORICAL_RUN, "historical", "cli__test_historical"),
    "daily": (DAILY_RUN, "incremental", "cli__test_daily"),
    "rerun": (DAILY_RUN, "incremental", "cli__test_daily_rerun"),  # same date again
    "broken": (BROKEN_DATE, "incremental", "cli__test_broken"),  # orders header broken
}


class LogCollector(logging.Handler):
    """Formatted INFO+ lines, with run context, as the pipeline would print them."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.setFormatter(logging.Formatter(LOG_FORMAT))
        self.addFilter(RunContextFilter())
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


@dataclass
class Snapshot:
    """Lake file counts per directory (run-scoped ``_tmp/`` excluded) and warehouse rows."""

    lake_files: dict[str, int]
    table_counts: dict[str, int]
    rows: dict[str, list[tuple]]


def snapshot(lake: Path, warehouse: Warehouse) -> Snapshot:
    files = Counter(
        path.parent.relative_to(lake).as_posix()
        for path in lake.rglob("*")
        if path.is_file() and path.relative_to(lake).parts[0] != TEMP_ROOT
    )
    rows = business_rows(warehouse)
    return Snapshot(dict(files), {table: len(values) for table, values in rows.items()}, rows)


@dataclass
class PipelineRuns:
    warehouse: Warehouse
    lake: Path
    storage: LocalStorage
    exit_codes: dict[str, int]
    log_lines: list[str]
    after_daily: Snapshot
    after_rerun: Snapshot


@pytest.fixture(scope="session")
def pipeline_runs(spark, tmp_path_factory: pytest.TempPathFactory) -> Iterator[PipelineRuns]:
    """The whole pipeline through the CLI runner (no Airflow) on ``data/sample``.

    Historical load -> daily run -> the same daily run again (idempotency) -> a daily
    run whose orders file has a broken header (must stop at ingest_orders).
    """
    from src.common import spark as spark_module

    source = tmp_path_factory.mktemp("e2e_source")
    shutil.copytree(SAMPLE_DIR, source, dirs_exist_ok=True)
    broken = source / "orders" / f"orders_{BROKEN_DATE.isoformat()}.csv"
    lines = broken.read_text(encoding="utf-8").splitlines()
    broken.write_text("\n".join(["order_id,oops", *lines[1:]]) + "\n", encoding="utf-8")
    lake = tmp_path_factory.mktemp("e2e_lake")

    with fresh_warehouse(E2E_SCHEMA) as warehouse, pytest.MonkeyPatch.context() as patch:
        settings = load_settings(
            {
                **os.environ,
                "WAREHOUSE_TYPE": "postgres",
                "REDSHIFT_SCHEMA": E2E_SCHEMA,
                "LOCAL_LAKE_PATH": str(lake),
                "SOURCE_DATA_PATH": str(source),
            }
        )
        patch.setattr(steps, "_settings", lambda: settings)
        collect = LogCollector()
        root = logging.getLogger()
        root.addHandler(collect)
        level = root.level
        root.setLevel(logging.INFO)
        # The steps' get_spark() reuses the test session but would set production's
        # shuffle partitions on it (a speed setting only) for every later Spark test.
        patch.setattr(
            spark_module, "SHUFFLE_PARTITIONS", spark.conf.get("spark.sql.shuffle.partitions")
        )
        exit_codes, snapshots = {}, {}
        try:
            for name, run in E2E_RUNS.items():
                exit_codes[name] = run_pipeline(*run)
                if name in ("daily", "rerun"):
                    snapshots[name] = snapshot(lake, warehouse)
        finally:
            root.removeHandler(collect)
            root.setLevel(level)
        yield PipelineRuns(
            warehouse,
            lake,
            LocalStorage(lake),
            exit_codes,
            collect.lines,
            snapshots["daily"],
            snapshots["rerun"],
        )
