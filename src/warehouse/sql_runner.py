"""Run the SQL files in ``sql/`` against the warehouse (spec 08 §7).

Placeholders in ``{braces}`` are identifiers (schema, table), substituted only after
validation; values always go through driver parameters (``%(name)s``), never into the
SQL text. Full-line ``--`` comments are removed before execution, so a comment that
mentions a parameter cannot break parameter binding.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import psycopg2

from src.common.exceptions import (
    ConfigError,
    WarehouseConnectionError,
    WarehouseLoadError,
)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_PLACEHOLDER = re.compile(r"\{([a-z_]+)\}")
_STATEMENT_END = re.compile(r";\s*(?:\n|$)")


def render_sql(
    relative_path: str,
    schema: str,
    fragments: Mapping[str, str] | None = None,
    **identifiers: str,
) -> str:
    """SQL text of ``sql/<relative_path>`` with placeholders filled.

    ``identifiers`` (schema, table, …) must be plain lowercase SQL identifiers.
    ``fragments`` are fixed SQL snippets chosen by code, never by user input.
    """
    text = (SQL_DIR / relative_path).read_text(encoding="utf-8")
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    for name, value in {"schema": schema, **identifiers}.items():
        if not _IDENTIFIER.match(value):
            raise ConfigError("Invalid SQL identifier", placeholder=name, value=value)
        text = text.replace("{" + name + "}", value)
    for name, value in (fragments or {}).items():
        text = text.replace("{" + name + "}", value)
    missing = _PLACEHOLDER.findall(text)
    if missing:
        raise ConfigError("Unfilled SQL placeholders", file=relative_path, placeholders=missing)
    return text


def split_statements(sql: str) -> list[str]:
    """Statements of a SQL script (our files never put ``;`` inside string literals)."""
    return [statement.strip() for statement in _STATEMENT_END.split(sql) if statement.strip()]


@contextmanager
def translate_errors(action: str, **context: Any) -> Iterator[None]:
    """Map driver errors to pipeline errors: connection problems are retryable, SQL errors not.

    Only the primary message and SQLSTATE are kept: libpq's DETAIL lines can quote row
    values, which do not belong in logs.
    """
    try:
        yield
    except psycopg2.OperationalError as exc:
        raise WarehouseConnectionError(
            f"Warehouse connection failed during {action}", error=_primary(exc), **context
        ) from None
    except psycopg2.Error as exc:
        raise WarehouseLoadError(
            f"Warehouse SQL failed during {action}",
            error=_primary(exc),
            sqlstate=exc.pgcode,
            **context,
        ) from None


def _primary(exc: psycopg2.Error) -> str:
    diag = getattr(exc, "diag", None)
    message = getattr(diag, "message_primary", None) or str(exc)
    return message.strip().splitlines()[0] if message.strip() else type(exc).__name__


def execute(conn: Any, sql: str, params: Mapping[str, Any] | None = None) -> list[int]:
    """Execute each statement of ``sql``; returns the row count of each."""
    counts = []
    with conn.cursor() as cursor:
        for statement in split_statements(sql):
            cursor.execute(statement, params)
            counts.append(cursor.rowcount)
    return counts


def run_sql_file(
    conn: Any,
    relative_path: str,
    schema: str,
    params: Mapping[str, Any] | None = None,
    **identifiers: str,
) -> list[int]:
    """Render and execute one SQL file (inside the caller's transaction)."""
    sql = render_sql(relative_path, schema, **identifiers)
    with translate_errors(relative_path):
        return execute(conn, sql, params)


def fetch_all(conn: Any, sql: str, params: Mapping[str, Any] | None = None) -> list[tuple]:
    with translate_errors("query"), conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def fetch_value(conn: Any, sql: str, params: Mapping[str, Any] | None = None) -> Any:
    return fetch_all(conn, sql, params)[0][0]


@contextmanager
def transaction(conn: Any) -> Iterator[Any]:
    """Commit on success, roll back on any error (FR-055)."""
    try:
        yield conn
        with translate_errors("commit"):
            conn.commit()
    except BaseException:
        with suppress(psycopg2.Error):
            conn.rollback()
        raise
