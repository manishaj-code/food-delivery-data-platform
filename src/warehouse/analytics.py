"""Power BI views and the 15 analytical queries (FR-060, FR-061; spec 02 §4, spec 08 §8).

Views hold the metric definitions of spec 02; the queries in ``sql/analytics/`` select from
them wherever a view already encodes the metric, so each definition exists once.

Demo usage::

    python -m src.warehouse.analytics --query 08_top_restaurants
    python -m src.warehouse.analytics --all
    python -m src.warehouse.analytics --monitoring      # pipeline_run_audit health queries
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from src.common.config import Settings, load_settings
from src.common.exceptions import ConfigError
from src.common.logging_config import configure_logging
from src.warehouse.connection import warehouse_connection
from src.warehouse.sql_runner import (
    SQL_DIR,
    render_sql,
    run_sql_file,
    transaction,
    translate_errors,
)

VIEWS = (
    "vw_daily_orders",
    "vw_daily_revenue",
    "vw_restaurant_performance",
    "vw_delivery_performance",
    "vw_customer_summary",
    "vw_payment_summary",
)
QUERIES = tuple(sorted(path.stem for path in (SQL_DIR / "analytics").glob("[0-9][0-9]_*.sql")))
# Pipeline health over ``pipeline_run_audit`` (spec 12 §4), e.g. ``monitoring/recent_runs``.
MONITORING_QUERIES = tuple(
    sorted(f"monitoring/{path.stem}" for path in (SQL_DIR / "analytics/monitoring").glob("*.sql"))
)


@dataclass(frozen=True)
class QueryResult:
    name: str
    columns: tuple[str, ...]
    rows: list[tuple]

    def as_dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, row, strict=True)) for row in self.rows]


def create_views(conn: Any, schema: str) -> None:
    """``CREATE OR REPLACE`` every Power BI view (after the tables exist)."""
    for view in VIEWS:
        with transaction(conn):
            run_sql_file(conn, f"analytics/views/{view}.sql", schema)


def run_query(conn: Any, schema: str, name: str) -> QueryResult:
    """Run ``sql/analytics/<name>.sql`` (e.g. ``08_top_restaurants``); only known names run."""
    if name not in QUERIES + MONITORING_QUERIES:
        available = [*QUERIES, *MONITORING_QUERIES]
        raise ConfigError("Unknown analytics query", query=name, available=available)
    sql = render_sql(f"analytics/{name}.sql", schema)
    with translate_errors(f"analytics query {name}"), conn.cursor() as cursor:
        cursor.execute(sql)
        columns = tuple(column.name for column in cursor.description)
        return QueryResult(name, columns, cursor.fetchall())


def format_result(result: QueryResult, limit: int = 25) -> str:
    """Plain-text table of the first ``limit`` rows."""
    shown = [tuple("" if value is None else str(value) for value in row) for row in result.rows]
    shown = shown[:limit]
    widths = [
        max([len(column)] + [len(row[i]) for row in shown])
        for i, column in enumerate(result.columns)
    ]

    def line(values: Sequence[str]) -> str:
        return "  ".join(value.ljust(width) for value, width in zip(values, widths, strict=True))

    lines = [
        f"-- {result.name} ({len(result.rows)} rows)",
        line(result.columns),
        line(["-" * width for width in widths]),
    ]
    lines += [line(row) for row in shown]
    if len(result.rows) > limit:
        lines.append(f"... {len(result.rows) - limit} more rows")
    return "\n".join(text.rstrip() for text in lines)


def run_analytics(settings: Settings, names: Sequence[str], conn: Any = None) -> list[QueryResult]:
    with warehouse_connection(settings, conn) as connection:
        return [run_query(connection, settings.redshift_schema, name) for name in names]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the analytics SQL against the warehouse.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--query",
        choices=QUERIES + MONITORING_QUERIES,
        help="query name, e.g. 08_top_restaurants or monitoring/recent_runs",
    )
    group.add_argument("--all", action="store_true", help="run all 15 queries")
    group.add_argument(
        "--monitoring", action="store_true", help="run the pipeline monitoring queries"
    )
    parser.add_argument("--limit", type=int, default=25, help="rows to print per query")
    args = parser.parse_args(argv)

    settings = load_settings()
    configure_logging(settings.log_level)
    names = QUERIES if args.all else MONITORING_QUERIES if args.monitoring else (args.query,)
    for result in run_analytics(settings, names):
        sys.stdout.write(format_result(result, args.limit) + "\n\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
