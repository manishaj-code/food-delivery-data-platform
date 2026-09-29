"""Create the warehouse objects and populate ``dim_date`` (FR-050, FR-051, FR-056, FR-061).

Idempotent: every table is ``CREATE … IF NOT EXISTS``, ``dim_date`` only receives missing
dates, and the Power BI views are ``CREATE OR REPLACE``d, so running it again changes nothing.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.common.config import Settings
from src.warehouse.analytics import create_views
from src.warehouse.connection import warehouse_connection
from src.warehouse.sql_runner import run_sql_file, transaction

logger = logging.getLogger(__name__)

DDL_FILES = (
    "01_schema.sql",
    "02_dimensions.sql",
    "03_facts.sql",
    "04_staging.sql",
    "05_audit.sql",
)  # 06_powerbi_reader.sql (Redshift only) is run separately by an admin
DIM_DATE_START = date(2025, 1, 1)
DIM_DATE_END = date(2027, 12, 31)


def populate_dim_date(
    conn: Any, schema: str, start: date = DIM_DATE_START, end: date = DIM_DATE_END
) -> int:
    """Insert missing calendar days in ``[start, end]``; returns the number added."""
    with transaction(conn):
        [inserted] = run_sql_file(
            conn,
            "warehouse/populate_dim_date.sql",
            schema,
            {"start_date": start, "end_date": end},
        )
    return inserted


def init_warehouse(settings: Settings, conn: Any = None) -> int:
    """Run the DDL, fill ``dim_date``, and create the views; returns the dim_date rows added."""
    schema = settings.redshift_schema
    with warehouse_connection(settings, conn) as connection:
        for name in DDL_FILES:
            with transaction(connection):
                run_sql_file(connection, f"ddl/{settings.warehouse_type}/{name}", schema)
        added = populate_dim_date(connection, schema)
        create_views(connection, schema)
    logger.info(
        "Warehouse initialised (warehouse_type=%s, schema=%s, dim_date rows added=%d)",
        settings.warehouse_type,
        schema,
        added,
    )
    return added
