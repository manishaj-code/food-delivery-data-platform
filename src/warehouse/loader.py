"""Load one run date's processed partitions into the warehouse (FR-052 – FR-055, FR-083, FR-092).

Per table, in dependency order (spec 08 §7):

1. ``DELETE FROM stg_x`` in its own transaction (Redshift ``TRUNCATE`` would commit
   implicitly, so it is not used).
2. Load the processed partition into ``stg_x`` and check its row count equals the
   partition's ``_manifest.json`` ``row_count``.
3. One transaction: UPDATE existing business keys (stale-batch guard), INSERT new ones,
   then confirm every staged key is present in the target. Any error rolls back the table.

Staging keeps the batch after the load; post-load checks read it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any

from src.common.config import Settings
from src.common.constants import (
    CUSTOMERS,
    DELIVERY,
    DELIVERY_PARTNERS,
    ORDERS,
    PAYMENTS,
    RESTAURANTS,
)
from src.common.exceptions import WarehouseLoadError
from src.common.logging_config import log_context
from src.common.paths import Zone, partition_path
from src.common.storage import Storage
from src.transformation.publish import read_manifest
from src.transformation.schemas import DAILY_ORDER_METRICS
from src.warehouse.connection import warehouse_connection
from src.warehouse.init_warehouse import populate_dim_date
from src.warehouse.sql_runner import (
    execute,
    fetch_value,
    run_sql_file,
    transaction,
    translate_errors,
)
from src.warehouse.staging_loader import StagingLoader, get_staging_loader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WarehouseTable:
    target: str
    staging: str
    dataset: str  # processed dataset loaded into staging
    business_key: str | None = None  # None: staging only (no upsert)

    @property
    def upsert_sql(self) -> str:
        return f"warehouse/upsert_{self.target}.sql"


LOAD_ORDER: tuple[WarehouseTable, ...] = (
    WarehouseTable("dim_customer", "stg_customer", CUSTOMERS, "customer_id"),
    WarehouseTable("dim_restaurant", "stg_restaurant", RESTAURANTS, "restaurant_id"),
    WarehouseTable(
        "dim_delivery_partner", "stg_delivery_partner", DELIVERY_PARTNERS, "delivery_partner_id"
    ),
    WarehouseTable("fact_order", "stg_order", ORDERS, "order_id"),
    WarehouseTable("fact_payment", "stg_payment", PAYMENTS, "payment_id"),
    WarehouseTable("fact_delivery", "stg_delivery", DELIVERY, "delivery_id"),
    # PySpark's metrics are only staged, for the AOV reconciliation check (WQ-005).
    WarehouseTable("stg_daily_order_metrics", "stg_daily_order_metrics", DAILY_ORDER_METRICS),
)


@dataclass
class TableLoadResult:
    table: str
    dataset: str
    status: str  # SUCCESS | NO_DATA
    records_in: int  # rows in the processed partition (manifest)
    records_staged: int
    rows_updated: int
    rows_inserted: int
    duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def manifest_row_count(storage: Storage, dataset: str, run_date: date) -> int:
    manifest = read_manifest(storage, dataset, run_date)
    if manifest is None:
        raise WarehouseLoadError(
            "Processed partition is not published (no _manifest.json); run publish first",
            dataset=dataset,
            key=partition_path(Zone.PROCESSED, dataset, run_date),
        )
    return manifest["row_count"]


def staging_row_count(conn: Any, schema: str, staging: str) -> int:
    return fetch_value(conn, f"SELECT COUNT(*) FROM {schema}.{staging}")


def stage_table(
    conn: Any,
    loader: StagingLoader,
    storage: Storage,
    schema: str,
    table: WarehouseTable,
    run_date: date,
) -> tuple[int, int]:
    """Replace ``stg_x`` with the run date's partition; returns (manifest rows, staged rows)."""
    expected = manifest_row_count(storage, table.dataset, run_date)
    with transaction(conn), translate_errors(f"clearing {table.staging}"):
        execute(conn, f"DELETE FROM {schema}.{table.staging}")
    with transaction(conn):
        loader.load(conn, table.staging, partition_path(Zone.PROCESSED, table.dataset, run_date))
    staged = staging_row_count(conn, schema, table.staging)
    if staged != expected:
        raise WarehouseLoadError(
            "Staging row count does not match the processed manifest",
            table=table.staging,
            expected=expected,
            actual=staged,
        )
    return expected, staged


def upsert_table(
    conn: Any, schema: str, table: WarehouseTable, loaded_at: datetime
) -> tuple[int, int]:
    """UPDATE-then-INSERT from staging in one transaction; returns (updated, inserted)."""
    key = table.business_key
    with transaction(conn):
        updated, inserted = run_sql_file(conn, table.upsert_sql, schema, {"loaded_at": loaded_at})
        missing = fetch_value(
            conn,
            f"SELECT COUNT(*) FROM {schema}.{table.staging} AS s "
            f"LEFT JOIN {schema}.{table.target} AS t ON t.{key} = s.{key} "
            f"WHERE t.{key} IS NULL",
        )
        if missing:
            raise WarehouseLoadError(
                "Staged rows missing from the target after the upsert",
                table=table.target,
                missing=missing,
            )
    return updated, inserted


def load_table(
    conn: Any,
    loader: StagingLoader,
    storage: Storage,
    schema: str,
    table: WarehouseTable,
    run_date: date,
    loaded_at: datetime,
) -> TableLoadResult:
    started = time.perf_counter()
    expected, staged = stage_table(conn, loader, storage, schema, table, run_date)
    updated = inserted = 0
    if table.business_key is not None:
        updated, inserted = upsert_table(conn, schema, table, loaded_at)
    result = TableLoadResult(
        table=table.target,
        dataset=table.dataset,
        status="SUCCESS" if staged else "NO_DATA",
        records_in=expected,
        records_staged=staged,
        rows_updated=updated,
        rows_inserted=inserted,
        duration_seconds=round(time.perf_counter() - started, 2),
    )
    logger.info(
        "Loaded %s: staged=%d updated=%d inserted=%d",
        table.target,
        staged,
        updated,
        inserted,
    )
    return result


def load_warehouse(
    settings: Settings,
    storage: Storage,
    run_date: date,
    run_id: str,
    conn: Any = None,
) -> list[TableLoadResult]:
    """Load every table for ``run_date`` in dependency order (spec 08 §7)."""
    schema = settings.redshift_schema
    loader = get_staging_loader(settings, storage)
    loaded_at = datetime.now(UTC).replace(tzinfo=None)  # warehouse TIMESTAMP columns are UTC
    with (
        log_context(run_id=run_id, run_date=run_date.isoformat()),
        warehouse_connection(settings, conn) as connection,
    ):
        logger.info("Starting warehouse load (warehouse_type=%s)", settings.warehouse_type)
        populate_dim_date(connection, schema)  # ensure; no-op once initialised
        results = []
        for table in LOAD_ORDER:
            with log_context(dataset=table.dataset):
                results.append(
                    load_table(connection, loader, storage, schema, table, run_date, loaded_at)
                )
        logger.info("Redshift load completed (warehouse_type=%s)", settings.warehouse_type)
        return results
