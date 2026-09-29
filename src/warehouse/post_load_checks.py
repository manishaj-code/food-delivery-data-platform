"""Post-load warehouse checks WQ-001 – WQ-006 (FR-026, spec 06 §6).

Each SQL check returns ``(object_name, failed_count)`` rows; a check passes when every
count is 0. WQ-001 compares staging with the processed manifests, so it runs in Python.
A failed ERROR check raises ``DataQualityThresholdError``; WARN checks are only logged.
The checks only read, so they never change the warehouse.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from src.common.config import Settings
from src.common.constants import SEVERITY_ERROR as ERROR
from src.common.constants import SEVERITY_WARN as WARN
from src.common.exceptions import DataQualityThresholdError
from src.common.logging_config import log_context
from src.common.storage import Storage
from src.warehouse.connection import warehouse_connection
from src.warehouse.loader import LOAD_ORDER, manifest_row_count, staging_row_count
from src.warehouse.sql_runner import fetch_all, render_sql

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WarehouseCheck:
    check_id: str
    severity: str
    description: str
    sql_file: str | None = None  # None: implemented in Python


CHECKS: tuple[WarehouseCheck, ...] = (
    WarehouseCheck("WQ-001", ERROR, "Staging row count equals processed partition row count"),
    WarehouseCheck(
        "WQ-002",
        ERROR,
        "Business keys are unique in every dimension and fact",
        "warehouse/checks/wq_002_business_key_uniqueness.sql",
    ),
    WarehouseCheck(
        "WQ-003",
        ERROR,
        "Fact dimension keys are set and resolve to dimension rows",
        "warehouse/checks/wq_003_fact_dimension_keys.sql",
    ),
    WarehouseCheck(
        "WQ-004",
        ERROR,
        "Every payment and delivery belongs to an order",
        "warehouse/checks/wq_004_orphan_facts.sql",
    ),
    WarehouseCheck(
        "WQ-005",
        WARN,
        "Warehouse AOV equals PySpark AOV (within 0.01) for the batch's order dates",
        "warehouse/checks/wq_005_aov_reconciliation.sql",
    ),
    WarehouseCheck(
        "WQ-006",
        ERROR,
        "No negative order or payment amounts",
        "warehouse/checks/wq_006_non_negative_amounts.sql",
    ),
)


@dataclass
class CheckResult:
    check_id: str
    severity: str
    description: str
    failures: dict[str, int] = field(default_factory=dict)  # object -> failed count

    @property
    def passed(self) -> bool:
        return not self.failures

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"passed": self.passed}


def _row_count_reconciliation(
    conn: Any, storage: Storage, schema: str, run_date: date
) -> dict[str, int]:
    """WQ-001: per staging table, |staged - processed| when they differ."""
    failures = {}
    for table in LOAD_ORDER:
        expected = manifest_row_count(storage, table.dataset, run_date)
        staged = staging_row_count(conn, schema, table.staging)
        if staged != expected:
            failures[table.staging] = abs(staged - expected)
    return failures


def run_check(
    conn: Any, check: WarehouseCheck, storage: Storage, schema: str, run_date: date
) -> CheckResult:
    if check.sql_file is None:
        failures = _row_count_reconciliation(conn, storage, schema, run_date)
    else:
        rows = fetch_all(conn, render_sql(check.sql_file, schema))
        failures = {name: int(count) for name, count in rows if count}
    return CheckResult(check.check_id, check.severity, check.description, failures)


def run_post_load_checks(
    settings: Settings,
    storage: Storage,
    run_date: date,
    run_id: str,
    conn: Any = None,
) -> list[CheckResult]:
    """Run WQ-001 – WQ-006; raises if any ERROR check fails (after running all of them)."""
    schema = settings.redshift_schema
    with (
        log_context(run_id=run_id, run_date=run_date.isoformat()),
        warehouse_connection(settings, conn) as connection,
    ):
        results = [run_check(connection, check, storage, schema, run_date) for check in CHECKS]
        for result in results:
            if result.passed:
                logger.info("Post-load check %s passed", result.check_id)
            else:
                level = logging.ERROR if result.severity == ERROR else logging.WARNING
                logger.log(
                    level,
                    "Post-load check %s failed (%s): %s",
                    result.check_id,
                    result.severity,
                    result.failures,
                )
        failed = [r for r in results if not r.passed and r.severity == ERROR]
        if failed:
            raise DataQualityThresholdError(
                "Post-load warehouse checks failed",
                checks=[r.check_id for r in failed],
                failures={r.check_id: r.failures for r in failed},
            )
        logger.info("Post-load checks passed")  # required message (spec 12 §2)
        return results
