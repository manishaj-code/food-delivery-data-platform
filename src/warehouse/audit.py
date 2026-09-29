"""Write ``pipeline_run_audit`` rows (FR-102, FR-093, spec 12 §4).

Grain: one row per ``run_id`` × ``dataset`` × ``stage``. Rows are replaced on rerun
(delete-then-insert in one transaction), so a retried stage never duplicates its row.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import astuple, dataclass, fields
from datetime import date, datetime
from typing import Any

from src.warehouse.sql_runner import transaction, translate_errors

ERROR_MESSAGE_LIMIT = 1000


@dataclass
class AuditRecord:
    run_id: str
    run_date: date
    load_type: str | None
    dataset: str
    stage: str
    status: str  # SUCCESS | NO_DATA | FAILED
    records_in: int | None = None
    records_out: int | None = None
    records_rejected: int | None = None
    quality_score: float | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.error_message is not None:
            self.error_message = self.error_message[:ERROR_MESSAGE_LIMIT]


AUDIT_COLUMNS = tuple(column.name for column in fields(AuditRecord))


def write_audit(conn: Any, schema: str, records: Iterable[AuditRecord]) -> int:
    """Replace the audit rows for the given records; returns the number written."""
    records = list(records)
    columns = ", ".join(AUDIT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(AUDIT_COLUMNS))
    with transaction(conn), translate_errors("audit write"), conn.cursor() as cursor:
        for record in records:
            cursor.execute(
                f"DELETE FROM {schema}.pipeline_run_audit "
                "WHERE run_id = %s AND dataset = %s AND stage = %s",
                (record.run_id, record.dataset, record.stage),
            )
            cursor.execute(
                f"INSERT INTO {schema}.pipeline_run_audit ({columns}) VALUES ({placeholders})",
                astuple(record),
            )
    return len(records)
