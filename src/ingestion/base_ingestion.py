"""Reusable ingestion flow: source CSV -> raw layer (FR-010 – FR-017, FR-090).

Dataset modules only declare an ``IngestionConfig``; all reading, checking,
writing, and logging lives here.
"""

from __future__ import annotations

import csv
import logging
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import ClassVar

from src.common.config import Settings
from src.common.constants import INGESTION_METADATA_COLUMNS, TIMESTAMP_FORMAT
from src.common.exceptions import PipelineError, SchemaValidationError, StorageError
from src.common.logging_config import log_context
from src.common.paths import raw_file_path
from src.common.sources import CsvFileSource, Source
from src.common.storage import Storage, get_storage

logger = logging.getLogger(__name__)

STATUS_SUCCESS = "SUCCESS"
STATUS_NO_DATA = "NO_DATA"


@dataclass(frozen=True)
class IngestionConfig:
    dataset: str
    expected_columns: tuple[str, ...]


@dataclass(frozen=True)
class IngestionResult:
    dataset: str
    status: str
    records_read: int
    records_written: int
    output_path: str
    duration_seconds: float
    source_file: str
    run_id: str
    run_date: str
    load_type: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class BaseIngestion:
    """Reads one dataset's source for a run and writes it to ``raw/`` with metadata."""

    config: ClassVar[IngestionConfig]

    def __init__(self, storage: Storage, source_root: Path) -> None:
        self.storage = storage
        self.source_root = Path(source_root)

    @classmethod
    def from_settings(cls, settings: Settings) -> BaseIngestion:
        return cls(get_storage(settings), settings.source_data_path)

    @property
    def dataset(self) -> str:
        return self.config.dataset

    def resolve_source(self, run_date: date, load_type: str) -> Source:
        return CsvFileSource.for_run(self.source_root, self.dataset, load_type, run_date)

    def run(
        self, run_date: date, load_type: str, run_id: str, source: Source | None = None
    ) -> IngestionResult:
        with log_context(dataset=self.dataset, run_id=run_id, run_date=run_date.isoformat()):
            started = time.perf_counter()
            logger.info("Starting %s ingestion", self.dataset)
            source = source or self.resolve_source(run_date, load_type)
            key = raw_file_path(self.dataset, run_date)
            logger.info("Source: %s", source.location)

            metadata = [
                datetime.now(UTC).strftime(TIMESTAMP_FORMAT),
                run_date.isoformat(),
                source.name,
            ]
            try:
                with tempfile.TemporaryDirectory(prefix="ingest-") as temp_dir:
                    temp_file = Path(temp_dir) / f"{self.dataset}.csv"
                    records = self._write_raw_csv(source, temp_file, metadata, run_id)
                    logger.info("Records received: %d", records)
                    self.storage.write_file(key, temp_file)
            except PipelineError as exc:
                # Typed errors carry the run context (FR-016); logged once at the step boundary.
                for name, value in (("dataset", self.dataset), ("run_id", run_id), ("key", key)):
                    exc.context.setdefault(name, value)
                raise
            logger.info("Records written to raw: %d", records)

            result = IngestionResult(
                dataset=self.dataset,
                status=STATUS_SUCCESS if records else STATUS_NO_DATA,
                records_read=records,
                records_written=records,
                output_path=self.storage.uri_for(key),
                duration_seconds=round(time.perf_counter() - started, 3),
                source_file=source.name,
                run_id=run_id,
                run_date=run_date.isoformat(),
                load_type=load_type,
            )
            logger.info(
                "Ingestion finished: status=%s records=%d output=%s duration=%.3fs",
                result.status,
                result.records_written,
                result.output_path,
                result.duration_seconds,
            )
            return result

    def _write_raw_csv(self, source: Source, target: Path, metadata: list[str], run_id: str) -> int:
        """Stream source rows into ``target`` with metadata columns; returns the row count."""
        expected = self.config.expected_columns
        records = 0
        with source.read_rows() as (header, rows):
            self._check_header(header, source)
            try:
                with target.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.writer(handle, lineterminator="\n")
                    writer.writerow([*expected, *INGESTION_METADATA_COLUMNS])
                    for records, row in enumerate(rows, start=1):
                        if len(row) != len(expected):
                            raise SchemaValidationError(
                                f"Row has {len(row)} fields, expected {len(expected)}",
                                path=source.location,
                                row_number=records,
                            )
                        writer.writerow([*row, *metadata, records, run_id])
            except OSError as exc:
                raise StorageError("Failed to stage raw file", path=str(target)) from exc
        return records

    def _check_header(self, header: list[str], source: Source) -> None:
        expected = list(self.config.expected_columns)
        if header == expected:
            return
        missing = [column for column in expected if column not in header]
        unexpected = [column for column in header if column not in expected]
        detail = f"missing={missing} unexpected={unexpected}" if missing or unexpected else ""
        raise SchemaValidationError(
            f"Header does not match expected columns {detail or '(order differs)'}".strip(),
            dataset=self.dataset,
            path=source.location,
            header=header,
        )
