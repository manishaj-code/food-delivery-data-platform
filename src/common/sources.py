"""Source interface for ingestion (FR-011, FR-017).

``BaseIngestion`` depends only on ``Source``, so another source type (e.g. an API)
can be added later without changing it. ``CsvFileSource`` reads the generated CSVs.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import date
from pathlib import Path
from typing import Protocol

from src.common.constants import (
    HISTORICAL_FILE_LABEL,
    LOAD_TYPE_HISTORICAL,
    LOAD_TYPES,
    source_file_name,
)
from src.common.exceptions import ConfigError, SchemaValidationError, SourceFileError

Rows = Iterator[list[str]]


class Source(Protocol):
    name: str  # recorded in ``_source_file`` for lineage
    location: str  # full location, for logs and errors

    def read_rows(self) -> AbstractContextManager[tuple[list[str], Rows]]:
        """Open the source; yields ``(header, rows)`` with every value as a string."""
        ...


class CsvFileSource:
    """A UTF-8 CSV file with a header row. A leading BOM is stripped."""

    def __init__(self, path: Path, name: str | None = None) -> None:
        self.path = Path(path)
        self.name = name or self.path.name
        self.location = str(self.path)

    @classmethod
    def for_run(
        cls, source_root: Path, dataset: str, load_type: str, run_date: date
    ) -> CsvFileSource:
        """``<dataset>_historical.csv`` or ``<dataset>_<run_date>.csv`` under ``source_root``."""
        if load_type not in LOAD_TYPES:
            raise ConfigError(f"load_type must be one of {LOAD_TYPES}", load_type=load_type)
        label = HISTORICAL_FILE_LABEL if load_type == LOAD_TYPE_HISTORICAL else run_date.isoformat()
        file_name = source_file_name(dataset, label)
        return cls(Path(source_root) / dataset / file_name, name=f"{dataset}/{file_name}")

    @contextmanager
    def read_rows(self) -> Iterator[tuple[list[str], Rows]]:
        try:
            handle = self.path.open(encoding="utf-8-sig", newline="")
        except FileNotFoundError as exc:
            raise SourceFileError("Source file not found", path=self.location) from exc
        except OSError as exc:
            raise SourceFileError("Source file is not readable", path=self.location) from exc

        with handle:
            reader = csv.reader(handle)
            try:
                header = next(reader, None)
            except (UnicodeDecodeError, csv.Error) as exc:
                raise SourceFileError(
                    "Source file is not valid UTF-8 CSV", path=self.location
                ) from exc
            if header is None:
                raise SchemaValidationError("Source file is empty (no header)", path=self.location)
            yield header, self._rows(reader)

    def _rows(self, reader: Iterator[list[str]]) -> Rows:
        try:
            for row in reader:
                if row:  # skip blank lines
                    yield row
        except (UnicodeDecodeError, csv.Error) as exc:
            raise SourceFileError("Source file is not valid UTF-8 CSV", path=self.location) from exc
