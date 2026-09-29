"""Tests for src/ingestion (FR-010 – FR-017, FR-090, AC-005, AC-006)."""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest

from src.common.constants import (
    DATASETS,
    INGESTION_METADATA_COLUMNS,
    ORDERS,
    SOURCE_COLUMNS,
)
from src.common.exceptions import (
    ConfigError,
    SchemaValidationError,
    SourceFileError,
    StorageError,
)
from src.common.paths import raw_file_path
from src.common.storage import LocalStorage
from src.ingestion import INGESTORS
from src.ingestion.base_ingestion import STATUS_NO_DATA, STATUS_SUCCESS
from src.ingestion.orders_ingestion import OrdersIngestion

pytestmark = pytest.mark.unit

SAMPLE_DIR = Path(__file__).resolve().parents[2] / "data" / "sample"
RUN_DATE = date(2026, 9, 1)
RUN_ID = "test__2026-09-01"
ORDER_HEADER = ",".join(SOURCE_COLUMNS[ORDERS])


@pytest.fixture
def storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(tmp_path / "lake")


def _read_csv(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def _write_orders_source(root: Path, text: str, label: str = "2026-09-01") -> Path:
    path = root / ORDERS / f"orders_{label}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))
    return path


def _raw_rows(storage: LocalStorage, dataset: str = ORDERS) -> list[list[str]]:
    return _read_csv(storage.root / raw_file_path(dataset, RUN_DATE))


def test_registry_covers_every_dataset() -> None:
    assert set(INGESTORS) == set(DATASETS)
    for dataset, ingestor in INGESTORS.items():
        assert ingestor.config.dataset == dataset
        assert ingestor.config.expected_columns == SOURCE_COLUMNS[dataset]


@pytest.mark.parametrize("dataset", DATASETS)
@pytest.mark.parametrize(
    ("load_type", "label"), [("incremental", "2026-09-01"), ("historical", "historical")]
)
def test_ingests_sample_file_with_metadata(
    storage: LocalStorage, dataset: str, load_type: str, label: str
) -> None:
    source = SAMPLE_DIR / dataset / f"{dataset}_{label}.csv"

    result = INGESTORS[dataset](storage, SAMPLE_DIR).run(RUN_DATE, load_type, RUN_ID)

    source_rows = _read_csv(source)
    raw = _raw_rows(storage, dataset)
    width = len(SOURCE_COLUMNS[dataset])
    assert raw[0] == [*SOURCE_COLUMNS[dataset], *INGESTION_METADATA_COLUMNS]
    assert [row[:width] for row in raw[1:]] == source_rows[1:]  # values unchanged
    assert [row[width + 3] for row in raw[1:]] == [str(n) for n in range(1, len(raw))]
    # "<=": some daily files are header-only (e.g. delivery_partners on 2026-09-01).
    assert {row[width + 1] for row in raw[1:]} <= {"2026-09-01"}
    assert {row[width + 2] for row in raw[1:]} <= {f"{dataset}/{source.name}"}
    assert {row[width + 4] for row in raw[1:]} <= {RUN_ID}

    records = len(source_rows) - 1
    assert result.status == (STATUS_SUCCESS if records else STATUS_NO_DATA)
    assert result.records_read == result.records_written == records
    assert result.output_path.endswith(raw_file_path(dataset, RUN_DATE))
    assert result.to_dict()["load_type"] == load_type


def test_quoted_commas_and_empty_values_are_preserved(
    storage: LocalStorage, tmp_path: Path
) -> None:
    _write_orders_source(
        tmp_path,
        f'{ORDER_HEADER}\nORD0000001,C10001,"R1,001",2026-09-01 12:00:00,,"DELIVERED"\n',
    )

    OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)

    assert _raw_rows(storage)[1][:6] == [
        "ORD0000001",
        "C10001",
        "R1,001",
        "2026-09-01 12:00:00",
        "",
        "DELIVERED",
    ]


def test_bom_is_stripped(storage: LocalStorage, tmp_path: Path) -> None:
    _write_orders_source(tmp_path, f"﻿{ORDER_HEADER}\nORD1,C1,R1,x,1,PLACED\n")

    result = OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)

    assert result.records_written == 1


def test_header_only_file_is_no_data_and_still_written(
    storage: LocalStorage, tmp_path: Path
) -> None:
    _write_orders_source(tmp_path, f"{ORDER_HEADER}\n")

    result = OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)

    assert result.status == STATUS_NO_DATA
    assert result.records_written == 0
    assert len(_raw_rows(storage)) == 1  # header only, replaces any earlier raw file


def test_missing_file_raises_source_file_error(storage: LocalStorage, tmp_path: Path) -> None:
    with pytest.raises(SourceFileError, match="not found") as excinfo:
        OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)

    assert not excinfo.value.retryable
    assert excinfo.value.context["dataset"] == ORDERS
    assert excinfo.value.context["run_id"] == RUN_ID
    assert excinfo.value.context["path"].endswith("orders_2026-09-01.csv")
    assert not storage.exists(raw_file_path(ORDERS, RUN_DATE))


@pytest.mark.parametrize(
    ("header", "message"),
    [
        (f"{ORDER_HEADER},extra_column", r"unexpected=\['extra_column'\]"),
        (
            "order_id,customer_id,restaurant_id,order_date,order_amount",
            r"missing=\['order_status'\]",
        ),
        (
            "customer_id,order_id,restaurant_id,order_date,order_amount,order_status",
            "order differs",
        ),
        ("ORDER_ID,customer_id,restaurant_id,order_date,order_amount,order_status", "missing"),
    ],
    ids=["extra", "missing", "reordered", "case"],
)
def test_wrong_header_raises_schema_error(
    storage: LocalStorage, tmp_path: Path, header: str, message: str
) -> None:
    _write_orders_source(tmp_path, f"{header}\n")

    with pytest.raises(SchemaValidationError, match=message) as excinfo:
        OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)

    assert not excinfo.value.retryable
    assert not storage.exists(raw_file_path(ORDERS, RUN_DATE))


def test_empty_file_raises_schema_error(storage: LocalStorage, tmp_path: Path) -> None:
    _write_orders_source(tmp_path, "")

    with pytest.raises(SchemaValidationError, match="empty"):
        OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)


def test_row_with_wrong_field_count_raises_schema_error(
    storage: LocalStorage, tmp_path: Path
) -> None:
    _write_orders_source(tmp_path, f"{ORDER_HEADER}\nORD1,C1,R1,x,1,PLACED\nORD2,C1,R1\n")

    with pytest.raises(SchemaValidationError, match="3 fields") as excinfo:
        OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)

    assert excinfo.value.context["row_number"] == 2


def test_non_utf8_file_raises_source_file_error(storage: LocalStorage, tmp_path: Path) -> None:
    path = _write_orders_source(tmp_path, f"{ORDER_HEADER}\n")
    path.write_bytes(path.read_bytes() + "ORD1,C1,Café,x,1,PLACED\n".encode("latin-1"))

    with pytest.raises(SourceFileError, match="UTF-8"):
        OrdersIngestion(storage, tmp_path).run(RUN_DATE, "incremental", RUN_ID)


def test_unknown_load_type_raises_config_error(storage: LocalStorage) -> None:
    with pytest.raises(ConfigError):
        OrdersIngestion(storage, SAMPLE_DIR).run(RUN_DATE, "weekly", RUN_ID)


def test_rerun_overwrites_single_raw_file(storage: LocalStorage) -> None:
    ingestion = OrdersIngestion(storage, SAMPLE_DIR)
    first = ingestion.run(RUN_DATE, "incremental", "run-1")
    second = ingestion.run(RUN_DATE, "incremental", "run-2")

    files = storage.list("raw/orders/")
    assert files == [raw_file_path(ORDERS, RUN_DATE)]
    assert first.records_written == second.records_written
    assert {row[-1] for row in _raw_rows(storage)[1:]} == {"run-2"}


def test_storage_write_failure_propagates_with_context(tmp_path: Path) -> None:
    blocker = tmp_path / "lake"
    blocker.write_text("not a directory")

    with pytest.raises(StorageError) as excinfo:
        OrdersIngestion(LocalStorage(blocker), SAMPLE_DIR).run(RUN_DATE, "incremental", RUN_ID)

    assert excinfo.value.retryable
    assert excinfo.value.context["dataset"] == ORDERS


def test_accepts_any_source_implementation(storage: LocalStorage) -> None:
    class InMemorySource:
        name = "api/orders"
        location = "memory://orders"

        @contextmanager
        def read_rows(self) -> Iterator[tuple[list[str], Iterator[list[str]]]]:
            yield list(SOURCE_COLUMNS[ORDERS]), iter([["ORD1", "C1", "R1", "x", "1", "PLACED"]])

    result = OrdersIngestion(storage, Path("unused")).run(
        RUN_DATE, "incremental", RUN_ID, source=InMemorySource()
    )

    assert result.source_file == "api/orders"
    assert _raw_rows(storage)[1][8] == "api/orders"  # _source_file


def test_logs_required_messages(storage: LocalStorage, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="src.ingestion")

    result = OrdersIngestion(storage, SAMPLE_DIR).run(RUN_DATE, "incremental", RUN_ID)

    messages = [record.getMessage() for record in caplog.records]
    assert "Starting orders ingestion" in messages
    assert f"Records received: {result.records_read}" in messages
    assert f"Records written to raw: {result.records_written}" in messages
    assert any(m.startswith("Ingestion finished: status=SUCCESS") for m in messages)
