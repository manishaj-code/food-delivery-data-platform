"""Tests for src/common/storage.py and src/common/paths.py (FR-014, FR-043 local backend)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.common.config import load_settings
from src.common.exceptions import ConfigError, StorageError
from src.common.paths import partition_path, raw_file_path
from src.common.storage import LocalStorage, get_storage

pytestmark = pytest.mark.unit

KEY = "raw/orders/year=2026/month=09/day=29/orders.csv"


@pytest.fixture
def storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(tmp_path / "lake")


def test_raw_file_path_is_partitioned_by_run_date() -> None:
    assert raw_file_path("orders", date(2026, 9, 29)) == KEY
    assert partition_path("raw", "customers", date(2026, 1, 5)) == (
        "raw/customers/year=2026/month=01/day=05/"
    )


def test_write_read_and_overwrite(storage: LocalStorage) -> None:
    storage.write_text(KEY, "first")
    storage.write_bytes(KEY, b"second")

    assert storage.exists(KEY)
    assert storage.read_bytes(KEY) == b"second"
    assert storage.list("raw/orders/") == [KEY]  # no temp files left behind


def test_write_file_copies_local_file(storage: LocalStorage, tmp_path: Path) -> None:
    source = tmp_path / "staged.csv"
    source.write_text("a,b\n1,2\n")

    storage.write_file(KEY, source)

    assert storage.read_bytes(KEY) == b"a,b\n1,2\n"


def test_list_filters_by_prefix(storage: LocalStorage) -> None:
    for key in (KEY, "raw/orders_extra/x.csv", "raw/customers/c.csv"):
        storage.write_text(key, "x")

    assert storage.list("raw/orders/") == [KEY]
    assert storage.list("raw/orders") == [KEY, "raw/orders_extra/x.csv"]
    assert storage.list("processed/") == []


def test_delete_prefix(storage: LocalStorage) -> None:
    storage.write_text(KEY, "x")
    storage.write_text("raw/customers/c.csv", "x")

    assert storage.delete_prefix("raw/orders/") == 1
    assert not storage.exists(KEY)
    assert storage.exists("raw/customers/c.csv")


def test_uri_for_is_file_uri(storage: LocalStorage) -> None:
    assert storage.uri_for(KEY).startswith("file://")
    assert storage.uri_for(KEY).endswith(KEY)


@pytest.mark.parametrize("key", ["", "/abs/key", "../escape.csv", "raw/../../x", "raw\\win"])
def test_invalid_keys_are_rejected(storage: LocalStorage, key: str) -> None:
    with pytest.raises(StorageError, match="Invalid storage key"):
        storage.write_text(key, "x")


def test_read_missing_key_raises_storage_error(storage: LocalStorage) -> None:
    with pytest.raises(StorageError) as excinfo:
        storage.read_bytes(KEY)
    assert excinfo.value.retryable


def test_write_failure_raises_storage_error(tmp_path: Path) -> None:
    blocker = tmp_path / "lake"
    blocker.write_text("a file where the lake directory should be")

    with pytest.raises(StorageError, match="Failed to write"):
        LocalStorage(blocker).write_text(KEY, "x")


def test_get_storage_selects_backend(tmp_path: Path) -> None:
    local = get_storage(load_settings({"LOCAL_LAKE_PATH": str(tmp_path)}))
    assert isinstance(local, LocalStorage)
    assert local.root == tmp_path

    with pytest.raises(ConfigError):
        get_storage(load_settings({"STORAGE_MODE": "s3", "S3_BUCKET": "bucket"}))
