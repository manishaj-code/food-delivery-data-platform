"""Tests for src/common/storage.py (FR-043, FR-044, AC-007).

Behavioural tests run against both backends: LocalStorage and moto-backed S3Storage.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.common.config import load_settings
from src.common.exceptions import StorageError
from src.common.paths import Zone, partition_path
from src.common.storage import LocalStorage, S3Storage, Storage, get_storage, replace_partition
from tests.conftest import TEST_BUCKET

pytestmark = pytest.mark.unit

KEY = "raw/orders/year=2026/month=09/day=29/orders.csv"
RUN_DATE = date(2026, 9, 29)


@pytest.fixture(params=["local", "s3"])
def storage(request: pytest.FixtureRequest, tmp_path: Path) -> Storage:
    if request.param == "local":
        return LocalStorage(tmp_path / "lake")
    return S3Storage(TEST_BUCKET, client=request.getfixturevalue("s3_client"))


def test_write_read_and_overwrite(storage: Storage) -> None:
    storage.write_text(KEY, "first")
    storage.write_bytes(KEY, b"second")

    assert storage.exists(KEY)
    assert storage.read_bytes(KEY) == b"second"
    assert storage.list("raw/orders/") == [KEY]  # no temp files left behind


def test_exists_is_false_for_missing_key(storage: Storage) -> None:
    assert not storage.exists(KEY)


def test_write_file_uploads_local_file(storage: Storage, tmp_path: Path) -> None:
    source = tmp_path / "staged.csv"
    source.write_text("a,b\n1,2\n")

    storage.write_file(KEY, source)

    assert storage.read_bytes(KEY) == b"a,b\n1,2\n"


def test_list_filters_by_prefix(storage: Storage) -> None:
    for key in (KEY, "raw/orders_extra/x.csv", "raw/customers/c.csv"):
        storage.write_text(key, "x")

    assert storage.list("raw/orders/") == [KEY]
    assert storage.list("raw/orders") == [KEY, "raw/orders_extra/x.csv"]
    assert storage.list("processed/") == []


def test_copy(storage: Storage) -> None:
    storage.write_text("a/one.csv", "data")

    storage.copy("a/one.csv", "b/one.csv")

    assert storage.read_bytes("b/one.csv") == b"data"
    assert storage.exists("a/one.csv")


def test_delete_prefix(storage: Storage) -> None:
    storage.write_text(KEY, "x")
    storage.write_text("raw/customers/c.csv", "x")

    assert storage.delete_prefix("raw/orders/") == 1
    assert not storage.exists(KEY)
    assert storage.exists("raw/customers/c.csv")
    assert storage.delete_prefix("raw/orders/") == 0


def test_delete_prefix_handles_more_than_one_s3_batch(storage: Storage) -> None:
    for number in range(1005):
        storage.write_bytes(f"validated/orders/part-{number:05d}.parquet", b"")

    assert storage.delete_prefix("validated/orders/") == 1005
    assert storage.list("validated/") == []


def test_read_missing_key_raises_retryable_storage_error(storage: Storage) -> None:
    with pytest.raises(StorageError) as excinfo:
        storage.read_bytes(KEY)
    assert excinfo.value.retryable


@pytest.mark.parametrize("key", ["", "/abs/key", "../escape.csv", "raw/../../x", "raw\\win"])
def test_invalid_keys_are_rejected(storage: Storage, key: str) -> None:
    with pytest.raises(StorageError, match="Invalid storage key"):
        storage.write_text(key, "x")


# --- replace_partition (overwrite semantics, FR-044) -------------------------------------


def test_replace_partition_replaces_only_target_partition(storage: Storage) -> None:
    target = partition_path(Zone.PROCESSED, "orders", RUN_DATE)
    other_day = partition_path(Zone.PROCESSED, "orders", date(2026, 9, 28))
    other_dataset = partition_path(Zone.PROCESSED, "payments", RUN_DATE)
    for key in (f"{target}part-old-1.parquet", f"{target}part-old-2.parquet"):
        storage.write_text(key, "old")
    storage.write_text(f"{other_day}part-0.parquet", "keep")
    storage.write_text(f"{other_dataset}part-0.parquet", "keep")

    def write(staging: str) -> None:
        storage.write_text(f"{staging}part-new.parquet", "new")

    published = replace_partition(storage, Zone.PROCESSED, "orders", RUN_DATE, "run-2", write)

    assert published == [f"{target}part-new.parquet"]
    assert storage.list(target) == published
    assert storage.read_bytes(published[0]) == b"new"
    assert storage.exists(f"{other_day}part-0.parquet")
    assert storage.exists(f"{other_dataset}part-0.parquet")
    assert storage.list("_tmp/") == []  # staging cleaned up


def test_replace_partition_rerun_leaves_one_copy(storage: Storage) -> None:
    def write(staging: str) -> None:
        storage.write_text(f"{staging}orders.csv", "data")

    for run_id in ("run-1", "run-1", "run-2"):
        replace_partition(storage, Zone.RAW, "orders", RUN_DATE, run_id, write)

    assert storage.list("raw/") == [KEY]


def test_failed_writer_leaves_existing_partition_intact(storage: Storage) -> None:
    storage.write_text(KEY, "previous good data")

    def failing_write(staging: str) -> None:
        storage.write_text(f"{staging}orders.csv", "partial")
        raise RuntimeError("writer crashed")

    with pytest.raises(RuntimeError):
        replace_partition(storage, Zone.RAW, "orders", RUN_DATE, "run-1", failing_write)

    assert storage.read_bytes(KEY) == b"previous good data"


def test_replace_partition_clears_stale_staging(storage: Storage) -> None:
    storage.write_text("_tmp/run-1/raw/orders/stale.csv", "left by a crashed attempt")

    def write(staging: str) -> None:
        storage.write_text(f"{staging}orders.csv", "data")

    replace_partition(storage, Zone.RAW, "orders", RUN_DATE, "run-1", write)

    assert storage.list("raw/") == [KEY]


# --- backend specifics --------------------------------------------------------------------


def test_uri_for_each_backend(tmp_path: Path, s3_client) -> None:
    local = LocalStorage(tmp_path).uri_for(KEY)
    assert local == f"file://{tmp_path.resolve().as_posix()}/{KEY}"  # "=" not percent-encoded
    assert S3Storage(TEST_BUCKET, client=s3_client).uri_for(KEY) == f"s3://{TEST_BUCKET}/{KEY}"


def test_local_write_failure_raises_storage_error(tmp_path: Path) -> None:
    blocker = tmp_path / "lake"
    blocker.write_text("a file where the lake directory should be")

    with pytest.raises(StorageError, match="Failed to write"):
        LocalStorage(blocker).write_text(KEY, "x")


def test_local_delete_prefix_removes_empty_directories(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    storage.write_text("_tmp/run-1/raw/orders/orders.csv", "x")

    storage.delete_prefix("_tmp/run-1/raw/orders/")

    assert not (tmp_path / "_tmp").exists()


def test_s3_missing_bucket_raises_storage_error(s3_client) -> None:
    storage = S3Storage("bucket-that-does-not-exist", client=s3_client)

    with pytest.raises(StorageError, match="S3 write failed") as excinfo:
        storage.write_text(KEY, "x")

    assert excinfo.value.retryable
    assert excinfo.value.context["bucket"] == "bucket-that-does-not-exist"


def test_get_storage_selects_backend(tmp_path: Path, s3_client) -> None:
    local = get_storage(load_settings({"LOCAL_LAKE_PATH": str(tmp_path)}))
    s3 = get_storage(load_settings({"STORAGE_MODE": "s3", "S3_BUCKET": TEST_BUCKET}))

    assert isinstance(local, LocalStorage)
    assert local.root == tmp_path
    assert isinstance(s3, S3Storage)
    assert s3.bucket == TEST_BUCKET
