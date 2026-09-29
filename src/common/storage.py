"""Storage abstraction for the data lake (FR-043, FR-044).

Callers work with relative keys such as ``raw/orders/year=2026/month=09/day=29/orders.csv``.
``LocalStorage`` maps them under ``LOCAL_LAKE_PATH``; ``S3Storage`` maps them into
``S3_BUCKET``. Both expose the same operations, and ``replace_partition`` gives
both the same overwrite semantics.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any, Protocol

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.common.config import Settings
from src.common.exceptions import StorageError
from src.common.paths import Zone, partition_path, temp_path

logger = logging.getLogger(__name__)

_S3_DELETE_BATCH = 1000  # DeleteObjects limit


class Storage(Protocol):
    def write_bytes(self, key: str, data: bytes) -> None: ...

    def write_text(self, key: str, text: str) -> None: ...

    def write_file(self, key: str, local_path: Path) -> None: ...

    def read_bytes(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def list(self, prefix: str) -> list[str]: ...

    def copy(self, source_key: str, target_key: str) -> None: ...

    def delete_prefix(self, prefix: str) -> int: ...

    def uri_for(self, key: str) -> str: ...


def _check_key(key: str) -> str:
    parts = key.split("/")
    if not key or key.startswith("/") or "\\" in key or ".." in parts:
        raise StorageError("Invalid storage key", key=key)
    return key


class LocalStorage:
    """Lake on the local filesystem. Writes are atomic (temp file + rename)."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / _check_key(key)

    def _atomic_write(self, key: str, write: Callable[[Path], object]) -> None:
        target = self._path(key)
        temp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            write(temp)
            os.replace(temp, target)
        except OSError as exc:
            with contextlib.suppress(OSError):
                temp.unlink(missing_ok=True)
            raise StorageError("Failed to write to local lake", key=key, error=exc) from exc

    def write_bytes(self, key: str, data: bytes) -> None:
        self._atomic_write(key, lambda temp: temp.write_bytes(data))

    def write_text(self, key: str, text: str) -> None:
        self.write_bytes(key, text.encode("utf-8"))

    def write_file(self, key: str, local_path: Path) -> None:
        self._atomic_write(key, lambda temp: shutil.copyfile(local_path, temp))

    def read_bytes(self, key: str) -> bytes:
        try:
            return self._path(key).read_bytes()
        except OSError as exc:
            raise StorageError("Failed to read from local lake", key=key, error=exc) from exc

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list(self, prefix: str) -> list[str]:
        """Keys of all files under ``prefix`` (recursive, sorted)."""
        _check_key(prefix)
        directory = self.root / prefix.rsplit("/", 1)[0] if "/" in prefix else self.root
        if not directory.is_dir():
            return []
        keys = (path.relative_to(self.root).as_posix() for path in directory.rglob("*"))
        return sorted(key for key in keys if key.startswith(prefix) and (self.root / key).is_file())

    def copy(self, source_key: str, target_key: str) -> None:
        self.write_file(target_key, self._path(source_key))

    def delete_prefix(self, prefix: str) -> int:
        """Delete all files under ``prefix`` (and directories left empty); returns the count."""
        keys = self.list(prefix)
        try:
            for key in keys:
                (self.root / key).unlink()
            if prefix.endswith("/"):
                self._prune_empty_dirs(self.root / prefix)
        except OSError as exc:
            raise StorageError("Failed to delete from local lake", prefix=prefix) from exc
        return len(keys)

    def _prune_empty_dirs(self, directory: Path) -> None:
        if directory.is_dir():
            for child in sorted(directory.rglob("*"), key=lambda p: len(p.parts), reverse=True):
                if child.is_dir() and not any(child.iterdir()):
                    child.rmdir()
        root = self.root.resolve()
        while directory.is_dir() and directory.resolve() != root and not any(directory.iterdir()):
            directory.rmdir()
            directory = directory.parent

    def uri_for(self, key: str) -> str:
        # Not Path.as_uri(): that percent-encodes "=" in partition names (year%3D2026).
        return f"file://{self._path(key).resolve().as_posix()}"


class S3Storage:
    """Lake in an S3 bucket. Credentials come from the default AWS chain, never from code."""

    def __init__(self, bucket: str, client: Any = None, region: str | None = None) -> None:
        self.bucket = bucket
        self.client = client or boto3.client("s3", region_name=region)

    def _call(self, action: str, key: str, operation: Callable[[], Any]) -> Any:
        try:
            return operation()
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(
                f"S3 {action} failed", bucket=self.bucket, key=key, error=exc
            ) from exc

    def write_bytes(self, key: str, data: bytes) -> None:
        _check_key(key)
        self._call(
            "write",
            key,
            lambda: self.client.put_object(Bucket=self.bucket, Key=key, Body=data),
        )

    def write_text(self, key: str, text: str) -> None:
        self.write_bytes(key, text.encode("utf-8"))

    def write_file(self, key: str, local_path: Path) -> None:
        _check_key(key)
        self._call(
            "upload", key, lambda: self.client.upload_file(str(local_path), self.bucket, key)
        )

    def read_bytes(self, key: str) -> bytes:
        _check_key(key)
        response = self._call(
            "read", key, lambda: self.client.get_object(Bucket=self.bucket, Key=key)
        )
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        _check_key(key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise StorageError("S3 head failed", bucket=self.bucket, key=key, error=exc) from exc
        except BotoCoreError as exc:
            raise StorageError("S3 head failed", bucket=self.bucket, key=key, error=exc) from exc
        return True

    def list(self, prefix: str) -> list[str]:
        _check_key(prefix)

        def _list() -> list[str]:
            paginator = self.client.get_paginator("list_objects_v2")
            return [
                item["Key"]
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
                for item in page.get("Contents", [])
            ]

        return sorted(self._call("list", prefix, _list))

    def copy(self, source_key: str, target_key: str) -> None:
        _check_key(source_key)
        _check_key(target_key)
        source = {"Bucket": self.bucket, "Key": source_key}
        self._call("copy", target_key, lambda: self.client.copy(source, self.bucket, target_key))

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list(prefix)
        for start in range(0, len(keys), _S3_DELETE_BATCH):
            batch = [{"Key": key} for key in keys[start : start + _S3_DELETE_BATCH]]
            response = self._call(
                "delete",
                prefix,
                lambda batch=batch: self.client.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True}
                ),
            )
            if response.get("Errors"):
                raise StorageError(
                    "S3 delete failed for some objects",
                    bucket=self.bucket,
                    prefix=prefix,
                    failed=len(response["Errors"]),
                )
        return len(keys)

    def uri_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{_check_key(key)}"


def replace_prefix(storage: Storage, staged_prefix: str, target_prefix: str) -> list[str]:
    """Make ``target_prefix`` contain exactly the files staged under ``staged_prefix``.

    Steps: delete target -> copy staged files in -> delete staging. The target is only
    touched after the staged output is complete, so a failed writer never damages it.
    A failure during the short copy step is repaired by rerunning (FR-044, FR-090).
    Returns the published keys.
    """
    staged = storage.list(staged_prefix)
    storage.delete_prefix(target_prefix)
    published = []
    for key in staged:
        target_key = target_prefix + key[len(staged_prefix) :]
        storage.copy(key, target_key)
        published.append(target_key)
    storage.delete_prefix(staged_prefix)
    return published


def replace_partition(
    storage: Storage,
    zone: Zone | str,
    dataset: str,
    run_date: date,
    run_id: str,
    write: Callable[[str], None],
) -> list[str]:
    """Replace one run-date partition with the output of ``write(staging_prefix)``.

    Only the ``(zone, dataset, run_date)`` partition changes; other partitions are untouched.
    """
    staging = temp_path(zone, dataset, run_id)
    target = partition_path(zone, dataset, run_date)
    storage.delete_prefix(staging)  # leftovers from an earlier failed attempt
    write(staging)
    published = replace_prefix(storage, staging, target)
    logger.info("Published %d file(s) to %s", len(published), storage.uri_for(target))
    return published


def get_storage(settings: Settings) -> Storage:
    """Storage backend selected by ``STORAGE_MODE``."""
    if settings.storage_mode == "s3":
        return S3Storage(settings.s3_bucket or "", region=settings.aws_region)
    return LocalStorage(settings.local_lake_path)
