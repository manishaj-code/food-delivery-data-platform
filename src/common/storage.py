"""Storage abstraction for the data lake (FR-043).

Callers work with relative keys such as ``raw/orders/year=2026/month=09/day=29/orders.csv``.
``LocalStorage`` maps them under ``LOCAL_LAKE_PATH``; ``S3Storage`` (Phase 3) maps
them into the bucket. Both expose the same operations.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from src.common.config import Settings
from src.common.exceptions import ConfigError, StorageError


class Storage(Protocol):
    def write_bytes(self, key: str, data: bytes) -> None: ...

    def write_text(self, key: str, text: str) -> None: ...

    def write_file(self, key: str, local_path: Path) -> None: ...

    def read_bytes(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def list(self, prefix: str) -> list[str]: ...

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

    def delete_prefix(self, prefix: str) -> int:
        """Delete all files under ``prefix``; returns the number deleted."""
        keys = self.list(prefix)
        try:
            for key in keys:
                (self.root / key).unlink()
        except OSError as exc:
            raise StorageError("Failed to delete from local lake", prefix=prefix) from exc
        return len(keys)

    def uri_for(self, key: str) -> str:
        # Not Path.as_uri(): that percent-encodes "=" in partition names (year%3D2026).
        return f"file://{self._path(key).resolve().as_posix()}"


def get_storage(settings: Settings) -> Storage:
    """Storage backend selected by ``STORAGE_MODE``."""
    if settings.storage_mode == "local":
        return LocalStorage(settings.local_lake_path)
    raise ConfigError("S3 storage is not available yet (Phase 3)", variable="STORAGE_MODE")
