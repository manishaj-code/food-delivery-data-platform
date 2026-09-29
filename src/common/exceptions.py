"""Exception hierarchy for the pipeline (docs/spec/07-data-pipeline-specification.md §9).

``retryable`` tells the orchestration layer whether retrying can help.
"""

from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """Base class for all pipeline errors. Carries structured context for logging."""

    retryable: bool = False

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = {key: value for key, value in context.items() if value is not None}

    def __str__(self) -> str:
        if not self.context:
            return self.message
        details = ", ".join(f"{key}={value}" for key, value in self.context.items())
        return f"{self.message} ({details})"


class ConfigError(PipelineError):
    """Missing or invalid configuration."""


class SourceFileError(PipelineError):
    """Source file missing or unreadable."""


class SchemaValidationError(PipelineError):
    """Source file structure (header) does not match the expected schema."""


class StorageError(PipelineError):
    """Reading from or writing to the lake failed (local disk or S3)."""

    retryable = True


class DataQualityThresholdError(PipelineError):
    """Data quality below threshold or a post-load check failed."""


class TransformationError(PipelineError):
    """PySpark transformation failed."""

    retryable = True


class WarehouseConnectionError(PipelineError):
    """Could not connect to the warehouse."""

    retryable = True


class WarehouseLoadError(PipelineError):
    """Warehouse SQL/data error during load."""
