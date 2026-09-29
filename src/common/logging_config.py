"""Central logging configuration (docs/spec/07-data-pipeline-specification.md §11).

Every log line carries run context (run_id, run_date, dataset) injected from
context variables, so modules just use ``logging.getLogger(__name__)``.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

LOG_FORMAT = (
    "%(asctime)s - %(levelname)s - %(name)s - "
    "[run_id=%(run_id)s run_date=%(run_date)s dataset=%(dataset)s] %(message)s"
)
CONTEXT_FIELDS = ("run_id", "run_date", "dataset")

_context: dict[str, ContextVar[str]] = {
    field: ContextVar(field, default="-") for field in CONTEXT_FIELDS
}


class RunContextFilter(logging.Filter):
    """Adds the current run context to each record."""

    def filter(self, record: logging.LogRecord) -> bool:
        for field, var in _context.items():
            if not hasattr(record, field):
                setattr(record, field, var.get())
        return True


def configure_logging(level: str | int = "INFO") -> None:
    """Configure the root logger once; safe to call repeatedly."""
    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        if getattr(handler, "_food_delivery_handler", False):
            root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(RunContextFilter())
    handler._food_delivery_handler = True  # type: ignore[attr-defined]
    root.addHandler(handler)


@contextmanager
def log_context(**values: object) -> Iterator[None]:
    """Temporarily set run context fields, e.g. ``log_context(dataset="orders")``."""
    unknown = set(values) - set(CONTEXT_FIELDS)
    if unknown:
        raise ValueError(f"Unknown log context fields: {sorted(unknown)}")
    tokens = [(_context[key], _context[key].set(str(value))) for key, value in values.items()]
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def current_context() -> dict[str, str]:
    """Current run context values (useful for audit records)."""
    return {field: var.get() for field, var in _context.items()}
