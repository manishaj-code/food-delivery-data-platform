"""Glue between the Airflow task context and ``steps`` (FR-073, FR-074).

No Airflow imports: the context is read as a mapping, so this module is unit-tested
without Airflow installed. The DAG maps non-retryable errors to ``AirflowFailException``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from src.common.config import get_settings
from src.common.constants import LOAD_TYPE_INCREMENTAL
from src.common.logging_config import configure_logging
from src.pipeline import steps

logger = logging.getLogger(__name__)


def run_arguments(context: Mapping[str, Any]) -> dict[str, Any]:
    """``run_date`` (logical date), ``run_id``, and ``load_type`` of the running DAG run.

    Airflow 3 manual runs may have no logical date; the run's ``run_after`` date is used.
    """
    logical_date = context.get("logical_date")
    if logical_date is None:
        logical_date = context["dag_run"].run_after
    params = context.get("params") or {}
    return {
        "run_date": logical_date.date().isoformat(),
        "run_id": context["run_id"],
        "load_type": params.get("load_type", LOAD_TYPE_INCREMENTAL),
    }


def run_step(step: Callable[..., dict[str, Any]], context: Mapping[str, Any], **kwargs: Any):
    """Run one step for this DAG run with the project's log format (run_id=, run_date=)."""
    configure_logging(get_settings().log_level)
    return step(**run_arguments(context), **kwargs)


def on_task_failure(context: Mapping[str, Any]) -> None:
    """``on_failure_callback``: failure log line, FAILED audit, ``PipelineFailure`` metric.

    Runs only after the last retry. Never raises.
    """
    try:
        configure_logging(get_settings().log_level)
        task_instance = context.get("task_instance") or context.get("ti")
        task_id = getattr(task_instance, "task_id", "unknown")
        exception = context.get("exception")
        # The DAG wraps non-retryable errors in AirflowFailException; report the original.
        exception = getattr(exception, "__cause__", None) or exception
        error = steps.error_message(exception) if exception else "unknown error"
        steps.record_failure(**run_arguments(context), task_id=task_id, error=error)
    except Exception:  # noqa: BLE001 — a callback must not raise
        logger.exception("Failure callback could not complete")
