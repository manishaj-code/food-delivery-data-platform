"""Command-line entry point: the same steps as the DAG, without Airflow (spec 07 §3).

Examples::

    python -m src.cli generate-data --mode incremental --date 2026-09-01
    python -m src.cli init-warehouse
    python -m src.cli run-step ingest --dataset orders --run-date 2026-09-01
    python -m src.cli run-pipeline --run-date 2026-08-31 --load-type historical
    python -m src.cli run-analytics --query 08_top_restaurants
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from typing import Any

from src.common.config import get_settings
from src.common.constants import DATASETS, LOAD_TYPE_INCREMENTAL, LOAD_TYPES
from src.common.exceptions import PipelineError
from src.common.logging_config import configure_logging
from src.pipeline import steps

logger = logging.getLogger("src.cli")

STEPS: dict[str, Callable[..., dict[str, Any]]] = {
    "prepare": steps.prepare_source_data,
    "ingest": steps.ingest_dataset,
    "validate": steps.validate_data,
    "transform": steps.transform_data,
    "publish": steps.publish_processed,
    "load": steps.load_warehouse,
    "checks": steps.run_dq_checks,
    "summary": steps.pipeline_summary,
}


def pipeline_plan() -> list[tuple[str, Callable[..., dict[str, Any]], dict[str, Any]]]:
    """(task id, step, extra kwargs) in the DAG's order (FR-070)."""
    plan: list[tuple[str, Callable[..., dict[str, Any]], dict[str, Any]]] = [
        ("prepare_source_data", steps.prepare_source_data, {})
    ]
    plan += [(f"ingest_{d}", steps.ingest_dataset, {"dataset": d}) for d in DATASETS]
    plan += [
        ("validate_data", steps.validate_data, {}),
        ("transform_data", steps.transform_data, {}),
        ("publish_processed", steps.publish_processed, {}),
        ("load_warehouse", steps.load_warehouse, {}),
        ("run_dq_checks", steps.run_dq_checks, {}),
        ("pipeline_summary", steps.pipeline_summary, {}),
    ]
    return plan


def default_run_id() -> str:
    return f"cli__{datetime.now(UTC):%Y%m%dT%H%M%S}"


def run_pipeline(run_date: date, load_type: str, run_id: str) -> int:
    """Run every step in order; stop at the first failure (no retries in the CLI)."""
    for task_id, step, kwargs in pipeline_plan():
        try:
            step(run_date=run_date, run_id=run_id, load_type=load_type, **kwargs)
        except Exception as exc:  # noqa: BLE001 — reported, then a non-zero exit code
            steps.record_failure(run_date, run_id, load_type, task_id, steps.error_message(exc))
            return 1
    return 0


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-date", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument("--load-type", choices=LOAD_TYPES, default=LOAD_TYPE_INCREMENTAL)
    parser.add_argument("--run-id", default=None, help="default: cli__<UTC timestamp>")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.cli", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser(
        "generate-data", help="synthetic source data (see: generate-data -h)", add_help=False
    )
    commands.add_parser("init-warehouse", help="create warehouse tables, dim_date, and views")

    step = commands.add_parser("run-step", help="run one pipeline step")
    step.add_argument("step", choices=sorted(STEPS))
    step.add_argument("--dataset", choices=DATASETS, help="required for the ingest step")
    _add_run_arguments(step)

    pipeline = commands.add_parser("run-pipeline", help="run all steps for one run date")
    _add_run_arguments(pipeline)

    commands.add_parser(
        "run-analytics", help="run analytics queries (see: run-analytics -h)", add_help=False
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # These two delegate their own argument parsing.
    if argv[:1] == ["generate-data"]:
        from scripts.generate_data import main as generate_main

        return generate_main(argv[1:])
    if argv[:1] == ["run-analytics"]:
        from src.warehouse.analytics import main as analytics_main

        return analytics_main(argv[1:])

    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(get_settings().log_level)

    if args.command == "init-warehouse":
        from src.warehouse.init_warehouse import init_warehouse

        try:
            init_warehouse(get_settings())
        except PipelineError as exc:
            logger.error("init-warehouse failed: %s", exc)
            return 1
        return 0

    run_id = args.run_id or default_run_id()
    if args.command == "run-pipeline":
        return run_pipeline(args.run_date, args.load_type, run_id)

    if args.step == "ingest" and not args.dataset:
        parser.error("run-step ingest requires --dataset")
    kwargs = {"dataset": args.dataset} if args.step == "ingest" else {}
    try:
        STEPS[args.step](run_date=args.run_date, run_id=run_id, load_type=args.load_type, **kwargs)
    except Exception:  # noqa: BLE001 — already logged by the step
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
