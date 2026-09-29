"""food_delivery_pipeline — daily food delivery ETL (FR-070 – FR-076, spec 07).

start -> prepare_source_data -> ingest_<dataset> x6 -> validate_data -> transform_data
-> publish_processed -> load_warehouse -> run_dq_checks -> pipeline_summary -> success

Orchestration only: every task calls a function in ``src.pipeline.steps``. Trigger with
``{"load_type": "historical"}`` for the initial load; scheduled runs are incremental.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, chain, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from src.common.constants import DATASETS, LOAD_TYPE_INCREMENTAL, LOAD_TYPES
from src.pipeline import steps
from src.pipeline.callbacks import on_task_failure, run_step

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}


def _run(step, **kwargs):
    """Run a step; errors that a retry cannot fix fail the task immediately (FR-072)."""
    try:
        return run_step(step, get_current_context(), **kwargs)
    except Exception as exc:
        if not steps.is_retryable(exc):
            raise AirflowFailException(steps.error_message(exc)) from exc
        raise


@dag(
    dag_id="food_delivery_pipeline",
    schedule="@daily",
    # The historical load runs with logical date 2026-08-31 (end of the historical window);
    # Airflow creates no tasks for logical dates before start_date.
    start_date=datetime(2026, 8, 31, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "load_type": Param(
            LOAD_TYPE_INCREMENTAL,
            enum=list(LOAD_TYPES),
            description="historical = initial full load; incremental = the run date's files",
        )
    },
    tags=["food-delivery", "etl"],
    doc_md=__doc__,
)
def food_delivery_pipeline():
    @task
    def prepare_source_data():
        return _run(steps.prepare_source_data)

    @task
    def ingest(dataset: str):
        return _run(steps.ingest_dataset, dataset=dataset)

    @task
    def validate_data():
        return _run(steps.validate_data)

    @task
    def transform_data():
        return _run(steps.transform_data)

    @task
    def publish_processed():
        return _run(steps.publish_processed)

    @task
    def load_warehouse():
        return _run(steps.load_warehouse)

    @task
    def run_dq_checks():
        return _run(steps.run_dq_checks)

    @task
    def pipeline_summary():
        return _run(steps.pipeline_summary)

    chain(
        EmptyOperator(task_id="start"),
        prepare_source_data(),
        # Sequential, parents first (referential dependency order).
        *[ingest.override(task_id=f"ingest_{dataset}")(dataset) for dataset in DATASETS],
        validate_data(),
        transform_data(),
        publish_processed(),
        load_warehouse(),
        run_dq_checks(),
        pipeline_summary(),
        EmptyOperator(task_id="success"),
    )


food_delivery_pipeline()
