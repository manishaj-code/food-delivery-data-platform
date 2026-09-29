# Phase 9 — Airflow

## Objective

Orchestrate the full pipeline with an Airflow 3 DAG that calls thin step functions, supports historical and incremental loads, retries, logging with run context, failure handling, and a pipeline summary — plus a CLI that runs the same steps without Airflow.

## Why This Phase Exists

Automation, scheduling, retries, and visibility are what turn scripts into a pipeline.

## Prerequisites

Phases 2–8 complete; Postgres service exists (Phase 7).

## Specifications Used

03 (FR-070 – FR-076, FR-080, FR-081, FR-093, FR-101, FR-105), 07 §3, §5, §9–11, 10 §2, 12 §2, §4, §7.

## Tasks

### Task 1 — Pipeline step API
`src/pipeline/steps.py`: `prepare_source_data`, `ingest_dataset`, `validate_data`, `transform_data`, `publish_processed`, `load_warehouse`, `run_dq_checks`, `pipeline_summary`. Each accepts `run_date`, `run_id`, `load_type`; sets logging context; measures duration; writes stage audit JSON (`reports/audit/…`); returns a small JSON-serialisable dict; converts non-retryable `PipelineError`s to a `NonRetryableError` flag the DAG maps to `AirflowFailException`.

### Task 2 — CLI
`src/cli.py` (argparse): `generate-data`, `init-warehouse`, `run-step <step> [--dataset]`, `run-pipeline --run-date --load-type`, `run-analytics --query`. Used by the `pipeline` container and for local debugging.

### Task 3 — DAG
`airflow/dags/food_delivery_pipeline.py`: TaskFlow `@task` wrappers calling `steps.*`; tasks and order per FR-070 (`start` and `success` as `EmptyOperator`); `default_args` (owner, retries=2, retry_delay=5 min, `on_failure_callback`); `schedule="@daily"`, `start_date=2026-08-31` (see "As implemented"), `catchup=False`, `max_active_runs=1`, `params={"load_type": Param("incremental", enum=[…])}`, tags. Wrapper maps non-retryable errors to `AirflowFailException`.

### Task 4 — Failure callback
`src/pipeline/callbacks.py`: `on_task_failure(context)` logs `Pipeline failed at task <task_id>` with run context, writes FAILED audit JSON, calls metrics publisher (log publisher until Phase 14).

### Task 5 — Airflow services in Compose
Add `airflow-init` and a single `airflow` service (`airflow standalone`, all components in one container — sized for the 8 GB host, TR-03) using a first version of `docker/airflow/Dockerfile` (Airflow 3 + Java 21 + project deps with Airflow constraints); `airflow` database in `docker/postgres/init.sql`; mount `airflow/dags`, `src`, `sql`, `lake`, `data`.

### Task 6 — Run and verify
Trigger historical run; then incremental runs for two consecutive dates (generator creates increments in `prepare_source_data`).

### Task 7 — Tests + commit
`feat: add airflow orchestration`.

## Files To Create

```text
src/pipeline/steps.py, callbacks.py
src/cli.py
airflow/dags/food_delivery_pipeline.py
docker/airflow/Dockerfile
tests/unit/test_dag_integrity.py, test_pipeline_steps.py
```

**As implemented:**

- **`start_date` is 2026-08-31, not 2026-09-01.** The historical run uses logical date 2026-08-31 (end of the historical window) so every daily run sees it as an earlier partition. Airflow creates no task instances for logical dates before `start_date`; a historical run triggered for 2026-08-31 against `start_date=2026-09-01` "succeeded" with zero tasks during verification.
- Steps take `run_date`, `run_id`, `load_type` and share one context manager (`_step`): log context, timing, stage audit JSON, `FAILED` record + single `logger.exception` on error, re-raise. `is_retryable(exc)` classifies errors; the DAG maps non-retryable ones to `AirflowFailException` (`airflow.sdk.exceptions`). Heavy modules (Spark, validation, transformation) are imported inside the steps so DAG parsing stays fast.
- Audit stages are one per step: `prepare_source` and `publish` were added to spec 12 §4. `pipeline_summary` refuses to mark a run successful if any stage record is `FAILED`. The failure callback (`record_failure`) also inserts the run's audit rows into the warehouse when reachable, so failed runs are visible in `pipeline_run_audit`.
- `src/pipeline/callbacks.py` has no Airflow imports (context read as a mapping): `run_arguments` (logical date, or `run_after` for Airflow 3 runs without one), `run_step`, `on_task_failure` (reports the original error behind `AirflowFailException`).
- `src/common/metrics.py`: log publisher only (`METRIC name=value …`); CloudWatch in Phase 14.
- `prepare_source_data` generates a missing daily increment with the `full` profile, seed 42; a missing historical file or a partial daily file set fails without retry.
- No `docker/postgres/init.sql`: `airflow-init` runs `docker/airflow/bootstrap.py db` (creates the `airflow` role/database if missing — also on existing volumes) and `airflow db migrate`. The `airflow` service runs `bootstrap.py auth` (SimpleAuthManager password file from `.env`) and `airflow standalone`.
- Airflow image: Java 21 copied from `eclipse-temurin:21-jre` (bookworm base has no Java 21 package); boto3 left at Airflow's constrained version (our pin conflicted with the Amazon provider's `aiobotocore`); `pip check` at build. The s3a jars are not in this image yet (Phase 10, needed for AWS mode).
- Airflow runs as uid 1000 (`AIRFLOW_UID`), the pipeline image's `app` user: with uid 50000 it could not write the uid-1000 folders in the mounted `lake/`/`data/`.
- The CLI's `generate-data` and `run-analytics` delegate to the existing entry points; `run-pipeline` runs the DAG's task list in-process without retries and calls `record_failure` on the first failure.
- Tests: `tests/unit/test_pipeline_steps.py`, `tests/unit/test_cli.py`, `tests/unit/test_dag_integrity.py` (structure tests run in the Airflow image; the orchestration-only check everywhere), `tests/integration/test_cli_pipeline.py` (historical → daily → broken-header run on the sample data; required log messages, audit rows, exit codes).

## Files To Modify

`docker-compose.yml`, `docker/postgres/init.sql`, `.env.example` (Airflow vars: admin user/password, Fernet key, secret key, `AIRFLOW_UID`), `src/validation/run_validation.py`/`loader.py` (if step integration needs small adapters).

## Implementation Details

- DAG file imports only `src.pipeline.steps`/`callbacks` and Airflow; no business logic (FR-071).
- `run_date = ds` (logical date); `run_id` from context; both passed explicitly.
- Ingest tasks are sequential per the required DAG shape (customers → restaurants → delivery_partners → orders → payments → delivery), matching referential dependency order.
- XCom carries only small dicts (counts, status, paths).
- `pipeline_summary` collects stage audit JSONs for the run, inserts them into `pipeline_run_audit`, logs the summary and `Pipeline completed successfully`.
- LocalExecutor parallelism 1 to protect memory (8 GB host).

## Testing Strategy

- `test_dag_integrity.py` (requires Airflow installed in test env — run in Airflow image or skip if not importable): DAG loads without import errors; task IDs; upstream/downstream chain; default args; schedule/catchup/max_active_runs; params.
- `test_pipeline_steps.py`: each step with mocked dependencies returns expected dict, sets log context, maps non-retryable errors; failure callback logs and calls metrics.
- CLI `run-pipeline` on `data/sample` locally (used later by e2e test).

## Validation

Airflow UI at `localhost:8080`: historical run all green; incremental runs process ~500 orders; forced failure (e.g. rename a source file) → fails without retry; forced transient error → retried.

## Expected Output

Green DAG runs, task logs with `run_id=`/`run_date=`, audit rows per stage, summary log.

## Definition of Done

Master plan §6; AC-040 – AC-046 pass; AC-043 evidence recorded.

## Risks / Considerations

- TR-02 dependency conflicts (Airflow constraints) — keep the app image separate.
- TR-03/TR-07 memory — low parallelism.
- IR-06 Airflow 3 API differences — use current docs (Context7) during implementation.

## Dependencies

Phases 2–8; finalised by Phase 10 (images) and Phase 14 (metrics).
