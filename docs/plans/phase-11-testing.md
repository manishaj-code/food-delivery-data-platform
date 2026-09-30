# Phase 11 — Testing

## Objective

Complete the automated test suite: end-to-end local pipeline test, finalised idempotency and incremental tests, required-log-message test, coverage measurement, consistent markers and fixtures — and fill any gaps from earlier phases.

## Why This Phase Exists

Unit and data-quality tests were written phase by phase. This phase proves the pieces work together and makes the suite fast and reliable enough for CI.

## Prerequisites

Phases 1–10 complete.

## Specifications Used

03 (FR-080 – FR-093, FR-101, FR-105), 04 (NFR-003, NFR-012, NFR-013), 13 (all T-verified ACs), master plan §5.

## Tasks

### Task 1 — Test infrastructure review
Consolidate fixtures in `tests/conftest.py` and `tests/integration/conftest.py`: session SparkSession (`local[1]`, UTC), temp lake dir, settings override fixture, Postgres fixture (unique schema per test module, dropped after). Register markers in `pyproject.toml`.

### Task 2 — End-to-end test
`tests/integration/test_pipeline_e2e.py`: run `run-pipeline` steps (without Airflow) on `data/sample` historical + one incremental day into temp lake + Postgres schema; assert counts, quarantine counts ≥ manifest, warehouse rows, post-load checks pass, audit rows, required log lines (FR-101).

### Task 3 — Idempotency and incremental (final)
Extend `test_idempotency.py`: rerun the full pipeline for the same date → identical lake file counts, warehouse counts, business values. Extend `test_incremental.py`: day-2 processes only day-2 records; status updates applied; stale-batch guard.

### Task 4 — Coverage and gaps
`pytest --cov=src --cov-report=term-missing`; raise coverage of `src/common`, `ingestion`, `validation`, `transformation` to ≥ 80%; add missing error-path tests (storage failures, connection failures, secret masking in logs).

### Task 5 — Speed
Ensure `pytest -m "not aws"` finishes ≤ 5 min: reduced volumes, one Spark session, no full 100k generation in default runs (mark the full-volume test `slow` and exclude in CI or keep if fast enough).

### Task 6 — Commit
`test: add pipeline integration and end-to-end tests`.

## Files To Create

```text
tests/integration/test_pipeline_e2e.py
```

**As implemented:**

- `test_pipeline_e2e.py` is the Phase 9 `test_cli_pipeline.py`, renamed and extended. The runs live in the session fixture `pipeline_runs` (`tests/integration/conftest.py`), so the e2e, idempotency, and incremental tests share one set of Spark runs: historical 2026-08-31 → daily 2026-09-01 → the same daily date again (new run_id) → 2026-09-02 with a broken orders header (exit 1, stops at `ingest_orders`). It runs on a copy of `data/sample` in a temp lake and schema `test_pipeline_e2e`.
- The e2e test checks exit codes, required log lines with run context (AC-070), raw files per dataset (AC-005), reports reconciled with the quarantine Parquet and with the bad-record manifests (AC-021/022), processed manifests carrying the last run_id (AC-012), post-load checks (AC-036), and audit rows (AC-071). It also checks warehouse rows = distinct processed business keys (AC-032). Keys rather than rows because the daily sample also updates an existing customer.
- Idempotency: `test_full_pipeline_rerun_of_a_date_changes_nothing` compares lake file counts per directory (run-scoped `_tmp/` excluded), table counts, and every business value except `updated_at` before and after the rerun. Incremental: records read per dataset = the daily file rows, and `fact_order` = historical + daily processed rows. The status-update and stale-batch cases from Phase 8 are unchanged.
- The helper `business_rows()` moved to the integration conftest. Settings overrides stay explicit (`load_settings({...})` is a pure function), so no extra fixture was needed.
- Markers: every module now has one. The new `airflow` marker is on the DAG structure tests. `tests/unit/test_metrics_publisher.py` covers AC-074 (metric errors are swallowed and logged as WARNING); Phase 14 extends it.
- Coverage: `[tool.coverage.report]` with `fail_under = 80`. Measured in the pipeline container, with the suite as it stood before these additions: total 96%. The core packages are all ≥ 92% (`common` 92–100%, `ingestion` 98–100%, `validation` 95–100%, `transformation` 97–100%). The error paths listed in Task 4 (storage failures, connection failures, secret masking) were already tested in earlier phases.
- Speed: the test SparkSession runs the JVM with `-XX:TieredStopAtLevel=1 -XX:+UseSerialGC`, which made fixtures about 40% faster. The e2e fixture keeps the steps from switching the shared session to production's 8 shuffle partitions. Result on the 8 GB dev host: `pytest -m "not aws"` = 404 passed, 4 skipped (the Airflow-only tests) in about 5m10s, just over the 5-minute target, with most of it in two fixtures (`pipeline_runs` ~100s, `validated_lake` ~85s). The full-volume generator test (`slow`, ~8s) is kept in the default run.

## Files To Modify

`tests/conftest.py`, `tests/integration/conftest.py`, `tests/integration/test_idempotency.py`, `tests/integration/test_incremental.py`, `pyproject.toml` (markers, coverage config), any test files with gaps.

## Implementation Details

- Tests must not need AWS or internet (moto, local Postgres).
- Integration tests read connection settings from env with defaults matching Compose/CI service container.
- Deterministic data only (sample dataset / seeded generator).

## Testing Strategy

This phase *is* the testing strategy consolidation (master plan §5). Final suite layout:

```text
tests/unit/           test_config, test_logging_config, test_generate_data, test_spark_smoke,
                      test_ingestion, test_storage, test_paths, test_transformations,
                      test_metric_calculations, test_dag_integrity, test_pipeline_steps,
                      test_metrics_publisher (Phase 14)
tests/data_quality/   test_rule_catalog, test_rules, test_bad_record_detection,
                      test_quality_report, test_quarantine
tests/integration/    test_processed_layer, test_warehouse_load, test_post_load_checks,
                      test_idempotency, test_incremental, test_analytics_sql, test_pipeline_e2e
```

## Validation

Full suite green in the pipeline container and against the Compose Postgres; coverage report meets target.

## Expected Output

Green `pytest -m "not aws"`; coverage ≥ 80% on core packages; timing ≤ 5 min.

## Definition of Done

Master plan §6; all "T" acceptance criteria in spec 13 have passing tests.

## Risks / Considerations

- Flaky Spark tests from parallelism → `local[1]`, deterministic ordering in assertions.
- DAG integrity test needs Airflow installed: run it inside the Airflow image in CI or mark `airflow` and run in a dedicated CI step.

## Dependencies

Phases 1–10; required by Phase 12.
