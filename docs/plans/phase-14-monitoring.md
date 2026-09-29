# Phase 14 — Monitoring

## Objective

Complete observability: CloudWatch metrics publisher (with a log-only local mode), full audit coverage, failure callback metrics, CloudWatch failure alarm and optional dashboard, and monitoring queries.

## Why This Phase Exists

A production-style pipeline must show whether it ran, what it processed, how good the data was, how long it took, and when it failed — without reading raw logs.

## Prerequisites

Phase 13 complete (log group exists; AWS mode works); Phase 9 hooks (steps, callback, audit JSON).

## Specifications Used

03 (FR-100 – FR-105, FR-074), 04 (NFR-010, NFR-011), 09 §3.4, 12 (entire).

## Tasks

### Task 1 — Metrics publisher
`src/common/metrics.py`: `MetricsPublisher` protocol; `LogMetricsPublisher` (`METRIC name=value …`); `CloudWatchMetricsPublisher` (boto3 `put_metric_data`, namespace `FoodDelivery/Pipeline`, dimensions `Environment`, `Dataset`; batched; exceptions logged as WARNING and swallowed *only* here, by design — AC-074); `get_metrics_publisher(settings)` factory by `METRICS_ENABLED`.

### Task 2 — Emit metrics
Wire into steps: `RecordsIngested` (ingest), `RecordsRejected` + `DataQualityScore` (validate), `RecordsProcessed` (publish), `PipelineDurationSeconds` + `PipelineSuccess` (summary), `PipelineFailure` (callback).

### Task 3 — Audit completeness
Ensure every stage writes audit JSON (success and failure) and `pipeline_summary` inserts all into `pipeline_run_audit` idempotently; failure path: if the DAG fails before summary, the next successful run (or `run-step pipeline_summary`) can backfill audit rows for the failed run — documented.

### Task 4 — CloudWatch alarm and dashboard
Extend `terraform/cloudwatch.tf`: `PipelineFailure` alarm (spec 12 §6), optional `DataQualityScore` alarm for orders, optional dashboard (ingested/rejected/processed, score, duration, failures). Outputs for alarm name/dashboard URL.

### Task 5 — Optional log shipping
Document (and optionally enable) Airflow remote logging to the CloudWatch log group using the Amazon provider configuration via env vars in `docker-compose.aws.yml`.

### Task 6 — Monitoring queries
`sql/analytics/monitoring/recent_runs.sql`, `quality_trend.sql`, `slowest_stages.sql` against `pipeline_run_audit`.

### Task 7 — Verify and commit
Forced failure in AWS mode → alarm ALARM state; successful run → metrics visible. Commit `feat: add cloudwatch monitoring and pipeline metrics`.

## Files To Create

```text
src/common/metrics.py
sql/analytics/monitoring/recent_runs.sql, quality_trend.sql, slowest_stages.sql
tests/unit/test_metrics_publisher.py
```

## Files To Modify

`src/pipeline/steps.py`, `src/pipeline/callbacks.py`, `src/warehouse/audit.py`, `terraform/cloudwatch.tf`, `terraform/outputs.tf`, `docker-compose.aws.yml`, `.env.example` (`METRICS_ENABLED`), `tests/integration/test_pipeline_e2e.py` (audit assertions).

## Implementation Details

- Metrics are best-effort; they never change pipeline outcome.
- Keep metric cardinality low (CR-04).
- No SNS by default; README explains adding an email subscription.

## Testing Strategy

- `test_metrics_publisher.py`: log publisher output format; CloudWatch publisher calls with correct namespace/dimensions (moto or botocore Stubber); publisher error → WARNING, no exception.
- e2e test asserts audit rows per stage and required log lines.
- Manual AWS verification with screenshots.

## Validation

AC-070 – AC-074 checked; CloudWatch console shows metrics for the latest run; alarm transitions on forced failure and back to OK after a success (treat missing data not breaching).

## Expected Output

CloudWatch metrics + alarm (+ optional dashboard); complete audit table; monitoring queries.

## Definition of Done

Master plan §6; AC-070 – AC-074 pass.

## Risks / Considerations

- Cost of custom metrics is small; avoid per-rule metrics.
- Swallowing exceptions is allowed only in the metrics publisher, documented explicitly (requirement: "do not silently swallow" — it is logged).

## Dependencies

Phases 9 and 13.
