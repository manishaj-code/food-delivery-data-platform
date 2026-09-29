# 12 — Monitoring Specification

Covers FR-100 – FR-105, NFR-010, NFR-011.

---

## 1. Observability Layers

| Layer | Local mode | AWS mode |
|---|---|---|
| Logs | Python `logging` → stdout → Airflow task logs (UI) and `docker compose logs` | Same + optional Airflow CloudWatch remote logging to `/food-delivery/<env>/pipeline` |
| Metrics | `METRIC name=value dims…` log lines | CloudWatch custom metrics `FoodDelivery/Pipeline` |
| Pipeline status | Airflow UI (DAG/task states), `pipeline_run_audit` | Same + CloudWatch alarm |
| Data quality | JSON reports in `reports/`, audit table | Same |

## 2. Logs

Format and context: spec 07 §11. Levels: `INFO` normal progress, `WARNING` WARN-rule failures / retries / reconciliation mismatches, `ERROR` failures (with traceback via `logger.exception`).

**Required log messages** (exact prefixes, tested by FR-101):

```text
INFO - Starting <dataset> ingestion
INFO - Records received: <n>
INFO - Records written to raw: <n>
INFO - Starting validation for <dataset>
INFO - Dataset: <dataset>
INFO - Total Records: <n>
INFO - Valid Records: <n>
INFO - Invalid Records: <n>
INFO - Quality Score: <score>%
INFO - Transformation completed
INFO - Redshift load completed          # also used in local postgres mode, with "(warehouse_type=postgres)"
INFO - Post-load checks passed
INFO - Pipeline completed successfully
ERROR - Pipeline failed at task <task_id>: <error>
```

## 3. Metrics

Namespace `FoodDelivery/Pipeline`; dimensions `Environment`, `Dataset` (where applicable). Published by `src/common/metrics.py` (`MetricsPublisher` interface: `CloudWatchMetricsPublisher`, `LogMetricsPublisher`).

| Metric | Unit | Emitted by | Meaning |
|---|---|---|---|
| RecordsIngested | Count | ingestion | Records written to raw per dataset. |
| RecordsRejected | Count | validation | Records quarantined per dataset. |
| RecordsProcessed | Count | transform/publish | Records written to processed per dataset. |
| DataQualityScore | Percent | validation | Quality score per dataset. |
| PipelineDurationSeconds | Seconds | pipeline_summary | Total run duration. |
| PipelineSuccess | Count | pipeline_summary | 1 per successful run. |
| PipelineFailure | Count | on_failure_callback | 1 per failed task. |

Metric publishing failures are logged as WARNING and never fail the pipeline.

## 4. Pipeline Run Audit (`pipeline_run_audit`)

| Column | Type | Description |
|---|---|---|
| run_id | VARCHAR(250) | Airflow run ID or CLI-generated ID |
| run_date | DATE | Logical date |
| load_type | VARCHAR(20) | `historical` / `incremental` |
| dataset | VARCHAR(50) | Dataset or `_pipeline` |
| stage | VARCHAR(30) | `prepare_source`, `ingestion`, `validation`, `transformation`, `publish`, `warehouse_load`, `post_load_checks`, `pipeline` (one per DAG step, so every failed step has a row) |
| status | VARCHAR(20) | `SUCCESS`, `NO_DATA`, `FAILED` |
| records_in | BIGINT | Records read |
| records_out | BIGINT | Records written/loaded |
| records_rejected | BIGINT | Quarantined (validation) |
| quality_score | DECIMAL(5,2) | Validation only |
| started_at / finished_at | TIMESTAMP | UTC |
| duration_seconds | DECIMAL(10,2) | For steps that process all datasets together (validation, transformation, publish, warehouse load), the step's elapsed time when the dataset's row was written — not a per-dataset timing. |
| error_message | VARCHAR(1000) | Sanitised, no secrets |

Grain: one row per `run_id` × `dataset` × `stage`; replaced on rerun (FR-093). Stages before the warehouse is reachable still produce audit records: they are written to `reports/audit/…/<stage>__<dataset>.json` in the lake and inserted into the table by `pipeline_summary` (so audit works even if the warehouse load fails later). Steps that cover several datasets write one row per dataset; a step-level failure is recorded with dataset `_pipeline` (removed when a retry of the step starts). On a failed run the failure callback writes the `pipeline` row with status `FAILED` (`error_message` = `<task_id>: <error>`) and, when the warehouse is reachable, inserts the run's records too.

Useful queries (documented in `docs/monitoring.md`): last 7 runs status; quality score trend per dataset; slowest stages.

## 5. Monitored Signals

| Signal | Where to see it |
|---|---|
| Pipeline status | Airflow UI; `stage = 'pipeline'` audit row; `PipelineSuccess`/`PipelineFailure` |
| Record counts | Logs, audit, `RecordsIngested`/`RecordsProcessed` |
| Invalid records | Quality report, quarantine partition, `RecordsRejected` |
| Processing duration | Audit `duration_seconds`, `PipelineDurationSeconds`, Airflow task durations |
| Failures | Airflow failed tasks, `ERROR` logs, `PipelineFailure`, alarm |
| Data-quality score | Report JSON, audit, `DataQualityScore` |

## 6. CloudWatch Usage

- **Metrics:** as §3 (AWS mode, `METRICS_ENABLED=true`).
- **Alarm:** `food-delivery-<env>-pipeline-failure`: `PipelineFailure` Sum ≥ 1 over 1 day (period 86400 s, `treat_missing_data = notBreaching`). Optional SNS email topic is a documented extension (not required).
- **Optional alarm:** `DataQualityScore` Minimum < 95 for dataset `orders`.
- **Log group:** `/food-delivery/<env>/pipeline`, retention 30 days.
- **Dashboard (optional, Terraform):** records ingested/rejected per dataset, quality score, duration, failures.

## 7. Failure Scenarios

| Failure | Detection | Behaviour | Retry | Data state | Operator action |
|---|---|---|---|---|---|
| **Ingestion fails** (missing file, bad header) | `SourceFileError`/`SchemaValidationError` in `ingest_<dataset>` | Task fails immediately; downstream skipped; audit `FAILED`; `PipelineFailure` metric; alarm (AWS) | No | Raw partition for the failed dataset not written (others may be) — safe because reruns overwrite | Fix/restore source file, clear task in Airflow |
| **Ingestion fails** (S3/storage error) | `StorageError` | Airflow retries 2× (5 min); then as above | Yes | Same | Check AWS credentials/connectivity |
| **Validation fails** (quality gate) | `DataQualityThresholdError` | Quarantine + report written first; task fails; no transformation/load | No | Warehouse unchanged | Inspect report/quarantine, fix source, rerun date |
| **Validation fails** (Spark/storage error) | `TransformationError`/`StorageError` | Retry 2× | Yes | Run-scoped temp output discarded | Check memory/logs |
| **Transformation fails** | `TransformationError` (incl. join-validation orphans) | Retry 2× for runtime errors; orphans fail after retries; processed partition not published (temp prefix) | Yes | Previous processed data intact; warehouse unchanged | Check logs; fix bug; clear task |
| **Redshift loading fails** (connection) | `WarehouseConnectionError` | Retry 2× | Yes | Per-table transaction rolled back; earlier tables may be loaded — safe, rerun is idempotent | Check workgroup status, SG CIDR, secret |
| **Redshift loading fails** (SQL/data error) | `WarehouseLoadError` | Fail without retry; ROLLBACK of current table | No | Consistent per table; rerun safe | Inspect error (e.g. `STL_LOAD_ERRORS` / `SYS_LOAD_ERROR_DETAIL` in Redshift), fix, rerun |
| **Post-load check fails** | `DataQualityThresholdError` from `run_dq_checks` | Task fails; summary marks run FAILED | No | Data loaded but flagged | Investigate check; fix and rerun date |

In all cases the `on_failure_callback` logs `Pipeline failed at task <task_id>` with run context and emits `PipelineFailure`.
