# 13 — Acceptance Criteria

The project is complete when every item below is checked. Each item is measurable and names how it is verified. IDs are stable and referenced by the traceability matrix in `docs/plans/00-master-implementation-plan.md`.

Verification keys: **T** = automated test, **R** = pipeline run evidence (logs/audit/query), **I** = inspection (file/console/review).

---

### Data Engineering

- [ ] **AC-001** — `generate_data.py --mode historical --seed 42` produces all six datasets with ≥ 10,000 customers, 500 restaurants, 1,000 partners, 100,000 orders, 100,000 payments, 100,000 deliveries. (T: `test_generate_data.py`)
- [ ] **AC-002** — Two generator runs with the same seed produce identical file checksums. (T)
- [ ] **AC-003** — All clean generated foreign keys resolve to existing parents; bad-record manifest exists. (T)
- [ ] **AC-004** — Incremental mode produces dated files for all six datasets containing only that day's new/changed records. (T)
- [ ] **AC-005** — Ingestion writes `raw/<dataset>/year=/month=/day=/<dataset>.csv` with source columns unchanged + 5 metadata columns, for all six datasets. (T: `test_ingestion.py`)
- [ ] **AC-006** — Ingestion rejects a missing file and a wrong header with typed errors, and handles a header-only file as `NO_DATA`. (T)
- [ ] **AC-007** — The same ingestion code writes to local storage and to S3 (moto) with identical paths. (T: `test_storage.py`)
- [ ] **AC-008** — In AWS mode, raw objects appear in the real S3 bucket after a run. (R/I, Phase 13)
- [ ] **AC-009** — PySpark transformations produce processed Parquet for all six datasets + `order_analytics` + `daily_order_metrics` with the schemas in spec 07 §6. (T: `test_transformations.py`)
- [ ] **AC-010** — Processed output has no duplicate business keys and no orphan child records. (T)
- [ ] **AC-011** — `delivery_duration_minutes` and AOV match hand-calculated expected values on fixtures. (T)
- [ ] **AC-012** — Each processed partition has a `_manifest.json` with row count and run_id. (T)

### Data Quality

- [ ] **AC-020** — The rule catalogue contains exactly the 34 rule IDs of spec 06. (T: `test_rule_catalog.py`)
- [ ] **AC-021** — Every injected bad record type in the manifest is detected (detected ≥ manifest count per rule). (T: `test_bad_record_detection.py`)
- [ ] **AC-022** — For every dataset: valid + quarantined = total records read. (T)
- [ ] **AC-023** — Quarantine records contain `_dq_failed_rules` and original values. (T)
- [ ] **AC-024** — Quality report JSON and required log lines are produced with the correct score formula. (T: `test_quality_report.py`)
- [ ] **AC-025** — A dataset below 95% quality fails validation and blocks downstream tasks. (T)
- [ ] **AC-026** — Referential checks accept parents from earlier processed partitions. (T)
- [ ] **AC-027** — Historical generated `orders` quality score is ≥ 99% and < 100%. (R)

### Data Warehouse

- [ ] **AC-030** — DDL creates 4 dims, 3 facts, staging tables, audit table; running DDL twice succeeds (both PostgreSQL and Redshift). (T: `test_warehouse_load.py`; I on Redshift)
- [ ] **AC-031** — `dim_date` has 1,095 rows (2025-01-01 → 2027-12-31) with `YYYYMMDD` keys. (T)
- [ ] **AC-032** — After historical load, warehouse row counts equal processed row counts per table. (R/T)
- [ ] **AC-033** — Loading the same partition twice leaves row counts and business values unchanged (idempotent). (T: `test_idempotency.py`)
- [ ] **AC-034** — Updating a dimension attribute keeps its surrogate key; an order status update changes the existing fact row. (T: `test_incremental.py`)
- [ ] **AC-035** — Reloading an older batch does not overwrite newer data (stale-batch guard). (T)
- [ ] **AC-036** — Post-load checks WQ-001–WQ-004, WQ-006 pass after historical and incremental runs. (R)
- [ ] **AC-037** — Data is loaded into Redshift Serverless via COPY in AWS mode. (R/I, Phase 13)

### Orchestration

- [ ] **AC-040** — `food_delivery_pipeline` DAG imports without errors and has the task IDs and dependencies of FR-070. (T: `test_dag_integrity.py`)
- [ ] **AC-041** — DAG default args: retries = 2, retry_delay = 5 min; schedule `@daily`, `catchup=False`, `max_active_runs=1`. (T)
- [ ] **AC-042** — A manual historical run completes successfully in Airflow (all tasks green). (R, screenshot)
- [ ] **AC-043** — Two consecutive incremental runs process only their date's records (records_in ≈ daily volume). (R)
- [ ] **AC-044** — A forced transient failure is retried and then succeeds; a quality-gate failure is not retried. (R/T)
- [ ] **AC-045** — Task logs include `run_id` and `run_date`. (R)
- [ ] **AC-046** — The DAG file contains no transformation/SQL logic (only calls to `src.pipeline.steps`). (I)

### DevOps

- [ ] **AC-050** — `docker compose up` starts Postgres and Airflow healthy; the DAG is visible in the UI. (R)
- [ ] **AC-051** — The pipeline image builds and `docker compose run --rm pipeline --help` works. (R/T in CI)
- [ ] **AC-052** — `ci.yml` runs on push and PR: lint, tests, Docker build, Terraform validate — all green on `main`. (I)
- [ ] **AC-053** — CI fails when a test fails or ruff reports issues (demonstrated on a branch). (I)
- [ ] **AC-054** — `terraform validate` and `terraform fmt -check` pass; `terraform plan` shows S3, IAM, Redshift Serverless, CloudWatch resources. (T in CI / R)
- [ ] **AC-055** — `terraform apply` creates the environment; `terraform destroy` removes it. (R)
- [ ] **AC-056** — Git history contains conventional commits, at least one per phase, no single giant commit. (I)

### Security

- [ ] **AC-060** — No secrets in the repository (manual scan of history + `.env`, `*.tfvars`, `*.tfstate` ignored). (I)
- [ ] **AC-061** — `.env.example` exists with placeholders only and documents every variable. (I)
- [ ] **AC-062** — GitHub has no AWS access-key secrets; `deploy.yml` authenticates via OIDC only. If the sandbox blocks OIDC provider creation, the OIDC resources pass `terraform validate` and are documented as not applied in the sandbox. (I)
- [ ] **AC-063** — S3 bucket blocks public access, is encrypted, and denies non-TLS requests. (I after apply)
- [ ] **AC-064** — The Redshift password is read from Secrets Manager in AWS mode and never appears in logs. (T: `test_config.py` masking; R)
- [ ] **AC-065** — IAM policies pass the least-privilege checklist (spec 11 §2). (I)
- [ ] **AC-066** — Docker images contain no `.env` or credentials and run as non-root. (I)

### Monitoring

- [ ] **AC-070** — Required log messages (spec 12 §2) appear in a successful run. (T/R)
- [ ] **AC-071** — `pipeline_run_audit` contains one row per run × dataset × stage with counts, score, duration, status. (T/R)
- [ ] **AC-072** — In AWS mode, CloudWatch shows `RecordsIngested`, `RecordsRejected`, `RecordsProcessed`, `DataQualityScore`, `PipelineDurationSeconds`. (R, screenshot)
- [ ] **AC-073** — A forced failure logs `Pipeline failed at task …`, emits `PipelineFailure`, and puts the alarm into ALARM (AWS mode). (R)
- [ ] **AC-074** — Metric publishing errors do not fail the pipeline. (T)

### Analytics

- [ ] **AC-080** — All 15 analytics queries run on the warehouse without error and return rows on generated data. (T: `test_analytics_sql.py`)
- [ ] **AC-081** — The six `vw_*` views exist with the columns in spec 08 §8. (T)
- [ ] **AC-082** — Metrics on a hand-built fixture match expected values: total orders, revenue, AOV, cancellation rate, payment success rate, avg delivery time, late deliveries, repeat customers. (T)
- [ ] **AC-083** — Peak-hours query ranks lunch/dinner hours highest on generated data. (R)
- [ ] **AC-084** — Power BI can connect with the read-only user and read the views (documented steps; screenshot optional). (I)

### Documentation

- [ ] **AC-090** — README covers the 18 sections of `project_details.md` §31 including Mermaid diagrams. (I)
- [ ] **AC-091** — `docs/architecture.md`, `data-model.md`, `pipeline.md`, `data-quality.md`, `deployment.md`, `monitoring.md`, `troubleshooting.md`, `power-bi.md` exist and match the specs. (I)
- [ ] **AC-092** — Following the README on a clean machine runs the local pipeline successfully (NFR-015). (R)
- [ ] **AC-093** — Specs and plans are updated to reflect any implementation deviations (no contradictions). (I)
- [ ] **AC-094** — Interview guide (architecture explanation, data flow, Q&A, resume bullets, future improvements) exists. (I)
