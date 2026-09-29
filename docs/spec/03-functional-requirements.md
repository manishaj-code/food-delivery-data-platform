# 03 — Functional Requirements

Requirement IDs are stable. Ranges are reserved per area so new requirements can be added without renumbering. Priority: **M** = Must (needed for final acceptance), **S** = Should (implement where practical).

Traceability to phases, tests, and acceptance criteria: `docs/plans/00-master-implementation-plan.md` §8.

| Area | ID range |
|---|---|
| Data Generation | FR-001 – FR-009 |
| Data Ingestion | FR-010 – FR-019 |
| Data Validation | FR-020 – FR-029 |
| Data Transformation | FR-030 – FR-039 |
| Data Lake | FR-040 – FR-049 |
| Data Warehouse | FR-050 – FR-059 |
| Analytics | FR-060 – FR-069 |
| Orchestration | FR-070 – FR-079 |
| Incremental Processing | FR-080 – FR-089 |
| Idempotency | FR-090 – FR-099 |
| Monitoring | FR-100 – FR-109 |
| CI/CD | FR-110 – FR-119 |
| Infrastructure | FR-120 – FR-129 |
| Security | FR-130 – FR-139 |

---

## Data Generation

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-001 | M | `scripts/generate_data.py` generates the six source datasets as CSV files under `data/generated/<dataset>/`. | Historical mode produces ≥ 10,000 customers, 500 restaurants, 1,000 delivery partners, 100,000 orders, 100,000 payments, 100,000 delivery records (valid + injected bad records). |
| FR-002 | M | Generation is deterministic for a given `--seed`. | Two runs with the same seed and arguments produce byte-identical files (checksum test). |
| FR-003 | M | Generated data respects relationships: orders → customers, orders → restaurants, payments → orders, delivery → orders, delivery → delivery_partners. | For clean records, 100% of foreign keys resolve to an existing parent. |
| FR-004 | M | The generator injects a controlled, configurable number of bad records (NULL customer ID, duplicate order ID, negative order amount, invalid payment amount, missing restaurant, invalid date, invalid status, pickup after delivery) and writes a manifest `data/generated/_bad_records_manifest_<label>.json` (label = `historical` or the date) listing counts per rule ID. | Manifest exists; validation detects at least the manifest counts per rule (FR-021). |
| FR-005 | M | Two modes: `--mode historical` (one file set covering 2026-01-01 to 2026-08-31) and `--mode incremental --date YYYY-MM-DD` (one day of new orders, new customers, and status updates to earlier orders). | Files follow the naming convention in `05-data-model-specification.md` §3; incremental files contain only that day's new/changed records. |
| FR-006 | M | A small deterministic sample dataset (≈ 100 orders) is committed under `data/sample/` for unit tests and demos. | Tests run without generating the full dataset. |
| FR-007 | S | Timestamps follow realistic patterns (lunch/dinner peaks, delivery 15–60 min, mostly `DELIVERED` statuses). | Peak-hours query ranks 12–14h and 19–22h highest on generated data. |

## Data Ingestion

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-010 | M | A reusable `BaseIngestion` class implements the ingestion flow; dataset-specific modules (`customers_ingestion.py`, `restaurants_ingestion.py`, `orders_ingestion.py`, `payments_ingestion.py`, `delivery_ingestion.py`, `delivery_partners_ingestion.py`) only declare configuration (dataset name, expected columns, source pattern). | No duplicated read/write/log logic across dataset modules. |
| FR-011 | M | Ingestion reads the source file(s) for the requested run: the dated file for `incremental`, the historical file for `historical`. | Correct file selected for a given `run_date` and `load_type`. |
| FR-012 | M | Basic file-structure validation: file exists, is readable, header matches the expected column list exactly (names and order). A header-only file is valid (0 records). | Missing file → `SourceFileError`; wrong header → `SchemaValidationError`; task fails without retry. |
| FR-013 | M | Adds metadata columns `_ingestion_timestamp` (UTC), `_ingestion_date`, `_source_file`, `_source_row_number` (1-based, used for deterministic de-duplication), `_run_id`. Source values are not modified. | Raw output = source columns (unchanged strings) + 5 metadata columns. |
| FR-014 | M | Writes the result to the raw layer at `raw/<dataset>/year=YYYY/month=MM/day=DD/<dataset>.csv`, where the date is the run's ingestion date. | File exists at the expected path in local and S3 mode. |
| FR-015 | M | Logs records received/written and returns an `IngestionResult` (dataset, status, records_read, records_written, output_path, duration_seconds). | Result returned and logged; status ∈ {`SUCCESS`, `NO_DATA`}. |
| FR-016 | M | Errors are raised as typed exceptions with context (dataset, path, run_id); never swallowed. | Unit tests cover missing file, bad header, storage write failure. |
| FR-017 | S | Reading is behind a `Source` interface (`CsvFileSource` implemented) so an API source can be added later without changing `BaseIngestion`. | Documented extension point; ingestion depends only on the interface. |

## Data Validation

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-020 | M | A declarative rule catalogue defines every rule in `06-data-quality-specification.md` (rule ID, dataset, column, check type, severity). | Catalogue contents match spec 06 one-to-one (test compares IDs). |
| FR-021 | M | The PySpark validator evaluates all rules for a dataset and tags each record with the list of failed rule IDs. | Injected bad records from the manifest are all detected. |
| FR-022 | M | Referential rules check parents against (valid records of the current batch) ∪ (records already in the processed layer). | An order referencing a customer loaded on an earlier day is valid. |
| FR-023 | M | Records failing any `ERROR` rule are written to `quarantine/<dataset>/year=/month=/day=/` with `_dq_failed_rules`, `_dq_validated_at`, `_run_id`. Valid records are written to `validated/<dataset>/…` (typed Parquet). | valid + quarantined = total records read, for every dataset. |
| FR-024 | M | A quality report is produced per dataset: total, valid, invalid, quality score, per-rule failure counts; logged in the required format and persisted as JSON at `reports/data_quality/year=/month=/day=/<dataset>.json`. | Report format matches spec 06 §5. |
| FR-025 | M | Quality gate: if a dataset's quality score is below `DQ_MIN_QUALITY_SCORE` (default 95.0), the validation task fails and downstream tasks do not run. | Test with a dataset below threshold raises `DataQualityThresholdError`. |
| FR-026 | M | Post-load warehouse checks: row-count reconciliation (processed vs loaded), business-key uniqueness, no orphan/null surrogate keys in facts, non-negative amounts (all ERROR); AOV reconciliation between PySpark `daily_order_metrics` and SQL (WARN). Checks WQ-001–WQ-006 in spec 06 §6. | Any failed ERROR check fails the `run_dq_checks` task. |

## Data Transformation

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-030 | M | Main transformations use PySpark DataFrame APIs (no row-by-row Python loops). | Code review + tests run with a local SparkSession. |
| FR-031 | M | Deduplication: within a batch keep one record per business key (lowest `_source_row_number`, i.e. first occurrence — consistent with DQ uniqueness rules). | No duplicate business keys in processed output. |
| FR-032 | M | Null handling: trim strings, convert empty strings to NULL, apply documented defaults only to optional descriptive fields (e.g. `email` NULL stays NULL; no invented values for keys or amounts). | Tests for empty-string → NULL and trimming. |
| FR-033 | M | Type conversion and date standardisation: cast to types in spec 05; parse timestamps as UTC `timestamp`, dates as `date`. | Processed Parquet schema matches spec 07 §6 exactly. |
| FR-034 | M | Status normalisation: statuses and payment methods upper-cased and trimmed before validation of allowed values. | `" delivered "` → `DELIVERED`. |
| FR-035 | M | Join validation: facts are joined to their dimensions/parents; any unmatched record fails the transformation (should be impossible after validation). | Orphan count asserted = 0. |
| FR-036 | M | `delivery_duration_minutes = (delivery_time − pickup_time)` in minutes (2 decimals); NULL when either time is NULL. | Unit test with known timestamps. |
| FR-037 | M | Produce analytical datasets: `order_analytics` (one row per order enriched with customer city, restaurant city/cuisine, payment and delivery info) and `daily_order_metrics` (per order date: total orders, paid orders, revenue, AOV, cancellation rate). | Datasets written to processed layer; AOV test with known inputs. |

## Data Lake

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-040 | M | Lake layout: `raw/`, `validated/`, `processed/`, `quarantine/`, `reports/` under one bucket (`S3_BUCKET`) or local root (`LOCAL_LAKE_PATH`). | Paths match `07-data-pipeline-specification.md` §4. |
| FR-041 | M | Date partitioning `year=YYYY/month=MM/day=DD` by ingestion (run) date for all batch datasets. | Example `processed/orders/year=2026/month=09/day=29/`. |
| FR-042 | M | Processed data stored as Parquet (Snappy). | Files readable by Spark and pyarrow. |
| FR-043 | M | A storage abstraction (`LocalStorage`, `S3Storage`) exposes the same operations and path layout; selected by `STORAGE_MODE`. | Same tests pass for both backends (S3 via moto). |
| FR-044 | M | Writing a partition replaces that partition only (overwrite semantics per run date). | Rerun leaves one copy of the data; other partitions untouched. |

## Data Warehouse

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-050 | M | DDL creates schema, 4 dimensions, 3 facts, 7 staging tables, and `pipeline_run_audit` as specified in spec 08. Idempotent (`CREATE … IF NOT EXISTS`). | Running DDL twice succeeds; tables match spec. |
| FR-051 | M | `dim_date` populated for 2025-01-01 → 2027-12-31 with `date_key = YYYYMMDD`. | 1,095 rows; no gaps. |
| FR-052 | M | Staging load: Redshift uses `COPY … FORMAT AS PARQUET` with an IAM role; local PostgreSQL loads the same Parquet via the Python loader. | Staging row count = processed row count for the batch. |
| FR-053 | M | Dimensions are upserted (SCD Type 1) with UPDATE-then-INSERT; existing surrogate keys are preserved. | Updating a customer's city keeps the same `customer_key`. |
| FR-054 | M | Facts are upserted by business key; surrogate keys resolved by joining dimensions on business keys. | No NULL dimension keys after load. |
| FR-055 | M | Each table upsert runs in a single transaction; failure rolls back the table load. | Simulated failure leaves target unchanged. |
| FR-056 | M | `WAREHOUSE_TYPE=postgres` runs the same model on local PostgreSQL (dialect-specific DDL only). | Integration tests load and query local warehouse. |

## Analytics

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-060 | M | 15 analytical queries in `sql/analytics/` (list in spec 02 §4). | Each query runs on the local warehouse without error and returns rows on generated data. |
| FR-061 | M | Six Power BI views: `vw_daily_orders`, `vw_daily_revenue`, `vw_restaurant_performance`, `vw_delivery_performance`, `vw_customer_summary`, `vw_payment_summary`. | Views created by DDL; columns match spec 08 §8. |
| FR-062 | M | Metric logic follows spec 02 §5–6 exactly. | Integration test with a hand-computed fixture checks each metric. |
| FR-063 | M | Power BI connection to Redshift documented (connector, read-only user, import vs DirectQuery). | `docs/power-bi.md` exists. |

## Orchestration

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-070 | M | Airflow DAG `food_delivery_pipeline` in `airflow/dags/food_delivery_pipeline.py` with tasks: `start → prepare_source_data → ingest_customers → ingest_restaurants → ingest_delivery_partners → ingest_orders → ingest_payments → ingest_delivery → validate_data → transform_data → publish_processed → load_warehouse → run_dq_checks → pipeline_summary → success`. | DAG integrity test asserts task IDs and dependencies. |
| FR-071 | M | DAG contains orchestration only; each task calls a function in `src/pipeline/steps.py`. | No business logic (transform/SQL) in the DAG file. |
| FR-072 | M | `retries=2`, `retry_delay=5 minutes` by default; non-retryable errors (schema, DQ threshold) fail immediately. | Configured in `default_args`; tested via DAG test. |
| FR-073 | M | Tasks receive the logical date (`ds`) and `run_id` and include them in logs and outputs. | Log lines contain `run_id=` and `run_date=`. |
| FR-074 | M | An `on_failure_callback` logs a failure summary and emits a `PipelineFailure` metric. | Forced failure produces the log line and metric call. |
| FR-075 | M | `pipeline_summary` logs and persists per-dataset counts, quality scores, durations, and final status. | Audit rows exist for the run. |
| FR-076 | M | Schedule `@daily`, `catchup=False`, `max_active_runs=1`; DAG param `load_type` ∈ {`incremental` (default), `historical`}. | Manual trigger with `historical` performs the initial load. |

## Incremental Processing

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-080 | M | A daily run processes only that run date's source files (new orders by `order_date` + changed records). | Records read ≈ daily volume, not full history. |
| FR-081 | M | Historical load mode loads the full historical file set to build the initial warehouse. | Warehouse populated from empty state. |
| FR-082 | M | Transformation and warehouse load process only the current run's partitions; dimensions needed for lookups are read from the warehouse/processed layer. | Daily load duration independent of history size (within reason). |
| FR-083 | M | Changed records (e.g. order status updates) are applied by upsert; a record from an older batch never overwrites a newer one (`source_ingestion_date` guard). | Re-running an older date does not revert a newer status. |

## Idempotency

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-090 | M | Raw write for (dataset, run date) overwrites the same partition. | Rerun → one raw file per partition. |
| FR-091 | M | Validated, quarantine, processed, and report outputs overwrite their run-date partition. | Rerun → identical file counts. |
| FR-092 | M | Warehouse upserts never create duplicate business keys. | Running the same date twice → identical row counts and values (except `updated_at`). |
| FR-093 | M | Audit rows for a (run_id, dataset, stage) are replaced, not appended, on rerun. | Single audit row per key. |

## Monitoring

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-100 | M | Python standard `logging`, one central configuration, format `timestamp - LEVEL - logger - [run_id=… dataset=…] message`. No `print` in `src/`. | ruff rule `T201` enabled. |
| FR-101 | M | Required log messages emitted (spec 12 §2). | Integration test captures them. |
| FR-102 | M | `pipeline_run_audit` table records per run/dataset/stage metrics. | Rows present after each run. |
| FR-103 | S | In AWS mode, publish CloudWatch custom metrics (namespace `FoodDelivery/Pipeline`); in local mode metrics are logged only. | moto/stub test for publisher; `METRICS_ENABLED` flag. |
| FR-104 | S | CloudWatch alarm on `PipelineFailure ≥ 1`. | Defined in Terraform. |
| FR-105 | M | Pipeline summary with total duration and final status. | Log line `Pipeline completed successfully` or failure summary. |

## CI/CD

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-110 | M | `.github/workflows/ci.yml` on `push` and `pull_request`: checkout → setup Python 3.12 → install deps → ruff lint + format check → pytest → Docker build. | Workflow green on main. |
| FR-111 | M | CI job runs `terraform fmt -check` and `terraform validate` (no AWS credentials needed). | Fails on unformatted Terraform. |
| FR-112 | S | `.github/workflows/deploy.yml` (manual `workflow_dispatch`) authenticates via GitHub OIDC and runs `terraform plan`, then `apply` behind a protected `dev` environment approval. | No AWS keys stored in GitHub. |
| FR-113 | M | Docker images (application, Airflow) build in CI. | `docker build` succeeds. |
| FR-114 | M | Git workflow: GitHub Flow, conventional commit messages, one commit per logical change. | Commit history follows spec 10 §1. |

## Infrastructure

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-120 | M | Terraform in `terraform/`: `providers.tf`, `main.tf`, `variables.tf`, `outputs.tf`, `s3.tf`, `iam.tf`, `redshift.tf`, `cloudwatch.tf`. | `terraform validate` passes. |
| FR-121 | M | S3 bucket with public access block, SSE-S3 encryption, TLS-only policy, lifecycle rules (`validated/` 7 days, `quarantine/` 90 days). | Plan shows resources; checked in AWS after apply. |
| FR-122 | M | IAM: pipeline role/policy (S3 prefix access, Secrets Manager read, CloudWatch put), Redshift S3-read role, GitHub OIDC deploy role — each behind a `create_*` flag so the code works in the sandbox account (spec 09 §3.3.1). | Least-privilege policies per spec 11; `terraform validate` passes with flags on and off. |
| FR-123 | M | Redshift Serverless namespace + workgroup (base 8 RPU) with admin password managed in Secrets Manager and a usage limit. | Workgroup reachable from allowed CIDR only. |
| FR-124 | S | CloudWatch log group with retention and failure alarm. | Present after apply. |
| FR-125 | M | Variables: `environment`, `aws_region`, `bucket_name`, `database_name`, `allowed_cidr_blocks`, `github_repository`; outputs: bucket name, Redshift endpoint, role ARNs, secret ARN. | No hardcoded values in resources. |
| FR-126 | M | `Dockerfile` for the pipeline application (Python 3.12 + Java 21 + PySpark). | Image runs `python -m src.cli --help`. |
| FR-127 | M | `docker-compose.yml` with PostgreSQL (Airflow metadata + local warehouse databases), Airflow services, and the pipeline application. | `docker compose up` → Airflow UI healthy; DAG visible. |

## Security

| ID | P | Requirement | Acceptance criteria |
|---|---|---|---|
| FR-130 | M | No secrets in Git, Docker images, or source code. `.gitignore` excludes `.env`, Terraform state, `*.tfvars`, generated data, lake folder. `.env.example` contains placeholders only. | Repository scan shows no credentials. |
| FR-131 | M | All configuration via environment variables loaded by `src/common/config.py`; in AWS mode the Redshift password is read from Secrets Manager (`REDSHIFT_SECRET_ARN`). | Missing required variable → `ConfigError` naming the variable. |
| FR-132 | M | IAM least privilege (no `*` actions on `*` resources). | Policy review checklist in spec 11. |
| FR-133 | M | GitHub Actions uses OIDC (`id-token: write`) to assume the deploy role. | No `AWS_ACCESS_KEY_ID` secret in the repo. |
| FR-134 | M | S3: block public access, encryption at rest, TLS-only bucket policy. | Terraform + AWS console check. |
| FR-135 | M | Redshift: credentials never logged; access restricted by security group CIDR; read-only user for Power BI. | Logs contain no passwords (test for secret masking). |
| FR-136 | M | `.dockerignore` excludes `.env`, `.git`, data, Terraform state. AWS credentials are mounted read-only at runtime, never copied. | Image inspection shows no `.env`. |
