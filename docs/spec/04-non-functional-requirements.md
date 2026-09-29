# 04 — Non-Functional Requirements

Targets are sized for a portfolio project running on the actual development laptop (Windows 11 Home, ~8 GB RAM total, Docker Desktop on WSL2 limited to ~5 GB via `.wslconfig`) and a minimal AWS `dev` environment. They are not enterprise SLAs.

| ID | Category | Requirement | How verified |
|---|---|---|---|
| NFR-001 | Performance | Historical load (~100k orders + related data) completes end-to-end locally in ≤ 30 minutes. | Timed run in Phase 15, recorded in `pipeline_run_audit`. |
| NFR-002 | Performance | A daily incremental run (~500 orders) completes in ≤ 10 minutes locally (Spark startup dominates). | Timed run. |
| NFR-003 | Performance | Unit + data-quality test suite runs in ≤ 5 minutes in CI. | GitHub Actions timing. |
| NFR-004 | Reliability | Transient failures (S3 write, DB connection) are retried by Airflow (2 retries, 5 min delay). | Forced-failure test. |
| NFR-005 | Reliability | A failed run leaves the warehouse in a consistent state (per-table transactions) and can be rerun safely. | Idempotency integration test. |
| NFR-006 | Maintainability | Modular layout (`src/common`, `ingestion`, `validation`, `transformation`, `warehouse`, `pipeline`); functions small and single-purpose; type hints on public functions. | Code review; ruff. |
| NFR-007 | Maintainability | Adding a new source dataset requires only: a schema entry, an ingestion config class, rules, a transform, and DDL — no framework changes. | Documented in `docs/pipeline.md`. |
| NFR-008 | Scalability | Processing uses PySpark so the same code could run on a cluster; volumes up to ~10× (1M orders) should work locally with more memory. | Design review (not load-tested). |
| NFR-009 | Security | No secrets in Git, images, or logs; least-privilege IAM; encryption at rest and in transit for S3/Redshift. | Spec 11 checklist. |
| NFR-010 | Observability | Every task logs start, record counts, outcome, and duration with `run_id` and `dataset` context. | Log inspection; FR-101 test. |
| NFR-011 | Observability | Every run writes audit rows (counts, quality score, duration, status). | `pipeline_run_audit` query. |
| NFR-012 | Testability | ≥ 80% line coverage for `src/common`, `src/ingestion`, `src/validation`, `src/transformation`; warehouse SQL covered by integration tests against local PostgreSQL. | `pytest --cov` report. |
| NFR-013 | Testability | Tests do not require AWS credentials or internet (moto for S3, local PostgreSQL for warehouse). | CI runs without AWS secrets. |
| NFR-014 | Reproducibility | Deterministic data generation (fixed seed); pinned dependency versions; pinned Docker base images. | Checksum test (FR-002). |
| NFR-015 | Reproducibility | A new developer can go from clone to a successful local pipeline run using only the README in ≤ 30 minutes (excluding image downloads). | README walkthrough in Phase 15. |
| NFR-016 | Portability | Runs on Windows (via Docker Desktop/WSL2), macOS, and Linux; all pipeline execution happens inside Linux containers. | Local run on Windows host. |
| NFR-017 | Portability | Switching local → AWS requires only environment variable changes (`STORAGE_MODE`, `WAREHOUSE_TYPE`, connection settings). | Phase 13 AWS run. |
| NFR-018 | Data quality | Every dataset in every run has a computed quality score; runs below `DQ_MIN_QUALITY_SCORE` (95%) stop before loading. | FR-025 test. |
| NFR-019 | Data quality | No duplicate business keys and no orphan fact rows in the warehouse. | Post-load checks (FR-026). |
| NFR-020 | Failure recovery | Any failed task can be cleared and rerun in Airflow; rerunning any past date is safe (overwrite partitions + guarded upserts). | Idempotency and stale-batch tests. |
| NFR-021 | Failure recovery | Quarantined records are retained for 90 days and can be inspected and manually replayed (documented procedure). | `docs/troubleshooting.md`. |
| NFR-022 | Cost | AWS `dev` environment costs ≤ ~USD 10/month when idle; Redshift Serverless has a usage limit; resources can be removed with `terraform destroy`. | Spec 14 cost risks; usage limit resource. |
