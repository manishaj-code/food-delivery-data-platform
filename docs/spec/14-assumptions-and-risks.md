# 14 — Assumptions and Risks

Probability / Impact scale: **L**ow, **M**edium, **H**igh.

---

## Assumptions

| ID | Assumption | Rationale / simplest practical choice |
|---|---|---|
| A-01 | The data is synthetic, India-based (8 cities, INR). | Matches requirement examples (Pune, Rahul Sharma). |
| A-02 | The development host is Windows; all Python/Spark execution runs in Linux Docker containers (Docker Desktop with WSL2 backend). | PySpark on native Windows is fragile (winutils). |
| A-03 | Compute (Airflow + Spark) runs locally in Docker even in "AWS mode"; AWS provides S3, Redshift, IAM, CloudWatch, Secrets Manager only. | Requirement limits AWS services; avoids EMR/Glue/MWAA/EC2 cost and complexity. |
| A-04 | Spark runs in local mode (`local[*]`); no Spark cluster. | Requirement §20 allows simple Spark setup; data volume is small. |
| A-05 | Redshift **Serverless** is used instead of a provisioned cluster. | Pay-per-use, no idle node cost, simple Terraform. |
| A-06 | Local warehouse is PostgreSQL 16 in the same container as the Airflow metadata DB (separate database). | Postgres already required; avoids a second DB container. |
| A-07 | `order_date` is a timestamp (date + time). | Peak-ordering-hour analysis requires time of day. |
| A-08 | Revenue = successful payments, attributed to order date and restaurant city. | Single consistent definition (spec 02 §5). |
| A-09 | Delivery partners get DQ rules and a quarantine folder although not listed in requirements. | Needed for valid `fact_delivery` keys. |
| A-10 | Late-arriving parents are quarantined; replay is manual. | Keeps incremental logic simple. |
| A-11 | Dimensions are SCD Type 1. | Simplicity; SCD2 is a future improvement. |
| A-12 | Terraform state is local by default; remote S3 backend optional (required only for CI `apply`). | Single developer; avoids bootstrap chicken-and-egg. |
| A-13 | One AWS environment (`dev`) in `ap-south-1` (configurable). | Portfolio scope; region near example cities. |
| A-14 | A `validated/` transient lake zone is added between raw and processed. | Airflow tasks exchange data via storage (spec 07 §3). |
| A-15 | Late delivery threshold = 45 minutes. | Not specified; common industry-style SLA for food delivery. |
| A-16 | Custom DQ framework instead of Great Expectations. | Requirement allows either; custom is lighter and easier to explain. |
| A-17 | Airflow 3.x (latest stable at implementation time) with LocalExecutor. | Airflow 2 reaches end of life in 2026; LocalExecutor needs no Redis/Celery. |
| A-18 | The project package is imported as `src.*` (e.g. `src.ingestion`) to match the required folder layout. | Avoids extra package nesting. |
| A-19 | Status updates to earlier orders appear as full records with the same business key in later daily files. | Simplest change-data representation for CSV sources. |
| A-20 | The Terraform-created AWS environment is created only for demonstrations and destroyed afterwards. | Cost control. |
| A-21 | AWS deployment target is a temporary sandbox account (IAM user creation not possible; temporary credentials; possible service/IAM restrictions; account wiped at session end). | User constraint. Handled by Terraform feature flags (spec 09 §3.3.1). **Update 2026-09-30:** the credentials provided are for a shared organisation account (SSO role, temporary credentials, region us-east-1, no default VPC; other teams' resources present), not a wiped sandbox, so cleanup relies on `terraform destroy`. The user deferred the real AWS apply; Phase 13 is validated but not applied. |
| A-22 | Development host: Windows 11 Home, ~8 GB RAM, Docker Desktop on the WSL2 backend (Home edition supports only WSL2; `wsl --status` shows version 2). | Verified 2026-09-29. Drives the low-memory choices in TR-03. |
| A-23 | Local folder stays `food-delivery-pipeline`; GitHub repository and project name are `food-delivery-data-platform`. | Confirmed by user. |

## Technical Risks

| ID | Risk | Impact | Probability | Mitigation |
|---|---|---|---|---|
| TR-01 | PySpark / Java / Hadoop-AWS (`s3a`) version incompatibility | H | M | Pin PySpark, Java 21 and matching `hadoop-aws` + AWS SDK bundle versions in the images; smoke test s3a read/write in Phase 3/13. |
| TR-02 | Airflow 3 dependency conflicts with PySpark/pandas | M | M | Install with Airflow constraints file; keep project deps minimal; separate app image for CLI. |
| TR-03 | Host has only ~8 GB RAM: Airflow + Postgres + Spark may exhaust memory | H | H | `.wslconfig` memory=5GB + swap; single `airflow` container running all Airflow 3 components (`airflow standalone`) instead of 3 containers; Spark driver 1 GB; LocalExecutor parallelism 1; stop Airflow when only running CLI/tests; close heavy Windows apps during full runs. |
| TR-04 | SQL dialect drift between Redshift and PostgreSQL | M | M | Isolate DDL per dialect; shared DML restricted to common subset; run DDL/load smoke test on real Redshift in Phase 13. |
| TR-05 | Redshift `TRUNCATE` implicit commit breaks transactional upsert | M | M | Use `DELETE` inside transactions; clear staging in a separate step (spec 08 §7). |
| TR-06 | Spark local-mode overwrite semantics leave partial partitions on failure | M | L | Write to run-scoped temp prefix, then publish (delete + move). |
| TR-07 | Airflow LocalExecutor running Spark in the scheduler container affects scheduling | L | M | Low parallelism; acceptable for portfolio; documented. |
| TR-08 | Windows line endings / path issues in mounted volumes | L | M | `.gitattributes` with `eol=lf` for `*.sh`, `*.py`, `*.sql`; paths built with `pathlib`/`posixpath`. |

## Data Risks

| ID | Risk | Impact | Probability | Mitigation |
|---|---|---|---|---|
| DR-01 | Cascade quarantine (quarantined order → its payment/delivery quarantined) confuses counts | L | H | Documented; tests assert `detected ≥ manifest`. |
| DR-02 | Small daily dimension files trip the 95% gate with one bad record | M | M | Increments inject bad records only into large datasets; threshold configurable. |
| DR-03 | Late-arriving parents lose child records to quarantine | M | L | Generator never produces them for clean data; manual replay documented. |
| DR-04 | Metric definitions ambiguous (revenue, city) → wrong dashboards | M | M | Single definitions in spec 02 §5; fixture-based metric tests. |
| DR-05 | Timezone mistakes shift orders across days | M | L | UTC everywhere; Spark session timezone UTC; tests at midnight boundaries. |
| DR-06 | Reruns of older dates revert newer statuses | M | L | `source_ingestion_date` guard (FR-083) + test. |

## AWS Cost Risks

| ID | Risk | Impact | Probability | Mitigation |
|---|---|---|---|---|
| CR-01 | Redshift Serverless left running long queries / frequent Power BI refresh | M | M | Usage limit resource; Power BI import mode with manual refresh; `terraform destroy` after demos. |
| CR-02 | Forgotten resources accumulate cost | M | M | All resources in Terraform; tags; destroy checklist in `docs/deployment.md`; AWS Budget alert recommended (manual). |
| CR-03 | S3 growth from repeated historical loads | L | L | Partition overwrite; lifecycle rules. |
| CR-04 | CloudWatch custom metric cardinality | L | L | Only `Environment` and `Dataset` dimensions. |
| CR-05 | Sandbox restrictions: Redshift Serverless, IAM role creation, or OIDC provider not allowed | H | M | Check allowed services before Phase 13; Terraform flags (`create_*`), `REDSHIFT_COPY_AUTH=session` fallback, provisioned-Redshift fallback decided then (spec 09 §3.3.1). |
| CR-06 | Sandbox session expires / account wiped mid-demo | M | H | One-command re-creation (apply + init-warehouse + historical run); local disposable state; capture evidence during the session. |

## Security Risks

| ID | Risk | Impact | Probability | Mitigation |
|---|---|---|---|---|
| SR-01 | Secrets committed to Git | H | M | `.gitignore`, `.env.example` only, review before commit, rotate on leak. |
| SR-02 | Publicly accessible Redshift endpoint | M | M | SG restricted to developer /32; TLS; strong managed password; destroy after demos; variable validation rejects `0.0.0.0/0`. |
| SR-03 | Over-permissive IAM (esp. GitHub deploy role) | H | M | Scoped trust policy (repo + environment); project-prefixed resources; review checklist. |
| SR-04 | Credentials in logs | M | L | Masked config; test asserting no password in logs. |
| SR-05 | AWS profile mounted into containers exposes broad credentials | M | L | Mount read-only; use a dedicated profile that assumes the pipeline role. |
| SR-06 | Sandbox temporary keys pasted into GitHub secrets or committed `.env` for convenience | H | M | Forbidden by spec 11; Terraform applied locally in sandbox mode; `deploy.yml` OIDC-only. |

## Implementation Risks

| ID | Risk | Impact | Probability | Mitigation |
|---|---|---|---|---|
| IR-01 | Phase order places Docker (10) after Airflow (9), but Airflow and Spark need containers earlier | M | H | Minimal dev container in Phase 1, Postgres in Phase 7, Airflow services in Phase 9; Phase 10 hardens/finalises (see master plan §3). |
| IR-02 | Phase order places Testing (11) after code, risking untested code | M | M | Each phase writes its own tests; Phase 11 adds integration/e2e, coverage, and gap filling. |
| IR-03 | Terraform (13) after S3/Redshift phases delays real AWS verification | M | M | Phases 3/7 verified with moto/Postgres; real AWS verification consolidated in Phase 13. |
| IR-04 | Scope creep (extra tools, dashboards, SCD2) | M | M | "Do not introduce" list; out-of-scope list in spec 01; future improvements section. |
| IR-05 | Spec/implementation drift | M | M | Update specs in the same PR as deviating code; AC-093. |
| IR-06 | Airflow 3 API changes vs. examples online (2.x) | L | M | Use Airflow 3 docs; keep DAG simple (TaskFlow `@task` + standard operators). |
