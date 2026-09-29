# Phase 7 — Redshift Warehouse

## Objective

Create the star-schema warehouse (Redshift + PostgreSQL dialects), populate `dim_date`, load processed partitions into staging, upsert dimensions and facts idempotently, and run post-load quality checks — verified locally on PostgreSQL.

## Why This Phase Exists

The warehouse is where analysts and Power BI consume data. Correct keys, grain, and idempotent incremental loading are the core data-engineering deliverables.

## Prerequisites

Phase 6 complete (processed Parquet + manifests).

## Specifications Used

03 (FR-026, FR-050 – FR-056, FR-083, FR-092, FR-102, FR-135), 06 §6, 08 (entire), 09 §3.2, §7, 12 §4.

## Tasks

### Task 1 — Local Postgres service
Add `postgres` (16) service to `docker-compose.yml` with `docker/postgres/init.sql` creating `warehouse` database and user (password from `.env`). (Airflow DB added in Phase 9.)

### Task 2 — DDL
`sql/ddl/postgres/01_schema.sql … 05_audit.sql` and `sql/ddl/redshift/01_schema.sql … 05_audit.sql` (+ `06_powerbi_reader.sql` Redshift template). `CREATE … IF NOT EXISTS`; Redshift DISTSTYLE/DISTKEY/SORTKEY per spec 08.

### Task 3 — Connection and SQL runner
`src/warehouse/connection.py`: `get_connection(settings)` (psycopg2, `sslmode=require` for Redshift, password from env or Secrets Manager via `src/common/secrets.py`); errors → `WarehouseConnectionError`.
`src/warehouse/sql_runner.py`: load SQL file, substitute `{schema}` from validated config, execute with parameters, transaction helper.

### Task 4 — Warehouse initialisation
`src/warehouse/init_warehouse.py`: run DDL for the configured dialect; populate `dim_date` via `sql/warehouse/populate_dim_date.sql` (insert missing dates only).

### Task 5 — Staging loaders
`src/warehouse/staging_loader.py`: `RedshiftCopyLoader` (renders `sql/staging/copy_<table>.sql` with S3 partition URI + `REDSHIFT_IAM_ROLE_ARN`, `FORMAT AS PARQUET`) and `PostgresLoader` (pyarrow reads partition files → `COPY … FROM STDIN` CSV buffer). Both: `DELETE FROM stg_x` first (separate transaction), then load, then verify count = manifest count.

### Task 6 — Upserts
`sql/warehouse/upsert_dim_customer.sql`, `upsert_dim_restaurant.sql`, `upsert_dim_delivery_partner.sql`, `upsert_fact_order.sql`, `upsert_fact_payment.sql`, `upsert_fact_delivery.sql`: UPDATE (with stale-batch guard) then INSERT new keys, surrogate key lookups via dimension joins. `src/warehouse/loader.py`: `load_warehouse(run_date, run_id)` runs tables in order, each in one transaction; logs `Redshift load completed`.

### Task 7 — Post-load checks
`src/warehouse/post_load_checks.py` + `sql/warehouse/checks/*.sql`: WQ-001 – WQ-006; ERROR checks raise `DataQualityThresholdError`; WARN logged.

### Task 8 — Audit writer
`src/warehouse/audit.py`: delete-then-insert rows into `pipeline_run_audit`.

### Task 9 — Tests + commit
`feat: add redshift warehouse schema` and `feat: add warehouse loading with idempotent upserts`.

## Files To Create

```text
docker/postgres/init.sql
sql/ddl/postgres/01_schema.sql … 05_audit.sql
sql/ddl/redshift/01_schema.sql … 06_powerbi_reader.sql
sql/staging/copy_stg_customer.sql … copy_stg_daily_order_metrics.sql
sql/warehouse/populate_dim_date.sql, upsert_*.sql (6), checks/*.sql
src/common/secrets.py
src/warehouse/connection.py, sql_runner.py, init_warehouse.py, staging_loader.py, loader.py,
              post_load_checks.py, audit.py
tests/integration/conftest.py (Postgres fixture: fresh schema per test module)
tests/integration/test_warehouse_load.py, test_post_load_checks.py
tests/integration/test_idempotency.py, test_incremental.py (initial versions)
```

## Files To Modify

`docker-compose.yml` (postgres service), `.env.example` (`WAREHOUSE_TYPE`, `REDSHIFT_*`), `requirements.txt` (psycopg2-binary if not yet).

## Implementation Details

- Shared DML must only use syntax valid on both engines: `UPDATE … FROM`, `INSERT … SELECT … LEFT JOIN … WHERE … IS NULL`, `CAST`, `COALESCE`, `TO_CHAR(date,'YYYYMMDD')::INT`. Avoid `ON CONFLICT` (not in Redshift) and Redshift-only functions.
- `updated_at = GETDATE()` vs `NOW()` differs → pass load timestamp as a parameter instead.
- Stale guard: `WHERE s.source_ingestion_date >= t.source_ingestion_date`.
- Never log connection parameters that include password.
- Integration tests need Postgres: locally via Compose, in CI via service container; skipped with a clear message if `REDSHIFT_HOST` unreachable.

## Testing Strategy

- `test_warehouse_load.py`: DDL twice; dim_date 1,095 rows; load sample processed partition → counts match; no NULL dimension keys; transaction rollback on injected failure.
- `test_post_load_checks.py`: each WQ check passes on good data and fails on crafted bad data.
- `test_idempotency.py`: load same partition twice → identical counts/values (except `updated_at`).
- `test_incremental.py`: day-2 batch with customer city change keeps `customer_key`; order status update modifies existing row; reloading day-1 after day-2 doesn't revert (stale guard).

## Validation

`init-warehouse` + load historical processed partition into local Postgres; query counts vs manifests; post-load checks pass.

## Expected Output

Populated local warehouse (≈ 100k rows per fact); audit rows; `Redshift load completed (warehouse_type=postgres)`.

## Definition of Done

Master plan §6; AC-030 – AC-036 pass locally; Redshift DDL reviewed (executed for real in Phase 13 → AC-037).

## Risks / Considerations

- TR-04 dialect drift, TR-05 TRUNCATE implicit commit.
- Postgres loader memory: stream Parquet by row groups.

## Dependencies

Phase 6. Real Redshift verification in Phase 13.
