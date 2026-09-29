# Phase 3 — S3 Raw Layer

## Objective

Add the S3 storage backend, centralise lake path conventions (zones + year/month/day partitions), and guarantee partition-overwrite semantics, so ingestion writes identically to local disk and S3.

## Why This Phase Exists

The data lake is the backbone of the architecture. Path conventions and overwrite semantics must be correct before validation and Spark depend on them. AWS must remain optional, so S3 is verified with moto now and against real AWS in Phase 13.

## Prerequisites

Phase 2 complete (storage protocol, ingestion).

## Specifications Used

03 (FR-014, FR-040, FR-041, FR-043, FR-044, FR-090), 07 §4, §8, 09 §3.1, §7, 11 §7.

## Tasks

### Task 1 — Lake paths module
`src/common/paths.py`: `Zone` enum (`raw`, `validated`, `quarantine`, `processed`, `reports`), `partition_path(zone, dataset, run_date)`, `raw_file_path(dataset, run_date)`, `report_path(...)`, `audit_path(...)`, `temp_path(zone, dataset, run_id)`, `spark_uri(path)` returning `file://…` or `s3a://bucket/…`. Extends the module created in Phase 2 (`partition_path`, `raw_file_path` already exist).

### Task 2 — S3 backend
`S3Storage` in `src/common/storage.py` using boto3 (client created from settings/region; credentials from the default chain — never from code). Implements the same protocol (`write_file` → `upload_file`); `delete_prefix` uses paginated list + batch delete; errors wrapped in `StorageError(retryable=True)`.

### Task 3 — Overwrite semantics
`replace_partition(zone, dataset, run_date, writer)` pattern: write to run-scoped temp prefix → delete target partition → move/copy into place → delete temp. Used by raw writes now and by Spark outputs later.

### Task 4 — Spark S3 configuration (prepared, not yet used)
Document/pin `hadoop-aws` + AWS SDK bundle versions matching the PySpark Hadoop version; add to the container image so Phase 4 can read `s3a://`. Uses `DefaultAWSCredentialsProviderChain`.

### Task 5 — Tests + commit
`feat: add s3 raw layer and lake path conventions`.

## Files To Create

```text
src/common/paths.py
tests/unit/test_paths.py
```

## Files To Modify

`src/common/storage.py` (S3Storage, replace_partition), `src/ingestion/base_ingestion.py` (use paths + replace), `tests/unit/test_storage.py` (parametrise backends: local + moto S3), `Dockerfile` (hadoop-aws jars), `requirements-dev.txt` (moto if not yet), `.env.example` (`STORAGE_MODE`, `S3_BUCKET`, `AWS_REGION`).

## Implementation Details

- Keys use forward slashes regardless of OS.
- Partition format zero-padded: `year=2026/month=09/day=29`.
- S3 "move" = copy + delete (acceptable for small files); Spark outputs are published per-file.
- Local mode still exercises the same `replace_partition` logic.

## Testing Strategy

- `test_paths.py`: every zone path for a known date; `spark_uri` for both modes.
- `test_storage.py`: same test suite for `LocalStorage` and moto-backed `S3Storage`: write/read/list/exists/delete_prefix/replace_partition; rerun leaves one object; other partitions untouched.
- Ingestion test with `STORAGE_MODE=s3` under moto.

## Validation

Tests green for both backends; local run shows unchanged raw layout.

## Expected Output

Backend-independent raw layer; `s3://<bucket>/raw/orders/year=2026/month=09/day=29/orders.csv` under moto.

## Definition of Done

Master plan §6; AC-007 passes; AC-008 deferred to Phase 13 (tracked).

## Risks / Considerations

- TR-01 (hadoop-aws version mismatch) — verify jar versions against the PySpark build.
- Don't add MinIO/LocalStack: moto + real AWS in Phase 13 is enough.

## Dependencies

Phase 2. Real-AWS verification depends on Phase 13.
