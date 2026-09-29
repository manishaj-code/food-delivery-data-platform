# Phase 6 — Processed Data Layer

## Objective

Persist transformation outputs as partitioned Parquet in `processed/`, publish atomically with a manifest, and provide readers for existing processed keys (used by validation referential checks and incremental transforms).

## Why This Phase Exists

The processed layer is the contract between Spark and the warehouse (COPY source), and the memory of previously loaded keys for incremental runs.

## Prerequisites

Phase 5 complete.

## Specifications Used

03 (FR-022, FR-037, FR-040 – FR-044, FR-082, FR-091), 07 §4, §6, §8, §12.

## Tasks

### Task 1 — Processed writer
`src/transformation/processed_writer.py`: `write_processed(dfs, run_date, run_id)` writes each DataFrame as Parquet (Snappy) to a run-scoped temp prefix (`coalesce` to a small number of files).

### Task 2 — Publish step
`src/transformation/publish.py`: `publish_processed(run_date, run_id)` verifies each temp output (row count > 0 or expected NO_DATA, schema matches spec), replaces the run-date partition, writes `_manifest.json` (dataset, run_id, run_date, row_count, schema, source raw partition, created_at), deletes temp. Logs counts.

**As implemented:** transform and publish are separate Airflow tasks that share only storage, so the writer (`write_processed(result, storage, settings)`) stages each dataset and writes `_staged.json` with Spark's row count last. `publish_processed(storage, run_date, run_id)` needs no Spark and no XCom: it reads the Parquet footers with pyarrow (new runtime dependency `pyarrow==25.0.1`), checks row count and schema for **all** datasets before replacing any partition, copies only `*.parquet` (no `.crc` files), writes `_manifest.json` last, and is a no-op on retry once published. Verification failures raise `TransformationError` (spec 07 §9). Spark now writes timestamps as `TIMESTAMP_MICROS` instead of legacy INT96 (`LAKE_WRITE_CONFIG` in `src/common/spark.py`, shared with the test session).

### Task 3 — Processed readers (done in Phase 5)
`src/transformation/processed_reader.py` already provides `read_processed`, `read_processed_keys`, and `current_state` (partitions before the run date), used by validation and transformation. Phase 6 only verifies them against real published partitions (incl. `_manifest.json` being ignored by the Parquet reader).

### Task 4 — Wire referential checks (done in Phase 5)
Validation and `transform_job` already use the reader.

### Task 5 — Tests + commit
`feat: add processed parquet layer`.

## Files To Create

```text
src/transformation/processed_writer.py, publish.py, processed_reader.py
tests/integration/__init__.py
tests/integration/test_processed_layer.py
```

## Files To Modify

`src/validation/validator.py`, `src/transformation/facts.py`, `src/transformation/transform_job.py` (write outputs), `tests/data_quality/test_rules.py` (referential across partitions — AC-026).

**As implemented:** the reader wiring and AC-026 tests were already done in Phases 4–5, so only `requirements.txt`, `src/common/spark.py`, `tests/conftest.py`, and `tests/sample_lake.py` (the shared fixture now stages and publishes both sample run dates) changed.

## Implementation Details

- Partition columns are not written inside files (path-based partitions only), keeping COPY simple: the warehouse loader points at one partition prefix.
- Reading keys across partitions de-duplicates keys (`distinct`).
- Manifest is small JSON written via the storage abstraction.
- Parquet column types must be COPY-compatible with Redshift (decimal, date, timestamp without tz, string).

## Testing Strategy

- `test_processed_layer.py`: write → publish → read back schema and counts; `_manifest.json` contents; rerun replaces partition (same file count); failure before publish leaves previous partition intact; keys readable across two run dates.
- Referential test: day-1 customers processed, day-2 orders reference them → valid.

## Validation

Historical run through validate → transform → publish; inspect `lake/processed/orders/year=…/` and manifest; read with pyarrow to confirm portability.

## Expected Output

`processed/<dataset>/year=YYYY/month=MM/day=DD/part-*.parquet` + `_manifest.json` for 8 datasets.

## Definition of Done

Master plan §6; AC-009, AC-012, AC-026 pass; idempotent partition replacement demonstrated.

## Risks / Considerations

- TR-06 partial partitions — covered by temp + publish.
- Reading all processed keys grows with history; acceptable at this scale (NFR-008 note).

## Dependencies

Phase 5; feeds Phase 7.
