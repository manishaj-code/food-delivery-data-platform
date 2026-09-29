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

### Task 3 — Processed readers
`src/transformation/processed_reader.py`: `read_processed_keys(dataset, key_col)` across all partitions (empty DataFrame if none), `read_processed(dataset)`; used by validation (replace Phase 4 stub) and transforms (e.g. order dates for delivery).

### Task 4 — Wire referential checks
Update `src/validation/validator.py` and `src/transformation/facts.py` to use the reader.

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
