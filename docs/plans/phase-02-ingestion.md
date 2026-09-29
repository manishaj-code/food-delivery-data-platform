# Phase 2 — Python Ingestion

## Objective

Build a reusable ingestion framework that reads source CSVs, validates file structure, adds metadata, writes to the raw layer (local storage for now), logs record counts, and returns a meaningful result.

## Why This Phase Exists

Ingestion is the pipeline entry point. Centralising it in one base class avoids duplicated logic across six datasets and creates the extension point for a future API source.

## Prerequisites

Phase 1 complete (config, logging, exceptions, constants, generated + sample data).

## Specifications Used

03 (FR-010 – FR-017, FR-043, FR-090), 05 §1, §3, 07 §3–4, §9–12, 12 §2.

## Tasks

### Task 1 — Storage interface + local backend
`src/common/storage.py`: `Storage` protocol (`write_text/bytes`, `read_bytes`, `exists`, `list`, `delete_prefix`, `uri_for`), `LocalStorage` rooted at `LOCAL_LAKE_PATH`, `get_storage(settings)` factory. (S3 backend added in Phase 3.)

### Task 2 — Source interface
`src/common/sources.py`: `Source` protocol (`describe()`, `read_rows()` → header + row iterator), `CsvFileSource` resolving `<dataset>_historical.csv` or `<dataset>_<date>.csv` under `SOURCE_DATA_PATH`.

### Task 3 — Base ingestion
`src/ingestion/base_ingestion.py`: `IngestionConfig` (dataset, expected_columns), `IngestionResult` dataclass, `BaseIngestion.run(run_date, load_type, run_id)`: resolve source → check exists/readable → check header exactly → stream rows adding `_ingestion_timestamp`, `_ingestion_date`, `_source_file`, `_source_row_number`, `_run_id` → write one CSV to the raw path (overwrite) → log counts → return result. Header-only → `NO_DATA`.

### Task 4 — Dataset modules
`customers_ingestion.py`, `restaurants_ingestion.py`, `delivery_partners_ingestion.py`, `orders_ingestion.py`, `payments_ingestion.py`, `delivery_ingestion.py`: each defines a small subclass/config using constants. `src/ingestion/__init__.py` exposes a registry `INGESTORS = {dataset: class}`.

### Task 5 — Temporary raw path helper
Minimal raw path function (moved into `src/common/paths.py` in Phase 3).

### Task 6 — Tests, run, commit
`feat: implement raw data ingestion`.

## Files To Create

```text
src/common/storage.py
src/common/sources.py
src/ingestion/base_ingestion.py
src/ingestion/customers_ingestion.py, restaurants_ingestion.py, delivery_partners_ingestion.py,
              orders_ingestion.py, payments_ingestion.py, delivery_ingestion.py
tests/unit/test_ingestion.py
tests/unit/test_storage.py (local backend part)
```

## Files To Modify

`src/ingestion/__init__.py` (registry), `.env.example` (`SOURCE_DATA_PATH`, `LOCAL_LAKE_PATH` if not present).

## Implementation Details

- Use the `csv` module (streaming; raw values untouched as strings). Pandas is not needed here.
- Write to a temporary file then rename/overwrite to avoid partial raw files.
- `delivery_partners_ingestion.py` is added beyond the suggested list because the dataset exists.
- Logging: `Starting <dataset> ingestion`, `Records received: n`, `Records written to raw: n` within `log_context(dataset=…)`.
- Errors wrap underlying `OSError` into `SourceFileError`/`StorageError` with dataset, path, run_id.

## Testing Strategy

- Happy path per dataset on `data/sample/` (parametrised).
- Missing file, wrong header (extra/missing/reordered column), header-only, file with quoted commas.
- Metadata columns present and row numbers sequential; source values unchanged.
- Rerun writes the same single file (overwrite).
- `caplog` asserts required log lines.

## Validation

Run ingestion for all datasets on generated historical data via a temporary script or test; inspect `lake/raw/orders/year=2026/month=09/day=29/orders.csv`.

## Expected Output

Six raw CSVs with metadata; `IngestionResult` logged per dataset.

## Definition of Done

Master plan §6; AC-005, AC-006 pass (local backend).

## Risks / Considerations

- Keep base class small; don't build a plugin framework.
- File encoding: enforce UTF-8; BOM stripped if present.

## Dependencies

Phase 1.
