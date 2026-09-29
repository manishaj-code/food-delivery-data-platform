# Phase 5 — PySpark Transformation

## Objective

Transform validated records into clean, conformed, analytics-ready DataFrames: deduplication, null handling, type/date standardisation, status normalisation, join validation, delivery duration, and the `order_analytics` and `daily_order_metrics` datasets.

## Why This Phase Exists

The warehouse and analytics require consistent types, derived fields, and guaranteed referential integrity. PySpark is the required transformation engine.

## Prerequisites

Phase 4 complete (`validated/` output, Spark session factory).

## Specifications Used

03 (FR-030 – FR-037, FR-082), 02 §5–6 (metric definitions), 07 §6–7, 08 §5 (columns needed by facts).

## Tasks

### Task 1 — Shared cleaning functions
`src/transformation/cleaning.py`: `trim_and_nullify(df, cols)`, `normalise_upper(df, cols)`, `deduplicate(df, key, order_col)`, `to_utc_timestamp_col`, `round_decimal`. Pure DataFrame → DataFrame functions.

### Task 2 — Dimension transforms
`src/transformation/dimensions.py`: `transform_customers`, `transform_restaurants`, `transform_delivery_partners` → processed schemas (spec 07 §6) + `source_ingestion_date`, `_run_id`.

### Task 3 — Fact transforms
`src/transformation/facts.py`: `transform_orders` (order_timestamp, order_date), `transform_payments` (payment_timestamp, payment_date), `transform_delivery` (duration minutes, order_date lookup from orders batch ∪ processed orders).

### Task 4 — Join validation
`src/transformation/join_validation.py`: `assert_no_orphans(child, parent_keys, key, dataset)` using left-anti join; raises `TransformationError` with count and sample keys.

### Task 5 — Analytical datasets
`src/transformation/analytics.py`: `build_order_analytics(orders, payments, delivery, customers_keys, restaurants)` and `build_daily_order_metrics(order_analytics)` per spec 02 M-01 – M-04 (batch scope).

### Task 6 — Job entry point
`src/transformation/transform_job.py`: `run_transformations(run_date, run_id)` reads `validated/` partitions, applies transforms, returns a dict of DataFrames (writing is Phase 6). Logs `Transformation completed` with counts.

### Task 7 — Tests + commit
`feat: add pyspark transformations`.

## Files To Create

```text
src/transformation/cleaning.py, dimensions.py, facts.py, join_validation.py, analytics.py, transform_job.py
tests/unit/test_transformations.py
tests/unit/test_metric_calculations.py
```

## Files To Modify

`src/common/constants.py` (typed column map). Processed `StructType` schemas live in `src/transformation/schemas.py` so `constants.py` (used by the generator) stays PySpark-free.

**As implemented:** `src/transformation/processed_reader.py` (`read_processed`, `read_processed_keys`, `current_state`) was created in this phase, because incremental analytics need earlier customer/restaurant rows; validation now uses it too. Lookups read processed partitions before the run date only (deterministic reruns).

## Implementation Details

- Explicit `StructType` schemas for processed outputs; final `select` enforces column order and types.
- Decimals: `DecimalType(10,2)`; duration `DecimalType(8,2)`; rounding half-up.
- Parent lookups for incremental runs use the processed-layer reader interface (implemented in Phase 6; in this phase tests pass parent DataFrames explicitly).
- No UDFs unless unavoidable (performance + explainability).
- Revenue = SUCCESS payments only; AOV = revenue / paid orders; attribution by order date (spec 02 §5).

## Testing Strategy

- `test_transformations.py`: trimming/empty→NULL, status normalisation, dedup keeps first row, type casts and schema equality, UTC parsing around midnight, orphan detection raises.
- `test_metric_calculations.py`: delivery duration on known timestamps (incl. NULLs), `order_analytics.is_late` at 45/45.01 minutes, `daily_order_metrics` totals/AOV/cancellation rate on a hand-computed fixture.

## Validation

Run `run_transformations` on the Phase 4 output for the historical run; print counts/schemas via logging; spot-check a few orders end to end.

## Expected Output

In-memory DataFrames for 6 datasets + 2 analytical datasets with correct schemas; zero orphans.

## Definition of Done

Master plan §6; AC-009 (schemas, in-memory), AC-010, AC-011 pass.

## Risks / Considerations

- Keep transforms small and composable; one function per dataset.
- Watch for implicit timezone conversions — session TZ fixed to UTC.

## Dependencies

Phase 4. Writing/publishing in Phase 6.
