# Phase 4 — Data Quality

## Objective

Implement the PySpark data-quality framework: rule catalogue, rule engine, referential checks, quarantine output, validated output, quality reports, and the quality gate.

## Why This Phase Exists

Bad records (duplicates, orphans, negative amounts, invalid dates) must be caught before transformation and loading. Quality results must be measurable and explainable (quality score, rule IDs).

## Prerequisites

Phase 3 complete (paths, storage, replace semantics); Spark runs in the dev container.

## Specifications Used

03 (FR-020 – FR-025, FR-091), 05 §5, §7, 06 (entire), 07 §3–4, §8, 12 §2.

## Tasks

### Task 1 — Spark session factory
`src/common/spark.py`: `get_spark(app_name)` — `local[*]`, `spark.sql.session.timeZone=UTC`, driver memory from config, shuffle partitions small (e.g. 8), s3a settings when `STORAGE_MODE=s3` (`spark.driver.extraClassPath=$SPARK_S3A_JARS_DIR/*`, `fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider` — Hadoop 3.5 uses AWS SDK v2, whose default chain includes named profiles).

### Task 2 — Rule model and catalogue
`src/validation/rules.py`: `Rule` dataclass (rule_id, dataset, columns, severity, check type, params) and check builders returning a Spark boolean "failed" Column (`not_null`, `unique`, `in_set`, `numeric_range`, `valid_date`, `valid_timestamp`, `column_lte`, `exists_in`, `required_when`).
`src/validation/rule_catalog.py`: the 34 rules of spec 06 §2 as data.

### Task 3 — Validator
`src/validation/validator.py`: read raw CSV as strings → normalise (trim, empty→NULL, upper enums) → apply rules → array of failed rule IDs → split ERROR-failed vs valid → cast valid rows to typed schema → return `ValidationOutcome` (valid DF, invalid DF, counts per rule, warnings).
Uniqueness: `row_number()` over key ordered by `_source_row_number`; rows > 1 fail.
Referential: parent keys = current valid parent DF ∪ keys read from `processed/<parent>/` (empty if none yet). The reader (`read_processed_keys`) is implemented here — it only needs Parquet under `processed/`, which Phase 6 starts writing; tests promote `validated/` output to `processed/` to exercise it.

### Task 4 — Quarantine and validated writers
`src/validation/quarantine.py`: write invalid rows (strings + `_dq_failed_rules`, `_dq_validated_at`) and valid rows to `quarantine/` and `validated/` using temp prefix + replace.

### Task 5 — Quality report + gate
`src/validation/report.py`: build report dict (spec 06 §5), log required lines, write JSON to `reports/data_quality/…`; `enforce_threshold(report, min_score)` raises `DataQualityThresholdError`.

### Task 6 — Orchestration function
`src/validation/run_validation.py`: `validate_all(run_date, run_id, load_type)` processes datasets in dependency order (parents first), reusing valid parent DataFrames for child referential checks; returns list of reports. Gate applied after all outputs are written.

### Task 7 — Tests + commit
`feat: add data quality validation`.

## Files To Create

```text
src/common/spark.py
src/validation/rules.py, rule_catalog.py, validator.py, quarantine.py, report.py, run_validation.py
tests/conftest.py (session SparkSession fixture — modify if exists)
tests/data_quality/__init__.py
tests/data_quality/test_rule_catalog.py, test_rules.py, test_bad_record_detection.py,
                   test_quality_report.py, test_quarantine.py
```

## Files To Modify

`tests/conftest.py`, `.env.example` (`DQ_MIN_QUALITY_SCORE`, `SPARK_DRIVER_MEMORY`), `src/common/constants.py` (typed schemas if needed).

## Implementation Details

- Everything is DataFrame operations; no collecting rows to the driver except small aggregates (counts).
- Single pass: compute all rule columns, then `array_remove`/`filter` to build `_dq_failed_rules`; cache the tagged DataFrame before counting and splitting.
- Counts per rule via one aggregation (`sum(when(failed, 1))` per rule).
- Report written even when the gate fails; gate error message includes dataset, score, threshold, report path.
- WARN rules are counted/logged, never quarantined.

## Testing Strategy

- `test_rule_catalog.py`: rule IDs in code == spec list (34), severities correct.
- `test_rules.py`: each check type on small crafted DataFrames (pass + fail, NULL handling, invalid dates like `2026-02-30`, non-numeric amounts, uniqueness keeps first by row number, referential with parent from "previous processed" fixture).
- `test_bad_record_detection.py`: run validation on `data/sample/` → each manifest rule detected ≥ expected.
- `test_quality_report.py`: score formula incl. 0-record case; JSON shape; gate raises below threshold, passes at exactly 95.00.
- `test_quarantine.py`: valid + invalid = total; quarantine columns; rerun overwrites partition.

## Validation

Run validation on the generated historical raw data; confirm orders score ≈ 99.9% with log lines as spec 06 §5, quarantine counts ≥ manifest, `validated/` written.

## Expected Output

`validated/`, `quarantine/`, `reports/data_quality/` partitions for the run date; required log lines.

## Definition of Done

Master plan §6; AC-020 – AC-025, AC-027 pass; AC-026 passes with the stubbed parent reader (re-verified in Phase 6).

## Risks / Considerations

- Spark startup overhead in tests → session-scoped fixture.
- Cascade quarantine (DR-01) — assert `>=`, document.
- Don't over-generalise the rule engine: only the check types needed by spec 06.

## Dependencies

Phase 3 (paths/storage), Phase 1 (container, constants). Parent-key reader completed in Phase 6.
