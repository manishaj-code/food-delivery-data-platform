# Phase 1 — Project Foundation + Synthetic Data

## Objective

Create the repository skeleton, central configuration, logging, exception hierarchy, a minimal Linux dev container, and a deterministic synthetic data generator producing historical and daily-increment datasets with controlled bad records.

## Why This Phase Exists

Every later phase needs consistent configuration, logging, reference values, a runtime that can run PySpark, and realistic, reproducible data (including bad records) to test against.

## Prerequisites

- Git, Docker Desktop running (WSL2 backend — confirmed), a GitHub account.
- `%UserProfile%\.wslconfig` limiting WSL2 to `memory=5GB`, `swap=4GB` (host has ~8 GB RAM; TR-03) — created in Task 6 with the user's approval, then `wsl --shutdown` and restart Docker Desktop.
- Specs approved.

## Specifications Used

01 (structure, stack), 03 (FR-001 – FR-007, FR-100, FR-114, FR-126, FR-130, FR-131), 04 (NFR-014, NFR-016), 05 (entire), 10 §1–2, 11 §4, §9, 14 (A-02, A-18).

## Tasks

### Task 1 — Initialise repository
`git init`, default branch `main`, `.gitignore` (spec 11 §9), `.gitattributes` (`eol=lf` for `*.py`, `*.sql`, `*.sh`, `*.yml`, `*.tf`), README skeleton, create folder structure from spec 01 §11 with `.gitkeep` where needed.

### Task 2 — Python project configuration
`pyproject.toml` (project metadata, ruff config incl. `T201` no-print, pytest config: `testpaths`, `pythonpath = ["."]`, markers `unit/spark/integration/aws`), `requirements.txt` (runtime, pinned: pyspark, pandas, pyarrow, boto3, psycopg2-binary, python-dotenv), `requirements-dev.txt` (pytest, pytest-cov, moto, ruff). Verify versions are Python 3.12-compatible.

### Task 3 — Common modules
- `src/common/config.py`: frozen dataclass `Settings` loaded from env (spec 11 §4), validation, secret masking, `get_settings()` cached.
- `src/common/logging_config.py`: `configure_logging()`, context filter (`run_id`, `run_date`, `dataset` via `contextvars`), `log_context(...)` context manager.
- `src/common/exceptions.py`: `PipelineError` → `ConfigError`, `SourceFileError`, `SchemaValidationError`, `StorageError`, `DataQualityThresholdError`, `TransformationError`, `WarehouseConnectionError`, `WarehouseLoadError`; attribute `retryable`.
- `src/common/constants.py`: dataset names, source column lists, enumerations, cities, cuisines, thresholds (`LATE_DELIVERY_THRESHOLD_MINUTES = 45`).

### Task 4 — Synthetic data generator
`scripts/generate_data.py` (argparse CLI + importable functions): `--mode historical|incremental`, `--date`, `--seed` (default 42), `--output-dir`, volume overrides. Uses `random.Random(seed)` and static name lists (no Faker). Implements spec 05 §6 consistency rules and timestamp patterns (lunch/dinner peaks), writes CSV per spec 05 §1/§3, injects bad records per spec 05 §7, writes `_bad_records_manifest_<label>.json`. Incremental mode derives new IDs deterministically from the date and includes status updates of earlier in-flight orders.

### Task 5 — Sample dataset
Generate a small dataset (≈ 100 orders, 50 customers, 10 restaurants, 20 partners, a handful of bad records of each type, plus one incremental day) into `data/sample/` and commit it.

### Task 6 — Minimal dev container
Initial `Dockerfile` (python:3.12-slim + openjdk-21-jre-headless + requirements) and `docker-compose.yml` with a `pipeline` service mounting the repo. Enough to run the generator and pytest (including a Spark smoke test) on Windows. Finalised in Phase 10.

### Task 7 — `.env.example`
All variables of spec 11 §4 with placeholders and comments.

### Task 8 — Tests and commit
Write tests; run in container; commit `chore: initialise project structure` and `feat: add synthetic food delivery data generator`.

## Files To Create

```text
.gitignore, .gitattributes, .env.example, README.md (skeleton)
pyproject.toml, requirements.txt, requirements-dev.txt
Dockerfile (minimal), docker-compose.yml (pipeline service only)
src/__init__.py
src/common/__init__.py, config.py, logging_config.py, exceptions.py, constants.py
src/{ingestion,validation,transformation,warehouse,pipeline}/__init__.py (empty packages)
scripts/__init__.py, scripts/generate_data.py
data/sample/<dataset>/... , data/generated/.gitkeep
tests/__init__.py, tests/conftest.py
tests/unit/test_config.py, test_logging_config.py, test_generate_data.py
tests/unit/test_spark_smoke.py (SparkSession starts in container)
```

## Files To Modify

None (new repository).

## Implementation Details

- Config: required vars depend on mode (e.g. `S3_BUCKET` only when `STORAGE_MODE=s3`; `REDSHIFT_PASSWORD` or `REDSHIFT_SECRET_ARN`). Unknown mode values → `ConfigError`.
- Generator performance: 100k rows with the `csv` module in < 1 minute; stream rows rather than building huge in-memory structures where easy.
- Determinism: never iterate over sets/dicts with non-deterministic order for generation; fixed base timestamp; no `datetime.now()` in generation.
- Bad records: selected by the seeded RNG, recorded with their rule ID; duplicates copy an existing row (optionally with changed amount).
- Historical window: 2026-01-01 → 2026-08-31; customers' `signup_date` ≤ their first order date.

## Testing Strategy

- `test_config.py`: defaults, missing required var → `ConfigError` naming variable, password masked in `repr`.
- `test_logging_config.py`: format includes context fields; no duplicate handlers on repeated configure.
- `test_generate_data.py`: volumes (using reduced volume args for speed + one slow full-volume test marked), determinism checksum, FK integrity of clean rows, manifest counts, incremental mode contents, peak-hour distribution.
- `test_spark_smoke.py`: create SparkSession `local[1]`, simple DataFrame count.

## Validation

- `docker compose run --rm pipeline python scripts/generate_data.py --mode historical` → six CSV folders + manifest; row counts printed via logging.
- `docker compose run --rm --entrypoint pytest pipeline tests/unit` passes.
- `ruff check . && ruff format --check .` pass.

## Expected Output

`data/generated/*/…_historical.csv` (≈ 100k orders), `_bad_records_manifest_<label>.json`, committed `data/sample/`, green unit tests.

## Definition of Done

Master plan §6, plus: AC-001 – AC-004 pass; Spark smoke test passes inside the container; `.env.example` complete; repository has first commits.

## Risks / Considerations

- TR-01 (Spark/Java versions) — pin now. TR-08 (line endings) — `.gitattributes`.
- Keep the generator readable: small functions per entity; avoid over-parameterisation.

## Dependencies

None (first phase). Unblocks all phases.
