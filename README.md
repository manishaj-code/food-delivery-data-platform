# food-delivery-data-platform

> An automated end-to-end data engineering platform for processing food delivery orders, customers, restaurants, payments, and delivery data using Python, PySpark, AWS S3, Amazon Redshift, Airflow, Docker, Terraform, and GitHub Actions.

**Status:** Phase 3 of 15 complete — synthetic data, raw ingestion, local/S3 lake storage. The full README is written in Phase 15.

## Documentation

- Specifications (source of truth): [`docs/spec/`](docs/spec/)
- Implementation plan and traceability: [`docs/plans/00-master-implementation-plan.md`](docs/plans/00-master-implementation-plan.md)

## Architecture (target)

```mermaid
flowchart LR
    SRC[Source CSV] --> ING[Python ingestion] --> RAW[(S3 raw)]
    RAW --> VAL[PySpark validation] --> QUA[(quarantine)]
    VAL --> ETL[PySpark transformation] --> PROC[(S3 processed Parquet)]
    PROC --> RS[(Redshift star schema)] --> BI[Analytics / Power BI]
    AF{{Airflow}} -.orchestrates.-> ING & VAL & ETL & RS
```

## Quick start (Phase 1)

Requirements: Docker Desktop (WSL2 backend on Windows).

```bash
cp .env.example .env                       # optional in Phase 1; never commit .env
docker compose build pipeline

# Generate the full historical dataset (~100k orders) into data/generated/
docker compose run --rm pipeline python -m scripts.generate_data --mode historical

# Generate one daily increment
docker compose run --rm pipeline python -m scripts.generate_data --mode incremental --date 2026-09-01

# Lint and tests
docker compose run --rm pipeline sh -c "ruff check . && ruff format --check . && pytest tests/unit"
```

A small deterministic sample dataset (historical + 2026-09-01 + 2026-09-02) is committed in [`data/sample/`](data/sample/).
