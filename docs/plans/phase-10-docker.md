# Phase 10 — Docker

## Objective

Finalise and harden the container setup: production-quality pipeline and Airflow images, a complete `docker-compose.yml` with healthchecks and dependencies, `.dockerignore`, non-root users, pinned bases — giving a one-command local environment.

## Why This Phase Exists

Containers existed incrementally since Phase 1 for development. This phase makes them reproducible, secure, and documented so anyone (and CI) can build and run the platform.

## Prerequisites

Phase 9 complete (Airflow services working).

## Specifications Used

03 (FR-113, FR-126, FR-127, FR-136), 04 (NFR-014 – NFR-017), 10 §2, §6, 11 §9–10.

## Tasks

### Task 1 — Pipeline image
Finalise `Dockerfile`: pinned `python:3.12-slim` tag, install JRE 21 + minimal OS packages in one layer, `pip install --no-cache-dir -r requirements.txt`, copy only `src/`, `scripts/`, `sql/`, `pyproject.toml`; `JAVA_HOME`, `PYTHONUNBUFFERED`, non-root `app` user; `ENTRYPOINT ["python","-m","src.cli"]`, `CMD ["--help"]`; hadoop-aws jars pinned.

### Task 2 — Airflow image
Finalise `docker/airflow/Dockerfile`: pinned `apache/airflow:3.x-python3.12`, JRE 21 (as root then back to `airflow` user), project requirements installed with Airflow constraints, project code copied (mounted in dev).

### Task 3 — Compose
Finalise `docker-compose.yml`: `postgres` (healthcheck `pg_isready`, volume), `airflow-init` (depends on healthy Postgres, runs once), `airflow` (single `airflow standalone` container, port 8080, healthcheck, memory limit), `pipeline` (profile `tools` or run-only); shared `x-airflow-common` anchor; `env_file: .env`; volumes for `lake`, `data`, dags; optional read-only `~/.aws` mount documented via override file `docker-compose.aws.yml`.

### Task 4 — .dockerignore and image checks
Create `.dockerignore` (spec 11 §9); verify no `.env` in image (`docker run … ls -a`), user is non-root.

### Task 5 — Developer convenience
Document commands (README draft section); optional `Makefile`-free approach — plain documented `docker compose` commands (no extra tooling).

### Task 6 — Commit
`build: finalise docker images and compose environment`.

## Files To Create

```text
.dockerignore
docker-compose.aws.yml (AWS-mode override: ~/.aws:ro mount, AWS env)
```

## Files To Modify

`Dockerfile`, `docker/airflow/Dockerfile`, `docker-compose.yml`, `docker/postgres/init.sql`, `.env.example`, `README.md` (Docker section draft).

## Implementation Details

- Keep one Postgres container with two databases (A-06).
- Airflow LocalExecutor; `AIRFLOW__CORE__LOAD_EXAMPLES=false`; parallelism 1 (8 GB host).
- Image size: slim base, no build tools left in final layer.
- Windows: bind mounts work via WSL2; document placing the repo in the WSL filesystem for performance if needed.

## Testing Strategy

- `docker compose build` succeeds from clean cache.
- `docker compose up -d` → all healthchecks healthy within ~2 minutes; DAG visible, no import errors.
- `docker compose run --rm pipeline --help` works; `docker compose run --rm --entrypoint pytest pipeline -m "not integration"` passes.
- Full historical DAG run succeeds in the finalised environment.

## Validation

Tear down (`docker compose down -v`), rebuild from scratch, rerun historical + one incremental run.

## Expected Output

One-command environment; images free of secrets; non-root.

## Definition of Done

Master plan §6; AC-050, AC-051, AC-066 pass.

## Risks / Considerations

- TR-02, TR-03 — constraints file, memory guidance.
- Don't add a Spark master/worker cluster (A-04).

## Dependencies

Phase 9; used by Phases 11–15.
