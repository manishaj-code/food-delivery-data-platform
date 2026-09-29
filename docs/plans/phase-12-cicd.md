# Phase 12 — GitHub Actions CI/CD

## Objective

Automate quality gates on every push and pull request (lint, tests, Docker builds, Terraform validation) and provide a manual, OIDC-authenticated Terraform deployment workflow.

## Why This Phase Exists

CI proves that every change keeps the project working; OIDC-based deployment shows secure DevOps practice without stored cloud keys.

## Prerequisites

Phase 11 complete (fast, AWS-free test suite); GitHub repository created and pushed.

## Specifications Used

03 (FR-110 – FR-114, FR-133), 10 §1, §3, §5, 11 §5.

## Tasks

### Task 1 — `ci.yml`
Triggers `push`, `pull_request` (to `main`); `permissions: contents: read`; concurrency per ref.
Job `lint-test`: checkout → setup-python 3.12 (pip cache) → setup-java (Java 21) → install `requirements.txt` + `requirements-dev.txt` → `ruff check .` → `ruff format --check .` → `pytest -m "not aws" --cov` with `postgres:16` service container and env vars → upload coverage artifact.
Job `dag-integrity`: build Airflow image (or install Airflow with constraints) and run `tests/unit/test_dag_integrity.py`.
Job `docker-build` (needs lint-test): buildx build `Dockerfile` and `docker/airflow/Dockerfile` (no push, GHA cache).
Job `terraform-validate`: setup-terraform → `terraform fmt -check -recursive` → `terraform init -backend=false` → `terraform validate` (skipped gracefully until Phase 13 adds files, or add minimal `providers.tf` placeholder).

### Task 2 — `deploy.yml`
`workflow_dispatch` with input `action` (`plan`/`apply`); `permissions: id-token: write, contents: read`; `aws-actions/configure-aws-credentials` with `role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}`; terraform init/plan; apply job uses `environment: dev` (required reviewer) and runs only when `action == apply` and a remote backend is configured (A-12). The workflow is written now and exercised in Phase 13.

### Task 3 — Branch protection
Configure `main` protection with required checks (`lint-test`, `docker-build`, `terraform-validate`, `dag-integrity`).

### Task 4 — Prove failure detection
On a throwaway branch, introduce a failing test and a lint error → CI red; revert.

### Task 5 — Commit
`ci: add github actions workflows`.

## Files To Create

```text
.github/workflows/ci.yml
.github/workflows/deploy.yml
```

## Files To Modify

`README.md` (CI badge + section draft), `pyproject.toml` (if CI-specific pytest options needed).

## Implementation Details

- Pin action versions (major tags or SHAs).
- No secrets required by `ci.yml`; test DB password is a throwaway value defined in the workflow for the ephemeral service container (not a real secret).
- Terraform version pinned via `setup-terraform`.

## Testing Strategy

- CI run on a feature branch PR → all jobs green.
- Deliberate failure branch → red (AC-053).
- `deploy.yml` syntax validated (actionlint optional locally; not added as a dependency).

## Validation

GitHub Actions UI shows green runs on PR and on `main`; branch protection blocks merge on red.

## Expected Output

Green CI badge; deploy workflow ready for Phase 13.

## Definition of Done

Master plan §6; AC-052, AC-053 pass; AC-062 (OIDC, no key secrets) verified after Phase 13.

## Risks / Considerations

- CI time: cache pip and Docker layers.
- Spark in CI needs Java — `setup-java`.

## Dependencies

Phase 11; Phase 13 completes the deploy path.
