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

**As implemented:**

- `ci.yml`: four jobs as specified (spec 10 §3). `lint-test` runs `pytest -m "not aws and not airflow"` with coverage (`fail_under = 80` from `pyproject.toml`) against a `postgres:16.10` service container, and uploads `coverage.xml` + JUnit XML. `dag-integrity` builds the Airflow image (without s3a jars, GHA layer cache) and runs `tests/unit/test_dag_integrity.py` inside it with the checkout mounted. The image needs no `.env` or database for this; checked locally with a plain `docker run`. `docker-build` is a matrix over both images: build (no push), smoke test (`--help` / `airflow version`), and a non-root uid check. `terraform-validate` runs fmt/init `-backend=false`/validate with Terraform 1.16.4.
- There was no `terraform/` yet, so Phase 12 adds the minimal root that Phase 13 extends: `providers.tf` (Terraform ≥ 1.16, AWS provider `~> 6.0`, default tags), `variables.tf` (`environment`, `aws_region`), and `.terraform.lock.hcl` (AWS provider 6.66.0; linux/windows/darwin checksums). The AWS provider major moved from the spec's 5.x to the current 6.x (specs 01, 10 and plan 13 updated).
- `deploy.yml`: manual `plan`/`apply` over OIDC (`id-token: write`), with repository variables `AWS_DEPLOY_ROLE_ARN`, `AWS_REGION`, optional `TF_STATE_BUCKET` and `ALLOWED_CIDR_BLOCKS`. The S3 backend is added through a generated `backend_override.tf` only when `TF_STATE_BUCKET` is set; otherwise `apply` is skipped with a warning (A-12). `apply` runs in environment `dev` and re-plans before applying, so no plan artifact is shared. The plan job runs without an environment, so its OIDC `sub` is `ref:refs/heads/main`; spec 09 already allows that and spec 11 §5 now says so too.
- Action versions (latest majors, checked 2026-09-30): checkout v7, setup-python v7, setup-java v6, upload-artifact v7, setup-buildx v4, build-push v7, setup-terraform v4, configure-aws-credentials v6. Both workflows pass `actionlint` 1.7.12 (with shellcheck), run via Docker; actionlint is not a project dependency.
- Tasks 3–4 and the green-run validation need the GitHub repository (`food-delivery-data-platform`); see the notes below once it is pushed.

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
