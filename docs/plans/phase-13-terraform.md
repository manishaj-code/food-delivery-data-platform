# Phase 13 — Terraform

## Objective

Define the AWS `dev` environment as code (S3, IAM, Redshift Serverless, CloudWatch base resources, GitHub OIDC), apply it, and run the pipeline against real S3 and Redshift for the first time.

## Why This Phase Exists

Infrastructure as code makes the AWS setup reproducible, reviewable, and destroyable. This phase also closes the deferred real-AWS verifications from Phases 3 and 7.

## Prerequisites

Phase 12 complete; AWS account with an admin-capable profile for the first apply; Terraform ≥ 1.6 installed (or run via `hashicorp/terraform` container); developer public IP known.

## Specifications Used

03 (FR-052, FR-112, FR-120 – FR-125, FR-131 – FR-135), 04 (NFR-017, NFR-022), 09 (entire), 10 §4, 11 (entire), 14 (CR-*, SR-*).

## Tasks

### Task 0 — Sandbox capability check (A-21)
Before writing resources: with the sandbox profile, confirm allowed actions — create S3 bucket, create IAM role, create OIDC provider, Redshift Serverless namespace/workgroup, Secrets Manager, CloudWatch — and session length. Set defaults of `create_pipeline_role`, `create_redshift_s3_role`, `create_github_oidc` accordingly; if Redshift Serverless is blocked, decide the provisioned fallback with the user (spec 09 §3.3.1).

### Task 1 — Provider and base
`providers.tf` (required_version, aws ~> 6.0, default_tags — created with `variables.tf` `environment`/`aws_region` and `.terraform.lock.hcl` in Phase 12; extend them), `main.tf` (locals: name prefix, account id via `aws_caller_identity`, default VPC/subnets data sources), `variables.tf` (with descriptions, types, defaults, validation: environment in [dev], CIDR not `0.0.0.0/0`), `outputs.tf`, `terraform.tfvars.example`.

### Task 2 — S3
`s3.tf`: bucket, `aws_s3_bucket_public_access_block`, SSE-S3 encryption, ownership controls, TLS-only bucket policy, lifecycle rules (validated 7d, quarantine 90d, reports 365d, abort multipart 7d).

### Task 3 — IAM
`iam.tf`: pipeline role + policy (trust: account principal/specified ARN), Redshift S3-read role, GitHub OIDC provider + deploy role (trust conditions: aud + sub for repo/environment), least-privilege policies (spec 09 §3.3).

### Task 4 — Redshift Serverless
`redshift.tf`: namespace (`manage_admin_password = true`, db name, IAM role), security group (5439 from `allowed_cidr_blocks`), workgroup (base 8 RPU, publicly accessible, default subnets), usage limit.

### Task 5 — CloudWatch base
`cloudwatch.tf`: log group `/food-delivery/<env>/pipeline` (30-day retention). Alarm/dashboard added in Phase 14.

### Task 6 — Apply and configure
`terraform init/plan/apply`; record outputs; create `.env` AWS values (`STORAGE_MODE=s3`, `WAREHOUSE_TYPE=redshift`, `S3_BUCKET`, `REDSHIFT_HOST`, `REDSHIFT_SECRET_ARN`, `REDSHIFT_IAM_ROLE_ARN`); configure AWS profile to assume the pipeline role.

### Task 7 — First AWS run
`init-warehouse` on Redshift (DDL + dim_date + views); run historical pipeline via Airflow with `docker-compose.aws.yml`; verify S3 objects, COPY loads, post-load checks, analytics queries. Run `aws`-marked smoke tests.

### Task 8 — OIDC deploy workflow
Only if `create_github_oidc = true` was possible in the sandbox (otherwise documented as validated-not-applied; Terraform is applied locally and sandbox keys are never stored in GitHub). Set GitHub variables (`AWS_DEPLOY_ROLE_ARN`, `AWS_REGION`); run `deploy.yml` with `plan`. Optional: configure S3 remote backend then `apply` through the approved environment.

### Task 9 — Destroy test and commit
`terraform destroy` then re-apply once to prove reproducibility (or document plan-only if cost-sensitive). Commit `feat: add terraform infrastructure`.

## Files To Create

```text
terraform/providers.tf, main.tf, variables.tf, outputs.tf, s3.tf, iam.tf, redshift.tf, cloudwatch.tf
terraform/terraform.tfvars.example
tests/integration/test_aws_smoke.py (marker aws)
```

## Files To Modify

`.env.example` (AWS-mode values documented), `docker-compose.aws.yml`, `.github/workflows/deploy.yml` (backend config if remote state chosen), `src/common/storage.py`/`spark.py`/`connection.py` only if real-AWS issues appear.

## Implementation Details

- No credentials in `.tf`; provider uses the ambient profile/OIDC.
- Bucket name default includes account ID for global uniqueness.
- Redshift admin password never output; only the secret ARN.
- `publicly_accessible = true` is a documented trade-off (SR-02) with /32 CIDR.
- IAM policy documents via `aws_iam_policy_document` data sources (readable, validated).

## Testing Strategy

- CI: `terraform fmt -check`, `validate`.
- Manual: `terraform plan` review against spec 09 checklist; least-privilege review (spec 11 §2).
- `test_aws_smoke.py`: write/read/delete an object under `raw/_smoke/`; connect to Redshift, `SELECT 1`, count `dim_date`.
- Pipeline run evidence in AWS (S3 console, Redshift query editor, audit table).

## Validation

AC-008, AC-037, AC-054, AC-055, AC-063, AC-064, AC-065 checked with evidence (screenshots/log excerpts saved for docs, no secrets).

## Expected Output

Provisioned `dev` environment; historical + one incremental run loaded into Redshift Serverless; Terraform outputs recorded.

## Definition of Done

Master plan §6 plus the ACs above; cost guardrails (usage limit) in place; destroy procedure documented.

## Risks / Considerations

- CR-01/CR-02 cost — usage limit, destroy after demo, recommend manual AWS Budget.
- TR-01 s3a credentials in containers — mounted profile + AWS SDK v2 `DefaultCredentialsProvider` (Hadoop 3.5).
- TR-04 dialect issues discovered on real Redshift — fix DDL/DML and update spec 08 if needed.

## Dependencies

Phase 12 (CI validate, deploy workflow), Phases 3 and 7 (code paths under test).
