# Phase 15 — Documentation + Final Validation

## Objective

Produce the final README and operational documentation, the Power BI connection guide, and the interview/portfolio material; then run the final acceptance validation against spec 13 and fix any gaps.

## Why This Phase Exists

A portfolio project is only as good as its explanation. Final validation proves every requirement is met and the specs match the implementation.

## Prerequisites

Phases 1–14 complete.

## Specifications Used

All specs; especially 01, 02, 13; `project_details.md` §31, §32, §38.

## Tasks

### Task 1 — README
Sections (project_details §31): overview, business problem, architecture diagram (Mermaid), technology stack, data model, pipeline flow, local setup, environment variables, Docker setup, running the pipeline, running tests, Airflow setup, AWS setup, Terraform setup, CI/CD, monitoring, Power BI connection, future improvements. CI badge; sample outputs (quality report, analytics results).

### Task 2 — Operational docs
`docs/architecture.md`, `data-model.md`, `pipeline.md` (incl. adding a new dataset — NFR-007), `data-quality.md`, `deployment.md` (apply/destroy checklist, cost guardrails), `monitoring.md` (metrics, alarm, audit queries), `troubleshooting.md` (common failures from spec 12 §7, Windows/Docker memory, s3a, Redshift COPY errors, quarantine replay procedure), `power-bi.md` (Redshift connector, read-only user, import vs DirectQuery, page/visual mapping from spec 02 §7). Concise; link to specs rather than duplicating.

### Task 3 — Interview guide
`docs/interview-guide.md`: complete architecture explanation, data-flow walkthrough, key decisions and trade-offs (master plan §7), interview Q&A (idempotency, incremental loading, DQ, star schema grain, Redshift dist/sort keys, Airflow retries, OIDC, cost), resume bullet points, future improvements (SCD2, MWAA/ECS hosting, Glue/EMR for scale, dbt, SNS alerts, quarantine replay automation, materialised views, data contracts).

### Task 4 — Final validation run
From a clean clone: follow README only → build → generate → historical run → two incremental runs → rerun one date (idempotency) → analytics → tests → (AWS mode, if environment is up) run + metrics. Record timings (NFR-001, NFR-002) and evidence.

### Task 5 — Acceptance checklist
Tick every item in `docs/spec/13-acceptance-criteria.md` with evidence reference; fix gaps; update specs for deviations (AC-093); final secrets scan of Git history (AC-060).

### Task 6 — Commit and tag
`docs: add deployment documentation`, `docs: finalise readme and interview guide`; tag `v1.0.0`.

## Files To Create

```text
docs/architecture.md, data-model.md, pipeline.md, data-quality.md, deployment.md,
docs/monitoring.md, troubleshooting.md, power-bi.md, interview-guide.md
```

## Files To Modify

`README.md`, `docs/spec/*` and `docs/plans/*` (deviations), `docs/spec/13-acceptance-criteria.md` (checkboxes).

## Implementation Details

- Mermaid diagrams render on GitHub; reuse diagrams from specs where appropriate.
- Screenshots (Airflow graph, CloudWatch, Power BI) optional under `docs/images/` — no secrets/account IDs visible.
- Keep docs practical: commands that were actually run.

## Testing Strategy

- README walkthrough on a clean environment (AC-092).
- Link check (manual) and Mermaid render check on GitHub.

## Validation

All spec 13 items checked; `terraform destroy` executed after the demo (cost).

## Expected Output

Complete, consistent documentation; tagged release; acceptance checklist complete.

## Definition of Done

Master plan §6; AC-084, AC-090 – AC-094 pass; every AC in spec 13 checked.

## Risks / Considerations

- Documentation drift — derive from final code; IR-05.
- Don't publish AWS account IDs or IPs in docs.

## Dependencies

All previous phases.
