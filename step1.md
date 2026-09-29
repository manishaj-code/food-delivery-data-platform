# Specification-First Development Workflow

You are the senior Data Engineer, Cloud Engineer, DevOps Engineer, Solution Architect, and technical mentor for this project.

I have provided the complete project requirements for a portfolio-quality:

**Food Delivery Data Engineering & DevOps Platform**

Before writing ANY implementation code, I want you to follow a specification-first and planning-first development workflow.

Do NOT start coding yet.

---

# OBJECTIVE

First understand the complete project requirements, analyze the architecture, identify dependencies and implementation concerns, and create a complete set of project specification documents.

Then create a detailed phased implementation plan.

Only after the specification and planning documents are complete should implementation begin.

The specification and planning documents will become the project's source of truth.

---

# STEP 1 — UNDERSTAND THE PROJECT

Read and analyze the complete project requirements provided to you.

Understand:

* Business problem
* Data sources
* Data entities
* Data relationships
* Data ingestion
* Data lake
* Data validation
* PySpark transformation
* Redshift warehouse
* Analytics
* Airflow orchestration
* Incremental processing
* Idempotency
* Docker
* Testing
* GitHub Actions
* Terraform
* AWS security
* Monitoring
* Power BI integration
* Documentation
* Acceptance criteria

Do not make assumptions unnecessarily.

If something is ambiguous, identify it as an assumption and choose the simplest practical approach suitable for a portfolio project.

Do not introduce technologies that are not required.

---

# STEP 2 — CREATE PROJECT SPECIFICATIONS

Create the following directory:

```text
docs/spec/
```

Create these specification documents:

```text
docs/spec/
├── 01-project-overview.md
├── 02-business-requirements.md
├── 03-functional-requirements.md
├── 04-non-functional-requirements.md
├── 05-data-model-specification.md
├── 06-data-quality-specification.md
├── 07-data-pipeline-specification.md
├── 08-data-warehouse-specification.md
├── 09-aws-infrastructure-specification.md
├── 10-devops-specification.md
├── 11-security-specification.md
├── 12-monitoring-specification.md
└── 13-acceptance-criteria.md
```

---

# SPECIFICATION DOCUMENT REQUIREMENTS

## 01-project-overview.md

Document:

* Project name
* Project purpose
* Business context
* Problem statement
* Goals
* Scope
* Out of scope
* Target users
* Technology stack
* High-level architecture
* Major components
* Expected final outcome

Include a Mermaid architecture diagram.

---

## 02-business-requirements.md

Document:

* Business problem
* Business objectives
* Business questions
* Required analytics
* Key business metrics

Include metrics such as:

* Total orders
* Total revenue
* Average order value
* Cancellation rate
* Payment success rate
* Average delivery time
* Restaurant performance
* Customer activity
* Revenue by city
* Orders by cuisine
* Peak ordering periods

For every metric explain:

* Definition
* Calculation
* Source data
* Expected output

---

## 03-functional-requirements.md

Document all functional requirements.

Organize them into:

### Data Generation

### Data Ingestion

### Data Validation

### Data Transformation

### Data Lake

### Data Warehouse

### Analytics

### Orchestration

### Incremental Processing

### Idempotency

### Monitoring

### CI/CD

### Infrastructure

### Security

Each requirement should have an ID.

Example:

```text
FR-001
FR-002
FR-003
```

Use clear acceptance criteria for each important requirement.

---

## 04-non-functional-requirements.md

Document:

* Performance
* Reliability
* Maintainability
* Scalability
* Security
* Observability
* Testability
* Reproducibility
* Portability
* Data quality
* Failure recovery

Keep the requirements realistic for a portfolio project.

Do not invent unrealistic enterprise SLAs.

---

## 05-data-model-specification.md

Document the complete source data model.

Entities:

```text
customers
restaurants
orders
payments
delivery
delivery_partners
```

For every table document:

* Purpose
* Columns
* Data types
* Nullable/non-nullable
* Primary/business key
* Relationships
* Example values

Include an ER diagram using Mermaid.

Document relationships such as:

```text
customers → orders
restaurants → orders
orders → payments
orders → delivery
delivery_partners → delivery
```

---

## 06-data-quality-specification.md

Define every data-quality rule.

Examples:

```text
customer_id NOT NULL
customer_id UNIQUE
restaurant_id NOT NULL
rating BETWEEN 0 AND 5
order_amount >= 0
payment_amount >= 0
order_status valid
payment_status valid
pickup_time <= delivery_time
```

For each rule document:

* Rule ID
* Dataset
* Column
* Rule
* Severity
* Failure behavior
* Quarantine behavior

Define the quality score calculation.

---

## 07-data-pipeline-specification.md

Document the complete pipeline.

Include:

```text
Source
 ↓
Python ingestion
 ↓
S3 Raw
 ↓
Validation
 ↓
Quarantine
 ↓
PySpark
 ↓
S3 Processed
 ↓
Redshift
 ↓
Analytics
```

Document:

* Batch processing
* Incremental processing
* Historical load
* Daily load
* Idempotency
* Retry behavior
* Error handling
* Logging
* Data lineage

Include a Mermaid data-flow diagram.

---

## 08-data-warehouse-specification.md

Document the Redshift star schema.

Dimensions:

```text
dim_customer
dim_restaurant
dim_delivery_partner
dim_date
```

Facts:

```text
fact_order
fact_payment
fact_delivery
```

For each table document:

* Purpose
* Columns
* Data types
* Keys
* Grain
* Relationships
* Load strategy

Clearly define the grain of every fact table.

Document the warehouse loading strategy, including incremental/upsert behavior.

---

## 09-aws-infrastructure-specification.md

Document required AWS services:

```text
S3
Redshift
IAM
CloudWatch
Secrets Manager
```

For each service explain:

* Why it is required
* What it stores/does
* How it connects to the pipeline
* Security considerations

Document the AWS environment architecture.

Keep the infrastructure simple.

---

## 10-devops-specification.md

Document:

### Git

Branching strategy.

### Docker

Containers and local development.

### GitHub Actions

CI workflow:

```text
Checkout
 ↓
Install dependencies
 ↓
Lint
 ↓
Tests
 ↓
Docker build
```

### Terraform

Infrastructure managed through IaC.

### Deployment

Document the intended deployment workflow.

Include:

```text
Developer
 ↓
Git
 ↓
GitHub
 ↓
GitHub Actions
 ↓
Tests
 ↓
Docker
 ↓
Terraform/AWS
```

---

## 11-security-specification.md

Document:

* IAM
* Least privilege
* Secrets management
* Environment variables
* GitHub Actions OIDC
* AWS credentials
* S3 security
* Redshift credentials
* Docker secrets
* `.env.example`
* `.gitignore`

Explicitly state:

**No secrets may be committed to Git.**

---

## 12-monitoring-specification.md

Define:

* Logs
* Metrics
* Pipeline status
* Record counts
* Invalid records
* Processing duration
* Failures
* Data-quality score

Document CloudWatch usage.

Define what happens when:

* ingestion fails
* validation fails
* transformation fails
* Redshift loading fails

---

## 13-acceptance-criteria.md

Create a complete checklist.

Organize it into:

### Data Engineering

### Data Quality

### Data Warehouse

### Orchestration

### DevOps

### Security

### Monitoring

### Analytics

### Documentation

Each item should be measurable and testable.

Use checkboxes:

```text
- [ ] AC-001
- [ ] AC-002
```

---

# STEP 3 — CREATE MASTER IMPLEMENTATION PLAN

After creating the specifications, create:

```text
docs/plans/
```

Create:

```text
docs/plans/
├── 00-master-implementation-plan.md
├── phase-01-foundation.md
├── phase-02-ingestion.md
├── phase-03-s3-raw-layer.md
├── phase-04-data-quality.md
├── phase-05-pyspark-transformation.md
├── phase-06-processed-layer.md
├── phase-07-redshift.md
├── phase-08-analytics.md
├── phase-09-airflow.md
├── phase-10-docker.md
├── phase-11-testing.md
├── phase-12-cicd.md
├── phase-13-terraform.md
├── phase-14-monitoring.md
└── phase-15-documentation.md
```

---

# MASTER IMPLEMENTATION PLAN

Create:

```text
docs/plans/00-master-implementation-plan.md
```

It should contain:

* Project phases
* Phase dependencies
* Deliverables
* Technologies used
* Expected outputs
* Testing strategy
* Definition of done

Use this order:

```text
Phase 1
Project Foundation + Synthetic Data

↓

Phase 2
Python Ingestion

↓

Phase 3
S3 Raw Layer

↓

Phase 4
Data Quality

↓

Phase 5
PySpark Transformation

↓

Phase 6
Processed Data Layer

↓

Phase 7
Redshift Warehouse

↓

Phase 8
Analytics SQL

↓

Phase 9
Airflow

↓

Phase 10
Docker

↓

Phase 11
Testing

↓

Phase 12
GitHub Actions CI/CD

↓

Phase 13
Terraform

↓

Phase 14
Monitoring

↓

Phase 15
Documentation + Final Validation
```

Identify dependencies between phases.

---

# PHASE PLAN FORMAT

Every phase document must follow this structure:

```text
# Phase X — Name

## Objective

## Why This Phase Exists

## Prerequisites

## Specifications Used

## Tasks

### Task 1
### Task 2
### Task 3

## Files To Create

## Files To Modify

## Implementation Details

## Testing Strategy

## Validation

## Expected Output

## Definition of Done

## Risks / Considerations

## Dependencies
```

Do not write implementation code inside these planning documents.

The plans should describe what will be implemented and how.

---

# STEP 4 — TRACEABILITY

Create a traceability matrix inside:

```text
docs/plans/00-master-implementation-plan.md
```

Map:

```text
Requirement
    ↓
Specification
    ↓
Implementation Phase
    ↓
Test
    ↓
Acceptance Criteria
```

Example:

```text
FR-001
 ↓
03-functional-requirements.md
 ↓
Phase 2
 ↓
test_ingestion.py
 ↓
AC-001
```

Every important requirement should map to an implementation phase and acceptance criterion.

---

# STEP 5 — IDENTIFY RISKS AND ASSUMPTIONS

Create:

```text
docs/spec/14-assumptions-and-risks.md
```

Document:

### Assumptions

### Technical Risks

### Data Risks

### AWS Cost Risks

### Security Risks

### Implementation Risks

For each risk provide:

* Risk
* Impact
* Probability
* Mitigation

Keep the project practical and avoid unnecessary complexity.

---

# STEP 6 — DO NOT IMPLEMENT YET

After creating all specification and planning documents:

STOP.

Do NOT create:

* Python implementation
* Dockerfiles
* Terraform implementation
* Airflow DAG
* SQL implementation
* GitHub Actions workflows

yet.

Only create:

```text
docs/spec/
docs/plans/
```

and the necessary directories/files required to hold those documents.

---

# STEP 7 — REVIEW THE PLAN

After creating the documents, provide me with:

## Specification Summary

List every specification document created and its purpose.

## Implementation Summary

List every phase and its purpose.

## Architecture Summary

Explain the final architecture briefly.

## Key Decisions

List important architectural decisions.

## Assumptions

List assumptions made.

## Risks

List major risks.

## Questions / Decisions Needed

If something genuinely requires my decision, ask me.

Do not ask unnecessary questions.

---

# IMPORTANT RULES

1. Do not start implementation.
2. Do not generate thousands of lines of code.
3. Do not invent unnecessary requirements.
4. Do not introduce unnecessary technologies.
5. Keep the project portfolio-friendly.
6. Keep specifications consistent with each other.
7. Avoid contradictions between specification documents.
8. Maintain requirement IDs consistently.
9. Maintain phase IDs consistently.
10. Keep the implementation plan traceable to requirements.
11. Use Mermaid diagrams where useful.
12. Use Markdown.
13. Keep documentation practical and understandable.
14. Optimize for a project that I can explain confidently in an interview.

The existing project requirements are the primary source of truth.

Begin now by analyzing the requirements and creating the specification and planning documents only.

DO NOT IMPLEMENT THE PROJECT YET.
