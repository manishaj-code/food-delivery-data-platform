# Food Delivery Data Engineering & DevOps Platform

You are a senior Data Engineer, Cloud Engineer, DevOps Engineer, and software architect.

I want you to build a complete portfolio-quality **Food Delivery Data Engineering Pipeline** from scratch.

The project should simulate a real-world food delivery company's analytics data platform.

The goal is to demonstrate:

* Python
* SQL
* AWS
* S3
* PySpark
* Amazon Redshift
* Apache Airflow
* Data Quality
* Docker
* Terraform
* GitHub Actions CI/CD
* Testing
* Monitoring
* Power BI-ready datasets
* Production-style engineering practices

Do NOT over-engineer the project. Build a practical system that one developer can understand, run, test, deploy, and explain in a technical interview.

---

# 1. PROJECT NAME

Use the project name:

`food-delivery-data-platform`

Description:

> An automated end-to-end data engineering platform for processing food delivery orders, customers, restaurants, payments, and delivery data using Python, PySpark, AWS S3, Amazon Redshift, Airflow, Docker, Terraform, and GitHub Actions.

---

# 2. CORE BUSINESS PROBLEM

The fictional food delivery company receives data about:

* Customers
* Restaurants
* Orders
* Payments
* Deliveries
* Delivery partners

The company wants to understand:

* Daily order volume
* Revenue
* Average order value
* Cancelled orders
* Payment success/failure
* Restaurant performance
* Delivery performance
* Average delivery time
* Customer activity
* Orders by city
* Orders by cuisine
* Peak ordering periods

Build a data platform that automatically collects, validates, transforms, stores, and analyzes this data.

---

# 3. IMPORTANT DEVELOPMENT RULE

Build this project incrementally.

Do NOT attempt to implement everything in one step.

Use the following phases:

Phase 1 → Project foundation and synthetic data

Phase 2 → Python ingestion

Phase 3 → AWS S3 raw layer

Phase 4 → Data validation

Phase 5 → PySpark transformation

Phase 6 → Processed S3 layer

Phase 7 → Redshift warehouse

Phase 8 → SQL analytics

Phase 9 → Airflow orchestration

Phase 10 → Docker

Phase 11 → Automated tests

Phase 12 → GitHub Actions CI/CD

Phase 13 → Terraform infrastructure

Phase 14 → Monitoring and logging

Phase 15 → Documentation

After completing each phase, verify that it works before moving to the next phase.

Do not skip testing.

---

# 4. TECHNOLOGY STACK

Use:

Backend/Data Engineering:

* Python 3.12+
* PySpark
* Pandas only where appropriate
* SQL

AWS:

* Amazon S3
* Amazon Redshift
* IAM
* CloudWatch
* AWS Secrets Manager where appropriate

Orchestration:

* Apache Airflow

DevOps:

* Docker
* Docker Compose
* Terraform
* GitHub Actions

Testing:

* pytest
* Great Expectations OR a lightweight custom data-quality framework

Visualization:

* Power BI-ready Redshift tables/views

Version control:

* Git
* GitHub

---

# 5. DO NOT USE

Do not introduce these unless absolutely necessary:

* Kafka
* Kubernetes
* EKS
* Lambda-heavy architecture
* Microservices
* GraphQL
* React
* Java
* Snowflake
* Databricks

The objective is a clean and understandable Data Engineering project.

---

# 6. DATA MODEL

Create these source datasets.

## customers

Fields:

```text
customer_id
customer_name
email
city
signup_date
```

Example:

```text
C10001
Rahul Sharma
rahul@example.com
Pune
2026-01-10
```

---

## restaurants

Fields:

```text
restaurant_id
restaurant_name
city
cuisine
rating
```

Example:

```text
R1001
Spice Kitchen
Pune
Indian
4.3
```

---

## orders

Fields:

```text
order_id
customer_id
restaurant_id
order_date
order_amount
order_status
```

Possible statuses:

```text
PLACED
PREPARING
OUT_FOR_DELIVERY
DELIVERED
CANCELLED
```

---

## payments

Fields:

```text
payment_id
order_id
payment_method
payment_amount
payment_status
payment_date
```

Payment methods:

```text
UPI
CARD
CASH
WALLET
```

Payment statuses:

```text
SUCCESS
FAILED
REFUNDED
```

---

## delivery

Fields:

```text
delivery_id
order_id
delivery_partner_id
pickup_time
delivery_time
delivery_status
```

Delivery statuses:

```text
ASSIGNED
PICKED_UP
DELIVERED
FAILED
```

---

## delivery_partners

Fields:

```text
delivery_partner_id
partner_name
city
joining_date
```

---

# 7. SYNTHETIC DATA

Create a realistic synthetic data generator.

Do not depend completely on an external API for the project.

Create:

```text
scripts/generate_data.py
```

It should generate at least:

```text
10,000 customers
500 restaurants
100,000 orders
100,000 payments
100,000 delivery records
1,000 delivery partners
```

Use deterministic random generation where possible so that tests are reproducible.

Create relationships between datasets.

For example:

```text
orders.customer_id
        ↓
customers.customer_id
```

and:

```text
orders.restaurant_id
        ↓
restaurants.restaurant_id
```

and:

```text
payments.order_id
        ↓
orders.order_id
```

and:

```text
delivery.order_id
        ↓
orders.order_id
```

Introduce a small number of intentionally bad records for testing data-quality rules.

Examples:

* NULL customer ID
* Duplicate order ID
* Negative order amount
* Invalid payment amount
* Missing restaurant
* Invalid date

These records should be detected by the validation layer.

---

# 8. SOURCE DATA STRUCTURE

Create:

```text
data/
├── customers/
├── restaurants/
├── orders/
├── payments/
├── delivery/
└── delivery_partners/
```

Use CSV initially.

The project should be able to replace the CSV source with an API later.

---

# 9. PROJECT ARCHITECTURE

Use this architecture:

```text
                 SOURCE DATA
                     |
                     v
              Python Ingestion
                     |
                     v
                 AWS S3 RAW
                     |
                     v
              Data Validation
                     |
              +------+------+
              |             |
             PASS          FAIL
              |             |
              v             v
        PySpark ETL      Quarantine
              |
              v
       S3 PROCESSED
              |
              v
          Redshift
              |
       +------+------+
       |             |
       v             v
   Analytics      Power BI
```

Airflow should orchestrate the complete pipeline.

---

# 10. S3 DATA LAKE

Use this logical structure:

```text
food-delivery-data/

raw/
    customers/
    restaurants/
    orders/
    payments/
    delivery/
    delivery_partners/

processed/
    customers/
    restaurants/
    orders/
    payments/
    delivery/
    delivery_partners/

quarantine/
    customers/
    restaurants/
    orders/
    payments/
    delivery/
```

Use date partitioning where appropriate:

```text
orders/year=2026/month=09/day=29/
```

Store processed data in Parquet.

---

# 11. PYTHON INGESTION

Create reusable ingestion code.

Suggested structure:

```text
src/
├── ingestion/
│   ├── __init__.py
│   ├── base_ingestion.py
│   ├── customers_ingestion.py
│   ├── restaurants_ingestion.py
│   ├── orders_ingestion.py
│   ├── payments_ingestion.py
│   └── delivery_ingestion.py
```

The ingestion framework should:

1. Read source data
2. Validate basic file structure
3. Add ingestion timestamp
4. Write data to S3 raw layer
5. Log record counts
6. Handle errors
7. Return meaningful status

Avoid duplicating the same logic in every ingestion script.

Use reusable functions/classes where appropriate.

---

# 12. DATA QUALITY

Create a data-quality framework.

Check:

### Customers

```text
customer_id NOT NULL
customer_id UNIQUE
city NOT NULL
signup_date valid
```

### Restaurants

```text
restaurant_id NOT NULL
restaurant_id UNIQUE
rating BETWEEN 0 AND 5
```

### Orders

```text
order_id NOT NULL
order_id UNIQUE
customer_id exists
restaurant_id exists
order_amount >= 0
order_status valid
order_date valid
```

### Payments

```text
payment_id NOT NULL
payment_id UNIQUE
order_id exists
payment_amount >= 0
payment_status valid
```

### Delivery

```text
delivery_id NOT NULL
delivery_id UNIQUE
order_id exists
pickup_time <= delivery_time
```

Output a quality report:

```text
Dataset: orders

Total Records: 100000
Valid Records: 99870
Invalid Records: 130
Quality Score: 99.87%
```

Invalid records should go to the quarantine layer.

---

# 13. PYSPARK TRANSFORMATION

Create:

```text
src/transformation/
```

Use PySpark for the main transformations.

Perform:

* Deduplication
* Null handling
* Data type conversion
* Date standardization
* Status normalization
* Join validation
* Delivery duration calculation
* Average order value calculation
* Business metric preparation

Calculate:

```text
delivery_duration_minutes
```

using:

```text
delivery_time - pickup_time
```

Create a clean analytical dataset.

---

# 14. REDSHIFT DATA WAREHOUSE

Use a star schema.

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

Create appropriate primary/business keys.

Do not rely on Redshift foreign-key enforcement for correctness. Validate relationships in the pipeline.

---

# 15. REDSHIFT TABLE DESIGN

### dim_customer

```text
customer_key
customer_id
customer_name
email
city
signup_date
created_at
updated_at
```

### dim_restaurant

```text
restaurant_key
restaurant_id
restaurant_name
city
cuisine
rating
created_at
updated_at
```

### fact_order

```text
order_key
order_id
customer_key
restaurant_key
order_date_key
order_amount
order_status
created_at
```

### fact_payment

```text
payment_key
payment_id
order_id
payment_method
payment_amount
payment_status
payment_date_key
```

### fact_delivery

```text
delivery_key
delivery_id
order_id
delivery_partner_key
pickup_time
delivery_time
delivery_duration_minutes
delivery_status
```

---

# 16. ANALYTICAL SQL

Create:

```text
sql/analytics/
```

Queries for:

1. Daily orders
2. Daily revenue
3. Average order value
4. Cancellation rate
5. Payment success rate
6. Revenue by city
7. Orders by restaurant
8. Top restaurants
9. Orders by cuisine
10. Average delivery time
11. Late deliveries
12. Customer order frequency
13. Repeat customers
14. Monthly revenue
15. Peak ordering hours

Create Redshift views where useful.

---

# 17. AIRFLOW

Create:

```text
airflow/dags/food_delivery_pipeline.py
```

DAG:

```text
start
 |
 v
generate/read source data
 |
 v
ingest customers
 |
 v
ingest restaurants
 |
 v
ingest orders
 |
 v
ingest payments
 |
 v
ingest delivery
 |
 v
validate data
 |
 v
transform using PySpark
 |
 v
write processed S3
 |
 v
load Redshift
 |
 v
run data-quality checks
 |
 v
pipeline summary
 |
 v
success
```

Add:

* retries
* retry delay
* task logging
* failure handling
* sensible task dependencies
* execution date
* run ID

Do not put all business logic directly inside the DAG.

DAG files should orchestrate services/modules.

---

# 18. INCREMENTAL PROCESSING

The pipeline must support incremental data.

Do not reload all historical data every day.

Implement a simple strategy using:

```text
order_date
ingestion_date
updated_at
```

The pipeline should be able to process:

```text
Historical Load
       ↓
Initial warehouse
       ↓
Daily Increment
       ↓
New/changed records
```

Document the incremental strategy clearly.

---

# 19. IDEMPOTENCY

The pipeline should be safe to rerun.

If the same pipeline run executes twice:

```text
Run 1 → 100 orders
Run 2 → same 100 orders
```

It should not create duplicate business records in the warehouse.

Implement appropriate deduplication/upsert logic.

---

# 20. DOCKER

Create Dockerfiles for the application.

Create:

```text
docker-compose.yml
```

The local environment should contain the minimum services needed for development, such as:

```text
Airflow
PostgreSQL metadata database
Pipeline application
```

If running Spark locally through Docker becomes unnecessarily complicated, keep Spark execution simple and document the setup rather than creating an overly complex container cluster.

---

# 21. TESTING

Use pytest.

Create:

```text
tests/
├── unit/
├── integration/
└── data_quality/
```

Test:

* Data generation
* Ingestion
* Transformations
* Duplicate handling
* Null handling
* Data-quality rules
* Business calculations
* SQL logic where practical

At minimum, create meaningful tests for the core pipeline.

---

# 22. GITHUB ACTIONS

Create:

```text
.github/workflows/
├── ci.yml
└── deploy.yml
```

CI should run on:

```text
push
pull_request
```

CI steps:

```text
Checkout
 ↓
Setup Python
 ↓
Install dependencies
 ↓
Lint
 ↓
Run pytest
 ↓
Build Docker image
```

Use appropriate Python tooling such as:

```text
ruff
pytest
```

Do not add unnecessary tools.

---

# 23. TERRAFORM

Create:

```text
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── providers.tf
├── s3.tf
├── iam.tf
└── redshift.tf
```

Terraform should manage the AWS infrastructure required by the project.

At minimum:

```text
S3 bucket
IAM roles/policies
Redshift resources where practical
CloudWatch resources where practical
```

Use variables for:

```text
environment
region
bucket name
database name
```

Never hardcode credentials.

---

# 24. AWS SECURITY

Never store AWS credentials in:

```text
GitHub
Git repository
Docker image
source code
.env committed to Git
```

Use:

```text
AWS IAM
GitHub Actions OIDC where practical
AWS Secrets Manager
environment variables
```

Add:

```text
.env.example
```

but never commit actual secrets.

---

# 25. MONITORING

Add CloudWatch logging/metrics where practical.

Track:

```text
Pipeline execution
Records ingested
Records rejected
Records processed
Pipeline duration
Failures
```

Create useful log messages such as:

```text
INFO - Starting orders ingestion
INFO - Records received: 100000
INFO - Valid records: 99870
INFO - Invalid records: 130
INFO - Transformation completed
INFO - Redshift load completed
INFO - Pipeline completed successfully
```

---

# 26. POWER BI

Do not build Power BI itself inside the code repository.

Instead, create clean Redshift views/tables for Power BI.

Create recommended views:

```text
vw_daily_orders
vw_daily_revenue
vw_restaurant_performance
vw_delivery_performance
vw_customer_summary
vw_payment_summary
```

Document how Power BI can connect to Redshift.

Dashboard requirements:

### Overview

```text
Total Orders
Total Revenue
Average Order Value
Cancellation Rate
Payment Success Rate
Average Delivery Time
```

### Restaurant

```text
Top Restaurants
Revenue by Restaurant
Orders by Cuisine
Rating vs Orders
```

### Delivery

```text
Average Delivery Time
Delivery Time by City
Late Deliveries
Delivery Status
```

---

# 27. PROJECT STRUCTURE

Create a clean repository:

```text
food-delivery-data-platform/
│
├── src/
│   ├── ingestion/
│   ├── transformation/
│   ├── validation/
│   ├── warehouse/
│   └── common/
│
├── airflow/
│   └── dags/
│
├── data/
│   ├── sample/
│   └── generated/
│
├── scripts/
│   └── generate_data.py
│
├── sql/
│   ├── ddl/
│   ├── staging/
│   ├── warehouse/
│   └── analytics/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
│
├── terraform/
│
├── docker/
│
├── .github/
│   └── workflows/
│
├── docs/
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

# 28. CONFIGURATION

Centralize configuration.

Use environment variables for:

```text
AWS_REGION
S3_BUCKET
REDSHIFT_HOST
REDSHIFT_PORT
REDSHIFT_DATABASE
REDSHIFT_SCHEMA
REDSHIFT_USER
REDSHIFT_PASSWORD
```

Never hardcode credentials.

---

# 29. LOGGING

Use Python's standard logging framework.

Do not use random print statements throughout production code.

Create structured and meaningful logs.

---

# 30. ERROR HANDLING

Implement clear error handling.

Examples:

```text
S3 upload failure
Database connection failure
Invalid input file
Data-quality failure
PySpark failure
Redshift load failure
```

Errors should:

1. Be logged
2. Include useful context
3. Cause the appropriate Airflow task to fail
4. Be retryable when appropriate

Do not silently swallow exceptions.

---

# 31. README

Create a professional README containing:

1. Project overview
2. Business problem
3. Architecture diagram
4. Technology stack
5. Data model
6. Pipeline flow
7. Local setup
8. Environment variables
9. Docker setup
10. Running the pipeline
11. Running tests
12. Airflow setup
13. AWS setup
14. Terraform setup
15. CI/CD
16. Monitoring
17. Power BI connection
18. Future improvements

Include Mermaid diagrams where useful.

---

# 32. DOCUMENTATION

Create:

```text
docs/
├── architecture.md
├── data-model.md
├── pipeline.md
├── data-quality.md
├── deployment.md
├── monitoring.md
└── troubleshooting.md
```

Documentation should be concise and practical.

---

# 33. DEVELOPMENT PRINCIPLES

Follow these principles:

* Clean code
* Modular design
* Reusable functions
* Type hints where useful
* Environment-based configuration
* No hardcoded secrets
* Proper logging
* Error handling
* Unit testing
* Idempotent processing
* Incremental processing
* Clear naming
* Small functions
* Avoid unnecessary abstractions
* Avoid over-engineering

---

# 34. IMPORTANT: LOCAL DEVELOPMENT FIRST

The entire pipeline should first be runnable locally using generated CSV data.

The local flow should be:

```text
Generate Data
     ↓
Python Ingestion
     ↓
Local/S3-compatible storage where appropriate
     ↓
Validation
     ↓
PySpark
     ↓
Warehouse
     ↓
Airflow
```

Then integrate AWS.

Do not make AWS configuration a blocker for basic development and testing.

Where appropriate, provide a local development mode.

---

# 35. IMPLEMENTATION STRATEGY

Start by inspecting the repository.

Then:

1. Create the project structure.
2. Create configuration management.
3. Create synthetic data generator.
4. Generate sample datasets.
5. Implement ingestion.
6. Implement validation.
7. Implement PySpark transformation.
8. Implement processed data layer.
9. Implement Redshift schema and loading.
10. Implement analytics SQL.
11. Implement Airflow DAG.
12. Add Docker.
13. Add tests.
14. Add GitHub Actions.
15. Add Terraform.
16. Add monitoring.
17. Complete documentation.

After each major phase:

* Run tests.
* Run the relevant pipeline.
* Fix errors.
* Verify outputs.
* Update documentation.

---

# 36. GIT COMMITS

Use meaningful commits.

Examples:

```text
feat: add synthetic food delivery data generator

feat: implement raw data ingestion

feat: add data quality validation

feat: add pyspark transformations

feat: add redshift warehouse schema

feat: add airflow orchestration

test: add pipeline unit tests

ci: add github actions workflow

feat: add terraform infrastructure

docs: add deployment documentation
```

Do not make one giant commit containing the entire project.

---

# 37. FINAL ACCEPTANCE CRITERIA

The project is considered complete only when:

### Data Engineering

* [ ] Synthetic data generated
* [ ] Raw data ingestion works
* [ ] S3 raw layer works
* [ ] Data validation works
* [ ] Invalid records quarantined
* [ ] PySpark transformations work
* [ ] Processed Parquet data generated
* [ ] Redshift warehouse schema created
* [ ] Data loaded into Redshift
* [ ] Analytics SQL works

### Pipeline

* [ ] Airflow DAG works
* [ ] Pipeline is incremental
* [ ] Pipeline is idempotent
* [ ] Retries work
* [ ] Errors are logged

### DevOps

* [ ] Docker works
* [ ] Docker Compose works
* [ ] GitHub Actions works
* [ ] Tests run automatically
* [ ] Terraform infrastructure is defined
* [ ] Secrets are not committed

### Quality

* [ ] Unit tests
* [ ] Data-quality tests
* [ ] Logging
* [ ] Error handling
* [ ] Documentation

### Analytics

* [ ] Redshift analytical views
* [ ] Power BI-ready datasets
* [ ] Business metrics documented

---

# 38. HOW YOU SHOULD WORK WITH ME

Act as my senior technical mentor while building this project.

Do not blindly generate thousands of lines of code.

For every phase:

1. Explain what we are building.
2. Show the proposed files.
3. Implement the phase.
4. Run/verify tests.
5. Show me what was completed.
6. Identify any issues.
7. Fix the issues.
8. Only then move to the next phase.

If a requirement is ambiguous, choose the simplest production-appropriate approach and explain the decision.

If a technology is unnecessary, do not introduce it.

Prioritize a project that I can actually understand and explain in an interview.

At the end, provide:

* Complete architecture explanation
* Data flow explanation
* Interview questions and answers
* Resume bullet points
* GitHub README
* Deployment instructions
* Possible future improvements

Begin with **Phase 1: Project foundation and synthetic data generation**.

Before writing implementation code, inspect the repository and show me the proposed initial project structure.
