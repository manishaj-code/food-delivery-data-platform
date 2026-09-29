# 06 — Data Quality Specification

Implements FR-020 – FR-026. Framework: **custom lightweight PySpark rule engine** in `src/validation/` (Great Expectations is not used — it adds heavy configuration for a small, fixed rule set; see Key Decisions in the master plan).

---

## 1. Concepts

| Concept | Definition |
|---|---|
| Rule | A check on one dataset/column, identified by `DQ-<DATASET>-<NNN>`. |
| Severity `ERROR` | Record fails → record is quarantined and excluded from downstream processing. |
| Severity `WARN` | Record is kept; failure is counted in the report and logged. |
| Record-level rule | Evaluated per row (not null, range, allowed values, format, cross-column). |
| Set-level rule | Evaluated across rows (uniqueness, referential integrity). Implemented with window functions / joins but still tags individual rows. |
| Valid record | Fails no `ERROR` rule. |
| Quarantine | `quarantine/<dataset>/year=/month=/day=/` Parquet with original (string) values + DQ metadata. |

Validation reads raw CSV **as strings**, trims values, converts empty strings to NULL, upper-cases enumerated columns (status, method), then evaluates rules. Type-dependent rules (date/number validity) use safe casts: a non-null value whose cast returns NULL fails the rule.

## 2. Rule Catalogue

Dataset order of evaluation: `customers`, `restaurants`, `delivery_partners` → `orders` → `payments`, `delivery` (parents before children so referential rules can use valid parents).

### customers

| Rule ID | Column | Rule | Severity | Failure behavior | Quarantine behavior |
|---|---|---|---|---|---|
| DQ-CUS-001 | customer_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-CUS-002 | customer_id | UNIQUE within batch (first occurrence by `_source_row_number` kept) | ERROR | Count + tag extra copies | Quarantine extra copies only |
| DQ-CUS-003 | city | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-CUS-004 | signup_date | Valid `YYYY-MM-DD` date, not in the future relative to run date | ERROR | Count + tag | Quarantine record |
| DQ-CUS-005 | email | NOT NULL | WARN | Count + log | Not quarantined |

### restaurants

| Rule ID | Column | Rule | Severity | Failure behavior | Quarantine behavior |
|---|---|---|---|---|---|
| DQ-RES-001 | restaurant_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-RES-002 | restaurant_id | UNIQUE within batch | ERROR | Count + tag extra copies | Quarantine extra copies |
| DQ-RES-003 | rating | NULL or numeric BETWEEN 0 AND 5 | ERROR | Count + tag | Quarantine record |

### delivery_partners

| Rule ID | Column | Rule | Severity | Failure behavior | Quarantine behavior |
|---|---|---|---|---|---|
| DQ-DPT-001 | delivery_partner_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-DPT-002 | delivery_partner_id | UNIQUE within batch | ERROR | Count + tag extra copies | Quarantine extra copies |
| DQ-DPT-003 | joining_date | Valid date | ERROR | Count + tag | Quarantine record |

> `project_details.md` defines no rules or quarantine folder for delivery_partners. DQ-DPT-001–003 and `quarantine/delivery_partners/` are added because `fact_delivery` needs a valid partner key (assumption A-09 in spec 14).

### orders

| Rule ID | Column | Rule | Severity | Failure behavior | Quarantine behavior |
|---|---|---|---|---|---|
| DQ-ORD-001 | order_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-ORD-002 | order_id | UNIQUE within batch | ERROR | Count + tag extra copies | Quarantine extra copies |
| DQ-ORD-003 | customer_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-ORD-004 | customer_id | Exists in valid customers (batch ∪ processed) | ERROR | Count + tag | Quarantine record |
| DQ-ORD-005 | restaurant_id | NOT NULL and exists in valid restaurants (batch ∪ processed) | ERROR | Count + tag | Quarantine record |
| DQ-ORD-006 | order_amount | Numeric and `>= 0` | ERROR | Count + tag | Quarantine record |
| DQ-ORD-007 | order_status | IN (PLACED, PREPARING, OUT_FOR_DELIVERY, DELIVERED, CANCELLED) | ERROR | Count + tag | Quarantine record |
| DQ-ORD-008 | order_date | Valid timestamp, not after end of run date | ERROR | Count + tag | Quarantine record |

A NULL `customer_id` fails both DQ-ORD-003 and DQ-ORD-004; both IDs are recorded.

### payments

| Rule ID | Column | Rule | Severity | Failure behavior | Quarantine behavior |
|---|---|---|---|---|---|
| DQ-PAY-001 | payment_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-PAY-002 | payment_id | UNIQUE within batch | ERROR | Count + tag extra copies | Quarantine extra copies |
| DQ-PAY-003 | order_id | NOT NULL and exists in valid orders (batch ∪ processed) | ERROR | Count + tag | Quarantine record |
| DQ-PAY-004 | payment_amount | Numeric and `>= 0` | ERROR | Count + tag | Quarantine record |
| DQ-PAY-005 | payment_status | IN (SUCCESS, FAILED, REFUNDED) | ERROR | Count + tag | Quarantine record |
| DQ-PAY-006 | payment_method | IN (UPI, CARD, CASH, WALLET) | ERROR | Count + tag | Quarantine record |
| DQ-PAY-007 | payment_date | Valid timestamp | ERROR | Count + tag | Quarantine record |

### delivery

| Rule ID | Column | Rule | Severity | Failure behavior | Quarantine behavior |
|---|---|---|---|---|---|
| DQ-DEL-001 | delivery_id | NOT NULL | ERROR | Count + tag | Quarantine record |
| DQ-DEL-002 | delivery_id | UNIQUE within batch | ERROR | Count + tag extra copies | Quarantine extra copies |
| DQ-DEL-003 | order_id | NOT NULL and exists in valid orders (batch ∪ processed) | ERROR | Count + tag | Quarantine record |
| DQ-DEL-004 | delivery_partner_id | NOT NULL and exists in valid delivery partners (batch ∪ processed) | ERROR | Count + tag | Quarantine record |
| DQ-DEL-005 | pickup_time, delivery_time | `pickup_time <= delivery_time` when both are present | ERROR | Count + tag | Quarantine record |
| DQ-DEL-006 | delivery_status | IN (ASSIGNED, PICKED_UP, DELIVERED, FAILED) | ERROR | Count + tag | Quarantine record |
| DQ-DEL-007 | pickup_time, delivery_time | Valid timestamp when non-null | ERROR | Count + tag | Quarantine record |
| DQ-DEL-008 | delivery_time | Present when `delivery_status = DELIVERED` | WARN | Count + log | Not quarantined (excluded from delivery-time metrics by definition) |

**Total: 34 rules (32 ERROR, 2 WARN).** The rule catalogue in code must contain exactly these IDs (FR-020 test).

## 3. Referential Integrity Scope

For a child dataset, the parent key set = `valid parent records of the current batch` ∪ `business keys present in processed/<parent>/` (all partitions). This supports incremental runs where the parent arrived on an earlier day.

**Late-arriving parents** (child arrives before parent) are quarantined. Replaying them is a manual, documented procedure (out of scope for automation — assumption A-10).

## 4. Quarantine Record Format

Parquet under `quarantine/<dataset>/year=YYYY/month=MM/day=DD/` (run date). Columns:

| Column | Description |
|---|---|
| *all source columns* | Original values as strings (after trim/empty→NULL). |
| `_ingestion_timestamp`, `_source_file`, `_source_row_number`, `_run_id` | Carried from raw. |
| `_dq_failed_rules` | Semicolon-separated rule IDs, e.g. `DQ-ORD-003;DQ-ORD-004`. |
| `_dq_validated_at` | UTC timestamp of validation. |

The partition is overwritten on rerun (FR-091). Retained 90 days (S3 lifecycle).

## 5. Quality Report and Score

### Score calculation

```text
quality_score = valid_records / total_records × 100      (rounded to 2 decimals)
valid_records = records failing no ERROR rule
total_records = records read from raw for the dataset and run
If total_records = 0 → quality_score = 100.00 and status = NO_DATA
```

WARN failures do not affect the score; they are reported separately.

### Quality gate

```text
quality_score < DQ_MIN_QUALITY_SCORE (default 95.00) → DataQualityThresholdError
```

The error fails `validate_data` **without retry** (the data will not change on retry). Quarantine and reports are still written before the task fails, so the problem can be investigated.

### Log format (required)

```text
INFO - Dataset: orders
INFO - Total Records: 100000
INFO - Valid Records: 99870
INFO - Invalid Records: 130
INFO - Quality Score: 99.87%
```

### Persisted report (`reports/data_quality/year=/month=/day=/<dataset>.json`)

```json
{
  "run_id": "scheduled__2026-09-29T00:00:00+00:00",
  "run_date": "2026-09-29",
  "dataset": "orders",
  "total_records": 100000,
  "valid_records": 99870,
  "invalid_records": 130,
  "quality_score": 99.87,
  "threshold": 95.0,
  "passed": true,
  "rule_results": [
    {"rule_id": "DQ-ORD-006", "severity": "ERROR", "failed_records": 10}
  ],
  "warnings": [],
  "validated_at": "2026-09-29T01:12:03Z"
}
```

The same totals are written to `pipeline_run_audit` (stage `validation`).

## 6. Post-Load Warehouse Checks (FR-026)

Run by task `run_dq_checks` after `load_warehouse`. Any failed `ERROR` check raises `DataQualityThresholdError`; `WARN` checks are logged and recorded in the audit table only.

| Check ID | Check | Pass condition | Severity |
|---|---|---|---|
| WQ-001 | Row-count reconciliation per table | Rows in staging for this batch = rows in processed partition | ERROR |
| WQ-002 | Business-key uniqueness in every dim/fact | `COUNT(*) = COUNT(DISTINCT business_key)` | ERROR |
| WQ-003 | No NULL surrogate/dimension keys in facts | 0 rows with NULL `customer_key`, `restaurant_key`, `order_date_key`, `delivery_partner_key`, `payment_date_key` | ERROR |
| WQ-004 | No orphan facts | Every `fact_payment.order_id` and `fact_delivery.order_id` exists in `fact_order` | ERROR |
| WQ-005 | AOV reconciliation | For order dates contained in the batch, SQL AOV (spec 02 M-03) equals PySpark `daily_order_metrics` AOV within 0.01 | WARN — later status updates or reruns of older dates can legitimately change warehouse values |
| WQ-006 | Non-negative amounts | 0 rows with negative `order_amount`/`payment_amount` | ERROR |
