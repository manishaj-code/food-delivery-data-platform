# Phase 8 — Analytics SQL

## Objective

Deliver the 15 analytical queries and the 6 Power BI views, implementing the metric definitions of spec 02 exactly, and prove them with fixture-based tests.

## Why This Phase Exists

Business value comes from answering the business questions. Consistent, tested metric SQL is what dashboards and interviews rely on.

## Prerequisites

Phase 7 complete (warehouse tables loaded).

## Specifications Used

02 (entire), 03 (FR-060 – FR-062), 08 §8–9.

## Tasks

### Task 1 — Views
`sql/analytics/views/vw_daily_orders.sql`, `vw_daily_revenue.sql`, `vw_restaurant_performance.sql`, `vw_delivery_performance.sql`, `vw_customer_summary.sql`, `vw_payment_summary.sql` (`CREATE OR REPLACE VIEW`). Add view creation to `init_warehouse` (after tables).

### Task 2 — Analytical queries
`sql/analytics/01_daily_orders.sql` … `15_peak_ordering_hours.sql` (list in spec 02 §4). Prefer selecting from views where a view already encodes the metric to keep one definition.

### Task 3 — Analytics runner (CLI helper)
`src/warehouse/analytics.py`: `run_query(name)` returns rows; used by tests and by a CLI command `run-analytics --query 08_top_restaurants` for demos.

### Task 4 — Metric fixture
`tests/integration/fixtures/analytics_fixture.sql` (or Python builder): a tiny, hand-computed set of dims/facts (e.g. 10 orders with known statuses/payments/deliveries) with expected results documented in the test.

### Task 5 — Tests + commit
`feat: add analytics sql and power bi views`.

## Files To Create

```text
sql/analytics/views/vw_*.sql (6)
sql/analytics/01_daily_orders.sql … 15_peak_ordering_hours.sql
src/warehouse/analytics.py
tests/integration/fixtures/analytics_fixture.sql
tests/integration/test_analytics_sql.py
```

**As implemented:**

- `src/warehouse/analytics.py`: `VIEWS`, `QUERIES` (discovered from `sql/analytics/NN_*.sql`; only these names can run), `create_views`, `run_query(conn, schema, name) -> QueryResult(columns, rows)`, `format_result`, and a demo entry point `python -m src.warehouse.analytics --query 08_top_restaurants | --all` until the Phase 9 CLI wraps it.
- The fixture is SQL (`tests/integration/fixtures/analytics_fixture.sql`: 5 customers, 4 restaurants, 9 orders, 10 payments, 8 deliveries over 2026-09-01/02) with expected values worked out in the test docstrings. It covers a failed-then-successful payment, a refunded payment, a payment made the day after its order, a 45-minute delivery (not late), a failed and an open delivery, and a restaurant and a customer without orders.
- WQ-005 now reads `vw_daily_revenue` instead of repeating the AOV SQL.
- `tests/integration/conftest.py` gained `fresh_warehouse(schema)` so the analytics tests use one schema for the fixture and another for the generated sample data.
- Extra tests: `tests/unit/test_analytics_runner.py` (15 spec query names, every file renders, threshold in one view only, unknown names rejected, output formatting, entry point); the views sum back to the fact tables on generated data (no join fan-out).

## Files To Modify

`src/warehouse/init_warehouse.py` (create views), `src/cli.py` if already present (else Phase 9).

## Implementation Details

- Use `NULLIF` for all rates; `ROUND(x, 2)`.
- Hour extraction: `EXTRACT(HOUR FROM order_timestamp)` works on both engines.
- Revenue joins payments to orders on `order_id` and attributes to `fact_order.order_date_key`.
- Late threshold 45 minutes as a literal with a comment referencing spec 02 §5 (single place: the view; queries reuse the view).
- Views are schema-qualified; Power BI reader grants in `06_powerbi_reader.sql`.

## Testing Strategy

- `test_analytics_sql.py`: on fixture — each metric equals expected (total orders, revenue, AOV, cancellation rate, payment success rate, avg delivery time, late deliveries, repeat customers, revenue by city, orders by cuisine, peak hour); all 15 queries execute without error; view column lists match spec 08 §8.
- Smoke on generated data: every query returns ≥ 1 row.

## Validation

Run all queries on the historical local warehouse; peak hours show lunch/dinner; AOV reconciles with `daily_order_metrics` (WQ-005).

## Expected Output

15 query files, 6 views, passing tests, sample outputs recorded for README.

## Definition of Done

Master plan §6; AC-080 – AC-083 pass.

## Risks / Considerations

- DR-04 ambiguous definitions — follow spec 02 only; update spec first if a change is needed.
- Keep queries readable (CTEs, comments with metric ID).

## Dependencies

Phase 7.
