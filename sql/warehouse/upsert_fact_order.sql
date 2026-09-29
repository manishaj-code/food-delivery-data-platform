-- fact_order upsert (spec 08 §5, §7, FR-054, FR-083). Run in one transaction.
-- Grain: one row per order holding its latest status. Dimension keys are resolved by
-- business key. The INSERT uses LEFT JOINs: a missing dimension row yields a NULL key,
-- which the NOT NULL constraint rejects, failing the load instead of dropping the order.
UPDATE {schema}.fact_order
SET customer_key          = c.customer_key,
    restaurant_key        = r.restaurant_key,
    order_date_key        = d.date_key,
    order_timestamp       = s.order_timestamp,
    order_amount          = s.order_amount,
    order_status          = s.order_status,
    source_ingestion_date = s.source_ingestion_date,
    updated_at            = %(loaded_at)s
FROM {schema}.stg_order AS s,
     {schema}.dim_customer AS c,
     {schema}.dim_restaurant AS r,
     {schema}.dim_date AS d
WHERE fact_order.order_id = s.order_id
  AND c.customer_id = s.customer_id
  AND r.restaurant_id = s.restaurant_id
  AND d.full_date = s.order_date
  AND s.source_ingestion_date >= fact_order.source_ingestion_date;

INSERT INTO {schema}.fact_order (
    order_id, customer_key, restaurant_key, order_date_key, order_timestamp,
    order_amount, order_status, source_ingestion_date, created_at, updated_at
)
SELECT s.order_id, c.customer_key, r.restaurant_key, d.date_key, s.order_timestamp,
       s.order_amount, s.order_status, s.source_ingestion_date, %(loaded_at)s, %(loaded_at)s
FROM {schema}.stg_order AS s
LEFT JOIN {schema}.dim_customer AS c ON c.customer_id = s.customer_id
LEFT JOIN {schema}.dim_restaurant AS r ON r.restaurant_id = s.restaurant_id
LEFT JOIN {schema}.dim_date AS d ON d.full_date = s.order_date
LEFT JOIN {schema}.fact_order AS t ON t.order_id = s.order_id
WHERE t.order_id IS NULL;
