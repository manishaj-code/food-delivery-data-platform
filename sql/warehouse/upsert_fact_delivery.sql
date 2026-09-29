-- fact_delivery upsert (spec 08 §5, §7, FR-054, FR-083). Run in one transaction.
-- Grain: one row per delivery holding its latest status; order_date_key is the related
-- order's date (resolved in PySpark as stg_delivery.order_date).
UPDATE {schema}.fact_delivery
SET order_id                  = s.order_id,
    delivery_partner_key      = p.delivery_partner_key,
    order_date_key            = d.date_key,
    pickup_time               = s.pickup_time,
    delivery_time             = s.delivery_time,
    delivery_duration_minutes = s.delivery_duration_minutes,
    delivery_status           = s.delivery_status,
    source_ingestion_date     = s.source_ingestion_date,
    updated_at                = %(loaded_at)s
FROM {schema}.stg_delivery AS s,
     {schema}.dim_delivery_partner AS p,
     {schema}.dim_date AS d
WHERE fact_delivery.delivery_id = s.delivery_id
  AND p.delivery_partner_id = s.delivery_partner_id
  AND d.full_date = s.order_date
  AND s.source_ingestion_date >= fact_delivery.source_ingestion_date;

INSERT INTO {schema}.fact_delivery (
    delivery_id, order_id, delivery_partner_key, order_date_key, pickup_time, delivery_time,
    delivery_duration_minutes, delivery_status, source_ingestion_date, created_at, updated_at
)
SELECT s.delivery_id, s.order_id, p.delivery_partner_key, d.date_key, s.pickup_time,
       s.delivery_time, s.delivery_duration_minutes, s.delivery_status,
       s.source_ingestion_date, %(loaded_at)s, %(loaded_at)s
FROM {schema}.stg_delivery AS s
LEFT JOIN {schema}.dim_delivery_partner AS p ON p.delivery_partner_id = s.delivery_partner_id
LEFT JOIN {schema}.dim_date AS d ON d.full_date = s.order_date
LEFT JOIN {schema}.fact_delivery AS t ON t.delivery_id = s.delivery_id
WHERE t.delivery_id IS NULL;
