-- fact_payment upsert (spec 08 §5, §7, FR-054, FR-083). Run in one transaction.
-- Grain: one row per payment transaction; order_id is a degenerate dimension.
UPDATE {schema}.fact_payment
SET order_id              = s.order_id,
    payment_method        = s.payment_method,
    payment_amount        = s.payment_amount,
    payment_status        = s.payment_status,
    payment_date_key      = d.date_key,
    payment_timestamp     = s.payment_timestamp,
    source_ingestion_date = s.source_ingestion_date,
    updated_at            = %(loaded_at)s
FROM {schema}.stg_payment AS s,
     {schema}.dim_date AS d
WHERE fact_payment.payment_id = s.payment_id
  AND d.full_date = s.payment_date
  AND s.source_ingestion_date >= fact_payment.source_ingestion_date;

INSERT INTO {schema}.fact_payment (
    payment_id, order_id, payment_method, payment_amount, payment_status,
    payment_date_key, payment_timestamp, source_ingestion_date, created_at, updated_at
)
SELECT s.payment_id, s.order_id, s.payment_method, s.payment_amount, s.payment_status,
       d.date_key, s.payment_timestamp, s.source_ingestion_date, %(loaded_at)s, %(loaded_at)s
FROM {schema}.stg_payment AS s
LEFT JOIN {schema}.dim_date AS d ON d.full_date = s.payment_date
LEFT JOIN {schema}.fact_payment AS t ON t.payment_id = s.payment_id
WHERE t.payment_id IS NULL;
