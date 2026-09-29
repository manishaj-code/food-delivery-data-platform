-- dim_customer upsert, SCD Type 1 (spec 08 §7, FR-053, FR-083). Run in one transaction.
-- UPDATE existing keys unless the staged batch is older than the stored row (stale guard),
-- then INSERT new keys. Existing surrogate keys never change.
UPDATE {schema}.dim_customer
SET customer_name         = s.customer_name,
    email                 = s.email,
    city                  = s.city,
    signup_date           = s.signup_date,
    source_ingestion_date = s.source_ingestion_date,
    updated_at            = %(loaded_at)s
FROM {schema}.stg_customer AS s
WHERE dim_customer.customer_id = s.customer_id
  AND s.source_ingestion_date >= dim_customer.source_ingestion_date;

INSERT INTO {schema}.dim_customer (
    customer_id, customer_name, email, city, signup_date,
    source_ingestion_date, created_at, updated_at
)
SELECT s.customer_id, s.customer_name, s.email, s.city, s.signup_date,
       s.source_ingestion_date, %(loaded_at)s, %(loaded_at)s
FROM {schema}.stg_customer AS s
LEFT JOIN {schema}.dim_customer AS t ON t.customer_id = s.customer_id
WHERE t.customer_id IS NULL;
