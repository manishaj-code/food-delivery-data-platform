-- dim_delivery_partner upsert, SCD Type 1 (spec 08 §7, FR-053, FR-083). Run in one transaction.
UPDATE {schema}.dim_delivery_partner
SET partner_name          = s.partner_name,
    city                  = s.city,
    joining_date          = s.joining_date,
    source_ingestion_date = s.source_ingestion_date,
    updated_at            = %(loaded_at)s
FROM {schema}.stg_delivery_partner AS s
WHERE dim_delivery_partner.delivery_partner_id = s.delivery_partner_id
  AND s.source_ingestion_date >= dim_delivery_partner.source_ingestion_date;

INSERT INTO {schema}.dim_delivery_partner (
    delivery_partner_id, partner_name, city, joining_date,
    source_ingestion_date, created_at, updated_at
)
SELECT s.delivery_partner_id, s.partner_name, s.city, s.joining_date,
       s.source_ingestion_date, %(loaded_at)s, %(loaded_at)s
FROM {schema}.stg_delivery_partner AS s
LEFT JOIN {schema}.dim_delivery_partner AS t ON t.delivery_partner_id = s.delivery_partner_id
WHERE t.delivery_partner_id IS NULL;
