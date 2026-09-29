-- dim_restaurant upsert, SCD Type 1 (spec 08 §7, FR-053, FR-083). Run in one transaction.
UPDATE {schema}.dim_restaurant
SET restaurant_name       = s.restaurant_name,
    city                  = s.city,
    cuisine               = s.cuisine,
    rating                = s.rating,
    source_ingestion_date = s.source_ingestion_date,
    updated_at            = %(loaded_at)s
FROM {schema}.stg_restaurant AS s
WHERE dim_restaurant.restaurant_id = s.restaurant_id
  AND s.source_ingestion_date >= dim_restaurant.source_ingestion_date;

INSERT INTO {schema}.dim_restaurant (
    restaurant_id, restaurant_name, city, cuisine, rating,
    source_ingestion_date, created_at, updated_at
)
SELECT s.restaurant_id, s.restaurant_name, s.city, s.cuisine, s.rating,
       s.source_ingestion_date, %(loaded_at)s, %(loaded_at)s
FROM {schema}.stg_restaurant AS s
LEFT JOIN {schema}.dim_restaurant AS t ON t.restaurant_id = s.restaurant_id
WHERE t.restaurant_id IS NULL;
