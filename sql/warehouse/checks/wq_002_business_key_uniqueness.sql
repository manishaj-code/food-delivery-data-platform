-- WQ-002 (ERROR): business keys are unique in every dimension and fact (spec 06 §6).
-- Redshift does not enforce UNIQUE, so this is the real guarantee there.
SELECT 'dim_customer' AS object_name, COUNT(*) - COUNT(DISTINCT customer_id) AS failed_count
FROM {schema}.dim_customer
UNION ALL
SELECT 'dim_restaurant', COUNT(*) - COUNT(DISTINCT restaurant_id) FROM {schema}.dim_restaurant
UNION ALL
SELECT 'dim_delivery_partner', COUNT(*) - COUNT(DISTINCT delivery_partner_id)
FROM {schema}.dim_delivery_partner
UNION ALL
SELECT 'dim_date', COUNT(*) - COUNT(DISTINCT full_date) FROM {schema}.dim_date
UNION ALL
SELECT 'fact_order', COUNT(*) - COUNT(DISTINCT order_id) FROM {schema}.fact_order
UNION ALL
SELECT 'fact_payment', COUNT(*) - COUNT(DISTINCT payment_id) FROM {schema}.fact_payment
UNION ALL
SELECT 'fact_delivery', COUNT(*) - COUNT(DISTINCT delivery_id) FROM {schema}.fact_delivery;
