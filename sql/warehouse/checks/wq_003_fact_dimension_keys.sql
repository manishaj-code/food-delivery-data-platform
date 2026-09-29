-- WQ-003 (ERROR): every fact dimension key is set and points at an existing dimension row
-- (spec 06 §6). A NULL key never matches, so one LEFT JOIN covers NULL and dangling keys.
SELECT 'fact_order.customer_key' AS object_name, COUNT(*) AS failed_count
FROM {schema}.fact_order AS f
LEFT JOIN {schema}.dim_customer AS d ON d.customer_key = f.customer_key
WHERE d.customer_key IS NULL
UNION ALL
SELECT 'fact_order.restaurant_key', COUNT(*)
FROM {schema}.fact_order AS f
LEFT JOIN {schema}.dim_restaurant AS d ON d.restaurant_key = f.restaurant_key
WHERE d.restaurant_key IS NULL
UNION ALL
SELECT 'fact_order.order_date_key', COUNT(*)
FROM {schema}.fact_order AS f
LEFT JOIN {schema}.dim_date AS d ON d.date_key = f.order_date_key
WHERE d.date_key IS NULL
UNION ALL
SELECT 'fact_payment.payment_date_key', COUNT(*)
FROM {schema}.fact_payment AS f
LEFT JOIN {schema}.dim_date AS d ON d.date_key = f.payment_date_key
WHERE d.date_key IS NULL
UNION ALL
SELECT 'fact_delivery.delivery_partner_key', COUNT(*)
FROM {schema}.fact_delivery AS f
LEFT JOIN {schema}.dim_delivery_partner AS d ON d.delivery_partner_key = f.delivery_partner_key
WHERE d.delivery_partner_key IS NULL
UNION ALL
SELECT 'fact_delivery.order_date_key', COUNT(*)
FROM {schema}.fact_delivery AS f
LEFT JOIN {schema}.dim_date AS d ON d.date_key = f.order_date_key
WHERE d.date_key IS NULL;
