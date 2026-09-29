-- WQ-004 (ERROR): every payment and delivery belongs to an order in fact_order (spec 06 §6).
SELECT 'fact_payment.order_id' AS object_name, COUNT(*) AS failed_count
FROM {schema}.fact_payment AS p
LEFT JOIN {schema}.fact_order AS o ON o.order_id = p.order_id
WHERE o.order_id IS NULL
UNION ALL
SELECT 'fact_delivery.order_id', COUNT(*)
FROM {schema}.fact_delivery AS f
LEFT JOIN {schema}.fact_order AS o ON o.order_id = f.order_id
WHERE o.order_id IS NULL;
