-- WQ-006 (ERROR): no negative amounts in the facts (spec 06 §6).
SELECT 'fact_order.order_amount' AS object_name, COUNT(*) AS failed_count
FROM {schema}.fact_order
WHERE order_amount < 0
UNION ALL
SELECT 'fact_payment.payment_amount', COUNT(*)
FROM {schema}.fact_payment
WHERE payment_amount < 0;
