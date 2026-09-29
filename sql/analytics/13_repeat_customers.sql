-- Q13 / BQ-08 / M-09: repeat customers (2 or more orders) and repeat rate over customers
-- with at least one order.
SELECT
    COUNT(*) AS active_customers,
    COUNT(CASE WHEN is_repeat_customer THEN 1 END) AS repeat_customers,
    ROUND(COUNT(CASE WHEN is_repeat_customer THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
        AS repeat_customer_rate_pct
FROM {schema}.vw_customer_summary
WHERE total_orders >= 1;
