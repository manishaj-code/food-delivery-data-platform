-- Q03 / BQ-03 / M-03: average order value overall and per day (revenue / paid orders).
-- Each order has exactly one order date, so daily paid orders and revenue add up to the total.
SELECT
    'overall' AS period,
    CAST(NULL AS DATE) AS order_date,
    SUM(paid_orders) AS paid_orders,
    SUM(total_revenue) AS total_revenue,
    ROUND(SUM(total_revenue) / NULLIF(SUM(paid_orders), 0), 2) AS average_order_value
FROM {schema}.vw_daily_revenue
UNION ALL
SELECT 'daily', order_date, paid_orders, total_revenue, average_order_value
FROM {schema}.vw_daily_revenue
ORDER BY order_date NULLS FIRST;
