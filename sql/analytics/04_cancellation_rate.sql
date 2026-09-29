-- Q04 / BQ-04 / M-04: share of orders whose current status is CANCELLED, overall and per day.
SELECT
    'overall' AS period,
    CAST(NULL AS DATE) AS order_date,
    SUM(total_orders) AS total_orders,
    SUM(cancelled_orders) AS cancelled_orders,
    ROUND(SUM(cancelled_orders) * 100.0 / NULLIF(SUM(total_orders), 0), 2)
        AS cancellation_rate_pct
FROM {schema}.vw_daily_orders
UNION ALL
SELECT 'daily', order_date, total_orders, cancelled_orders, cancellation_rate_pct
FROM {schema}.vw_daily_orders
ORDER BY order_date NULLS FIRST;
