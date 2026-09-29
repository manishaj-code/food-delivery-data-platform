-- Q01 / BQ-01 / M-01: orders per day, regardless of status.
SELECT order_date, total_orders, delivered_orders, cancelled_orders
FROM {schema}.vw_daily_orders
ORDER BY order_date;
