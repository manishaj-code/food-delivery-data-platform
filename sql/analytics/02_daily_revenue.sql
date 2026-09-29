-- Q02 / BQ-02 / M-02: revenue (SUCCESS payments) per order date.
SELECT order_date, paid_orders, total_revenue
FROM {schema}.vw_daily_revenue
ORDER BY order_date;
