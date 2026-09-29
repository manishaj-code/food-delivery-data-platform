-- Q14 / BQ-02 / M-02, M-03: revenue, paid orders, and AOV per order month.
SELECT
    TO_CHAR(order_date, 'YYYY-MM') AS order_month,
    SUM(paid_orders) AS paid_orders,
    SUM(total_revenue) AS total_revenue,
    ROUND(SUM(total_revenue) / NULLIF(SUM(paid_orders), 0), 2) AS average_order_value
FROM {schema}.vw_daily_revenue
GROUP BY TO_CHAR(order_date, 'YYYY-MM')
ORDER BY order_month;
