-- Q15 / BQ-11 / M-12: orders by hour of day (UTC) and day of week, busiest first.
SELECT
    CAST(EXTRACT(HOUR FROM o.order_timestamp) AS INTEGER) AS order_hour,
    d.day_name,
    COUNT(*) AS total_orders,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS order_rank
FROM {schema}.fact_order AS o
JOIN {schema}.dim_date AS d ON d.date_key = o.order_date_key
GROUP BY CAST(EXTRACT(HOUR FROM o.order_timestamp) AS INTEGER), d.day_of_week, d.day_name
ORDER BY total_orders DESC, d.day_of_week, order_hour;
