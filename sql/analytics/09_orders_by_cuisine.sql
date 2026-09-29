-- Q09 / BQ-10 / M-11: orders and revenue per restaurant cuisine, with share of all orders.
WITH cuisine AS (
    SELECT
        cuisine,
        SUM(total_orders) AS total_orders,
        SUM(total_revenue) AS total_revenue
    FROM {schema}.vw_restaurant_performance
    GROUP BY cuisine
)
SELECT
    cuisine,
    total_orders,
    total_revenue,
    ROUND(total_orders * 100.0 / NULLIF(SUM(total_orders) OVER (), 0), 2) AS share_of_orders_pct
FROM cuisine
ORDER BY total_orders DESC, cuisine;
