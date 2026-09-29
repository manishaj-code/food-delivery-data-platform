-- Q06 / BQ-09 / M-10: revenue attributed to the restaurant's city.
SELECT
    city,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue
FROM {schema}.vw_restaurant_performance
GROUP BY city
ORDER BY total_revenue DESC, city;
