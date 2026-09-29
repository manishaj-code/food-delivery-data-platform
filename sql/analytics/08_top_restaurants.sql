-- Q08 / BQ-07 / M-08: top 10 restaurants by revenue.
SELECT
    restaurant_id,
    restaurant_name,
    city,
    cuisine,
    rating,
    total_orders,
    total_revenue,
    average_order_value,
    avg_delivery_minutes
FROM {schema}.vw_restaurant_performance
ORDER BY total_revenue DESC, restaurant_id
LIMIT 10;
