-- Q07 / BQ-07 / M-08: orders per restaurant (all statuses), with cancellation rate.
SELECT
    restaurant_id,
    restaurant_name,
    city,
    cuisine,
    total_orders,
    cancellation_rate_pct
FROM {schema}.vw_restaurant_performance
ORDER BY total_orders DESC, restaurant_id;
