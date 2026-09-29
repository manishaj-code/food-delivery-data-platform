-- Q10 / BQ-06 / M-06: average minutes from pickup to delivery for completed deliveries
-- (DELIVERED with both times), overall and per restaurant city. Averaged from fact_delivery,
-- not from the rounded daily averages of vw_delivery_performance.
WITH completed AS (
    SELECT r.city, dl.delivery_duration_minutes
    FROM {schema}.fact_delivery AS dl
    JOIN {schema}.fact_order AS o ON o.order_id = dl.order_id
    JOIN {schema}.dim_restaurant AS r ON r.restaurant_key = o.restaurant_key
    WHERE dl.delivery_status = 'DELIVERED'
      AND dl.pickup_time IS NOT NULL
      AND dl.delivery_time IS NOT NULL
)
SELECT
    'overall' AS level,
    CAST(NULL AS VARCHAR(50)) AS city,
    COUNT(*) AS completed_deliveries,
    ROUND(AVG(delivery_duration_minutes), 2) AS avg_delivery_minutes
FROM completed
UNION ALL
SELECT 'city', city, COUNT(*), ROUND(AVG(delivery_duration_minutes), 2)
FROM completed
GROUP BY city
ORDER BY city NULLS FIRST;
