-- Power BI view: deliveries per order date x restaurant city (spec 08 §8; spec 02 M-06, M-07).
-- Completed delivery = DELIVERED with pickup and delivery times. Late = completed and more
-- than 45 minutes (LATE_DELIVERY_THRESHOLD_MINUTES, spec 02 §5). This view is the only place
-- the threshold appears in SQL; the late-delivery query aggregates it.
CREATE OR REPLACE VIEW {schema}.vw_delivery_performance AS
WITH deliveries AS (
    SELECT
        dl.order_date_key,
        r.city,
        dl.delivery_status,
        dl.delivery_duration_minutes,
        CASE
            WHEN dl.delivery_status = 'DELIVERED'
             AND dl.pickup_time IS NOT NULL
             AND dl.delivery_time IS NOT NULL
            THEN 1 ELSE 0
        END AS is_completed
    FROM {schema}.fact_delivery AS dl
    JOIN {schema}.fact_order AS o ON o.order_id = dl.order_id
    JOIN {schema}.dim_restaurant AS r ON r.restaurant_key = o.restaurant_key
)
SELECT
    d.full_date AS order_date,
    x.city,
    COUNT(*) AS total_deliveries,
    COUNT(CASE WHEN x.is_completed = 1 THEN 1 END) AS completed_deliveries,
    COUNT(CASE WHEN x.delivery_status = 'FAILED' THEN 1 END) AS failed_deliveries,
    ROUND(AVG(CASE WHEN x.is_completed = 1 THEN x.delivery_duration_minutes END), 2)
        AS avg_delivery_minutes,
    COUNT(CASE WHEN x.is_completed = 1 AND x.delivery_duration_minutes > 45 THEN 1 END)
        AS late_deliveries,
    ROUND(
        COUNT(CASE WHEN x.is_completed = 1 AND x.delivery_duration_minutes > 45 THEN 1 END)
            * 100.0 / NULLIF(COUNT(CASE WHEN x.is_completed = 1 THEN 1 END), 0),
        2
    ) AS late_delivery_rate_pct
FROM deliveries AS x
JOIN {schema}.dim_date AS d ON d.date_key = x.order_date_key
GROUP BY d.full_date, x.city;
