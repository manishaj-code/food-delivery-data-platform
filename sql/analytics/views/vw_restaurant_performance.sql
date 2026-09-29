-- Power BI view: one row per restaurant (spec 08 §8; spec 02 M-08). Restaurants without
-- orders are included (0 orders/revenue, NULL rates). Orders, payments, and deliveries are
-- aggregated separately so the joins cannot multiply rows.
CREATE OR REPLACE VIEW {schema}.vw_restaurant_performance AS
WITH paid AS (
    SELECT order_id, SUM(payment_amount) AS revenue
    FROM {schema}.fact_payment
    WHERE payment_status = 'SUCCESS'
    GROUP BY order_id
),
orders AS (
    SELECT
        o.restaurant_key,
        COUNT(*) AS total_orders,
        COUNT(paid.order_id) AS paid_orders,
        SUM(paid.revenue) AS total_revenue,
        COUNT(CASE WHEN o.order_status = 'CANCELLED' THEN 1 END) AS cancelled_orders
    FROM {schema}.fact_order AS o
    LEFT JOIN paid ON paid.order_id = o.order_id
    GROUP BY o.restaurant_key
),
deliveries AS (
    -- Completed deliveries only (spec 02 §5).
    SELECT o.restaurant_key, AVG(dl.delivery_duration_minutes) AS avg_delivery_minutes
    FROM {schema}.fact_delivery AS dl
    JOIN {schema}.fact_order AS o ON o.order_id = dl.order_id
    WHERE dl.delivery_status = 'DELIVERED'
      AND dl.pickup_time IS NOT NULL
      AND dl.delivery_time IS NOT NULL
    GROUP BY o.restaurant_key
)
SELECT
    r.restaurant_id,
    r.restaurant_name,
    r.city,
    r.cuisine,
    r.rating,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_revenue, 0) AS total_revenue,
    ROUND(o.total_revenue / NULLIF(o.paid_orders, 0), 2) AS average_order_value,
    ROUND(o.cancelled_orders * 100.0 / NULLIF(o.total_orders, 0), 2) AS cancellation_rate_pct,
    ROUND(dl.avg_delivery_minutes, 2) AS avg_delivery_minutes
FROM {schema}.dim_restaurant AS r
LEFT JOIN orders AS o ON o.restaurant_key = r.restaurant_key
LEFT JOIN deliveries AS dl ON dl.restaurant_key = r.restaurant_key;
