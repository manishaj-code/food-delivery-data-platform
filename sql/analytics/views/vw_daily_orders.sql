-- Power BI view: orders per order date (spec 08 §8; spec 02 M-01, M-04).
-- fact_order holds one row per order_id (latest status), so COUNT(*) = COUNT(DISTINCT order_id).
CREATE OR REPLACE VIEW {schema}.vw_daily_orders AS
SELECT
    d.full_date AS order_date,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
    COUNT(CASE WHEN o.order_status = 'CANCELLED' THEN 1 END) AS cancelled_orders,
    ROUND(
        COUNT(CASE WHEN o.order_status = 'CANCELLED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2
    ) AS cancellation_rate_pct
FROM {schema}.fact_order AS o
JOIN {schema}.dim_date AS d ON d.date_key = o.order_date_key
GROUP BY d.full_date;
