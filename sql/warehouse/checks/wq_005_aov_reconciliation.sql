-- WQ-005 (WARN): for the order dates in this batch (stg_daily_order_metrics), warehouse AOV
-- (spec 02 M-03, defined once in vw_daily_revenue) equals the PySpark AOV within 0.01.
-- Differences can be legitimate (a later status update or rerun changed the warehouse),
-- hence WARN. failed_count = mismatching days.
SELECT 'daily_order_metrics.average_order_value' AS object_name, COUNT(*) AS failed_count
FROM {schema}.stg_daily_order_metrics AS m
LEFT JOIN {schema}.vw_daily_revenue AS w ON w.order_date = m.order_date
WHERE ABS(COALESCE(w.average_order_value, 0) - COALESCE(m.average_order_value, 0)) > 0.01;
