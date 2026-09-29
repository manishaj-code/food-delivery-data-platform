-- WQ-005 (WARN): for the order dates in this batch (stg_daily_order_metrics), warehouse AOV
-- (spec 02 M-03) equals the PySpark AOV within 0.01. Revenue = SUCCESS payments, reported on
-- the order's date; AOV = revenue / paid orders. Differences can be legitimate (a later
-- status update or rerun changed the warehouse), hence WARN. failed_count = mismatching days.
SELECT 'daily_order_metrics.average_order_value' AS object_name, COUNT(*) AS failed_count
FROM {schema}.stg_daily_order_metrics AS m
LEFT JOIN (
    SELECT o.order_date_key,
           ROUND(SUM(paid.revenue) / COUNT(*), 2) AS average_order_value
    FROM {schema}.fact_order AS o
    JOIN (
        SELECT order_id, SUM(payment_amount) AS revenue
        FROM {schema}.fact_payment
        WHERE payment_status = 'SUCCESS'
        GROUP BY order_id
    ) AS paid ON paid.order_id = o.order_id
    GROUP BY o.order_date_key
) AS w ON w.order_date_key = CAST(TO_CHAR(m.order_date, 'YYYYMMDD') AS INTEGER)
WHERE ABS(COALESCE(w.average_order_value, 0) - COALESCE(m.average_order_value, 0)) > 0.01;
