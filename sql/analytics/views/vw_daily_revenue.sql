-- Power BI view: revenue per order date (spec 08 §8; spec 02 M-02, M-03).
-- Revenue = SUCCESS payments, reported on the date of the order they pay for (not the payment
-- date). Paid order = order with at least one SUCCESS payment; AOV = revenue / paid orders.
-- Every order date appears; a day without paid orders has revenue 0 and AOV NULL.
CREATE OR REPLACE VIEW {schema}.vw_daily_revenue AS
WITH paid AS (
    SELECT order_id, SUM(payment_amount) AS revenue
    FROM {schema}.fact_payment
    WHERE payment_status = 'SUCCESS'
    GROUP BY order_id
)
SELECT
    d.full_date AS order_date,
    COUNT(paid.order_id) AS paid_orders,
    COALESCE(SUM(paid.revenue), 0) AS total_revenue,
    ROUND(SUM(paid.revenue) / NULLIF(COUNT(paid.order_id), 0), 2) AS average_order_value
FROM {schema}.fact_order AS o
JOIN {schema}.dim_date AS d ON d.date_key = o.order_date_key
LEFT JOIN paid ON paid.order_id = o.order_id
GROUP BY d.full_date;
