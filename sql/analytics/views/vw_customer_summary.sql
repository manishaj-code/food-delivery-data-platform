-- Power BI view: one row per customer (spec 08 §8; spec 02 M-09). Customers without orders
-- are included with 0 orders. Spend = SUCCESS payments; repeat customer = 2 or more orders.
CREATE OR REPLACE VIEW {schema}.vw_customer_summary AS
WITH paid AS (
    SELECT order_id, SUM(payment_amount) AS revenue
    FROM {schema}.fact_payment
    WHERE payment_status = 'SUCCESS'
    GROUP BY order_id
),
orders AS (
    SELECT
        o.customer_key,
        COUNT(*) AS total_orders,
        SUM(paid.revenue) AS total_spend,
        MIN(d.full_date) AS first_order_date,
        MAX(d.full_date) AS last_order_date
    FROM {schema}.fact_order AS o
    JOIN {schema}.dim_date AS d ON d.date_key = o.order_date_key
    LEFT JOIN paid ON paid.order_id = o.order_id
    GROUP BY o.customer_key
)
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.signup_date,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_spend, 0) AS total_spend,
    o.first_order_date,
    o.last_order_date,
    CASE WHEN o.total_orders >= 2 THEN TRUE ELSE FALSE END AS is_repeat_customer
FROM {schema}.dim_customer AS c
LEFT JOIN orders AS o ON o.customer_key = c.customer_key;
