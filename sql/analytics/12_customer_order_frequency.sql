-- Q12 / BQ-08 / M-09: distribution of orders per customer (customers with at least one order).
WITH frequency AS (
    SELECT total_orders AS orders_per_customer, COUNT(*) AS customers
    FROM {schema}.vw_customer_summary
    WHERE total_orders >= 1
    GROUP BY total_orders
)
SELECT
    orders_per_customer,
    customers,
    ROUND(customers * 100.0 / NULLIF(SUM(customers) OVER (), 0), 2) AS share_of_customers_pct
FROM frequency
ORDER BY orders_per_customer;
