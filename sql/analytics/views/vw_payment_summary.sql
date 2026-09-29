-- Power BI view: payment attempts per payment date x method x status (spec 08 §8; spec 02
-- M-05). Every status is kept so success rates can be computed per method. Revenue by order
-- date comes from vw_daily_revenue, not from this view.
CREATE OR REPLACE VIEW {schema}.vw_payment_summary AS
SELECT
    d.full_date AS payment_date,
    p.payment_method,
    p.payment_status,
    COUNT(*) AS payment_count,
    SUM(p.payment_amount) AS total_amount
FROM {schema}.fact_payment AS p
JOIN {schema}.dim_date AS d ON d.date_key = p.payment_date_key
GROUP BY d.full_date, p.payment_method, p.payment_status;
