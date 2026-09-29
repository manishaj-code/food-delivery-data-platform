-- Q05 / BQ-05 / M-05: share of payment attempts that succeeded, per payment method
-- (lowest success rate first, i.e. the methods that fail most).
SELECT
    payment_method,
    SUM(payment_count) AS total_payments,
    SUM(CASE WHEN payment_status = 'SUCCESS' THEN payment_count ELSE 0 END)
        AS successful_payments,
    ROUND(
        SUM(CASE WHEN payment_status = 'SUCCESS' THEN payment_count ELSE 0 END) * 100.0
            / NULLIF(SUM(payment_count), 0),
        2
    ) AS success_rate_pct
FROM {schema}.vw_payment_summary
GROUP BY payment_method
ORDER BY success_rate_pct, payment_method;
