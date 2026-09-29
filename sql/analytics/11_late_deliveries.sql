-- Q11 / BQ-06 / M-07: late deliveries (completed, more than 45 minutes) and their share of
-- completed deliveries, overall and per restaurant city. The threshold lives in
-- vw_delivery_performance; its counts add up across days.
SELECT
    'overall' AS level,
    CAST(NULL AS VARCHAR(50)) AS city,
    SUM(completed_deliveries) AS completed_deliveries,
    SUM(late_deliveries) AS late_deliveries,
    ROUND(SUM(late_deliveries) * 100.0 / NULLIF(SUM(completed_deliveries), 0), 2)
        AS late_delivery_rate_pct
FROM {schema}.vw_delivery_performance
UNION ALL
SELECT
    'city',
    city,
    SUM(completed_deliveries),
    SUM(late_deliveries),
    ROUND(SUM(late_deliveries) * 100.0 / NULLIF(SUM(completed_deliveries), 0), 2)
FROM {schema}.vw_delivery_performance
GROUP BY city
ORDER BY city NULLS FIRST;
