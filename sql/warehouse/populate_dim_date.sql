-- dim_date: one row per day from %(start_date)s to %(end_date)s (spec 08 §4, FR-051).
-- Idempotent: inserts missing dates only. Runs on PostgreSQL and Redshift, so no
-- generate_series (leader-node only on Redshift): day offsets come from four digit tables.
INSERT INTO {schema}.dim_date (
    date_key, full_date, day_of_month, day_of_week, day_name, week_of_year,
    month, month_name, quarter, year, is_weekend
)
SELECT
    CAST(TO_CHAR(days.full_date, 'YYYYMMDD') AS INTEGER),
    days.full_date,
    CAST(EXTRACT(DAY FROM days.full_date) AS SMALLINT),
    -- ISO day of week (1 = Monday ... 7 = Sunday) from DOW (0 = Sunday)
    CAST(MOD(CAST(EXTRACT(DOW FROM days.full_date) AS INTEGER) + 6, 7) + 1 AS SMALLINT),
    TRIM(TO_CHAR(days.full_date, 'Day')),
    CAST(TO_CHAR(days.full_date, 'IW') AS SMALLINT),
    CAST(EXTRACT(MONTH FROM days.full_date) AS SMALLINT),
    TRIM(TO_CHAR(days.full_date, 'Month')),
    CAST(EXTRACT(QUARTER FROM days.full_date) AS SMALLINT),
    CAST(EXTRACT(YEAR FROM days.full_date) AS SMALLINT),
    CASE WHEN EXTRACT(DOW FROM days.full_date) IN (0, 6) THEN TRUE ELSE FALSE END
FROM (
    SELECT CAST(CAST(%(start_date)s AS DATE) + (d1.n + 10 * d2.n + 100 * d3.n + 1000 * d4.n) AS DATE)
        AS full_date
    FROM
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d1,
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d2,
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d3,
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d4
) AS days
LEFT JOIN {schema}.dim_date AS existing ON existing.full_date = days.full_date
WHERE days.full_date <= CAST(%(end_date)s AS DATE)
  AND existing.date_key IS NULL;
