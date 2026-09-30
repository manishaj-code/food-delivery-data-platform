-- Monitoring (spec 12 §4): data-quality score per dataset and run date. When a date was
-- run more than once, the latest validation counts.
WITH ranked AS (
    SELECT
        run_date,
        dataset,
        run_id,
        records_in,
        records_rejected,
        quality_score,
        ROW_NUMBER() OVER (
            PARTITION BY run_date, dataset ORDER BY finished_at DESC, run_id DESC
        ) AS attempt
    FROM {schema}.pipeline_run_audit
    WHERE stage = 'validation'
      AND dataset <> '_pipeline'
      AND quality_score IS NOT NULL
)
SELECT
    dataset,
    run_date,
    run_id,
    records_in,
    records_rejected,
    quality_score
FROM ranked
WHERE attempt = 1
ORDER BY dataset, run_date;
