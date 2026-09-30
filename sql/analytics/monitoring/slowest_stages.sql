-- Monitoring (spec 12 §4): stages by average duration per run. Each dataset is its own
-- ingestion task, so ingestion time is summed per run; the other steps process every
-- dataset together and write the step's elapsed time on each row, so the maximum is used.
WITH per_run AS (
    SELECT
        run_id,
        stage,
        CASE
            WHEN stage = 'ingestion' THEN SUM(duration_seconds)
            ELSE MAX(duration_seconds)
        END AS seconds
    FROM {schema}.pipeline_run_audit
    WHERE stage <> 'pipeline'
      AND duration_seconds IS NOT NULL
    GROUP BY run_id, stage
)
SELECT
    stage,
    COUNT(*) AS runs,
    CAST(AVG(seconds) AS DECIMAL(10, 2)) AS avg_seconds,
    MAX(seconds) AS max_seconds
FROM per_run
GROUP BY stage
ORDER BY avg_seconds DESC, stage;
