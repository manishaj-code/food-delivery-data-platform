-- Monitoring (spec 12 §4): status of the last 7 pipeline runs, newest first.
SELECT
    run_id,
    run_date,
    load_type,
    status,
    records_in,
    records_out,
    records_rejected,
    duration_seconds,
    finished_at,
    error_message
FROM {schema}.pipeline_run_audit
WHERE stage = 'pipeline'
ORDER BY finished_at DESC, run_id
LIMIT 7;
