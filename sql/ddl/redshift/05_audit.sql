-- Pipeline run audit (Amazon Redshift). Spec 12 §4. Grain: run_id x dataset x stage.
CREATE TABLE IF NOT EXISTS {schema}.pipeline_run_audit (
    run_id           VARCHAR(250)  NOT NULL,
    run_date         DATE          NOT NULL,
    load_type        VARCHAR(20),
    dataset          VARCHAR(50)   NOT NULL,
    stage            VARCHAR(30)   NOT NULL,
    status           VARCHAR(20)   NOT NULL,
    records_in       BIGINT,
    records_out      BIGINT,
    records_rejected BIGINT,
    quality_score    DECIMAL(5,2),
    started_at       TIMESTAMP,
    finished_at      TIMESTAMP,
    duration_seconds DECIMAL(10,2),
    error_message    VARCHAR(1000),
    PRIMARY KEY (run_id, dataset, stage)
)
SORTKEY (run_date);
