-- Staging tables (PostgreSQL). Spec 08 §6: same columns, order, and types as the processed
-- Parquet (spec 07 §6), so one partition loads column-for-column. Cleared before each load.

CREATE TABLE IF NOT EXISTS {schema}.stg_customer (
    customer_id           VARCHAR(10),
    customer_name         VARCHAR(100),
    email                 VARCHAR(150),
    city                  VARCHAR(50),
    signup_date           DATE,
    source_ingestion_date DATE,
    _run_id               VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS {schema}.stg_restaurant (
    restaurant_id         VARCHAR(10),
    restaurant_name       VARCHAR(100),
    city                  VARCHAR(50),
    cuisine               VARCHAR(50),
    rating                DECIMAL(2,1),
    source_ingestion_date DATE,
    _run_id               VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS {schema}.stg_delivery_partner (
    delivery_partner_id   VARCHAR(10),
    partner_name          VARCHAR(100),
    city                  VARCHAR(50),
    joining_date          DATE,
    source_ingestion_date DATE,
    _run_id               VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS {schema}.stg_order (
    order_id              VARCHAR(12),
    customer_id           VARCHAR(10),
    restaurant_id         VARCHAR(10),
    order_timestamp       TIMESTAMP,
    order_date            DATE,
    order_amount          DECIMAL(10,2),
    order_status          VARCHAR(20),
    source_ingestion_date DATE,
    _run_id               VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS {schema}.stg_payment (
    payment_id            VARCHAR(12),
    order_id              VARCHAR(12),
    payment_method        VARCHAR(10),
    payment_amount        DECIMAL(10,2),
    payment_status        VARCHAR(10),
    payment_timestamp     TIMESTAMP,
    payment_date          DATE,
    source_ingestion_date DATE,
    _run_id               VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS {schema}.stg_delivery (
    delivery_id               VARCHAR(12),
    order_id                  VARCHAR(12),
    delivery_partner_id       VARCHAR(10),
    pickup_time               TIMESTAMP,
    delivery_time             TIMESTAMP,
    delivery_duration_minutes DECIMAL(8,2),
    delivery_status           VARCHAR(12),
    order_date                DATE,
    source_ingestion_date     DATE,
    _run_id                   VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS {schema}.stg_daily_order_metrics (
    order_date            DATE,
    total_orders          BIGINT,
    paid_orders           BIGINT,
    total_revenue         DECIMAL(14,2),
    average_order_value   DECIMAL(10,2),
    cancelled_orders      BIGINT,
    cancellation_rate_pct DECIMAL(5,2),
    source_ingestion_date DATE,
    _run_id               VARCHAR(250)
);
