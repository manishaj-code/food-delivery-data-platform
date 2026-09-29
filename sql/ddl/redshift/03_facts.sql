-- Facts (Amazon Redshift). Spec 08 §5. All three facts are distributed on order_id so an
-- order, its payments, and its delivery sit on the same slice; sorted by date for range scans.

CREATE TABLE IF NOT EXISTS {schema}.fact_order (
    order_key             BIGINT IDENTITY(1,1),
    order_id              VARCHAR(12)   NOT NULL,
    customer_key          BIGINT        NOT NULL,
    restaurant_key        BIGINT        NOT NULL,
    order_date_key        INTEGER       NOT NULL,
    order_timestamp       TIMESTAMP     NOT NULL,
    order_amount          DECIMAL(10,2) NOT NULL,
    order_status          VARCHAR(20)   NOT NULL,
    source_ingestion_date DATE          NOT NULL,
    created_at            TIMESTAMP     NOT NULL,
    updated_at            TIMESTAMP     NOT NULL,
    PRIMARY KEY (order_key),
    UNIQUE (order_id)
)
DISTKEY (order_id)
SORTKEY (order_date_key);

CREATE TABLE IF NOT EXISTS {schema}.fact_payment (
    payment_key           BIGINT IDENTITY(1,1),
    payment_id            VARCHAR(12)   NOT NULL,
    order_id              VARCHAR(12)   NOT NULL,
    payment_method        VARCHAR(10)   NOT NULL,
    payment_amount        DECIMAL(10,2) NOT NULL,
    payment_status        VARCHAR(10)   NOT NULL,
    payment_date_key      INTEGER       NOT NULL,
    payment_timestamp     TIMESTAMP     NOT NULL,
    source_ingestion_date DATE          NOT NULL,
    created_at            TIMESTAMP     NOT NULL,
    updated_at            TIMESTAMP     NOT NULL,
    PRIMARY KEY (payment_key),
    UNIQUE (payment_id)
)
DISTKEY (order_id)
SORTKEY (payment_date_key);

CREATE TABLE IF NOT EXISTS {schema}.fact_delivery (
    delivery_key              BIGINT IDENTITY(1,1),
    delivery_id               VARCHAR(12)  NOT NULL,
    order_id                  VARCHAR(12)  NOT NULL,
    delivery_partner_key      BIGINT       NOT NULL,
    order_date_key            INTEGER      NOT NULL,
    pickup_time               TIMESTAMP,
    delivery_time             TIMESTAMP,
    delivery_duration_minutes DECIMAL(8,2),
    delivery_status           VARCHAR(12)  NOT NULL,
    source_ingestion_date     DATE         NOT NULL,
    created_at                TIMESTAMP    NOT NULL,
    updated_at                TIMESTAMP    NOT NULL,
    PRIMARY KEY (delivery_key),
    UNIQUE (delivery_id)
)
DISTKEY (order_id)
SORTKEY (order_date_key);
