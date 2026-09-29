-- Dimensions (Amazon Redshift). Spec 08 §4. SCD Type 1; small tables copied to every
-- node (DISTSTYLE ALL) so fact joins never redistribute. PK/UNIQUE are informational only.

CREATE TABLE IF NOT EXISTS {schema}.dim_customer (
    customer_key          BIGINT IDENTITY(1,1),
    customer_id           VARCHAR(10)  NOT NULL,
    customer_name         VARCHAR(100) NOT NULL,
    email                 VARCHAR(150),
    city                  VARCHAR(50)  NOT NULL,
    signup_date           DATE         NOT NULL,
    source_ingestion_date DATE         NOT NULL,
    created_at            TIMESTAMP    NOT NULL,
    updated_at            TIMESTAMP    NOT NULL,
    PRIMARY KEY (customer_key),
    UNIQUE (customer_id)
)
DISTSTYLE ALL
SORTKEY (customer_id);

CREATE TABLE IF NOT EXISTS {schema}.dim_restaurant (
    restaurant_key        BIGINT IDENTITY(1,1),
    restaurant_id         VARCHAR(10)  NOT NULL,
    restaurant_name       VARCHAR(100) NOT NULL,
    city                  VARCHAR(50)  NOT NULL,
    cuisine               VARCHAR(50)  NOT NULL,
    rating                DECIMAL(2,1),
    source_ingestion_date DATE         NOT NULL,
    created_at            TIMESTAMP    NOT NULL,
    updated_at            TIMESTAMP    NOT NULL,
    PRIMARY KEY (restaurant_key),
    UNIQUE (restaurant_id)
)
DISTSTYLE ALL
SORTKEY (restaurant_id);

CREATE TABLE IF NOT EXISTS {schema}.dim_delivery_partner (
    delivery_partner_key  BIGINT IDENTITY(1,1),
    delivery_partner_id   VARCHAR(10)  NOT NULL,
    partner_name          VARCHAR(100) NOT NULL,
    city                  VARCHAR(50)  NOT NULL,
    joining_date          DATE         NOT NULL,
    source_ingestion_date DATE         NOT NULL,
    created_at            TIMESTAMP    NOT NULL,
    updated_at            TIMESTAMP    NOT NULL,
    PRIMARY KEY (delivery_partner_key),
    UNIQUE (delivery_partner_id)
)
DISTSTYLE ALL
SORTKEY (delivery_partner_id);

CREATE TABLE IF NOT EXISTS {schema}.dim_date (
    date_key     INTEGER     NOT NULL,
    full_date    DATE        NOT NULL,
    day_of_month SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,
    day_name     VARCHAR(10) NOT NULL,
    week_of_year SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    month_name   VARCHAR(10) NOT NULL,
    quarter      SMALLINT    NOT NULL,
    year         SMALLINT    NOT NULL,
    is_weekend   BOOLEAN     NOT NULL,
    PRIMARY KEY (date_key),
    UNIQUE (full_date)
)
DISTSTYLE ALL
SORTKEY (date_key);
