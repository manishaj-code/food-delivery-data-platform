-- Optional read-only user for Power BI (Amazon Redshift). Spec 08 §9, spec 11.
-- Not part of init-warehouse: run once by an admin after the Power BI views exist (Phase 8).
-- The password is passed as the query parameter %(powerbi_password)s at run time and is
-- never committed or logged. Grants cover the reporting views only, not the base tables.

CREATE USER powerbi_reader PASSWORD %(powerbi_password)s;

GRANT USAGE ON SCHEMA {schema} TO powerbi_reader;

GRANT SELECT ON
    {schema}.vw_daily_orders,
    {schema}.vw_daily_revenue,
    {schema}.vw_restaurant_performance,
    {schema}.vw_delivery_performance,
    {schema}.vw_customer_summary,
    {schema}.vw_payment_summary
TO powerbi_reader;
