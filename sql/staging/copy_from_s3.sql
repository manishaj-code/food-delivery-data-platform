-- Load one processed partition into a staging table (Amazon Redshift). Spec 08 §7, FR-052.
-- %(s3_prefix)s is the partition's "…/part-" key prefix, so _manifest.json is not loaded.
-- {authorization} is either IAM_ROLE %(iam_role_arn)s or CREDENTIALS %(credentials)s
-- (sandbox fallback, REDSHIFT_COPY_AUTH=session); values are bound as query parameters.
COPY {schema}.{table}
FROM %(s3_prefix)s
{authorization}
FORMAT AS PARQUET;
