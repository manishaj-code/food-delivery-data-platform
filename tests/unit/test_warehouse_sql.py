"""SQL rendering and Redshift COPY statements (no database needed)."""

from __future__ import annotations

import pytest

from src.common.config import load_settings
from src.common.exceptions import ConfigError
from src.warehouse.init_warehouse import DDL_FILES
from src.warehouse.loader import LOAD_ORDER
from src.warehouse.post_load_checks import CHECKS
from src.warehouse.sql_runner import SQL_DIR, render_sql, split_statements
from src.warehouse.staging_loader import RedshiftCopyLoader

pytestmark = pytest.mark.unit

REDSHIFT = {
    "WAREHOUSE_TYPE": "redshift",
    "STORAGE_MODE": "s3",
    "S3_BUCKET": "food-delivery-data-dev",
    "REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:ap-south-1:123456789012:secret:rs",
    "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123456789012:role/redshift-s3-read",
}
PARTITION = "processed/orders/year=2026/month=09/day=29/"


def test_render_fills_identifiers_and_strips_comments() -> None:
    sql = render_sql("warehouse/upsert_dim_customer.sql", "food_delivery")

    assert "food_delivery.dim_customer" in sql
    assert "{" not in sql
    assert "--" not in sql
    assert len(split_statements(sql)) == 2  # UPDATE, INSERT


@pytest.mark.parametrize("schema", ["Food", "x; DROP TABLE t", "1abc", ""])
def test_render_rejects_unsafe_identifiers(schema: str) -> None:
    with pytest.raises(ConfigError, match="identifier"):
        render_sql("ddl/postgres/01_schema.sql", schema)


def test_render_reports_unfilled_placeholders() -> None:
    with pytest.raises(ConfigError, match="placeholders"):
        render_sql("staging/copy_from_s3.sql", "food_delivery", table="stg_order")


@pytest.mark.parametrize("dialect", ["postgres", "redshift"])
def test_both_dialects_define_the_same_tables(dialect: str) -> None:
    """TR-04: the two DDL sets must not drift apart in which tables they create."""
    tables = set()
    for name in DDL_FILES:
        sql = render_sql(f"ddl/{dialect}/{name}", "s")
        tables |= {
            statement.split("s.", 1)[1].split()[0]
            for statement in split_statements(sql)
            if statement.upper().startswith("CREATE TABLE")
        }
    expected = {table.target for table in LOAD_ORDER} | {t.staging for t in LOAD_ORDER}
    assert tables == expected | {"dim_date", "pipeline_run_audit"}


def test_every_sql_file_referenced_by_code_exists() -> None:
    files = [table.upsert_sql for table in LOAD_ORDER if table.business_key]
    files += [check.sql_file for check in CHECKS if check.sql_file]
    assert all((SQL_DIR / name).is_file() for name in files)


def test_redshift_copy_with_iam_role() -> None:
    copy = RedshiftCopyLoader(load_settings(REDSHIFT))

    statement = copy.copy_statement("stg_order")
    params = copy.copy_parameters(PARTITION)

    assert statement.split() == [
        "COPY",
        "food_delivery.stg_order",
        "FROM",
        "%(s3_prefix)s",
        "IAM_ROLE",
        "%(iam_role_arn)s",
        "FORMAT",
        "AS",
        "PARQUET;",
    ]
    assert params == {
        "s3_prefix": f"s3://food-delivery-data-dev/{PARTITION}part-",  # never _manifest.json
        "iam_role_arn": REDSHIFT["REDSHIFT_IAM_ROLE_ARN"],
    }


def test_redshift_copy_with_session_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sandbox fallback: temporary credentials from the chain, bound as a parameter."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "ASIAEXAMPLE",
        "AWS_SECRET_ACCESS_KEY": "secret-example",
        "AWS_SESSION_TOKEN": "token-example",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    copy = RedshiftCopyLoader(load_settings(REDSHIFT | {"REDSHIFT_COPY_AUTH": "session"}))

    statement = copy.copy_statement("stg_order")
    params = copy.copy_parameters(PARTITION)

    assert "CREDENTIALS %(credentials)s" in statement
    assert "secret-example" not in statement  # only in the bound parameter
    assert params["credentials"] == (
        "aws_access_key_id=ASIAEXAMPLE;aws_secret_access_key=secret-example;token=token-example"
    )


def test_redshift_copy_needs_s3_storage() -> None:
    settings = load_settings(REDSHIFT | {"STORAGE_MODE": "local"})
    with pytest.raises(ConfigError, match="STORAGE_MODE=s3"):
        RedshiftCopyLoader(settings)
