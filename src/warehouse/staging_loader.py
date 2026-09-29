"""Load one processed partition into a staging table (FR-052, spec 08 §7).

- ``RedshiftCopyLoader``: Redshift reads the Parquet straight from S3 with
  ``COPY … FORMAT AS PARQUET`` (IAM role, or session credentials in a sandbox).
- ``PostgresLoader``: local stand-in. pyarrow reads the partition's Parquet files batch by
  batch and streams them as CSV into ``COPY … FROM STDIN``.

Both load into the caller's transaction and never touch ``_manifest.json``.
"""

from __future__ import annotations

import io
from typing import Any, Protocol

import boto3
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from src.common.config import Settings
from src.common.exceptions import ConfigError
from src.common.storage import Storage
from src.warehouse.sql_runner import execute, render_sql, translate_errors

PART_PREFIX = "part-"  # Spark's data files; everything else in a partition is metadata

_AUTHORIZATION = {
    "iam_role": "IAM_ROLE %(iam_role_arn)s",
    "session": "CREDENTIALS %(credentials)s",
}


class StagingLoader(Protocol):
    def load(self, conn: Any, table: str, partition: str) -> None: ...


class PostgresLoader:
    def __init__(self, storage: Storage, schema: str, batch_rows: int = 50_000) -> None:
        self.storage = storage
        self.schema = schema
        self.batch_rows = batch_rows

    def load(self, conn: Any, table: str, partition: str) -> None:
        keys = [
            key for key in self.storage.list(partition + PART_PREFIX) if key.endswith(".parquet")
        ]
        with translate_errors(f"staging load of {table}", key=partition), conn.cursor() as cursor:
            for key in keys:
                parquet = pq.ParquetFile(pa.BufferReader(self.storage.read_bytes(key)))
                columns = ", ".join(parquet.schema_arrow.names)
                copy_sql = f"COPY {self.schema}.{table} ({columns}) FROM STDIN WITH (FORMAT csv)"
                for batch in parquet.iter_batches(batch_size=self.batch_rows):
                    buffer = io.BytesIO()
                    pacsv.write_csv(batch, buffer, pacsv.WriteOptions(include_header=False))
                    buffer.seek(0)
                    cursor.copy_expert(copy_sql, buffer)


class RedshiftCopyLoader:
    def __init__(self, settings: Settings) -> None:
        if settings.storage_mode != "s3" or not settings.s3_bucket:
            raise ConfigError(
                "Redshift COPY reads from S3; set STORAGE_MODE=s3 and S3_BUCKET",
                variable="STORAGE_MODE",
            )
        self.settings = settings

    def copy_statement(self, table: str) -> str:
        return render_sql(
            "staging/copy_from_s3.sql",
            self.settings.redshift_schema,
            fragments={"authorization": _AUTHORIZATION[self.settings.redshift_copy_auth]},
            table=table,
        )

    def copy_parameters(self, partition: str) -> dict[str, str]:
        params = {"s3_prefix": f"s3://{self.settings.s3_bucket}/{partition}{PART_PREFIX}"}
        if self.settings.redshift_copy_auth == "iam_role":
            params["iam_role_arn"] = self.settings.redshift_iam_role_arn or ""
        else:
            params["credentials"] = _session_credentials(self.settings.aws_region)
        return params

    def load(self, conn: Any, table: str, partition: str) -> None:
        with translate_errors(f"COPY into {table}", key=partition):
            execute(conn, self.copy_statement(table), self.copy_parameters(partition))


def _session_credentials(region: str) -> str:
    """COPY ``CREDENTIALS`` string from the current AWS credential chain (sandbox fallback)."""
    credentials = boto3.session.Session(region_name=region).get_credentials()
    if credentials is None:
        raise ConfigError("No AWS credentials available for REDSHIFT_COPY_AUTH=session")
    frozen = credentials.get_frozen_credentials()
    parts = [
        f"aws_access_key_id={frozen.access_key}",
        f"aws_secret_access_key={frozen.secret_key}",
    ]
    if frozen.token:
        parts.append(f"token={frozen.token}")
    return ";".join(parts)


def get_staging_loader(settings: Settings, storage: Storage) -> StagingLoader:
    if settings.warehouse_type == "redshift":
        return RedshiftCopyLoader(settings)
    return PostgresLoader(storage, settings.redshift_schema)
