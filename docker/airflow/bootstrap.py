"""Bootstrap for the local Airflow container (docker-compose only).

``db``    Create the ``airflow`` role and database in the Compose PostgreSQL if missing,
          and keep the role's password in sync with ``AIRFLOW_DB_PASSWORD``. Runs as the
          local warehouse superuser (``REDSHIFT_USER``), so it works on a new or an
          existing Postgres volume (``docker-entrypoint-initdb.d`` only runs on new ones).
``auth``  Write the SimpleAuthManager password file from ``AIRFLOW_ADMIN_USERNAME`` /
          ``AIRFLOW_ADMIN_PASSWORD`` (Airflow 3 would otherwise generate a random one).

Credentials come only from the environment (the untracked ``.env``) and are never printed.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

AIRFLOW_DB = "airflow"
AIRFLOW_ROLE = "airflow"


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"{name} is not set; add it to .env (see .env.example)")
    return value


def create_metadata_db() -> None:
    import psycopg2
    from psycopg2 import sql

    password = _require("AIRFLOW_DB_PASSWORD")
    conn = psycopg2.connect(
        host=os.environ.get("REDSHIFT_HOST", "postgres"),
        port=int(os.environ.get("REDSHIFT_PORT", "5432")),
        dbname=os.environ.get("REDSHIFT_DATABASE", "warehouse"),
        user=os.environ.get("REDSHIFT_USER", "warehouse_user"),
        password=_require("REDSHIFT_PASSWORD"),
        connect_timeout=10,
    )
    # CREATE DATABASE cannot run inside a transaction. No "with conn:" — since psycopg2 2.9
    # that opens a transaction even on an autocommit connection.
    conn.autocommit = True
    with conn.cursor() as cursor:
        role = sql.Identifier(AIRFLOW_ROLE)
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (AIRFLOW_ROLE,))
        verb = "ALTER" if cursor.fetchone() else "CREATE"
        cursor.execute(sql.SQL(verb + " ROLE {} LOGIN PASSWORD %s").format(role), (password,))
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (AIRFLOW_DB,))
        if not cursor.fetchone():
            cursor.execute(
                sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(AIRFLOW_DB), role)
            )
            print(f"Created database {AIRFLOW_DB}")
    conn.close()
    print(f"Airflow metadata database ready (role {verb.lower()}d)")


def write_passwords_file() -> None:
    path = Path(_require("AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"))
    username = os.environ.get("AIRFLOW_ADMIN_USERNAME", "admin")
    path.write_text(json.dumps({username: _require("AIRFLOW_ADMIN_PASSWORD")}), encoding="utf-8")
    path.chmod(0o600)
    print(f"Airflow login user: {username}")


if __name__ == "__main__":
    commands = {"db": create_metadata_db, "auth": write_passwords_file}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        sys.exit(f"usage: bootstrap.py {{{'|'.join(commands)}}}")
    commands[sys.argv[1]]()
