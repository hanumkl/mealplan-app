"""
Lakebase (Databricks-managed Postgres) connection helper.

Follows the bootcamp pattern: a single LAKEBASE_URL pointing at a native
Postgres role with a static password, so there's no token-refresh logic.

Resolution order:
  1. LAKEBASE_URL environment variable  (local development, from .env)
  2. the Databricks secret scope/key    (how Databricks Apps runs it)
"""

import base64
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

_cached_url: str | None = None


def _lakebase_url() -> str:
    """Return the Postgres URL, preferring the env var so local dev needs no workspace auth."""
    global _cached_url
    if _cached_url:
        return _cached_url

    env_url = os.environ.get("LAKEBASE_URL")
    if env_url:
        _cached_url = env_url
        return _cached_url

    # Imported lazily: local runs with LAKEBASE_URL set shouldn't need the SDK
    # to be configured at all.
    from databricks.sdk import WorkspaceClient

    secret = WorkspaceClient().secrets.get_secret(scope=_SCOPE, key=_KEY)
    _cached_url = base64.b64decode(secret.value).decode("utf-8")
    return _cached_url


@contextmanager
def get_connection():
    """Yield a psycopg2 connection with a RealDictCursor factory."""
    conn = psycopg2.connect(_lakebase_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def get_engine():
    """Return a SQLAlchemy engine for Lakebase (used by the Spark notebooks)."""
    return create_engine(_lakebase_url())


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a read query and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_one(sql: str, params: tuple | dict | None = None) -> dict | None:
    """Run a read query and return the first row, or None."""
    rows = run_query(sql, params)
    return rows[0] if rows else None


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    """Run an INSERT/UPDATE/DELETE and return the affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


def run_returning(sql: str, params: tuple | dict | None = None) -> dict | None:
    """Run a write with a RETURNING clause and give back the returned row."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return row
