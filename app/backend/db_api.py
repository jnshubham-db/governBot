"""
Databricks SQL connection and query helpers for the API (no Streamlit).
Reuses the same connection logic as app/db.py but with a simple cache for the process.
"""
from __future__ import annotations

import os
from typing import Any

import pandas as pd

# In-process cache: http_path -> connection (no cross-request persistence; uvicorn workers may fork)
_connection_cache: dict[str, Any] = {}


def get_config():
    from databricks.sdk.core import Config
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "").strip()
    return Config(profile=profile) if profile else Config()


def _credentials_provider():
    cfg = get_config()
    def get_headers():
        return cfg.authenticate()
    return get_headers


def get_connection(http_path: str) -> Any:
    if http_path in _connection_cache:
        return _connection_cache[http_path]
    from databricks import sql as databricks_sql
    cfg = get_config()
    host = (cfg.host or "").replace("https://", "").replace("http://", "").strip("/").split("/")[0]
    if not host:
        raise ValueError("Databricks host not set (DATABRICKS_HOST)")
    tls_no_verify = os.environ.get("DATABRICKS_TLS_NO_VERIFY", "").strip().lower() in ("1", "true", "yes")
    conn = databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        credentials_provider=_credentials_provider,
        _tls_no_verify=tls_no_verify,
    )
    _connection_cache[http_path] = conn
    return conn


def run_query(conn: Any, sql: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall_arrow()
        if result is None:
            return pd.DataFrame()
        return result.to_pandas()


def execute_statement(conn: Any, sql: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql)
    try:
        conn.commit()
    except Exception:
        pass


TABLE_CONFIG_WORKSPACES = "governance_config_workspaces"
TABLE_PREAPPROVED_IDENTITIES = "governance_preapproved_identities"
TABLE_VIOLATIONS_STAGING = "governance_violations_staging"
TABLE_CONTROL_ACTIONS = "governance_control_actions"
TABLE_FILTERS = "governance_filters"
TABLE_PENDING_APPROVALS = "governance_pending_approvals"
