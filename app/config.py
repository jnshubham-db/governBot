"""
GovernBot config: catalog, schema, warehouse HTTP path from environment.
Used by the FastAPI backend (no Streamlit).
"""
import os


def get_catalog() -> str:
    return (os.environ.get("GOVERNANCE_CATALOG") or "").strip()


def get_schema() -> str:
    return (os.environ.get("GOVERNANCE_SCHEMA") or "").strip()


def get_warehouse_http_path() -> str:
    return (os.environ.get("DATABRICKS_APP_WAREHOUSE_HTTP_PATH") or "").strip()
