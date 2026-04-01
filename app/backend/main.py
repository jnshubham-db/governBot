"""
GovernBot FastAPI backend: config, summary, actions, configs (workspaces, identities, filters).

Data routes use the Databricks SQL connector with user authorization (OBO) when
``X-Forwarded-Access-Token`` or ``Authorization: Bearer`` is present (Databricks Apps
forwards the user token). Otherwise unified SDK auth is used (e.g. local dev with
a CLI profile). Set ``GOVERNANCE_REQUIRE_OBO=1`` to reject requests without a user token.

All routes accept X-Catalog, X-Schema, X-Warehouse-HTTP-Path headers (or env defaults).
"""
import math
import os
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Generator, Optional

# Add parent app dir for config (so "from config" works when running from app/)
_APP_DIR = os.path.join(os.path.dirname(__file__), "..")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import get_catalog, get_schema, get_warehouse_http_path
from backend import db_api

get_connection = db_api.get_connection
run_query = db_api.run_query
execute_statement = db_api.execute_statement
TABLE_CONFIG_WORKSPACES = db_api.TABLE_CONFIG_WORKSPACES
TABLE_PREAPPROVED_IDENTITIES = db_api.TABLE_PREAPPROVED_IDENTITIES
TABLE_VIOLATIONS_STAGING = db_api.TABLE_VIOLATIONS_STAGING
TABLE_CONTROL_ACTIONS = db_api.TABLE_CONTROL_ACTIONS
TABLE_FILTERS = db_api.TABLE_FILTERS
TABLE_PENDING_APPROVALS = db_api.TABLE_PENDING_APPROVALS


@dataclass
class GovernanceSql:
    conn: Any
    catalog: str
    schema: str


def _catalog_header(x: Optional[str] = Header(None, alias="X-Catalog")) -> str:
    return (x or "").strip() or get_catalog()


def _schema_header(x: Optional[str] = Header(None, alias="X-Schema")) -> str:
    return (x or "").strip() or get_schema()


def _http_path_header(x: Optional[str] = Header(None, alias="X-Warehouse-HTTP-Path")) -> str:
    return (x or "").strip() or get_warehouse_http_path()


def get_governance_sql(
    request: Request,
    catalog: str = Depends(_catalog_header),
    schema: str = Depends(_schema_header),
    http_path: str = Depends(_http_path_header),
    x_forwarded_access_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token"),
    authorization: Optional[str] = Header(None),
) -> Generator[GovernanceSql, None, None]:
    """SQL warehouse connection using user token (OBO) when present, else SDK auth (local dev)."""
    if not catalog or not schema or not http_path:
        raise HTTPException(400, "Set X-Catalog, X-Schema, X-Warehouse-HTTP-Path or env vars.")
    token = (x_forwarded_access_token or "").strip()
    if not token and authorization:
        auth = authorization.strip()
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
    use_obo = bool(token)
    require_obo = os.environ.get("GOVERNANCE_REQUIRE_OBO", "").strip().lower() in ("1", "true", "yes")
    if require_obo and not use_obo:
        raise HTTPException(
            401,
            "User authorization required: X-Forwarded-Access-Token or Authorization Bearer "
            "(enable user authorization and the sql scope on the Databricks app).",
        )
    # If DATABRICKS_HOST is missing, OBO token exchange + SQL API need the workspace host from the proxy.
    host_override = None
    if use_obo and not (os.environ.get("DATABRICKS_HOST") or "").strip():
        host_override = (request.headers.get("x-forwarded-host") or "").strip()
    try:
        conn = get_connection(
            http_path,
            access_token=token if use_obo else None,
            host_override=host_override or None,
            catalog=catalog,
            schema=schema,
        )
    except Exception as e:
        raise HTTPException(
            502,
            f"Could not connect to Databricks: {db_api.format_sql_driver_error(e)}",
        )
    try:
        yield GovernanceSql(conn=conn, catalog=catalog, schema=schema)
    finally:
        if use_obo:
            try:
                conn.close()
            except Exception:
                pass


def _esc(s) -> str:
    if s is None or (isinstance(s, float) and str(s) == "nan"):
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _opt(s) -> str:
    if s is None or (isinstance(s, str) and not str(s).strip()):
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _arr_sql(arr) -> str:
    if not arr:
        return "array()"
    return "array(" + ", ".join(_esc(x) for x in arr) + ")"


def _json_safe(val: Any) -> Any:
    """Convert a value to something JSON-serializable (no nan, inf, NaT, numpy, or Arrow)."""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except Exception:
        pass
    if hasattr(val, "isoformat"):
        return val.isoformat()
    # Lists/arrays (e.g. ARRAY<STRING> from Delta) — convert to plain list
    if isinstance(val, (list, tuple)) or (hasattr(val, "__iter__") and not isinstance(val, (str, dict))):
        try:
            return [_json_safe(x) for x in val]
        except Exception:
            return list(val)
    # Dict-like (e.g. MAP from Delta)
    if isinstance(val, dict):
        return {str(k): _json_safe(v) for k, v in val.items()}
    # Numpy/scalar -> native Python
    if hasattr(val, "item"):
        return _json_safe(val.item())
    if isinstance(val, (bool, int, str)):
        return val
    if isinstance(val, float):
        return val
    return str(val)


def _rows_json_safe(rows: list[dict]) -> list[dict]:
    """Replace nan/inf/NaT in row dicts so FastAPI can serialize to JSON."""
    for r in rows:
        for k in list(r.keys()):
            r[k] = _json_safe(r[k])
    return rows


def _ensure_pending_approvals(conn, catalog: str, schema: str):
    full = f"{catalog}.{schema}.{TABLE_PENDING_APPROVALS}"
    execute_statement(
        conn,
        f"""CREATE TABLE IF NOT EXISTS {full} (
            violation_id STRING, approved_at TIMESTAMP, note STRING, created_at TIMESTAMP
        ) USING DELTA"""
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # cleanup if needed


app = FastAPI(title="GovernBot API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# --- Models ---
class AppConfig(BaseModel):
    catalog: str = ""
    schema_: str = ""
    warehouse_http_path: str = ""

    class Config:
        populate_by_name = True
        fields = {"schema_": "schema"}


class SummaryResponse(BaseModel):
    total: int
    pending: int
    completed: int
    by_type: list[dict]
    by_object_type: list[dict]
    latest: list[dict]


class TrendPoint(BaseModel):
    period: str
    generated: int
    failed: int
    completed: int


class SummaryTrendResponse(BaseModel):
    trend: list[TrendPoint]


class ActionRow(BaseModel):
    violation_id: str
    workspace_id: Optional[str]
    object_id: Optional[str]
    event_time: Optional[str]
    violation_type: Optional[str]
    object_type: Optional[str]
    object_name: Optional[str]
    user_email: Optional[str]
    violation_reason: Optional[str]
    remediation_action: Optional[str]
    processing_status: Optional[str]
    ca_status: Optional[str]
    retry_count: Optional[int]
    error_message: Optional[str]
    remediation_details: Optional[str]


class ApproveBody(BaseModel):
    note: Optional[str] = ""


class RejectBody(BaseModel):
    reason: str = ""


class NoteBody(BaseModel):
    note: str = ""


class WorkspaceBody(BaseModel):
    workspace_id: str
    workspace_name: str
    workspace_url: str = ""
    warehouse_id: Optional[str] = None
    enforcement_enabled: bool = False
    notification_email: Optional[str] = None
    notification_slack_webhook: Optional[str] = None
    enabled_object_types: list[str] = []
    max_retry_attempts: int = 3
    created_by: str = "api"


class IdentityBody(BaseModel):
    identity_name: str
    identity_type: str
    display_name: str = ""
    can_manage_resources: bool = False
    can_manage_permissions: bool = False
    approved_actions: list[str] = []
    is_active: bool = True


class FilterBody(BaseModel):
    filter_id: Optional[str] = None
    filter_name: str
    service_name: str = ""
    action_name: str = ""
    object_type: str = ""
    object_id_expr: str = ""
    object_name_expr: str = ""
    violation_type: str = ""
    remediation_action: str = ""
    is_active: bool = True
    description: str = ""


# --- Routes ---
@app.get("/api/config")
def api_config():
    return {
        "catalog": get_catalog(),
        "schema": get_schema(),
        "warehouse_http_path": get_warehouse_http_path() or "",
    }


@app.get("/api/summary")
def api_summary(
    hours: int = 24,
    gs: GovernanceSql = Depends(get_governance_sql),
):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    filters = f"{catalog}.{schema}.{TABLE_FILTERS}"
    actions = f"{catalog}.{schema}.{TABLE_CONTROL_ACTIONS}"
    interval_sql = f"current_timestamp() - INTERVAL '{max(1, hours)}' HOUR"
    sql = f"""
        SELECT violation_id, workspace_id, created_at as event_time, violation_type, object_type, violator_email as user_email, object_name, remediation_status as processing_status
        FROM {actions} actions
        inner join (select distinct violation_type, remediation_action from {filters}) filters 
            on trim(actions.action_type) = trim(filters.remediation_action)
        WHERE created_at >= {interval_sql}
        ORDER BY created_at DESC
    """
    sql_status = f"""
        select 
            count(*) as total_violations, 
            count(case when remediation_status not in ('SUCCESS','SKIPPED') then 1 else null end) as pending_violations, 
            total_violations - pending_violations as resolved_violations 
        from {actions}
        where created_at >= {interval_sql}"""
    try:
        df = run_query(conn, sql)
        df_status = run_query(conn, sql_status)
    except Exception as e:
        raise HTTPException(502, str(e))
    df.columns = [str(c).lower() for c in df.columns]
    total, pending, completed = df_status.iloc[0]
    by_type = []
    if not df.empty and "violation_type" in df.columns:
        by_type = df["violation_type"].value_counts().reset_index().rename(columns={"violation_type": "name", "count": "count"})
        by_type = by_type.to_dict("records")
    by_object_type = []
    if not df.empty and "object_type" in df.columns:
        by_object_type = df["object_type"].value_counts().reset_index().rename(columns={"object_type": "name", "count": "count"})
        by_object_type = by_object_type.to_dict("records")
    latest = df.head(50).to_dict("records") if not df.empty else []
    latest = _rows_json_safe(latest)
    return SummaryResponse(total=total, pending=pending, completed=completed, by_type=by_type, by_object_type=by_object_type, latest=latest)


NUM_TREND_PERIODS = 5


@app.get("/api/summary/trend")
def api_summary_trend(
    hours: int = 24,
    gs: GovernanceSql = Depends(get_governance_sql),
):
    """Time-series for the summary line chart: 5 equal intervals, generated/resolved per period and running pending."""
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    actions = f"{catalog}.{schema}.{TABLE_CONTROL_ACTIONS}"
    hours_clamped = max(1, hours)
    interval_sql = f"current_timestamp() - INTERVAL '{hours_clamped}' HOUR"
    range_sec = hours_clamped * 3600
    bucket_sec = range_sec / NUM_TREND_PERIODS
    trend: list[dict] = []

    # Bucket by period index 0..4: floor((unix_ts - start_ts) / bucket_sec), clamped. Use seconds to avoid dialect issues.
    sql_query = f"""
        select 
            period_idx, 
            sum(failed) over (order by period_idx) as failed_cumulative, 
            sum(generated) over (order by period_idx) as generated_cumulative
        from (
            SELECT 
                least({NUM_TREND_PERIODS - 1}, greatest(0, cast(floor(
                    (unix_timestamp(created_at) - (unix_timestamp(current_timestamp()) - {int(range_sec)})) / {bucket_sec}
                ) as int))) AS period_idx, 
                count(case when remediation_status NOT IN ('COMPLETED', 'SKIPPED') then 1 else null end) AS failed,
                count(*) AS generated
            FROM {actions}
            WHERE created_at >= {interval_sql}
            GROUP BY 1
            ORDER BY 1
        ) as subquery
    """

    def run_summary_trend():
        df = run_query(conn, sql_query)
        df.columns = [str(c).lower() for c in df.columns]
        return {
            int(row["period_idx"]): [
                                    int(row.get("generated_cumulative", 0)), 
                                    int(row.get("failed_cumulative", 0))
                                    ] for _, row in df.iterrows()
        }
    try:
        data = run_summary_trend()
        prev_generated = 0
        prev_failed = 0
        for i in range(NUM_TREND_PERIODS):
            period = i + 1
            row = data.get(i, [prev_generated, prev_failed])
            trend.append({
                "period": str(period),
                "generated": row[0],
                "failed": row[1],
                "completed": row[0] - row[1],
            })
            prev_generated = row[0]
            prev_failed = row[1]
    except Exception as e:
        raise HTTPException(502, str(e))
    return SummaryTrendResponse(trend=trend)


@app.get("/api/actions")
def api_actions_list(
    workspace: Optional[str] = None,
    violation_type: Optional[str] = None,
    remediation_action: Optional[str] = None,
    gs: GovernanceSql = Depends(get_governance_sql),
):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    filters = f"{catalog}.{schema}.{TABLE_FILTERS}"
    actions = f"{catalog}.{schema}.{TABLE_CONTROL_ACTIONS}"
    sql = f"""
        SELECT a.violation_id, a.workspace_id, a.object_id, date_format(a.created_at, 'yyyy-MM-dd HH:mm:ss') as event_time, a.object_type, a.object_name,
                a.violator_email as user_email, filter.violation_type as violation_type, a.action_type as remediation_action, a.remediation_status as processing_status,
                a.remediation_status as ca_status, a.retry_count, a.error_message, a.remediation_details
        FROM {actions} a
        inner join (select distinct violation_type, remediation_action from {filters}) filter
            on trim(a.action_type) = trim(filter.remediation_action)
        WHERE 
        (a.remediation_status NOT IN ('SUCCESS', 'SKIPPED')) or 
        (a.remediation_status IN ('SUCCESS') and a.action_type like '%REPORT%')
        ORDER BY a.created_at ASC
    """
    try:
        df = run_query(conn, sql)
        df.columns = [str(c).lower() for c in df.columns]
        if workspace and workspace != "(all)" and "workspace_id" in df.columns:
            df = df[df["workspace_id"].astype(str) == workspace]
        if violation_type and violation_type != "(all)" and "violation_type" in df.columns:
            df = df[df["violation_type"].astype(str) == violation_type]
        if remediation_action and remediation_action != "(all)" and "remediation_action" in df.columns:
            df = df[df["remediation_action"].astype(str) == remediation_action]
        rows = df.to_dict("records")
        return {"rows": _rows_json_safe(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Actions query failed: {e}")


@app.post("/api/actions/approve")
def api_actions_approve(
    body: ApproveBody,
    violation_id: str = Header(..., alias="X-Violation-Id"),
    gs: GovernanceSql = Depends(get_governance_sql),
):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    _ensure_pending_approvals(conn, catalog, schema)
    actions = f"{catalog}.{schema}.{TABLE_CONTROL_ACTIONS}"
    execute_statement(
        conn,
        f"UPDATE {actions} SET remediation_status = 'SUCCESS', remediation_details = {_esc(body.note)}, updated_at = current_timestamp() WHERE violation_id = {_esc(violation_id)}",
    )
    return {"ok": True}


@app.post("/api/actions/reject")
def api_actions_reject(
    body: RejectBody,
    violation_id: str = Header(..., alias="X-Violation-Id"),
    gs: GovernanceSql = Depends(get_governance_sql),
):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    actions = f"{catalog}.{schema}.{TABLE_CONTROL_ACTIONS}"
    # Minimal row from violation - we need to fetch it or accept from body
    execute_statement(
        conn,
        f"""UPDATE {actions} SET remediation_status = 'SKIPPED', remediation_details = {_esc(body.reason)}, updated_at = current_timestamp() WHERE violation_id = {_esc(violation_id)}""",
    )
    return {"ok": True}

@app.post("/api/actions/note")
def api_actions_note(
    body: NoteBody,
    violation_id: str = Header(..., alias="X-Violation-Id"),
    gs: GovernanceSql = Depends(get_governance_sql),
):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    actions = f"{catalog}.{schema}.{TABLE_CONTROL_ACTIONS}"
    execute_statement(
        conn,
        f"UPDATE {actions} SET remediation_details = {_esc(body.note)}, updated_at = current_timestamp() WHERE violation_id = {_esc(violation_id)}",
    )
    return {"ok": True}


@app.get("/api/configs/workspaces")
def api_workspaces_list(gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_CONFIG_WORKSPACES}"
        df = run_query(conn, f"SELECT * FROM {full}")
        df.columns = [str(c).lower() for c in df.columns]
        return {"rows": _rows_json_safe(df.to_dict("records"))}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Workspaces query failed: {e}")


@app.post("/api/configs/workspaces")
def api_workspaces_create(body: WorkspaceBody, gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_CONFIG_WORKSPACES}"
        sql = f"""INSERT INTO {full} (workspace_id, workspace_name, workspace_url, warehouse_id, enforcement_enabled, notification_email, notification_slack_webhook, enabled_object_types, max_retry_attempts, created_at, updated_at, created_by)
    VALUES ({_esc(body.workspace_id)}, {_esc(body.workspace_name)}, {_esc(body.workspace_url)}, {_opt(body.warehouse_id)}, {str(body.enforcement_enabled).upper()}, {_opt(body.notification_email)}, {_opt(body.notification_slack_webhook)}, {_arr_sql(body.enabled_object_types)}, {body.max_retry_attempts}, current_timestamp(), current_timestamp(), {_esc(body.created_by)})"""
        execute_statement(conn, sql)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Workspaces create failed: {e}")


@app.put("/api/configs/workspaces/{workspace_id}")
def api_workspaces_update(workspace_id: str, body: WorkspaceBody, gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_CONFIG_WORKSPACES}"
        sql = f"""UPDATE {full} SET workspace_name = {_esc(body.workspace_name)}, workspace_url = {_esc(body.workspace_url)}, warehouse_id = {_opt(body.warehouse_id)}, enforcement_enabled = {str(body.enforcement_enabled).upper()}, notification_email = {_opt(body.notification_email)}, notification_slack_webhook = {_opt(body.notification_slack_webhook)}, enabled_object_types = {_arr_sql(body.enabled_object_types)}, max_retry_attempts = {body.max_retry_attempts}, updated_at = current_timestamp(), created_by = {_esc(body.created_by)} WHERE workspace_id = {_esc(workspace_id)}"""
        execute_statement(conn, sql)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Workspaces update failed: {e}")


@app.delete("/api/configs/workspaces/{workspace_id}")
def api_workspaces_delete(workspace_id: str, gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_CONFIG_WORKSPACES}"
        execute_statement(conn, f"DELETE FROM {full} WHERE workspace_id = {_esc(workspace_id)}")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Workspaces delete failed: {e}")


@app.get("/api/configs/identities")
def api_identities_list(gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_PREAPPROVED_IDENTITIES}"
        df = run_query(conn, f"SELECT * FROM {full}")
        df.columns = [str(c).lower() for c in df.columns]
        return {"rows": _rows_json_safe(df.to_dict("records"))}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Identities query failed: {e}")


@app.post("/api/configs/identities")
def api_identities_create(body: IdentityBody, gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_PREAPPROVED_IDENTITIES}"
        sql = f"""INSERT INTO {full} (identity_name, identity_type, display_name, can_manage_resources, can_manage_permissions, approved_actions, is_active, created_at, updated_at)
    VALUES ({_esc(body.identity_name)}, {_esc(body.identity_type)}, {_esc(body.display_name)}, {str(body.can_manage_resources).upper()}, {str(body.can_manage_permissions).upper()}, {_arr_sql(body.approved_actions)}, {str(body.is_active).upper()}, current_timestamp(), current_timestamp())"""
        execute_statement(conn, sql)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Identities create failed: {e}")


@app.put("/api/configs/identities/{identity_name}")
def api_identities_update(identity_name: str, body: IdentityBody, identity_type: str = Header(..., alias="X-Identity-Type"), gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_PREAPPROVED_IDENTITIES}"
        sql = f"""UPDATE {full} SET display_name = {_esc(body.display_name)}, can_manage_resources = {str(body.can_manage_resources).upper()}, can_manage_permissions = {str(body.can_manage_permissions).upper()}, approved_actions = {_arr_sql(body.approved_actions)}, is_active = {str(body.is_active).upper()}, updated_at = current_timestamp() WHERE identity_name = {_esc(identity_name)} AND identity_type = {_esc(identity_type)}"""
        execute_statement(conn, sql)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Identities update failed: {e}")


@app.delete("/api/configs/identities/{identity_name}")
def api_identities_delete(identity_name: str, identity_type: str = Header(..., alias="X-Identity-Type"), gs: GovernanceSql = Depends(get_governance_sql)):
    try:
        conn = gs.conn
        catalog = gs.catalog
        schema = gs.schema
        full = f"{catalog}.{schema}.{TABLE_PREAPPROVED_IDENTITIES}"
        execute_statement(conn, f"DELETE FROM {full} WHERE identity_name = {_esc(identity_name)} AND identity_type = {_esc(identity_type)}")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Identities delete failed: {e}")


@app.get("/api/configs/filters")
def api_filters_list(gs: GovernanceSql = Depends(get_governance_sql)):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    full = f"{catalog}.{schema}.{TABLE_FILTERS}"
    df = run_query(conn, f"SELECT * FROM {full}")
    df.columns = [str(c).lower() for c in df.columns]
    return {"rows": _rows_json_safe(df.to_dict("records"))}


@app.post("/api/configs/filters")
def api_filters_create(body: FilterBody, gs: GovernanceSql = Depends(get_governance_sql)):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    full = f"{catalog}.{schema}.{TABLE_FILTERS}"
    fid = body.filter_id or str(uuid.uuid4())
    sql = f"""INSERT INTO {full} (filter_id, filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, extra_columns, violation_type, remediation_action, is_active, description, created_at, updated_at)
    VALUES ({_esc(fid)}, {_esc(body.filter_name)}, {_esc(body.service_name)}, {_esc(body.action_name)}, {_esc(body.object_type)}, {_esc(body.object_id_expr)}, {_esc(body.object_name_expr)}, map(), {_esc(body.violation_type)}, {_esc(body.remediation_action)}, {str(body.is_active).upper()}, {_esc(body.description)}, current_timestamp(), current_timestamp())"""
    execute_statement(conn, sql)
    return {"ok": True}


@app.put("/api/configs/filters/{filter_id}")
def api_filters_update(filter_id: str, body: FilterBody, gs: GovernanceSql = Depends(get_governance_sql)):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    full = f"{catalog}.{schema}.{TABLE_FILTERS}"
    sql = f"""UPDATE {full} SET filter_name = {_esc(body.filter_name)}, service_name = {_esc(body.service_name)}, action_name = {_esc(body.action_name)}, object_type = {_esc(body.object_type)}, object_id_expr = {_esc(body.object_id_expr)}, object_name_expr = {_esc(body.object_name_expr)}, violation_type = {_esc(body.violation_type)}, remediation_action = {_esc(body.remediation_action)}, is_active = {str(body.is_active).upper()}, description = {_esc(body.description)}, updated_at = current_timestamp() WHERE filter_id = {_esc(filter_id)}"""
    execute_statement(conn, sql)
    return {"ok": True}


@app.delete("/api/configs/filters/{filter_id}")
def api_filters_delete(filter_id: str, gs: GovernanceSql = Depends(get_governance_sql)):
    conn = gs.conn
    catalog = gs.catalog
    schema = gs.schema
    full = f"{catalog}.{schema}.{TABLE_FILTERS}"
    execute_statement(conn, f"DELETE FROM {full} WHERE filter_id = {_esc(filter_id)}")
    return {"ok": True}


# Serve React build when present; always serve index.html for SPA routes so /summary, /configs etc. don't 404
# Resolve to absolute path so it works regardless of process cwd (e.g. run from app/ vs repo root).
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend", "dist"))
if os.path.isdir(static_dir):
    _assets_dir = os.path.join(static_dir, "assets")
    if os.path.isdir(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    """Catch-all for SPA: return index.html for non-API paths so client-side routing works."""
    if full_path.startswith("api/"):
        raise HTTPException(404)
    from fastapi.responses import FileResponse, HTMLResponse
    index = os.path.join(static_dir, "index.html")
    if os.path.isfile(index):
        return FileResponse(index)
    # No frontend build deployed — return a minimal page so / and other paths don't 404 with JSON
    return HTMLResponse(
        "<!DOCTYPE html><html><head><title>GovernBot</title></head><body style='font-family:system-ui;background:#0F172A;color:#E2E8F0;padding:2rem;'>"
        "<h1>GovernBot API</h1><p>The API is running. Use <strong>/api/config</strong>, <strong>/api/summary</strong>, "
        "<strong>/api/actions</strong>, <strong>/api/configs/workspaces</strong>, etc.</p>"
        "<p>To serve the React UI, build the frontend (<code>cd frontend && npm run build</code>) and deploy the <code>frontend/dist</code> folder with the app.</p>"
        "</body></html>",
        status_code=200,
    )
