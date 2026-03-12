#!/usr/bin/env python3
"""
GovernBot Test Data Generator

Creates real assets in a Databricks workspace using two service principals
(allowed + unapproved) to generate authentic audit log entries for testing
sync_approved_changes, watcher, and remediation workflows.

Usage:
  python test_data_gen/generate_test_data.py --no-cleanup --verbose
  python test_data_gen/generate_test_data.py --categories uc_data compute
  python test_data_gen/generate_test_data.py --cleanup-only
  python test_data_gen/generate_test_data.py --dry-run
"""
from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WORKSPACE_URL = "https://fe-sandbox-classic-sandbox-kj1pbc.cloud.databricks.com"
SECRET_SCOPE = "karthik_test_datagen"
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "govern_bot"
TRACKING_TABLE = "test_data_gen_operations"
ADMIN_PROFILE = "fe-sandbox-kj1pbc"

ALL_CATEGORIES = ["uc_data", "compute", "workspace", "ml_ai", "admin_security",
                  "workspace_extended", "uc_data_extended", "acl_extended", "admin_extended", "ml_ai_extended"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def get_secret(key: str) -> str:
    """Retrieve a secret via Databricks CLI (returns JSON with base64-encoded value)."""
    result = subprocess.run(
        ["databricks", "secrets", "get-secret", SECRET_SCOPE, key, "--profile", ADMIN_PROFILE],
        capture_output=True, text=True, check=True,
    )
    data = json.loads(result.stdout)
    return base64.b64decode(data["value"]).decode("utf-8")


_SENTINEL_FAIL = object()  # unique sentinel to distinguish None-success from failure


def safe_execute(desc: str, fn: Callable, *args, **kwargs) -> Optional[Any]:
    """Run fn(*args, **kwargs), return result or _SENTINEL_FAIL on failure. Prints status.
    Note: many SDK calls return None on success (e.g. permission updates, deletes).
    Use `succeeded(result)` to check if the call succeeded."""
    try:
        result = fn(*args, **kwargs)
        print(f"  OK  {desc}")
        return result
    except Exception as e:
        print(f"  FAIL {desc}: {e}")
        return _SENTINEL_FAIL


def succeeded(result: Any) -> bool:
    """Check if a safe_execute result indicates success (not the failure sentinel)."""
    return result is not _SENTINEL_FAIL


def run_sql(client: Any, warehouse_id: str, sql: str, max_wait_sec: int = 120) -> Any:
    """Execute SQL via Statement Execution API and return response."""
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    state = getattr(resp.status.state, "value", str(resp.status.state)) if resp.status else ""
    if state in ("FAILED", "CANCELED"):
        msg = getattr(resp.status, "error", None) or getattr(resp.status, "error_message", None) or state
        raise RuntimeError(f"SQL failed: {msg}")
    elapsed = 0
    while state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED") and elapsed < max_wait_sec:
        time.sleep(2)
        elapsed += 2
        resp = client.statement_execution.get_statement(resp.statement_id)
        state = getattr(resp.status.state, "value", str(resp.status.state)) if resp.status else ""
        if state in ("FAILED", "CANCELED"):
            msg = getattr(resp.status, "error", None) or getattr(resp.status, "error_message", None) or state
            raise RuntimeError(f"SQL failed: {msg}")
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL timed out after {max_wait_sec}s, state={state}")
    return resp


# ---------------------------------------------------------------------------
# Tracking Table
# ---------------------------------------------------------------------------
class TrackingTable:
    """Records every operation to a Delta table."""

    def __init__(self, admin_client: Any, warehouse_id: str, catalog: str, schema: str):
        self.client = admin_client
        self.warehouse_id = warehouse_id
        self.full_table = f"{catalog}.{schema}.{TRACKING_TABLE}"
        self.run_id = str(uuid.uuid4())[:8]
        self._ensure_table()

    def _ensure_table(self):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table} (
            operation_id STRING,
            timestamp TIMESTAMP,
            run_id STRING,
            sp_role STRING,
            sp_client_id STRING,
            asset_category STRING,
            asset_type STRING,
            operation STRING,
            asset_name STRING,
            asset_id STRING,
            status STRING,
            error_message STRING,
            expected_violation_type STRING,
            expected_remediation STRING
        ) USING DELTA
        """
        run_sql(self.client, self.warehouse_id, ddl)

    def record(
        self,
        sp_role: str,
        sp_client_id: str,
        asset_category: str,
        asset_type: str,
        operation: str,
        asset_name: str,
        asset_id: str = "",
        status: str = "SUCCESS",
        error_message: str = "",
        expected_violation_type: str = "NONE",
        expected_remediation: str = "NONE",
    ):
        op_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        # Escape single quotes
        for val in [asset_name, asset_id, error_message]:
            val = val.replace("'", "''") if val else ""
        asset_name_esc = asset_name.replace("'", "''") if asset_name else ""
        asset_id_esc = asset_id.replace("'", "''") if asset_id else ""
        error_esc = error_message.replace("'", "''") if error_message else ""

        sql = f"""
        INSERT INTO {self.full_table} VALUES (
            '{op_id}', '{now}', '{self.run_id}', '{sp_role}', '{sp_client_id}',
            '{asset_category}', '{asset_type}', '{operation}',
            '{asset_name_esc}', '{asset_id_esc}', '{status}', '{error_esc}',
            '{expected_violation_type}', '{expected_remediation}'
        )
        """
        try:
            run_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            print(f"  WARN: Failed to record tracking entry: {e}")


# ---------------------------------------------------------------------------
# Cleanup Registry
# ---------------------------------------------------------------------------
class CleanupRegistry:
    """Tracks resources for reverse-order cleanup."""

    def __init__(self):
        self._entries: list[tuple[str, Callable, str]] = []  # (desc, fn, asset_name)

    def register(self, desc: str, cleanup_fn: Callable, asset_name: str = ""):
        self._entries.append((desc, cleanup_fn, asset_name))

    def execute(self):
        print("\n--- Cleanup ---")
        for desc, fn, asset_name in reversed(self._entries):
            try:
                fn()
                print(f"  CLEANED: {desc} ({asset_name})")
            except Exception as e:
                print(f"  CLEAN-FAIL: {desc} ({asset_name}): {e}")


# ---------------------------------------------------------------------------
# Client creation
# ---------------------------------------------------------------------------
@dataclass
class Clients:
    allowed: Any = None
    unapproved: Any = None
    admin: Any = None
    allowed_client_id: str = ""
    unapproved_client_id: str = ""
    warehouse_id: str = ""


def create_clients(warehouse_id: str = "") -> Clients:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config

    print("Retrieving service principal credentials...")
    allowed_id = get_secret("allowed_sp_client_id")
    allowed_secret = get_secret("allowed_sp_secret")
    unapproved_id = get_secret("unallowed_sp_client_id")
    unapproved_secret = get_secret("unallowed_sp_secret")

    print("Creating workspace clients...")
    allowed_client = WorkspaceClient(
        host=WORKSPACE_URL,
        client_id=allowed_id,
        client_secret=allowed_secret,
    )
    unapproved_client = WorkspaceClient(
        host=WORKSPACE_URL,
        client_id=unapproved_id,
        client_secret=unapproved_secret,
    )
    admin_client = WorkspaceClient(config=Config(profile=ADMIN_PROFILE))

    # Discover warehouse
    if not warehouse_id:
        wh_list = list(admin_client.warehouses.list())
        if not wh_list:
            print("ERROR: No SQL warehouse found.", file=sys.stderr)
            sys.exit(1)
        warehouse_id = wh_list[0].id
        print(f"Using warehouse: {wh_list[0].name} ({warehouse_id})")

    return Clients(
        allowed=allowed_client,
        unapproved=unapproved_client,
        admin=admin_client,
        allowed_client_id=allowed_id,
        unapproved_client_id=unapproved_id,
        warehouse_id=warehouse_id,
    )


# ---------------------------------------------------------------------------
# Permission change helpers
# ---------------------------------------------------------------------------
def change_uc_permission(client: Any, securable_type: str, full_name: str, principal: str, privilege: str = "SELECT"):
    """Grant a UC privilege to trigger audit log permission change event."""
    from databricks.sdk.service.catalog import PermissionsChange, Privilege
    client.grants.update(
        securable_type=securable_type,
        full_name=full_name,
        changes=[PermissionsChange(add=[Privilege(privilege)], principal=principal)],
    )


def change_workspace_permission(client: Any, object_type: str, object_id: str, principal: str, level: str = "CAN_MANAGE"):
    """Change workspace object permission to trigger audit log ACL change event."""
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    client.permissions.update(
        request_object_type=object_type,
        request_object_id=object_id,
        access_control_list=[
            AccessControlRequest(group_name=principal, permission_level=PermissionLevel(level))
        ],
    )


# ---------------------------------------------------------------------------
# Asset Generators
# ---------------------------------------------------------------------------

def generate_uc_data(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate UC data assets: catalog, schema, table, volume, function, registered model, connection, share, recipient, provider."""
    from databricks.sdk.service.catalog import VolumeType

    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin
    wh = cat.warehouse_id

    catalog_name = f"gbot_test_{prefix}_catalog"
    schema_name = "test_schema"
    full_schema = f"{catalog_name}.{schema_name}"

    # ---- Phase 1: Allowed SP creates ----
    print("\n[uc_data] Phase 1: Allowed SP creates")

    # Catalog — SDK create (managed). NOTE: Some workspaces with Default Storage enabled
    # require UI-only catalog creation; this will fail in those workspaces.
    catalog_obj = safe_execute("Create catalog (SDK managed)", lambda: admin.catalogs.create(name=catalog_name, comment="GovernBot test catalog"))
    if succeeded(catalog_obj):
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "catalog", "CREATE", catalog_name, catalog_name, "SUCCESS", "", "NONE", "NONE")
        cleanup.register("Delete catalog", lambda: allowed.catalogs.delete(name=catalog_name, force=True), catalog_name)

        # Grant unapproved SP usage on catalog so it can create schemas/tables
        safe_execute("Grant unapproved SP catalog usage", lambda: change_uc_permission(admin, "catalog", catalog_name, cat.unapproved_client_id, "USE_CATALOG"))
        safe_execute("Grant unapproved SP catalog create schema", lambda: change_uc_permission(admin, "catalog", catalog_name, cat.unapproved_client_id, "CREATE_SCHEMA"))
        safe_execute("Grant allowed SP catalog usage", lambda: change_uc_permission(admin, "catalog", catalog_name, cat.allowed_client_id, "ALL_PRIVILEGES"))
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "catalog", "CREATE", catalog_name, "", "FAILED", "Creation failed", "NONE", "NONE")
        print("  SKIP: Cannot proceed with uc_data without catalog")
        return

    # Schema
    schema_obj = safe_execute("Create schema", lambda: allowed.schemas.create(name=schema_name, catalog_name=catalog_name))
    if schema_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "schema", "CREATE", full_schema, full_schema, "SUCCESS", "", "NONE", "NONE")
        # Grant unapproved SP usage on schema
        safe_execute("Grant unapproved SP schema usage", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "USE_SCHEMA"))
        safe_execute("Grant unapproved SP schema create table", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_TABLE"))
        safe_execute("Grant unapproved SP schema create volume", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_VOLUME"))
        safe_execute("Grant unapproved SP schema create function", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_FUNCTION"))
        safe_execute("Grant unapproved SP schema create model", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_MODEL"))
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "schema", "CREATE", full_schema, "", "FAILED", "Creation failed", "NONE", "NONE")
        return

    # Table (via SQL)
    table_name_allowed = f"gbot_test_{prefix}_table_allowed"
    full_table = f"{full_schema}.{table_name_allowed}"
    table_ok = safe_execute("Create table (SQL)", lambda: run_sql(admin, wh, f"CREATE TABLE {full_table} (id INT, name STRING) USING DELTA"))
    if table_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "table", "CREATE", full_table, full_table)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "table", "CREATE", full_table, "", "FAILED", "SQL create failed")

    # Volume
    vol_name_allowed = f"gbot_test_{prefix}_vol_allowed"
    full_vol = f"{full_schema}.{vol_name_allowed}"
    vol_obj = safe_execute("Create volume", lambda: allowed.volumes.create(
        catalog_name=catalog_name, schema_name=schema_name, name=vol_name_allowed, volume_type=VolumeType.MANAGED
    ))
    if vol_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "volume", "CREATE", full_vol, full_vol)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "volume", "CREATE", full_vol, "", "FAILED", "Creation failed")

    # Function (via SQL)
    func_name_allowed = f"gbot_test_{prefix}_func_allowed"
    full_func = f"{full_schema}.{func_name_allowed}"
    func_ok = safe_execute("Create function (SQL)", lambda: run_sql(admin, wh,
        f"CREATE FUNCTION {full_func}(x INT) RETURNS INT RETURN x + 1"))
    if func_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "function", "CREATE", full_func, full_func)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "function", "CREATE", full_func, "", "FAILED", "SQL create failed")

    # UC Registered Model
    model_name_allowed = f"gbot_test_{prefix}_model_allowed"
    full_model = f"{full_schema}.{model_name_allowed}"
    model_obj = safe_execute("Create UC registered model", lambda: allowed.registered_models.create(
        catalog_name=catalog_name, schema_name=schema_name, name=model_name_allowed
    ))
    if model_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "ucRegisteredModel", "CREATE", full_model, full_model)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "ucRegisteredModel", "CREATE", full_model, "", "FAILED", "Creation failed")

    # Connection (best-effort)
    conn_name_allowed = f"gbot_test_{prefix}_conn_allowed"
    conn_obj = safe_execute("Create connection", lambda: allowed.connections.create(
        name=conn_name_allowed, connection_type="MYSQL",
        options={"host": "localhost", "port": "3306", "user": "test", "password": "test"}
    ))
    if conn_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "connection", "CREATE", conn_name_allowed, conn_name_allowed)
        cleanup.register("Delete connection (allowed)", lambda: allowed.connections.delete(name=conn_name_allowed), conn_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "connection", "CREATE", conn_name_allowed, "", "FAILED", "Creation failed")

    # Share (best-effort)
    share_name_allowed = f"gbot_test_{prefix}_share_allowed"
    share_obj = safe_execute("Create share", lambda: allowed.shares.create(name=share_name_allowed))
    if share_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "share", "CREATE", share_name_allowed, share_name_allowed)
        cleanup.register("Delete share (allowed)", lambda: allowed.shares.delete(name=share_name_allowed), share_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "share", "CREATE", share_name_allowed, "", "FAILED", "Creation failed")

    # Recipient (best-effort)
    recip_name_allowed = f"gbot_test_{prefix}_recip_allowed"
    recip_obj = safe_execute("Create recipient", lambda: allowed.recipients.create(name=recip_name_allowed, authentication_type="TOKEN"))
    if recip_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "recipient", "CREATE", recip_name_allowed, recip_name_allowed)
        cleanup.register("Delete recipient (allowed)", lambda: allowed.recipients.delete(name=recip_name_allowed), recip_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "recipient", "CREATE", recip_name_allowed, "", "FAILED", "Creation failed")

    # Provider (best-effort)
    provider_name_allowed = f"gbot_test_{prefix}_prov_allowed"
    prov_obj = safe_execute("Create provider", lambda: allowed.providers.create(
        name=provider_name_allowed, authentication_type="TOKEN"
    ))
    if prov_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "provider", "CREATE", provider_name_allowed, provider_name_allowed)
        cleanup.register("Delete provider (allowed)", lambda: allowed.providers.delete(name=provider_name_allowed), provider_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "provider", "CREATE", provider_name_allowed, "", "FAILED", "Creation failed")

    # ---- Phase 2: Unapproved SP creates ----
    print("\n[uc_data] Phase 2: Unapproved SP creates")

    table_name_unapp = f"gbot_test_{prefix}_table_unapproved"
    full_table_unapp = f"{full_schema}.{table_name_unapp}"
    # Use admin client for SQL (SP may not have SQL warehouse access)
    table_unapp_ok = safe_execute("Unapproved create table (SQL)", lambda: run_sql(admin, wh,
        f"CREATE TABLE {full_table_unapp} (id INT, val STRING) USING DELTA"))
    if table_unapp_ok:
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "table", "CREATE", full_table_unapp, full_table_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "table", "CREATE", full_table_unapp, "", "FAILED", "SQL create failed")

    vol_name_unapp = f"gbot_test_{prefix}_vol_unapproved"
    full_vol_unapp = f"{full_schema}.{vol_name_unapp}"
    vol_unapp = safe_execute("Unapproved create volume", lambda: unapproved.volumes.create(
        catalog_name=catalog_name, schema_name=schema_name, name=vol_name_unapp, volume_type=VolumeType.MANAGED
    ))
    if vol_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "volume", "CREATE", full_vol_unapp, full_vol_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "volume", "CREATE", full_vol_unapp, "", "FAILED", "Creation failed")

    model_name_unapp = f"gbot_test_{prefix}_model_unapproved"
    full_model_unapp = f"{full_schema}.{model_name_unapp}"
    model_unapp = safe_execute("Unapproved create UC model", lambda: unapproved.registered_models.create(
        catalog_name=catalog_name, schema_name=schema_name, name=model_name_unapp
    ))
    if model_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "ucRegisteredModel", "CREATE", full_model_unapp, full_model_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "ucRegisteredModel", "CREATE", full_model_unapp, "", "FAILED", "Creation failed")

    # ---- Phase 3: Unapproved SP changes permissions ----
    print("\n[uc_data] Phase 3: Unapproved SP permission changes")

    if table_ok:
        perm_ok = safe_execute("Unapproved change table permission", lambda: change_uc_permission(
            unapproved, "table", full_table, "account users", "SELECT"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "table", "CHANGE_PERMISSION", full_table, full_table,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if vol_obj:
        perm_ok = safe_execute("Unapproved change volume permission", lambda: change_uc_permission(
            unapproved, "volume", full_vol, "account users", "READ_VOLUME"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "volume", "CHANGE_PERMISSION", full_vol, full_vol,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    # ---- Phase 4: Unapproved SP deletes own assets ----
    print("\n[uc_data] Phase 4: Unapproved SP deletes own assets")

    if table_unapp_ok:
        del_ok = safe_execute("Unapproved delete table", lambda: unapproved.tables.delete(full_name=full_table_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "table", "DELETE", full_table_unapp, full_table_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if vol_unapp:
        del_ok = safe_execute("Unapproved delete volume", lambda: unapproved.volumes.delete(full_name=full_vol_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "volume", "DELETE", full_vol_unapp, full_vol_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if model_unapp:
        del_ok = safe_execute("Unapproved delete UC model", lambda: unapproved.registered_models.delete(
            full_name=full_model_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data", "ucRegisteredModel", "DELETE", full_model_unapp, full_model_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    # ---- Phase 5: Allowed SP changes permissions ----
    print("\n[uc_data] Phase 5: Allowed SP permission changes")

    if table_ok:
        perm_ok = safe_execute("Allowed change table permission", lambda: change_uc_permission(
            allowed, "table", full_table, "account users", "MODIFY"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "table", "CHANGE_PERMISSION", full_table, full_table, status)

    if schema_obj:
        perm_ok = safe_execute("Allowed change schema permission", lambda: change_uc_permission(
            allowed, "schema", full_schema, "account users", "USE_SCHEMA"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "schema", "CHANGE_PERMISSION", full_schema, full_schema, status)

    # ---- Phase 6: Allowed SP deletes own assets ----
    print("\n[uc_data] Phase 6: Allowed SP deletes own assets")

    if func_ok:
        del_ok = safe_execute("Allowed delete function", lambda: allowed.functions.delete(name=full_func))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "function", "DELETE", full_func, full_func, status)

    if model_obj:
        del_ok = safe_execute("Allowed delete UC model", lambda: allowed.registered_models.delete(full_name=full_model))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "ucRegisteredModel", "DELETE", full_model, full_model, status)

    if table_ok:
        del_ok = safe_execute("Allowed delete table", lambda: allowed.tables.delete(full_name=full_table))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "table", "DELETE", full_table, full_table, status)

    if vol_obj:
        del_ok = safe_execute("Allowed delete volume", lambda: allowed.volumes.delete(full_name=full_vol))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "volume", "DELETE", full_vol, full_vol, status)

    if schema_obj:
        del_ok = safe_execute("Allowed delete schema", lambda: allowed.schemas.delete(full_name=full_schema, force=True))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "schema", "DELETE", full_schema, full_schema, status)

    # Catalog delete — explicitly tracked (also registered in CleanupRegistry as fallback)
    if succeeded(catalog_obj):
        del_ok = safe_execute("Allowed delete catalog", lambda: allowed.catalogs.delete(name=catalog_name, force=True))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data", "catalog", "DELETE", catalog_name, catalog_name, status)


def generate_compute(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate compute assets: cluster, clusterPolicy, instancePool, warehouse."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    perm_group = "users"  # workspace group for permission changes

    # ---- Phase 1: Allowed SP creates ----
    print("\n[compute] Phase 1: Allowed SP creates")

    # Cluster
    cluster_name_allowed = f"gbot_test_{prefix}_cluster_allowed"
    cluster_obj = safe_execute("Create cluster", lambda: allowed.clusters.create(
        cluster_name=cluster_name_allowed,
        spark_version="15.4.x-scala2.12",
        num_workers=0,
        autotermination_minutes=10,
        node_type_id="i3.xlarge",
    ).result())
    cluster_id_allowed = getattr(cluster_obj, "cluster_id", None) if cluster_obj else None
    if cluster_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "compute", "cluster", "CREATE", cluster_name_allowed, cluster_id_allowed)
        cleanup.register("Delete cluster (allowed)", lambda: allowed.clusters.permanent_delete(cluster_id=cluster_id_allowed), cluster_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "compute", "cluster", "CREATE", cluster_name_allowed, "", "FAILED", "Creation failed")

    # Cluster Policy
    policy_name_allowed = f"gbot_test_{prefix}_policy_allowed"
    policy_def = json.dumps({"spark_version": {"type": "fixed", "value": "15.4.x-scala2.12"}})
    policy_obj = safe_execute("Create cluster policy", lambda: allowed.cluster_policies.create(
        name=policy_name_allowed, definition=policy_def
    ))
    policy_id_allowed = getattr(policy_obj, "policy_id", None) if policy_obj else None
    if policy_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "compute", "clusterPolicy", "CREATE", policy_name_allowed, policy_id_allowed)
        cleanup.register("Delete policy (allowed)", lambda: allowed.cluster_policies.delete(policy_id=policy_id_allowed), policy_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "compute", "clusterPolicy", "CREATE", policy_name_allowed, "", "FAILED", "Creation failed")

    # Instance Pool
    pool_name_allowed = f"gbot_test_{prefix}_pool_allowed"
    pool_obj = safe_execute("Create instance pool", lambda: allowed.instance_pools.create(
        instance_pool_name=pool_name_allowed,
        min_idle_instances=0,
        node_type_id="i3.xlarge",
    ))
    pool_id_allowed = getattr(pool_obj, "instance_pool_id", None) if pool_obj else None
    if pool_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "compute", "instancePool", "CREATE", pool_name_allowed, pool_id_allowed)
        cleanup.register("Delete pool (allowed)", lambda: allowed.instance_pools.delete(instance_pool_id=pool_id_allowed), pool_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "compute", "instancePool", "CREATE", pool_name_allowed, "", "FAILED", "Creation failed")

    # Warehouse
    wh_name_allowed = f"gbot_test_{prefix}_wh_allowed"
    wh_obj = safe_execute("Create warehouse", lambda: allowed.warehouses.create(
        name=wh_name_allowed, cluster_size="2X-Small", auto_stop_mins=10, max_num_clusters=1,
    ).result())
    wh_id_allowed = getattr(wh_obj, "id", None) if wh_obj else None
    if wh_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "compute", "warehouse", "CREATE", wh_name_allowed, wh_id_allowed)
        cleanup.register("Delete warehouse (allowed)", lambda: allowed.warehouses.delete(id=wh_id_allowed), wh_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "compute", "warehouse", "CREATE", wh_name_allowed, "", "FAILED", "Creation failed")

    # ---- Phase 2: Unapproved SP creates ----
    print("\n[compute] Phase 2: Unapproved SP creates")

    cluster_name_unapp = f"gbot_test_{prefix}_cluster_unapproved"
    cluster_unapp = safe_execute("Unapproved create cluster", lambda: unapproved.clusters.create(
        cluster_name=cluster_name_unapp,
        spark_version="15.4.x-scala2.12",
        num_workers=0,
        autotermination_minutes=10,
        node_type_id="i3.xlarge",
    ).result())
    cluster_id_unapp = getattr(cluster_unapp, "cluster_id", None) if cluster_unapp else None
    if cluster_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "cluster", "CREATE", cluster_name_unapp, cluster_id_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete cluster (unapproved)", lambda: unapproved.clusters.permanent_delete(cluster_id=cluster_id_unapp), cluster_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "cluster", "CREATE", cluster_name_unapp, "", "FAILED", "Creation failed")

    policy_name_unapp = f"gbot_test_{prefix}_policy_unapproved"
    policy_unapp = safe_execute("Unapproved create policy", lambda: unapproved.cluster_policies.create(
        name=policy_name_unapp, definition=policy_def
    ))
    policy_id_unapp = getattr(policy_unapp, "policy_id", None) if policy_unapp else None
    if policy_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "clusterPolicy", "CREATE", policy_name_unapp, policy_id_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete policy (unapproved)", lambda: unapproved.cluster_policies.delete(policy_id=policy_id_unapp), policy_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "clusterPolicy", "CREATE", policy_name_unapp, "", "FAILED", "Creation failed")

    pool_name_unapp = f"gbot_test_{prefix}_pool_unapproved"
    pool_unapp = safe_execute("Unapproved create pool", lambda: unapproved.instance_pools.create(
        instance_pool_name=pool_name_unapp, min_idle_instances=0, node_type_id="i3.xlarge",
    ))
    pool_id_unapp = getattr(pool_unapp, "instance_pool_id", None) if pool_unapp else None
    if pool_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "instancePool", "CREATE", pool_name_unapp, pool_id_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete pool (unapproved)", lambda: unapproved.instance_pools.delete(instance_pool_id=pool_id_unapp), pool_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "instancePool", "CREATE", pool_name_unapp, "", "FAILED", "Creation failed")

    # ---- Phase 3: Unapproved SP changes permissions ----
    print("\n[compute] Phase 3: Unapproved SP permission changes")

    if cluster_id_allowed:
        perm_ok = safe_execute("Unapproved change cluster permission", lambda: change_workspace_permission(
            unapproved, "clusters", cluster_id_allowed, perm_group, "CAN_RESTART"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "cluster", "CHANGE_PERMISSION", cluster_name_allowed, cluster_id_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if policy_id_allowed:
        perm_ok = safe_execute("Unapproved change policy permission", lambda: change_workspace_permission(
            unapproved, "cluster-policies", policy_id_allowed, perm_group, "CAN_USE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "clusterPolicy", "CHANGE_PERMISSION", policy_name_allowed, policy_id_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if pool_id_allowed:
        perm_ok = safe_execute("Unapproved change pool permission", lambda: change_workspace_permission(
            unapproved, "instance-pools", pool_id_allowed, perm_group, "CAN_ATTACH_TO"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "instancePool", "CHANGE_PERMISSION", pool_name_allowed, pool_id_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    # ---- Phase 4: Unapproved SP deletes own assets ----
    print("\n[compute] Phase 4: Unapproved SP deletes own assets")

    if cluster_id_unapp:
        del_ok = safe_execute("Unapproved delete cluster", lambda: unapproved.clusters.permanent_delete(cluster_id=cluster_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "cluster", "DELETE", cluster_name_unapp, cluster_id_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if policy_id_unapp:
        del_ok = safe_execute("Unapproved delete policy", lambda: unapproved.cluster_policies.delete(policy_id=policy_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "clusterPolicy", "DELETE", policy_name_unapp, policy_id_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if pool_id_unapp:
        del_ok = safe_execute("Unapproved delete pool", lambda: unapproved.instance_pools.delete(instance_pool_id=pool_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "compute", "instancePool", "DELETE", pool_name_unapp, pool_id_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    # ---- Phase 5: Allowed SP changes permissions ----
    print("\n[compute] Phase 5: Allowed SP permission changes")

    if cluster_id_allowed:
        perm_ok = safe_execute("Allowed change cluster permission", lambda: change_workspace_permission(
            allowed, "clusters", cluster_id_allowed, perm_group, "CAN_MANAGE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "compute", "cluster", "CHANGE_PERMISSION", cluster_name_allowed, cluster_id_allowed, status)

    if policy_id_allowed:
        perm_ok = safe_execute("Allowed change policy permission", lambda: change_workspace_permission(
            allowed, "cluster-policies", policy_id_allowed, perm_group, "CAN_USE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "compute", "clusterPolicy", "CHANGE_PERMISSION", policy_name_allowed, policy_id_allowed, status)

    # ---- Phase 6: Allowed SP deletes own assets ----
    print("\n[compute] Phase 6: Allowed SP deletes own assets")

    if wh_id_allowed:
        del_ok = safe_execute("Allowed delete warehouse", lambda: allowed.warehouses.delete(id=wh_id_allowed))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "compute", "warehouse", "DELETE", wh_name_allowed, wh_id_allowed, status)


def generate_workspace(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate workspace assets: notebook, dashboard, jobs, pipelines, mlflowExperiments, query, alert, secretScope."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin
    perm_group = "users"

    # ---- Phase 1: Allowed SP creates ----
    print("\n[workspace] Phase 1: Allowed SP creates")

    # Notebook
    nb_path_allowed = f"/Shared/gbot_test_{prefix}_notebook_allowed"
    nb_content = base64.b64encode(b"# Test notebook for GovernBot\nprint('hello')").decode()
    from databricks.sdk.service.workspace import ImportFormat, Language
    nb_obj = safe_execute("Create notebook", lambda: allowed.workspace.import_(
        path=nb_path_allowed, content=nb_content, format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True
    ))
    if succeeded(nb_obj):
        tracker.record("allowed", cat.allowed_client_id, "workspace", "notebook", "CREATE", nb_path_allowed, nb_path_allowed)
        cleanup.register("Delete notebook (allowed)", lambda: allowed.workspace.delete(path=nb_path_allowed), nb_path_allowed)
        # Get notebook object ID for permission changes
        nb_status = safe_execute("Get notebook status", lambda: allowed.workspace.get_status(path=nb_path_allowed))
        nb_obj_id = str(getattr(nb_status, "object_id", "")) if nb_status else ""
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "notebook", "CREATE", nb_path_allowed, "", "FAILED", "Creation failed")
        nb_obj_id = ""

    # Dashboard (Lakeview)
    dash_name_allowed = f"gbot_test_{prefix}_dash_allowed"
    from databricks.sdk.service.dashboards import Dashboard
    dash_obj = safe_execute("Create dashboard", lambda: allowed.lakeview.create(
        dashboard=Dashboard(display_name=dash_name_allowed)
    ))
    dash_id_allowed = getattr(dash_obj, "dashboard_id", None) if dash_obj else None
    if dash_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "dashboard", "CREATE", dash_name_allowed, dash_id_allowed)
        cleanup.register("Trash dashboard (allowed)", lambda: allowed.lakeview.trash(dash_id_allowed), dash_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "dashboard", "CREATE", dash_name_allowed, "", "FAILED", "Creation failed")

    # Job
    job_name_allowed = f"gbot_test_{prefix}_job_allowed"
    from databricks.sdk.service.jobs import Task, NotebookTask
    job_obj = safe_execute("Create job", lambda: allowed.jobs.create(
        name=job_name_allowed,
        tasks=[Task(task_key="test_task", notebook_task=NotebookTask(notebook_path="/Shared/fake_notebook"))]
    ))
    job_id_allowed = getattr(job_obj, "job_id", None) if job_obj else None
    if job_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "jobs", "CREATE", job_name_allowed, str(job_id_allowed))
        cleanup.register("Delete job (allowed)", lambda: allowed.jobs.delete(job_id=job_id_allowed), job_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "jobs", "CREATE", job_name_allowed, "", "FAILED", "Creation failed")

    # Pipeline
    pipe_name_allowed = f"gbot_test_{prefix}_pipe_allowed"
    from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
    pipe_obj = safe_execute("Create pipeline", lambda: allowed.pipelines.create(
        name=pipe_name_allowed, continuous=False,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path="/Shared/fake_pipeline_notebook"))],
    ))
    pipe_id_allowed = getattr(pipe_obj, "pipeline_id", None) if pipe_obj else None
    if pipe_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "pipelines", "CREATE", pipe_name_allowed, pipe_id_allowed)
        cleanup.register("Delete pipeline (allowed)", lambda: allowed.pipelines.delete(pipeline_id=pipe_id_allowed), pipe_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "pipelines", "CREATE", pipe_name_allowed, "", "FAILED", "Creation failed")

    # MLflow Experiment
    exp_name_allowed = f"/Shared/gbot_test_{prefix}_exp_allowed"
    exp_obj = safe_execute("Create experiment", lambda: allowed.experiments.create_experiment(name=exp_name_allowed))
    exp_id_allowed = getattr(exp_obj, "experiment_id", None) if exp_obj else None
    if exp_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "mlflowExperiments", "CREATE", exp_name_allowed, exp_id_allowed)
        cleanup.register("Delete experiment (allowed)", lambda: allowed.experiments.delete_experiment(experiment_id=exp_id_allowed), exp_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "mlflowExperiments", "CREATE", exp_name_allowed, "", "FAILED", "Creation failed")

    # Secret Scope
    scope_name_allowed = f"gbot_test_{prefix}_scope_allowed"
    scope_ok = safe_execute("Create secret scope", lambda: allowed.secrets.create_scope(scope=scope_name_allowed))
    if succeeded(scope_ok):
        tracker.record("allowed", cat.allowed_client_id, "workspace", "secretScope", "CREATE", scope_name_allowed, scope_name_allowed)
        cleanup.register("Delete scope (allowed)", lambda: allowed.secrets.delete_scope(scope=scope_name_allowed), scope_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "secretScope", "CREATE", scope_name_allowed, "", "FAILED", "Creation failed")

    # Query (best-effort)
    query_name_allowed = f"gbot_test_{prefix}_query_allowed"
    from databricks.sdk.service.sql import CreateQueryRequestQuery
    query_obj = safe_execute("Create query", lambda: allowed.queries.create(
        query=CreateQueryRequestQuery(
            query_text="SELECT 1 AS test", display_name=query_name_allowed, warehouse_id=cat.warehouse_id
        )
    ))
    query_id_allowed = getattr(query_obj, "id", None) if query_obj else None
    if query_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "query", "CREATE", query_name_allowed, query_id_allowed)
        cleanup.register("Delete query (allowed)", lambda: allowed.queries.delete(id=query_id_allowed), query_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace", "query", "CREATE", query_name_allowed, "", "FAILED", "Creation failed")

    # ---- Phase 2: Unapproved SP creates ----
    print("\n[workspace] Phase 2: Unapproved SP creates")

    nb_path_unapp = f"/Shared/gbot_test_{prefix}_notebook_unapproved"
    nb_unapp = safe_execute("Unapproved create notebook", lambda: unapproved.workspace.import_(
        path=nb_path_unapp, content=nb_content, format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True
    ))
    if succeeded(nb_unapp):
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "notebook", "CREATE", nb_path_unapp, nb_path_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete notebook (unapproved)", lambda: unapproved.workspace.delete(path=nb_path_unapp), nb_path_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "notebook", "CREATE", nb_path_unapp, "", "FAILED", "Creation failed")

    job_name_unapp = f"gbot_test_{prefix}_job_unapproved"
    job_unapp = safe_execute("Unapproved create job", lambda: unapproved.jobs.create(
        name=job_name_unapp,
        tasks=[Task(task_key="test_task", notebook_task=NotebookTask(notebook_path="/Shared/fake_notebook"))]
    ))
    job_id_unapp = getattr(job_unapp, "job_id", None) if job_unapp else None
    if job_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "jobs", "CREATE", job_name_unapp, str(job_id_unapp),
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete job (unapproved)", lambda: unapproved.jobs.delete(job_id=job_id_unapp), job_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "jobs", "CREATE", job_name_unapp, "", "FAILED", "Creation failed")

    pipe_name_unapp = f"gbot_test_{prefix}_pipe_unapproved"
    pipe_unapp = safe_execute("Unapproved create pipeline", lambda: unapproved.pipelines.create(
        name=pipe_name_unapp, continuous=False,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path="/Shared/fake_pipeline_notebook"))],
    ))
    pipe_id_unapp = getattr(pipe_unapp, "pipeline_id", None) if pipe_unapp else None
    if pipe_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "pipelines", "CREATE", pipe_name_unapp, pipe_id_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete pipeline (unapproved)", lambda: unapproved.pipelines.delete(pipeline_id=pipe_id_unapp), pipe_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "pipelines", "CREATE", pipe_name_unapp, "", "FAILED", "Creation failed")

    exp_name_unapp = f"/Shared/gbot_test_{prefix}_exp_unapproved"
    exp_unapp = safe_execute("Unapproved create experiment", lambda: unapproved.experiments.create_experiment(name=exp_name_unapp))
    exp_id_unapp = getattr(exp_unapp, "experiment_id", None) if exp_unapp else None
    if exp_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "mlflowExperiments", "CREATE", exp_name_unapp, exp_id_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete experiment (unapproved)", lambda: unapproved.experiments.delete_experiment(experiment_id=exp_id_unapp), exp_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "mlflowExperiments", "CREATE", exp_name_unapp, "", "FAILED", "Creation failed")

    scope_name_unapp = f"gbot_test_{prefix}_scope_unapproved"
    scope_unapp = safe_execute("Unapproved create scope", lambda: unapproved.secrets.create_scope(scope=scope_name_unapp))
    if succeeded(scope_unapp):
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "secretScope", "CREATE", scope_name_unapp, scope_name_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete scope (unapproved)", lambda: unapproved.secrets.delete_scope(scope=scope_name_unapp), scope_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "secretScope", "CREATE", scope_name_unapp, "", "FAILED", "Creation failed")

    # ---- Phase 3: Unapproved SP changes permissions ----
    print("\n[workspace] Phase 3: Unapproved SP permission changes")

    if nb_obj_id:
        perm_ok = safe_execute("Unapproved change notebook permission", lambda: change_workspace_permission(
            unapproved, "notebooks", nb_obj_id, perm_group, "CAN_RUN"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "notebook", "CHANGE_PERMISSION", nb_path_allowed, nb_obj_id,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if job_id_allowed:
        perm_ok = safe_execute("Unapproved change job permission", lambda: change_workspace_permission(
            unapproved, "jobs", str(job_id_allowed), perm_group, "CAN_VIEW"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "jobs", "CHANGE_PERMISSION", job_name_allowed, str(job_id_allowed),
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if pipe_id_allowed:
        perm_ok = safe_execute("Unapproved change pipeline permission", lambda: change_workspace_permission(
            unapproved, "pipelines", pipe_id_allowed, perm_group, "CAN_VIEW"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "pipelines", "CHANGE_PERMISSION", pipe_name_allowed, pipe_id_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if exp_id_allowed:
        perm_ok = safe_execute("Unapproved change experiment permission", lambda: change_workspace_permission(
            unapproved, "experiments", exp_id_allowed, perm_group, "CAN_READ"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "mlflowExperiments", "CHANGE_PERMISSION", exp_name_allowed, exp_id_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    if succeeded(scope_ok):
        from databricks.sdk.service.workspace import AclPermission
        perm_ok = safe_execute("Unapproved change scope permission", lambda: unapproved.secrets.put_acl(
            scope=scope_name_allowed, principal=perm_group, permission=AclPermission.READ))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "secretScope", "CHANGE_PERMISSION", scope_name_allowed, scope_name_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    # ---- Phase 4: Unapproved SP deletes own assets ----
    print("\n[workspace] Phase 4: Unapproved SP deletes own assets")

    if succeeded(nb_unapp):
        del_ok = safe_execute("Unapproved delete notebook", lambda: unapproved.workspace.delete(path=nb_path_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "notebook", "DELETE", nb_path_unapp, nb_path_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if job_id_unapp:
        del_ok = safe_execute("Unapproved delete job", lambda: unapproved.jobs.delete(job_id=job_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "jobs", "DELETE", job_name_unapp, str(job_id_unapp),
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if pipe_id_unapp:
        del_ok = safe_execute("Unapproved delete pipeline", lambda: unapproved.pipelines.delete(pipeline_id=pipe_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "pipelines", "DELETE", pipe_name_unapp, pipe_id_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if exp_id_unapp:
        del_ok = safe_execute("Unapproved delete experiment", lambda: unapproved.experiments.delete_experiment(experiment_id=exp_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "mlflowExperiments", "DELETE", exp_name_unapp, exp_id_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    if succeeded(scope_unapp):
        del_ok = safe_execute("Unapproved delete scope", lambda: unapproved.secrets.delete_scope(scope=scope_name_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace", "secretScope", "DELETE", scope_name_unapp, scope_name_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    # ---- Phase 5: Allowed SP changes permissions ----
    print("\n[workspace] Phase 5: Allowed SP permission changes")

    if job_id_allowed:
        perm_ok = safe_execute("Allowed change job permission", lambda: change_workspace_permission(
            allowed, "jobs", str(job_id_allowed), perm_group, "CAN_MANAGE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "workspace", "jobs", "CHANGE_PERMISSION", job_name_allowed, str(job_id_allowed), status)

    if pipe_id_allowed:
        perm_ok = safe_execute("Allowed change pipeline permission", lambda: change_workspace_permission(
            allowed, "pipelines", pipe_id_allowed, perm_group, "CAN_MANAGE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "workspace", "pipelines", "CHANGE_PERMISSION", pipe_name_allowed, pipe_id_allowed, status)

    # ---- Phase 6: Allowed SP deletes own assets ----
    print("\n[workspace] Phase 6: Allowed SP deletes own assets")

    if dash_id_allowed:
        del_ok = safe_execute("Allowed trash dashboard", lambda: allowed.lakeview.trash(dash_id_allowed))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "workspace", "dashboard", "DELETE", dash_name_allowed, dash_id_allowed, status)

    if query_id_allowed:
        del_ok = safe_execute("Allowed delete query", lambda: allowed.queries.delete(id=query_id_allowed))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "workspace", "query", "DELETE", query_name_allowed, query_id_allowed, status)


def generate_ml_ai(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate ML/AI assets: servingEndpoint, registeredModels (workspace), vectorSearchEndpoint, monitors."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    perm_group = "users"

    # ---- Phase 1: Allowed SP creates ----
    print("\n[ml_ai] Phase 1: Allowed SP creates")

    # Serving Endpoint
    ep_name_allowed = f"gbot_test_{prefix}_ep_allowed"
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
    ep_obj = safe_execute("Create serving endpoint", lambda: allowed.serving_endpoints.create(
        name=ep_name_allowed,
        config=EndpointCoreConfigInput(
            name=ep_name_allowed,
            served_entities=[ServedEntityInput(
                entity_name="databricks-meta-llama-3-1-8b-instruct",
                entity_version="1",
                workload_size="Small",
                scale_to_zero_enabled=True,
            )]
        ),
    ))
    ep_created = succeeded(ep_obj)
    if ep_created:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "servingEndpoint", "CREATE", ep_name_allowed, ep_name_allowed)
        cleanup.register("Delete serving endpoint (allowed)", lambda: allowed.serving_endpoints.delete(name=ep_name_allowed), ep_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "servingEndpoint", "CREATE", ep_name_allowed, "", "FAILED", "Creation failed")

    # Workspace Registered Model (legacy MLflow)
    model_name_allowed = f"gbot_test_{prefix}_wsmodel_allowed"
    model_obj = safe_execute("Create workspace model", lambda: allowed.model_registry.create_model(name=model_name_allowed))
    model_created = succeeded(model_obj)
    if model_created:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "registeredModel", "CREATE", model_name_allowed, model_name_allowed)
        cleanup.register("Delete workspace model (allowed)", lambda: allowed.model_registry.delete_model(name=model_name_allowed), model_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "registeredModel", "CREATE", model_name_allowed, "", "FAILED", "Creation failed (may be disabled)")

    # Vector Search Endpoint (best-effort)
    vs_name_allowed = f"gbot_test_{prefix}_vs_allowed"
    from databricks.sdk.service.vectorsearch import EndpointType
    vs_obj = safe_execute("Create vector search endpoint", lambda: allowed.vector_search_endpoints.create_endpoint(
        name=vs_name_allowed, endpoint_type=EndpointType.STANDARD
    ))
    vs_created = succeeded(vs_obj)
    if vs_created:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "vectorSearchEndpoint", "CREATE", vs_name_allowed, vs_name_allowed)
        cleanup.register("Delete vector search endpoint (allowed)", lambda: allowed.vector_search_endpoints.delete_endpoint(endpoint_name=vs_name_allowed), vs_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "vectorSearchEndpoint", "CREATE", vs_name_allowed, "", "FAILED", "Creation failed")

    # ---- Phase 2: Unapproved SP creates ----
    print("\n[ml_ai] Phase 2: Unapproved SP creates")

    ep_name_unapp = f"gbot_test_{prefix}_ep_unapproved"
    ep_unapp = safe_execute("Unapproved create serving endpoint", lambda: unapproved.serving_endpoints.create(
        name=ep_name_unapp,
        config=EndpointCoreConfigInput(
            name=ep_name_unapp,
            served_entities=[ServedEntityInput(
                entity_name="databricks-meta-llama-3-1-8b-instruct",
                entity_version="1",
                workload_size="Small",
                scale_to_zero_enabled=True,
            )]
        ),
    ))
    ep_unapp_created = succeeded(ep_unapp)
    if ep_unapp_created:
        tracker.record("unapproved", cat.unapproved_client_id, "ml_ai", "servingEndpoint", "CREATE", ep_name_unapp, ep_name_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete serving endpoint (unapproved)", lambda: unapproved.serving_endpoints.delete(name=ep_name_unapp), ep_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "ml_ai", "servingEndpoint", "CREATE", ep_name_unapp, "", "FAILED", "Creation failed")

    # ---- Phase 3: Unapproved SP changes permissions ----
    print("\n[ml_ai] Phase 3: Unapproved SP permission changes")

    if ep_created:
        perm_ok = safe_execute("Unapproved change endpoint permission", lambda: change_workspace_permission(
            unapproved, "serving-endpoints", ep_name_allowed, perm_group, "CAN_QUERY"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "ml_ai", "servingEndpoint", "CHANGE_PERMISSION", ep_name_allowed, ep_name_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    # ---- Phase 4: Unapproved SP deletes own assets ----
    print("\n[ml_ai] Phase 4: Unapproved SP deletes own assets")

    if ep_unapp_created:
        del_ok = safe_execute("Unapproved delete endpoint", lambda: unapproved.serving_endpoints.delete(name=ep_name_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "ml_ai", "servingEndpoint", "DELETE", ep_name_unapp, ep_name_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    # ---- Phase 5: Allowed SP changes permissions ----
    print("\n[ml_ai] Phase 5: Allowed SP permission changes")

    if ep_created:
        perm_ok = safe_execute("Allowed change endpoint permission", lambda: change_workspace_permission(
            allowed, "serving-endpoints", ep_name_allowed, perm_group, "CAN_MANAGE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "servingEndpoint", "CHANGE_PERMISSION", ep_name_allowed, ep_name_allowed, status)

    # ---- Phase 6: Allowed SP deletes own assets ----
    print("\n[ml_ai] Phase 6: Allowed SP deletes own assets")

    if vs_created:
        del_ok = safe_execute("Allowed delete vector search endpoint", lambda: allowed.vector_search_endpoints.delete_endpoint(endpoint_name=vs_name_allowed))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "vectorSearchEndpoint", "DELETE", vs_name_allowed, vs_name_allowed, status)

    if model_created:
        del_ok = safe_execute("Allowed delete workspace model", lambda: allowed.model_registry.delete_model(name=model_name_allowed))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "ml_ai", "registeredModel", "DELETE", model_name_allowed, model_name_allowed, status)


def generate_admin_security(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate admin/security assets: groups (best-effort). Other admin assets are report-only."""
    cat = clients
    allowed = cat.allowed

    print("\n[admin_security] Phase 1: Allowed SP creates (best-effort)")

    # Groups
    group_name = f"gbot_test_{prefix}_group_allowed"
    group_obj = safe_execute("Create group", lambda: allowed.groups.create(display_name=group_name))
    group_id = getattr(group_obj, "id", None) if group_obj else None
    if group_id:
        tracker.record("allowed", cat.allowed_client_id, "admin_security", "groups", "CREATE", group_name, group_id)
        cleanup.register("Delete group (allowed)", lambda: allowed.groups.delete(id=group_id), group_name)
    else:
        tracker.record("allowed", cat.allowed_client_id, "admin_security", "groups", "CREATE", group_name, "", "FAILED",
                       "Group creation failed (may need admin privileges)")

    # Report-only entries for assets we can't create via SP
    for asset_type, reason in [
        ("storageCredential", "Requires cloud IAM role ARN"),
        ("externalLocation", "Requires storage credential + cloud URL"),
        ("metastores", "Account-level operation"),
        ("users", "Account/admin API only"),
        ("servicePrincipals", "Account/admin API only"),
        ("tokensAcls", "Admin entitlement only"),
    ]:
        tracker.record("allowed", cat.allowed_client_id, "admin_security", asset_type, "CREATE",
                       f"SKIPPED_{asset_type}", "", "SKIPPED", reason)


# ---------------------------------------------------------------------------
# Extended generators for missing asset types
# ---------------------------------------------------------------------------

def generate_workspace_extended(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate missing workspace assets: apps, alerts, genie spaces, files, folders, secrets."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin
    wh = cat.warehouse_id

    # ---- Apps ----
    print("\n[workspace_extended] Apps")

    from databricks.sdk.service.apps import App
    app_name_allowed = f"gbot-test-{prefix.replace('_', '-')}-appok"[:30]
    app_obj = safe_execute("Create app (allowed)", lambda: allowed.apps.create(
        app=App(name=app_name_allowed, description="GovernBot test app")
    ))
    if app_obj:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "apps", "CREATE", app_name_allowed, app_name_allowed)
        cleanup.register("Delete app (allowed)", lambda: admin.apps.delete(name=app_name_allowed), app_name_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "apps", "CREATE", app_name_allowed, "", "FAILED", "Creation failed")

    app_name_unapp = f"gbot-test-{prefix.replace('_', '-')}-appun"[:30]
    app_unapp = safe_execute("Create app (unapproved)", lambda: unapproved.apps.create(
        app=App(name=app_name_unapp, description="GovernBot test app unapproved")
    ))
    if app_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "apps", "CREATE", app_name_unapp, app_name_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        cleanup.register("Delete app (unapproved)", lambda: admin.apps.delete(name=app_name_unapp), app_name_unapp)
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "apps", "CREATE", app_name_unapp, "", "FAILED", "Creation failed")

    # App ACL change
    if app_obj:
        perm_ok = safe_execute("Unapproved change app ACL", lambda: change_workspace_permission(
            unapproved, "apps", getattr(app_obj, "name", app_name_allowed), "users", "CAN_USE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "apps", "CHANGE_PERMISSION", app_name_allowed, app_name_allowed,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    # App delete (unapproved)
    if app_unapp:
        del_ok = safe_execute("Unapproved delete app", lambda: unapproved.apps.delete(name=app_name_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "apps", "DELETE", app_name_unapp, app_name_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")

    # ---- Alerts ----
    print("\n[workspace_extended] Alerts")

    from databricks.sdk.service.sql import (CreateAlertRequestAlert, AlertCondition, AlertConditionOperand,
                                            AlertConditionThreshold, AlertOperandColumn, AlertOperandValue,
                                            AlertOperator, CreateQueryRequestQuery)
    alert_name_allowed = f"gbot_test_{prefix}_alert_allowed"
    # Need a query first for the alert
    alert_query_name = f"gbot_test_{prefix}_alert_query"
    alert_query = safe_execute("Create query for alert", lambda: allowed.queries.create(
        query=CreateQueryRequestQuery(
            query_text="SELECT 1 AS value", display_name=alert_query_name, warehouse_id=wh
        )
    ))
    alert_query_id = getattr(alert_query, "id", None) if alert_query else None

    alert_condition = AlertCondition(
        op=AlertOperator.GREATER_THAN,
        operand=AlertConditionOperand(column=AlertOperandColumn(name="value")),
        threshold=AlertConditionThreshold(value=AlertOperandValue(double_value=100)),
    )

    if alert_query_id:
        cleanup.register("Delete alert query", lambda: allowed.queries.delete(id=alert_query_id), alert_query_name)
        alert_obj = safe_execute("Create alert (allowed)", lambda: allowed.alerts.create(
            alert=CreateAlertRequestAlert(
                condition=alert_condition,
                display_name=alert_name_allowed,
                query_id=alert_query_id,
            )
        ))
        alert_id_allowed = getattr(alert_obj, "id", None) if alert_obj else None
        if alert_id_allowed:
            tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "alert", "CREATE", alert_name_allowed, alert_id_allowed)
            cleanup.register("Trash alert (allowed)", lambda: allowed.alerts.delete(id=alert_id_allowed), alert_name_allowed)
        else:
            tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "alert", "CREATE", alert_name_allowed, "", "FAILED", "Creation failed")

        # Unapproved alert
        alert_name_unapp = f"gbot_test_{prefix}_alert_unapproved"
        alert_unapp = safe_execute("Create alert (unapproved)", lambda: unapproved.alerts.create(
            alert=CreateAlertRequestAlert(
                condition=alert_condition,
                display_name=alert_name_unapp,
                query_id=alert_query_id,
            )
        ))
        alert_id_unapp = getattr(alert_unapp, "id", None) if alert_unapp else None
        if alert_id_unapp:
            tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "alert", "CREATE", alert_name_unapp, alert_id_unapp,
                           "SUCCESS", "", "UNAPPROVED_CREATION", "SKIP_REMEDIATION")
            # Trash unapproved alert
            del_ok = safe_execute("Unapproved trash alert", lambda: unapproved.alerts.delete(id=alert_id_unapp))
            status = "SUCCESS" if succeeded(del_ok) else "FAILED"
            tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "alert", "DELETE", alert_name_unapp, alert_id_unapp,
                           status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")
        else:
            tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "alert", "CREATE", alert_name_unapp, "", "FAILED", "Creation failed")
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "alert", "CREATE", alert_name_allowed, "", "FAILED", "No query for alert")

    # ---- Genie Spaces ----
    print("\n[workspace_extended] Genie Spaces")

    genie_name_allowed = f"gbot_test_{prefix}_genie_allowed"
    # serialized_space needs a version field for the ExportConverter
    genie_serialized = json.dumps({"version": 2})
    genie_obj = safe_execute("Create genie space (allowed)", lambda: admin.genie.create_space(
        warehouse_id=wh, serialized_space=genie_serialized, title=genie_name_allowed, description="GovernBot test genie space"
    ))
    genie_id_allowed = getattr(genie_obj, "space_id", None) if genie_obj else None
    if genie_id_allowed:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "genieSpace", "CREATE", genie_name_allowed, genie_id_allowed)
        cleanup.register("Trash genie space (allowed)", lambda: admin.genie.trash_space(space_id=genie_id_allowed), genie_name_allowed)
        # Trash genie space
        del_ok = safe_execute("Allowed trash genie space", lambda: admin.genie.trash_space(space_id=genie_id_allowed))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "genieSpace", "DELETE", genie_name_allowed, genie_id_allowed, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "genieSpace", "CREATE", genie_name_allowed, "", "FAILED", "Creation failed")

    # ---- Files ----
    print("\n[workspace_extended] Workspace Files")

    from databricks.sdk.service.workspace import ImportFormat, Language
    file_path_allowed = f"/Shared/gbot_test_{prefix}_file_allowed.py"
    file_content = b"# GovernBot test file\nprint('hello from file')\n"
    file_obj = safe_execute("Create file (allowed)", lambda: allowed.workspace.import_(
        path=file_path_allowed,
        content=base64.b64encode(file_content).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    ))
    if succeeded(file_obj):
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "file", "CREATE", file_path_allowed, file_path_allowed)
        cleanup.register("Delete file (allowed)", lambda: allowed.workspace.delete(path=file_path_allowed), file_path_allowed)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "file", "CREATE", file_path_allowed, "", "FAILED", "Creation failed")

    file_path_unapp = f"/Shared/gbot_test_{prefix}_file_unapproved.py"
    file_unapp = safe_execute("Create file (unapproved)", lambda: unapproved.workspace.import_(
        path=file_path_unapp,
        content=base64.b64encode(file_content).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    ))
    if succeeded(file_unapp):
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "file", "CREATE", file_path_unapp, file_path_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "DELETE_RESOURCE")
        # Delete unapproved file
        del_ok = safe_execute("Unapproved delete file", lambda: unapproved.workspace.delete(path=file_path_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "file", "DELETE", file_path_unapp, file_path_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "file", "CREATE", file_path_unapp, "", "FAILED", "Creation failed")

    # ---- Folders ----
    print("\n[workspace_extended] Workspace Folders")

    folder_path_allowed = f"/Shared/gbot_test_{prefix}_folder_allowed"
    folder_obj = safe_execute("Create folder (allowed)", lambda: allowed.workspace.mkdirs(path=folder_path_allowed))
    if succeeded(folder_obj):
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "folder", "CREATE", folder_path_allowed, folder_path_allowed)
        cleanup.register("Delete folder (allowed)", lambda: allowed.workspace.delete(path=folder_path_allowed, recursive=True), folder_path_allowed)

        # Folder ACL change (workspace ACL for directories)
        folder_status = safe_execute("Get folder status", lambda: allowed.workspace.get_status(path=folder_path_allowed))
        folder_obj_id = str(getattr(folder_status, "object_id", "")) if folder_status else ""
        if folder_obj_id:
            perm_ok = safe_execute("Unapproved change folder ACL", lambda: change_workspace_permission(
                unapproved, "directories", folder_obj_id, "users", "CAN_READ"))
            status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
            tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "directory", "CHANGE_PERMISSION",
                           folder_path_allowed, folder_obj_id, status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "folder", "CREATE", folder_path_allowed, "", "FAILED", "Creation failed")

    folder_path_unapp = f"/Shared/gbot_test_{prefix}_folder_unapproved"
    folder_unapp = safe_execute("Create folder (unapproved)", lambda: unapproved.workspace.mkdirs(path=folder_path_unapp))
    if succeeded(folder_unapp):
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "folder", "CREATE", folder_path_unapp, folder_path_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "REPORT_DELETION")
        del_ok = safe_execute("Unapproved delete folder", lambda: unapproved.workspace.delete(path=folder_path_unapp, recursive=True))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "folder", "DELETE", folder_path_unapp, folder_path_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "workspace_extended", "folder", "CREATE", folder_path_unapp, "", "FAILED", "Creation failed")

    # ---- Secrets (put + delete) ----
    print("\n[workspace_extended] Secrets")

    scope_name = f"gbot_test_{prefix}_scope_ext"
    scope_ok = safe_execute("Create scope for secrets", lambda: allowed.secrets.create_scope(scope=scope_name))
    if succeeded(scope_ok):
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "secretScope", "CREATE", scope_name, scope_name)
        cleanup.register("Delete scope (ext)", lambda: admin.secrets.delete_scope(scope=scope_name), scope_name)

        # Put a secret
        secret_key = f"gbot_test_{prefix}_key"
        secret_ok = safe_execute("Put secret", lambda: allowed.secrets.put_secret(
            scope=scope_name, key=secret_key, string_value="test_value_governbot"))
        if succeeded(secret_ok):
            tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "secret", "CREATE", f"{scope_name}/{secret_key}", scope_name)
            # Delete secret (tracked by delete_filters)
            del_ok = safe_execute("Delete secret", lambda: allowed.secrets.delete_secret(scope=scope_name, key=secret_key))
            status = "SUCCESS" if succeeded(del_ok) else "FAILED"
            tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "secret", "DELETE", f"{scope_name}/{secret_key}", scope_name, status)

        # Secret scope ACL delete
        from databricks.sdk.service.workspace import AclPermission
        acl_ok = safe_execute("Put scope ACL", lambda: allowed.secrets.put_acl(
            scope=scope_name, principal="users", permission=AclPermission.READ))
        if succeeded(acl_ok):
            tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "secretScope", "CHANGE_PERMISSION", scope_name, scope_name)
            # Delete ACL (tracked by acl_filters as secret_scope_acl_delete)
            del_acl = safe_execute("Delete scope ACL", lambda: allowed.secrets.delete_acl(scope=scope_name, principal="users"))
            status = "SUCCESS" if succeeded(del_acl) else "FAILED"
            tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "secretScope", "DELETE_ACL", scope_name, scope_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "workspace_extended", "secretScope", "CREATE", scope_name, "", "FAILED", "Creation failed")


def generate_uc_data_extended(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate missing UC data assets: table constraints, model versions, object updates, clean rooms.
    Uses existing 'main' catalog since this workspace requires UI for catalog creation with default storage."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin
    wh = cat.warehouse_id

    # Use existing 'main' catalog (workspace requires UI for new catalog creation with default storage)
    catalog_name = "main"
    schema_name = f"gbot_test_{prefix}_uc_ext"
    full_schema = f"{catalog_name}.{schema_name}"

    print("\n[uc_data_extended] Setup: Create schema under main catalog")

    # Schema
    schema_obj = safe_execute("Create schema", lambda: run_sql(admin, wh, f"CREATE SCHEMA IF NOT EXISTS {full_schema}"))
    if not schema_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "schema", "CREATE", full_schema, "", "FAILED", "Creation failed")
        return

    tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "schema", "CREATE", full_schema, full_schema)
    cleanup.register("Delete schema (uc_ext)", lambda: run_sql(admin, wh, f"DROP SCHEMA IF EXISTS {full_schema} CASCADE"), full_schema)
    safe_execute("Grant unapproved SP schema usage", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "USE_SCHEMA"))
    safe_execute("Grant unapproved SP schema create table", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_TABLE"))
    safe_execute("Grant unapproved SP schema create model", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_MODEL"))
    safe_execute("Grant unapproved SP schema create volume", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_VOLUME"))
    safe_execute("Grant unapproved SP schema create function", lambda: change_uc_permission(admin, "schema", full_schema, cat.unapproved_client_id, "CREATE_FUNCTION"))
    safe_execute("Grant allowed SP schema all", lambda: change_uc_permission(admin, "schema", full_schema, cat.allowed_client_id, "ALL_PRIVILEGES"))

    # ---- Table for constraints ----
    print("\n[uc_data_extended] Table Constraints")
    constraint_table = f"{full_schema}.gbot_test_{prefix}_constraint_tbl"
    tbl_ok = safe_execute("Create table for constraints", lambda: run_sql(admin, wh,
        f"CREATE TABLE {constraint_table} (id INT NOT NULL, name STRING, price DECIMAL(10,2)) USING DELTA"))
    if tbl_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "table", "CREATE", constraint_table, constraint_table)
        # Add constraint
        constraint_ok = safe_execute("Add table constraint", lambda: run_sql(admin, wh,
            f"ALTER TABLE {constraint_table} ADD CONSTRAINT pk_id PRIMARY KEY (id)"))
        if constraint_ok:
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "tableConstraint", "CREATE", constraint_table, constraint_table)
            # Drop constraint
            drop_ok = safe_execute("Drop table constraint", lambda: run_sql(admin, wh,
                f"ALTER TABLE {constraint_table} DROP CONSTRAINT pk_id"))
            status = "SUCCESS" if drop_ok else "FAILED"
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "tableConstraint", "DELETE", constraint_table, constraint_table, status)
        else:
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "tableConstraint", "CREATE", constraint_table, "", "FAILED", "Constraint failed")

    # ---- UC Registered Model + Model Version ----
    print("\n[uc_data_extended] UC Model Versions")
    model_name = f"gbot_test_{prefix}_model_ext"
    full_model = f"{full_schema}.{model_name}"
    model_obj = safe_execute("Create UC registered model", lambda: allowed.registered_models.create(
        catalog_name=catalog_name, schema_name=schema_name, name=model_name
    ))
    if model_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "ucRegisteredModel", "CREATE", full_model, full_model)
        cleanup.register("Delete UC model (ext)", lambda: allowed.registered_models.delete(full_name=full_model), full_model)

        # Model version — requires MLflow logging, not available via direct API
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "ucModelVersion", "CREATE",
                       full_model, "", "SKIPPED", "Model version creation requires MLflow logging runtime")
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "ucRegisteredModel", "CREATE", full_model, "", "FAILED", "Creation failed")

    # ---- Object Updates (rename/update tracked by object_changes_filters) ----
    print("\n[uc_data_extended] Object Updates (table rename, schema comment, catalog comment)")

    update_table = f"{full_schema}.gbot_test_{prefix}_rename_tbl"
    renamed_table = f"{full_schema}.gbot_test_{prefix}_renamed_tbl"
    tbl2_ok = safe_execute("Create table for rename", lambda: run_sql(admin, wh,
        f"CREATE TABLE {update_table} (id INT, val STRING) USING DELTA"))
    if tbl2_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "table", "CREATE", update_table, update_table)
        # Rename table (triggers uc_table_update)
        rename_ok = safe_execute("Rename table", lambda: run_sql(admin, wh,
            f"ALTER TABLE {update_table} RENAME TO {renamed_table}"))
        if rename_ok:
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "table", "UPDATE", update_table, renamed_table)
        else:
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "table", "UPDATE", update_table, "", "FAILED", "Rename failed")

    # Schema update (triggers uc_schema_update via comment change)
    schema_update_ok = safe_execute("Update schema comment", lambda: run_sql(admin, wh,
        f"ALTER SCHEMA {full_schema} SET DBPROPERTIES ('comment' = 'GovernBot test update')"))
    if schema_update_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "schema", "UPDATE", full_schema, full_schema)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "schema", "UPDATE", full_schema, "", "FAILED", "Schema update failed")

    # Catalog update (triggers uc_catalog_update) — use SDK instead of SQL
    catalog_update_ok = safe_execute("Update catalog comment", lambda: admin.catalogs.update(
        name=catalog_name, comment="GovernBot test catalog update"
    ))
    if catalog_update_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "catalog", "UPDATE", catalog_name, catalog_name)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "catalog", "UPDATE", catalog_name, "", "FAILED", "Catalog update failed")

    # UC registered model update (rename)
    if model_obj:
        model_new_name = f"gbot_test_{prefix}_model_renamed"
        full_model_new = f"{full_schema}.{model_new_name}"
        model_update_ok = safe_execute("Rename UC registered model", lambda: allowed.registered_models.update(
            full_name=full_model, new_name=model_new_name
        ))
        if model_update_ok:
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "ucRegisteredModel", "UPDATE", full_model, full_model_new)
        else:
            tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "ucRegisteredModel", "UPDATE", full_model, "", "FAILED", "Rename failed")

    # ---- Clean Room (best-effort) ----
    print("\n[uc_data_extended] Clean Room (best-effort)")
    from databricks.sdk.service.cleanrooms import CleanRoom as CleanRoomModel, CleanRoomCollaborator
    clean_room_name = f"gbot_test_{prefix}_cleanroom"
    # Clean room needs at least 1 collaborator — difficult without multi-account setup
    cr_obj = safe_execute("Create clean room", lambda: admin.clean_rooms.create(
        clean_room=CleanRoomModel(name=clean_room_name, comment="GovernBot test clean room")
    ))
    if cr_obj:
        cr_name = getattr(cr_obj, "name", clean_room_name)
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "cleanRoom", "CREATE", cr_name, cr_name)
        cleanup.register("Delete clean room", lambda: admin.clean_rooms.delete(name=cr_name), cr_name)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "cleanRoom", "CREATE", clean_room_name, "", "FAILED",
                       "Clean Room creation failed (may need premium tier)")

    # ---- UC Grants on various securables (for uc_grants_change coverage) ----
    print("\n[uc_data_extended] UC Grants changes")

    # Grant on catalog (different from create — triggers updatePermissions)
    grant_ok = safe_execute("Unapproved change catalog permission", lambda: change_uc_permission(
        unapproved, "catalog", catalog_name, "account users", "USE_CATALOG"))
    status = "SUCCESS" if succeeded(grant_ok) else "FAILED"
    tracker.record("unapproved", cat.unapproved_client_id, "uc_data_extended", "catalog", "CHANGE_PERMISSION", catalog_name, catalog_name,
                   status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    grant_ok2 = safe_execute("Unapproved change schema permission", lambda: change_uc_permission(
        unapproved, "schema", full_schema, "account users", "USE_SCHEMA"))
    status = "SUCCESS" if succeeded(grant_ok2) else "FAILED"
    tracker.record("unapproved", cat.unapproved_client_id, "uc_data_extended", "schema", "CHANGE_PERMISSION", full_schema, full_schema,
                   status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

    # ---- Volume, Function, Connection, Share, Recipient, Provider (retry from uc_data) ----
    print("\n[uc_data_extended] Volume + Function + Connection + Share + Recipient + Provider")

    from databricks.sdk.service.catalog import VolumeType

    vol_name = f"gbot_test_{prefix}_vol_ext"
    full_vol = f"{full_schema}.{vol_name}"
    vol_obj = safe_execute("Create volume", lambda: allowed.volumes.create(
        catalog_name=catalog_name, schema_name=schema_name, name=vol_name, volume_type=VolumeType.MANAGED
    ))
    if vol_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "volume", "CREATE", full_vol, full_vol)
        # Permission change on volume
        perm_ok = safe_execute("Unapproved change volume permission", lambda: change_uc_permission(
            unapproved, "volume", full_vol, "account users", "READ_VOLUME"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "uc_data_extended", "volume", "CHANGE_PERMISSION", full_vol, full_vol,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")
        # Delete volume
        del_ok = safe_execute("Delete volume", lambda: allowed.volumes.delete(name=full_vol))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "volume", "DELETE", full_vol, full_vol, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "volume", "CREATE", full_vol, "", "FAILED", "Creation failed")

    func_name = f"gbot_test_{prefix}_func_ext"
    full_func = f"{full_schema}.{func_name}"
    func_ok = safe_execute("Create function (SQL)", lambda: run_sql(admin, wh,
        f"CREATE FUNCTION {full_func}(x INT) RETURNS INT RETURN x + 1"))
    if func_ok:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "function", "CREATE", full_func, full_func)
        del_ok = safe_execute("Delete function", lambda: allowed.functions.delete(name=full_func))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "function", "DELETE", full_func, full_func, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "function", "CREATE", full_func, "", "FAILED", "SQL create failed")

    from databricks.sdk.service.catalog import ConnectionType
    conn_name = f"gbot_test_{prefix}_conn_ext"
    conn_obj = safe_execute("Create connection", lambda: allowed.connections.create(
        name=conn_name, connection_type=ConnectionType.MYSQL,
        options={"host": "localhost", "port": "3306", "user": "test", "password": "test"}
    ))
    if conn_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "connection", "CREATE", conn_name, conn_name)
        del_ok = safe_execute("Delete connection", lambda: allowed.connections.delete(name=conn_name))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "connection", "DELETE", conn_name, conn_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "connection", "CREATE", conn_name, "", "FAILED", "Creation failed")

    share_name = f"gbot_test_{prefix}_share_ext"
    share_obj = safe_execute("Create share", lambda: allowed.shares.create(name=share_name))
    if share_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "share", "CREATE", share_name, share_name)
        del_ok = safe_execute("Delete share", lambda: allowed.shares.delete(name=share_name))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "share", "DELETE", share_name, share_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "share", "CREATE", share_name, "", "FAILED", "Creation failed")

    from databricks.sdk.service.sharing import AuthenticationType as ShareAuthType
    recip_name = f"gbot_test_{prefix}_recip_ext"
    recip_obj = safe_execute("Create recipient", lambda: allowed.recipients.create(name=recip_name, authentication_type=ShareAuthType.TOKEN))
    if recip_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "recipient", "CREATE", recip_name, recip_name)
        del_ok = safe_execute("Delete recipient", lambda: allowed.recipients.delete(name=recip_name))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "recipient", "DELETE", recip_name, recip_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "recipient", "CREATE", recip_name, "", "FAILED", "Creation failed")

    prov_name = f"gbot_test_{prefix}_prov_ext"
    prov_obj = safe_execute("Create provider", lambda: allowed.providers.create(
        name=prov_name, authentication_type=ShareAuthType.TOKEN,
        recipient_profile_str=json.dumps({
            "shareCredentialsVersion": 1,
            "bearerToken": "placeholder",
            "endpoint": "https://placeholder.cloud.databricks.com",
        })
    ))
    if prov_obj:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "provider", "CREATE", prov_name, prov_name)
        del_ok = safe_execute("Delete provider", lambda: allowed.providers.delete(name=prov_name))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "provider", "DELETE", prov_name, prov_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "uc_data_extended", "provider", "CREATE", prov_name, "", "FAILED", "Creation failed")


def generate_acl_extended(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate missing ACL change events: warehouse ACL, workspace ACL for dashboards/queries, registered model ACL, VS endpoint ACL, feature table ACL."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin
    perm_group = "users"
    wh = cat.warehouse_id

    # ---- Warehouse ACL change ----
    print("\n[acl_extended] Warehouse ACL change")

    wh_name = f"gbot_test_{prefix}_wh_acl"
    wh_obj = safe_execute("Create warehouse for ACL test", lambda: allowed.warehouses.create(
        name=wh_name, cluster_size="2X-Small", auto_stop_mins=10, max_num_clusters=1,
    ).result())
    wh_id = getattr(wh_obj, "id", None) if wh_obj else None
    if wh_id:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "warehouse", "CREATE", wh_name, wh_id)
        cleanup.register("Delete warehouse (acl)", lambda: admin.warehouses.delete(id=wh_id), wh_name)

        # Warehouse ACL change (unapproved)
        perm_ok = safe_execute("Unapproved change warehouse ACL", lambda: change_workspace_permission(
            unapproved, "sql/warehouses", wh_id, perm_group, "CAN_USE"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "acl_extended", "warehouse", "CHANGE_PERMISSION", wh_name, wh_id,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

        # Allowed change
        perm_ok2 = safe_execute("Allowed change warehouse ACL", lambda: change_workspace_permission(
            allowed, "sql/warehouses", wh_id, perm_group, "CAN_MANAGE"))
        status = "SUCCESS" if succeeded(perm_ok2) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "warehouse", "CHANGE_PERMISSION", wh_name, wh_id, status)

        # Delete warehouse
        del_ok = safe_execute("Delete warehouse", lambda: admin.warehouses.delete(id=wh_id))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "warehouse", "DELETE", wh_name, wh_id, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "warehouse", "CREATE", wh_name, "", "FAILED", "Creation failed")

    # ---- Dashboard ACL change (workspace ACL) ----
    print("\n[acl_extended] Dashboard ACL via workspace ACL")

    dash_name = f"gbot_test_{prefix}_dash_acl"
    from databricks.sdk.service.dashboards import Dashboard
    dash_obj = safe_execute("Create dashboard for ACL test", lambda: allowed.lakeview.create(
        dashboard=Dashboard(display_name=dash_name)
    ))
    dash_id = getattr(dash_obj, "dashboard_id", None) if dash_obj else None
    if dash_id:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "dashboard", "CREATE", dash_name, dash_id)
        cleanup.register("Trash dashboard (acl)", lambda: admin.lakeview.trash(dash_id), dash_name)

        # Dashboard permission via workspace ACL (dashboardsv3/<id> pattern)
        perm_ok = safe_execute("Unapproved change dashboard ACL", lambda: change_workspace_permission(
            unapproved, "dashboards", dash_id, perm_group, "CAN_READ"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "acl_extended", "dashboard", "CHANGE_PERMISSION", dash_name, dash_id,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

        # Dashboard clone
        clone_obj = safe_execute("Clone dashboard", lambda: allowed.lakeview.create(
            dashboard=Dashboard(display_name=f"{dash_name}_clone")
        ))
        clone_id = getattr(clone_obj, "dashboard_id", None) if clone_obj else None
        if clone_id:
            tracker.record("allowed", cat.allowed_client_id, "acl_extended", "dashboard", "CLONE", f"{dash_name}_clone", clone_id)
            cleanup.register("Trash cloned dashboard", lambda: admin.lakeview.trash(clone_id), f"{dash_name}_clone")

        # Trash dashboard
        del_ok = safe_execute("Trash dashboard", lambda: allowed.lakeview.trash(dash_id))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "dashboard", "DELETE", dash_name, dash_id, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "dashboard", "CREATE", dash_name, "", "FAILED", "Creation failed")

    # ---- Query ACL change (workspace ACL) ----
    print("\n[acl_extended] Query ACL via workspace ACL")

    query_name = f"gbot_test_{prefix}_query_acl"
    from databricks.sdk.service.sql import CreateQueryRequestQuery
    query_obj = safe_execute("Create query for ACL test", lambda: allowed.queries.create(
        query=CreateQueryRequestQuery(
            query_text="SELECT 1 AS test_acl", display_name=query_name, warehouse_id=wh
        )
    ))
    query_id = getattr(query_obj, "id", None) if query_obj else None
    if query_id:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "query", "CREATE", query_name, query_id)
        cleanup.register("Delete query (acl)", lambda: admin.queries.delete(id=query_id), query_name)

        # Query ACL (queries/<id>)
        perm_ok = safe_execute("Unapproved change query ACL", lambda: change_workspace_permission(
            unapproved, "queries", query_id, perm_group, "CAN_RUN"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "acl_extended", "query", "CHANGE_PERMISSION", query_name, query_id,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")
    else:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "query", "CREATE", query_name, "", "FAILED", "Creation failed")

    # ---- Serving Endpoint ACL change ----
    print("\n[acl_extended] Serving Endpoint")

    ep_name = f"gbot_test_{prefix}_ep_acl"
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput, ExternalModel, OpenAiConfig, ExternalModelProvider
    ep_obj = safe_execute("Create serving endpoint for ACL", lambda: allowed.serving_endpoints.create(
        name=ep_name,
        config=EndpointCoreConfigInput(
            name=ep_name,
            served_entities=[ServedEntityInput(
                external_model=ExternalModel(
                    name="gpt-4",
                    provider=ExternalModelProvider.OPENAI,
                    task="llm/v1/chat",
                    openai_config=OpenAiConfig(openai_api_key_plaintext="sk-placeholder-key-for-governbot-test"),
                ),
            )]
        ),
    ))
    ep_created = succeeded(ep_obj)
    if ep_created:
        # Get the endpoint ID for permission changes
        ep_details = safe_execute("Get endpoint details", lambda: allowed.serving_endpoints.get(name=ep_name))
        ep_id = getattr(ep_details, "id", ep_name) if ep_details else ep_name
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "servingEndpoint", "CREATE", ep_name, ep_id)
        cleanup.register("Delete endpoint (acl)", lambda: admin.serving_endpoints.delete(name=ep_name), ep_name)

        # Endpoint ACL change (use endpoint ID)
        perm_ok = safe_execute("Unapproved change endpoint ACL", lambda: change_workspace_permission(
            unapproved, "serving-endpoints", ep_id, perm_group, "CAN_QUERY"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "acl_extended", "servingEndpoint", "CHANGE_PERMISSION", ep_name, ep_id,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

        # Allowed endpoint ACL
        perm_ok2 = safe_execute("Allowed change endpoint ACL", lambda: change_workspace_permission(
            allowed, "serving-endpoints", ep_id, perm_group, "CAN_MANAGE"))
        status = "SUCCESS" if succeeded(perm_ok2) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "servingEndpoint", "CHANGE_PERMISSION", ep_name, ep_id, status)

        # Delete endpoint
        del_ok = safe_execute("Delete serving endpoint", lambda: admin.serving_endpoints.delete(name=ep_name))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "servingEndpoint", "DELETE", ep_name, ep_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "servingEndpoint", "CREATE", ep_name, "", "FAILED", "Creation failed")

    # ---- Vector Search Endpoint ACL ----
    print("\n[acl_extended] Vector Search Endpoint ACL")

    vs_name = f"gbot_test_{prefix}_vs_acl"
    from databricks.sdk.service.vectorsearch import EndpointType
    vs_obj = safe_execute("Create VS endpoint for ACL", lambda: allowed.vector_search_endpoints.create_endpoint(
        name=vs_name, endpoint_type=EndpointType.STANDARD
    ))
    vs_created = succeeded(vs_obj)
    if vs_created:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "vectorSearchEndpoint", "CREATE", vs_name, vs_name)
        cleanup.register("Delete VS endpoint (acl)", lambda: admin.vector_search_endpoints.delete_endpoint(endpoint_name=vs_name), vs_name)

        # VS endpoint ACL change — must use endpoint ID (not name) with permissions.update
        # Wait for endpoint to provision first
        print("  Waiting 60s for VS endpoint to provision...")
        time.sleep(60)
        # Get the endpoint ID
        vs_details = safe_execute("Get VS endpoint details", lambda: admin.vector_search_endpoints.get_endpoint(endpoint_name=vs_name))
        vs_id = getattr(vs_details, "id", None) if succeeded(vs_details) else None
        if vs_id:
            from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
            perm_ok = safe_execute("Unapproved change VS endpoint ACL (by ID)", lambda: unapproved.permissions.update(
                request_object_type="vector-search-endpoints",
                request_object_id=vs_id,
                access_control_list=[AccessControlRequest(group_name=perm_group, permission_level=PermissionLevel.CAN_MANAGE)]
            ))
        else:
            print("  SKIP: Could not get VS endpoint ID")
            perm_ok = _SENTINEL_FAIL
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "acl_extended", "vectorSearchEndpoint", "CHANGE_PERMISSION", vs_name, vs_name,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")
    else:
        tracker.record("allowed", cat.allowed_client_id, "acl_extended", "vectorSearchEndpoint", "CREATE", vs_name, "", "FAILED", "Creation failed")


def generate_admin_extended(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate admin/security events: group delete, entitlement changes, set/remove admin."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin

    # ---- Group lifecycle (create + delete) ----
    print("\n[admin_extended] Group lifecycle (create + delete)")

    group_name = f"gbot_test_{prefix}_grp_del"
    group_obj = safe_execute("Create group for delete test", lambda: admin.groups.create(display_name=group_name))
    group_id = getattr(group_obj, "id", None) if group_obj else None
    if group_id:
        tracker.record("allowed", cat.allowed_client_id, "admin_extended", "groups", "CREATE", group_name, group_id)
        # Delete group (triggers workspace_groups_deletion filter)
        del_ok = safe_execute("Delete group", lambda: admin.groups.delete(id=group_id))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("allowed", cat.allowed_client_id, "admin_extended", "groups", "DELETE", group_name, group_id, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "admin_extended", "groups", "CREATE", group_name, "", "FAILED", "Group creation failed")

    # Unapproved group lifecycle
    group_name_unapp = f"gbot_test_{prefix}_grp_unapp"
    group_unapp = safe_execute("Unapproved create group", lambda: unapproved.groups.create(display_name=group_name_unapp))
    group_id_unapp = getattr(group_unapp, "id", None) if group_unapp else None
    if group_id_unapp:
        tracker.record("unapproved", cat.unapproved_client_id, "admin_extended", "groups", "CREATE", group_name_unapp, group_id_unapp,
                       "SUCCESS", "", "UNAPPROVED_CREATION", "REPORT_SECURITY_TEAM")
        del_ok = safe_execute("Unapproved delete group", lambda: unapproved.groups.delete(id=group_id_unapp))
        status = "SUCCESS" if succeeded(del_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "admin_extended", "groups", "DELETE", group_name_unapp, group_id_unapp,
                       status, "", "UNAUTHORIZED_DELETION", "REPORT_DELETION")
    else:
        tracker.record("unapproved", cat.unapproved_client_id, "admin_extended", "groups", "CREATE", group_name_unapp, "", "FAILED", "Creation failed")

    # ---- Entitlement changes (best-effort, requires admin) ----
    print("\n[admin_extended] Entitlement changes (best-effort)")

    # These require account admin — record as SKIPPED if not possible
    for op_name, desc in [
        ("changeDatabricksWorkspaceAcl", "Workspace grant change — requires account admin"),
        ("changeDatabricksSqlAcl", "DBSQL grant change — requires account admin"),
        ("changeDbTokenAcl", "Token ACL change — requires admin"),
        ("setAdmin", "Set admin — requires account admin"),
        ("removeAdmin", "Remove admin — requires account admin"),
    ]:
        tracker.record("allowed", cat.allowed_client_id, "admin_extended", "identity_replace", "ENTITLEMENT_CHANGE",
                       f"SKIPPED_{op_name}", "", "SKIPPED", desc)

    # ---- SQL Permissions (any_file grant/revoke) ----
    print("\n[admin_extended] SQL Permissions (best-effort)")

    for op_name, desc in [
        ("grantPermission_any_file", "ANY FILE grant — requires admin SQL permissions"),
        ("revokePermission_any_file", "ANY FILE revoke — requires admin SQL permissions"),
    ]:
        tracker.record("allowed", cat.allowed_client_id, "admin_extended", "any_file_permissions", "SQL_PERMISSION",
                       f"SKIPPED_{op_name}", "", "SKIPPED", desc)


def generate_ml_ai_extended(clients: Clients, tracker: TrackingTable, cleanup: CleanupRegistry, prefix: str, dry_run: bool, verbose: bool):
    """Generate missing ML/AI assets: monitors, feature tables, vector indexes."""
    cat = clients
    allowed = cat.allowed
    unapproved = cat.unapproved
    admin = cat.admin
    wh = cat.warehouse_id

    # ---- Monitors (requires a UC table) ----
    print("\n[ml_ai_extended] Monitors")

    # Use existing main catalog with a dedicated schema for monitors
    mon_catalog = "main"
    mon_schema = f"{mon_catalog}.gbot_test_{prefix}_mon"
    mon_table = f"{mon_schema}.mon_table"

    schema_ok = safe_execute("Create schema for monitor", lambda: run_sql(admin, wh, f"CREATE SCHEMA IF NOT EXISTS {mon_schema}"))
    if schema_ok:
        cleanup.register("Delete monitor schema", lambda: run_sql(admin, wh, f"DROP SCHEMA IF EXISTS {mon_schema} CASCADE"), mon_schema)
        tbl_ok = safe_execute("Create table for monitor", lambda: run_sql(admin, wh,
            f"CREATE TABLE {mon_table} (id INT, prediction DOUBLE, label DOUBLE, ts TIMESTAMP) USING DELTA"))

        if tbl_ok:
            # Insert sample data
            safe_execute("Insert monitor data", lambda: run_sql(admin, wh,
                f"INSERT INTO {mon_table} VALUES (1, 0.8, 1.0, current_timestamp()), (2, 0.3, 0.0, current_timestamp())"))

            # Create monitor
            from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType
            mon_obj = safe_execute("Create monitor", lambda: admin.quality_monitors.create(
                table_name=mon_table,
                assets_dir=f"/Shared/gbot_test_{prefix}_mon_assets",
                output_schema_name=mon_schema,
                inference_log=MonitorInferenceLog(
                    model_id_col="id", prediction_col="prediction", label_col="label", timestamp_col="ts",
                    problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
                    granularities=["1 day"],
                ),
            ))
            if mon_obj:
                tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "monitors", "CREATE", mon_table, mon_table)
                # Delete monitor
                del_ok = safe_execute("Delete monitor", lambda: admin.quality_monitors.delete(table_name=mon_table))
                status = "SUCCESS" if succeeded(del_ok) else "FAILED"
                tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "monitors", "DELETE", mon_table, mon_table, status)
            else:
                tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "monitors", "CREATE", mon_table, "", "FAILED", "Monitor creation failed")
        else:
            tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "monitors", "CREATE", mon_table, "", "FAILED", "Table creation failed")
    else:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "monitors", "CREATE", mon_table, "", "FAILED", "Schema creation failed")

    # ---- Feature Tables (legacy — best-effort) ----
    print("\n[ml_ai_extended] Feature Tables (best-effort)")
    tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "featureTable", "CREATE",
                   "SKIPPED_featureTable", "", "SKIPPED", "Legacy feature store — requires runtime with ML")

    # ---- Feature Spec (best-effort) ----
    tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "featureSpec", "CREATE",
                   "SKIPPED_featureSpec", "", "SKIPPED", "Feature Spec — requires feature store runtime")

    # ---- Vector Index (requires VS endpoint + source table) ----
    print("\n[ml_ai_extended] Vector Index (best-effort)")
    tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "vectorIndex", "CREATE",
                   "SKIPPED_vectorIndex", "", "SKIPPED", "Vector Index — requires VS endpoint in ONLINE state + embedding model")

    # ---- Workspace Registered Model ACL ----
    print("\n[ml_ai_extended] Workspace Registered Model ACL")

    model_name = f"gbot_test_{prefix}_wsmodel_acl"
    model_obj = safe_execute("Create workspace model for ACL", lambda: allowed.model_registry.create_model(name=model_name))
    if model_obj:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "registeredModel", "CREATE", model_name, model_name)
        cleanup.register("Delete ws model (ml_ext)", lambda: admin.model_registry.delete_model(name=model_name), model_name)

        # Registered model ACL change (needs the registered model's internal ID)
        model_details = safe_execute("Get model details", lambda: allowed.model_registry.get_model(name=model_name))
        model_id = None
        if model_details:
            rm = getattr(model_details, "registered_model_databricks", None) or getattr(model_details, "registered_model", None)
            model_id = getattr(rm, "id", None) if rm else None
        perm_ok = safe_execute("Unapproved change registered model ACL", lambda: change_workspace_permission(
            unapproved, "registered-models", model_id or model_name, "users", "CAN_READ"))
        status = "SUCCESS" if succeeded(perm_ok) else "FAILED"
        tracker.record("unapproved", cat.unapproved_client_id, "ml_ai_extended", "registeredModel", "CHANGE_PERMISSION", model_name, model_name,
                       status, "", "UNAUTHORIZED_PERMISSION_CHANGE", "REVERT_PERMISSION")

        # Workspace registered model rename (triggers object_changes filter)
        new_model_name = f"gbot_test_{prefix}_wsmodel_renamed"
        rename_ok = safe_execute("Rename workspace model", lambda: allowed.model_registry.rename_model(
            name=model_name, new_name=new_model_name
        ))
        if rename_ok:
            tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "registeredModel", "UPDATE", model_name, new_model_name)
            # Delete renamed model
            del_ok = safe_execute("Delete renamed model", lambda: admin.model_registry.delete_model(name=new_model_name))
            status = "SUCCESS" if succeeded(del_ok) else "FAILED"
            tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "registeredModel", "DELETE", new_model_name, new_model_name, status)
        else:
            tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "registeredModel", "UPDATE", model_name, "", "FAILED", "Rename failed")
            del_ok = safe_execute("Delete original model", lambda: admin.model_registry.delete_model(name=model_name))
            status = "SUCCESS" if succeeded(del_ok) else "FAILED"
            tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "registeredModel", "DELETE", model_name, model_name, status)
    else:
        tracker.record("allowed", cat.allowed_client_id, "ml_ai_extended", "registeredModel", "CREATE", model_name, "", "FAILED",
                       "Workspace model creation failed (may be disabled)")


# ---------------------------------------------------------------------------
# Cleanup-only mode
# ---------------------------------------------------------------------------
def cleanup_only(clients: Clients):
    """Scan for gbot_test_* resources and delete them."""
    print("Scanning for gbot_test_* resources to clean up...")
    admin = clients.admin
    allowed = clients.allowed

    # Catalogs
    try:
        for cat in admin.catalogs.list():
            if cat.name and cat.name.startswith("gbot_test_"):
                print(f"  Deleting catalog: {cat.name}")
                safe_execute(f"Delete catalog {cat.name}", lambda n=cat.name: admin.catalogs.delete(name=n, force=True))
    except Exception as e:
        print(f"  WARN: catalog scan: {e}")

    # Clusters
    try:
        for c in admin.clusters.list():
            if c.cluster_name and c.cluster_name.startswith("gbot_test_"):
                print(f"  Deleting cluster: {c.cluster_name}")
                safe_execute(f"Delete cluster {c.cluster_name}", lambda cid=c.cluster_id: admin.clusters.permanent_delete(cluster_id=cid))
    except Exception as e:
        print(f"  WARN: cluster scan: {e}")

    # Cluster policies
    try:
        for p in admin.cluster_policies.list():
            if p.name and p.name.startswith("gbot_test_"):
                print(f"  Deleting policy: {p.name}")
                safe_execute(f"Delete policy {p.name}", lambda pid=p.policy_id: admin.cluster_policies.delete(policy_id=pid))
    except Exception as e:
        print(f"  WARN: policy scan: {e}")

    # Instance pools
    try:
        for p in admin.instance_pools.list():
            if p.instance_pool_name and p.instance_pool_name.startswith("gbot_test_"):
                print(f"  Deleting pool: {p.instance_pool_name}")
                safe_execute(f"Delete pool {p.instance_pool_name}", lambda pid=p.instance_pool_id: admin.instance_pools.delete(instance_pool_id=pid))
    except Exception as e:
        print(f"  WARN: pool scan: {e}")

    # Warehouses
    try:
        for w in admin.warehouses.list():
            if w.name and w.name.startswith("gbot_test_"):
                print(f"  Deleting warehouse: {w.name}")
                safe_execute(f"Delete warehouse {w.name}", lambda wid=w.id: admin.warehouses.delete(id=wid))
    except Exception as e:
        print(f"  WARN: warehouse scan: {e}")

    # Jobs
    try:
        for j in admin.jobs.list(name="gbot_test_"):
            if j.settings and j.settings.name and j.settings.name.startswith("gbot_test_"):
                print(f"  Deleting job: {j.settings.name}")
                safe_execute(f"Delete job {j.settings.name}", lambda jid=j.job_id: admin.jobs.delete(job_id=jid))
    except Exception as e:
        print(f"  WARN: job scan: {e}")

    # Pipelines
    try:
        for p in admin.pipelines.list_pipelines(filter=f"name LIKE 'gbot_test_%'"):
            if p.name and p.name.startswith("gbot_test_"):
                print(f"  Deleting pipeline: {p.name}")
                safe_execute(f"Delete pipeline {p.name}", lambda pid=p.pipeline_id: admin.pipelines.delete(pipeline_id=pid))
    except Exception as e:
        print(f"  WARN: pipeline scan: {e}")

    # Serving endpoints
    try:
        for ep in admin.serving_endpoints.list():
            if ep.name and ep.name.startswith("gbot_test_"):
                print(f"  Deleting endpoint: {ep.name}")
                safe_execute(f"Delete endpoint {ep.name}", lambda n=ep.name: admin.serving_endpoints.delete(name=n))
    except Exception as e:
        print(f"  WARN: endpoint scan: {e}")

    # Secret scopes
    try:
        for s in admin.secrets.list_scopes():
            if s.name and s.name.startswith("gbot_test_"):
                print(f"  Deleting scope: {s.name}")
                safe_execute(f"Delete scope {s.name}", lambda n=s.name: admin.secrets.delete_scope(scope=n))
    except Exception as e:
        print(f"  WARN: scope scan: {e}")

    # Notebooks/workspace
    try:
        items = admin.workspace.list(path="/Shared")
        if items:
            for item in items:
                path = getattr(item, "path", "")
                if path and "gbot_test_" in path:
                    print(f"  Deleting workspace item: {path}")
                    safe_execute(f"Delete {path}", lambda p=path: admin.workspace.delete(path=p, recursive=True))
    except Exception as e:
        print(f"  WARN: workspace scan: {e}")

    # Connections
    try:
        for c in admin.connections.list():
            if c.name and c.name.startswith("gbot_test_"):
                print(f"  Deleting connection: {c.name}")
                safe_execute(f"Delete connection {c.name}", lambda n=c.name: admin.connections.delete(name=n))
    except Exception as e:
        print(f"  WARN: connection scan: {e}")

    # Shares
    try:
        for s in admin.shares.list():
            if s.name and s.name.startswith("gbot_test_"):
                print(f"  Deleting share: {s.name}")
                safe_execute(f"Delete share {s.name}", lambda n=s.name: admin.shares.delete(name=n))
    except Exception as e:
        print(f"  WARN: share scan: {e}")

    # Recipients
    try:
        for r in admin.recipients.list():
            if r.name and r.name.startswith("gbot_test_"):
                print(f"  Deleting recipient: {r.name}")
                safe_execute(f"Delete recipient {r.name}", lambda n=r.name: admin.recipients.delete(name=n))
    except Exception as e:
        print(f"  WARN: recipient scan: {e}")

    # Providers
    try:
        for p in admin.providers.list():
            if p.name and p.name.startswith("gbot_test_"):
                print(f"  Deleting provider: {p.name}")
                safe_execute(f"Delete provider {p.name}", lambda n=p.name: admin.providers.delete(name=n))
    except Exception as e:
        print(f"  WARN: provider scan: {e}")

    # Groups
    try:
        for g in admin.groups.list(filter=f"displayName sw \"gbot_test_\""):
            if g.display_name and g.display_name.startswith("gbot_test_"):
                print(f"  Deleting group: {g.display_name}")
                safe_execute(f"Delete group {g.display_name}", lambda gid=g.id: admin.groups.delete(id=gid))
    except Exception as e:
        print(f"  WARN: group scan: {e}")

    # Vector search endpoints
    try:
        for v in admin.vector_search_endpoints.list_endpoints():
            if v.name and v.name.startswith("gbot_test_"):
                print(f"  Deleting vector search endpoint: {v.name}")
                safe_execute(f"Delete VS endpoint {v.name}", lambda n=v.name: admin.vector_search_endpoints.delete_endpoint(endpoint_name=n))
    except Exception as e:
        print(f"  WARN: vector search scan: {e}")

    # Apps
    try:
        for a in admin.apps.list():
            if a.name and a.name.startswith("gbot-test-"):
                print(f"  Deleting app: {a.name}")
                safe_execute(f"Delete app {a.name}", lambda n=a.name: admin.apps.delete(name=n))
    except Exception as e:
        print(f"  WARN: app scan: {e}")

    # Alerts
    try:
        for a in admin.alerts.list():
            name = getattr(a, "display_name", "") or getattr(a, "name", "") or ""
            alert_id = getattr(a, "id", None)
            if name and name.startswith("gbot_test_") and alert_id:
                print(f"  Deleting alert: {name}")
                safe_execute(f"Delete alert {name}", lambda aid=alert_id: admin.alerts.delete(id=aid))
    except Exception as e:
        print(f"  WARN: alert scan: {e}")

    # Workspace models (legacy)
    try:
        for m in admin.model_registry.list_models():
            name = getattr(m, "name", "") or ""
            if name.startswith("gbot_test_"):
                print(f"  Deleting workspace model: {name}")
                safe_execute(f"Delete model {name}", lambda n=name: admin.model_registry.delete_model(name=n))
    except Exception as e:
        print(f"  WARN: workspace model scan: {e}")

    print("\nCleanup scan complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
GENERATORS = {
    "uc_data": generate_uc_data,
    "compute": generate_compute,
    "workspace": generate_workspace,
    "ml_ai": generate_ml_ai,
    "admin_security": generate_admin_security,
    "workspace_extended": generate_workspace_extended,
    "uc_data_extended": generate_uc_data_extended,
    "acl_extended": generate_acl_extended,
    "admin_extended": generate_admin_extended,
    "ml_ai_extended": generate_ml_ai_extended,
}


def main():
    parser = argparse.ArgumentParser(description="GovernBot Test Data Generator")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without executing")
    parser.add_argument("--cleanup-only", action="store_true", help="Only clean up gbot_test_* resources")
    parser.add_argument("--no-cleanup", action="store_true", help="Leave test resources for inspection")
    parser.add_argument("--categories", nargs="+", choices=ALL_CATEGORIES, default=ALL_CATEGORIES,
                        help="Which asset categories to generate")
    parser.add_argument("--warehouse-id", default="", help="SQL warehouse ID (auto-detected if not set)")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help=f"Catalog for tracking table (default: {DEFAULT_CATALOG})")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help=f"Schema for tracking table (default: {DEFAULT_SCHEMA})")
    parser.add_argument("--verbose", action="store_true", help="Extra output")
    args = parser.parse_args()

    prefix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print(f"GovernBot Test Data Generator")
    print(f"  Workspace: {WORKSPACE_URL}")
    print(f"  Prefix:    gbot_test_{prefix}")
    print(f"  Categories: {', '.join(args.categories)}")
    print(f"  Dry run:   {args.dry_run}")
    print(f"  Cleanup:   {'cleanup-only' if args.cleanup_only else ('no-cleanup' if args.no_cleanup else 'auto')}")
    print()

    clients = create_clients(warehouse_id=args.warehouse_id)

    if args.cleanup_only:
        cleanup_only(clients)
        return

    print(f"\nInitializing tracking table: {args.catalog}.{args.schema}.{TRACKING_TABLE}")
    tracker = TrackingTable(clients.admin, clients.warehouse_id, args.catalog, args.schema)
    print(f"  Run ID: {tracker.run_id}")

    cleanup_reg = CleanupRegistry()

    for category in args.categories:
        gen_fn = GENERATORS.get(category)
        if gen_fn:
            print(f"\n{'='*60}")
            print(f"Generating: {category}")
            print(f"{'='*60}")
            try:
                gen_fn(clients, tracker, cleanup_reg, prefix, args.dry_run, args.verbose)
            except Exception as e:
                print(f"\nERROR in {category}: {e}")
                tracker.record("system", "", category, "category", "GENERATE", category, "", "FAILED", str(e))

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"  Run ID: {tracker.run_id}")
    print(f"  Tracking table: {tracker.full_table}")
    print(f"  Query: SELECT * FROM {tracker.full_table} WHERE run_id = '{tracker.run_id}' ORDER BY timestamp")

    if not args.no_cleanup and not args.dry_run:
        print("\nWaiting 5 seconds before cleanup (for audit log propagation)...")
        time.sleep(5)
        cleanup_reg.execute()
    elif args.no_cleanup:
        print("\n--no-cleanup specified: resources left in place for inspection.")
        print(f"  Run with --cleanup-only to remove them later.")

    print("\nDone.")


if __name__ == "__main__":
    main()
