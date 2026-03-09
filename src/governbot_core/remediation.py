"""
Remediation: delete handlers per object type (dispatcher), revert_permissions, execute_remediation.
All delete_* functions are registered in object_types.DELETE_HANDLERS.
Parity with governBot/mx_scripts/06_remediation.py: get_resource_definition, revert_permissions
(with get_approved_permissions), revert_entitlement_change, execute_remediation (all action types, return order).
"""
import json
from typing import Any, Callable, List, Optional, Set, Tuple

from governbot_core.object_types import get_delete_handler, register_delete_handler
from governbot_core.permissions import build_access_control_request, detect_principal_type

try:
    from databricks.sdk.errors import ResourceDoesNotExist
except ImportError:
    ResourceDoesNotExist = Exception  # noqa: A001


def delete_job(client: Any, job_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.jobs.delete(int(job_id))
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_cluster(client: Any, cluster_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.clusters.permanent_delete(cluster_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_pipeline(client: Any, pipeline_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.pipelines.delete(pipeline_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_serving_endpoint(client: Any, endpoint_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.serving_endpoints.delete(endpoint_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_app(client: Any, app_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.apps.delete(app_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_mlflow_experiment(client: Any, experiment_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.experiments.delete_experiment(experiment_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_monitor(client: Any, table_name: str) -> Tuple[bool, Optional[str]]:
    try:
        if hasattr(client, "quality_monitors"):
            client.quality_monitors.delete(table_name=table_name)
            return (True, None)
        return (False, "Lakehouse Monitoring API not available")
    except Exception as e:
        return (False, str(e))


def delete_alert(client: Any, alert_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.alerts.delete(alert_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_warehouse(client: Any, warehouse_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.warehouses.delete(warehouse_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_cluster_policy(client: Any, policy_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.cluster_policies.delete(policy_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_instance_pool(client: Any, pool_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.instance_pools.delete(pool_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_secret_scope(client: Any, scope_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.secrets.delete_scope(scope_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_dashboard(client: Any, dashboard_id: str) -> Tuple[bool, Optional[str]]:
    try:
        try:
            client.lakeview.trash(dashboard_id)
            return (True, None)
        except (AttributeError, Exception):
            pass
        client.dashboards.delete(dashboard_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_lakeview_dashboard(client: Any, dashboard_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.lakeview.trash(dashboard_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_query(client: Any, query_id: str) -> Tuple[bool, Optional[str]]:
    try:
        client.queries.delete(query_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_uc_schema(client: Any, full_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.schemas.delete(full_name=full_name, force=True)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_uc_table(client: Any, full_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.tables.delete(full_name=full_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_uc_volume(client: Any, full_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.volumes.delete(full_name=full_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_uc_catalog(client: Any, catalog_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.catalogs.delete(name=catalog_name, force=True)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_uc_connection(client: Any, connection_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.connections.delete(name=connection_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def delete_uc_function(client: Any, full_name: str) -> Tuple[bool, Optional[str]]:
    try:
        client.functions.delete(full_name=full_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))


def get_resource_definition(client: Any, object_type: str, object_id: str) -> Optional[str]:
    """
    Get resource definition as JSON string for backup. May raise ResourceDoesNotExist.
    Returns None if backup is not supported or fails (e.g. API not available).
    """
    definition = None
    ot = object_type

    try:
        if ot in ("jobs", "job"):
            job = client.jobs.get(job_id=int(object_id))
            definition = job.settings.as_dict() if hasattr(job, "settings") and hasattr(job.settings, "as_dict") else (job.as_dict() if hasattr(job, "as_dict") else {})
        elif ot in ("pipelines", "pipeline"):
            pipeline = client.pipelines.get(pipeline_id=object_id)
            definition = pipeline.spec.as_dict() if hasattr(pipeline, "spec") and hasattr(pipeline.spec, "as_dict") else (pipeline.as_dict() if hasattr(pipeline, "as_dict") else {})
        elif ot == "apps":
            app = client.apps.get(name=object_id)
            definition = app.as_dict() if hasattr(app, "as_dict") else {}
        elif ot == "mlflowExperiments":
            exp = client.experiments.get_experiment(experiment_id=object_id)
            definition = exp.as_dict() if hasattr(exp, "as_dict") else {}
        elif ot == "monitors":
            if hasattr(client, "quality_monitors"):
                monitor = client.quality_monitors.get(table_name=object_id)
                definition = monitor.as_dict() if hasattr(monitor, "as_dict") else {}
            else:
                definition = {"table_name": object_id, "note": "Lakehouse Monitoring API not available"}
        elif ot == "cluster":
            cluster = client.clusters.get(cluster_id=object_id)
            definition = cluster.as_dict() if hasattr(cluster, "as_dict") else {}
        elif ot == "alert":
            alert = client.alerts.get(id=object_id)
            definition = alert.as_dict() if hasattr(alert, "as_dict") else {}
        elif ot == "warehouse":
            warehouse = client.warehouses.get(id=object_id)
            definition = warehouse.as_dict() if hasattr(warehouse, "as_dict") else {}
        elif ot == "clusterPolicy":
            policy = client.cluster_policies.get(policy_id=object_id)
            definition = policy.as_dict() if hasattr(policy, "as_dict") else {}
        elif ot == "instancePool":
            pool = client.instance_pools.get(instance_pool_id=object_id)
            definition = pool.as_dict() if hasattr(pool, "as_dict") else {}
        elif ot == "servingEndpoint":
            endpoint = client.serving_endpoints.get(name=object_id)
            definition = endpoint.as_dict() if hasattr(endpoint, "as_dict") else {}
        elif ot == "secretScope":
            definition = {"scope_name": object_id}
        elif ot == "dashboard":
            try:
                if hasattr(client, "lakeview"):
                    dashboard = client.lakeview.get(dashboard_id=object_id)
                else:
                    dashboard = client.dashboards.get(dashboard_id=object_id)
                definition = dashboard.as_dict() if hasattr(dashboard, "as_dict") else {}
            except Exception:
                dashboard = client.dashboards.get(dashboard_id=object_id)
                definition = dashboard.as_dict() if hasattr(dashboard, "as_dict") else {}
        elif ot == "lakeview_dashboard":
            if hasattr(client, "lakeview"):
                dashboard = client.lakeview.get(dashboard_id=object_id)
                definition = dashboard.as_dict() if hasattr(dashboard, "as_dict") else {}
            else:
                definition = {"dashboard_id": object_id, "note": "Lakeview API not available"}
        elif ot == "query":
            query = client.queries.get(id=object_id)
            definition = query.as_dict() if hasattr(query, "as_dict") else {}
        elif ot == "catalog":
            cat = client.catalogs.get(name=object_id)
            definition = cat.as_dict() if hasattr(cat, "as_dict") else {}
        elif ot == "schema":
            schema_obj = client.schemas.get(full_name=object_id)
            definition = schema_obj.as_dict() if hasattr(schema_obj, "as_dict") else {}
        elif ot == "table":
            table = client.tables.get(full_name=object_id)
            definition = table.as_dict() if hasattr(table, "as_dict") else {}
        elif ot == "volume":
            volume = client.volumes.read(name=object_id)
            definition = volume.as_dict() if hasattr(volume, "as_dict") else {}
        elif ot == "connection":
            connection = client.connections.get(name=object_id)
            definition = connection.as_dict() if hasattr(connection, "as_dict") else {}
        elif ot == "function":
            function = client.functions.get(name=object_id)
            definition = function.as_dict() if hasattr(function, "as_dict") else {}
    except Exception:
        raise

    if definition is not None:
        return json.dumps(definition) if not isinstance(definition, str) else definition
    return None


def _get_perm(perm: Any, attr: str) -> Any:
    """Get attribute from permission row or dict (Spark Row / dict from governance table)."""
    if perm is None:
        return None
    if hasattr(perm, attr):
        return getattr(perm, attr)
    if isinstance(perm, dict):
        return perm.get(attr) or perm.get("principal_email" if attr == "principal_email" else attr)
    return None


# Object type categories for revert_permissions routing
_UC_OBJECT_TYPES = {
    "catalog", "schema", "table", "volume", "function", "connection",
    "externalLocation", "storageCredential", "share", "recipient",
    "provider", "metastore", "ucRegisteredModel", "registedModel",
    "vectorIndex", "featureTable", "monitors", "external_location",
}
_SECRET_SCOPE_TYPES = {"secretScope"}
_WORKSPACE_TYPE_MAP = {
    "notebook": "notebooks",
    "dashboard": "dbsql-dashboards",
    "lakeview_dashboard": "dashboards",
    "query": "queries",
    "folder": "directories",
    "directory": "directories",
    "file": "files",
    "repo": "repos",
    "project": "repos",
    "workspace_object": "directories",
    "alert": "alerts",
    "alerts": "alerts",
    "alertsv2": "alertsv2",
    "cluster": "clusters",
    "clusterPolicy": "cluster-policies",
    "instancePool": "instance-pools",
    "jobs": "jobs",
    "job": "jobs",
    "pipelines": "pipelines",
    "pipeline": "pipelines",
    "warehouse": "warehouses",
    "apps": "apps",
    "servingEndpoint": "serving-endpoints",
    "registeredModel": "registered-models",
    "mlflowExperiments": "experiments",
    "genieSpace": "genie",
    "dataroom": "genie",
}


def _revert_uc_permissions(
    client: Any, object_id: str, object_type: str, approved_perms: List[Any]
) -> Tuple[bool, Optional[str]]:
    """Revert Unity Catalog object permissions to pre-approved state using grants API."""
    from databricks.sdk.service.catalog import PermissionsChange, Privilege

    SECURABLE_TYPE_MAP = {
        "catalog": "catalog",
        "schema": "schema",
        "table": "table",
        "volume": "volume",
        "function": "function",
        "connection": "connection",
        "externalLocation": "external_location",
        "storageCredential": "storage_credential",
        "share": "share",
        "recipient": "recipient",
        "provider": "provider",
        "metastore": "metastore",
        "ucRegisteredModel": "function",
    }
    securable_type = SECURABLE_TYPE_MAP.get(object_type)
    if not securable_type:
        return (False, f"Unsupported UC object type: {object_type}")

    try:
        current_grants = client.grants.get(securable_type=securable_type, full_name=object_id)
    except Exception as e:
        return (False, f"Failed to get current UC grants: {e}")

    current_state = {}
    if getattr(current_grants, "privilege_assignments", None):
        for assignment in current_grants.privilege_assignments:
            principal = assignment.principal
            privileges = set()
            enum_map = {}
            for priv_enum in getattr(assignment, "privileges") or []:
                priv_name = getattr(priv_enum, "value", str(priv_enum))
                privileges.add(priv_name)
                enum_map[priv_name] = priv_enum
            current_state[principal] = {"privileges": privileges, "enums": enum_map}

    target_state = {}
    if approved_perms and len(approved_perms) > 0:
        first = approved_perms[0]
        perms_list = getattr(first, "permissions", None) or (first.get("permissions") if isinstance(first, dict) else None)
        if perms_list:
            for perm in perms_list:
                principal = _get_perm(perm, "principal_email")
                privilege = _get_perm(perm, "permission_level")
                if principal and privilege:
                    if principal not in target_state:
                        target_state[principal] = set()
                    target_state[principal].add(privilege)

    revoked_count = 0
    added_count = 0
    for principal, data in list(current_state.items()):
        current_privs = data["privileges"]
        target_privs = target_state.get(principal, set())
        privs_to_revoke = current_privs - target_privs - {"OWNER"}
        if privs_to_revoke:
            try:
                enums_to_revoke = [data["enums"][p] for p in privs_to_revoke if p in data["enums"]]
                if enums_to_revoke:
                    client.grants.update(
                        securable_type=securable_type,
                        full_name=object_id,
                        changes=[PermissionsChange(remove=enums_to_revoke, principal=principal)],
                    )
                    revoked_count += len(enums_to_revoke)
            except Exception:
                pass
    for principal, target_privs in target_state.items():
        current_privs = current_state.get(principal, {}).get("privileges", set())
        privs_to_add = target_privs - current_privs - {"OWNER", "ALL_PRIVILEGES"}
        if privs_to_add:
            try:
                enums_to_add = []
                for p in privs_to_add:
                    try:
                        enums_to_add.append(Privilege(p))
                    except ValueError:
                        pass
                if enums_to_add:
                    client.grants.update(
                        securable_type=securable_type,
                        full_name=object_id,
                        changes=[PermissionsChange(add=enums_to_add, principal=principal)],
                    )
                    added_count += len(enums_to_add)
            except Exception:
                pass

    return (True, f"Synced UC grants: revoked {revoked_count}, added {added_count} privileges")


def _revert_secret_scope_permissions(
    client: Any, scope_name: str, approved_perms: List[Any]
) -> Tuple[bool, Optional[str]]:
    """Revert secret scope ACL permissions to pre-approved state."""
    from databricks.sdk.service.workspace import AclPermission

    try:
        current_acls = list(client.secrets.list_acls(scope=scope_name))
    except Exception as e:
        return (False, f"Failed to get current secret scope ACLs: {e}")

    current_state = {}
    for acl in current_acls:
        principal = getattr(acl, "principal", None)
        permission = getattr(getattr(acl, "permission", None), "value", None) or str(getattr(acl, "permission", ""))
        if principal:
            current_state[principal] = permission

    target_state = {}
    if approved_perms and len(approved_perms) > 0:
        first = approved_perms[0]
        perms_list = getattr(first, "permissions", None) or (first.get("permissions") if isinstance(first, dict) else None)
        if perms_list:
            for perm in perms_list:
                principal = _get_perm(perm, "principal_email")
                permission = _get_perm(perm, "permission_level")
                if principal and permission:
                    target_state[principal] = permission

    updated_count = 0
    added_count = 0
    removed_count = 0
    for principal, target_perm in target_state.items():
        current_perm = current_state.get(principal)
        if current_perm != target_perm:
            try:
                client.secrets.put_acl(
                    scope=scope_name,
                    principal=principal,
                    permission=AclPermission(target_perm),
                )
                if current_perm:
                    updated_count += 1
                else:
                    added_count += 1
            except Exception:
                pass
    for principal in list(current_state.keys()):
        if principal not in target_state:
            try:
                client.secrets.delete_acl(scope=scope_name, principal=principal)
                removed_count += 1
            except Exception:
                pass

    return (True, f"Synced secret scope ACLs: updated {updated_count}, added {added_count}, removed {removed_count}")


def _revert_workspace_permissions(
    client: Any, object_id: str, object_type: str, approved_perms: List[Any], type_mapping: dict
) -> Tuple[bool, Optional[str]]:
    """Revert workspace object permissions to pre-approved state using permissions API."""
    permissions_api_type = type_mapping.get(object_type)
    if not permissions_api_type:
        return (False, f"Unsupported object type: {object_type} [_revert_workspace_permissions]")

    multiple_type_mapping = {
        "dbsql-dashboards": ["dbsql-dashboards", "dashboards"],
        "dashboards": ["dashboards", "dbsql-dashboards"],
        "alerts": ["alerts", "alertsv2"],
    }
    if permissions_api_type in multiple_type_mapping:
        for perm_type in multiple_type_mapping[permissions_api_type]:
            try:
                client.permissions.get(perm_type, object_id)
                permissions_api_type = perm_type
                break
            except Exception:
                continue

    acl_list = []
    if approved_perms and len(approved_perms) > 0:
        first = approved_perms[0]
        perms_list = getattr(first, "permissions", None) or (first.get("permissions") if isinstance(first, dict) else None)
        if perms_list:
            for perm in perms_list:
                principal = _get_perm(perm, "principal_email")
                level = _get_perm(perm, "permission_level")
                ptype = _get_perm(perm, "principal_type") or detect_principal_type(principal or "")
                if principal and level:
                    req = build_access_control_request(principal, level, ptype or "user")
                    if req:
                        acl_list.append(req)

    try:
        client.permissions.set(
            request_object_type=permissions_api_type,
            request_object_id=object_id,
            access_control_list=acl_list,
        )
        return (True, f"Set {len(acl_list)} ACLs on {permissions_api_type}/{object_id}")
    except ResourceDoesNotExist as e:
        return (True, f"Failed to set ACLs, because resource does not exist (SKIPPED) /{object_id}: {repr(e)}")
    except Exception as e:
        return (False, f"Failed to set ACLs on {permissions_api_type}/{object_id}: {repr(e)}")


def revert_permissions(
    client: Any,
    workspace_id: str,
    object_id: str,
    object_type: str,
    get_approved_permissions: Optional[Callable[[str, str], Any]] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Revert object permissions to pre-approved state. Routes to UC grants, secret scope ACLs, or workspace permissions API.
    get_approved_permissions(workspace_id, object_id) must return a list of row-like objects with .permissions (list of principal_email, permission_level, principal_type).
    """
    if get_approved_permissions is None:
        return (False, "No get_approved_permissions provided")
    try:
        approved_perms = get_approved_permissions(workspace_id, object_id)
    except Exception as e:
        return (False, f"Failed to fetch approved permissions: {e}")
    if approved_perms is None:
        return (False, f"No pre-approved permissions found for {object_id} of type {object_type}. Retrying later.")
    if not isinstance(approved_perms, list):
        approved_perms = list(approved_perms) if approved_perms else []

    if object_type in _UC_OBJECT_TYPES:
        return _revert_uc_permissions(client, object_id, object_type, approved_perms)
    if object_type in _SECRET_SCOPE_TYPES:
        return _revert_secret_scope_permissions(client, object_id, approved_perms)
    return _revert_workspace_permissions(client, object_id, object_type, approved_perms, _WORKSPACE_TYPE_MAP)


def revert_entitlement_change(
    client: Any,
    workspace_id: str,
    warehouse_id: Optional[str],
    object_name: str,
    object_type: str,
    action_name: str,
) -> Tuple[bool, Optional[str]]:
    """Revert entitlement change (e.g. any file grant/revoke)."""
    try:
        if object_type == "any_file_permissions":
            return _revert_any_file_grant_change(client, warehouse_id, object_name, object_type, action_name)
        return (False, f"Unsupported object type: {object_type}")
    except Exception as e:
        return (False, f"Failed to revert entitlement change for {object_type}:{object_name}: {e}")


def _revert_any_file_grant_change(
    client: Any,
    warehouse_id: Optional[str],
    object_name: str,
    object_type: str,
    action_name: str,
) -> Tuple[bool, Optional[str]]:
    """Revert any file grant/revoke via SQL (statement execution)."""
    if not warehouse_id:
        return (False, "warehouse_id required for any_file_permissions revert")
    try:
        permission = json.loads(object_name)
        securable_name = permission["securable"]["name"].strip("/ ").replace("_", " ")
        principal_name = permission["principal"]["name"]
        grant_name = permission["action"]["name"]
        if action_name == "grantPermission":
            statement = f"REVOKE {grant_name} ON {securable_name} FROM `{principal_name}`"
            client.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement)
            return (True, f"Reverted any file grant change for {object_type}: {statement}")
        if action_name == "revokePermission":
            statement = f"GRANT {grant_name} ON {securable_name} TO `{principal_name}`"
            client.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement)
            return (True, f"Reverted any file revoke change for {object_type}: {statement}")
        return (False, f"Unsupported action name: {action_name} for {object_type}")
    except Exception as e:
        return (False, f"Failed to revert entitlement change for {object_type}: {e}")


def _register_all_handlers() -> None:
    register_delete_handler("jobs", lambda c, id: delete_job(c, id))
    register_delete_handler("cluster", lambda c, id: delete_cluster(c, id))
    register_delete_handler("pipelines", lambda c, id: delete_pipeline(c, id))
    register_delete_handler("servingEndpoint", lambda c, id: delete_serving_endpoint(c, id))
    register_delete_handler("apps", lambda c, id: delete_app(c, id))
    register_delete_handler("mlflowExperiments", lambda c, id: delete_mlflow_experiment(c, id))
    register_delete_handler("monitors", lambda c, id: delete_monitor(c, id))
    register_delete_handler("alert", lambda c, id: delete_alert(c, id))
    register_delete_handler("warehouse", lambda c, id: delete_warehouse(c, id))
    register_delete_handler("clusterPolicy", lambda c, id: delete_cluster_policy(c, id))
    register_delete_handler("instancePool", lambda c, id: delete_instance_pool(c, id))
    register_delete_handler("secretScope", lambda c, id: delete_secret_scope(c, id))
    register_delete_handler("dashboard", lambda c, id: delete_dashboard(c, id))
    register_delete_handler("lakeview_dashboard", lambda c, id: delete_lakeview_dashboard(c, id))
    register_delete_handler("query", lambda c, id: delete_query(c, id))
    register_delete_handler("schema", lambda c, id: delete_uc_schema(c, id))
    register_delete_handler("table", lambda c, id: delete_uc_table(c, id))
    register_delete_handler("volume", lambda c, id: delete_uc_volume(c, id))
    register_delete_handler("catalog", lambda c, id: delete_uc_catalog(c, id))
    register_delete_handler("connection", lambda c, id: delete_uc_connection(c, id))
    register_delete_handler("function", lambda c, id: delete_uc_function(c, id))


_register_all_handlers()


def execute_remediation(
    client: Any,
    warehouse_id: Optional[str],
    violation: dict,
    dry_run: bool = False,
    enabled_object_types: Optional[Set[str]] = None,
    get_approved_permissions: Optional[Callable[[str, str], Any]] = None,
) -> Tuple[str, str, Optional[str], Optional[str]]:
    """
    Execute remediation for one violation. Returns (status, details, error, backup_definition) for parity with mx_scripts.
    If enabled_object_types is set, skip if violation object_type not in set.
    get_approved_permissions(workspace_id, object_id) is required for REVERT_PERMISSION (e.g. query governance table).
    """
    object_type = violation.get("object_type", "")
    object_id = violation.get("object_id", "")
    object_name = violation.get("object_name", "")
    workspace_id = violation.get("workspace_id", "")
    action_name = violation.get("action_name", "")
    remediation_action = violation.get("remediation_action", "")

    if enabled_object_types and object_type not in enabled_object_types:
        return ("SKIPPED", "Object type not in enabled_object_types", None, None)

    backup_definition = None

    if dry_run:
        return ("SUCCESS", f"DRY RUN: {remediation_action}", None, None)

    try:
        if remediation_action == "DELETE_RESOURCE":
            try:
                backup_definition = get_resource_definition(client, object_type, object_id)
            except ResourceDoesNotExist:
                error = f"Resource does not exist: {object_type}:{object_id}"
                return ("SKIPPED", f"Skipped: {error}", error, None)
            except Exception as e:
                error = f"Failed to backup {object_type}:{object_id}: {repr(e)}"
                return ("FAILED", f"Aborted: {error}", error, None)

            if not backup_definition:
                error = f"Backup failed for {object_type}:{object_id}. Aborting deletion."
                return ("FAILED", f"Aborted: {error}", error, None)

            handler = get_delete_handler(object_type)
            if not handler:
                return ("FAILED", f"Object type '{object_type}' is not supported for deletion", f"No delete handler for object_type={object_type}", backup_definition)
            success, error = handler(client, object_id)
            if success:
                details = f"Deleted {object_type}: {object_name} (ID: {object_id})"
                return ("SUCCESS", details, None, backup_definition)
            details = f"Failed to delete {object_type}: {object_name} (ID: {object_id})"
            return ("FAILED", details, error, backup_definition)

        if remediation_action == "REVERT_PERMISSION":
            success, error = revert_permissions(
                client, workspace_id, object_id, object_type, get_approved_permissions=get_approved_permissions
            )
            if success:
                if error and "ResourceDoesNotExist" in error:
                    details = f"Reverted permissions for {object_type}: {object_name}, is SKIPPED [ResourceDoesNotExist]"
                else:
                    details = f"Reverted permissions for {object_type}: {object_name}"
                return ("SUCCESS", details, None, None)
            details = f"Failed to revert permissions for {object_type}: {object_name}"
            return ("FAILED", details, error, None)

        if remediation_action == "ALERT_ENTITLEMENT_CHANGE":
            details = (
                f"Entitlement change reported for security team review: {object_type} '{object_name}' (ID: {object_id}) "
                f"changed by {violation.get('user_email', 'Unknown')}"
            )
            return ("SUCCESS", details, None, None)

        if remediation_action in ("REPORT_DELETION", "REPORT_SECURITY_TEAM"):
            details = (
                f"Deletion reported for security team review: {object_type} '{object_name}' (ID: {object_id}) "
                f"deleted by {violation.get('user_email', 'Unknown')}"
            )
            return ("SUCCESS", details, None, None)

        if remediation_action == "REVERT_ENTITLEMENT_CHANGE":
            success, error = revert_entitlement_change(
                client, workspace_id, warehouse_id, object_name, object_type, action_name
            )
            if success:
                return ("SUCCESS", f"Reverted entitlement change for {object_type}: {object_name}", None, None)
            return ("FAILED", f"Failed to revert entitlement change for {object_type}: {object_name}", error, None)

        if remediation_action == "REPORT_OBJECT_UPDATE":
            details = (
                f"Object update reported for security team review: {object_type} '{object_name}' (ID: {object_id}) "
                f"updated by {violation.get('user_email', 'Unknown')}"
            )
            return ("SUCCESS", details, None, None)

        error = f"Unknown remediation action: {remediation_action}"
        return ("FAILED", error, error, None)

    except Exception as e:
        error = str(e)
        details = f"Exception during remediation: {error}"
        return ("FAILED", details, error, backup_definition)
