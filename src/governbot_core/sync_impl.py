"""
Sync implementation: permission fetching and entitlement/budget helpers for 04a_sync_approved_changes.
Import from governbot_core: from governbot_core import sync_impl; sync_impl.fetch_current_permissions(...)
"""
from __future__ import annotations
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from databricks.sdk import WorkspaceClient, AccountClient
except ImportError:
    WorkspaceClient = None
    AccountClient = None


def _detect_principal_type(principal: str) -> str:
    if not principal:
        return "user"
    uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    if re.match(uuid_pattern, principal):
        return "service_principal"
    if "@" in principal:
        return "user"
    return "group"


def get_workspace_permissions(
    client: Any, object_type: str, object_id: str
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    type_mapping = {
        "notebook": "notebooks",
        "dashboard": "dbsql-dashboards",
        "lakeview_dashboard": "dashboards",
        "query": "queries",
        "folder": "directories",
        "directory": "directories",
        "repo": "repos",
        "cluster": "clusters",
        "clusterPolicy": "cluster-policies",
        "instancePool": "instance-pools",
        "jobs": "jobs",
        "job": "jobs",
        "pipelines": "pipelines",
        "pipeline": "pipelines",
        "warehouse": "warehouses",
        "alert": "alerts",
        "alerts": "alerts",
        "alertsv2": "alertsv2",
        "apps": "apps",
        "servingEndpoint": "serving-endpoints",
        "vectorSearchEndpoint": "vector-search-endpoints",
        "mlflowExperiments": "experiments",
        "registeredModel": "registered-models",
        "genieSpace": "genie",
    }
    multiple_type_mapping = {
        "dbsql-dashboards": ["dbsql-dashboards", "dashboards"],
        "dashboards": ["dashboards", "dbsql-dashboards"],
        "alerts": ["alerts", "alertsv2"],
    }
    permissions_type = type_mapping.get(object_type)
    if not permissions_type:
        return [], f"Unsupported object type '{object_type}' for workspace permissions API"
    try:
        if permissions_type in multiple_type_mapping:
            for perm_type in multiple_type_mapping[permissions_type]:
                try:
                    permissions = client.permissions.get(perm_type, object_id)
                    break
                except Exception:
                    continue
            else:
                return [], "No permissions API succeeded"
        else:
            permissions = client.permissions.get(permissions_type, object_id)
        acl_list = []
        if permissions.access_control_list:
            for acl in permissions.access_control_list:
                principal_email = acl.user_name or acl.service_principal_name or acl.group_name
                principal_type = "user" if acl.user_name else ("service_principal" if acl.service_principal_name else "group")
                if principal_email and acl.all_permissions:
                    for perm in acl.all_permissions:
                        if perm.inherited:
                            continue
                        acl_list.append({
                            "principal_email": principal_email,
                            "principal_type": principal_type,
                            "permission_level": perm.permission_level.value,
                        })
        return acl_list, None
    except Exception as e:
        return [], f"Permission fetch failed for {permissions_type}/{object_id}: {type(e).__name__}: {str(e)}"


def get_uc_grants(client: Any, object_type: str, full_name: str) -> Tuple[List[Dict[str, str]], Optional[str]]:
    try:
        from databricks.sdk.service.catalog import SecurableType
        securable_type_map = {
            "catalog": SecurableType.CATALOG.value,
            "schema": SecurableType.SCHEMA.value,
            "table": SecurableType.TABLE.value,
            "volume": SecurableType.VOLUME.value,
            "function": SecurableType.FUNCTION.value,
            "connection": SecurableType.CONNECTION.value,
            "externalLocation": SecurableType.EXTERNAL_LOCATION.value,
            "storageCredential": SecurableType.STORAGE_CREDENTIAL.value,
            "share": SecurableType.SHARE.value,
            "recipient": SecurableType.RECIPIENT.value,
            "provider": SecurableType.PROVIDER.value,
            "metastore": SecurableType.METASTORE.value,
            "ucRegisteredModel": SecurableType.FUNCTION.value,
            "registeredModel": SecurableType.FUNCTION.value,
            "vectorIndex": SecurableType.TABLE.value,
            "featureTable": SecurableType.TABLE.value,
            "monitors": SecurableType.TABLE.value,
        }
        sec_type = securable_type_map.get(object_type)
        if not sec_type:
            return [], f"Unsupported object type '{object_type}' for UC grants API"
        grants = client.grants.get(securable_type=sec_type, full_name=full_name)
        acl_list = []
        if grants and grants.privilege_assignments:
            for assignment in grants.privilege_assignments:
                principal = getattr(assignment, "principal", None)
                if principal and assignment.privileges:
                    principal_type = _detect_principal_type(principal)
                    for privilege in assignment.privileges:
                        priv_name = privilege.value if hasattr(privilege, "value") else str(privilege)
                        acl_list.append({
                            "principal_email": principal,
                            "principal_type": principal_type,
                            "permission_level": priv_name,
                        })
        return acl_list, None
    except Exception as e:
        return [], f"UC grants fetch failed for {object_type}/{full_name}: {type(e).__name__}: {str(e)}"


def get_secret_scope_acls(client: Any, scope_name: str) -> Tuple[List[Dict[str, str]], Optional[str]]:
    try:
        acl_list = []
        for acl in client.secrets.list_acls(scope=scope_name):
            principal = getattr(acl, "principal", None)
            permission = acl.permission.value if hasattr(acl.permission, "value") else str(acl.permission)
            if principal:
                acl_list.append({
                    "principal_email": principal,
                    "principal_type": _detect_principal_type(principal),
                    "permission_level": permission,
                })
        return acl_list, None
    except Exception as e:
        return [], f"Secret scope ACLs fetch failed for scope '{scope_name}': {type(e).__name__}: {str(e)}"


def get_genie_space_permissions(client: Any, space_id: str) -> Tuple[List[Dict[str, str]], Optional[str]]:
    try:
        acl = client.permissions.get(request_object_type="genie", request_object_id=space_id)
        permissions = []
        if acl and acl.access_control_list:
            for ace in acl.access_control_list:
                principal_email = ace.user_name or ace.service_principal_name or ace.group_name
                principal_type = "user" if ace.user_name else ("service_principal" if ace.service_principal_name else "group")
                if principal_email and ace.all_permissions:
                    for perm in ace.all_permissions:
                        if perm.inherited:
                            continue
                        permissions.append({
                            "principal_email": principal_email,
                            "principal_type": principal_type,
                            "permission_level": perm.permission_level.value if hasattr(perm.permission_level, "value") else str(perm.permission_level),
                        })
        return permissions, None
    except Exception as e:
        return [], f"Genie space permission fetch failed for space '{space_id}': {type(e).__name__}: {str(e)}"


def fetch_current_permissions(
    client: Any, object_type: str, object_id: str
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    uc_types = {
        "catalog", "schema", "table", "volume", "function", "connection",
        "externalLocation", "storageCredential", "share", "recipient",
        "provider", "metastore", "ucRegisteredModel", "registeredModel",
        "vectorIndex", "featureTable", "monitors",
    }
    if object_type in uc_types:
        return get_uc_grants(client, object_type, object_id)
    if object_type == "secretScope":
        return get_secret_scope_acls(client, object_id)
    if object_type == "genieSpace":
        return get_genie_space_permissions(client, object_id)
    return get_workspace_permissions(client, object_type, object_id)


def sql_executor(
    spark: Any, catalog: str, schema: str, client: Any, workspace_id: str, sql_query: str
) -> List[Dict[str, Any]]:
    table = f"{catalog}.{schema}.governance_config_workspaces"
    warehouse_id = spark.sql(f"SELECT warehouse_id FROM {table} WHERE workspace_id = '{workspace_id}'").collect()[0][0]
    results = client.statement_execute.execute_statement(warehouse_id=warehouse_id, statement=sql_query)
    data = []
    columns = sorted(results.manifest.schema.columns, key=lambda x: x.position)
    for row in results.result.data_array:
        row_data = {column.name: row[i] for i, column in enumerate(columns)}
        data.append(row_data)
    return data


def fetch_any_file_permissions(
    spark: Any, catalog: str, schema: str, client: Any, workspace_id: str
) -> Tuple[Dict[str, Any], Optional[str]]:
    try:
        grants = sql_executor(spark, catalog, schema, client, workspace_id, "SHOW GRANTS ON ANY FILE")
        return {"grants": grants}, None
    except Exception as e:
        return {}, f"Error discovering ANY_FILES: {str(e)}"


def _as_dict_or_str(x: Any) -> Any:
    if hasattr(x, "as_dict"):
        return x.as_dict()
    return str(x)


def fetch_group_details(client: Any, group_id: str) -> Tuple[Optional[str], Optional[Dict], Optional[str]]:
    """Returns (group_name, metadata, error). Uses groups_v2 if available."""
    try:
        if hasattr(client, "groups_v2"):
            group = client.groups_v2.get(id=group_id)
        else:
            group = client.groups.get(id=group_id)
        name = getattr(group, "display_name", None) or ""
        metadata = {}
        if getattr(group, "entitlements", None):
            metadata["entitlements"] = [_as_dict_or_str(e) for e in group.entitlements]
        if getattr(group, "members", None):
            metadata["members"] = [_as_dict_or_str(m) for m in group.members]
        return name, metadata, None
    except Exception as e:
        return None, None, f"Group fetch failed for id '{group_id}': {type(e).__name__}: {str(e)}"


def fetch_user_details(client: Any, user_id: str) -> Tuple[Optional[str], Optional[Dict], Optional[str]]:
    """Returns (user_name, metadata, error). Uses users or users_v2."""
    try:
        if hasattr(client, "users_v2"):
            user = client.users_v2.get(id=user_id)
        else:
            user = client.users.get(id=user_id)
        name = getattr(user, "user_name", None) or ""
        metadata = {}
        if getattr(user, "entitlements", None):
            metadata["entitlements"] = [_as_dict_or_str(e) for e in user.entitlements]
        if getattr(user, "groups", None):
            metadata["groups"] = [getattr(g, "display_name", None) or str(g) for g in user.groups]
        return name, metadata, None
    except Exception as e:
        return None, None, f"User fetch failed for id '{user_id}': {type(e).__name__}: {str(e)}"


def fetch_service_principal_details(client: Any, sp_id: str) -> Tuple[Optional[str], Optional[Dict], Optional[str]]:
    """Returns (application_id, metadata, error). Uses service_principals or service_principals_v2."""
    try:
        if hasattr(client, "service_principals_v2"):
            sp = client.service_principals_v2.get(id=sp_id)
        else:
            sp = client.service_principals.get(id=sp_id)
        name = getattr(sp, "application_id", None) or ""
        metadata = {}
        if getattr(sp, "entitlements", None):
            metadata["entitlements"] = [_as_dict_or_str(e) for e in sp.entitlements]
        return name, metadata, None
    except Exception as e:
        return None, None, f"Service principal fetch failed for id '{sp_id}': {type(e).__name__}: {str(e)}"


def discover_serverless_budget_policies(
    client: Any, account_id: str, workspace_ids: List[str],
) -> List[Dict[str, Any]]:
    """Returns list of preapproved-object-like dicts (one row per policy per bound workspace)."""
    if AccountClient is None or client is None:
        return []
    discovered = []
    try:
        budget_policies = client.budget_policy.list()
        for policy in budget_policies:
            binding = policy.binding_workspace_ids or []
            for wid in workspace_ids:
                if wid not in binding:
                    continue
                object_id = policy.policy_id
                object_name = policy.policy_name
                policy_name = f"accounts/{account_id}/budgetPolicies/{object_id}/ruleSets/default"
                try:
                    rule_set = client.access_control.get_rule_set(name=policy_name, etag="")
                except Exception:
                    rule_set = None
                permissions = []
                if rule_set and getattr(rule_set, "grant_rules", None):
                    for acl in rule_set.grant_rules:
                        permission_level = getattr(acl, "role", "CAN_USE")
                        for principal in getattr(acl, "principals", []) or []:
                            parts = (principal or "").split("/", 1)
                            principal_type = parts[0] if len(parts) > 1 else "user"
                            principal_email = parts[1] if len(parts) > 1 else principal
                            permissions.append({
                                "principal_email": principal_email,
                                "principal_type": principal_type,
                                "permission_level": permission_level,
                            })
                discovered.append({
                    "object_id": object_id,
                    "workspace_id": wid,
                    "object_type": "serverless_budget_policy",
                    "object_name": object_name,
                    "object_path": None,
                    "owner_email": "unknown",
                    "permissions": permissions,
                    "metadata": {"binding_workspace_ids": str(binding), "etag": str(getattr(rule_set, "etag", "") if rule_set else "")},
                    "is_active": True,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                })
    except Exception as e:
        print(f"✗ Error discovering Serverless Budget Policies: {str(e)}")
    return discovered
