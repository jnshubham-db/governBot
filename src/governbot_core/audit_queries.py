"""
Build dynamic SQL for audit log queries from governance filters.
Used by watcher (violations) and sync (approved user changes).
"""
from typing import Any, List, Optional


def _apply_filter_condition(f: Any, workspace_ids_str: str) -> str:
    """Apply service-specific exclusions to a filter condition. Returns full condition SQL."""
    condition = f"service_name='{f.service_name}' AND action_name='{f.action_name}'"
    if f.service_name == "clusters" and f.action_name in ("create", "createResult"):
        condition = f"""({condition}) AND
                    NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
                    AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
                    AND NOT (
                        request_params.kind='SERVERLESS_SQL_WAREHOUSE' AND request_params.cluster_creator='SQL_SERVICE'
                        OR request_params.kind='SERVERLESS_PREVIEW' AND request_params.cluster_creator='COMPUTE_GATEWAY_LAUNCHER'
                        OR request_params.kind='SERVERLESS_REPL_VM' AND request_params.cluster_creator='REPL_LAUNCHER'
                        )"""
    elif f.service_name == "clusters" and f.action_name == "delete":
        condition = f"""({condition} AND
                    NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
                    AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
                    )"""
    elif f.service_name == "clusters" and f.action_name == "changeClusterAcl":
        condition = f"""({condition}) AND request_params.resourceId IN 
                    (select distinct cluster_id from system.compute.clusters where cluster_source IN ('API','UI') 
                    {f'''and workspace_id IN ('{workspace_ids_str}')''' if workspace_ids_str else ''})
                    """
    elif f.service_name == "jobs" and f.action_name == "changeJobAcl":
        condition = f"""({condition})
                        AND NOT (NVL(request_params.aclPermissionSet,'x') ='Owner' AND NVL(USER_AGENT,'x')='AzureDataFactory')
                        """
    else:
        condition = f"({condition})"
    return condition


def build_audit_query_from_filters(
    filters: List[Any],
    workspace_ids_str: str,
    lookback_hours: int,
    identity_filter_str: str,
    is_permission_change: bool = False,
    is_delete_event: bool = False,
    is_entitlement_change: bool = False,
    extra_where_sql: str = "",
) -> Optional[str]:
    """
    Build SQL query for audit log from governance filters (watcher: find violations).
    identity_filter_str: comma-separated approved identities to EXCLUDE (NOT IN).
    extra_where_sql: optional AND clause from env/config (e.g. excluded user IDs).
    """
    if not filters:
        return None
    object_id_cases = []
    object_name_cases = []
    object_type_cases = []
    remediation_cases = []
    action_conditions = []
    service_names = []
    all_acls = []
    for f in filters:
        condition = f"service_name='{f.service_name}' AND action_name='{f.action_name}'"
        service_names.append(f.service_name)
        if is_permission_change:
            all_acls.append(f.action_name)
        object_id_expr = f"COALESCE({f.object_id_expr}, 'unknown')"
        object_name_expr = f"COALESCE({f.object_name_expr}, 'unknown')"
        object_id_cases.append(f"WHEN {condition} THEN {object_id_expr}")
        object_name_cases.append(f"WHEN {condition} THEN {object_name_expr}")
        object_type_str = (f.object_type or "").strip() or "unknown"
        is_dynamic = (
            object_type_str.upper().startswith("CASE")
            or "request_params" in object_type_str.lower()
            or object_type_str.upper().startswith("LOWER(")
            or object_type_str.upper().startswith("UPPER(")
            or object_type_str.upper().startswith("COALESCE(")
        )
        if is_dynamic:
            object_type_cases.append(f"WHEN {condition} THEN COALESCE({object_type_str}, 'unknown')")
        else:
            object_type_cases.append(f"WHEN {condition} THEN '{object_type_str}'")
        remediation_cases.append(f"WHEN {condition} THEN '{f.remediation_action}'")
        action_conditions.append(f"({_apply_filter_condition(f, workspace_ids_str)})")
    object_id_sql = "CASE \n            " + "\n            ".join(object_id_cases) + "\n            ELSE 'unknown'\n        END"
    object_name_sql = "CASE \n            " + "\n            ".join(object_name_cases) + "\n            ELSE 'unknown'\n        END"
    object_type_sql = "CASE \n            " + "\n            ".join(object_type_cases) + "\n            ELSE 'unknown'\n        END"
    remediation_sql = "CASE \n            " + "\n            ".join(remediation_cases) + "\n            ELSE 'ATTENTION_REQUIRED'\n        END"
    action_filter_sql = " \n\t\t\tOR ".join(action_conditions)
    excl_services = ",".join(f"'{s}'" for s in set(service_names))
    excl_acls = ",".join(f"'{a}'" for a in set(all_acls))
    if is_delete_event:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike '%delete%' AND NOT SERVICE_NAME IN ({excl_services}))"
    elif is_permission_change:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike 'change%Acl' AND NOT ACTION_NAME IN ({excl_acls}))"
    elif is_entitlement_change:
        default_filter = ""
    else:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ILIKE '%create%' AND NOT SERVICE_NAME IN ({excl_services}))"
    identity_filter = f"NOT user_identity.email IN ('{identity_filter_str}')" if identity_filter_str else "1=1"
    query = f"""
    SELECT
        event_id,
        workspace_id,
        event_time,
        event_date,
        service_name,
        action_name,
        user_identity.email as user_email,
        {str(is_permission_change).lower()} AS is_permission_change,
        {str(is_delete_event).lower()} AS is_delete_event,
        {str(is_entitlement_change).lower()} AS is_entitlement_change,
        {object_id_sql} as object_id,
        {object_name_sql} as object_name,
        {object_type_sql} as object_type,
        {remediation_sql} as remediation_action,
        request_params,
        response,
        request_id
    FROM system.access.audit A
    WHERE EVENT_TIME >= DATE_TRUNC('HOUR',CURRENT_TIMESTAMP()) - INTERVAL {lookback_hours + 1} HOUR
        AND EVENT_TIME < DATE_TRUNC('HOUR',CURRENT_TIMESTAMP()) + INTERVAL 1 SECOND
        {f"AND workspace_id IN ('{workspace_ids_str}')" if workspace_ids_str else ""}
        AND user_identity.email IS NOT NULL
        AND NVL(user_identity.email,'x') <> 'System-User'
        AND response.status_code IN (200, 201, 202, 203, 204, 205, 206, 207, 208)
        AND ({action_filter_sql}{default_filter})
        AND {identity_filter}
        {extra_where_sql}
    ORDER BY EVENT_TIME DESC
    """
    return query


def build_approved_user_query(
    filters: List[Any],
    workspace_ids_str: str,
    lookback_hours: int,
    approved_identities_str: str,
    is_permission_change: bool = False,
    is_delete_event: bool = False,
    is_entitlement_change: bool = False,
    extra_where_sql: str = "",
) -> Optional[str]:
    """
    Build SQL query for audit log for APPROVED users (sync: find changes to sync to baseline).
    approved_identities_str: comma-separated approved identities to INCLUDE (only these users).
    """
    if not filters or not approved_identities_str:
        return None
    object_id_cases = []
    object_name_cases = []
    object_type_cases = []
    action_conditions = []
    service_names = []
    all_acls = []
    for f in filters:
        condition = f"service_name='{f.service_name}' AND action_name='{f.action_name}'"
        service_names.append(f.service_name)
        if is_permission_change:
            all_acls.append(f.action_name)
        object_id_expr = f"COALESCE({f.object_id_expr}, 'unknown')"
        object_name_expr = f"COALESCE({f.object_name_expr}, 'unknown')"
        object_id_cases.append(f"WHEN {condition} THEN {object_id_expr}")
        object_name_cases.append(f"WHEN {condition} THEN {object_name_expr}")
        object_type_str = (f.object_type or "").strip() or "unknown"
        is_dynamic = (
            object_type_str.upper().startswith("CASE")
            or "request_params" in object_type_str.lower()
            or object_type_str.upper().startswith("LOWER(")
            or object_type_str.upper().startswith("UPPER(")
            or object_type_str.upper().startswith("COALESCE(")
        )
        if is_dynamic:
            object_type_cases.append(f"WHEN {condition} THEN COALESCE({object_type_str}, 'unknown')")
        else:
            object_type_cases.append(f"WHEN {condition} THEN '{object_type_str}'")
        action_conditions.append(f"({_apply_filter_condition(f, workspace_ids_str)})")
    object_id_sql = "CASE \n            " + "\n            ".join(object_id_cases) + "\n            ELSE 'unknown'\n        END"
    object_name_sql = "CASE \n            " + "\n            ".join(object_name_cases) + "\n            ELSE 'unknown'\n        END"
    object_type_sql = "CASE \n            " + "\n            ".join(object_type_cases) + "\n            ELSE 'unknown'\n        END"
    action_filter_sql = " \n\t\t\tOR ".join(action_conditions)
    excl_services = ",".join(f"'{s}'" for s in set(service_names))
    excl_acls = ",".join(f"'{a}'" for a in set(all_acls))
    if is_delete_event:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike '%delete%' AND NOT SERVICE_NAME IN ({excl_services}))"
    elif is_permission_change:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike 'change%Acl' AND NOT ACTION_NAME IN ({excl_acls}))"
    elif is_entitlement_change:
        default_filter = ""
    else:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ILIKE '%create%' AND NOT SERVICE_NAME IN ({excl_services}))"
    identity_filter = f"user_identity.email IN ('{approved_identities_str}')"
    query = f"""
    SELECT
        event_id,
        workspace_id,
        event_time,
        event_date,
        service_name,
        action_name,
        user_identity.email as user_email,
        {object_id_sql} as object_id,
        {object_name_sql} as object_name,
        {object_type_sql} as object_type,
        request_params,
        response,
        request_id
    FROM system.access.audit A
    WHERE EVENT_TIME >= DATE_TRUNC('HOUR',CURRENT_TIMESTAMP()) - INTERVAL {lookback_hours + 1} HOUR
        AND EVENT_TIME < DATE_TRUNC('HOUR',CURRENT_TIMESTAMP()) + INTERVAL 1 SECOND
        {f"AND workspace_id IN ('{workspace_ids_str}')" if workspace_ids_str else ""}
        AND user_identity.email IS NOT NULL
        AND NVL(user_identity.email,'x') <> 'System-User'
        AND response.status_code IN (200, 201, 202, 203, 204, 205, 206, 207, 208)
        AND ({action_filter_sql}{default_filter})
        AND {identity_filter}
        {extra_where_sql}
    ORDER BY EVENT_TIME DESC
    """
    return query
