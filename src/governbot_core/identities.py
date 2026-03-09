"""
Identity helpers: expand_approved_actions, prepare_identity_records, load/parse identities, expand_group_members.
Uses constants.APPROVED_ACTION_GROUPS; no hardcoded groups.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from governbot_core.constants import APPROVED_ACTION_GROUPS
from governbot_core.constants import TABLE_PREAPPROVED_IDENTITIES, full_table_name


def expand_approved_actions(actions: List[str]) -> List[str]:
    """Expand group aliases in approved_actions to individual object types."""
    if not actions:
        return ["ALL"]
    expanded = []
    for action in actions:
        action_upper = action.upper()
        if action_upper in APPROVED_ACTION_GROUPS:
            expanded.extend(APPROVED_ACTION_GROUPS[action_upper])
        else:
            expanded.append(action)
    return list(set(expanded))


def prepare_identity_records(identities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepare identity records for insertion into governance_preapproved_identities."""
    records = []
    current_time = datetime.utcnow()
    for identity in identities:
        identity_name = identity["name"]
        identity_type = identity["type"]
        can_manage_resources = identity.get("can_manage_resources", True)
        can_manage_permissions = identity.get("can_manage_permissions", False)
        approved_actions_raw = identity.get("approved_actions", ["ALL"])
        approved_actions = expand_approved_actions(approved_actions_raw)
        if identity_type == "USER" and "@" in identity_name:
            display_name = identity_name.split("@")[0]
        else:
            display_name = identity_name
        records.append({
            "identity_name": identity_name,
            "identity_type": identity_type,
            "display_name": display_name,
            "can_manage_resources": can_manage_resources,
            "can_manage_permissions": can_manage_permissions,
            "approved_actions": approved_actions,
            "is_active": True,
            "created_at": current_time,
            "updated_at": current_time,
        })
    return records


def load_preapproved_identities(spark: Any, catalog: str, schema: str) -> List[Any]:
    """Load active rows from governance_preapproved_identities. Returns list of Row-like objects."""
    table = full_table_name(catalog, schema, TABLE_PREAPPROVED_IDENTITIES)
    df = spark.sql(f"""
        SELECT
            identity_name,
            identity_type,
            COALESCE(can_manage_resources, true) as can_manage_resources,
            COALESCE(can_manage_permissions, false) as can_manage_permissions,
            COALESCE(approved_actions, ARRAY('ALL')) as approved_actions
        FROM {table}
        WHERE is_active = true
    """)
    return df.collect()


def parse_identity_lists(rows: List[Any]) -> Dict[str, Any]:
    """
    Parse identity rows into resource/permission lists and identity_approved_actions.
    Returns dict with: resource_approved_users, resource_approved_service_principals, resource_approved_groups,
    permission_approved_users, permission_approved_service_principals, permission_approved_groups,
    identity_approved_actions.
    """
    resource_approved_users = []
    resource_approved_service_principals = []
    resource_approved_groups = []
    permission_approved_users = []
    permission_approved_service_principals = []
    permission_approved_groups = []
    identity_approved_actions: Dict[str, List[str]] = {}
    for row in rows:
        identity_name = row.identity_name
        identity_type = getattr(row, "identity_type", "USER")
        can_manage_resources = getattr(row, "can_manage_resources", True)
        can_manage_permissions = getattr(row, "can_manage_permissions", False)
        approved_actions = list(row.approved_actions) if getattr(row, "approved_actions", None) else ["ALL"]
        identity_approved_actions[identity_name] = approved_actions
        if identity_type == "USER":
            if can_manage_resources:
                resource_approved_users.append(identity_name)
            if can_manage_permissions:
                permission_approved_users.append(identity_name)
        elif identity_type == "SERVICE_PRINCIPAL":
            if can_manage_resources:
                resource_approved_service_principals.append(identity_name)
            if can_manage_permissions:
                permission_approved_service_principals.append(identity_name)
        elif identity_type == "GROUP":
            if can_manage_resources:
                resource_approved_groups.append(identity_name)
            if can_manage_permissions:
                permission_approved_groups.append(identity_name)
    return {
        "resource_approved_users": resource_approved_users,
        "resource_approved_service_principals": resource_approved_service_principals,
        "resource_approved_groups": resource_approved_groups,
        "permission_approved_users": permission_approved_users,
        "permission_approved_service_principals": permission_approved_service_principals,
        "permission_approved_groups": permission_approved_groups,
        "identity_approved_actions": identity_approved_actions,
    }


def resolve_member_identity(client: Any, member_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Resolve member ID to (identity_name, identity_type). identity_type is 'user' or 'sp'."""
    try:
        user = client.users.get(id=member_id)
        if user and getattr(user, "user_name", None):
            return (user.user_name, "user")
    except Exception:
        pass
    try:
        sp = client.service_principals.get(id=member_id)
        if sp and getattr(sp, "application_id", None):
            return (sp.application_id, "sp")
    except Exception:
        pass
    return (None, None)


def expand_group_members(
    group_names: List[str],
    client: Any,
    identity_approved_actions: Dict[str, List[str]],
) -> Tuple[List[str], List[str]]:
    """
    Expand group names to member users and service principals. Mutates identity_approved_actions to inherit
    approved_actions from group to members. Returns (expanded_users, expanded_sps).
    """
    expanded_users: List[str] = []
    expanded_sps: List[str] = []
    if not group_names:
        return expanded_users, expanded_sps
    for group_name in group_names:
        group_approved_actions = identity_approved_actions.get(group_name, ["ALL"])
        try:
            group_members = client.groups.list(filter=f"displayName eq '{group_name}'")
            for group in group_members:
                if getattr(group, "display_name", None) == group_name:
                    try:
                        members = client.groups.list(filter=f"id eq '{group.id}'")
                        for member_group in members:
                            if getattr(member_group, "members", None):
                                for member in member_group.members:
                                    member_id = getattr(member, "value", None)
                                    if not member_id:
                                        continue
                                    identity_name, identity_type = resolve_member_identity(client, member_id)
                                    if identity_name:
                                        if identity_name in identity_approved_actions:
                                            existing = identity_approved_actions[identity_name]
                                            if "ALL" not in existing:
                                                identity_approved_actions[identity_name] = list(
                                                    set(existing + group_approved_actions)
                                                )
                                        else:
                                            identity_approved_actions[identity_name] = group_approved_actions
                                        if identity_type == "user":
                                            expanded_users.append(identity_name)
                                        elif identity_type == "sp":
                                            expanded_sps.append(identity_name)
                    except Exception:
                        pass
                    break
        except Exception:
            pass
    return list(set(expanded_users)), list(set(expanded_sps))
