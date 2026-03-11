"""
Permission helpers: detect_principal_type, build_access_control_request.
Used by remediation (revert) and identity handling.
"""
import re
from typing import Any, Optional

def detect_principal_type(principal: str) -> str:
    """
    Detect principal type from identifier.
    Returns: 'user', 'service_principal', or 'group'.
    """
    if not principal:
        return "user"
    uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    if re.match(uuid_pattern, principal):
        return "service_principal"
    if "@" in principal:
        return "user"
    return "group"


def build_access_control_request(
    principal: str, permission_level: str, principal_type: str
) -> Optional[Any]:
    """
    Build an AccessControlRequest for the given principal (for revert_permissions).
    Returns None if permission_level is invalid.
    """
    try:
        from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    except ImportError:
        return None
    try:
        level = PermissionLevel(permission_level)
    except ValueError:
        return None
    if principal_type == "user":
        return AccessControlRequest(user_name=principal, permission_level=level)
    if principal_type == "service_principal":
        return AccessControlRequest(service_principal_name=principal, permission_level=level)
    return AccessControlRequest(group_name=principal, permission_level=level)

# Resolve missing object_type/object_name for workspace admin filters using workspace client
def resolve_identity_by_id(client: Any, identity_id: str) -> tuple[str, str]:
    """
    Resolve identity id to (object_type, object_name).
    Returns (None, None) when lookup fails.
    """
    try:
        group = client.groups_v2.get(id=identity_id)
        if group and group.display_name:
            return ("groups", group.display_name)
    except Exception:
        pass
    
    try:
        user = client.users_v2.get(id=identity_id)
        if user and user.user_name:
            return ("users", user.user_name)
    except Exception:
        pass
    
    try:
        sp = client.service_principals_v2.get(id=identity_id)
        if sp and sp.application_id:
            return ("service_principal", sp.application_id)
    except Exception:
        pass
    
    return (None, None)
