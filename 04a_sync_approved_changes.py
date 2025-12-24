# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Pre-Approved User Changes
# MAGIC
# MAGIC This notebook monitors audit logs for changes made by **pre-approved users** and 
# MAGIC syncs those changes back to the `governance_preapproved_objects` table.
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Track new resources created by approved users and add them to pre-approved objects
# MAGIC - Track permission changes made by approved users and update the permissions in pre-approved objects
# MAGIC - Ensure the governance baseline stays current with authorized changes
# MAGIC
# MAGIC **Note:** This is the inverse of the watcher notebook - instead of flagging violations,
# MAGIC we're capturing authorized changes to keep the pre-approved objects table up to date.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")
dbutils.widgets.text("lookback_hours", "24", "Lookback Hours for Audit Logs")
dbutils.widgets.dropdown("sync_creations", "Y", ["Y", "N"], "Sync New Creations")
dbutils.widgets.dropdown("sync_permissions", "Y", ["Y", "N"], "Sync Permission Changes")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))
sync_creations = dbutils.widgets.get("sync_creations") == "Y"
sync_permissions = dbutils.widgets.get("sync_permissions") == "Y"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Lookback Hours: {lookback_hours}")
print(f"Sync Creations: {sync_creations}")
print(f"Sync Permissions: {sync_permissions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any, Tuple, Optional
import json
import requests

# Initialize workspace client
client = WorkspaceClient()
print("✓ Workspace client initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permission Fetching Helper Functions

# COMMAND ----------

import re

def _detect_principal_type(principal: str) -> str:
    """
    Detect principal type based on the principal identifier.
    
    Rules:
    - Users: contain '@' in email format (e.g., user@company.com)
    - Service Principals: follow UUID/GUID pattern (e.g., d118594b-a1db-41b2-a6e2-a201377c2aec)
    - Groups: everything else (e.g., "admins", "data_engineers", "users")
    
    Returns: 'user', 'service_principal', or 'group'
    """
    if not principal:
        return 'user'
    
    # UUID/GUID pattern for service principals
    # Format: 8-4-4-4-12 hexadecimal characters
    uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    
    if re.match(uuid_pattern, principal):
        # Service principal (UUID format)
        return 'service_principal'
    elif '@' in principal:
        # User (email format)
        return 'user'
    else:
        # Group (e.g., "admins", "data_engineers")
        return 'group'


def get_workspace_permissions(client, object_type: str, object_id: str) -> List[Dict[str, str]]:
    """
    Get workspace-level permissions for an object using the permissions API.
    Returns list of {principal_email, principal_type, permission_level} dicts.
    
    Uses the same approach as 04_discover.py get_permissions_safe() function.
    """
    # Map object types to permissions API object types
    # Supported types: alerts, alertsv2, apps, authorization, clusters, cluster-policies,
    # dashboards, database-instances, database-projects, dbsql-dashboards, directories,
    # experiments, files, genie, instance-pools, jobs, notebooks, pipelines, queries,
    # registered-models, repos, serving-endpoints, warehouses, vector-search-endpoints
    type_mapping = {
        "notebook": "notebooks",
        "dashboard": "dbsql-dashboards",  # Legacy SQL dashboards
        "lakeview_dashboard": "dashboards",  # Lakeview (AI/BI) dashboards
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
        "apps": "apps",
        "servingEndpoint": "serving-endpoints",
        "vectorSearchEndpoint": "vector-search-endpoints",
        "mlflowExperiments": "experiments",
        "registeredModel": "registered-models",
        "genieSpace": "genie",
    }
    
    permissions_type = type_mapping.get(object_type)
    if not permissions_type:
        return []
    
    try:
        # Use positional arguments - same as 04_discover.py get_permissions_safe()
        permissions = client.permissions.get(permissions_type, object_id)
        acl_list = []
        
        if permissions.access_control_list:
            for acl in permissions.access_control_list:
                principal_email = None
                if acl.user_name:
                    principal_email = acl.user_name
                elif acl.service_principal_name:
                    principal_email = acl.service_principal_name
                elif acl.group_name:
                    # Also capture group permissions
                    principal_email = acl.group_name
                
                # Determine principal type for more accurate remediation
                principal_type = 'user'
                if acl.user_name:
                    principal_type = 'user'
                elif acl.service_principal_name:
                    principal_type = 'service_principal'
                elif acl.group_name:
                    principal_type = 'group'
                
                if principal_email and acl.all_permissions:
                    for perm in acl.all_permissions:
                        # Include ALL permissions (both inherited and direct)
                        acl_list.append({
                            'principal_email': principal_email,
                            'principal_type': principal_type,
                            'permission_level': perm.permission_level.value
                        })
        
        return acl_list
    except Exception as e:
        # Silently fail for permission errors - same as 04_discover.py
        print(f"  ⚠️  Permission fetch failed for {permissions_type}/{object_id}: {str(e)}")
        return []


def get_uc_grants(client, object_type: str, full_name: str) -> List[Dict[str, str]]:
    """
    Get Unity Catalog grants for a securable object using grants.get API.
    Returns list of {principal_email, principal_type, permission_level} dicts.
    
    Uses the same approach as 04_discover.py get_uc_grants_safe() function.
    """
    try:
        from databricks.sdk.service.catalog import SecurableType
        
        # Map object types to SecurableType enum - use .value for the API call (same as 04_discover.py)
        securable_type_map = {
            'catalog': SecurableType.CATALOG.value,
            'CATALOG': SecurableType.CATALOG.value,
            'schema': SecurableType.SCHEMA.value,
            'SCHEMA': SecurableType.SCHEMA.value,
            'table': SecurableType.TABLE.value,
            'TABLE': SecurableType.TABLE.value,
            'volume': SecurableType.VOLUME.value,
            'VOLUME': SecurableType.VOLUME.value,
            'function': SecurableType.FUNCTION.value,
            'FUNCTION': SecurableType.FUNCTION.value,
            'connection': SecurableType.CONNECTION.value,
            'CONNECTION': SecurableType.CONNECTION.value,
            'externalLocation': SecurableType.EXTERNAL_LOCATION.value,
            'EXTERNAL_LOCATION': SecurableType.EXTERNAL_LOCATION.value,
            'storageCredential': SecurableType.STORAGE_CREDENTIAL.value,
            'STORAGE_CREDENTIAL': SecurableType.STORAGE_CREDENTIAL.value,
            'share': SecurableType.SHARE.value,
            'SHARE': SecurableType.SHARE.value,
            'recipient': SecurableType.RECIPIENT.value,
            'RECIPIENT': SecurableType.RECIPIENT.value,
            'provider': SecurableType.PROVIDER.value,
            'PROVIDER': SecurableType.PROVIDER.value,
            'metastore': SecurableType.METASTORE.value,
            'METASTORE': SecurableType.METASTORE.value,
            'ucRegisteredModel': SecurableType.FUNCTION.value,  # UC models use FUNCTION type
            'REGISTERED_MODEL': SecurableType.FUNCTION.value,
            'registeredModel': SecurableType.FUNCTION.value,
            'vectorIndex': SecurableType.TABLE.value,
            'featureTable': SecurableType.TABLE.value,
            'monitors': SecurableType.TABLE.value,  # Monitors use table grants
        }
        
        sec_type = securable_type_map.get(object_type)
        if not sec_type:
            return []
        
        # Use grants.get with securable_type string value (same as 04_discover.py)
        grants = client.grants.get(securable_type=sec_type, full_name=full_name)
        
        acl_list = []
        
        if grants and grants.privilege_assignments:
            for assignment in grants.privilege_assignments:
                principal = assignment.principal if hasattr(assignment, 'principal') else None
                if principal and assignment.privileges:
                    # Determine principal type based on naming conventions
                    # UC grants don't explicitly tell us if it's user/group/sp
                    principal_type = _detect_principal_type(principal)
                    
                    for privilege in assignment.privileges:
                        # Handle privilege extraction - same as 04_discover.py
                        priv_name = privilege.privilege.value if hasattr(privilege.privilege, 'value') else str(privilege.privilege)
                        
                        acl_list.append({
                            'principal_email': principal,
                            'principal_type': principal_type,
                            'permission_level': priv_name
                        })
        
        return acl_list
        
    except Exception as e:
        # Silently fail for permission errors - same as 04_discover.py
        return []


def get_secret_scope_acls(client, scope_name: str) -> List[Dict[str, str]]:
    """
    Get secret scope ACLs using secrets.list_acls API.
    Returns list of {principal_email, principal_type, permission_level} dicts.
    
    Uses the same approach as 04_discover.py get_secret_acls_safe() function.
    """
    try:
        acl_list = []
        
        for acl in client.secrets.list_acls(scope=scope_name):
            principal = acl.principal if hasattr(acl, 'principal') else None
            permission = acl.permission.value if hasattr(acl, 'permission') and hasattr(acl.permission, 'value') else str(acl.permission)
            
            if principal:
                # Determine principal type
                principal_type = _detect_principal_type(principal)
                
                acl_list.append({
                    'principal_email': principal,
                    'principal_type': principal_type,
                    'permission_level': permission
                })
        
        return acl_list
    except Exception as e:
        # Silently fail for permission errors - same as 04_discover.py
        return []


def get_genie_space_permissions(client, space_id: str) -> List[Dict[str, str]]:
    """
    Get permissions for a Genie space using the permissions API.
    Returns list of {principal_email, principal_type, permission_level} dicts.
    
    Uses the same approach as 04_discover.py get_genie_space_permissions() function.
    """
    try:
        # Use keyword arguments - same as 04_discover.py
        acl = client.permissions.get(
            request_object_type="genie",
            request_object_id=space_id
        )
        
        permissions = []
        
        if acl and acl.access_control_list:
            for ace in acl.access_control_list:
                principal_email = None
                principal_type = 'user'
                
                if ace.user_name:
                    principal_email = ace.user_name
                    principal_type = 'user'
                elif ace.service_principal_name:
                    principal_email = ace.service_principal_name
                    principal_type = 'service_principal'
                elif ace.group_name:
                    principal_email = ace.group_name
                    principal_type = 'group'
                
                if principal_email and ace.all_permissions:
                    for perm in ace.all_permissions:
                        perm_level = perm.permission_level.value if hasattr(perm.permission_level, 'value') else str(perm.permission_level)
                        permissions.append({
                            'principal_email': principal_email,
                            'principal_type': principal_type,
                            'permission_level': perm_level
                        })
        
        return permissions
    except Exception as e:
        # Silently fail for permission errors - same as 04_discover.py
        print(f"  ⚠️  Permission fetch failed for genie/{space_id}: {str(e)}")
        return []


def fetch_current_permissions(client, object_type: str, object_id: str) -> List[Dict[str, str]]:
    """
    Fetch current permissions for an object based on its type.
    Routes to the appropriate API based on object type.
    Returns list of {principal_email, principal_type, permission_level} dicts.
    """
    # Unity Catalog object types
    uc_types = {
        'catalog', 'schema', 'table', 'volume', 'function', 'connection',
        'externalLocation', 'storageCredential', 'share', 'recipient',
        'provider', 'metastore', 'ucRegisteredModel', 'registeredModel',
        'vectorIndex', 'featureTable', 'monitors'
    }
    
    if object_type in uc_types:
        return get_uc_grants(client, object_type, object_id)
    elif object_type == 'secretScope':
        return get_secret_scope_acls(client, object_id)
    elif object_type == 'genieSpace':
        return get_genie_space_permissions(client, object_id)
    else:
        return get_workspace_permissions(client, object_type, object_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Enabled Workspaces

# COMMAND ----------

# Load enabled workspaces
enabled_workspaces_df = spark.sql(f"""
    SELECT 
        workspace_id,
        workspace_name,
        enabled_object_types
    FROM {catalog}.{schema}.governance_config_workspaces
    WHERE enforcement_enabled = true
""")

enabled_workspace_ids = [row.workspace_id for row in enabled_workspaces_df.collect()]
print(f"Governance enabled for {len(enabled_workspace_ids)} workspace(s)")

if not enabled_workspace_ids:
    print("WARNING: No workspaces have governance enabled. Exiting.")
    dbutils.notebook.exit('{"status": "SKIPPED", "reason": "No enabled workspaces"}')

workspace_ids_str = "', '".join([str(wid) for wid in enabled_workspace_ids])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Pre-Approved Identities

# COMMAND ----------

# Load pre-approved identities that can manage resources (including approved_actions)
preapproved_identities_df = spark.sql(f"""
    SELECT 
        identity_name,
        identity_type,
        COALESCE(can_manage_resources, true) as can_manage_resources,
        COALESCE(can_manage_permissions, false) as can_manage_permissions,
        COALESCE(approved_actions, ARRAY('ALL')) as approved_actions
    FROM {catalog}.{schema}.governance_preapproved_identities
    WHERE is_active = true
""")

# Build lists of approved identities and track approved_actions per identity
resource_approved_identities = []
permission_approved_identities = []

# Dictionary to track approved_actions per identity
# Key: identity_name, Value: list of approved object types
identity_approved_actions: Dict[str, List[str]] = {}

for row in preapproved_identities_df.collect():
    identity_name = row.identity_name
    approved_actions = list(row.approved_actions) if row.approved_actions else ['ALL']
    
    # Store approved_actions for this identity
    identity_approved_actions[identity_name] = approved_actions
    
    if row.can_manage_resources:
        resource_approved_identities.append(identity_name)
    if row.can_manage_permissions:
        permission_approved_identities.append(identity_name)

# Separate identities with ALL vs restricted approved_actions
resource_identities_with_all = []
resource_identities_with_restrictions: Dict[str, List[str]] = {}

for identity in resource_approved_identities:
    actions = identity_approved_actions.get(identity, ['ALL'])
    if 'ALL' in actions:
        resource_identities_with_all.append(identity)
    else:
        resource_identities_with_restrictions[identity] = actions

print(f"Approved identities for RESOURCE management: {len(resource_approved_identities)}")
print(f"  - With ALL permissions: {len(resource_identities_with_all)}")
print(f"  - With RESTRICTED permissions: {len(resource_identities_with_restrictions)}")
print(f"Approved identities for PERMISSION management: {len(permission_approved_identities)}")

# Print restricted identities for visibility
if resource_identities_with_restrictions:
    print("\nIdentities with RESTRICTED approved_actions:")
    for identity, actions in resource_identities_with_restrictions.items():
        print(f"  - {identity}: can create [{', '.join(actions)}]")

# Build SQL-safe identity strings (include ALL resource approved identities for querying)
if resource_approved_identities:
    escaped_resource_ids = [id.replace("'", "''") for id in resource_approved_identities]
    resource_ids_str = "', '".join(escaped_resource_ids)
else:
    resource_ids_str = ""

if permission_approved_identities:
    escaped_permission_ids = [id.replace("'", "''") for id in permission_approved_identities]
    permission_ids_str = "', '".join(escaped_permission_ids)
else:
    permission_ids_str = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Governance Filters

# COMMAND ----------

# Load active filters from governance_filters table
filters_df = spark.sql(f"""
    SELECT 
        filter_name,
        service_name,
        action_name,
        object_type,
        object_id_expr,
        object_name_expr,
        violation_type,
        remediation_action
    FROM {catalog}.{schema}.governance_filters
    WHERE is_active = true
    ORDER BY filter_name
""")

filters_list = filters_df.collect()

# Group filters by type
create_filters = [f for f in filters_list if f.violation_type == 'UNAPPROVED_CREATION']
acl_filters = [f for f in filters_list if f.violation_type == 'UNAUTHORIZED_PERMISSION_CHANGE']

print(f"Loaded {len(create_filters)} create filters")
print(f"Loaded {len(acl_filters)} ACL change filters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Logs for Approved User Creations

# COMMAND ----------

def build_approved_user_query(
    filters: List[Any],
    workspace_ids_str: str,
    lookback_hours: int,
    approved_identities_str: str,
    is_permission_change: bool = False,
    is_delete_event: bool = False
) -> str:
    """
    Build a SQL query to find events by APPROVED users (opposite of watcher).
    Uses the same filter logic as build_audit_query_from_filters in 05_watcher.py.
    
    Args:
        filters: List of filter records from governance_filters table
        workspace_ids_str: Comma-separated workspace IDs for IN clause
        lookback_hours: Hours to look back in audit logs
        approved_identities_str: Comma-separated approved identity names
        is_permission_change: Whether these are permission change events
        is_delete_event: Whether these are delete events
    
    Returns:
        SQL query string
    """
    if not filters or not approved_identities_str:
        return None
    
    # Build CASE statements for object_id, object_name, and object_type
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
        
        # Wrap expressions with COALESCE to handle null request_params
        object_id_expr = f"COALESCE({f.object_id_expr}, 'unknown')"
        object_name_expr = f"COALESCE({f.object_name_expr}, 'unknown')"
        
        object_id_cases.append(f"WHEN {condition} THEN {object_id_expr}")
        object_name_cases.append(f"WHEN {condition} THEN {object_name_expr}")
        
        # Check if object_type is a dynamic SQL expression (contains CASE, request_params, or LOWER)
        # If so, use it directly; otherwise, treat it as a static string literal
        object_type_str = f.object_type.strip() if f.object_type else 'unknown'
        is_dynamic_expr = (
            object_type_str.upper().startswith('CASE') or 
            'request_params' in object_type_str.lower() or
            object_type_str.upper().startswith('LOWER(') or
            object_type_str.upper().startswith('UPPER(') or
            object_type_str.upper().startswith('COALESCE(')
        )
        
        if is_dynamic_expr:
            # Dynamic expression - use as-is with COALESCE for null safety
            object_type_cases.append(f"WHEN {condition} THEN COALESCE({object_type_str}, 'unknown')")
        else:
            # Static string - wrap in quotes
            object_type_cases.append(f"WHEN {condition} THEN '{object_type_str}'")
        
        # Apply custom filters per service/action - same as 05_watcher.py
        if f.service_name == 'clusters' and f.action_name in ('create', 'createResult'):
            condition = f"""({condition}) AND
                    NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
                    AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
                    AND NOT (
                        request_params.kind='SERVERLESS_SQL_WAREHOUSE' AND request_params.cluster_creator='SQL_SERVICE'
                        OR request_params.kind='SERVERLESS_PREVIEW' AND request_params.cluster_creator='COMPUTE_GATEWAY_LAUNCHER'
                        OR request_params.kind='SERVERLESS_REPL_VM' AND request_params.cluster_creator='REPL_LAUNCHER'
                        )"""
        elif f.service_name == 'clusters' and f.action_name == 'delete':
            condition = f"""({condition} AND
                    NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
                    AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
                    )"""
        elif f.service_name == 'clusters' and f.action_name == 'changeClusterAcl':
            condition = f"""({condition}) AND request_params.resourceId IN 
                    (select distinct cluster_id from system.compute.clusters where cluster_source IN ('API','UI') 
                    and workspace_id IN ('{workspace_ids_str}'))
                    """
        elif f.service_name == 'jobs' and f.action_name == 'changeJobAcl':
            condition = f"""({condition})
                        --Exclusión por asignación de Owner desde DataFactory
                        AND NOT (NVL(request_params.aclPermissionSet,'x') ='Owner' AND NVL(USER_AGENT,'x')='AzureDataFactory')
                        AND NOT (
                            user_identity.email = '7cdf5dcf-54d6-4a1c-ba10-7fd308054e87'
                            AND (
                                request_params.aclPermissionSet = 'Manage Run' AND request_params.targetUserId ='954664791769442' /*SopDWH*/
                                OR request_params.aclPermissionSet = 'Admin' AND request_params.targetUserId = '81986449886208' /*Admin_prod*/
                                OR request_params.aclPermissionSet = 'Admin' AND request_params.targetUserId = '41075057382951' /*AdminUsers*/
                                OR request_params.aclPermissionSet = 'Owner' AND request_params.targetUserId = '6517422260732230' /*SPDBPRD*/
                                OR request_params.aclPermissionSet = 'View' AND request_params.targetUserId = '83165665297392' /*BigData*/
                                )
                            )
                        """
        elif (f.service_name == 'notebook' and f.action_name == 'createNotebook') or (f.service_name == 'workspace' and f.action_name == 'createFile'):
            condition = f"""({condition} 
                    AND NOT (
                        request_params.path LIKE '/Workspace/Repos/%' AND REGEXP_COUNT(request_params.path,'/') > 3
                        OR request_params.path LIKE '/Workspace/Users/%' AND REGEXP_COUNT(request_params.path,'/') > 2
                        OR request_params.path LIKE '/Users/%' AND REGEXP_COUNT(request_params.path,'/') >= 2
                        OR request_params.path LIKE '/Workspace/%' AND REGEXP_COUNT(request_params.path,'/') > 2 AND NOT SPLIT_PART(request_params.path,'/',3) IN ('Users','Repos')
                    )
                )"""
        elif f.service_name == 'workspace' and f.action_name == 'fileDelete':
            condition = f"""({condition} 
                    AND NOT (
                        request_params.path LIKE '/Workspace/Repos/%' AND REGEXP_COUNT(request_params.path,'/') > 3
                        OR request_params.path LIKE '/Workspace/Users/%' AND REGEXP_COUNT(request_params.path,'/') > 2
                        OR request_params.path LIKE '/Users/%' AND REGEXP_COUNT(request_params.path,'/') >= 2
                        OR request_params.path LIKE '/Workspace/%' AND REGEXP_COUNT(request_params.path,'/') > 2 AND NOT SPLIT_PART(request_params.path,'/',3) IN ('Users','Repos')
                    )
                )"""
        else:
            condition = f"({condition})"

        action_conditions.append(f"({condition})")
    
    # Build CASE SQL
    object_id_sql = "CASE \n            " + "\n            ".join(object_id_cases) + "\n            ELSE 'unknown'\n        END"
    object_name_sql = "CASE \n            " + "\n            ".join(object_name_cases) + "\n            ELSE 'unknown'\n        END"
    object_type_sql = "CASE \n            " + "\n            ".join(object_type_cases) + "\n            ELSE 'unknown'\n        END"
    action_filter_sql = " \n\t\t\tOR ".join(action_conditions)
    
    # Build default filter for unhandled events (same as 05_watcher.py)
    excl_services_name = ','.join(list(map(lambda x: f"'{x}'", [evento for evento in set(service_names)])))
    excl_acls = ','.join(list(map(lambda x: f"'{x}'", [acl for acl in set(all_acls)])))
    
    default_filter = ""
    if is_delete_event:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike '%delete%' AND NOT SERVICE_NAME IN ({excl_services_name}))"
    elif is_permission_change:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike 'change%Acl' AND NOT ACTION_NAME IN ({excl_acls}))"
    else:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ILIKE '%create%' AND NOT SERVICE_NAME IN ({excl_services_name}))"
    
    # Filter FOR approved users (opposite of watcher - use IN instead of NOT IN)
    identity_filter = f"user_identity.email IN ('{approved_identities_str}')"
    
    # Build exclusion for personal workspace deletions (same as 05_watcher.py)
    personal_workspace_exclusion = ""
    if is_delete_event:
        personal_workspace_exclusion = """
        -- Exclude notebook/folder/repo deletions in personal workspace (/Workspace/Users/)
        AND NOT (
            service_name = 'notebook' 
            AND action_name IN ('deleteNotebook', 'deleteFolder', 'deleteRepo')
            AND (
                request_params.path LIKE '/Workspace/Users/%'
                or request_params.path LIKE '/Users/%'
                --Carpetas /Workspace/FolderName/...
                OR NOT SPLIT_PART(request_params.path,'/',3) IN ('Users','Repos') 
                    AND NOT REGEXP_LIKE(request_params.path,'^/Workspace/[^*]+[/.?]')
                )
        )"""
    
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
        {object_id_sql} as object_id,
        {object_name_sql} as object_name,
        {object_type_sql} as object_type,
        request_params,
        response
    FROM system.access.audit
    WHERE EVENT_TIME >= DATE_TRUNC('HOUR',CURRENT_TIMESTAMP()) - INTERVAL {lookback_hours} HOUR
        AND workspace_id IN ('{workspace_ids_str}')
        AND user_identity.email IS NOT NULL
        AND NVL(user_identity.email,'x') <> 'System-User'
        AND response.status_code IN (200, 201, 202, 203, 204, 205, 206, 207, 208)
        AND ({action_filter_sql}{default_filter})
        AND {identity_filter}{personal_workspace_exclusion}
    ORDER BY EVENT_TIME DESC
    """
    
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync New Creations by Approved Users

# COMMAND ----------

creations_synced = 0
creations_filtered_by_actions = 0

if sync_creations and resource_ids_str:
    print("="*80)
    print("SYNCING NEW CREATIONS BY APPROVED USERS")
    print("="*80)
    
    create_query = build_approved_user_query(
        create_filters,
        workspace_ids_str,
        lookback_hours,
        resource_ids_str,
        is_permission_change=False,
        is_delete_event=False
    )
    
    if create_query:
        create_events_df = spark.sql(create_query)
        
        # Filter out unknown object_ids
        valid_creates_df = create_events_df.filter(
            (col("object_id") != "unknown") & 
            (col("object_id").isNotNull())
        )
        
        initial_count = valid_creates_df.count()
        print(f"Found {initial_count} creation events by approved users")
        
        # Apply approved_actions filtering - only sync objects that match the user's allowed object types
        # For users with restricted approved_actions, filter to only their allowed object types
        if resource_identities_with_restrictions:
            print(f"\nApplying approved_actions filter for {len(resource_identities_with_restrictions)} restricted identities...")
            
            # Create a DataFrame of allowed (identity, object_type) pairs
            allowed_pairs = []
            for identity, actions in resource_identities_with_restrictions.items():
                for action in actions:
                    allowed_pairs.append((identity, action.lower()))
            
            # Create DataFrame with allowed combinations
            allowed_df = spark.createDataFrame(
                allowed_pairs, 
                ["allowed_identity", "allowed_object_type"]
            )
            
            # Add lowercase object_type column for case-insensitive matching
            valid_creates_with_lower = valid_creates_df.withColumn(
                "object_type_lower", lower(col("object_type"))
            )
            
            # Split events into two groups:
            # 1. Events from users with ALL permissions - always sync
            # 2. Events from users with restrictions - only sync if object_type matches
            
            # Get list of restricted user emails for filtering
            restricted_users = list(resource_identities_with_restrictions.keys())
            
            # Events from users with ALL permissions (always sync)
            events_from_all_users = valid_creates_with_lower.filter(
                ~col("user_email").isin(restricted_users)
            )
            
            # Events from restricted users that match their allowed object types
            events_from_restricted_users = valid_creates_with_lower.filter(
                col("user_email").isin(restricted_users)
            )
            
            # Join restricted user events with allowed pairs to find valid ones
            valid_restricted_events = events_from_restricted_users.join(
                allowed_df,
                (events_from_restricted_users["user_email"] == allowed_df["allowed_identity"]) &
                (events_from_restricted_users["object_type_lower"] == allowed_df["allowed_object_type"]),
                "inner"
            ).drop("allowed_identity", "allowed_object_type")
            
            # Combine both sets
            valid_creates_df = events_from_all_users.unionByName(valid_restricted_events).drop("object_type_lower")
            
            filtered_count = valid_creates_df.count()
            creations_filtered_by_actions = initial_count - filtered_count
            print(f"After approved_actions filtering: {filtered_count} events to sync")
            print(f"Filtered out {creations_filtered_by_actions} events (object type not in user's approved_actions)")
        
        create_count = valid_creates_df.count()
        print(f"Total creation events to sync: {create_count}")
        
        if create_count > 0:
            # Check which objects are NOT already in pre-approved objects
            existing_objects_df = spark.sql(f"""
                SELECT workspace_id, object_id
                FROM {catalog}.{schema}.governance_preapproved_objects
                WHERE is_active = true
            """)
            
            # Find new objects not yet in the table
            new_objects_df = valid_creates_df.alias("new").join(
                existing_objects_df.alias("existing"),
                (col("new.workspace_id") == col("existing.workspace_id")) &
                (col("new.object_id") == col("existing.object_id")),
                "left_anti"
            )
            
            new_count = new_objects_df.count()
            print(f"New objects to add: {new_count}")
            
            if new_count > 0:
                # Collect new objects and fetch their current permissions
                new_objects_list = new_objects_df.collect()
                new_objects_with_permissions = []
                
                print(f"Fetching permissions for {len(new_objects_list)} new objects...")
                
                for row in new_objects_list:
                    object_id = row.object_id
                    object_type = row.object_type
                    workspace_id = row.workspace_id
                    object_name = row.object_name
                    user_email = row.user_email
                    
                    # Fetch current permissions for this object
                    permissions = fetch_current_permissions(client, object_type, object_id)
                    
                    # Try to get object path if available from audit log
                    object_path = None
                    if hasattr(row, 'object_path') and row.object_path:
                        object_path = row.object_path
                    
                    # Build metadata dict
                    metadata = {}
                    if hasattr(row, 'event_time') and row.event_time:
                        metadata['created_at'] = str(row.event_time)
                    if hasattr(row, 'action_name') and row.action_name:
                        metadata['created_by_action'] = row.action_name
                    
                    # Determine owner from permissions if not set
                    owner_email = user_email
                    for perm in permissions:
                        if perm.get('permission_level') in ['CAN_MANAGE', 'MANAGE', 'ALL_PRIVILEGES', 'OWNER']:
                            if perm.get('principal_type') == 'user':
                                owner_email = perm.get('principal_email', user_email)
                                break
                    
                    # Convert permission dictionaries to Row objects for proper DataFrame creation
                    # This is required for PySpark to correctly map to ArrayType(StructType([...]))
                    permissions_array = None
                    if permissions:
                        permissions_array = [
                            Row(
                                principal_email=p['principal_email'],
                                principal_type=p.get('principal_type', 'user'),
                                permission_level=p['permission_level']
                            )
                            for p in permissions
                        ]
                    
                    new_objects_with_permissions.append({
                        'object_id': object_id,
                        'workspace_id': workspace_id,
                        'object_type': object_type,
                        'object_name': object_name,
                        'object_path': object_path,
                        'owner_email': owner_email,
                        'permissions': permissions_array,
                        'metadata': metadata if metadata else None,
                        'is_active': True,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    })
                
                print(f"  Fetched permissions for {len(new_objects_with_permissions)} objects")
                
                # Define schema for the DataFrame - must match governance_preapproved_objects table schema
                new_objects_schema = StructType([
                    StructField('object_id', StringType(), True),
                    StructField('workspace_id', StringType(), True),
                    StructField('object_type', StringType(), True),
                    StructField('object_name', StringType(), True),
                    StructField('object_path', StringType(), True),
                    StructField('owner_email', StringType(), True),
                    StructField('permissions', ArrayType(StructType([
                        StructField('principal_email', StringType(), True),
                        StructField('principal_type', StringType(), True),
                        StructField('permission_level', StringType(), True)
                    ])), True),
                    StructField('metadata', MapType(StringType(), StringType()), True),
                    StructField('is_active', BooleanType(), True),
                    StructField('created_at', TimestampType(), True),
                    StructField('updated_at', TimestampType(), True)
                ])
                
                # Create DataFrame with explicit schema
                new_objects_to_insert = spark.createDataFrame(new_objects_with_permissions, schema=new_objects_schema)
                
                # Insert new objects
                new_objects_to_insert.createOrReplaceTempView("new_approved_objects")
                
                spark.sql(f"""
                    MERGE INTO {catalog}.{schema}.governance_preapproved_objects AS target
                    USING new_approved_objects AS source
                    ON target.workspace_id = source.workspace_id 
                    AND target.object_id = source.object_id
                    WHEN NOT MATCHED THEN INSERT *
                """)
                
                creations_synced = new_count
                # Use len() with list comprehension instead of sum() to avoid conflict with pyspark.sql.functions.sum
                perm_count = len([obj for obj in new_objects_with_permissions if obj['permissions']])
                print(f"✓ Added {new_count} new objects to pre-approved objects table")
                print(f"  - Objects with permissions fetched: {perm_count}")
                
                # Show sample of synced objects
                print("\nSample of newly synced objects:")
                display(new_objects_to_insert.limit(10))
else:
    print("Skipping creation sync (disabled or no approved identities)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Permission Changes by Approved Users

# COMMAND ----------

permissions_synced = 0
permissions_updated = 0
permissions_added = 0

if sync_permissions and permission_ids_str:
    print("="*80)
    print("SYNCING PERMISSION CHANGES BY APPROVED USERS")
    print("="*80)
    
    acl_query = build_approved_user_query(
        acl_filters,
        workspace_ids_str,
        lookback_hours,
        permission_ids_str,
        is_permission_change=True,
        is_delete_event=False
    )
    
    if acl_query:
        acl_events_df = spark.sql(acl_query)
        
        # Filter out unknown object_ids
        valid_acl_df = acl_events_df.filter(
            (col("object_id") != "unknown") & 
            (col("object_id").isNotNull())
        )
        
        acl_count = valid_acl_df.count()
        print(f"Found {acl_count} ACL change events by approved users")
        
        if acl_count > 0:
            # Get unique objects that had permission changes
            objects_with_changes = valid_acl_df.select(
                "workspace_id", "object_id", "object_type"
            ).distinct()
            
            unique_objects = objects_with_changes.count()
            print(f"Unique objects with permission changes: {unique_objects}")
            
            # Fetch current permissions for each object and update the table
            objects_list = objects_with_changes.collect()
            
            print(f"\nFetching and syncing current permissions...")
            
            updated_records = []
            
            for obj in objects_list:
                workspace_id = obj.workspace_id
                object_id = obj.object_id
                object_type = obj.object_type
                
                print(f"\n  Processing: {object_type}:{object_id}")
                
                # Fetch current permissions from Databricks APIs
                current_permissions = fetch_current_permissions(client, object_type, object_id)
                
                if current_permissions:
                    print(f"    → Fetched {len(current_permissions)} permission entries")
                    
                    # Convert to the format expected by the table (must include all 3 fields)
                    permissions_array = [
                        Row(
                            principal_email=p['principal_email'], 
                            principal_type=p.get('principal_type', 'user'),  # Include principal_type
                            permission_level=p['permission_level']
                        )
                        for p in current_permissions
                    ]
                    
                    updated_records.append({
                        'workspace_id': workspace_id,
                        'object_id': object_id,
                        'object_type': object_type,
                        'permissions': permissions_array,
                        'updated_at': datetime.utcnow()
                    })
                    permissions_synced += 1
                else:
                    print(f"    → No permissions found or object not accessible")
            
            # Update the pre-approved objects table with new permissions
            if updated_records:
                print(f"\n{'='*60}")
                print(f"Updating {len(updated_records)} objects with new permissions...")
                
                # Check which objects already exist in the table
                existing_objects_df = spark.sql(f"""
                    SELECT workspace_id, object_id, object_type, object_name, object_path, owner_email, metadata, is_active, created_at
                    FROM {catalog}.{schema}.governance_preapproved_objects
                    WHERE is_active = true
                """)
                existing_objects = {(row.workspace_id, row.object_id): row for row in existing_objects_df.collect()}
                
                records_to_update = []
                records_to_insert = []
                
                for record in updated_records:
                    key = (record['workspace_id'], record['object_id'])
                    
                    if key in existing_objects:
                        # Object exists - update permissions
                        existing = existing_objects[key]
                        records_to_update.append({
                            'workspace_id': record['workspace_id'],
                            'object_id': record['object_id'],
                            'object_type': record['object_type'],
                            'object_name': existing.object_name,
                            'object_path': existing.object_path,
                            'owner_email': existing.owner_email,
                            'permissions': record['permissions'],
                            'metadata': existing.metadata,
                            'is_active': True,
                            'created_at': existing.created_at,
                            'updated_at': record['updated_at']
                        })
                        permissions_updated += 1
                    else:
                        # Object doesn't exist - insert new record
                        records_to_insert.append({
                            'workspace_id': record['workspace_id'],
                            'object_id': record['object_id'],
                            'object_type': record['object_type'],
                            'object_name': record['object_id'],  # Use object_id as name if not known
                            'object_path': None,
                            'owner_email': 'unknown',
                            'permissions': record['permissions'],
                            'metadata': None,
                            'is_active': True,
                            'created_at': record['updated_at'],
                            'updated_at': record['updated_at']
                        })
                        permissions_added += 1
                
                # Define schema for the DataFrame - must match governance_preapproved_objects table schema
                permissions_schema = StructType([
                    StructField('workspace_id', StringType(), True),
                    StructField('object_id', StringType(), True),
                    StructField('object_type', StringType(), True),
                    StructField('object_name', StringType(), True),
                    StructField('object_path', StringType(), True),
                    StructField('owner_email', StringType(), True),
                    StructField('permissions', ArrayType(StructType([
                        StructField('principal_email', StringType(), True),
                        StructField('principal_type', StringType(), True),  # 'user', 'group', or 'service_principal'
                        StructField('permission_level', StringType(), True)
                    ])), True),
                    StructField('metadata', MapType(StringType(), StringType()), True),
                    StructField('is_active', BooleanType(), True),
                    StructField('created_at', TimestampType(), True),
                    StructField('updated_at', TimestampType(), True)
                ])
                
                # Merge updates and inserts
                all_records = records_to_update + records_to_insert
                
                if all_records:
                    permissions_df = spark.createDataFrame(all_records, schema=permissions_schema)
                    permissions_df.createOrReplaceTempView("permission_updates")
                    
                    spark.sql(f"""
                        MERGE INTO {catalog}.{schema}.governance_preapproved_objects AS target
                        USING permission_updates AS source
                        ON target.workspace_id = source.workspace_id 
                        AND target.object_id = source.object_id
                        WHEN MATCHED THEN UPDATE SET
                            permissions = source.permissions,
                            updated_at = source.updated_at
                        WHEN NOT MATCHED THEN INSERT *
                    """)
                    
                    print(f"✓ Updated permissions for {permissions_updated} existing objects")
                    print(f"✓ Added {permissions_added} new objects with permissions")
                    
                    # Show sample of updated permissions
                    if permissions_updated > 0 or permissions_added > 0:
                        print("\nSample of synced permission changes:")
                        for record in all_records[:5]:
                            perm_count = len(record['permissions']) if record['permissions'] else 0
                            print(f"  - {record['object_type']}:{record['object_id']} ({perm_count} permissions)")
else:
    print("Skipping permission sync (disabled or no approved identities)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("SYNC SUMMARY")
print("="*80)
print(f"Lookback Period:          {lookback_hours} hours")
print(f"Workspaces Monitored:     {len(enabled_workspace_ids)}")
print(f"\nApproved Identities:")
print(f"  - Resource Management:  {len(resource_approved_identities)}")
print(f"    - With ALL permissions:      {len(resource_identities_with_all)}")
print(f"    - With RESTRICTED permissions: {len(resource_identities_with_restrictions)}")
print(f"  - Permission Management: {len(permission_approved_identities)}")
print(f"\nSync Results:")
print(f"  - New Creations Synced:         {creations_synced}")
print(f"  - Creations Filtered (by actions): {creations_filtered_by_actions}")
print(f"  - Permission Changes Found:     {permissions_synced}")
print(f"  - Permissions Updated:          {permissions_updated}")
print(f"  - New Objects Added:            {permissions_added}")
print("="*80)

# Show current state of pre-approved objects
summary_df = spark.sql(f"""
    SELECT 
        object_type,
        COUNT(*) as total_count,
        COUNT(DISTINCT owner_email) as unique_owners,
        MAX(updated_at) as last_updated
    FROM {catalog}.{schema}.governance_preapproved_objects
    WHERE is_active = true
    GROUP BY object_type
    ORDER BY total_count DESC
""")

print("\nPre-Approved Objects Summary:")
display(summary_df)

# COMMAND ----------

# Return status
dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'creations_synced': creations_synced,
    'creations_filtered_by_actions': creations_filtered_by_actions,
    'permissions_synced': permissions_synced,
    'permissions_updated': permissions_updated,
    'permissions_added': permissions_added,
    'timestamp': datetime.utcnow().isoformat()
}))

