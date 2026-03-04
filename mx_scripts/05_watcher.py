# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Watcher
# MAGIC
# MAGIC This notebook reads audit logs and detects violations by comparing against
# MAGIC pre-approved objects and identities. Detected violations are saved to the staging table.
# MAGIC
# MAGIC **Violation Types:**
# MAGIC - UNAPPROVED_CREATION: Unauthorized resource creation
# MAGIC - UNAUTHORIZED_PERMISSION_CHANGE: Unauthorized ACL changes
# MAGIC - UNAUTHORIZED_DELETION: Unauthorized resource deletion
# MAGIC - UNAUTHORIZED_ENTITLEMENT_CHANGE: Unauthorized entitlement changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

#dbutils.widgets.text("catalog", "qadl", "Catalog Name")
#dbutils.widgets.text("schema", "sch_mng_admon", "Schema Name")
#dbutils.widgets.text("lookback_hours", "168", "Lookback Hours for Audit Logs")

# COMMAND ----------

import os
environment = os.environ.get('DATABRICKS_RUNTIME_VERSION',None)
print(environment)

flagSERVERLESS = False

if environment.startswith("client."):
	flagSERVERLESS = True
	spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

print(f"Serverless: {flagSERVERLESS}")

# COMMAND ----------

try:
    auth_type = dbutils.widgets.get("auth_type")
except Exception as e:
    auth_type = "azure-client-secret"

# COMMAND ----------

from dbruntime.databricks_repl_context import get_context
from datetime import datetime
import json
import pytz
from databricks.sdk import WorkspaceClient

workspaceId = get_context().workspaceId
tz = pytz.timezone("America/Mexico_City")

if workspaceId == "4126527463676543":
    catalog = "qadl"
    if auth_type == "azure-client-secret":
        kv_scope = "azueskvsadl01"
        kv_client_id_key = "b8a66bbe-9d97-4f64-bd3d-9e4768835700"
        kv_client_secret_key = 'serviceprincipal-SPDBPROD'
        kv_tenant_id_key = 'ApiRestTenant'
    elif auth_type == "pat":
        kv_scope = "azueskvsadl01"
        kv_client_secret_key = "add-secret-id-token-databricks"
        kv_client_secret_key2 = "add-secret-id-token-databricks2"
else:
    catalog = "dlprod"
    if auth_type == "azure-client-secret":
        kv_scope = "TBD" #"esazukvspdl01"
        kv_client_id_key = "TDB" #"7cdf5dcf-54d6-4a1c-ba10-7fd308054e87"
        kv_client_secret_key = "TBD" #"serviceprincipal-SPDBPROD"
        kv_tenant_id_key = "TBD" #"ApiRestTenant"
    elif auth_type == "pat":
        kv_scope = "esazukvspcso01"
        kv_client_secret_key = "WADatabricks"
        kv_client_secret_key2 = "WADatabricks2"

schema = "sch_mng_admon"

print(f"Catalog: {catalog}")

# COMMAND ----------

#catalog = dbutils.widgets.get("catalog")
#schema = dbutils.widgets.get("schema")
try:
    lookback_hours = int(dbutils.widgets.get("lookback_hours"))
except Exception:
    lookback_hours = 24

try:
    show_query = True if dbutils.widgets.get("show_query") == "Y" or dbutils.widgets.get("show_query") == "S" else False
except Exception:
    show_query = False

try:
    load_filters = True if dbutils.widgets.get("load_filters") == "Y" or dbutils.widgets.get("load_filters") == "S" else False
except Exception:
    load_filters = False

try:
    enable_discover = True if dbutils.widgets.get("enable_discover") == "Y" or dbutils.widgets.get("enable_discover") == "S" else False
except Exception:
    enable_discover = False

try:
    load_approved_id = True if dbutils.widgets.get("load_approved_id") == "Y" or dbutils.widgets.get("load_approved_id") == "S" else False
except:
    load_approved_id = False

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Lookback Hours: {lookback_hours}")
print(f"Show query: {show_query}")
print(f"Load filters: {load_filters}")
print(f"Enable discover: {enable_discover}")
print(f"Load Approved identities: {load_approved_id}")
print(f"Auth type: {auth_type}")

# COMMAND ----------

if load_filters or enable_discover or load_approved_id:
    dbutils.notebook.exit(json.dumps({
    'status': 'SKIPPED',
    'reason': "Proceso abanderado para realizar el discovery de objetos, actualizar los filtros o agregar Identidades para manejo de recursos/permisos",
    'load filters': load_filters,
    'Enable discover': enable_discover,
    'timestamp': datetime.now(tz).isoformat()
}, indent=3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Enabled Workspaces

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import uuid
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any, Tuple

# Initialize workspace client for group expansion
client = WorkspaceClient()

# Load enabled workspaces first to use as filter
enabled_workspaces_df = spark.sql(f"""
    SELECT 
        workspace_id,
        workspace_name,
        enabled_object_types,
        max_retry_attempts,
        workspace_url
    FROM {catalog}.{schema}.governance_config_workspaces
    WHERE enforcement_enabled = true
""")

enabled_workspace_ids = [row.workspace_id for row in enabled_workspaces_df.collect()]
print(f"Governance enabled for {len(enabled_workspace_ids)} workspace(s)")

if not enabled_workspace_ids:
    print("WARNING: No workspaces have governance enabled. Exiting.")
    dbutils.notebook.exit('{"status": "SKIPPED", "reason": "No enabled workspaces"}')

# Build the workspace IDs string for SQL IN clause
workspace_ids_str = "', '".join([str(wid) for wid in enabled_workspace_ids])
print(f"\tWorkspace Ids: {workspace_ids_str}")

# COMMAND ----------

def create_workspace_client(workspace_url: str) -> WorkspaceClient:
    """Create a WorkspaceClient with Azure authentication using Key Vault secrets."""    
    if kv_scope and auth_type == "azure-client-secret":
        # Get credentials from Key Vault
        azure_client_id = kv_client_id_key #dbutils.secrets.get(scope=kv_scope, key=kv_client_id_key)
        client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key)
        tenant_id = dbutils.secrets.get(scope=kv_scope, key=kv_tenant_id_key)

        return WorkspaceClient(
            host=workspace_url,
            azure_client_id=azure_client_id,
            azure_client_secret=client_secret,
            azure_tenant_id=tenant_id,
            auth_type="azure-client-secret"
        )
    elif kv_scope and auth_type == "pat":
        if "4126527463676543" in workspace_url or "4782182804791024" in workspace_url:
            client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key)
        else:
            client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key2)

        return WorkspaceClient(
            host=workspace_url,
            token = client_secret,
            auth_type="pat"
        )
    else:
        return WorkspaceClient()

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
        extra_columns,
        violation_type,
        remediation_action
    FROM {catalog}.{schema}.governance_filters
    WHERE is_active = true
    ORDER BY filter_name
""")

filters_list = filters_df.collect()
print(f"Loaded {len(filters_list)} active governance filters")

# Group filters by violation type
create_filters = [f for f in filters_list if f.violation_type == 'UNAPPROVED_CREATION']
acl_filters = [f for f in filters_list if f.violation_type == 'UNAUTHORIZED_PERMISSION_CHANGE']
delete_filters = [f for f in filters_list if f.violation_type == 'UNAUTHORIZED_DELETION']
entitlement_filters = [f for f in filters_list if f.violation_type == 'UNAUTHORIZED_ENTITLEMENT_CHANGE']
object_changes_filters = [f for f in filters_list if f.violation_type == 'UNAUTHORIZED_OBJECT_CHANGE']

objects_with_path = ",".join(list(map(lambda x: f"'{x}'",[row.object_type for row in filters_df.filter("object_id_expr like '%request_params.path%'").select("object_type").distinct().collect()])))
print(f"Type of objects with value in path: {objects_with_path}")

print(f"\nFilter breakdown:")
print(f"  - Create filters: {len(create_filters)}")
print(f"  - ACL change filters: {len(acl_filters)}")
print(f"  - Delete filters: {len(delete_filters)}")
print(f"  - Entitlement change filters: {len(entitlement_filters)}")
print(f"  - Object changes filters: {len(object_changes_filters)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Expand Pre-Approved Identities
# MAGIC
# MAGIC Identities are loaded with granular permission flags:
# MAGIC - **can_manage_resources**: Allowed to create/delete resources
# MAGIC - **can_manage_permissions**: Allowed to grant/revoke permissions

# COMMAND ----------

# Load pre-approved identities with permission flags and approved_actions
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
display(preapproved_identities_df)
# Separate by type AND permission flags
# Also track approved_actions per identity for granular object-type filtering
resource_approved_users = []
resource_approved_service_principals = []
resource_approved_groups = []

permission_approved_users = []
permission_approved_service_principals = []
permission_approved_groups = []

# Dictionary to track approved_actions per identity
# Key: identity_name, Value: list of approved object types
identity_approved_actions: Dict[str, List[str]] = {}

for row in preapproved_identities_df.collect():
    identity_name = row.identity_name
    identity_type = row.identity_type
    can_manage_resources = row.can_manage_resources
    can_manage_permissions = row.can_manage_permissions
    approved_actions = list(row.approved_actions) if row.approved_actions else ['ALL']
    
    # Store approved_actions for this identity
    identity_approved_actions[identity_name] = approved_actions
    
    if identity_type == 'USER':
        if can_manage_resources:
            resource_approved_users.append(identity_name)
        if can_manage_permissions:
            permission_approved_users.append(identity_name)
    elif identity_type == 'SERVICE_PRINCIPAL':
        if can_manage_resources:
            resource_approved_service_principals.append(identity_name)
        if can_manage_permissions:
            permission_approved_service_principals.append(identity_name)
    elif identity_type == 'GROUP':
        if can_manage_resources:
            resource_approved_groups.append(identity_name)
        if can_manage_permissions:
            permission_approved_groups.append(identity_name)

print(f"Direct approved identities for RESOURCE management:")
print(f"  - Users: {len(resource_approved_users)}")
print(f"  - Service Principals: {len(resource_approved_service_principals)}")
print(f"  - Groups: {len(resource_approved_groups)}")

print(f"\nDirect approved identities for PERMISSION management:")
print(f"  - Users: {len(permission_approved_users)}")
print(f"  - Service Principals: {len(permission_approved_service_principals)}")
print(f"  - Groups: {len(permission_approved_groups)}")

# Show identities with restricted approved_actions (not ALL)
restricted_identities = {k: v for k, v in identity_approved_actions.items() if 'ALL' not in v}
if restricted_identities:
    print(f"\nIdentities with RESTRICTED approved_actions (not ALL):")
    for identity, actions in restricted_identities.items():
        print(f"  - {identity}: {actions}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Expand Groups for Both Permission Types

# COMMAND ----------


def resolve_member_identity(client, member_id):
    """Resolve member ID to username/email."""
    try:
        user = client.users.get(id=member_id)
        if user and user.user_name:
            return (user.user_name, 'user')
    except Exception:
        pass
    
    try:
        sp = client.service_principals.get(id=member_id)
        if sp and sp.application_id:
            return (sp.application_id, 'sp')
    except Exception:
        pass
    
    return (None, None)

def expand_group_members(group_names: List[str], client, identity_approved_actions: Dict[str, List[str]]) -> Tuple[List[str], List[str]]:
    """
    Expand group names to get member users and service principals.
    Also inherits approved_actions from parent group to expanded members.
    """
    expanded_users = []
    expanded_sps = []
    
    if not group_names:
        return expanded_users, expanded_sps
    
    print(f"\nExpanding {len(group_names)} group(s)...")    
    for group_name in group_names:
        # Get the group's approved_actions to inherit to members
        group_approved_actions = identity_approved_actions.get(group_name, ['ALL'])
        
        try:
            group_members = client.groups.list(filter=f"displayName eq '{group_name}'")
            
            for group in group_members:
                if group.display_name == group_name:
                    try:
                        members = client.groups.list(filter=f"id eq '{group.id}'")
                        
                        for member_group in members:
                            if hasattr(member_group, 'members') and member_group.members:
                                for member in member_group.members:
                                    if hasattr(member, 'value'):
                                        member_id = member.value
                                        identity_name, identity_type = resolve_member_identity(client, member_id)
                                        
                                        if identity_name:
                                            # Inherit approved_actions from group
                                            # If member already has approved_actions, merge them
                                            if identity_name in identity_approved_actions:
                                                existing = identity_approved_actions[identity_name]
                                                if 'ALL' not in existing:
                                                    # Merge group's approved_actions with existing
                                                    merged = list(set(existing + group_approved_actions))
                                                    identity_approved_actions[identity_name] = merged
                                            else:
                                                # New member - inherit group's approved_actions
                                                identity_approved_actions[identity_name] = group_approved_actions
                                            
                                            if identity_type == 'user':
                                                expanded_users.append(identity_name)
                                            elif identity_type == 'sp':
                                                expanded_sps.append(identity_name)
                    except Exception as member_error:
                        print(f"  Warning: Could not get members for group {group_name}: {str(member_error)}")
                    break
            print(f"  ✓ Expanded group: {group_name}")
        except Exception as e:
            print(f"  ✗ Error expanding group {group_name}: {str(e)}")
    
    return list(set(expanded_users)), list(set(expanded_sps))

# Expand groups for resource management (also updates identity_approved_actions)
resource_expanded_users, resource_expanded_sps = expand_group_members(resource_approved_groups, client, identity_approved_actions)

# Expand groups for permission management (also updates identity_approved_actions)
permission_expanded_users, permission_expanded_sps = expand_group_members(permission_approved_groups, client, identity_approved_actions)

print(f"\nExpanded from groups for RESOURCE management:")
print(f"  - Users: {len(resource_expanded_users)}")
print(f"  - Service Principals: {len(resource_expanded_sps)}")

print(f"\nExpanded from groups for PERMISSION management:")
print(f"  - Users: {len(permission_expanded_users)}")
print(f"  - Service Principals: {len(permission_expanded_sps)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Final Approved Identity Lists

# COMMAND ----------


# Combine all approved identities for resource management
all_resource_approved_identities = list(set(
    resource_approved_users + 
    resource_approved_service_principals + 
    resource_expanded_users + 
    resource_expanded_sps
))

# Combine all approved identities for permission management
all_permission_approved_identities = list(set(
    permission_approved_users + 
    permission_approved_service_principals + 
    permission_expanded_users + 
    permission_expanded_sps
))

# Separate identities into those with ALL approved_actions vs restricted approved_actions
# Identities with ALL can be filtered at SQL level; restricted ones need Python-level filtering
resource_identities_with_all = []  # Can create ANY object type
resource_identities_with_restrictions = {}  # Can only create specific object types

for identity in all_resource_approved_identities:
    actions = identity_approved_actions.get(identity, ['ALL'])
    if 'ALL' in actions:
        resource_identities_with_all.append(identity)
    else:
        resource_identities_with_restrictions[identity] = actions

print(f"\nTotal approved identities for RESOURCE management: {len(all_resource_approved_identities)}")
print(f"  - With ALL permissions (excluded at SQL level): {len(resource_identities_with_all)}")
print(f"     {resource_identities_with_all}")
print(f"  - With RESTRICTED permissions (filtered in Python): {len(resource_identities_with_restrictions)}")
print(f"Total approved identities for PERMISSION management: {len(all_permission_approved_identities)}")
print(f"   {all_permission_approved_identities}")

# Build identity filter strings for SQL (only for identities with ALL)
# Identities with restrictions will be included in query results and filtered later
if not resource_identities_with_all:
    print("WARNING: No approved identities with ALL permissions. All creation/deletion events will be queried.")
    resource_approved_identities_str = ""
else:
    # Escape single quotes in identity names for SQL safety
    escaped_resource_identities = [id.replace("'", "''") for id in resource_identities_with_all]
    resource_approved_identities_str = "', '".join(escaped_resource_identities)

if not all_permission_approved_identities:
    print("WARNING: No approved identities for permission management. All permission changes will be flagged.")
    permission_approved_identities_str = ""
else:
    # Escape single quotes in identity names for SQL safety
    escaped_permission_identities = [id.replace("'", "''") for id in all_permission_approved_identities]
    permission_approved_identities_str = "', '".join(escaped_permission_identities)

# Print restricted identities for visibility
if resource_identities_with_restrictions:
    print("\nIdentities with RESTRICTED object type permissions:")
    for identity, actions in resource_identities_with_restrictions.items():
        print(f"  - {identity}: can create [{', '.join(actions)}]")


# COMMAND ----------

dfInfoUsers = spark.table("sch_mng_respaldos.tbl_ctl_usuarios_hist").selectExpr("USER_NAME","DISPLAY_NAME","OWNER_SUITS").distinct()
if flagSERVERLESS == False:
    dfInfoUsers = dfInfoUsers.cache()
print(f"Usuarios/SPs: {dfInfoUsers.count()}")

# COMMAND ----------

def _is_path_excluded(object_type: str, event_id: str, path: str) -> bool:
    new_path = (
        path
        if path.startswith("/Volumes/")
        else path
        if path.startswith("/Workspace/")
        else "/Workspace" + path
        if path.startswith("/Repos/")
        else "/Workspace" + path
    )

    #print(f"recibe {object_type}: \n\t{path} > {new_path}")

    if new_path.startswith("/Volumes/"):
        return True, new_path
    elif new_path.startswith("/Workspace/Users/"):
        return True, new_path
    elif new_path.startswith("/Workspace/Repos/") and len(new_path.split("/")) > 4:
        #print(f"\topc2 {len(new_path.split('/'))}")
        return True, new_path
    elif (
        new_path.startswith("/Workspace/")
        and new_path.split("/")[2] not in ["Users", "Repos"]
        and len(new_path.split("/")) > 3
    ):
        #print(f"\topc1 {new_path.split('/')[3]} {len(new_path.split('/'))}")
        return True, new_path
    else:
        return False, path

# COMMAND ----------

eventos = spark.sql("""SELECT EVENT_ID,EVENT_DATE,SERVICE_NAME,ACTION_NAME,request_params.path path
                    ,CASE WHEN action_name ilike '%folder%' or action_name ilike '%direct%' then 'directory'
                        when action_name ilike '%file%' then 'file'
                        when action_name ilike '%notebook%' then 'notebook'
                        else 'files' end object_type
                    from system.access.audit where EVENT_DATE >= current_date()-2 and request_params.path is not null and not action_name in ('filesPut','directoriesHead','filesHead') and regexp_like(lower(action_name),'create|delete') AND 1=2""")
print(f"Eventos {eventos.count()}")
eventos.display()

# COMMAND ----------

#print(_is_path_excluded("notebook","","/Volumes/Users/s12466"))
#print(_is_path_excluded("notebook","", "/Workspace/Users/s12466"))
#print(_is_path_excluded("notebook","", "/Users/s12466"))
#print(_is_path_excluded("notebook","", "/Repos/Ciencia_Datos_Deployment"))
#print(_is_path_excluded("notebook","", "/Repos/Ciencia_Datos_Deployment/arh.py"))
#print(_is_path_excluded("notebook","", "/Ciencia_Datos_Deployment"))
#print(_is_path_excluded("notebook","", "/Ciencia_Datos_Deployment/bundle_assets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Dynamic SQL Query from Filters

# COMMAND ----------

def build_audit_query_from_filters(
    filters: List[Any],
    workspace_ids_str: str,
    lookback_hours: int,
    identity_filter_str: str,
    is_permission_change: bool = False,
    is_delete_event: bool = False,
    is_entitlement_change: bool = False
) -> str:
    """
    Build a dynamic SQL query from governance filters.
    
    Args:
        filters: List of filter records from governance_filters table
        workspace_ids_str: Comma-separated workspace IDs for IN clause
        lookback_hours: Hours to look back in audit logs
        identity_filter_str: Comma-separated approved identity names
        is_permission_change: Whether these are permission change events
        is_delete_event: Whether these are delete events
    
    Returns:
        SQL query string
    """
    if not filters:
        return None
    
    # Build CASE statements for object_id, object_name, and object_type
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
        
        remediation_cases.append(f"WHEN {condition} THEN '{f.remediation_action}'")
        if (f.service_name == 'clusters' and f.action_name in ('create', 'createResult')):
            condition = f"""({condition}) AND
                    NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
                    AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
                    AND NOT (
                        request_params.kind='SERVERLESS_SQL_WAREHOUSE' AND request_params.cluster_creator='SQL_SERVICE'
                        OR request_params.kind='SERVERLESS_PREVIEW' AND request_params.cluster_creator='COMPUTE_GATEWAY_LAUNCHER'
                        OR request_params.kind='SERVERLESS_REPL_VM' AND request_params.cluster_creator='REPL_LAUNCHER'
                        )"""
        elif (f.service_name == 'clusters' and f.action_name == 'delete'):
            condition = f"""({condition} AND
                    NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
                    AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
                    )"""
        elif (f.service_name == 'clusters' and f.action_name == 'changeClusterAcl'):
            condition = f"""({condition}) AND request_params.resourceId IN 
                    (select distinct cluster_id from system.compute.clusters where cluster_source IN ('API','UI') 
                    {f'''and workspace_id IN ('{workspace_ids_str}')''' if workspace_ids_str else ''})
                    """
        elif (f.service_name == 'jobs' and f.action_name == 'changeJobAcl'):
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
                            AND (NOT EXISTS (SELECT 1 FROM SYSTEM.ACCESS.AUDIT WHERE REQUEST_ID = A.REQUEST_ID AND SERVICE_NAME = 'jobs' AND ACTION_NAME = 'submitRun' AND EVENT_DATE = A.EVENT_DATE)
                            AND NOT NVL(REQUEST_PARAMS.aclPermissionSet,'x') = 'Owner') 
                        """
        #elif (f.service_name == 'notebook' and f.action_name == 'createNotebook') or (f.service_name == 'workspace' and f.action_name == 'createFile'):
        #    condition = f"""({condition} 
        #            AND NOT (
        #                concat('/Workspace', request_params.path) LIKE '/Workspace/Repos/%' AND REGEXP_COUNT(request_params.path,'/') > 3
        #                OR concat('/Workspace', request_params.path) LIKE '/Workspace/Users/%' AND REGEXP_COUNT(request_params.path,'/') > 2
        #                OR request_params.path LIKE '/Users/%' AND REGEXP_COUNT(request_params.path,'/') >= 2
        #                OR concat('/Workspace', request_params.path) LIKE '/Workspace/%' AND REGEXP_COUNT(concat('/Workspace', request_params.path),'/') > 2 AND NOT SPLIT_PART(concat('/Workspace', request_params.path),'/',3) IN ('Users','Repos')
        #            )
        #        )"""
        #elif (f.service_name == 'workspace' and f.action_name == 'fileDelete'):
        #    condition = f"""({condition} 
        #            AND NOT (
        #            ---/Workspace/Users/
        #            CASE WHEN request_params.path LIKE '/Workspace/%' THEN request_params.path else concat('/Workspace', request_params.path) #END LIKE '/Workspace/Users/%'
        #            ---/Workspace/Repos/.../..
        #            OR REGEXP_COUNT(CASE WHEN request_params.path LIKE '/Workspace/%' THEN request_params.path else concat('/Workspace', #request_params.path) END,'/') > 3 AND CASE WHEN request_params.path LIKE '/Workspace/%' THEN request_params.path else concat('/Workspace', request_params.path) END like '/Workspace/Repos/%'
        #            ---/Workspace/otherFolders/...
        #            OR REGEXP_COUNT(CASE WHEN request_params.path LIKE '/Workspace/%' THEN request_params.path else concat('/Workspace', request_params.path) END,'/') > 3 
        #            AND NOT CASE WHEN request_params.path LIKE '/Workspace/%' THEN request_params.path else concat('/Workspace', request_params.path) END like '/Workspace/Repos/%' 
        #            AND NOT CASE WHEN request_params.path LIKE '/Workspace/%' THEN request_params.path else concat('/Workspace', request_params.path) END LIKE '/Workspace/Users/%'
        #            )
        #        )"""
        elif (f.service_name == 'unityCatalog' and f.action_name == 'createTable'):
            condition = f"""({condition}
                ----Tablas temporales que solo existen por un corto periodo, se crean/eliminan de forma automática
                AND NOT (user_identity.email = 'm59079@mx.att.com' and (request_params.full_name_arg like 'ccampaignsp.%.ztbl_tmplmk_%' or request_params.full_name_arg like 'ccampaignsp.%.ztbl_tmpsms_%'))
                )"""
        #elif (f.service_name == 'filesystem' and (f.action_name == 'filesDelete' or f.action_name == 'directoriesDelete')):
        #    condition = f"""({condition})
        #        AND NOT (
        #            request_params.path like '/Volumes/%'    
        #        )"""
        #elif (f.service_name == 'filesystem' and f.action_name == 'createDownloadUrl'):
        #    condition = f"""({condition})
        #    AND NOT (
        #            request_params.path like '/Volumes/%'    
        #        )"""
        elif (f.service_name == 'jobs' and f.action_name == 'delete'):
            condition = f"""({condition})
                ---Se excluyen operaciones internas
                AND NOT NVL(user_identity.email,'X') = '829e635d-eb80-4c3f-ac0c-9add9c31c557'
                """
        else:
            condition = f"({condition})"

        action_conditions.append(f"({condition})")
    
    # Build CASE SQL
    object_id_sql = "CASE \n            " + "\n            ".join(object_id_cases) + "\n            ELSE 'unknown'\n        END"
    object_name_sql = "CASE \n            " + "\n            ".join(object_name_cases) + "\n            ELSE 'unknown'\n        END"
    object_type_sql = "CASE \n            " + "\n            ".join(object_type_cases) + "\n            ELSE 'unknown'\n        END"
    remediation_sql = "CASE \n            " + "\n            ".join(remediation_cases) + "\n            ELSE 'ATTENTION_REQUIRED'\n        END"
    action_filter_sql = " \n\t\t\tOR ".join(action_conditions)

    excl_services_name = ','.join(list(map(lambda x: f"'{x}'", [evento for evento in set(service_names)])))
    excl_acls = ','.join(list(map(lambda x: f"'{x}'", [acl for acl in set(all_acls)])))

    default_filter = ""
    if is_delete_event:
        # Exclude notebook/folder/repo deletions from personal workspaces
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike '%delete%' AND NOT SERVICE_NAME IN ({excl_services_name}))"
    elif is_permission_change:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ilike 'change%Acl' AND NOT ACTION_NAME IN ({excl_acls}))"
    elif is_entitlement_change:
        default_filter = ""
    else:
        default_filter = f" \n\t\t\tOR (ACTION_NAME ILIKE '%create%' AND NOT SERVICE_NAME IN ({excl_services_name}))"
    
    # Build identity filter (identity_filter_str is already escaped and comma-separated)
    if identity_filter_str:
        #identity_filter = f"user_identity.email NOT IN ('{identity_filter_str}')"
        identity_filter = f"NOT user_identity.email IN ('{identity_filter_str}')"
    else:
        identity_filter = "1=1"
    
    # Build exclusion for personal workspace deletions
    personal_workspace_exclusion = ""
    if is_delete_event:
        personal_workspace_exclusion = """        
        --Tablas de LiveMKT que son temporales
        AND NOT (
            service_name == 'unityCatalog'  and action_name == 'deleteTable' 
            and user_identity.email = 'm59079@mx.att.com' 
            and (request_params.full_name_arg like 'ccampaignsp.%.ztbl_tmplmk_%' or request_params.full_name_arg like 'ccampaignsp.%.ztbl_tmpsms_%')
            )
        ---tablas temporales creadas durante la ingesta por DataFactory
        AND NOT (
            service_name == 'unityCatalog'  and action_name == 'deleteTable' 
            AND user_identity.email IN ('c45db2e0-8f92-4897-852e-c082d168a7de','efb9c1a7-0a94-4450-8c82-6a2b8d69174a') 
            AND request_params.full_name_arg like '%.%.tmp_%'
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
        {f'''AND workspace_id IN ('{workspace_ids_str}')''' if workspace_ids_str else ''}
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
# MAGIC ## Query Audit Logs for Create Events

# COMMAND ----------

create_query = build_audit_query_from_filters(
    create_filters, 
    workspace_ids_str, 
    lookback_hours,
    resource_approved_identities_str,
    is_permission_change=False,
    is_delete_event=False,
    is_entitlement_change=False
)

if create_query:
    if show_query:
        print(create_query)

    create_events_df = spark.sql(create_query)
    print(f"\n\n✓ [{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Initial Result:")
    if flagSERVERLESS == False:
        create_events_df = create_events_df.cache()

    create_events_df.display()

    # Filter out events with unknown object_id and System-User
    create_events_parsed_df = create_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User")
    )
    
    # Phase 2: Apply approved_actions filtering for identities with restrictions
    # Using native Spark operations (no UDF) for better performance
    if resource_identities_with_restrictions:
        print(f"\nApplying approved_actions filter for {len(resource_identities_with_restrictions)} restricted identities...")
        
        # Create a DataFrame of allowed (identity, object_type) pairs
        # This represents what each restricted identity IS allowed to create
        allowed_pairs = []
        for identity, actions in resource_identities_with_restrictions.items():
            for action in actions:
                # Store both original case and lowercase for matching
                allowed_pairs.append((identity, action.lower()))
        
        # Create DataFrame with allowed combinations
        allowed_df = spark.createDataFrame(
            allowed_pairs, 
            ["allowed_identity", "allowed_object_type"]
        )
        
        # Add lowercase object_type column for case-insensitive matching
        create_events_with_lower = create_events_parsed_df.withColumn(
            "object_type_lower", lower(col("object_type"))
        )
        
        # Left anti-join to find violations:
        # - Events from restricted identities where (user_email, object_type) is NOT in allowed pairs
        # - Events from unapproved identities (no match in restrictions) remain as violations
        
        # First, identify events from restricted identities that ARE allowed (to exclude them)
        allowed_events = create_events_with_lower.join(
            allowed_df,
            (create_events_with_lower["user_email"] == allowed_df["allowed_identity"]) &
            (create_events_with_lower["object_type_lower"] == allowed_df["allowed_object_type"]),
            "inner"
        ).select(create_events_with_lower["event_id"])
        
        # Exclude allowed events - remaining events are violations
        create_events_parsed_df = create_events_with_lower.join(
            allowed_events,
            on="event_id",
            how="left_anti"
        ).drop("object_type_lower")
        
        print(f"After approved_actions filtering: {create_events_parsed_df.count()} events remain as violations")
        
        #Exclusion by path (request_params.path)
        path_exclusiones = []
        for row in create_events_parsed_df.filter(f"object_type in ({objects_with_path})").collect():
            res, new_path = _is_path_excluded(row.object_type, row.event_id, row.request_params['path'])
            path_exclusiones.append([row.workspace_id, row.evebt_id, new_path, res])

        dfExclud = spark.createDataFrame(path_exclusiones, "workspace_id string, event_id string, new_path string, is_excluded boolean")
        print("\ndfExclud:")
        dfExclud.display()

        create_events_parsed_df1 = create_events_parsed_df.alias("eventos").join(dfExclud.alias("paths"), on=["event_id","workspace_id"], how="left").selectExpr("eventos.*","paths.new_path","paths.is_excluded")
        print("\ncreate_events_parsed_df1:")
        create_events_parsed_df1.display()
        create_events_parsed_df = create_events_parsed_df1.withColumn("is_excluded", when(col("is_excluded").isNull(), lit(False).cast("boolean")).otherwise(col("is_excluded"))).filter("is_excluded = false").drop("new_path","is_excluded")

    create_events_count = create_events_parsed_df.count()
    print(f"⚠ Found {create_events_count} create events by unauthorized identities (after approved_actions check)")

    if create_events_count > 0:
        print("\n\nSummary by service_name/action_name:")
        create_events_parsed_df.groupBy("service_name","action_name").count().orderBy("count", ascending=False).display()
        print("\n\n✅ Final output:")
        create_events_parsed_df.display()

        print(f"\n\nSummay by user_name/display name [From initial result]")
        create_events_parsed_df.alias('evt').join(dfInfoUsers.alias('u'),on = [create_events_parsed_df.user_email == dfInfoUsers.USER_NAME], how = 'left').groupBy("evt.user_email","u.DISPLAY_NAME","u.OWNER_SUITS").count().orderBy("count", ascending=False).display()
else:
    create_events_parsed_df = None
    create_events_count = 0
    raise Exception ("No create filters defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Logs for ACL Change Events

# COMMAND ----------

acl_query = build_audit_query_from_filters(
    acl_filters, 
    workspace_ids_str, 
    lookback_hours,
    permission_approved_identities_str,
    is_permission_change=True,
    is_delete_event=False,
    is_entitlement_change=False
)

if acl_query:
    if show_query:
        print(acl_query)

    acl_events_df = spark.sql(acl_query)
    print(f"\n\n✓ [{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Initial Result:")
    if flagSERVERLESS == False:
        acl_events_df = acl_events_df.cache()

    acl_events_df.display()

    # Filter out events with unknown object_id and System-User
    acl_events_parsed_df = acl_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User")
    )
    
    acl_events_count = acl_events_parsed_df.count()
    print(f"⚠ Found {acl_events_count} ACL change events by unauthorized identities")

    if acl_events_count > 0:
        print("\n\nSummary by service_name/action_name:")
        acl_events_df.groupBy("service_name","action_name").count().orderBy("count", ascending=False).display()

        print("\n\n✅ Final output:")
        acl_events_parsed_df.display()

        #Summay by user_name/display name
        acl_events_parsed_df.alias('evt').join(dfInfoUsers.alias('u'),on = [acl_events_parsed_df.user_email == dfInfoUsers.USER_NAME], how = 'left').groupBy("evt.user_email","u.DISPLAY_NAME","u.OWNER_SUITS").count().orderBy("count", ascending=False).display()    
else:
    acl_events_parsed_df = None
    acl_events_count = 0
    print("No ACL change filters defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Logs for Entitlement Change Events

# COMMAND ----------

entitlement_query = build_audit_query_from_filters(
    entitlement_filters,
    workspace_ids_str,
    lookback_hours,
    permission_approved_identities_str,
    is_permission_change=False,
    is_delete_event=False,
    is_entitlement_change=True
)

if entitlement_query:
    if show_query:
        print(entitlement_query)
    
    entitlement_events_df = spark.sql(entitlement_query)
    print(f"\n\n✓ [{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Initial Result:")
    if flagSERVERLESS == False:
        entitlement_events_df = entitlement_events_df.cache()

    entitlement_events_df.display()

    # Resolve missing object_type/object_name for workspace admin filters using workspace client
    def _resolve_identity_by_id(client: WorkspaceClient, identity_id: str) -> Tuple[str, str]:
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
    
    missing_identity_ids = (
        entitlement_events_df.filter(
            (
                col("object_type").isNull() |
                (trim(col("object_type")) == "identity_replace") |
                (col("object_type") == "unknown") |
                col("object_name").isNull() |
                (trim(col("object_name")) == "") |
                (col("object_name") == "unknown")
            )
        )
        .select("object_id", "workspace_id")
        .distinct()
        .collect()
    )
    
    if missing_identity_ids:
        resolved_rows = []
        for row in missing_identity_ids:
            resolved_type, resolved_name = _resolve_identity_by_id(client, row.object_id)
            if resolved_type or resolved_name:
                resolved_rows.append((row.object_id, resolved_type, resolved_name))
        
        if resolved_rows:
            resolved_df = spark.createDataFrame(
                resolved_rows,
                ["object_id", "resolved_object_type", "resolved_object_name"]
            )
            
            entitlement_events_df = (
                entitlement_events_df.alias("events")
                .join(resolved_df.alias("resolved"), on="object_id", how="left")
                .withColumn(
                    "object_type",
                    when(
                        col("events.object_type").isNull() |
                        (trim(col("events.object_type")) == "identity_replace") |
                        (col("events.object_type") == "unknown"),
                        col("resolved.resolved_object_type")
                    ).otherwise(col("events.object_type"))
                )
                .withColumn(
                    "object_name",
                    when(
                        col("events.object_name").isNull() |
                        (trim(col("events.object_name")) == "") |
                        (col("events.object_name") == "unknown"),
                        col("resolved.resolved_object_name")
                    ).otherwise(col("events.object_name"))
                )
                .drop("resolved_object_type", "resolved_object_name")
            )
    
    print("\n\n✓ Initial Result:")
    entitlement_events_df.display()
    
    # Filter out events with unknown object_id and System-User
    entitlement_events_parsed_df = entitlement_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User") &
        (col("object_name").isNotNull() | col("object_type").isNotNull())
    )
    
    entitlement_events_count = entitlement_events_parsed_df.count()
    print(f"⚠ Found {entitlement_events_count} entitlement change events by unauthorized identities")

    if entitlement_events_count > 0:
        entitlement_events_df.groupBy("service_name","action_name").count().orderBy("count", ascending=False).display()

        print("\n\n✅ Final output:")
        entitlement_events_parsed_df.display()

        #Summay by user_name/display name
        entitlement_events_parsed_df.alias('evt').join(dfInfoUsers.alias('u'),on = [entitlement_events_parsed_df.user_email == dfInfoUsers.USER_NAME], how = 'left').groupBy("evt.user_email","u.DISPLAY_NAME","u.OWNER_SUITS").count().orderBy("count", ascending=False).display()    
else:
    entitlement_events_parsed_df = None
    entitlement_events_count = 0
    print("No active entitlement change filters defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Logs for Delete Events

# COMMAND ----------

delete_query = build_audit_query_from_filters(
    delete_filters, 
    workspace_ids_str, 
    lookback_hours,
    #resource_approved_identities_str,
    "ALL", #Derivado de cualquier eliminación, la base de objetos aprovados debe actualizarse
    is_permission_change=False,
    is_delete_event=True,
    is_entitlement_change=False
)

if delete_query:
    if show_query:
        print(delete_query)

    delete_events_df = spark.sql(delete_query)
    print(f"\n\n✓ [{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Initial result:")
    if flagSERVERLESS == False:
        delete_events_df = delete_events_df.cache()

    delete_events_df.display()
    
    # Filter out events with unknown object_id and System-User
    delete_events_parsed_df = delete_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User")
    )
    
    delete_events_count = delete_events_parsed_df.count()
    print(f"⚠ Found {delete_events_count} delete events by unauthorized identities")

    #Exclusion by path (request_params.path)
    path_exclusiones = []
    for row in delete_events_df.filter(f"object_type in ({objects_with_path})").collect():
        res, new_path = _is_path_excluded(row.object_type, row.event_id, row.request_params['path'])
        path_exclusiones.append([row.workspace_id, row.event_id, new_path, res])

    dfExclud = spark.createDataFrame(path_exclusiones, "workspace_id string, event_id string, new_path string, is_excluded boolean")
    print("\ndfExclud:")
    dfExclud.display()

    delete_events_parsed_df1 = delete_events_df.alias("eventos").join(dfExclud.alias("paths"), on=["event_id","workspace_id"], how="left").selectExpr("eventos.*","paths.new_path","paths.is_excluded").withColumn("is_excluded", when(col("is_excluded").isNull(), lit(False).cast("boolean")).otherwise(col("is_excluded")))
    print("\ndelete_events_parsed_df1:")
    delete_events_parsed_df1.display()
    delete_events_parsed_df = delete_events_parsed_df1.filter("is_excluded = false").drop("new_path","is_excluded")

    delete_events_count = delete_events_parsed_df.count()

    if delete_events_count > 0:
        print("\n\nSummary by service_name/action_name:")
        delete_events_df.groupBy("service_name","action_name").count().orderBy("count", ascending=False).display()

        print("\n\n✅ Final output:")
        delete_events_parsed_df.display()

        #Summay by user_name/display name
        delete_events_parsed_df.alias('evt').join(dfInfoUsers.alias('u'),on = [delete_events_parsed_df.user_email == dfInfoUsers.USER_NAME], how = 'left').groupBy("evt.user_email","u.DISPLAY_NAME","u.OWNER_SUITS").count().orderBy("count", ascending=False).display()
else:
    delete_events_parsed_df = None
    delete_events_count = 0
    print("No delete filters defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Logs for Unity Catalog Object Changes

# COMMAND ----------

object_changes_query = build_audit_query_from_filters(
    object_changes_filters,
    workspace_ids_str=None, # All workspaces
    lookback_hours=lookback_hours,
    identity_filter_str=permission_approved_identities_str,
    is_permission_change=False,
    is_delete_event=False,
    is_entitlement_change=False
)

if object_changes_query:
    if show_query:
        print(object_changes_query)
    
    object_changes_events_df = spark.sql(object_changes_query).filter(coalesce(col('request_params.dry_run'), 'false') != 'true')
    
    object_changes_events_count = object_changes_events_df.count()
    print(f"⚠ Found {object_changes_events_count} object changes events by unauthorized identities")
    object_changes_events_df.groupBy("service_name","action_name").count().orderBy("count", ascending=False).display()

    #Summay by user_name/display name
    object_changes_events_df.alias('evt').join(dfInfoUsers.alias('u'),on = [object_changes_events_df.user_email == dfInfoUsers.USER_NAME], how = 'left').groupBy("evt.user_email","u.DISPLAY_NAME","u.OWNER_SUITS").count().orderBy("count", ascending=False).display()    
else:
    object_changes_events_df = None
    object_changes_events_count = 0
    print("No active object changes filters defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Create Events with Pre-Approved Objects

# COMMAND ----------

unapproved_creation_count = 0

if create_events_parsed_df and create_events_count > 0:
    # Load pre-approved objects
    preapproved_objects_df = spark.sql(f"""
        SELECT 
            workspace_id,
            object_id,
            object_type,
            owner_email
        FROM {catalog}.{schema}.governance_preapproved_objects
        WHERE is_active = true
    """)
    
    # Left join to identify objects NOT in preapproved list
    events_with_approval_df = create_events_parsed_df.alias("events").join(
        preapproved_objects_df.alias("approved"),
        (col("events.workspace_id") == col("approved.workspace_id")) &
        (col("events.object_id") == col("approved.object_id")),
        "left"
    ).select(
        col("events.*"),
        col("approved.object_id").alias("approved_object_id")
    )
    
    # Filter to only unapproved objects
    unapproved_creation_events_df = events_with_approval_df.filter(
        col("approved_object_id").isNull()
    ).drop("approved_object_id")
    
    unapproved_creation_count = unapproved_creation_events_df.count()
    print(f"Unapproved creation events: {unapproved_creation_count}")
else:
    unapproved_creation_events_df = None
    print("No creation events to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Violation Events

# COMMAND ----------

# Start with unapproved creations
all_events = []

if unapproved_creation_events_df and unapproved_creation_count > 0:
    all_events.append(unapproved_creation_events_df)
    
# Add ACL change events (all are violations since already filtered by unauthorized identities)
if acl_events_parsed_df and acl_events_count > 0:
    all_events.append(acl_events_parsed_df)

# Add entitlement change events (all are violations since already filtered by unauthorized identities)
if entitlement_events_parsed_df and entitlement_events_count > 0:
    all_events.append(entitlement_events_parsed_df)

# Add delete events (all are violations since already filtered by unauthorized identities)
if delete_events_parsed_df and delete_events_count > 0:
    all_events.append(delete_events_parsed_df)

# Add object changes events (all are violations since already filtered by unauthorized identities)
if object_changes_events_df and object_changes_events_count > 0:
    all_events.append(object_changes_events_df)

if all_events:
    # Union all event DataFrames using unionByName for schema safety
    all_violation_events_df = all_events[0]
    for df in all_events[1:]:
        all_violation_events_df = all_violation_events_df.unionByName(df)
    
    total_events_count = all_violation_events_df.count()
else:
    all_violation_events_df = None
    total_events_count = 0

print(f"\nTotal violation events: {total_events_count}")
print(f"  - Unapproved creations: {unapproved_creation_count}")
print(f"  - Unauthorized ACL changes: {acl_events_count}")
print(f"  - Unauthorized entitlement changes: {entitlement_events_count}")
print(f"  - Unauthorized deletions: {delete_events_count}")
print(f"  - Object changes events: {object_changes_events_count}")

if total_events_count == 0:
    print("\nNo violations detected. Exiting.")

    dbutils.notebook.exit('{"status": "SUCCESS", "violations_detected": 0}')

# COMMAND ----------

#WA for reclassification of objects (Alerts/Alertsv2/Dashboard/Dashboardv3)
if flagSERVERLESS == False:
    all_violation_events_df = all_violation_events_df.cache()

all_violation_events_df = all_violation_events_df.withColumn(
    "object_type", 
    when(
        (col("object_type") == "dashboard") & (~col("object_id").like("%-%")), 
        lit("lakeview_dashboard")
    ).otherwise(col("object_type"))) \
    .withColumn(
    "object_type", 
    when(
        (col("object_type") == "alert") & (~col("object_id").like("%-%")), 
        lit("alertsv2")
    ).otherwise(col("object_type")))

all_violation_events_df.select("event_id","object_id","object_type","object_name","workspace_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Violations for Staging

# COMMAND ----------

# Get workspace configurations
workspace_configs = {}
for row in enabled_workspaces_df.collect():
    workspace_configs[row.workspace_id] = row.asDict()

print(f"Loaded {len(workspace_configs)} workspace configuration(s)")

# Get unique workspace IDs from violations and create clients upfront
#violation_workspace_ids = set(row.workspace_id for row in all_violation_events_df.select("workspace_id").distinct().collect())
violation_workspace_ids = set(row.workspace_id for row in enabled_workspaces_df.select("workspace_id").distinct().collect())
print(f"Violations span {len(violation_workspace_ids)} workspace(s): {violation_workspace_ids}")

# Create workspace clients for each workspace with violations
workspace_clients = {}
for ws_id in violation_workspace_ids:
    config = workspace_configs.get(ws_id, {})
    workspace_url = config.get('workspace_url')
    
    # Check if we have Key Vault scope configured and workspace URL
    if kv_scope and workspace_url:
        try:
            workspace_clients[ws_id] = create_workspace_client(workspace_url)
            print(f"  ✓ Created client for workspace {ws_id} ({workspace_url})")
            print(f"    {workspace_clients[ws_id]}")
        except Exception as e:
            print(f"  ✗ Failed to create client for {ws_id}: {e}, using default")
            workspace_clients[ws_id] = WorkspaceClient()
    else:
        print(f"  → Using default client for workspace {ws_id}")
        workspace_clients[ws_id] = WorkspaceClient()

# COMMAND ----------

dfFolders = spark.sql(f"SELECT WORKSPACE_ID, OBJECT_ID, OBJECT_NAME,ROW_NUMBER() OVER (PARTITION BY WORKSPACE_ID, OBJECT_ID ORDER BY CTRL_INGEST_DATE DESC) RN FROM SCH_MNG_RESPALDOS.TBL_SNAP_GRANTS WHERE CTRL_INGEST_DATE>=CURRENT_DATE()-1 AND TIPO_OBJETO = 'WORKSPACE-DIRECTORY'").filter(("RN=1"))

IDs = {}
for row in dfFolders.collect():
    IDs[f"{row.WORKSPACE_ID}-{row.OBJECT_ID}"] = row.OBJECT_NAME

print(f"Registros: {len(IDs)}")

# COMMAND ----------

def _resolve_workspace_path(client: WorkspaceClient, object_type: str, object_id: str) -> str:

    """
    Best-effort workspace path resolution for workspace-scoped objects.
    Returns empty string when lookup fails.
    """
    
    path = ""
    error_msg = None

    try:
        if object_type == "query":
            try:
                path = client.queries.get(object_id).parent_path
            except Exception as e:
                if 'draft query' in str(e):
                    path = "/Workspace/Users/user"
                else:
                    path = ""
                    error_msg = str(e)
        elif object_type == "dashboard" or object_type == "lakeview_dashboard":
            try:
                # Try Lakeview dashboard first (newer AI/BI dashboards)
                path = client.lakeview.get(dashboard_id=object_id).parent_path                
            except Exception as e:
                error_msg = str(e)

                try:
                    parent = client.dashboards.get(dashboard_id=object_id).parent
                    path = IDs.get(f"{client.get_workspace_id()}-{parent.split('/')[1]}", "")
                    error_msg = None
                except Exception as e:
                    path = ""
                    error_msg = str(e)
        #if object_type == "lakeview_dashboard":
        #    path = client.lakeview.get(dashboard_id=object_id).parent_path
        elif object_type in ["alert", "alerts", "alertsv2"]:
            try:
                path = client.alerts_v2.get_alert(id=object_id).parent_path
            except Exception as e:
                error_msg = str(e)

                try:
                    path = client.alerts.get(id=object_id).parent_path
                    error_msg = None
                except Exception as e:
                    path = ""
                    error_msg = str(e)
        elif object_type == "alertsv2":
            path = client.alerts_v2.get_alert(id=object_id).parent_path
        elif object_type == "mlflowExperiments":
            path = client.experiments.get_experiment(experiment_id=object_id).experiment.name
        elif object_type == "repo" or object_type=="repos":
            path = client.repos.get(repo_id=object_id).path
        else:
            print(f"{object_type} path resolution not implemented")
            path = ""
    except Exception as e:
        print(f"Warning: could not resolve path for {object_type} {object_id}: {str(e)}")
        path = ""
        error_msg = str(e)

    return path.strip(), error_msg

def _is_personal_workspace(path: str) -> bool:
    return path.startswith("/Workspace/Users/") or path.startswith("/Users/")

# COMMAND ----------

# Filter out violations in personal workspace paths for selected object types
workspace_path_object_types = [
    "dashboard",
    "lakeview_dashboard",
    "query",
    "alert",
    "alerts",
    "alertsv2",
    "mlflowExperiments",
    "repo",
    "repos"
]

object_filters = ','.join(list(map(lambda x: f"'{x}'", [x for x in workspace_path_object_types])))
print(f"[{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Objects type to filter: {object_filters}")

if flagSERVERLESS == False:
   all_violation_events_df = all_violation_events_df.cache()

if all_violation_events_df and total_events_count > 0:
    candidate_rows = all_violation_events_df.filter(f"object_type in ({object_filters})").select("object_id", "object_type","workspace_id").distinct().collect()
    
    print(f"[{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Candidate rows:")
    display(candidate_rows)

    personal_objects = []
    no_personal_objects = []

    for row in candidate_rows:        
        workspace_id = row['workspace_id']
        
        # Get the pre-created client for this workspace
        client = workspace_clients.get(workspace_id, WorkspaceClient())

        path, error_msg = _resolve_workspace_path(client, row.object_type, row.object_id)
        is_personal = _is_personal_workspace(path)

        if is_personal:
            #print(f"Se agrega {row.object_id} | Personal")
            personal_objects.append([row.object_id, path, workspace_id, error_msg])
        else:
            #print(f"Se agrega {row.object_id} | No personal")
            no_personal_objects.append([row.object_id, path, workspace_id, error_msg])

    if len(candidate_rows) > 0:
        dfPersonal = spark.createDataFrame(personal_objects,"object_id string, path string, workspace_id string, error_msg string")
        print("\nObjects in personal folders")
        display(dfPersonal)
        
        print("\nObjects not in personal folders (To delete)")
        dfNoPersonal = spark.createDataFrame(no_personal_objects,"object_id string, path string, workspace_id string, error_msg string")
        display(dfNoPersonal)

    if personal_objects:
        #res1 = all_violation_events_df.alias("events").join(
        #    dfPersonal.alias("paths"), on=["object_id","workspace_id"], how="left"
        #).withColumn(
        #    "is_personal_workspace", when(col("paths.object_id").isNotNull(), lit(True)).otherwise(lit(False)).cast("boolean")
        #)
        #res1.display()

        all_violation_events_df1 = all_violation_events_df.alias("events").join(
            dfPersonal.alias("paths"), on=["object_id","workspace_id"], how="left"
        ).withColumn(
            "is_personal_workspace", when(col("paths.object_id").isNotNull(), lit(True)).otherwise(lit(False)).cast("boolean")
        ).select("events.*", "is_personal_workspace")
    else:
        all_violation_events_df1 = all_violation_events_df.withColumn(
            "is_personal_workspace", lit(False).cast("boolean")
        )
        
    before_filter = total_events_count
    if flagSERVERLESS == False:
        all_violation_events_df1 = all_violation_events_df1.cache()
    
    all_violation_events_df = all_violation_events_df1.filter(~col("is_personal_workspace"))
    print(f"✅ [{datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}] Resultado final")
    display(all_violation_events_df)

    #print(f"result 2nd {datetime.now(tz).isoformat()}")
    total_events_count = all_violation_events_df.count()
    #print(f"filtered_out {datetime.now(tz).isoformat()}")
    filtered_out = before_filter - total_events_count
    print(f"Filtered {filtered_out} personal workspace violation(s)")

    if total_events_count == 0:
        print("\nNo violations detected after personal workspace filtering. Exiting.")
        dbutils.notebook.exit('{"status": "SUCCESS", "violations_detected": 0}')

# Select and transform violation fields
violations_df = all_violation_events_df.select(
    col("event_id"),
    col("workspace_id"),
    col("event_time"),
    col("service_name"),
    col("action_name"),
    col("user_email"),
    col("object_id"),
    col("object_type"),
    col("object_name"),
    col("is_permission_change"),
    col("is_delete_event"),
    col("is_entitlement_change"),
    col("remediation_action"),
    col("request_id")
)
#print(f"display {datetime.now(tz).isoformat()}")
#display(violations_df)

violations_count = violations_df.count()

# Add metadata for violations
violations_staging_df = violations_df.withColumn(
    "violation_id", expr("uuid()")
).withColumn(
    "violation_type", 
    when(col("is_entitlement_change"), lit("UNAUTHORIZED_ENTITLEMENT_CHANGE"))
    .when(col("is_delete_event"), lit("UNAUTHORIZED_DELETION"))
    .when(col("is_permission_change"), lit("UNAUTHORIZED_PERMISSION_CHANGE"))
    .otherwise(lit("UNAPPROVED_CREATION"))
).withColumn(
    "violation_reason",
    when(
        col("is_entitlement_change"),
        concat(
            lit("Unauthorized entitlement change by: "),
            col("user_email"),
            lit(" on "),
            col("object_name")
        )
    )
    .when(
        col("is_delete_event"),
        concat(
            lit("Unauthorized deletion of "),
            col("object_type"),
            lit(" by: "),
            col("user_email"),
            lit(" (object_id: "),
            col("object_id"),
            lit(")")
        )
    )
    .when(
        col("is_permission_change"),
        concat(
            lit("Unauthorized permission change by: "),
            col("user_email"),
            lit(" on "),
            col("object_name")
        )
    )
    .otherwise(
        concat(
            lit("Object created by unauthorized user: "),
            col("user_email"),
            lit(" (object not pre-approved)")
        )
    )
).withColumn(
    "processing_status", 
    when(col("is_delete_event"), lit("PENDING_REPORT"))
    .otherwise(lit("PENDING"))
).withColumn(
    "processed_at", lit(None).cast("timestamp")
).withColumn(
    "created_at", current_timestamp()
)

# Filter out violations that should be skipped (but keep REPORT_DELETION)
violations_staging_df = violations_staging_df.filter(
    col("remediation_action") != "SKIP_REMEDIATION"
)

final_violations_count = violations_staging_df.count()

# Get counts by violation type
create_violations = violations_staging_df.filter(col("violation_type") == "UNAPPROVED_CREATION").count()
permission_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_PERMISSION_CHANGE").count()
delete_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_DELETION").count()
entitlement_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_ENTITLEMENT_CHANGE").count()
object_changes_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_OBJECT_CHANGE").count()

print(f"\nViolations by Type (after filtering):")
print(f"  - Unapproved Creations:           {create_violations}")
print(f"  - Unauthorized Permission Changes: {permission_violations}")
print(f"  - Unauthorized Deletions:          {delete_violations}")
print(f"  - Unauthorized Entitlement Changes: {entitlement_violations}")
print(f"  - Unauthorized object changes: {object_changes_violations}")
print(f"  - Total:                           {final_violations_count}")

# Show sample violations
if final_violations_count > 0:
    print("\nSample Violations Detected:")
    display(violations_staging_df.limit(100))

# COMMAND ----------

if final_violations_count > 0:
    staging_table = f"{catalog}.{schema}.governance_violations_staging"
    
    # Create temp view for merge operation
    violations_staging_df.createOrReplaceTempView("new_violations")
    
    # Merge with existing data - only insert new events
    spark.sql(f"""
        MERGE INTO {staging_table} AS target
        USING new_violations AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN 
            INSERT *
    """)
    
    print(f"✓ Processed {final_violations_count} violations")
    print(f"  - New violations merged to {staging_table}")
    print(f"  - Duplicate events (already existing) were skipped")
else:
    print("No violations to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("WATCHER SUMMARY")
print("="*80)
print(f"Governed Workspaces:              {len(enabled_workspace_ids)}")
print(f"\nGovernance Filters Loaded:")
print(f"  - Create filters:               {len(create_filters)}")
print(f"  - ACL change filters:           {len(acl_filters)}")
print(f"  - Delete filters:               {len(delete_filters)}")
print(f"  - Entitlement change filters:   {len(entitlement_filters)}")
print(f"\nApproved Identities for RESOURCE Management: {len(all_resource_approved_identities)}")
print(f"  - Direct Users:                 {len(resource_approved_users)}")
print(f"  - Direct Service Principals:    {len(resource_approved_service_principals)}")
print(f"  - Groups:                       {len(resource_approved_groups)}")
print(f"  - Users from Groups:            {len(resource_expanded_users)}")
print(f"  - SPs from Groups:              {len(resource_expanded_sps)}")
print(f"\nApproved Identities for PERMISSION Management: {len(all_permission_approved_identities)}")
print(f"  - Direct Users:                 {len(permission_approved_users)}")
print(f"  - Direct Service Principals:    {len(permission_approved_service_principals)}")
print(f"  - Groups:                       {len(permission_approved_groups)}")
print(f"  - Users from Groups:            {len(permission_expanded_users)}")
print(f"  - SPs from Groups:              {len(permission_expanded_sps)}")
print(f"\nAudit Events Found:")
print(f"  - Create Events:                {create_events_count}")
print(f"  - ACL Change Events:            {acl_events_count}")
print(f"  - Entitlement Change Events:    {entitlement_events_count}")
print(f"  - Delete Events:                {delete_events_count}")
print(f"  - Object changes events: {object_changes_events_count}")
print(f"\nViolations Detected:")
print(f"  - Unapproved Creations:         {create_violations}")
print(f"  - Unauthorized Permission Changes: {permission_violations}")
print(f"  - Unauthorized Deletions:       {delete_violations}")
print(f"  - Unauthorized Entitlement Changes: {entitlement_violations}")
print(f"  - Unauthorized object changes: {object_changes_violations}")
print(f"  - Total:                        {final_violations_count}")
print("="*80)

# Show breakdown by object type
if final_violations_count > 0:
    breakdown_df = spark.sql(f"""
        SELECT 
            object_type,
            violation_type,
            COUNT(*) as violation_count
        FROM {catalog}.{schema}.governance_violations_staging
        WHERE processing_status IN ('PENDING', 'PENDING_REPORT')
        GROUP BY object_type, violation_type
        ORDER BY violation_count DESC
    """)
    
    print("\nViolations by Object Type and Violation Type:")
    display(breakdown_df)

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'violations_detected': final_violations_count,
    'create_violations': create_violations,
    'permission_violations': permission_violations,
    'delete_violations': delete_violations,
    'entitlement_violations': entitlement_violations,
    'timestamp': datetime.now(tz).isoformat()
}, indent=3))