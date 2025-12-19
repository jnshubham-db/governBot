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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")
dbutils.widgets.text("lookback_hours", "200", "Lookback Hours for Audit Logs")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Lookback Hours: {lookback_hours}")

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
        max_retry_attempts
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

print(f"\nFilter breakdown:")
print(f"  - Create filters: {len(create_filters)}")
print(f"  - ACL change filters: {len(acl_filters)}")
print(f"  - Delete filters: {len(delete_filters)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Expand Pre-Approved Identities
# MAGIC
# MAGIC Identities are loaded with granular permission flags:
# MAGIC - **can_manage_resources**: Allowed to create/delete resources
# MAGIC - **can_manage_permissions**: Allowed to grant/revoke permissions

# COMMAND ----------

# Load pre-approved identities with permission flags
preapproved_identities_df = spark.sql(f"""
    SELECT 
        identity_name,
        identity_type,
        COALESCE(can_manage_resources, true) as can_manage_resources,
        COALESCE(can_manage_permissions, false) as can_manage_permissions
    FROM {catalog}.{schema}.governance_preapproved_identities
    WHERE is_active = true
""")

# Separate by type AND permission flags
resource_approved_users = []
resource_approved_service_principals = []
resource_approved_groups = []

permission_approved_users = []
permission_approved_service_principals = []
permission_approved_groups = []

for row in preapproved_identities_df.collect():
    identity_name = row.identity_name
    identity_type = row.identity_type
    can_manage_resources = row.can_manage_resources
    can_manage_permissions = row.can_manage_permissions
    
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

def expand_group_members(group_names: List[str], client) -> Tuple[List[str], List[str]]:
    """Expand group names to get member users and service principals."""
    expanded_users = []
    expanded_sps = []
    
    if not group_names:
        return expanded_users, expanded_sps
    
    print(f"\nExpanding {len(group_names)} group(s)...")    
    for group_name in group_names:
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

# Expand groups for resource management
resource_expanded_users, resource_expanded_sps = expand_group_members(resource_approved_groups, client)

# Expand groups for permission management
permission_expanded_users, permission_expanded_sps = expand_group_members(permission_approved_groups, client)

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

print(f"\nTotal approved identities for RESOURCE management: {len(all_resource_approved_identities)}")
print(f"Total approved identities for PERMISSION management: {len(all_permission_approved_identities)}")

# Build identity filter strings (escape single quotes to prevent SQL injection)
if not all_resource_approved_identities:
    print("WARNING: No approved identities for resource management. All creation/deletion events will be flagged.")
    resource_approved_identities_str = ""
else:
    # Escape single quotes in identity names for SQL safety
    escaped_resource_identities = [id.replace("'", "''") for id in all_resource_approved_identities]
    resource_approved_identities_str = "', '".join(escaped_resource_identities)

if not all_permission_approved_identities:
    print("WARNING: No approved identities for permission management. All permission changes will be flagged.")
    permission_approved_identities_str = ""
else:
    # Escape single quotes in identity names for SQL safety
    escaped_permission_identities = [id.replace("'", "''") for id in all_permission_approved_identities]
    permission_approved_identities_str = "', '".join(escaped_permission_identities)

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
    is_delete_event: bool = False
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
    
    for f in filters:
        condition = f"service_name='{f.service_name}' AND action_name='{f.action_name}'"
        
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
        if(f.service_name == 'clusters' and f.action_name in ('create', 'createResult')):
            condition = f"""({condition}) AND
             NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
             AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
            AND NOT (
                request_params.kind='SERVERLESS_SQL_WAREHOUSE' AND request_params.cluster_creator='SQL_SERVICE'
                    OR request_params.kind='SERVERLESS_PREVIEW' AND request_params.cluster_creator='COMPUTE_GATEWAY_LAUNCHER'
                    OR request_params.kind='SERVERLESS_REPL_VM' AND request_params.cluster_creator='REPL_LAUNCHER'
            )"""
        elif(f.service_name == 'clusters' and f.action_name == 'delete'):
            condition = f"""({condition} AND
            NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/jobs/%'
            AND NOT NVL(request_params.acl_path_prefix,'x') like '/clusters/pipelines/%'
            )"""
        elif(f.service_name == 'clusters' and f.action_name == 'changeClusterAcl'):
            condition = f"""({condition}) AND request_params.resourceId in 
                    (select distinct cluster_id from system.compute.clusters where cluster_source IN ('API','UI') 
                    and workspace_id IN ('{workspace_ids_str}'))
            """
        elif(f.service_name == 'jobs' and f.action_name == 'changeJobAcl'):
            condition = f"""({condition})
            --Exclusión por asignación de Owner desde DataFactory
            AND NOT (NVL(request_params.aclPermissionSet,'x') ='Owner' AND NVL(USER_AGENT,'x')='AzureDataFactory')
            """
        else:
            condition = f"({condition})"

        action_conditions.append(f"({condition})")
    
    # Build CASE SQL
    object_id_sql = "CASE \n            " + "\n            ".join(object_id_cases) + "\n            ELSE 'unknown'\n        END"
    object_name_sql = "CASE \n            " + "\n            ".join(object_name_cases) + "\n            ELSE 'unknown'\n        END"
    object_type_sql = "CASE \n            " + "\n            ".join(object_type_cases) + "\n            ELSE 'unknown'\n        END"
    remediation_sql = "CASE \n            " + "\n            ".join(remediation_cases) + "\n            ELSE 'SKIP_REMEDIATION'\n        END"
    action_filter_sql = " OR ".join(action_conditions)
    
    # Build identity filter (identity_filter_str is already escaped and comma-separated)
    if identity_filter_str:
        identity_filter = f"user_identity.email NOT IN ('{identity_filter_str}')"
    else:
        identity_filter = "1=1"
    
    # Build exclusion for personal workspace deletions
    personal_workspace_exclusion = ""
    if is_delete_event:
        personal_workspace_exclusion = """
        -- Exclude notebook/folder/repo deletions in personal workspace (/Workspace/Users/)
        AND NOT (
            service_name = 'notebook' 
            AND action_name IN ('deleteNotebook', 'deleteFolder', 'deleteRepo')
            AND request_params.path LIKE '/Workspace/Users/%'
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
        {remediation_sql} as remediation_action,
        request_params,
        response 
    FROM system.access.audit
    WHERE event_date >= current_date() - INTERVAL {lookback_hours} HOUR
        AND response.status_code IN (200, 201, 203, 204, 205, 206)
        AND workspace_id IN ('{workspace_ids_str}')
        AND user_identity.email IS NOT NULL
        AND ({action_filter_sql})
        AND {identity_filter}{personal_workspace_exclusion}
    ORDER BY event_time DESC
    """
    print(query)
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
    is_delete_event=False
)

if create_query:
    create_events_df = spark.sql(create_query)
    
    # Filter out events with unknown object_id and System-User
    create_events_parsed_df = create_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User")
    )
    
    create_events_count = create_events_parsed_df.count()
    print(f"Found {create_events_count} create events by unauthorized identities")
    create_events_parsed_df.display()
else:
    create_events_parsed_df = None
    create_events_count = 0
    print("No create filters defined")

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
    is_delete_event=False
)

if acl_query:
    acl_events_df = spark.sql(acl_query)
    
    # Filter out events with unknown object_id and System-User
    acl_events_parsed_df = acl_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User")
    )
    
    acl_events_count = acl_events_parsed_df.count()
    print(f"Found {acl_events_count} ACL change events by unauthorized identities")
else:
    acl_events_parsed_df = None
    acl_events_count = 0
    print("No ACL change filters defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Logs for Delete Events

# COMMAND ----------

delete_query = build_audit_query_from_filters(
    delete_filters, 
    workspace_ids_str, 
    lookback_hours,
    resource_approved_identities_str,
    is_permission_change=False,
    is_delete_event=True
)

if delete_query:
    delete_events_df = spark.sql(delete_query)
    
    # Filter out events with unknown object_id and System-User
    delete_events_parsed_df = delete_events_df.filter(
        (col("object_id") != "unknown") & 
        (col("user_email") != "System-User")
    )
    
    delete_events_count = delete_events_parsed_df.count()
    print(f"Found {delete_events_count} delete events by unauthorized identities")
else:
    delete_events_parsed_df = None
    delete_events_count = 0
    print("No delete filters defined")

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
    
# Add delete events (all are violations since already filtered by unauthorized identities)
if delete_events_parsed_df and delete_events_count > 0:
    all_events.append(delete_events_parsed_df)

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
print(f"  - Unauthorized deletions: {delete_events_count}")

if total_events_count == 0:
    print("\nNo violations detected. Exiting.")
    dbutils.notebook.exit('{"status": "SUCCESS", "violations_detected": 0}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Violations for Staging

# COMMAND ----------

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
    col("remediation_action")
)

violations_count = violations_df.count()

# Add metadata for violations
violations_staging_df = violations_df.withColumn(
    "violation_id", expr("uuid()")
).withColumn(
    "violation_type", 
    when(col("is_delete_event"), lit("UNAUTHORIZED_DELETION"))
    .when(col("is_permission_change"), lit("UNAUTHORIZED_PERMISSION_CHANGE"))
    .otherwise(lit("UNAPPROVED_CREATION"))
).withColumn(
    "violation_reason",
    when(
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

print(f"\nViolations by Type (after filtering):")
print(f"  - Unapproved Creations:           {create_violations}")
print(f"  - Unauthorized Permission Changes: {permission_violations}")
print(f"  - Unauthorized Deletions:          {delete_violations}")
print(f"  - Total:                           {final_violations_count}")

# Show sample violations
if final_violations_count > 0:
    print("\nSample Violations Detected:")
    display(violations_staging_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Violations to Staging Table

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
print(f"  - Delete Events:                {delete_events_count}")
print(f"\nViolations Detected:")
print(f"  - Unapproved Creations:         {create_violations}")
print(f"  - Unauthorized Permission Changes: {permission_violations}")
print(f"  - Unauthorized Deletions:       {delete_violations}")
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
    'timestamp': datetime.utcnow().isoformat()
}))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table sjdatabricks.governance.governance_preapproved_objects_bkp2 as select * from sjdatabricks.governance.governance_preapproved_objects
# MAGIC select s.*, a.request_params, a.response, a.user_identity from sjdatabricks.governance.governance_violations_staging s left join system.access.audit a on s.event_id = a.event_id 
# MAGIC --1082376942873335
# MAGIC --41fa5fb6-bd87-4196-bd3c-98a17470a329

# COMMAND ----------

# MAGIC %md
# MAGIC ### Doubts
# MAGIC - Cluster ACLs for serverless, how can we identify and ignore those.
# MAGIC - Cluster creation for job cluster needs to be excluded, cannot find any direct way.
# MAGIC - Cluster of serverless created also sends an audit log.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM sjdatabricks.governance.governance_violations_staging WHERE action_name IN ('createTable', 'createCatalog', 'createSchema')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- truncate table sjdatabricks.governance.governance_violations_staging
# MAGIC -- select * from

# COMMAND ----------


