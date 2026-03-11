# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Watcher (Thin - uses governbot_core wheel)
# MAGIC
# MAGIC Reads audit logs and detects violations. Config from env; install wheel then run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install wheel and parameters

# COMMAND ----------

# Install from workspace or dist (adjust path as needed)
# %pip install /Workspace/GovernBot/src/governbot_core-0.1.0-py3-none-any.whl

# COMMAND ----------
import os

try:
    lookback_hours = int(dbutils.widgets.get("lookback_hours"))
except Exception:
    lookback_hours = os.environ.get("LOOKBACK_HOURS_DEFAULT", "24")
try:
    show_query = dbutils.widgets.get("show_query") in ("Y", "S")
except Exception:
    show_query = False
try:
    load_filters = dbutils.widgets.get("load_filters") in ("Y", "S")
except Exception:
    load_filters = False
try:
    enable_discover = dbutils.widgets.get("enable_discover") in ("Y", "S")
except Exception:
    enable_discover = False
try:
    load_approved_id = dbutils.widgets.get("load_approved_id") in ("Y", "S")
except Exception:
    load_approved_id = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config and clients (from env)

# COMMAND ----------

import json
import pytz
from datetime import datetime

from governbot_core import (
    GovernanceConfig,
    ClientFactory,
    load_enabled_workspaces,
    load_filters,
    build_audit_query_from_filters,
)
from governbot_core.identities import (
    load_preapproved_identities,
    parse_identity_lists,
    expand_group_members,
)
from governbot_core.constants import (
    full_table_name,
    TABLE_VIOLATIONS_STAGING,
    VIOLATION_UNAPPROVED_CREATION,
    VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE,
    VIOLATION_UNAUTHORIZED_DELETION,
    VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE,
    VIOLATION_UNAUTHORIZED_OBJECT_CHANGE,
)
from governbot_core.permissions import resolve_identity_by_id

config = GovernanceConfig.from_env()
get_secret = lambda scope, key: dbutils.secrets.get(scope=scope, key=key)
factory = ClientFactory(config, get_secret)
tz = pytz.timezone("America/Mexico_City")
catalog = config.catalog
schema = config.schema

if load_filters or enable_discover:
    dbutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "reason": "load_filters or enable_discover set",
        "timestamp": datetime.now(tz).isoformat(),
    }, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load enabled workspaces

# COMMAND ----------

workspace_rows = load_enabled_workspaces(spark, catalog, schema)
if not workspace_rows:
    dbutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "reason": "No enabled workspaces",
        "timestamp": datetime.now(tz).isoformat(),
    }, indent=2))

enabled_workspace_ids = [row.workspace_id for row in workspace_rows]
workspace_urls = [row.workspace_url for row in workspace_rows]
workspace_ids_str = "', '".join(str(w) for w in enabled_workspace_ids)
workspace_clients = {}
for ws_id, ws_url in zip(enabled_workspace_ids, workspace_urls):
    try:
        workspace_clients[ws_id] = factory.create_workspace_client(ws_url)
    except Exception as e:
        print(f"Failed client for {ws_id}: {e}")
        workspace_clients[ws_id] = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load filters and identities

# COMMAND ----------

filters_by_type = load_filters(spark, catalog, schema)
create_filters = filters_by_type.get(VIOLATION_UNAPPROVED_CREATION, [])
acl_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE, [])
delete_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_DELETION, [])
entitlement_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE, [])
object_changes_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_OBJECT_CHANGE, [])

identity_rows = load_preapproved_identities(spark, catalog, schema)
parsed = parse_identity_lists(identity_rows)
resource_users = parsed["resource_approved_users"]
resource_sps = parsed["resource_approved_service_principals"]
resource_groups = parsed["resource_approved_groups"]
permission_users = parsed["permission_approved_users"]
permission_sps = parsed["permission_approved_service_principals"]
permission_groups = parsed["permission_approved_groups"]
identity_approved_actions = parsed["identity_approved_actions"]

client_for_groups = workspace_clients.get(enabled_workspace_ids[0]) if enabled_workspace_ids else None
if client_for_groups:
    resource_expanded_users, resource_expanded_sps = expand_group_members(resource_groups, client_for_groups, identity_approved_actions)
    permission_expanded_users, permission_expanded_sps = expand_group_members(permission_groups, client_for_groups, identity_approved_actions)
else:
    resource_expanded_users, resource_expanded_sps = [], []
    permission_expanded_users, permission_expanded_sps = [], []

all_resource = list(set(resource_users + resource_sps + resource_expanded_users + resource_expanded_sps))
all_permission = list(set(permission_users + permission_sps + permission_expanded_users + permission_expanded_sps))
resource_approved_identities_str = "', '".join(r.replace("'", "''") for r in all_resource)
permission_approved_identities_str = "', '".join(p.replace("'", "''") for p in all_permission)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and run audit queries (all violation types)

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, concat, current_timestamp, expr

def run_violation_query(query_sql, is_object_change=False):
    if not query_sql:
        return None
    df = spark.sql(query_sql)
    df = df.filter((col("object_id") != "unknown") & (col("user_email") != "System-User"))
    if is_object_change:
        df = df.withColumn("is_object_change", lit(True))
    else:
        df = df.withColumn("is_object_change", lit(False))
    return df

create_query = build_audit_query_from_filters(create_filters, workspace_ids_str, lookback_hours, resource_approved_identities_str, is_permission_change=False, is_delete_event=False, is_entitlement_change=False)
acl_query = build_audit_query_from_filters(acl_filters, workspace_ids_str, lookback_hours, permission_approved_identities_str, is_permission_change=True, is_delete_event=False, is_entitlement_change=False)
delete_query = build_audit_query_from_filters(delete_filters, workspace_ids_str, lookback_hours, resource_approved_identities_str, is_permission_change=False, is_delete_event=True, is_entitlement_change=False)
entitlement_query = build_audit_query_from_filters(entitlement_filters, workspace_ids_str, lookback_hours, permission_approved_identities_str, is_permission_change=False, is_delete_event=False, is_entitlement_change=True)
object_changes_query = build_audit_query_from_filters(object_changes_filters, workspace_ids_str=None, lookback_hours=lookback_hours, identity_filter_str=permission_approved_identities_str, is_permission_change=False, is_delete_event=False, is_entitlement_change=False)

if show_query and create_query:
    print("Create query (sample):", create_query)
if show_query and acl_query:
    print("ACL query (sample):", acl_query)
if show_query and delete_query:
    print("Delete query (sample):", delete_query)
if show_query and entitlement_query:
    print("Entitlement query (sample):", entitlement_query)
if show_query and object_changes_query:
    print("Object changes query (sample):", object_changes_query)

dfs = []
if create_query:
    dfs.append(run_violation_query(create_query))
if acl_query:
    dfs.append(run_violation_query(acl_query))
if delete_query:
    dfs.append(run_violation_query(delete_query))
if entitlement_query:
    entitlement_events_df = run_violation_query(entitlement_query)
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
            resolved_type, resolved_name = resolve_identity_by_id(workspace_clients[row.workspace_id], row.object_id)
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
    dfs.append(entitlement_events_df)
if object_changes_query:
    dfs.append(run_violation_query(object_changes_query, is_object_change=True))

if not dfs:
    all_violation_events_df = None
else:
    all_violation_events_df = dfs[0]
    for d in dfs[1:]:
        all_violation_events_df = all_violation_events_df.unionByName(d, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build staging DataFrame and merge to table

# COMMAND ----------

staging_table = full_table_name(catalog, schema, TABLE_VIOLATIONS_STAGING)
final_violations_count = 0
create_violations = permission_violations = delete_violations = entitlement_violations = object_changes_violations = 0

if all_violation_events_df is not None:
    violations_df = all_violation_events_df.select(
        col("event_id"), col("workspace_id"), col("event_time"), col("service_name"), col("action_name"),
        col("user_email"), col("object_id"), col("object_type"), col("object_name"),
        col("is_permission_change"), col("is_delete_event"), col("is_entitlement_change"),
        col("remediation_action"), col("request_id"), col("is_object_change"),
    )
    violations_staging_df = violations_df.withColumn("violation_id", expr("uuid()")).withColumn(
        "violation_type",
        when(col("is_object_change"), lit("UNAUTHORIZED_OBJECT_CHANGE"))
        .when(col("is_entitlement_change"), lit("UNAUTHORIZED_ENTITLEMENT_CHANGE"))
        .when(col("is_delete_event"), lit("UNAUTHORIZED_DELETION"))
        .when(col("is_permission_change"), lit("UNAUTHORIZED_PERMISSION_CHANGE"))
        .otherwise(lit("UNAPPROVED_CREATION"))
    ).withColumn(
        "violation_reason",
        when(col("is_object_change"), concat(lit("Unauthorized object change by: "), col("user_email"), lit(" on "), col("object_name")))
        .when(col("is_entitlement_change"), concat(lit("Unauthorized entitlement change by: "), col("user_email"), lit(" on "), col("object_name")))
        .when(col("is_delete_event"), concat(lit("Unauthorized deletion of "), col("object_type"), lit(" by: "), col("user_email"), lit(" (object_id: "), col("object_id"), lit(")")))
        .when(col("is_permission_change"), concat(lit("Unauthorized permission change by: "), col("user_email"), lit(" on "), col("object_name")))
        .otherwise(concat(lit("Object created by unauthorized user: "), col("user_email"), lit(" (object not pre-approved)")))
    ).withColumn(
        "processing_status",
        when(col("is_delete_event"), lit("PENDING_REPORT")).otherwise(lit("PENDING"))
    ).withColumn("processed_at", lit(None).cast("timestamp")).withColumn("created_at", current_timestamp())
    violations_staging_df = violations_staging_df.filter(col("remediation_action") != "SKIP_REMEDIATION")
    final_violations_count = violations_staging_df.count()
    create_violations = violations_staging_df.filter(col("violation_type") == "UNAPPROVED_CREATION").count()
    permission_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_PERMISSION_CHANGE").count()
    delete_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_DELETION").count()
    entitlement_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_ENTITLEMENT_CHANGE").count()
    object_changes_violations = violations_staging_df.filter(col("violation_type") == "UNAUTHORIZED_OBJECT_CHANGE").count()
    if final_violations_count > 0:
        violations_staging_df.createOrReplaceTempView("new_violations")
        spark.sql(f"""
            MERGE INTO {staging_table} AS target
            USING new_violations AS source
            ON target.event_id = source.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✓ Merged {final_violations_count} violations to {staging_table}")

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "violations_detected": final_violations_count,
    "create_violations": create_violations,
    "permission_violations": permission_violations,
    "delete_violations": delete_violations,
    "entitlement_violations": entitlement_violations,
    "object_changes_violations": object_changes_violations,
    "timestamp": datetime.now(tz).isoformat(),
}, indent=2))
