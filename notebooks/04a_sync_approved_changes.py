# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Pre-Approved User Changes
# MAGIC
# MAGIC Syncs audit events from approved users into governance_preapproved_objects: creations (with permission fetch),
# MAGIC permission changes, deletions, entitlements, object changes, serverless budget policies. Config from env.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os
import json
from datetime import datetime

spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

try:
    lookback_hours = int(dbutils.widgets.get("lookback_hours"))
except Exception:
    lookback_hours = os.environ.get("LOOKBACK_HOURS_DEFAULT", "24")
try:
    sync_creations = dbutils.widgets.get("sync_creations") == "Y"
except Exception:
    sync_creations = True
try:
    sync_permissions = dbutils.widgets.get("sync_permissions") == "Y"
except Exception:
    sync_permissions = True
try:
    sync_entitlements = dbutils.widgets.get("sync_entitlements") == "Y"
except Exception:
    sync_entitlements = True
try:
    sync_object_changes = dbutils.widgets.get("sync_object_changes") == "Y"
except Exception:
    sync_object_changes = True
try:
    sync_deletions = dbutils.widgets.get("sync_deletions") == "Y"
except Exception:
    sync_deletions = True
try:
    sync_serverless_budget_policies = dbutils.widgets.get("sync_serverless_budget_policies") == "Y"
except Exception:
    sync_serverless_budget_policies = True
try:
    load_filters_flag = dbutils.widgets.get("load_filters") in ("Y", "S")
except Exception:
    load_filters_flag = False
try:
    enable_discover_flag = dbutils.widgets.get("enable_discover") in ("Y", "S")
except Exception:
    enable_discover_flag = False
try:
    account_id = dbutils.widgets.get("account_id")
except Exception:
    account_id = os.environ.get("ACCOUNT_ID", "")

if load_filters_flag or enable_discover_flag:
    dbutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "reason": "load_filters or enable_discover is set; run filters/discover notebooks first",
        "timestamp": datetime.utcnow().isoformat(),
    }, indent=2))

# COMMAND ----------

from governbot_core import (
    GovernanceConfig,
    ClientFactory,
    load_enabled_workspaces,
    load_filters,
    build_approved_user_query,
)
from governbot_core.constants import (
    TABLE_PREAPPROVED_OBJECTS,
    TABLE_CONTROL_ACTIONS,
    full_table_name,
    VIOLATION_UNAPPROVED_CREATION,
    VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE,
    VIOLATION_UNAUTHORIZED_DELETION,
    VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE,
    VIOLATION_UNAUTHORIZED_OBJECT_CHANGE,
)
from governbot_core.identities import load_preapproved_identities, parse_identity_lists, expand_group_members

config = GovernanceConfig.from_env()
catalog = config.catalog
schema = config.schema
table_name = full_table_name(catalog, schema, TABLE_PREAPPROVED_OBJECTS)
control_actions_table = full_table_name(catalog, schema, TABLE_CONTROL_ACTIONS)
get_secret = lambda s, k: dbutils.secrets.get(scope=s, key=k)
factory = ClientFactory(config, get_secret)
account_url = config.account_url or "https://accounts.azuredatabricks.net/"

# Load workspaces
rows = load_enabled_workspaces(spark, catalog, schema)
if not rows:
    dbutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "reason": "No enabled workspaces",
        "timestamp": datetime.utcnow().isoformat(),
    }, indent=2))
workspace_ids = [r.workspace_id for r in rows]
workspace_urls = [getattr(r, "workspace_url", None) or f"https://adb-{r.workspace_id}.x.azuredatabricks.net/" for r in rows]
workspace_ids_str = "', '".join(str(wid) for wid in workspace_ids)
workspace_clients = {}
for wid, wurl in zip(workspace_ids, workspace_urls):
    try:
        workspace_clients[wid] = factory.create_workspace_client(wurl)
    except Exception:
        workspace_clients[wid] = None

# Load identities and expand groups
identity_rows = load_preapproved_identities(spark, catalog, schema)
parsed = parse_identity_lists(identity_rows)
resource_users = parsed["resource_approved_users"]
resource_sps = parsed["resource_approved_service_principals"]
resource_groups = parsed["resource_approved_groups"]
permission_users = parsed["permission_approved_users"]
permission_sps = parsed["permission_approved_service_principals"]
permission_groups = parsed["permission_approved_groups"]
identity_approved_actions = parsed["identity_approved_actions"]
first_client = next((c for c in workspace_clients.values() if c is not None), None)
if first_client and resource_groups:
    ru, rsp = expand_group_members(resource_groups, first_client, identity_approved_actions)
    resource_users = list(set(resource_users + ru))
    resource_sps = list(set(resource_sps + rsp))
if first_client and permission_groups:
    pu, psp = expand_group_members(permission_groups, first_client, identity_approved_actions)
    permission_users = list(set(permission_users + pu))
    permission_sps = list(set(permission_sps + psp))
resource_approved_identities = resource_users + resource_sps
permission_approved_identities = permission_users + permission_sps
resource_identities_with_all = [i for i in resource_approved_identities if "ALL" in identity_approved_actions.get(i, ["ALL"])]
resource_identities_with_restrictions = {i: identity_approved_actions[i] for i in resource_approved_identities if i not in resource_identities_with_all and identity_approved_actions.get(i)}
resource_ids_str = "', '".join(i.replace("'", "''") for i in resource_approved_identities) if resource_approved_identities else ""
permission_ids_str = "', '".join(i.replace("'", "''") for i in permission_approved_identities) if permission_approved_identities else ""

# Load filters
filters_by_type = load_filters(spark, catalog, schema)
create_filters = filters_by_type.get(VIOLATION_UNAPPROVED_CREATION, [])
acl_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE, [])
deletion_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_DELETION, [])
entitlement_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE, [])
object_changes_filters = filters_by_type.get(VIOLATION_UNAUTHORIZED_OBJECT_CHANGE, [])

# Account client for serverless budget policies
account_client = None
if (account_id or config.account_id) and (account_url or config.account_url):
    try:
        account_client = factory.create_account_client(account_url, account_id or config.account_id)
    except Exception:
        account_client = None

from governbot_core import sync_impl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync creations (with permission fetch)

# COMMAND ----------

from pyspark.sql.functions import col, lower
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, MapType, ArrayType

creations_synced = 0
creations_filtered_by_actions = 0
if sync_creations and resource_ids_str and create_filters:
    create_query = build_approved_user_query(
        create_filters, workspace_ids_str, lookback_hours, resource_ids_str,
        is_permission_change=False, is_delete_event=False, is_entitlement_change=False,
    )
    if create_query:
        create_events_df = spark.sql(create_query)
        valid_creates_df = create_events_df.filter((col("object_id") != "unknown") & col("object_id").isNotNull())
        initial_count = valid_creates_df.count()
        if resource_identities_with_restrictions:
            allowed_pairs = [(id, a.lower()) for id, actions in resource_identities_with_restrictions.items() for a in actions]
            allowed_df = spark.createDataFrame(allowed_pairs, ["allowed_identity", "allowed_object_type"])
            valid_creates_with_lower = valid_creates_df.withColumn("object_type_lower", lower(col("object_type")))
            restricted_users = list(resource_identities_with_restrictions.keys())
            events_all = valid_creates_with_lower.filter(~col("user_email").isin(restricted_users))
            events_restricted = valid_creates_with_lower.filter(col("user_email").isin(restricted_users))
            valid_restricted = events_restricted.join(
                allowed_df,
                (events_restricted["user_email"] == allowed_df["allowed_identity"]) & (events_restricted["object_type_lower"] == allowed_df["allowed_object_type"]),
                "inner",
            ).drop("allowed_identity", "allowed_object_type")
            valid_creates_df = events_all.unionByName(valid_restricted).drop("object_type_lower")
            creations_filtered_by_actions = initial_count - valid_creates_df.count()
        existing_df = spark.sql(f"SELECT workspace_id, object_id FROM {table_name} WHERE is_active = true")
        new_objects_df = valid_creates_df.join(
            existing_df,
            (valid_creates_df.workspace_id == existing_df.workspace_id) & (valid_creates_df.object_id == existing_df.object_id),
            "left_anti",
        ).drop(existing_df.workspace_id).drop(existing_df.object_id)
        new_count = new_objects_df.count()
        if new_count > 0:
            new_objects_list = new_objects_df.collect()
            new_objects_with_permissions = []
            for row in new_objects_list:
                object_id = row.object_id
                object_type = row.object_type
                workspace_id = row.workspace_id
                object_name = row.object_name
                user_email = row.user_email
                client = workspace_clients.get(workspace_id)
                permissions = []
                perm_error = None
                if client:
                    permissions, perm_error = sync_impl.fetch_current_permissions(client, object_type, object_id)
                owner_email = user_email
                for p in permissions:
                    if p.get("permission_level") in ("CAN_MANAGE", "MANAGE", "ALL_PRIVILEGES", "OWNER") and p.get("principal_type") == "user":
                        owner_email = p.get("principal_email", user_email)
                        break
                permissions_array = [Row(principal_email=p["principal_email"], principal_type=p.get("principal_type", "user"), permission_level=p["permission_level"]) for p in permissions] if permissions else []
                metadata = {}
                if getattr(row, "event_time", None):
                    metadata["created_at"] = str(row.event_time)
                if getattr(row, "action_name", None):
                    metadata["created_by_action"] = row.action_name
                new_objects_with_permissions.append({
                    "object_id": object_id,
                    "workspace_id": workspace_id,
                    "object_type": object_type,
                    "object_name": object_name,
                    "object_path": getattr(row, "object_path", None) or object_name,
                    "owner_email": owner_email,
                    "permissions": permissions_array,
                    "metadata": metadata,
                    "is_active": True,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                })
            schema_preapproved = StructType([
                StructField("object_id", StringType(), True),
                StructField("workspace_id", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("object_path", StringType(), True),
                StructField("owner_email", StringType(), True),
                StructField("permissions", ArrayType(StructType([
                    StructField("principal_email", StringType(), True),
                    StructField("principal_type", StringType(), True),
                    StructField("permission_level", StringType(), True),
                ])), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
                StructField("is_active", BooleanType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
            ])
            df_sync = spark.createDataFrame(new_objects_with_permissions, schema=schema_preapproved)
            df_sync.createOrReplaceTempView("new_approved_objects")
            spark.sql(f"""
                MERGE INTO {table_name} AS target
                USING new_approved_objects AS source
                ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id
                WHEN NOT MATCHED THEN INSERT *
            """)
            creations_synced = new_count
            print(f"✓ Synced {creations_synced} new creations to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync permission changes

# COMMAND ----------

permissions_synced = 0
permissions_updated = 0
permissions_added = 0
if sync_permissions and permission_ids_str and acl_filters:
    acl_query = build_approved_user_query(
        acl_filters, workspace_ids_str, lookback_hours, permission_ids_str,
        is_permission_change=True, is_delete_event=False, is_entitlement_change=False,
    )
    if acl_query:
        acl_events_df = spark.sql(acl_query)
        valid_acl_df = acl_events_df.filter((col("object_id") != "unknown") & col("object_id").isNotNull())
        objects_with_changes = valid_acl_df.select("workspace_id", "object_id", "object_type").distinct()
        objects_list = objects_with_changes.collect()
        updated_records = []
        for obj in objects_list:
            client = workspace_clients.get(obj.workspace_id)
            if not client:
                continue
            current_permissions, _ = sync_impl.fetch_current_permissions(client, obj.object_type, obj.object_id)
            if current_permissions:
                permissions_array = [Row(principal_email=p["principal_email"], principal_type=p.get("principal_type", "user"), permission_level=p["permission_level"]) for p in current_permissions]
                updated_records.append({
                    "workspace_id": obj.workspace_id,
                    "object_id": obj.object_id,
                    "object_type": obj.object_type,
                    "permissions": permissions_array,
                    "updated_at": datetime.utcnow(),
                })
                permissions_synced += 1
        if updated_records:
            existing_objects_df = spark.sql(f"SELECT workspace_id, object_id, object_type, object_name, object_path, owner_email, metadata, is_active, created_at FROM {table_name} WHERE is_active = true")
            existing_objects = {(row.workspace_id, row.object_id): row for row in existing_objects_df.collect()}
            records_to_update = []
            records_to_insert = []
            for record in updated_records:
                key = (record["workspace_id"], record["object_id"])
                if key in existing_objects:
                    existing = existing_objects[key]
                    records_to_update.append({
                        "workspace_id": record["workspace_id"], "object_id": record["object_id"], "object_type": record["object_type"],
                        "object_name": existing.object_name, "object_path": existing.object_path, "owner_email": existing.owner_email,
                        "permissions": record["permissions"], "metadata": existing.metadata, "is_active": True,
                        "created_at": existing.created_at, "updated_at": record["updated_at"],
                    })
                    permissions_updated += 1
                else:
                    records_to_insert.append({
                        "workspace_id": record["workspace_id"], "object_id": record["object_id"], "object_type": record["object_type"],
                        "object_name": record["object_id"], "object_path": None, "owner_email": "unknown",
                        "permissions": record["permissions"], "metadata": None, "is_active": True,
                        "created_at": record["updated_at"], "updated_at": record["updated_at"],
                    })
                    permissions_added += 1
            permissions_schema = StructType([
                StructField("workspace_id", StringType(), True), StructField("object_id", StringType(), True),
                StructField("object_type", StringType(), True), StructField("object_name", StringType(), True),
                StructField("object_path", StringType(), True), StructField("owner_email", StringType(), True),
                StructField("permissions", ArrayType(StructType([
                    StructField("principal_email", StringType(), True), StructField("principal_type", StringType(), True),
                    StructField("permission_level", StringType(), True),
                ])), True),
                StructField("metadata", MapType(StringType(), StringType()), True), StructField("is_active", BooleanType(), True),
                StructField("created_at", TimestampType(), True), StructField("updated_at", TimestampType(), True),
            ])
            permissions_df = spark.createDataFrame(records_to_update + records_to_insert, schema=permissions_schema)
            permissions_df.createOrReplaceTempView("permission_updates")
            spark.sql(f"""
                MERGE INTO {table_name} AS target
                USING permission_updates AS source
                ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id
                WHEN MATCHED THEN UPDATE SET permissions = source.permissions, updated_at = source.updated_at
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"✓ Permissions updated: {permissions_updated} existing, {permissions_added} new")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync deletions

# COMMAND ----------

deletions_synced = 0
if sync_deletions and resource_ids_str and deletion_filters:
    deletion_query = build_approved_user_query(
        deletion_filters, workspace_ids_str, lookback_hours, resource_ids_str,
        is_permission_change=False, is_delete_event=True, is_entitlement_change=False,
    )
    if deletion_query:
        deletion_events_df = spark.sql(deletion_query)
        valid_deletions_df = deletion_events_df.filter((col("object_id") != "unknown") & col("object_id").isNotNull())
        objects_with_deletion = valid_deletions_df.select("workspace_id", "object_id", "object_type").distinct()
        if objects_with_deletion.count() > 0:
            objects_with_deletion.createOrReplaceTempView("objects_with_deletion")
            spark.sql(f"""
                MERGE INTO {table_name} AS target
                USING objects_with_deletion AS source
                ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id AND target.is_active = true
                WHEN MATCHED THEN UPDATE SET is_active = false, updated_at = current_timestamp()
            """)
            deletions_synced = objects_with_deletion.count()
            print(f"✓ Marked {deletions_synced} objects inactive (deletions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync entitlement changes

# COMMAND ----------

entitlements_synced = 0
entitlements_updated = 0
entitlements_added = 0
if sync_entitlements and permission_ids_str and entitlement_filters:
    entitlement_query = build_approved_user_query(
        entitlement_filters, workspace_ids_str, lookback_hours, permission_ids_str,
        is_permission_change=False, is_delete_event=False, is_entitlement_change=True,
    )
    if entitlement_query:
        entitlement_events_df = spark.sql(entitlement_query)
        valid_entitlements_df = entitlement_events_df.filter((col("object_id") != "unknown") & col("object_id").isNotNull())
        identities_with_changes = valid_entitlements_df.select("workspace_id", "object_id", "object_type", "object_name").dropDuplicates(["workspace_id", "object_id"])
        identities_list = identities_with_changes.collect()
        updated_records = []
        for identity in identities_list:
            client = workspace_clients.get(identity.workspace_id)
            if not client:
                continue
            object_name = identity.object_name or identity.object_id
            metadata = None
            if identity.object_type == "groups":
                object_name, metadata, _ = sync_impl.fetch_group_details(client, identity.object_id)
                object_name = object_name or identity.object_id
            elif identity.object_type == "users":
                object_name, metadata, _ = sync_impl.fetch_user_details(client, identity.object_id)
                object_name = object_name or identity.object_id
            elif identity.object_type == "service_principal":
                object_name, metadata, _ = sync_impl.fetch_service_principal_details(client, identity.object_id)
                object_name = object_name or identity.object_id
            if metadata is not None:
                metadata_flat = {k: str(v) for k, v in (metadata or {}).items()}
                updated_records.append({
                    "workspace_id": identity.workspace_id, "object_id": identity.object_id, "object_type": identity.object_type,
                    "object_name": object_name or identity.object_id, "object_path": None, "owner_email": "unknown",
                    "permissions": [], "metadata": metadata_flat, "is_active": True,
                    "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
                })
                entitlements_synced += 1
        if updated_records:
            entitlements_schema = StructType([
                StructField("workspace_id", StringType(), True), StructField("object_id", StringType(), True),
                StructField("object_type", StringType(), True), StructField("object_name", StringType(), True),
                StructField("object_path", StringType(), True), StructField("owner_email", StringType(), True),
                StructField("permissions", ArrayType(StructType([
                    StructField("principal_email", StringType(), True), StructField("principal_type", StringType(), True),
                    StructField("permission_level", StringType(), True),
                ])), True),
                StructField("metadata", MapType(StringType(), StringType()), True), StructField("is_active", BooleanType(), True),
                StructField("created_at", TimestampType(), True), StructField("updated_at", TimestampType(), True),
            ])
            entitlements_df = spark.createDataFrame(updated_records, schema=entitlements_schema)
            entitlements_df.createOrReplaceTempView("entitlement_updates")
            spark.sql(f"""
                MERGE INTO {table_name} AS target
                USING entitlement_updates AS source
                ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id
                WHEN MATCHED THEN UPDATE SET object_name = source.object_name, metadata = source.metadata, updated_at = source.updated_at
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"✓ Entitlements synced: {len(updated_records)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync serverless budget policies

# COMMAND ----------

serverless_budget_policies_synced = 0
serverless_budget_policies_updated = 0
serverless_budget_policies_added = 0
serverless_budget_policies_deleted = 0
if sync_serverless_budget_policies and account_client and workspace_ids:
    policies = sync_impl.discover_serverless_budget_policies(account_client, account_id or config.account_id or "", workspace_ids)
    if policies:
        policy_rows = []
        for p in policies:
            perms = p.get("permissions") or []
            policy_rows.append({
                "object_id": p["object_id"], "workspace_id": p["workspace_id"], "object_type": p["object_type"],
                "object_name": p["object_name"], "object_path": None, "owner_email": p.get("owner_email", "unknown"),
                "permissions": [Row(principal_email=x["principal_email"], principal_type=x.get("principal_type", "user"), permission_level=x["permission_level"]) for x in perms],
                "metadata": p.get("metadata") or {}, "is_active": True, "created_at": p.get("created_at", datetime.utcnow()), "updated_at": p.get("updated_at", datetime.utcnow()),
            })
        policies_schema = StructType([
            StructField("object_id", StringType(), True), StructField("workspace_id", StringType(), True),
            StructField("object_type", StringType(), True), StructField("object_name", StringType(), True),
            StructField("object_path", StringType(), True), StructField("owner_email", StringType(), True),
            StructField("permissions", ArrayType(StructType([
                StructField("principal_email", StringType(), True), StructField("principal_type", StringType(), True),
                StructField("permission_level", StringType(), True),
            ])), True),
            StructField("metadata", MapType(StringType(), StringType()), True), StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True), StructField("updated_at", TimestampType(), True),
        ])
        policies_df = spark.createDataFrame(policy_rows, schema=policies_schema)
        policies_df.createOrReplaceTempView("serverless_budget_policies")
        spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING serverless_budget_policies AS source
            ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id AND target.is_active = true
            WHEN MATCHED THEN UPDATE SET object_name = source.object_name, permissions = source.permissions, metadata = source.metadata, updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        serverless_budget_policies_synced = len(policy_rows)
        print(f"✓ Serverless budget policies synced: {serverless_budget_policies_synced}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync object changes (UC renames)

# COMMAND ----------

object_changes_updated = 0
if sync_object_changes and permission_ids_str and object_changes_filters:
    object_changes_query = build_approved_user_query(
        object_changes_filters, "", lookback_hours, permission_ids_str,
        is_permission_change=False, is_delete_event=False, is_entitlement_change=False,
    )
    if object_changes_query:
        from pyspark.sql.functions import coalesce
        object_changes_events_df = (
            spark.sql(object_changes_query)
            .filter((coalesce(col("request_params.dry_run"), "false") != "true") & (col("object_id") != col("object_name")))
            .withColumnRenamed("object_name", "new_object_name")
            .select("object_id", "new_object_name", "object_type")
        )
        if object_changes_events_df.count() > 0:
            object_changes_events_df.createOrReplaceTempView("object_changes_events")
            spark.sql(f"""
                CREATE OR REPLACE TEMPORARY VIEW object_changes_final_changes AS
                SELECT target.workspace_id, target.object_id, source.new_object_name, source.object_type
                FROM object_changes_events AS source
                INNER JOIN {table_name} target
                ON (
                    (target.object_type = source.object_type AND target.object_id = source.object_id)
                    OR (source.object_type IN ('catalog','schema') AND target.object_id LIKE CONCAT(source.object_id, '.%'))
                )
                WHERE target.is_active = true
            """)
            spark.sql(f"""
                MERGE INTO {table_name} AS target
                USING object_changes_final_changes AS source
                ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id
                WHEN MATCHED THEN UPDATE SET
                    object_id = source.new_object_name,
                    object_name = source.new_object_name,
                    updated_at = current_timestamp()
            """)
            object_changes_updated = spark.sql("SELECT COUNT(*) FROM object_changes_final_changes").collect()[0][0]
            print(f"✓ Object changes updated: {object_changes_updated}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mark control actions for inactive objects as SKIPPED

# COMMAND ----------

skipped_violations_count = 0  # MERGE does not return row count in all runtimes
spark.sql(f"""
    MERGE INTO {control_actions_table} AS target
    USING (
        SELECT object_id, object_type FROM {table_name} WHERE is_active = false
    ) AS source
    ON target.object_id = source.object_id AND target.object_type = source.object_type
      AND target.remediation_status NOT IN ('SKIPPED', 'SUCCESS')
    WHEN MATCHED THEN UPDATE SET
        remediation_status = 'SKIPPED',
        remediation_details = concat_ws(chr(10), target.remediation_details, 'Skipping as Object is inactive.', current_timestamp()),
        updated_at = current_timestamp()
""")
print("✓ Control actions updated for inactive objects")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "creations_synced": creations_synced,
    "creations_filtered_by_actions": creations_filtered_by_actions,
    "permissions_synced": permissions_synced,
    "permissions_updated": permissions_updated,
    "permissions_added": permissions_added,
    "deletions_synced": deletions_synced,
    "entitlements_synced": entitlements_synced,
    "entitlements_updated": entitlements_updated,
    "entitlements_added": entitlements_added,
    "serverless_budget_policies_synced": serverless_budget_policies_synced,
    "object_changes_updated": object_changes_updated,
    "skipped_violations": skipped_violations_count,
    "timestamp": datetime.utcnow().isoformat(),
}, indent=2))
