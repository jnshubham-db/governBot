# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Environment
# MAGIC
# MAGIC Single notebook that runs, in order:
# MAGIC 1. **Setup tables** – Create catalog/schema and all governance tables (optionally drop existing).
# MAGIC 2. **Load workspace** – MERGE workspace configs from widget/env into `governance_config_workspaces`.
# MAGIC 3. **Load approved identities** – MERGE approved identities (when `load_approved_id` is Y/S).
# MAGIC 4. **Load filters** – MERGE governance filters (when `load_filters` is Y/S); uses full default set from `governance_filter_definitions.py` if no JSON provided.
# MAGIC
# MAGIC Config from env (governbot_core). All optional steps are skippable via widgets; final exit JSON summarizes what ran.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# %pip install /Workspace/GovernBot/src/governbot_core-0.1.0-py3-none-any.whl

# COMMAND ----------

import os
import json
import sys
import uuid
from datetime import datetime

tz = __import__("pytz").timezone("America/Mexico_City")

if os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client."):
    spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

# ----- 01 Setup tables -----
try:
    drop_existing = dbutils.widgets.get("drop_existing").lower() == "true"
except Exception:
    drop_existing = False

# ----- 02 Load workspace -----
try:
    workspace_configs_json = dbutils.widgets.get("workspace_configs")
except Exception:
    workspace_configs_json = os.environ.get("WORKSPACE_CONFIGS_JSON", "[]")

# ----- 03 Approved identities -----
try:
    load_approved_id = dbutils.widgets.get("load_approved_id") in ("Y", "S")
except Exception:
    load_approved_id = False
try:
    approved_identities_json = dbutils.widgets.get("approved_identities")
except Exception:
    approved_identities_json = os.environ.get("APPROVED_IDENTITIES_JSON", "[]")

# ----- 03a Load filters -----
try:
    replace_existing_filters = dbutils.widgets.get("replace_existing").lower() == "true"
except Exception:
    replace_existing_filters = False
try:
    load_filters = dbutils.widgets.get("load_filters") in ("Y", "S")
except Exception:
    load_filters = False
if load_filters:
    replace_existing_filters = True
try:
    filter_definitions_json = dbutils.widgets.get("filter_definitions_json")
except Exception:
    filter_definitions_json = os.environ.get("GOVERNANCE_FILTER_DEFINITIONS_JSON", "")

# COMMAND ----------

from governbot_core import GovernanceConfig, prepare_identity_records
from governbot_core.constants import (
    full_table_name,
    TABLE_CONFIG_WORKSPACES,
    TABLE_PREAPPROVED_OBJECTS,
    TABLE_PREAPPROVED_IDENTITIES,
    TABLE_VIOLATIONS_STAGING,
    TABLE_CONTROL_ACTIONS,
    TABLE_FILTERS,
)

config = GovernanceConfig.from_env()
catalog = config.catalog
schema = config.schema
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Drop existing tables: {drop_existing}")
print(f"Load approved identities: {load_approved_id}")
print(f"Load filters: {load_filters}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup Tables
# MAGIC Create catalog/schema and all governance tables.

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")
print(f"✓ Using catalog and schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1: Workspace Configuration

# COMMAND ----------

workspace_config_table = full_table_name(catalog, schema, TABLE_CONFIG_WORKSPACES)
if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {workspace_config_table}")
    print(f"Dropped existing table: {workspace_config_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {workspace_config_table} (
    workspace_id STRING,
    workspace_name STRING,
    workspace_url STRING,
    warehouse_id STRING,
    enforcement_enabled BOOLEAN,
    notification_email STRING,
    notification_slack_webhook STRING,
    enabled_object_types ARRAY<STRING>,
    max_retry_attempts INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_by STRING
)
USING DELTA
COMMENT 'Configuration for workspaces under governance with Azure authentication details'
""")
print(f"✓ Created table: {workspace_config_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2: Pre-Approved Objects

# COMMAND ----------

preapproved_objects_table = full_table_name(catalog, schema, TABLE_PREAPPROVED_OBJECTS)
if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {preapproved_objects_table}")
    print(f"Dropped existing table: {preapproved_objects_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {preapproved_objects_table} (
    object_id STRING,
    workspace_id STRING,
    object_type STRING,
    object_name STRING,
    object_path STRING,
    owner_email STRING,
    permissions ARRAY<STRUCT<
        principal_email: STRING,
        principal_type: STRING,
        permission_level: STRING
    >>,
    metadata MAP<STRING, STRING>,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Pre-approved objects and their permissions. principal_type can be: user, group, or service_principal'
""")
print(f"✓ Created table: {preapproved_objects_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 3: Pre-Approved Identities

# COMMAND ----------

preapproved_identities_table = full_table_name(catalog, schema, TABLE_PREAPPROVED_IDENTITIES)
if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {preapproved_identities_table}")
    print(f"Dropped existing table: {preapproved_identities_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {preapproved_identities_table} (
    identity_name STRING,
    identity_type STRING,
    display_name STRING,
    can_manage_resources BOOLEAN,
    can_manage_permissions BOOLEAN,
    approved_actions ARRAY<STRING>,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Pre-approved identities with granular permission flags. approved_actions specifies which object types the identity can create'
""")
print(f"✓ Created table: {preapproved_identities_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 4: Staging Table for Violations

# COMMAND ----------

staging_table = full_table_name(catalog, schema, TABLE_VIOLATIONS_STAGING)
if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
    print(f"Dropped existing table: {staging_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {staging_table} (
    violation_id STRING,
    workspace_id STRING,
    event_id STRING,
    event_time TIMESTAMP,
    service_name STRING,
    action_name STRING,
    user_email STRING,
    object_id STRING,
    object_type STRING,
    object_name STRING,
    is_permission_change BOOLEAN,
    is_delete_event BOOLEAN,
    is_entitlement_change BOOLEAN,
    violation_type STRING,
    violation_reason STRING,
    remediation_action STRING,
    processing_status STRING,
    processed_at TIMESTAMP,
    created_at TIMESTAMP
)
USING DELTA
COMMENT 'Staging table for detected violations (creations, permission changes, and deletions)'
""")
print(f"✓ Created table: {staging_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 5: Control Actions (Audit Trail)

# COMMAND ----------

control_actions_table = full_table_name(catalog, schema, TABLE_CONTROL_ACTIONS)
if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {control_actions_table}")
    print(f"Dropped existing table: {control_actions_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_actions_table} (
    action_id STRING,
    violation_id STRING,
    workspace_id STRING,
    action_type STRING,
    object_id STRING,
    object_type STRING,
    object_name STRING,
    violator_email STRING,
    remediation_status STRING,
    remediation_details STRING,
    backup_definition STRING,
    error_message STRING,
    retry_count INT,
    max_retries INT,
    last_retry_at TIMESTAMP,
    completed_at TIMESTAMP,
    notification_sent BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Audit trail of all governance remediation actions'
""")
print(f"✓ Created table: {control_actions_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 6: Governance Filters

# COMMAND ----------

governance_filters_table = full_table_name(catalog, schema, TABLE_FILTERS)
if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {governance_filters_table}")
    print(f"Dropped existing table: {governance_filters_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {governance_filters_table} (
    filter_id STRING,
    filter_name STRING,
    service_name STRING,
    action_name STRING,
    object_type STRING,
    object_id_expr STRING,
    object_name_expr STRING,
    extra_columns MAP<STRING, STRING>,
    violation_type STRING,
    remediation_action STRING,
    is_active BOOLEAN,
    description STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Dynamic filter configurations for audit log processing'
""")
print(f"✓ Created table: {governance_filters_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Tables

# COMMAND ----------

tables_to_verify = [
    TABLE_CONFIG_WORKSPACES,
    TABLE_PREAPPROVED_OBJECTS,
    TABLE_PREAPPROVED_IDENTITIES,
    TABLE_VIOLATIONS_STAGING,
    TABLE_CONTROL_ACTIONS,
    TABLE_FILTERS,
]
print("\n" + "="*80)
print("TABLE VERIFICATION")
print("="*80 + "\n")
for table_base in tables_to_verify:
    full = full_table_name(catalog, schema, table_base)
    count = spark.table(full).count()
    print(f"✓ {full}: {count} rows")
print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Workspace Configuration

# COMMAND ----------

if isinstance(workspace_configs_json, str) and workspace_configs_json.strip():
    workspace_configs = json.loads(workspace_configs_json)
else:
    workspace_configs = []

print(f"Number of workspace configurations: {len(workspace_configs)}")
for ws in workspace_configs:
    status = "ENABLED" if ws.get("enforcement_enabled", False) else "DISABLED"
    print(f"  - {ws.get('workspace_name', '')} ({ws.get('workspace_id', '')}): {status}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType

def prepare_workspace_records(configs):
    records = []
    current_time = datetime.now(tz)
    for c in configs:
        records.append({
            "workspace_id": c["workspace_id"],
            "workspace_name": c["workspace_name"],
            "workspace_url": c["workspace_url"],
            "enforcement_enabled": c.get("enforcement_enabled", False),
            "notification_email": c.get("notification_email"),
            "notification_slack_webhook": c.get("notification_slack_webhook"),
            "enabled_object_types": c.get("enabled_object_types", ["notebook", "query"]),
            "max_retry_attempts": c.get("max_retry_attempts", 3),
            "created_at": current_time,
            "updated_at": current_time,
            "created_by": c.get("created_by", "admin"),
            "warehouse_id": c.get("warehouse_id"),
        })
    return records

workspace_records = prepare_workspace_records(workspace_configs)
workspace_loaded_count = 0

# COMMAND ----------

table_workspaces = full_table_name(catalog, schema, TABLE_CONFIG_WORKSPACES)
if workspace_records:
    workspace_schema = StructType([
        StructField("workspace_id", StringType(), True),
        StructField("workspace_name", StringType(), True),
        StructField("workspace_url", StringType(), True),
        StructField("enforcement_enabled", BooleanType(), True),
        StructField("notification_email", StringType(), True),
        StructField("notification_slack_webhook", StringType(), True),
        StructField("enabled_object_types", ArrayType(StringType()), True),
        StructField("max_retry_attempts", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("created_by", StringType(), True),
        StructField("warehouse_id", StringType(), True),
    ])
    df = spark.createDataFrame(workspace_records, schema=workspace_schema)
    df.createOrReplaceTempView("new_workspaces")
    spark.sql(f"""
        MERGE INTO {table_workspaces} AS target
        USING new_workspaces AS source
        ON target.workspace_id = source.workspace_id
        WHEN MATCHED THEN
            UPDATE SET
                workspace_name = source.workspace_name,
                workspace_url = source.workspace_url,
                enforcement_enabled = source.enforcement_enabled,
                notification_email = source.notification_email,
                notification_slack_webhook = source.notification_slack_webhook,
                enabled_object_types = source.enabled_object_types,
                max_retry_attempts = source.max_retry_attempts,
                updated_at = source.updated_at,
                warehouse_id = source.warehouse_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    workspace_loaded_count = len(workspace_records)
    print(f"✓ Loaded {workspace_loaded_count} workspace configurations to {table_workspaces}")
else:
    print("No workspace configurations to load. Set widget workspace_configs (JSON) or env WORKSPACE_CONFIGS_JSON.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Approved Identities

# COMMAND ----------

result_approved_id = {"status": "SKIPPED", "reason": "load_approved_id not set (Y/S)", "loaded_count": 0}

if load_approved_id:
    if isinstance(approved_identities_json, str) and approved_identities_json.strip():
        approved_identities = json.loads(approved_identities_json)
    else:
        approved_identities = []

    print(f"Number of approved identities: {len(approved_identities)}")
    for identity in approved_identities:
        res = "✓" if identity.get("can_manage_resources", False) else "✗"
        perm = "✓" if identity.get("can_manage_permissions", False) else "✗"
        actions = identity.get("approved_actions", ["ALL"])
        print(f"  - {identity.get('type',''):20s}: {identity.get('name',''):40s} [Resources:{res}] [Permissions:{perm}] [{','.join(actions)}]")

    identity_records = prepare_identity_records(approved_identities)
    result_approved_id["loaded_count"] = len(identity_records)

    if identity_records:
        table_identities = full_table_name(catalog, schema, TABLE_PREAPPROVED_IDENTITIES)
        identity_schema = StructType([
            StructField("identity_name", StringType(), True),
            StructField("identity_type", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("can_manage_resources", BooleanType(), True),
            StructField("can_manage_permissions", BooleanType(), True),
            StructField("approved_actions", ArrayType(StringType()), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])
        df_id = spark.createDataFrame(identity_records, schema=identity_schema)
        df_id.createOrReplaceTempView("new_identities")
        spark.sql(f"""
            MERGE INTO {table_identities} AS target
            USING new_identities AS source
            ON target.identity_name = source.identity_name AND target.identity_type = source.identity_type
            WHEN MATCHED THEN
                UPDATE SET
                    is_active = source.is_active,
                    display_name = source.display_name,
                    can_manage_resources = source.can_manage_resources,
                    can_manage_permissions = source.can_manage_permissions,
                    approved_actions = source.approved_actions,
                    updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✓ Loaded {len(identity_records)} identities to {table_identities}")
        result_approved_id["status"] = "SUCCESS"
    else:
        print("No identities to load. Set widget approved_identities (JSON) or env APPROVED_IDENTITIES_JSON.")
        result_approved_id["reason"] = "No identity records to load"
else:
    print("Skipping approved identities (load_approved_id not Y/S).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Governance Filters

# COMMAND ----------

result_filters = {"status": "SKIPPED", "reason": "load_filters not set (Y/S)", "filters_loaded": 0, "active_filters": 0}

if load_filters:
    table_filters = full_table_name(catalog, schema, TABLE_FILTERS)

    def convert_to_filter_record(raw_filter, violation_type):
        return {
            "filter_id": str(uuid.uuid4()),
            "filter_name": raw_filter[0],
            "service_name": raw_filter[1],
            "action_name": raw_filter[2],
            "object_type": raw_filter[3],
            "object_id_expr": raw_filter[4],
            "object_name_expr": raw_filter[5],
            "extra_columns": raw_filter[7] if raw_filter[7] else {},
            "violation_type": violation_type,
            "remediation_action": raw_filter[6],
            "is_active": raw_filter[8],
            "description": raw_filter[9],
            "created_at": datetime.now(tz),
            "updated_at": datetime.now(tz),
        }

    if filter_definitions_json and filter_definitions_json.strip():
        definitions = json.loads(filter_definitions_json)
        create_filters_raw = definitions.get("create_filters_raw", [])
        acl_filters_raw = definitions.get("acl_filters_raw", [])
        delete_filters_raw = definitions.get("delete_filters_raw", [])
        workspace_admin_filters_raw = definitions.get("workspace_admin_filters_raw", [])
        object_changes_filters_raw = definitions.get("object_changes_filters_raw", [])
    else:
        from governbot_core import get_governance_filter_definitions
        definitions = get_governance_filter_definitions()
        create_filters_raw = definitions["create_filters_raw"]
        acl_filters_raw = definitions["acl_filters_raw"]
        delete_filters_raw = definitions["delete_filters_raw"]
        workspace_admin_filters_raw = definitions["workspace_admin_filters_raw"]
        object_changes_filters_raw = definitions["object_changes_filters_raw"]

    create_filters = [convert_to_filter_record(f, "UNAPPROVED_CREATION") for f in create_filters_raw]
    acl_filters = [convert_to_filter_record(f, "UNAUTHORIZED_PERMISSION_CHANGE") for f in acl_filters_raw]
    delete_filters = [convert_to_filter_record(f, "UNAUTHORIZED_DELETION") for f in delete_filters_raw]
    workspace_admin_filters = [convert_to_filter_record(f, "UNAUTHORIZED_ENTITLEMENT_CHANGE") for f in workspace_admin_filters_raw]
    object_changes_filters = [convert_to_filter_record(f, "UNAUTHORIZED_OBJECT_CHANGE") for f in object_changes_filters_raw]

    all_filters = create_filters + acl_filters + delete_filters + workspace_admin_filters + object_changes_filters
    active_count = sum(1 for f in all_filters if f["is_active"])
    print(f"Total filters: {len(all_filters)} (active: {active_count})")

    from pyspark.sql.types import MapType

    filter_schema = StructType([
        StructField("filter_id", StringType(), True),
        StructField("filter_name", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("action_name", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("object_id_expr", StringType(), True),
        StructField("object_name_expr", StringType(), True),
        StructField("extra_columns", MapType(StringType(), StringType()), True),
        StructField("violation_type", StringType(), True),
        StructField("remediation_action", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("description", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    df_f = spark.createDataFrame(all_filters, schema=filter_schema)
    if replace_existing_filters:
        spark.sql(f"TRUNCATE TABLE {table_filters}")
        print(f"Truncated {table_filters}")
    df_f.createOrReplaceTempView("new_filters")
    spark.sql(f"""
        MERGE INTO {table_filters} AS target
        USING new_filters AS source
        ON target.filter_name = source.filter_name
        WHEN MATCHED THEN
            UPDATE SET
                service_name = source.service_name,
                action_name = source.action_name,
                object_type = source.object_type,
                object_id_expr = source.object_id_expr,
                object_name_expr = source.object_name_expr,
                extra_columns = source.extra_columns,
                violation_type = source.violation_type,
                remediation_action = source.remediation_action,
                is_active = source.is_active,
                description = source.description,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Loaded {len(all_filters)} filters to {table_filters}")
    result_filters = {
        "status": "SUCCESS",
        "filters_loaded": len(all_filters),
        "active_filters": active_count,
        "create_filters": len(create_filters),
        "acl_filters": len(acl_filters),
        "delete_filters": len(delete_filters),
        "workspace_admin_filters": len(workspace_admin_filters),
        "object_changes_filters": len(object_changes_filters),
    }
else:
    print("Skipping governance filters (load_filters not Y/S).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Exit

# COMMAND ----------

summary = {
    "status": "SUCCESS",
    "setup_tables": {"catalog": catalog, "schema": schema, "tables_verified": len(tables_to_verify)},
    "workspace": {"loaded_count": workspace_loaded_count},
    "approved_identities": result_approved_id,
    "filters": result_filters,
    "timestamp": datetime.now(tz).isoformat(),
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary, indent=2))
