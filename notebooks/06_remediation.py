# Databricks notebook source
# MAGIC %md
# MAGIC # Execute Remediation
# MAGIC
# MAGIC Reads pending violations from staging, runs DELETE_RESOURCE / REVERT_PERMISSION (or reports),
# MAGIC writes to governance_control_actions and updates staging status. Config from env.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# %pip install /Workspace/GovernBot/src/governbot_core-0.1.0-py3-none-any.whl

# COMMAND ----------

import os
import json
import uuid
from datetime import datetime

if os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client."):
    spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

try:
    dry_run = dbutils.widgets.get("dry_run").lower() == "true"
except Exception:
    dry_run = True

# COMMAND ----------

from governbot_core import GovernanceConfig, ClientFactory, load_enabled_workspaces, execute_remediation
from governbot_core.constants import (
    TABLE_VIOLATIONS_STAGING,
    TABLE_CONTROL_ACTIONS,
    TABLE_PREAPPROVED_OBJECTS,
    full_table_name,
)

config = GovernanceConfig.from_env()
catalog = config.catalog
schema = config.schema
staging_table = full_table_name(catalog, schema, TABLE_VIOLATIONS_STAGING)
control_actions_table = full_table_name(catalog, schema, TABLE_CONTROL_ACTIONS)
preapproved_objects_table = full_table_name(catalog, schema, TABLE_PREAPPROVED_OBJECTS)


def get_approved_permissions(workspace_id: str, object_id: str):
    """Fetch pre-approved permissions for REVERT_PERMISSION from governance_preapproved_objects."""
    wi = (workspace_id or "").replace("'", "''")
    oi = (object_id or "").replace("'", "''")
    return spark.sql(f"""
        SELECT permissions
        FROM {preapproved_objects_table}
        WHERE workspace_id = '{wi}' AND object_id = '{oi}' AND is_active = true
    """).collect()
get_secret = lambda s, k: dbutils.secrets.get(scope=s, key=k)
factory = ClientFactory(config, get_secret)
tz = __import__("pytz").timezone("America/Mexico_City")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load pending violations (new + retryable)

# COMMAND ----------

new_violations_df = spark.sql(f"""
    SELECT
        violation_id, workspace_id, event_id, event_time, action_name, user_email,
        object_id, object_type, object_name,
        is_permission_change, COALESCE(is_delete_event, false) as is_delete_event,
        COALESCE(is_entitlement_change, false) as is_entitlement_change,
        violation_type, violation_reason, remediation_action,
        0 as retry_count, null as remediation_details, current_timestamp() as created_at
    FROM {staging_table}
    WHERE processing_status IN ('PENDING', 'PENDING_REPORT')
    ORDER BY event_time ASC
""")
retryable_violations_df = spark.sql(f"""
    SELECT
        v.violation_id, v.workspace_id, v.event_id, v.event_time, v.action_name, v.user_email,
        v.object_id, v.object_type, v.object_name,
        v.is_permission_change, COALESCE(v.is_delete_event, false) as is_delete_event,
        COALESCE(v.is_entitlement_change, false) as is_entitlement_change,
        v.violation_type, v.violation_reason, v.remediation_action,
        ca.retry_count + 1 as retry_count, ca.remediation_details, ca.created_at
    FROM {staging_table} v
    INNER JOIN {control_actions_table} ca ON v.violation_id = ca.violation_id
    WHERE ca.remediation_status NOT IN ('SUCCESS', 'SKIPPED') AND ca.retry_count <= ca.max_retries
""")
pending_violations_df = retryable_violations_df.unionByName(new_violations_df, allowMissingColumns=True)
pending_count = pending_violations_df.count()
if pending_count == 0:
    dbutils.notebook.exit(json.dumps({
        "status": "SUCCESS",
        "remediated_count": 0,
        "success_count": 0,
        "failed_count": 0,
        "timestamp": datetime.now(tz).isoformat(),
    }, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load workspaces and create clients

# COMMAND ----------

workspace_rows = load_enabled_workspaces(spark, catalog, schema)
workspace_configs = {}
workspace_objects = {}
for r in workspace_rows:
    wid = r.workspace_id
    wurl = getattr(r, "workspace_url", None) or f"https://adb-{wid}.x.azuredatabricks.net/"
    eot = getattr(r, "enabled_object_types", None)
    workspace_configs[wid] = {
        "workspace_url": wurl,
        "warehouse_id": getattr(r, "warehouse_id", None),
        "max_retry_attempts": getattr(r, "max_retry_attempts", 3),
        "enabled_object_types": set(eot) if eot and hasattr(eot, "__iter__") and not isinstance(eot, str) else None,
    }
    try:
        workspace_objects[wid] = {"client": factory.create_workspace_client(wurl), "warehouse_id": workspace_configs[wid]["warehouse_id"]}
    except Exception as e:
        workspace_objects[wid] = {"client": None, "warehouse_id": workspace_configs[wid]["warehouse_id"]}

violation_workspace_ids = {row.workspace_id for row in pending_violations_df.select("workspace_id").distinct().collect()}
for ws_id in violation_workspace_ids:
    if ws_id not in workspace_objects:
        workspace_configs[ws_id] = {"workspace_url": None, "warehouse_id": None, "max_retry_attempts": 3}
        workspace_objects[ws_id] = {"client": None, "warehouse_id": None}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process each violation

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

control_actions = []
processed_violations = []
violations_list = pending_violations_df.collect()

for violation_row in violations_list:
    violation = violation_row.asDict()
    workspace_id = violation["workspace_id"]
    wo = workspace_objects.get(workspace_id, {"client": None, "warehouse_id": None})
    client = wo["client"]
    warehouse_id = wo["warehouse_id"]
    retry_count = int(violation.get("retry_count", 0))
    created_at = violation.get("created_at")
    previous_remediation_details = violation.get("remediation_details") or ""
    max_retries = workspace_configs.get(workspace_id, {}).get("max_retry_attempts", 3)
    enabled_object_types = workspace_configs.get(workspace_id, {}).get("enabled_object_types")
    if client is None:
        status, details, error, backup_definition = ("FAILED", "No workspace client", "Workspace client not available", None)
    else:
        status, details, error, backup_definition = execute_remediation(
            client, warehouse_id, violation, dry_run,
            enabled_object_types=enabled_object_types,
            get_approved_permissions=get_approved_permissions,
        )
    control_action = {
        "action_id": str(uuid.uuid4()),
        "violation_id": violation["violation_id"],
        "workspace_id": workspace_id,
        "action_type": violation.get("remediation_action", ""),
        "object_id": violation.get("object_id", ""),
        "object_type": violation.get("object_type", ""),
        "object_name": violation.get("object_name", ""),
        "violator_email": violation.get("user_email", ""),
        "remediation_status": status,
        "remediation_details": f"{previous_remediation_details}Retry: {retry_count}\n{details}\n",
        "backup_definition": backup_definition,
        "error_message": error,
        "retry_count": retry_count,
        "max_retries": max_retries,
        "last_retry_at": datetime.now(tz) if status == "FAILED" else None,
        "completed_at": datetime.now(tz) if status == "SUCCESS" else None,
        "notification_sent": False,
        "created_at": created_at or datetime.now(tz),
        "updated_at": datetime.now(tz),
    }
    control_actions.append(control_action)
    processed_violations.append(violation["violation_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write control actions and update staging

# COMMAND ----------

if not dry_run and control_actions:
    control_actions_schema = StructType([
        StructField("action_id", StringType(), True),
        StructField("violation_id", StringType(), True),
        StructField("workspace_id", StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("object_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("object_name", StringType(), True),
        StructField("violator_email", StringType(), True),
        StructField("remediation_status", StringType(), True),
        StructField("remediation_details", StringType(), True),
        StructField("backup_definition", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("retry_count", IntegerType(), True),
        StructField("max_retries", IntegerType(), True),
        StructField("last_retry_at", TimestampType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("notification_sent", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    control_actions_df = spark.createDataFrame(control_actions, schema=control_actions_schema)
    control_actions_df.createOrReplaceTempView("control_actions_source")
    spark.sql(f"""
        MERGE INTO {control_actions_table} AS target
        USING control_actions_source AS source
        ON target.violation_id = source.violation_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Written {len(control_actions)} control actions to {control_actions_table}")
    if processed_violations:
        violation_ids_str = "', '".join(processed_violations)
        spark.sql(f"""
            UPDATE {staging_table}
            SET processing_status = 'COMPLETED', processed_at = current_timestamp()
            WHERE violation_id IN ('{violation_ids_str}')
        """)
        print(f"✓ Updated {len(processed_violations)} violations to COMPLETED")

success_count = sum(1 for a in control_actions if a["remediation_status"] == "SUCCESS")
failed_count = sum(1 for a in control_actions if a["remediation_status"] == "FAILED")

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "remediated_count": len(control_actions),
    "success_count": success_count,
    "failed_count": failed_count,
    "timestamp": datetime.now(tz).isoformat(),
}, indent=2))
