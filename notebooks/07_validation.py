# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Validation
# MAGIC
# MAGIC Validates remediation results, summarizes control actions, and reports retries / manual intervention. Config from env.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# %pip install /Workspace/GovernBot/src/governbot_core-0.1.0-py3-none-any.whl

# COMMAND ----------

import os
import json
from datetime import datetime

if os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client."):
    spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

# COMMAND ----------

from governbot_core import GovernanceConfig
from governbot_core.constants import TABLE_CONTROL_ACTIONS, TABLE_VIOLATIONS_STAGING, full_table_name

config = GovernanceConfig.from_env()
catalog = config.catalog
schema = config.schema
control_actions_table = full_table_name(catalog, schema, TABLE_CONTROL_ACTIONS)
staging_table = full_table_name(catalog, schema, TABLE_VIOLATIONS_STAGING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load recent control actions

# COMMAND ----------

recent_actions_df = spark.sql(f"""
    SELECT action_id, violation_id, workspace_id, action_type, object_id, object_type, object_name,
           violator_email, remediation_status, remediation_details, error_message,
           retry_count, max_retries, last_retry_at, completed_at, created_at
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 2 HOUR
    ORDER BY created_at DESC
""")
print(f"Found {recent_actions_df.count()} recent control actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation summary

# COMMAND ----------

status_summary_df = spark.sql(f"""
    SELECT remediation_status, COUNT(*) as count,
           COUNT(DISTINCT workspace_id) as workspaces, COUNT(DISTINCT object_type) as object_types
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY remediation_status
    ORDER BY count DESC
""")
print("Remediation Status Summary (Last 24 Hours):")
display(status_summary_df)

action_type_summary_df = spark.sql(f"""
    SELECT action_type, remediation_status, COUNT(*) as count
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY action_type, remediation_status
    ORDER BY action_type, remediation_status
""")
print("\nBreakdown by Action Type:")
display(action_type_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retryable and max-retries exceeded

# COMMAND ----------

failed_retryable_df = spark.sql(f"""
    SELECT action_id, action_type, object_type, object_name, error_message, retry_count, max_retries, last_retry_at, created_at
    FROM {control_actions_table}
    WHERE remediation_status = 'FAILED' AND retry_count < max_retries
    ORDER BY created_at ASC
""")
retryable_count = failed_retryable_df.count()
print(f"Actions that can be retried: {retryable_count}")
if retryable_count > 0:
    display(failed_retryable_df.limit(10))

max_retries_exceeded_df = spark.sql(f"""
    SELECT action_id, action_type, object_type, object_name, violator_email, error_message, retry_count, max_retries, created_at
    FROM {control_actions_table}
    WHERE remediation_status = 'FAILED' AND retry_count >= max_retries
    ORDER BY created_at ASC
""")
exceeded_count = max_retries_exceeded_df.count()
print(f"\nActions that exceeded max retries: {exceeded_count}")
if exceeded_count > 0:
    display(max_retries_exceeded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success rate and recent success/failures

# COMMAND ----------

success_rates_df = spark.sql(f"""
    SELECT object_type, COUNT(*) as total_attempts,
           SUM(CASE WHEN remediation_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
           SUM(CASE WHEN remediation_status = 'FAILED' THEN 1 ELSE 0 END) as failed,
           ROUND(100.0 * SUM(CASE WHEN remediation_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY object_type
    ORDER BY total_attempts DESC
""")
print("Success Rates by Object Type (Last 7 Days):")
display(success_rates_df)

recent_success_df = spark.sql(f"""
    SELECT action_id, workspace_id, action_type, object_type, object_name, violator_email, remediation_details, completed_at
    FROM {control_actions_table}
    WHERE remediation_status = 'SUCCESS' AND completed_at >= current_timestamp() - INTERVAL 24 HOURS
    ORDER BY completed_at DESC LIMIT 20
""")
recent_failures_df = spark.sql(f"""
    SELECT action_id, workspace_id, action_type, object_type, object_name, violator_email, error_message, retry_count, max_retries, created_at
    FROM {control_actions_table}
    WHERE remediation_status = 'FAILED' AND created_at >= current_timestamp() - INTERVAL 24 HOURS
    ORDER BY created_at DESC LIMIT 20
""")
if recent_success_df.count() > 0:
    display(recent_success_df)
if recent_failures_df.count() > 0:
    display(recent_failures_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation checks (unprocessed, orphans)

# COMMAND ----------

unprocessed_violations_df = spark.sql(f"""
    SELECT violation_id, workspace_id, object_type, object_name, user_email, violation_type, created_at
    FROM {staging_table}
    WHERE processing_status = 'PENDING' AND created_at < current_timestamp() - INTERVAL 5 MINUTES
""")
unprocessed_count = unprocessed_violations_df.count()
if unprocessed_count > 0:
    print(f"⚠️  WARNING: {unprocessed_count} violations still PENDING after 5+ minutes")
    display(unprocessed_violations_df)
else:
    print("✓ All violations have been processed")

orphan_violations_df = spark.sql(f"""
    SELECT s.violation_id, s.object_type, s.object_name, s.processing_status, s.created_at
    FROM {staging_table} s
    LEFT JOIN {control_actions_table} c ON s.violation_id = c.violation_id
    WHERE s.processing_status = 'COMPLETED' AND c.action_id IS NULL
      AND s.created_at >= current_timestamp() - INTERVAL 24 HOURS
""")
orphan_count = orphan_violations_df.count()
if orphan_count > 0:
    print(f"⚠️  WARNING: {orphan_count} completed violations have no control actions")
    display(orphan_violations_df)
else:
    print("✓ All completed violations have corresponding control actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall stats and exit

# COMMAND ----------

overall_row = spark.sql(f"""
    SELECT
        COUNT(*) as total_actions,
        SUM(CASE WHEN remediation_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
        SUM(CASE WHEN remediation_status = 'FAILED' AND retry_count < max_retries THEN 1 ELSE 0 END) as pending_retry,
        SUM(CASE WHEN remediation_status = 'FAILED' AND retry_count >= max_retries THEN 1 ELSE 0 END) as needs_intervention,
        ROUND(100.0 * SUM(CASE WHEN remediation_status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as success_rate_pct
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
""").collect()[0]
total_actions = int(overall_row["total_actions"] or 0)
successful = int(overall_row["successful"] or 0)
pending_retry = int(overall_row["pending_retry"] or 0)
needs_intervention = int(overall_row["needs_intervention"] or 0)
success_rate_pct = float(overall_row["success_rate_pct"] or 0.0)

validation_status = "SUCCESS"
validation_issues = []
if unprocessed_count > 0:
    validation_issues.append(f"{unprocessed_count} unprocessed violations")
    validation_status = "WARNING"
if orphan_count > 0:
    validation_issues.append(f"{orphan_count} orphan violations")
    validation_status = "WARNING"
if exceeded_count > 0:
    validation_issues.append(f"{exceeded_count} actions need manual intervention")
    validation_status = "WARNING"
if total_actions > 0 and success_rate_pct < 80:
    validation_issues.append(f"Success rate below 80%: {success_rate_pct}%")
    validation_status = "WARNING"

dbutils.notebook.exit(json.dumps({
    "status": validation_status,
    "total_actions": total_actions,
    "successful": successful,
    "pending_retry": pending_retry,
    "needs_intervention": needs_intervention,
    "success_rate_pct": success_rate_pct,
    "validation_issues": validation_issues,
    "timestamp": datetime.utcnow().isoformat(),
}, indent=2))
