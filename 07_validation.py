# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Validation
# MAGIC
# MAGIC This notebook validates that remediation actions were successfully completed
# MAGIC and handles retry logic for failed actions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "governance", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Recent Control Actions

# COMMAND ----------

from datetime import datetime, timedelta

control_actions_table = f"{catalog}.{schema}.governance_control_actions"

# Load recent control actions for validation
recent_actions_df = spark.sql(f"""
    SELECT 
        action_id,
        violation_id,
        workspace_id,
        action_type,
        object_id,
        object_type,
        object_name,
        violator_email,
        remediation_status,
        remediation_details,
        error_message,
        retry_count,
        max_retries,
        last_retry_at,
        completed_at,
        created_at
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 1 HOUR
    ORDER BY created_at DESC
""")

total_count = recent_actions_df.count()
print(f"Found {total_count} recent control actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

# Count by status
status_summary_df = spark.sql(f"""
    SELECT 
        remediation_status,
        COUNT(*) as count,
        COUNT(DISTINCT workspace_id) as workspaces,
        COUNT(DISTINCT object_type) as object_types
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY remediation_status
    ORDER BY count DESC
""")

print("Remediation Status Summary (Last 24 Hours):")
display(status_summary_df)

# COMMAND ----------

# Breakdown by action type
action_type_summary_df = spark.sql(f"""
    SELECT 
        action_type,
        remediation_status,
        COUNT(*) as count
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY action_type, remediation_status
    ORDER BY action_type, remediation_status
""")

print("\nBreakdown by Action Type:")
display(action_type_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Actions Needing Retry

# COMMAND ----------

def calculate_backoff_seconds(retry_count: int, base_delay: int = 60) -> int:
    """Calculate exponential backoff delay in seconds."""
    delay = base_delay * (2 ** retry_count)
    return min(delay, 3600)  # Max 1 hour

# Actions that failed and can be retried
failed_retryable_df = spark.sql(f"""
    SELECT 
        action_id,
        action_type,
        object_type,
        object_name,
        error_message,
        retry_count,
        max_retries,
        last_retry_at,
        created_at
    FROM {control_actions_table}
    WHERE remediation_status = 'FAILED'
    AND retry_count < max_retries
    ORDER BY created_at ASC
""")

retryable_count = failed_retryable_df.count()
print(f"\nActions that can be retried: {retryable_count}")

if retryable_count > 0:
    print("\nSample Failed Actions (Pending Retry):")
    display(failed_retryable_df.limit(10))

# COMMAND ----------

# Actions that exceeded max retries (need manual intervention)
max_retries_exceeded_df = spark.sql(f"""
    SELECT 
        action_id,
        action_type,
        object_type,
        object_name,
        violator_email,
        error_message,
        retry_count,
        max_retries,
        created_at
    FROM {control_actions_table}
    WHERE remediation_status = 'FAILED'
    AND retry_count >= max_retries
    ORDER BY created_at ASC
""")

exceeded_count = max_retries_exceeded_df.count()
print(f"\nActions that exceeded max retries: {exceeded_count}")

if exceeded_count > 0:
    print("\n⚠️  WARNING: These actions need manual intervention:")
    display(max_retries_exceeded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success Rate Metrics

# COMMAND ----------

# Calculate success rates
success_rates_df = spark.sql(f"""
    SELECT 
        object_type,
        COUNT(*) as total_attempts,
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Successful Actions

# COMMAND ----------

recent_success_df = spark.sql(f"""
    SELECT 
        action_id,
        workspace_id,
        action_type,
        object_type,
        object_name,
        violator_email,
        remediation_details,
        completed_at
    FROM {control_actions_table}
    WHERE remediation_status = 'SUCCESS'
    AND completed_at >= current_timestamp() - INTERVAL 24 HOURS
    ORDER BY completed_at DESC
    LIMIT 20
""")

success_count = recent_success_df.count()
print(f"\nRecent Successful Actions (Last 24 Hours): {success_count}")
if success_count > 0:
    display(recent_success_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Failed Actions

# COMMAND ----------

recent_failures_df = spark.sql(f"""
    SELECT 
        action_id,
        workspace_id,
        action_type,
        object_type,
        object_name,
        violator_email,
        error_message,
        retry_count,
        max_retries,
        created_at
    FROM {control_actions_table}
    WHERE remediation_status = 'FAILED'
    AND created_at >= current_timestamp() - INTERVAL 24 HOURS
    ORDER BY created_at DESC
    LIMIT 20
""")

failure_count = recent_failures_df.count()
print(f"\nRecent Failed Actions (Last 24 Hours): {failure_count}")
if failure_count > 0:
    display(recent_failures_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checks

# COMMAND ----------

# Check 1: Verify all pending staging violations were processed
unprocessed_violations_df = spark.sql(f"""
    SELECT 
        violation_id,
        workspace_id,
        object_type,
        object_name,
        user_email,
        violation_type,
        created_at
    FROM {catalog}.{schema}.governance_violations_staging
    WHERE processing_status = 'PENDING'
    AND created_at < current_timestamp() - INTERVAL 5 MINUTES
""")

unprocessed_count = unprocessed_violations_df.count()
if unprocessed_count > 0:
    print(f"⚠️  WARNING: {unprocessed_count} violations are still PENDING after 5+ minutes")
    display(unprocessed_violations_df)
else:
    print("✓ All violations have been processed")

# COMMAND ----------

# Check 2: Verify control actions were created for all violations
orphan_violations_df = spark.sql(f"""
    SELECT 
        s.violation_id,
        s.object_type,
        s.object_name,
        s.processing_status,
        s.created_at
    FROM {catalog}.{schema}.governance_violations_staging s
    LEFT JOIN {control_actions_table} c
        ON s.violation_id = c.violation_id
    WHERE s.processing_status = 'COMPLETED'
    AND c.action_id IS NULL
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
# MAGIC ## Summary Report

# COMMAND ----------

# Get overall statistics
overall_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_actions,
        SUM(CASE WHEN remediation_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
        SUM(CASE WHEN remediation_status = 'FAILED' AND retry_count < max_retries THEN 1 ELSE 0 END) as pending_retry,
        SUM(CASE WHEN remediation_status = 'FAILED' AND retry_count >= max_retries THEN 1 ELSE 0 END) as needs_intervention,
        ROUND(100.0 * SUM(CASE WHEN remediation_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
    FROM {control_actions_table}
    WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
""").collect()[0]

print("\n" + "="*80)
print("VALIDATION SUMMARY (Last 24 Hours)")
print("="*80)
print(f"Total Actions:            {overall_stats['total_actions']}")
print(f"Successful:               {overall_stats['successful']}")
print(f"Pending Retry:            {overall_stats['pending_retry']}")
print(f"Needs Intervention:       {overall_stats['needs_intervention']}")
print(f"Success Rate:             {overall_stats['success_rate_pct']}%")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Status

# COMMAND ----------

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

if overall_stats['success_rate_pct'] < 80:
    validation_issues.append(f"Success rate below 80%: {overall_stats['success_rate_pct']}%")
    validation_status = "WARNING"

if validation_issues:
    print("\n⚠️  Validation Issues:")
    for issue in validation_issues:
        print(f"  - {issue}")
else:
    print("\n✓ Validation passed - no issues detected")

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': validation_status,
    'total_actions': int(overall_stats['total_actions']),
    'successful': int(overall_stats['successful']),
    'pending_retry': int(overall_stats['pending_retry']),
    'needs_intervention': int(overall_stats['needs_intervention']),
    'success_rate_pct': float(overall_stats['success_rate_pct']),
    'validation_issues': validation_issues,
    'timestamp': datetime.utcnow().isoformat()
}))

