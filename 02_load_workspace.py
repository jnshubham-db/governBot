# Databricks notebook source
# MAGIC %md
# MAGIC # Load Workspace Configuration
# MAGIC
# MAGIC This notebook loads workspace configuration into the governance_config_workspaces table.
# MAGIC Simply update the configuration list and run the notebook.

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
# MAGIC ## Define Workspace Configurations
# MAGIC
# MAGIC Update this list with your workspace configurations.

# COMMAND ----------

# **UPDATE THIS LIST WITH YOUR WORKSPACE CONFIGURATIONS**
workspace_configs = [
    {
        'workspace_id': '1234567890123456',
        'workspace_name': 'Production Workspace',
        'workspace_url': 'https://your-workspace.cloud.databricks.com',
        'enforcement_enabled': True,
        'notification_email': 'admin@example.com',
        'notification_slack_webhook': None,  # Optional: add Slack webhook URL
        'enabled_object_types': ['notebook', 'query', 'dashboard', 'job', 'cluster', 'pipeline'],
        'max_retry_attempts': 3,
        'created_by': 'admin'
    },
    # Add more workspace configurations here
    # {
    #     'workspace_id': '9876543210987654',
    #     'workspace_name': 'Dev Workspace',
    #     'workspace_url': 'https://dev-workspace.cloud.databricks.com',
    #     'enforcement_enabled': False,
    #     'notification_email': 'devops@example.com',
    #     'notification_slack_webhook': None,
    #     'enabled_object_types': ['notebook', 'query'],
    #     'max_retry_attempts': 3,
    #     'created_by': 'admin'
    # },
]

print(f"Number of workspace configurations: {len(workspace_configs)}")
print("\nWorkspace configurations:")
for ws in workspace_configs:
    status = "ENABLED" if ws['enforcement_enabled'] else "DISABLED"
    print(f"  - {ws['workspace_name']} ({ws['workspace_id']}): {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data

# COMMAND ----------

from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql.types import *

def prepare_workspace_records(configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepare workspace configuration records for insertion."""
    records = []
    current_time = datetime.utcnow()
    
    for config in configs:
        records.append({
            'workspace_id': config['workspace_id'],
            'workspace_name': config['workspace_name'],
            'workspace_url': config['workspace_url'],
            'enforcement_enabled': config.get('enforcement_enabled', False),
            'notification_email': config.get('notification_email'),
            'notification_slack_webhook': config.get('notification_slack_webhook'),
            'enabled_object_types': config.get('enabled_object_types', ['notebook', 'query']),
            'max_retry_attempts': config.get('max_retry_attempts', 3),
            'created_at': current_time,
            'updated_at': current_time,
            'created_by': config.get('created_by', 'admin')
        })
    
    return records

workspace_records = prepare_workspace_records(workspace_configs)
print(f"Prepared {len(workspace_records)} workspace configuration records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to Table

# COMMAND ----------

if workspace_records:
    # Define schema for workspace records
    workspace_schema = StructType([
        StructField('workspace_id', StringType(), True),
        StructField('workspace_name', StringType(), True),
        StructField('workspace_url', StringType(), True),
        StructField('enforcement_enabled', BooleanType(), True),
        StructField('notification_email', StringType(), True),
        StructField('notification_slack_webhook', StringType(), True),
        StructField('enabled_object_types', ArrayType(StringType()), True),
        StructField('max_retry_attempts', IntegerType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True),
        StructField('created_by', StringType(), True)
    ])
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(workspace_records, schema=workspace_schema)
    
    # Write to table
    table_name = f"{catalog}.{schema}.governance_config_workspaces"
    df.createOrReplaceTempView("new_workspaces")
    
    # Merge with existing data
    spark.sql(f"""
        MERGE INTO {table_name} AS target
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
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✓ Loaded {len(workspace_records)} workspace configurations to {table_name}")
else:
    print("No workspace configurations to load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Loaded Data

# COMMAND ----------

table_name = f"{catalog}.{schema}.governance_config_workspaces"

# Count workspaces
count_df = spark.sql(f"""
    SELECT 
        COUNT(*) as total_count,
        SUM(CASE WHEN enforcement_enabled THEN 1 ELSE 0 END) as enabled_count
    FROM {table_name}
""")

print("Workspace Status:")
display(count_df)

# Show all workspaces
all_workspaces_df = spark.sql(f"""
    SELECT 
        workspace_id,
        workspace_name,
        workspace_url,
        enforcement_enabled,
        notification_email,
        enabled_object_types,
        max_retry_attempts,
        created_by,
        created_at,
        updated_at
    FROM {table_name}
    ORDER BY workspace_name
""")

print("\nAll Workspace Configurations:")
display(all_workspaces_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

enabled_workspaces = [ws['workspace_name'] for ws in workspace_configs if ws['enforcement_enabled']]
enabled_list = '\n  - '.join(enabled_workspaces) if enabled_workspaces else '  (none)'

print(f"""
╔══════════════════════════════════════════════════════════════════════════╗
║                 WORKSPACE CONFIGURATIONS LOADED                           ║
╚══════════════════════════════════════════════════════════════════════════╝

Loaded {len(workspace_configs)} workspace configuration(s)

Enforcement ENABLED for:
  {enabled_list}

To update workspace configuration:
1. Modify the workspace_configs list in this notebook
2. Re-run the notebook

To enable/disable enforcement for a workspace:
- Run: UPDATE {table_name}
        SET enforcement_enabled = true, updated_at = current_timestamp()
        WHERE workspace_id = 'your_workspace_id'
        
Next Steps:
1. Ensure approved identities are loaded (run approved_id notebook)
2. Run discovery to catalog existing resources (run discover notebook)
3. Start the governance workflow (watcher -> remediation -> validation -> notification)
""")

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'loaded_count': len(workspace_records),
    'timestamp': datetime.utcnow().isoformat()
}))

