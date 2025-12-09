# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Governance Tables
# MAGIC
# MAGIC This notebook creates all required tables for the governance system.
# MAGIC No defaults, constraints, or indexes as per requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")
dbutils.widgets.dropdown("drop_existing", "true", ["true", "false"], "Drop Existing Tables")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
drop_existing = dbutils.widgets.get("drop_existing").lower() == "true"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Drop Existing: {drop_existing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

print(f"✓ Catalog and schema created: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: Workspace Configuration

# COMMAND ----------

workspace_config_table = f"{catalog}.{schema}.governance_config_workspaces"

if drop_existing:
    spark.sql(f"DROP TABLE IF EXISTS {workspace_config_table}")
    print(f"Dropped existing table: {workspace_config_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {workspace_config_table} (
    workspace_id STRING,
    workspace_name STRING,
    workspace_url STRING,
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
# MAGIC ## Table 2: Pre-Approved Objects

# COMMAND ----------

preapproved_objects_table = f"{catalog}.{schema}.governance_preapproved_objects"

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
        permission_level: STRING
    >>,
    metadata MAP<STRING, STRING>,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Pre-approved objects and their permissions'
""")

print(f"✓ Created table: {preapproved_objects_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: Pre-Approved Identities

# COMMAND ----------

preapproved_identities_table = f"{catalog}.{schema}.governance_preapproved_identities"

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
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Pre-approved identities (users, groups, service principals) with granular permission flags for resource management and permission management'
""")

print(f"✓ Created table: {preapproved_identities_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: Staging Table for Violations

# COMMAND ----------

staging_table = f"{catalog}.{schema}.governance_violations_staging"

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
# MAGIC ## Table 5: Control Actions (Audit Trail)

# COMMAND ----------

control_actions_table = f"{catalog}.{schema}.governance_control_actions"

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
# MAGIC ## Table 6: Governance Filters (Dynamic Audit Log Mappings)

# COMMAND ----------

governance_filters_table = f"{catalog}.{schema}.governance_filters"

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
COMMENT 'Dynamic filter configurations for audit log processing - maps service/action to object extraction and remediation'
""")

print(f"✓ Created table: {governance_filters_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Tables

# COMMAND ----------

tables_to_verify = [
    "governance_config_workspaces",
    "governance_preapproved_objects",
    "governance_preapproved_identities",
    "governance_violations_staging",
    "governance_control_actions",
    "governance_filters"
]

print("\n" + "="*80)
print("TABLE VERIFICATION")
print("="*80 + "\n")

for table_name in tables_to_verify:
    full_table_name = f"{catalog}.{schema}.{table_name}"
    count = spark.table(full_table_name).count()
    print(f"✓ {full_table_name}: {count} rows")

print("\n" + "="*80)
print("ALL TABLES CREATED SUCCESSFULLY!")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════════════════════════╗
║                       GOVERNANCE TABLES SETUP COMPLETE                    ║
╚══════════════════════════════════════════════════════════════════════════╝

Catalog: {catalog}
Schema: {schema}

Tables Created:
  1. governance_config_workspaces      - Workspace configurations
  2. governance_preapproved_objects    - Approved objects and permissions
  3. governance_preapproved_identities - Approved user identities
  4. governance_violations_staging     - Detected violations staging
  5. governance_control_actions        - Remediation action audit trail
  6. governance_filters                - Dynamic audit log filter mappings

Next Steps:
  1. Load workspace configurations using load_workspace notebook
  2. Load approved identities using approved_id notebook
  3. Load governance filters using load_filters notebook
  4. Run discovery notebook to populate preapproved objects
  5. Configure and start governance workflow
""")

