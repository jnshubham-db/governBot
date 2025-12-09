# Databricks notebook source
# MAGIC %md
# MAGIC # Load Approved Identities
# MAGIC
# MAGIC This notebook loads pre-approved identities into the governance_preapproved_identities table.
# MAGIC Simply update the list in the first cell and run the notebook.

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
# MAGIC ## Define Approved Identities
# MAGIC
# MAGIC Update this list with identities (users, groups, service principals) who are authorized to perform governance actions.
# MAGIC These identities' actions will NOT trigger remediation based on their permission flags.
# MAGIC
# MAGIC **Format:** Each entry is a dict with the following fields:
# MAGIC - **type**: 'USER', 'GROUP', or 'SERVICE_PRINCIPAL'
# MAGIC - **name**: email for users, group name for groups, SP name for service principals
# MAGIC - **can_manage_resources**: True if identity can create/delete resources (jobs, pipelines, apps, etc.)
# MAGIC - **can_manage_permissions**: True if identity can grant/revoke permissions (changeWorkspaceAcl)
# MAGIC
# MAGIC **Note:** An identity can have both flags set to True if they are authorized for both actions.

# COMMAND ----------

# **UPDATE THIS LIST WITH APPROVED IDENTITIES**
# Each identity has two permission flags:
#   - can_manage_resources: Can create/delete jobs, pipelines, apps, experiments, monitors
#   - can_manage_permissions: Can grant/revoke permissions (changeWorkspaceAcl)
approved_identities = [
    # Users (email addresses)
    # Admin can do both - manage resources AND manage permissions
    {"name": "admin@example.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": True},
    # Data engineer can only manage resources, not permissions
    {"name": "dataengineer@example.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False},
    
    # Service Principals
    # Governance SP can do both
    {"name": "governance-sp", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": True},
    
    # Groups
    # Data engineers group can manage resources only
    {"name": "data-engineers", "type": "GROUP", "can_manage_resources": True, "can_manage_permissions": False},
    # Admins group can do both
    {"name": "admins", "type": "GROUP", "can_manage_resources": True, "can_manage_permissions": True},
    
    # Add more identities here
]

print(f"Number of approved identities: {len(approved_identities)}")
print("\nApproved identities:")
for identity in approved_identities:
    res_flag = "✓" if identity.get('can_manage_resources', False) else "✗"
    perm_flag = "✓" if identity.get('can_manage_permissions', False) else "✗"
    print(f"  - {identity['type']:20s}: {identity['name']:40s} [Resources: {res_flag}] [Permissions: {perm_flag}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data

# COMMAND ----------

from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql.types import *

def prepare_identity_records(identities: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Prepare identity records for insertion."""
    records = []
    current_time = datetime.utcnow()
    
    for identity in identities:
        identity_name = identity['name']
        identity_type = identity['type']
        # Get permission flags with defaults (backward compatible - default to True for resources)
        can_manage_resources = identity.get('can_manage_resources', True)
        can_manage_permissions = identity.get('can_manage_permissions', False)
        
        # Extract display name (simple extraction from email or use as-is)
        if identity_type == 'USER' and '@' in identity_name:
            display_name = identity_name.split('@')[0]
        else:
            display_name = identity_name
        
        records.append({
            'identity_name': identity_name,
            'identity_type': identity_type,
            'display_name': display_name,
            'can_manage_resources': can_manage_resources,
            'can_manage_permissions': can_manage_permissions,
            'is_active': True,
            'created_at': current_time,
            'updated_at': current_time
        })
    
    return records

identity_records = prepare_identity_records(approved_identities)
print(f"Prepared {len(identity_records)} identity records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to Table

# COMMAND ----------

if identity_records:
    # Define schema for identity records
    identity_schema = StructType([
        StructField('identity_name', StringType(), True),
        StructField('identity_type', StringType(), True),
        StructField('display_name', StringType(), True),
        StructField('can_manage_resources', BooleanType(), True),
        StructField('can_manage_permissions', BooleanType(), True),
        StructField('is_active', BooleanType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True)
    ])
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(identity_records, schema=identity_schema)
    
    # Write to table
    table_name = f"{catalog}.{schema}.governance_preapproved_identities"
    df.createOrReplaceTempView("new_identities")
    
    # Merge with existing data
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING new_identities AS source
        ON target.identity_name = source.identity_name 
        AND target.identity_type = source.identity_type
        WHEN MATCHED THEN 
            UPDATE SET 
                is_active = source.is_active,
                display_name = source.display_name,
                can_manage_resources = source.can_manage_resources,
                can_manage_permissions = source.can_manage_permissions,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✓ Loaded {len(identity_records)} identities to {table_name}")
else:
    print("No identities to load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Loaded Data

# COMMAND ----------

table_name = f"{catalog}.{schema}.governance_preapproved_identities"

# Count active identities
count_df = spark.sql(f"""
    SELECT 
        COUNT(*) as total_count,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_count
    FROM {table_name}
""")

print("Identity Status:")
display(count_df)

# Show all identities
all_identities_df = spark.sql(f"""
    SELECT 
        identity_type,
        identity_name,
        display_name,
        can_manage_resources,
        can_manage_permissions,
        is_active,
        created_at,
        updated_at
    FROM {table_name}
    ORDER BY identity_type, identity_name
""")

print("\nAll Identities:")
display(all_identities_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════════════════════════╗
║                    APPROVED IDENTITIES LOADED                             ║
╚══════════════════════════════════════════════════════════════════════════╝

The following identities are now approved with granular permissions:
- can_manage_resources: Can create/delete jobs, pipelines, apps, experiments, monitors
- can_manage_permissions: Can grant/revoke permissions (changeWorkspaceAcl)

Supported Identity Types:
- USER: Individual users (email addresses)
- GROUP: Databricks workspace groups
- SERVICE_PRINCIPAL: Service principals

To add more identities:
1. Update the approved_identities list in this notebook
2. Add entries with 'name', 'type', 'can_manage_resources', and 'can_manage_permissions' fields
3. Re-run the notebook

To deactivate an identity:
- Run: UPDATE {table_name}
        SET is_active = false, updated_at = current_timestamp()
        WHERE identity_name = 'name' AND identity_type = 'TYPE'

To update permission flags:
- Run: UPDATE {table_name}
        SET can_manage_resources = true/false,
            can_manage_permissions = true/false,
            updated_at = current_timestamp()
        WHERE identity_name = 'name' AND identity_type = 'TYPE'
        
Example - grant permission management to a user:
  UPDATE {table_name}
  SET can_manage_permissions = true, updated_at = current_timestamp()
  WHERE identity_name = 'user@example.com' AND identity_type = 'USER'
""")

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'loaded_count': len(identity_records),
    'timestamp': datetime.utcnow().isoformat()
}))

