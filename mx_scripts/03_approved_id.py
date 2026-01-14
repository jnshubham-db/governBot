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

#dbutils.widgets.text("catalog", "qadl", "Catalog Name")
#dbutils.widgets.text("schema", "sch_mng_admon", "Schema Name")

# COMMAND ----------

import os
environment = os.environ.get('DATABRICKS_RUNTIME_VERSION',None)
print(environment)

flagSERVERLESS = False

if environment.startswith("client."):
	flagSERVERLESS = True
	spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

print(f"Serverless: {flagSERVERLESS}")

# COMMAND ----------

from dbruntime.databricks_repl_context import get_context
workspaceId = get_context().workspaceId

if workspaceId == "4126527463676543":
    catalog = "qadl"
else:
    catalog = "dlprod"
schema = "sch_mng_admon"

print(f"Catalog: {catalog}")

# COMMAND ----------

#catalog = dbutils.widgets.get("catalog")
#schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Define Approved Identities
# MAGIC Update this list with identities (users, groups, service principals) who are authorized to perform governance actions.
# MAGIC These identities' actions will NOT trigger remediation based on their permission flags.
# MAGIC **Format:** Each entry is a dict with the following fields:
# MAGIC - **type**: 'USER', 'GROUP', or 'SERVICE_PRINCIPAL'
# MAGIC - **name**: email for users, group name for groups, SP name for service principals
# MAGIC - **can_manage_resources**: True if identity can create/delete resources (jobs, pipelines, apps, etc.)
# MAGIC - **can_manage_permissions**: True if identity can grant/revoke permissions (changeWorkspaceAcl)
# MAGIC - **approved_actions**: List of object types this identity can create. Options:
# MAGIC   - `["ALL"]` - Can create any object type (default if not specified)
# MAGIC   - `["table", "schema", "volume"]` - Can only create specific object types
# MAGIC   - Group aliases: `["UC_DATA_OBJECTS"]`, `["COMPUTE"]`, `["ML_AI"]`, etc.
# MAGIC **Supported Object Types:**
# MAGIC - Dashboard/BI: `dashboard`, `genieSpace`, `alert`, `query`
# MAGIC - Compute: `cluster`, `clusterPolicy`, `instancePool`, `warehouse`
# MAGIC - Orchestration: `jobs`, `pipelines`
# MAGIC - Apps: `apps`
# MAGIC - ML/AI: `mlflowExperiments`, `servingEndpoint`, `registeredModel`, `featureSpec`, `featureTable`
# MAGIC - Secrets: `secretScope`
# MAGIC - Vector Search: `vectorSearchEndpoint`, `vectorIndex`
# MAGIC - Clean Rooms: `cleanRoom`
# MAGIC - Unity Catalog: `catalog`, `schema`, `table`, `volume`, `function`, `connection`, `externalLocation`, `storageCredential`, `ucRegisteredModel`, `ucModelVersion`, `abacPolicy`
# MAGIC - Delta Sharing: `recipient`, `share`, `provider`
# MAGIC - Monitors: `monitors`
# MAGIC - Workspace: `notebook`, `directory`, `repo`, `folder`
# MAGIC
# MAGIC **Group Aliases (expand to multiple object types):**
# MAGIC - `ALL`: All object types (wildcard)
# MAGIC - `UC_DATA_OBJECTS`: catalog, schema, table, volume, function, tableConstraint
# MAGIC - `UC_SECURITY`: storageCredential, externalLocation, connection
# MAGIC - `UC_ALL`: All Unity Catalog objects
# MAGIC - `COMPUTE`: cluster, clusterPolicy, instancePool, warehouse
# MAGIC - `ML_AI`: mlflowExperiments, servingEndpoint, registeredModel, featureSpec, featureTable, ucRegisteredModel, ucModelVersion
# MAGIC - `DATA_SHARING`: share, recipient, provider
# MAGIC - `DASHBOARDS_BI`: dashboard, genieSpace, alert, query
# MAGIC - `ORCHESTRATION`: jobs, pipelines
# MAGIC - `SECRETS`: secretScope
# MAGIC - `VECTOR_SEARCH`: vectorSearchEndpoint, vectorIndex
# MAGIC - `APPS`: apps
# MAGIC - `MONITORING`: monitors
# MAGIC - `CLEAN_ROOMS`: cleanRoom
# MAGIC
# MAGIC **Note:** An identity can have both flags set to True if they are authorized for both actions.
# MAGIC

# COMMAND ----------

# **UPDATE THIS LIST WITH APPROVED IDENTITIES**
# Each identity has the following permission flags:
#   - can_manage_resources: Can create/delete resources (True/False)
#   - can_manage_permissions: Can grant/revoke permissions (True/False)
#   - approved_actions: List of object types this identity can create
#     - ["ALL"] = can create any object type (default behavior)
#     - ["table", "schema"] = can only create specific object types
#     - ["UC_DATA_OBJECTS"] = can create Unity Catalog data objects (table, schema, volume, etc.)
if workspaceId == "4126527463676543":
    approved_identities = [
        # Users (email addresses)
        # Admin user - can create ALL objects and manage permissions
        # {"name": "shubham.j@databricks.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        
        # Example: Data Engineer - can only create UC data objects (tables, schemas, volumes, functions)
        # {"name": "data.engineer@example.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["UC_DATA_OBJECTS"]},
        
        # Example: ML Engineer - can only create ML/AI objects
        # {"name": "ml.engineer@example.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["ML_AI"]},
        
        # Example: Analytics user - can only create dashboards and queries
        # {"name": "analyst@example.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["dashboard", "query", "alert"]},
        
        # Example: DevOps - can only create compute resources
        # {"name": "devops@example.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["COMPUTE", "jobs", "pipelines"]},

        
        # Admin can do both - manage resources AND manage permissions
        {"name": "s12466@mx.att.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        # Data engineer can only manage resources, not permissions
        {"name": "s01841@mx.att.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["ALL"]},
        {"name": "s01828@mx.att.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["ALL"]},
        
        # Service Principals
        # Governance SP can do both
        # System Service Principal
        {"name": "829e635d-eb80-4c3f-ac0c-9add9c31c557", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        # ADF_Service_principal
        {"name": "b8a66bbe-9d97-4f64-bd3d-9e4768835700", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": True, "approved_actions": ["table","SECRETS","COMPUTE","ORCHESTRATION"]},
        #DataFactory - esazudfwade01
        {"name": "b01451c9-ed9c-4e53-9a21-3928f0bb49c1", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        #DataFactory - esazudfwade02
        {"name": "c45db2e0-8f92-4897-852e-c082d168a7de", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        #DataFactory - esazudfwade04
        {"name": "f720c885-258a-4eca-96da-333412ffecc0", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        # Groups
        # Data engineers group can manage resources only
        {"name": "mx_Azure_DLE_databricks_AdminUsers_qa", "type": "GROUP", "can_manage_resources": False, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        {"name": "users", "type": "GROUP", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table","function","DASHBOARDS_BI","ORCHESTRATION","mlflowExperiments","registeredModel","cleanRoom","ucRegisteredModel","ucModelVersion","notebook","directory","repo","folder"]},
        # Admins group can do both
        #{"name": "admins", "type": "GROUP", "can_manage_resources": True, "can_manage_permissions": True},
    ]
else:
    #Production
    approved_identities = [
        # Users
        # Remover
        {"name": "s12466@mx.att.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        # Campaigns manager
        {"name": "m59079@mx.att.com", "type": "USER", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        # System Service Principal
        {"name": "829e635d-eb80-4c3f-ac0c-9add9c31c557", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        # SPDBPROD
        {"name": "7cdf5dcf-54d6-4a1c-ba10-7fd308054e87", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table","SECRETS","COMPUTE","ORCHESTRATION"]},
        #azdevops
        {"name": "96a21f13-7e6d-4cd3-ae3c-383b712c358b", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table","ML_AI","ORCHESTRATION","VECTOR_SEARCH"]},
        #DataFactory - esazudfwpde02
        {"name": "67084b64-2ac6-4b75-9e93-b3e0f764dc46", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        #DataFactory - esazudfwpde03
        {"name": "efb9c1a7-0a94-4450-8c82-6a2b8d69174a", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        #DataFactory - esazudfwpde04
        {"name": "0855fe1b-f8d7-4b58-be65-00a892bac024", "type": "SERVICE_PRINCIPAL", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["table"]},
        # Groups
        {"name": "AZURE_DLE_databricks_AdminUsers_prod", "type": "GROUP", "can_manage_resources": False, "can_manage_permissions": True, "approved_actions": ["ALL"]},
        {"name": "AZURE_DLE_databricks_Admin_prod", "type": "GROUP", "can_manage_resources": True, "can_manage_permissions": False, "approved_actions": ["ALL"]},
    ]

print(f"Number of approved identities: {len(approved_identities)}")
print("\nApproved identities:")
for identity in approved_identities:
    res_flag = "✓" if identity.get('can_manage_resources', False) else "✗"
    perm_flag = "✓" if identity.get('can_manage_permissions', False) else "✗"
    actions = identity.get('approved_actions', ['ALL'])
    actions_str = ", ".join(actions) if actions else "ALL"
    print(f"  - {identity['type']:20s}: {identity['name']:40s}")
    print(f"      [Resources: {res_flag}] [Permissions: {perm_flag}] [Actions: {actions_str}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data

# COMMAND ----------


from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql.types import *

# Group alias expansions - map group names to their constituent object types
# IMPORTANT: These object types MUST match exactly with object_type values in governance_filters table
APPROVED_ACTION_GROUPS = {
    'ALL': ['ALL'],  # Special wildcard - handled separately in watcher
    
    # Unity Catalog Data Objects
    'UC_DATA_OBJECTS': ['catalog', 'schema', 'table', 'volume', 'function', 'tableConstraint'],
    
    # Unity Catalog Security/Infrastructure
    'UC_SECURITY': ['storageCredential', 'externalLocation', 'connection'],
    
    # All Unity Catalog Objects
    'UC_ALL': ['catalog', 'schema', 'table', 'volume', 'function', 'connection', 
               'externalLocation', 'storageCredential', 'ucRegisteredModel', 'ucModelVersion', 
               'abacPolicy', 'recipient', 'share', 'provider', 'tableConstraint'],
    
    # Compute Resources
    'COMPUTE': ['cluster', 'clusterPolicy', 'instancePool', 'warehouse'],
    
    # ML/AI Resources (both MLflow and UC models)
    'ML_AI': ['mlflowExperiments', 'servingEndpoint', 'registeredModel', 'featureSpec', 
              'featureTable', 'ucRegisteredModel', 'ucModelVersion'],
    
    # Delta Sharing
    'DATA_SHARING': ['share', 'recipient', 'provider'],
    
    # Dashboards and BI
    'DASHBOARDS_BI': ['dashboard', 'genieSpace', 'alert', 'query'],
    
    # Orchestration
    'ORCHESTRATION': ['jobs', 'pipelines'],
    
    # Secrets
    'SECRETS': ['secretScope'],
    
    # Vector Search
    'VECTOR_SEARCH': ['vectorSearchEndpoint', 'vectorIndex'],
    
    # Apps
    'APPS': ['apps'],
    
    # Monitoring
    'MONITORING': ['monitors'],
    
    # Clean Rooms
    'CLEAN_ROOMS': ['cleanRoom'],
}

def expand_approved_actions(actions: List[str]) -> List[str]:
    """Expand group aliases in approved_actions to individual object types."""
    if not actions:
        return ['ALL']  # Default to ALL if not specified
    
    expanded = []
    for action in actions:
        action_upper = action.upper()
        if action_upper in APPROVED_ACTION_GROUPS:
            # It's a group alias - expand it
            expanded.extend(APPROVED_ACTION_GROUPS[action_upper])
        else:
            # It's an individual object type
            expanded.append(action)
    
    return list(set(expanded))  # Remove duplicates

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
        # Get approved_actions with default to ALL (backward compatible)
        approved_actions_raw = identity.get('approved_actions', ['ALL'])
        # Expand any group aliases
        approved_actions = expand_approved_actions(approved_actions_raw)
        
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
            'approved_actions': approved_actions,
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
        StructField('approved_actions', ArrayType(StringType()), True),
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
                approved_actions = source.approved_actions,
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
        approved_actions,
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
- approved_actions: List of specific object types the identity can create
  - ["ALL"] = can create any object (default, backward compatible)
  - ["table", "schema"] = can only create specific object types
  - Group aliases expand automatically (UC_DATA_OBJECTS, COMPUTE, ML_AI, etc.)

Supported Identity Types:
- USER: Individual users (email addresses)
- GROUP: Databricks workspace groups
- SERVICE_PRINCIPAL: Service principals

To add more identities:
1. Update the approved_identities list in this notebook
2. Add entries with 'name', 'type', 'can_manage_resources', 'can_manage_permissions', and 'approved_actions' fields
3. Re-run the notebook

To deactivate an identity:
- Run: UPDATE {table_name}
        SET is_active = false, updated_at = current_timestamp()
        WHERE identity_name = 'name' AND identity_type = 'TYPE'

To update permission flags or approved actions:
- Run: UPDATE {table_name}
        SET can_manage_resources = true/false,
            can_manage_permissions = true/false,
            approved_actions = ARRAY('table', 'schema', 'volume'),
            updated_at = current_timestamp()
        WHERE identity_name = 'name' AND identity_type = 'TYPE'
        
Example - allow a user to only create tables and schemas:
  UPDATE {table_name}
  SET approved_actions = ARRAY('table', 'schema'), updated_at = current_timestamp()
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
