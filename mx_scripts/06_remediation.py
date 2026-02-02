# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Remediation
# MAGIC
# MAGIC This notebook reads violations from the staging table and executes remediation actions:
# MAGIC - **DELETE_RESOURCE**: Delete unauthorized resources (jobs, pipelines, apps, etc.)
# MAGIC - **REVERT_PERMISSION**: Revert unauthorized permission changes
# MAGIC - **REPORT_DELETION**: Log deletion events for security team review (no automated action)
# MAGIC - **REPORT_SECURITY_TEAM**: No actions, only to report
# MAGIC - **SKIP_REMEDIATION**: No actions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

#dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
#dbutils.widgets.text("schema", "sch_mng_admon", "Schema Name")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run Mode")
dbutils.widgets.dropdown("auth_type","azure-client-secret",["pat","azure-client-secret"],"Auth Type")

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

try:
    auth_type = dbutils.widgets.get("auth_type")
except Exception as e:
    auth_type = "azure-client-secret"

try:
    load_filters = True if dbutils.widgets.get("load_filters") == "Y" or dbutils.widgets.get("load_filters") == "S" else False
except:
    load_filters = False

try:
    enable_discover = True if dbutils.widgets.get("enable_discover") == "Y" or dbutils.widgets.get("enable_discover") == "S" else False
except:
    enable_discover = False

try:
    load_approved_id = True if dbutils.widgets.get("load_approved_id") == "Y" or dbutils.widgets.get("load_approved_id") == "S" else False
except:
    load_approved_id = False

print(f"Auth type: {auth_type}")
print(f"Load Filters: {load_filters}")
print(f"Enable Discover: {enable_discover}")
print(f"Load Approved identities: {load_approved_id}")

# COMMAND ----------

from dbruntime.databricks_repl_context import get_context
from databricks.sdk.errors import InvalidParameterValue
from datetime import datetime
import json
import pytz
from delta.tables import DeltaTable

workspaceId = get_context().workspaceId
tz = pytz.timezone("America/Mexico_City")

if workspaceId == "4126527463676543":
    catalog = "qadl"
    if auth_type == "azure-client-secret":
        kv_scope = "azueskvsadl01"
        kv_client_id_key = "b8a66bbe-9d97-4f64-bd3d-9e4768835700"
        kv_client_secret_key = 'serviceprincipal-SPDBPROD'
        kv_tenant_id_key = 'ApiRestTenant'
    elif auth_type == "pat":
        kv_scope = "azueskvsadl01"
        kv_client_secret_key = "add-secret-id-token-databricks"
        kv_client_secret_key2 = "add-secret-id-token-databricks2"
else:
    catalog = "dlprod"
    if auth_type == "azure-client-secret":
        kv_scope = "TBD" #"esazukvspdl01"
        kv_client_id_key = "TDB" #"7cdf5dcf-54d6-4a1c-ba10-7fd308054e87"
        kv_client_secret_key = "TBD" #"serviceprincipal-SPDBPROD"
        kv_tenant_id_key = "TBD" #"ApiRestTenant"
    elif auth_type == "pat":
        kv_scope = "esazukvspcso01"
        kv_client_secret_key = "WADatabricks"
        kv_client_secret_key2 = "WADatabricks2"

schema = "sch_mng_admon"

print(f"Catalog: {catalog}")

# COMMAND ----------

if load_filters or enable_discover or load_approved_id:
    dbutils.notebook.exit(json.dumps({
    'status': 'SKIPPED',
    'reason': "Proceso abanderado para realizar el discovery de objetos, actualizar los filtros o agregar Identidades para manejo de recursos/permisos",
    'load filters': load_filters,
    'Enable discover': enable_discover,
    'load approved identities': load_approved_id,
    'timestamp': datetime.now(tz).isoformat()
}, indent=3))

# COMMAND ----------

#catalog = dbutils.widgets.get("catalog")
#schema = dbutils.widgets.get("schema")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Dry Run: {dry_run}")
print(f"Key Vault Scope: {kv_scope if kv_scope else '(not set - will use default client)'}")

if dry_run:
    print("\n" + "="*80)
    print("DRY RUN MODE - No actual changes will be made")
    print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Workspace Clients

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from typing import Tuple, Optional, Dict
from datetime import datetime
from pyspark.sql.types import *
from dbruntime.databricks_repl_context import get_context
import json
import re


def _detect_principal_type(principal: str) -> str:
    """
    Detect principal type based on the principal identifier.
    
    Rules:
    - Users: contain '@' in email format (e.g., user@company.com)
    - Service Principals: follow UUID/GUID pattern (e.g., d118594b-a1db-41b2-a6e2-a201377c2aec)
    - Groups: everything else (e.g., "admins", "data_engineers", "users")
    
    Returns: 'user', 'service_principal', or 'group'
    """
    if not principal:
        return 'user'
    
    # UUID/GUID pattern for service principals
    # Format: 8-4-4-4-12 hexadecimal characters
    uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    
    if re.match(uuid_pattern, principal):
        # Service principal (UUID format)
        return 'service_principal'
    elif '@' in principal:
        # User (email format)
        return 'user'
    else:
        # Group (e.g., "admins", "data_engineers")
        return 'group'

# Get current workspace ID
current_workspace_id = get_context().workspaceId
print(f"Current Workspace ID: {current_workspace_id}")

def create_workspace_client(workspace_url: str) -> WorkspaceClient:
    """Create a WorkspaceClient with Azure authentication using Key Vault secrets."""    
    if kv_scope and auth_type == "azure-client-secret":
        # Get credentials from Key Vault
        azure_client_id = kv_client_id_key #dbutils.secrets.get(scope=kv_scope, key=kv_client_id_key)
        client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key)
        tenant_id = dbutils.secrets.get(scope=kv_scope, key=kv_tenant_id_key)

        return WorkspaceClient(
            host=workspace_url,
            azure_client_id=azure_client_id,
            azure_client_secret=client_secret,
            azure_tenant_id=tenant_id,
            auth_type="azure-client-secret"
        )
    elif kv_scope and auth_type == "pat":
        if "4126527463676543" in workspace_url or "4782182804791024" in workspace_url:
            client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key)
        else:
            client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key2)

        return WorkspaceClient(
            host=workspace_url,
            token = client_secret,
            auth_type="pat"
        )
    else:
        return WorkspaceClient()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Pending Violations

# COMMAND ----------

# Load pending violations from staging table
# Include both PENDING (for active remediation) and PENDING_REPORT (for deletion reporting)
staging_table = f"{catalog}.{schema}.governance_violations_staging"
control_actions_table = f"{catalog}.{schema}.governance_control_actions"

new_violations_df = spark.sql(f"""
    SELECT 
        violation_id,
        workspace_id,
        event_id,
        event_time,
        action_name,
        user_email,
        object_id,
        object_type,
        object_name,
        is_permission_change,
        COALESCE(is_delete_event, false) as is_delete_event,
        COALESCE(is_entitlement_change, false) as is_entitlement_change,
        violation_type,
        violation_reason,
        remediation_action,
        0 as retry_count,
        current_timestamp() as created_at
    FROM {staging_table}
    WHERE processing_status IN ('PENDING', 'PENDING_REPORT')
    ORDER BY event_time ASC
""")
retryable_violations_df = spark.sql(f"""
    SELECT 
        v.violation_id,
        v.workspace_id,
        v.event_id,
        v.event_time,
        v.action_name,
        v.user_email,
        v.object_id,
        v.object_type,
        v.object_name,
        v.is_permission_change,
        COALESCE(v.is_delete_event, false) as is_delete_event,
        COALESCE(v.is_entitlement_change, false) as is_entitlement_change,
        v.violation_type,
        v.violation_reason,
        v.remediation_action,
        ca.retry_count + 1 as retry_count,
        ca.created_at as created_at
    FROM {staging_table} v
    INNER JOIN {control_actions_table} ca ON v.violation_id = ca.violation_id
    WHERE ca.remediation_status <> 'SUCCESS' AND ca.retry_count <= ca.max_retries
""")

pending_violations_df = retryable_violations_df.union(new_violations_df)

display(pending_violations_df)
pending_count = pending_violations_df.count()
print(f"⚠ Found {pending_count} pending violations to remediate")

if pending_count == 0:
    print("✅ No pending violations. Exiting.")
    dbutils.notebook.exit('{"status": "SUCCESS", "remediated_count": 0}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remediation Functions

# COMMAND ----------

# ==============================================================================
# DELETE FUNCTIONS FOR VARIOUS OBJECT TYPES
# ==============================================================================

def delete_job(client, job_id: str) -> Tuple[bool, Optional[str]]:
    """Delete a job."""
    try:
        client.jobs.delete(int(job_id))
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_cluster(client, cluster_id: str) -> Tuple[bool, Optional[str]]:
    """Permanently delete a cluster."""
    try:
        client.clusters.permanent_delete(cluster_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_pipeline(client, pipeline_id: str) -> Tuple[bool, Optional[str]]:
    """Delete a pipeline."""
    try:
        client.pipelines.delete(pipeline_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_serving_endpoint(client, endpoint_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a serving endpoint."""
    try:
        client.serving_endpoints.delete(endpoint_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_app(client, app_name: str) -> Tuple[bool, Optional[str]]:
    """Delete an app."""
    try:
        client.apps.delete(app_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_mlflow_experiment(client, experiment_id: str) -> Tuple[bool, Optional[str]]:
    """Delete an MLflow experiment."""
    try:
        client.experiments.delete_experiment(experiment_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_monitor(client, table_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a data quality monitor."""
    try:
        # Use quality_monitors API (the correct SDK attribute name)
        if hasattr(client, 'quality_monitors'):
            client.quality_monitors.delete(table_name=table_name)
            return (True, None)
        else:
            return (False, "Lakehouse Monitoring API not available in SDK. Consider upgrading databricks-sdk to 0.20.0 or later.")
    except AttributeError as ae:
        return (False, f"Lakehouse Monitoring API not available: {str(ae)}")
    except Exception as e:
        return (False, str(e))

def delete_alert(client, alert_id: str) -> Tuple[bool, Optional[str]]:
    """Delete/trash an alert."""
    try:
        client.alerts.delete(alert_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_alertsv2(client, alert_id: str) -> Tuple[bool, Optional[str]]:
    """Delete/trash an alert v2."""
    try:
        client.alerts_v2.trash_alert(alert_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_warehouse(client, warehouse_id: str) -> Tuple[bool, Optional[str]]:
    """Delete a SQL warehouse."""
    try:
        client.warehouses.delete(warehouse_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_cluster_policy(client, policy_id: str) -> Tuple[bool, Optional[str]]:
    """Delete a cluster policy."""
    try:
        client.cluster_policies.delete(policy_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_instance_pool(client, pool_id: str) -> Tuple[bool, Optional[str]]:
    """Delete an instance pool."""
    try:
        client.instance_pools.delete(pool_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_secret_scope(client, scope_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a secret scope."""
    try:
        client.secrets.delete_scope(scope_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_vector_search_endpoint(client, endpoint_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a vector search endpoint."""
    try:
        client.vector_search_endpoints.delete_endpoint(endpoint_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_vector_index(client, index_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a vector index."""
    try:
        client.vector_search_indexes.delete_index(index_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_registered_model(client, model_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a registered model from model registry."""
    try:
        client.model_registry.delete_model(model_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_uc_registered_model(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a registered model from Unity Catalog."""
    try:
        client.registered_models.delete(full_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_dashboard(client, dashboard_id: str) -> Tuple[bool, Optional[str]]:
    """Trash a dashboard (supports both Lakeview and legacy SQL dashboards)."""
    try:
        # Try Lakeview dashboard first (newer AI/BI dashboards)
        try:
            client.lakeview.trash(dashboard_id)
            return (True, None)
        except AttributeError:
            pass  # Lakeview API not available
        except Exception as lakeview_error:
            # If it's not a Lakeview dashboard, try legacy
            if "not found" not in str(lakeview_error).lower():
                pass  # Try legacy anyway
        
        # Try legacy SQL dashboard API
        try:
            client.dashboards.delete(dashboard_id)
            return (True, None)
        except Exception as legacy_error:
            return (False, f"Failed to delete dashboard: {str(legacy_error)}")
    except Exception as e:
        return (False, str(e))


def delete_lakeview_dashboard(client, dashboard_id: str) -> Tuple[bool, Optional[str]]:
    """Trash a Lakeview (AI/BI) dashboard specifically."""
    try:
        client.lakeview.trash(dashboard_id)
        return (True, None)
    except AttributeError:
        return (False, "Lakeview API not available in SDK")
    except Exception as e:
        return (False, str(e))

def delete_query(client, query_id: str) -> Tuple[bool, Optional[str]]:
    """Trash a query."""
    try:
        client.queries.delete(query_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_uc_catalog(client, catalog_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog catalog."""
    try:
        # client.catalogs.delete(catalog_name, force=True)
        # return (True, None)
        raise Exception(f"Catalog deletion not supported. Catalog: {catalog_name}")
    except Exception as e:
        return (False, str(e))

def delete_uc_schema(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog schema."""
    try:
        # client.schemas.delete(full_name)
        # return (True, None)
        raise Exception(f"Schema deletion not supported. Schema: {full_name}")
    except Exception as e:
        return (False, str(e))

def delete_uc_table(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog table."""
    try:
        # client.tables.delete(full_name)
        # return (True, None)
        raise Exception(f"Table deletion not supported. Table: {full_name}")
    except Exception as e:
        return (False, str(e))

def delete_uc_volume(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog volume."""
    try:
        client.volumes.delete(full_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_uc_connection(client, connection_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog connection."""
    try:
        client.connections.delete(connection_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_uc_function(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog function."""
    try:
        client.functions.delete(full_name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_genie_space(client, space_id: str) -> Tuple[bool, Optional[str]]:
    """Delete/trash a Genie space (AI/BI Genie)."""
    try:
        # Genie spaces are managed through the genie API
        # The SDK method may vary by version - try multiple approaches
        try:
            # Try the standard genie API
            client.genie.delete(space_id)
            return (True, None)
        except AttributeError:
            # Fallback: Try using the workspace API to delete the genie space
            # Genie spaces are often stored as workspace objects
            try:
                client.workspace.delete(path=f"/Workspace/.genie/{space_id}")
                return (True, None)
            except Exception:
                pass
            # If all else fails, report that manual deletion is required
            return (False, "Genie space deletion requires manual intervention - API method not available in SDK")
    except Exception as e:
        return (False, str(e))

def delete_feature_table(client, table_name: str) -> Tuple[bool, Optional[str]]:
    """
    Delete a feature table.
    Note: In Unity Catalog, feature tables are regular Delta tables.
    The feature_store API is deprecated in favor of UC tables.
    """
    try:
        # First try the feature engineering client if available
        try:
            from databricks.feature_engineering import FeatureEngineeringClient
            fe_client = FeatureEngineeringClient()
            fe_client.drop_table(name=table_name)
            return (True, None)
        except ImportError:
            pass
        except Exception:
            pass
        
        # Fallback: Feature tables in UC are just regular tables
        # Try deleting as a UC table
        try:
            client.tables.delete(table_name)
            return (True, None)
        except Exception as table_error:
            # If table deletion fails, try the legacy feature store API
            try:
                # Legacy workspace feature store
                from databricks.feature_store import FeatureStoreClient
                fs_client = FeatureStoreClient()
                fs_client.drop_table(name=table_name)
                return (True, None)
            except ImportError:
                return (False, f"Feature table deletion failed: {str(table_error)}. Feature store client not available.")
            except Exception as fs_error:
                return (False, f"Feature table deletion failed: {str(fs_error)}")
    except Exception as e:
        return (False, str(e))

def delete_external_location(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog external location."""
    try:
        # client.external_locations.delete(name)
        # return (True, None)
        raise Exception(f"External location deletion not supported. External location: {name}")
    except Exception as e:
        return (False, str(e))

def delete_storage_credential(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog storage credential."""
    try:
        # client.storage_credentials.delete(name)
        # return (True, None)
        raise Exception(f"Storage credential deletion not supported. Storage credential: {name}")
    except Exception as e:
        return (False, str(e))

def delete_share(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog share."""
    try:
        client.shares.delete(name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_recipient(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog recipient."""
    try:
        client.recipients.delete(name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_provider(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog provider."""
    try:
        client.providers.delete(name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_clean_room(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a clean room."""
    try:
        client.clean_rooms.delete(name)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_metastore(client, metastore_id: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog metastore."""
    try:
        # client.metastores.delete(metastore_id, force=True)
        # return (True, None)
        raise Exception(f"Metastore deletion not supported. Metastore: {metastore_id}")
    except Exception as e:
        return (False, str(e))

def delete_model_version(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a model version from model registry."""
    try:
        # Model version name format is typically "model_name/version"
        parts = name.rsplit('/', 1)
        if len(parts) == 2:
            client.model_registry.delete_model_version(name=parts[0], version=parts[1])
        else:
            # Try as UC registered model version
            client.registered_models.delete_alias(full_name=name, alias="latest")
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_uc_model_version(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog model version."""
    try:
        # UC model version format: catalog.schema.model/version
        parts = full_name.rsplit('/', 1)
        if len(parts) == 2:
            # Delete specific version - UC doesn't have direct version delete, 
            # would need to delete the entire model or use aliases
            return (False, "UC model version deletion requires deleting the entire model")
        return (False, "Invalid model version format")
    except Exception as e:
        return (False, str(e))

def delete_table_constraint(client, full_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog table constraint."""
    try:
        # Constraint deletion requires table name and constraint name
        # full_name format expected: catalog.schema.table.constraint_name
        parts = full_name.rsplit('.', 1)
        if len(parts) == 2:
            table_name = parts[0]
            constraint_name = parts[1]
            client.tables.delete_constraint(full_name=table_name, name=constraint_name)
            return (True, None)
        return (False, "Invalid constraint name format")
    except Exception as e:
        return (False, str(e))

def delete_registry_webhook(client, webhook_id: str) -> Tuple[bool, Optional[str]]:
    """Delete a model registry webhook."""
    try:
        client.model_registry.delete_webhook(id=webhook_id)
        return (True, None)
    except Exception as e:
        return (False, str(e))

def delete_abac_policy(client, policy_name: str) -> Tuple[bool, Optional[str]]:
    """Delete a Unity Catalog ABAC policy."""
    try:
        # ABAC policies are managed through the Unity Catalog grants/policies API
        # The exact API may vary - this is a placeholder
        client.workspace_bindings.delete(name=policy_name)
        return (True, None)
    except AttributeError:
        return (False, "ABAC policy deletion API not available in SDK")
    except Exception as e:
        return (False, str(e))

def delete_feature_spec(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a feature spec."""
    try:
        # Feature specs are managed through the feature engineering API
        try:
            from databricks.feature_engineering import FeatureEngineeringClient
            fe_client = FeatureEngineeringClient()
            fe_client.delete_feature_spec(name=name)
            return (True, None)
        except ImportError:
            return (False, "Feature engineering client not available")
        except Exception as e:
            return (False, str(e))
    except Exception as e:
        return (False, str(e))

def delete_database_instance(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a database instance (Lakebase)."""
    try:
        # Database instances are part of Lakebase (preview feature)
        # The API may vary based on SDK version
        try:
            client.database_instances.delete(name=name)
            return (True, None)
        except AttributeError:
            return (False, "Database instance deletion API not available in SDK")
    except Exception as e:
        return (False, str(e))

def delete_database_catalog(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a database catalog (Lakebase)."""
    try:
        try:
            client.database_catalogs.delete(name=name)
            return (True, None)
        except AttributeError:
            return (False, "Database catalog deletion API not available in SDK")
    except Exception as e:
        return (False, str(e))

def delete_database_table(client, name: str) -> Tuple[bool, Optional[str]]:
    """Delete a database table (Lakebase)."""
    try:
        try:
            client.database_tables.delete(name=name)
            return (True, None)
        except AttributeError:
            return (False, "Database table deletion API not available in SDK")
    except Exception as e:
        return (False, str(e))

# ==============================================================================
# DELETE FUNCTION DISPATCHER
# ==============================================================================

def get_delete_function(object_type: str):
    """
    Returns the appropriate delete function for the given object type.
    This is a dispatcher that maps object types to their delete functions.
    """
    delete_functions = {
        # Jobs & Compute
        'jobs': delete_job,
        'job': delete_job,
        'cluster': delete_cluster,
        'pipelines': delete_pipeline,
        'pipeline': delete_pipeline,
        'clusterPolicy': delete_cluster_policy,
        'instancePool': delete_instance_pool,
        # SQL & Warehouse
        'alert': delete_alert,
        'alertsv2': delete_alertsv2,
        'warehouse': delete_warehouse,
        'query': delete_query,
        'dashboard': delete_dashboard,  # Handles both legacy and Lakeview
        'lakeview_dashboard': delete_lakeview_dashboard,  # Lakeview-specific
        # ML & Serving
        'servingEndpoint': delete_serving_endpoint,
        'apps': delete_app,
        'mlflowExperiments': delete_mlflow_experiment,
        'monitors': delete_monitor,
        'registeredModel': delete_registered_model,
        'ucRegisteredModel': delete_uc_registered_model,
        'featureTable': delete_feature_table,
        # Security & Secrets
        'secretScope': delete_secret_scope,
        # Vector Search
        'vectorSearchEndpoint': delete_vector_search_endpoint,
        'vectorIndex': delete_vector_index,
        # Unity Catalog
        'catalog': delete_uc_catalog,
        'schema': delete_uc_schema,
        'table': delete_uc_table,
        'volume': delete_uc_volume,
        'connection': delete_uc_connection,
        'function': delete_uc_function,
        'externalLocation': delete_external_location,
        'storageCredential': delete_storage_credential,
        # Delta Sharing
        'share': delete_share,
        'recipient': delete_recipient,
        'provider': delete_provider,
        # Other
        'genieSpace': delete_genie_space,
        'cleanRoom': delete_clean_room,
        'metastore': delete_metastore,
        # Model versions and constraints
        'modelVersion': delete_model_version,
        'ucModelVersion': delete_uc_model_version,
        'tableConstraint': delete_table_constraint,
        'registryWebhook': delete_registry_webhook,
        # Workspace objects (from changeWorkspaceAcl dynamic type detection)
        'notebook': None,  # Notebooks use REVERT_PERMISSION, not DELETE
        'folder': None,    # Folders use REVERT_PERMISSION, not DELETE
        'repo': None,      # Repos use REVERT_PERMISSION, not DELETE
        'directory': None, # Directories use REVERT_PERMISSION, not DELETE
        'query': delete_query,  # Queries can be deleted
        # Additional object types
        'abacPolicy': delete_abac_policy,
        'featureSpec': delete_feature_spec,
        # Lakebase (Database Instances)
        'databaseInstance': delete_database_instance,
        'databaseCatalog': delete_database_catalog,
        'databaseTable': delete_database_table,
    }
    return delete_functions.get(object_type)

def get_resource_definition(client, object_type: str, object_id: str) -> Optional[str]:
    """
    Get resource definition as JSON string for backup.
    Returns None if backup is not supported or fails.
    """
    try:
        definition = None
        
        if object_type in ['jobs', 'job']:
            job = client.jobs.get(job_id=int(object_id))
            if hasattr(job, 'settings'):
                definition = job.settings.as_dict()
            else:
                definition = job.as_dict()
                
        elif object_type in ['pipelines', 'pipeline']:
            pipeline = client.pipelines.get(pipeline_id=object_id)
            if hasattr(pipeline, 'spec'):
                definition = pipeline.spec.as_dict()
            else:
                definition = pipeline.as_dict()
                
        elif object_type == 'apps':
            app = client.apps.get(name=object_id)
            definition = app.as_dict()
             
        elif object_type == 'mlflowExperiments':
            exp = client.experiments.get_experiment(experiment_id=object_id)
            definition = exp.as_dict()

        elif object_type == 'monitors':
            # Use quality_monitors API (the correct SDK attribute name)
            if hasattr(client, 'quality_monitors'):
                monitor = client.quality_monitors.get(table_name=object_id)
                definition = monitor.as_dict()
            else:
                definition = {'table_name': object_id, 'note': 'Lakehouse Monitoring API not available for full backup'}
            
        elif object_type == 'cluster':
            cluster = client.clusters.get(cluster_id=object_id)
            definition = cluster.as_dict()
            
        elif object_type == 'alert':
            alert = client.alerts.get(id=object_id)
            definition = alert.as_dict()
            
        elif object_type == 'alertsv2':
            alert = client.alerts_v2.get_alert(id=object_id)
            definition = alert.as_dict()
            
        elif object_type == 'warehouse':
            warehouse = client.warehouses.get(id=object_id)
            definition = warehouse.as_dict()
            
        elif object_type == 'clusterPolicy':
            policy = client.cluster_policies.get(policy_id=object_id)
            definition = policy.as_dict()
            
        elif object_type == 'instancePool':
            pool = client.instance_pools.get(instance_pool_id=object_id)
            definition = pool.as_dict()
            
        elif object_type == 'servingEndpoint':
            endpoint = client.serving_endpoints.get(name=object_id)
            definition = endpoint.as_dict()
            
        elif object_type == 'secretScope':
            # Secret scopes don't have much config to backup
            definition = {'scope_name': object_id}
            
        elif object_type == 'vectorSearchEndpoint':
            endpoint = client.vector_search_endpoints.get_endpoint(endpoint_name=object_id)
            definition = endpoint.as_dict()
            
        elif object_type in ['registeredModel', 'ucRegisteredModel']:
            model = client.registered_models.get(full_name=object_id)
            definition = model.as_dict()
            
        elif object_type == 'dashboard':
            # Try Lakeview dashboard first, then legacy
            try:
                if hasattr(client, 'lakeview'):
                    dashboard = client.lakeview.get(dashboard_id=object_id)
                    definition = dashboard.as_dict()
                else:
                    dashboard = client.dashboards.get(dashboard_id=object_id)
                    definition = dashboard.as_dict()
            except Exception:
                # Fallback to legacy SQL dashboard API
                dashboard = client.dashboards.get(dashboard_id=object_id)
                definition = dashboard.as_dict()
        
        elif object_type == 'lakeview_dashboard':
            if hasattr(client, 'lakeview'):
                dashboard = client.lakeview.get(dashboard_id=object_id)
                definition = dashboard.as_dict()
            else:
                definition = {'dashboard_id': object_id, 'note': 'Lakeview API not available for full backup'}
            
        elif object_type == 'query':
            query = client.queries.get(id=object_id)
            definition = query.as_dict()
            
        elif object_type == 'catalog':
            catalog = client.catalogs.get(name=object_id)
            definition = catalog.as_dict()
            
        elif object_type == 'schema':
            schema = client.schemas.get(full_name=object_id)
            definition = schema.as_dict()
            
        elif object_type == 'table':
            table = client.tables.get(full_name=object_id)
            definition = table.as_dict()
            
        elif object_type == 'volume':
            volume = client.volumes.read(name=object_id)
            definition = volume.as_dict()
            
        elif object_type == 'connection':
            connection = client.connections.get(name=object_id)
            definition = connection.as_dict()
            
        elif object_type == 'function':
            function = client.functions.get(name=object_id)
            definition = function.as_dict()
            
        elif object_type == 'externalLocation':
            ext_loc = client.external_locations.get(name=object_id)
            definition = ext_loc.as_dict()
            
        elif object_type == 'storageCredential':
            cred = client.storage_credentials.get(name=object_id)
            definition = cred.as_dict()
            
        elif object_type == 'share':
            share = client.shares.get(name=object_id)
            definition = share.as_dict()
            
        elif object_type == 'recipient':
            recipient = client.recipients.get(name=object_id)
            definition = recipient.as_dict()
            
        elif object_type == 'provider':
            provider = client.providers.get(name=object_id)
            definition = provider.as_dict()
            
        elif object_type == 'cleanRoom':
            clean_room = client.clean_rooms.get(name=object_id)
            definition = clean_room.as_dict()
            
        elif object_type == 'metastore':
            metastore = client.metastores.get(id=object_id)
            definition = metastore.as_dict()
            
        elif object_type == 'featureTable':
            # Feature tables don't have a direct get API, minimal backup
            definition = {'feature_table_name': object_id}
            
        elif object_type == 'genieSpace':
            # Genie space backup - minimal info
            definition = {'space_id': object_id}
        
        elif object_type == 'vectorIndex':
            # Vector index backup - minimal info
            definition = {'index_name': object_id}
        
        elif object_type == 'abacPolicy':
            # ABAC policy backup - minimal info
            definition = {'policy_name': object_id}
        
        elif object_type == 'featureSpec':
            # Feature spec backup - minimal info
            definition = {'feature_spec_name': object_id}
        
        elif object_type in ['databaseInstance', 'databaseCatalog', 'databaseTable']:
            # Lakebase objects - minimal info
            definition = {'object_name': object_id, 'object_type': object_type}
        
        if definition:
            return json.dumps(definition)
        return None
        
    except Exception as e:
        print(f"  Warning: Failed to backup {object_type} {object_id}: {str(e)}")
        return None

# COMMAND ----------


def revert_permissions(client, workspace_id: str, object_id: str, object_type: str) -> Tuple[bool, Optional[str]]:
    """
    Revert object permissions to pre-approved state.
    
    Routes to the appropriate permission API based on object type:
    - Unity Catalog objects → grants API
    - Secret scopes → secrets ACL API
    - Workspace objects → permissions API
    
    Args:
        client: Databricks WorkspaceClient
        workspace_id: Workspace ID where the object resides
        object_id: Object identifier (ID or full name)
        object_type: Type of object
    
    Returns:
        Tuple of (success: bool, message: Optional[str])
    """
    # Object type categories
    UC_OBJECT_TYPES = {
        "catalog", "schema", "table", "volume", "function", "connection",
        "externalLocation", "storageCredential", "share", "recipient",
        "provider", "metastore", "ucRegisteredModel"
    }
    
    SECRET_SCOPE_TYPES = {"secretScope"}
    
    # Mapping from governance object types to permissions API types
    # Valid request_object_type values: alerts, alertsv2, authorization, clusters, 
    # cluster-policies, dashboards, dbsql-dashboards, directories, experiments, files, 
    # genie, instance-pools, jobs, notebooks, pipelines, queries, registered-models, 
    # repos, serving-endpoints, warehouses
    WORKSPACE_TYPE_MAP = {
        # Workspace objects (from acl_path_prefix patterns in 03a_load_filters.py)
        "notebook": "notebooks",
        "dashboard": "dbsql-dashboards",
        "lakeview_dashboard": "dashboards",
        "query": "queries",
        "folder": "directories",
        "directory": "directories",
        "file": "files",
        "repo": "repos",
        "project": "repos",
        "workspace_object": "directories",
        # Alerts - both legacy and v2
        "alert": "alerts",
        "alerts": "alerts",
        "alertsv2": "alertsv2",
        # Compute
        "cluster": "clusters",
        "clusterPolicy": "cluster-policies",
        "instancePool": "instance-pools",
        # Jobs & Pipelines
        "jobs": "jobs",
        "job": "jobs",
        "pipelines": "pipelines",
        "pipeline": "pipelines",
        # SQL
        "warehouse": "warehouses",
        # Apps & Serving
        "apps": "apps",
        "servingEndpoint": "serving-endpoints",
        # Models & ML
        "registeredModel": "registered-models",
        "vectorSearchEndpoint": "vector-search-endpoints",
        "mlflowExperiments": "experiments",
        # Other
        "genieSpace": "genie",
        "dataroom": "genie",
    }
    
    # Fetch pre-approved permissions from governance table
    print(f"  → Checking for pre-approved permissions in governance table")
    approved_perms = spark.sql(f"""
        SELECT permissions
        FROM {catalog}.{schema}.governance_preapproved_objects
        WHERE workspace_id = '{workspace_id}'
          AND object_id = '{object_id}'
          AND is_active = true
    """).collect()
    
    # Route to appropriate handler
    if object_type in UC_OBJECT_TYPES:
        return _revert_uc_permissions(client, object_id, object_type, approved_perms)
    elif object_type in SECRET_SCOPE_TYPES:
        return _revert_secret_scope_permissions(client, object_id, approved_perms)
    else:
        return _revert_workspace_permissions(client, object_id, object_type, approved_perms, WORKSPACE_TYPE_MAP)


def _revert_uc_permissions(client, object_id: str, object_type: str, approved_perms) -> Tuple[bool, Optional[str]]:
    """
    Revert Unity Catalog object permissions to pre-approved state using grants API.
    
    Reference: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/catalog/grants.html
    
    Args:
        client: Databricks WorkspaceClient
        object_id: Full name of the UC object (e.g., "catalog.schema.table")
        object_type: Type of UC object (catalog, schema, table, etc.)
        approved_perms: Pre-approved permissions from governance table
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    from databricks.sdk.service.catalog import PermissionsChange, Privilege
    
    # Map governance object types to UC securable type strings
    SECURABLE_TYPE_MAP = {
        "catalog": "catalog",
        "schema": "schema",
        "table": "table",
        "volume": "volume",
        "function": "function",
        "connection": "connection",
        "externalLocation": "external_location",
        "storageCredential": "storage_credential",
        "share": "share",
        "recipient": "recipient",
        "provider": "provider",
        "metastore": "metastore",
        "ucRegisteredModel": "function",
    }
    
    securable_type = SECURABLE_TYPE_MAP.get(object_type)
    if not securable_type:
        return (False, f"Unsupported UC object type: {object_type}")
    
    # Fetch current grants from Unity Catalog
    try:
        current_grants = client.grants.get(securable_type=securable_type, full_name=object_id)
    except Exception as e:
        return (False, f"Failed to get current UC grants: {e}")
    
    # Parse current grants into a structured format
    # {principal: {"privileges": set(str), "enums": {str: Privilege}}}
    current_state = {}
    
    if current_grants.privilege_assignments:
        for assignment in current_grants.privilege_assignments:
            principal = assignment.principal
            privileges = set()
            enum_map = {}
            
            for priv_enum in (assignment.privileges or []):
                priv_name = priv_enum.value
                privileges.add(priv_name)
                enum_map[priv_name] = priv_enum
            
            current_state[principal] = {"privileges": privileges, "enums": enum_map}
    
    print(f"  → Current grants: {len(current_state)} principals")
    for principal, data in current_state.items():
        print(f"      {principal}: {data['privileges']}")
    
    # Determine target state from approved permissions
    if approved_perms and approved_perms[0].permissions:
        # Use pre-approved permissions as target state
        target_state = {}
        for perm in approved_perms[0].permissions:
            principal = perm.principal_email
            privilege = perm.permission_level
            if principal and privilege:
                if principal not in target_state:
                    target_state[principal] = set()
                target_state[principal].add(privilege)
        
        print(f"  → Target state (pre-approved): {len(target_state)} principals")
        for principal, privs in target_state.items():
            print(f"      {principal}: {privs}")
    else:
        # No pre-approved permissions - target is empty (revoke all except OWNER)
        target_state = {}
        print(f"  → Target state: empty (revoke all grants except OWNER)")
    
    # Calculate changes needed
    revoked_count = 0
    added_count = 0
    
    # Process each principal in current state
    for principal, data in current_state.items():
        current_privs = data["privileges"]
        target_privs = target_state.get(principal, set())
        
        # Never revoke OWNER privilege - it's managed separately
        privs_to_revoke = current_privs - target_privs - {"OWNER"}
        
        if privs_to_revoke:
            try:
                enums_to_revoke = [data["enums"][p] for p in privs_to_revoke if p in data["enums"]]
                if enums_to_revoke:
                    client.grants.update(
                        securable_type=securable_type,
                        full_name=object_id,
                        changes=[PermissionsChange(remove=enums_to_revoke, principal=principal)]
                    )
                    revoked_count += len(enums_to_revoke)
                    print(f"    - Revoked from {principal}: {privs_to_revoke}")
            except Exception as e:
                print(f"    - Warning: Failed to revoke from {principal}: {e}")
    
    # Process each principal in target state (for additions)
    for principal, target_privs in target_state.items():
        current_privs = current_state.get(principal, {}).get("privileges", set())
        
        # Don't try to grant OWNER or ALL_PRIVILEGES - they require special handling
        privs_to_add = target_privs - current_privs - {"OWNER", "ALL_PRIVILEGES"}
        
        if privs_to_add:
            try:
                enums_to_add = []
                for p in privs_to_add:
                    try:
                        enums_to_add.append(Privilege(p))
                    except ValueError:
                        print(f"    - Warning: Unknown privilege '{p}', skipping")
                
                if enums_to_add:
                    client.grants.update(
                        securable_type=securable_type,
                        full_name=object_id,
                        changes=[PermissionsChange(add=enums_to_add, principal=principal)]
                    )
                    added_count += len(enums_to_add)
                    print(f"    - Added to {principal}: {privs_to_add}")
            except Exception as e:
                print(f"    - Warning: Failed to add grants to {principal}: {e}")
    
    return (True, f"Synced UC grants: revoked {revoked_count}, added {added_count} privileges")

def _revert_secret_scope_permissions(client, scope_name: str, approved_perms) -> Tuple[bool, Optional[str]]:
    """
    Revert secret scope ACL permissions to pre-approved state using secrets API.
    
    Args:
        client: Databricks WorkspaceClient
        scope_name: Name of the secret scope
        approved_perms: Pre-approved permissions from governance table
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    from databricks.sdk.service.workspace import AclPermission
    
    # Fetch current ACLs from the secret scope
    try:
        current_acls = list(client.secrets.list_acls(scope=scope_name))
    except Exception as e:
        return (False, f"Failed to get current secret scope ACLs: {e}")
    
    # Parse current ACLs: {principal: permission_string}
    current_state = {}
    for acl in current_acls:
        principal = acl.principal
        permission = acl.permission.value
        current_state[principal] = permission
    
    print(f"  → Current ACLs: {len(current_state)} principals")
    for principal, perm in current_state.items():
        print(f"      {principal}: {perm}")
    
    # Determine target state from approved permissions
    if approved_perms and approved_perms[0].permissions:
        target_state = {}
        for perm in approved_perms[0].permissions:
            principal = perm.principal_email
            permission = perm.permission_level
            if principal and permission:
                target_state[principal] = permission
        
        print(f"  → Target state (pre-approved): {len(target_state)} principals")
        for principal, perm in target_state.items():
            print(f"      {principal}: {perm}")
    else:
        target_state = {}
        print(f"  → Target state: empty (revoke all ACLs)")
    
    # Sync to target state
    updated_count = 0
    added_count = 0
    removed_count = 0
    
    # Add or update ACLs to match target
    for principal, target_perm in target_state.items():
        current_perm = current_state.get(principal)
        
        if current_perm != target_perm:
            try:
                client.secrets.put_acl(
                    scope=scope_name,
                    principal=principal,
                    permission=AclPermission(target_perm)
                )
                if current_perm:
                    updated_count += 1
                    print(f"    - Updated {principal}: {current_perm} → {target_perm}")
                else:
                    added_count += 1
                    print(f"    - Added {principal}: {target_perm}")
            except Exception as e:
                print(f"    - Warning: Failed to set ACL for {principal}: {e}")
    
    # Remove ACLs not in target
    for principal in current_state:
        if principal not in target_state:
            try:
                client.secrets.delete_acl(scope=scope_name, principal=principal)
                removed_count += 1
                print(f"    - Removed {principal}")
            except Exception as e:
                print(f"    - Warning: Failed to remove {principal}: {e}")
    
    return (True, f"Synced secret scope ACLs: updated {updated_count}, added {added_count}, removed {removed_count}")


def _revert_workspace_permissions(client, object_id: str, object_type: str, approved_perms, type_mapping: dict) -> Tuple[bool, Optional[str]]:
    """
    Revert workspace object permissions to pre-approved state using permissions API.
    
    Reference: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/iam/permissions.html
    
    Args:
        client: Databricks WorkspaceClient
        object_id: Object ID (numeric ID or path)
        object_type: Type of workspace object (notebook, directory, job, etc.)
        approved_perms: Pre-approved permissions from governance table
        type_mapping: Mapping from object_type to permissions API type
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    
    # Map object type to permissions API type
    permissions_api_type = type_mapping.get(object_type)
    if not permissions_api_type:
        return (False, f"Unsupported object type: {object_type}")
    
    # Determine target ACLs
    if approved_perms and approved_perms[0].permissions:
        # Build ACL list from pre-approved permissions
        acl_list = []
        
        print(f"  → Building target ACLs from pre-approved permissions")
        for perm in approved_perms[0].permissions:
            principal = perm.principal_email
            level = perm.permission_level
            ptype = getattr(perm, 'principal_type', None) or _detect_principal_type(principal)
            
            if not principal or not level:
                continue
            
            # Build AccessControlRequest based on principal type
            acl = _build_access_control_request(principal, level, ptype)
            if acl:
                acl_list.append(acl)
                print(f"      {principal} ({ptype}): {level}")
        
        print(f"  → Setting {len(acl_list)} ACLs")
    else:
        # No approved permissions - clear all explicit permissions
        acl_list = []
        print(f"  → No pre-approved permissions found")
        print(f"  → Clearing all explicit permissions")
    
    # Apply permissions
    client.permissions.set(
        request_object_type=permissions_api_type,
        request_object_id=object_id,
        access_control_list=acl_list
    )
    
    return (True, f"Set {len(acl_list)} ACLs on {permissions_api_type}/{object_id}")


def _build_access_control_request(principal: str, permission_level: str, principal_type: str):
    """
    Build an AccessControlRequest for the given principal.
    
    Args:
        principal: Principal identifier (email, group name, or service principal ID)
        permission_level: Permission level string (CAN_VIEW, CAN_RUN, CAN_MANAGE, etc.)
        principal_type: Type of principal (user, group, service_principal)
    
    Returns:
        AccessControlRequest or None if invalid
    """
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    
    try:
        level = PermissionLevel(permission_level)
    except ValueError:
        print(f"    - Warning: Unknown permission level '{permission_level}', skipping")
        return None
    
    if principal_type == 'user':
        return AccessControlRequest(user_name=principal, permission_level=level)
    elif principal_type == 'service_principal':
        return AccessControlRequest(service_principal_name=principal, permission_level=level)
    else:  # group or unknown
        return AccessControlRequest(group_name=principal, permission_level=level)

# COMMAND ----------

def execute_remediation(client, violation: dict, dry_run: bool = False) -> Tuple[str, str, Optional[str], Optional[str]]:
    """Execute remediation action for a violation."""
    remediation_action = violation['remediation_action']
    object_type = violation['object_type']
    object_id = violation['object_id']
    object_name = violation['object_name']
    workspace_id = violation['workspace_id']
    
    backup_definition = None
    
    if dry_run:
        print(f"DRY RUN: Would execute {remediation_action} for {object_type}:{object_id}")
        # Attempt backup even in dry run to verify it works, but don't fail if it doesn't? 
        # Or just skip for dry run speed. Let's skip.
        return ('SUCCESS', f'DRY RUN: {remediation_action}', None, None)
    
    success = False
    error = None
    details = None
    
    try:
        if remediation_action == 'DELETE_RESOURCE':
            # Step 1: Backup the resource definition
            print(f"  → Attempting to backup definition for {object_type}:{object_id}")
            backup_definition = get_resource_definition(client, object_type, object_id)
            
            if not backup_definition:
                # Backup failed or not supported. 
                # Requirement: "Only after backup is successful then delete the resource."
                # We must fail the remediation if backup fails.
                error = f"Backup failed for {object_type}:{object_id}. Aborting deletion."
                print(f"✗ {error}")
                return ('FAILED', f"Aborted: {error}", error, None)
            
            print(f"  ✓ Backup successful ({len(backup_definition)} chars)")
            
            # Step 2: Get the appropriate delete function and execute
            delete_func = get_delete_function(object_type)
            
            if delete_func:
                success, error = delete_func(client, object_id)
            else:
                success, error = False, f"Object type '{object_type}' is not supported for deletion"
            
            if success:
                details = f"Deleted {object_type}: {object_name} (ID: {object_id})"
                print(f"✓ {details}")
            else:
                details = f"Failed to delete {object_type}: {object_name} (ID: {object_id})"
                print(f"✗ {details} - {error}")
        
        elif remediation_action == 'REVERT_PERMISSION':
            # Revert permissions: reset to pre-approved state if found in governance table,
            # otherwise remove all explicit permissions
            print(f"Reverting permissions for {object_type}:{object_name} (ID: {object_id})")
            success, error = revert_permissions(
                client, 
                workspace_id, 
                object_id,
                object_type
            )
            
            if success:
                details = f"Reverted permissions for {object_type}: {object_name}"
                print(f"✓ {details}")
            else:
                details = f"Failed to revert permissions for {object_type}: {object_name}"
                print(f"✗ {details} - {error}")
        
        elif remediation_action == 'ALERT_ENTITLEMENT_CHANGE':
            # Alert entitlement change to security team - no automated action needed
            # This is logged in control_actions table for security team review
            print(f"📋 Logging entitlement change event for security team review:")
            print(f"   Object Type: {object_type}")
            print(f"   Object ID: {object_id}")
            print(f"   Object Name: {object_name}")
            print(f"   Changed By: {violation.get('user_email', 'Unknown')}")
            print(f"   Action: {violation.get('action_name', 'Unknown')}")
            
            success = True
            error = None
            details = f"Entitlement change reported for security team review: {object_type} '{object_name}' (ID: {object_id}) changed by {violation.get('user_email', 'Unknown')}"
            print(f"✓ {details}")
        
        elif remediation_action == 'REPORT_DELETION' or remediation_action == 'REPORT_SECURITY_TEAM':
            # Report deletion to security team - no automated action needed
            # This is logged in control_actions table for security team review
            print(f"📋 Logging deletion event for security team review:")
            print(f"   Object Type: {object_type}")
            print(f"   Object ID: {object_id}")
            print(f"   Object Name: {object_name}")
            print(f"   Deleted By: {violation.get('user_email', 'Unknown')}")
            print(f"   Action: {violation.get('action_name', 'Unknown')}")
            
            success = True
            error = None
            details = f"Deletion reported for security team review: {object_type} '{object_name}' (ID: {object_id}) deleted by {violation.get('user_email', 'Unknown')}"
            print(f"✓ {details}")
        
        else:
            success, error = False, f"Unknown remediation action: {remediation_action}"
            details = error
        
        status = 'SUCCESS' if success else 'FAILED'
        return (status, details, error, backup_definition)
        
    except Exception as e:
        error = str(e)
        details = f"Exception during remediation: {error}"
        print(f"✗ {details}")
        return ('FAILED', details, error, backup_definition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Violations

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
import uuid

# Get workspace configurations
workspace_configs = {}
ws_config_df = spark.sql(f"""
    SELECT 
        workspace_id, 
        workspace_url,
        max_retry_attempts
    FROM {catalog}.{schema}.governance_config_workspaces
    WHERE enforcement_enabled = true
""")

for row in ws_config_df.collect():
    workspace_configs[row.workspace_id] = row.asDict()

print(f"Loaded {len(workspace_configs)} workspace configuration(s)")

# Get unique workspace IDs from violations and create clients upfront
violation_workspace_ids = set(row.workspace_id for row in pending_violations_df.select("workspace_id").distinct().collect())
print(f"Violations span {len(violation_workspace_ids)} workspace(s): {violation_workspace_ids}")

# Create workspace clients for each workspace with violations
workspace_clients = {}
for ws_id in violation_workspace_ids:
    config = workspace_configs.get(ws_id, {})
    workspace_url = config.get('workspace_url')
    
    # Check if we have Key Vault scope configured and workspace URL
    if kv_scope and workspace_url:
        try:
            workspace_clients[ws_id] = create_workspace_client(workspace_url)
            print(f"  ✓ Created client for workspace {ws_id} ({workspace_url})")
        except Exception as e:
            print(f"  ✗ Failed to create client for {ws_id}: {e}, using default")
            workspace_clients[ws_id] = WorkspaceClient()
    else:
        print(f"  → Using default client for workspace {ws_id}")
        workspace_clients[ws_id] = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Violations

# COMMAND ----------

# Process each violation
violations_list = pending_violations_df.collect()
control_actions = []
processed_violations = []

print(f"\nProcessing {len(violations_list)} violations...")
print("="*80)

for violation_row in violations_list:
    violation = violation_row.asDict()
    workspace_id = violation['workspace_id']
    retry_count = violation['retry_count']
    created_at = violation['created_at']
    
    # Get the pre-created client for this workspace
    client = workspace_clients.get(workspace_id, WorkspaceClient())
    
    print(f"\nProcessing: {violation['object_type']}:{violation['object_id']} (workspace: {workspace_id})")
    
    # Execute remediation
    status, details, error, backup_definition = execute_remediation(client, violation, dry_run)
    
    # Get max retries from config
    max_retries = workspace_configs.get(workspace_id, {}).get('max_retry_attempts', 3)
    
    # Create control action record
    control_action = {
        'action_id': str(uuid.uuid4()),
        'violation_id': violation['violation_id'],
        'workspace_id': workspace_id,
        'action_type': violation['remediation_action'],
        'object_id': violation['object_id'],
        'object_type': violation['object_type'],
        'object_name': violation['object_name'],
        'violator_email': violation['user_email'],
        'remediation_status': status,
        'remediation_details': details,
        'backup_definition': backup_definition,
        'error_message': error,
        'retry_count': retry_count,
        'max_retries': max_retries,
        'last_retry_at': datetime.now(tz) if status == 'FAILED' else None,
        'completed_at': datetime.now(tz) if status == 'SUCCESS' else None,
        'notification_sent': False,
        'created_at': created_at,
        'updated_at': datetime.now(tz)
    }
    
    control_actions.append(control_action)
    processed_violations.append(violation['violation_id'])

print("="*80)
print(f"Processed {len(control_actions)} remediation actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

if not dry_run:
    # Write control actions
    if control_actions:
        # Define schema for control actions
        control_actions_schema = StructType([
            StructField('action_id', StringType(), True),
            StructField('violation_id', StringType(), True),
            StructField('workspace_id', StringType(), True),
            StructField('action_type', StringType(), True),
            StructField('object_id', StringType(), True),
            StructField('object_type', StringType(), True),
            StructField('object_name', StringType(), True),
            StructField('violator_email', StringType(), True),
            StructField('remediation_status', StringType(), True),
            StructField('remediation_details', StringType(), True),
            StructField('backup_definition', StringType(), True),
            StructField('error_message', StringType(), True),
            StructField('retry_count', IntegerType(), True),
            StructField('max_retries', IntegerType(), True),
            StructField('last_retry_at', TimestampType(), True),
            StructField('completed_at', TimestampType(), True),
            StructField('notification_sent', BooleanType(), True),
            StructField('created_at', TimestampType(), True),
            StructField('updated_at', TimestampType(), True)
        ])
        
        # Create DataFrame with explicit schema
        control_actions_df = spark.createDataFrame(control_actions, schema=control_actions_schema)
        DeltaTable.forName(spark, control_actions_table).alias("target").merge(
            control_actions_df.alias("source"),
            "target.violation_id = source.violation_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✓ Written {len(control_actions)} control actions to {control_actions_table}")
    
    # Update staging table
    if processed_violations:
        violation_ids_str = "', '".join(processed_violations)
        spark.sql(f"""
            UPDATE {staging_table}
            SET processing_status = 'COMPLETED',
                processed_at = current_timestamp()
            WHERE violation_id IN ('{violation_ids_str}')
        """)
        print(f"✓ Updated {len(processed_violations)} violations to COMPLETED")
else:
    print("DRY RUN: No changes written to database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

success_count = sum(1 for action in control_actions if action['remediation_status'] == 'SUCCESS')
failed_count = sum(1 for action in control_actions if action['remediation_status'] == 'FAILED')
deletion_reports = sum(1 for action in control_actions if action['action_type'] == 'REPORT_DELETION')

print("\n" + "="*80)
print("REMEDIATION SUMMARY")
print("="*80)
print(f"Total Violations Processed:  {len(control_actions)}")
print(f"Successful Remediations:     {success_count}")
print(f"Failed Remediations:         {failed_count}")
print(f"Deletion Reports (for security team): {deletion_reports}")
print("="*80)

# Show breakdown by action type and status
if control_actions:
    import pandas as pd
    
    summary_data = {}
    for action in control_actions:
        key = (action['action_type'], action['remediation_status'])
        summary_data[key] = summary_data.get(key, 0) + 1
    
    print("\nBreakdown by Action Type:")
    for (action_type, status), count in sorted(summary_data.items()):
        print(f"  {action_type:25s} {status:10s}: {count}")

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'remediated_count': len(control_actions),
    'success_count': success_count,
    'failed_count': failed_count,
    'timestamp': datetime.now(tz).isoformat()
}, indent=3))