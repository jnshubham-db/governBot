# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Remediation
# MAGIC
# MAGIC This notebook reads violations from the staging table and executes remediation actions:
# MAGIC - **DELETE_RESOURCE**: Delete unauthorized resources (jobs, pipelines, apps, etc.)
# MAGIC - **REVERT_PERMISSION**: Revert unauthorized permission changes
# MAGIC - **REPORT_DELETION**: Log deletion events for security team review (no automated action)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run Mode")
# Azure Key Vault parameters for cross-workspace authentication
dbutils.widgets.text("kv_scope", "", "Key Vault Scope Name")
dbutils.widgets.text("kv_client_id_key", "AzureClientId", "Key Vault Key for Client ID")
dbutils.widgets.text("kv_client_secret_key", "AzureClientSecret", "Key Vault Key for Client Secret")
dbutils.widgets.text("kv_tenant_id_key", "TenantId", "Key Vault Key for Tenant ID")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

# Azure Key Vault configuration
kv_scope = dbutils.widgets.get("kv_scope")
kv_client_id_key = dbutils.widgets.get("kv_client_id_key")
kv_client_secret_key = dbutils.widgets.get("kv_client_secret_key")
kv_tenant_id_key = dbutils.widgets.get("kv_tenant_id_key")

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
    # Get credentials from Key Vault
    azure_client_id = dbutils.secrets.get(scope=kv_scope, key=kv_client_id_key)
    client_secret = dbutils.secrets.get(scope=kv_scope, key=kv_client_secret_key)
    tenant_id = dbutils.secrets.get(scope=kv_scope, key=kv_tenant_id_key)
    if kv_scope:
        return WorkspaceClient(
            host=workspace_url,
            azure_client_id=azure_client_id,
            azure_client_secret=client_secret,
            azure_tenant_id=tenant_id,
            auth_type="azure-client-secret"
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

pending_violations_df = spark.sql(f"""
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
        violation_type,
        violation_reason,
        remediation_action
    FROM {staging_table}
    WHERE processing_status IN ('PENDING', 'PENDING_REPORT')
    ORDER BY event_time ASC
""")

pending_count = pending_violations_df.count()
print(f"Found {pending_count} pending violations to remediate")

if pending_count == 0:
    print("No pending violations. Exiting.")
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
            alert = client.alerts.get(alert_id=object_id)
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
    Revert object permissions:
    1. Check if object has pre-approved permissions in governance table
    2. If found: Reset to pre-approved permissions
    3. If not found: Remove all explicit permissions (keep only inherited)
    
    Supports both workspace-level permissions API and Unity Catalog grants API.
    
    Args:
        client: WorkspaceClient instance
        workspace_id: Workspace ID
        object_id: Object ID
        object_type: Object type
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        # Unity Catalog object types that use grants API instead of permissions API
        uc_object_types = {
            "catalog", "schema", "table", "volume", "function", "connection",
            "externalLocation", "storageCredential", "share", "recipient", 
            "provider", "metastore", "ucRegisteredModel"
        }
        
        # Map object type to permissions API object type (for workspace objects)
        # Supported types: alerts, alertsv2, apps, authorization, clusters, cluster-policies,
        # dashboards, database-instances, database-projects, dbsql-dashboards, directories,
        # experiments, files, genie, instance-pools, jobs, notebooks, pipelines, queries,
        # registered-models, repos, serving-endpoints, warehouses, vector-search-endpoints
        workspace_type_mapping = {
            # Workspace objects
            "notebook": "notebooks",
            "dashboard": "dbsql-dashboards",  # Legacy SQL dashboards
            "lakeview_dashboard": "dashboards",  # Lakeview (AI/BI) dashboards
            "query": "queries",
            "folder": "directories",
            "directory": "directories",
            "file": "files",
            "repo": "repos",
            "workspace_object": "directories",  # Generic workspace object
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
            "warehouse": "warehouses",  # Fixed: was sql/warehouses
            "alert": "alerts",  # Fixed: was sql/alerts
            # Apps & Serving
            "apps": "apps",
            "servingEndpoint": "serving-endpoints",
            # Model Registry (workspace-level)
            "registeredModel": "registered-models",
            # Vector Search
            "vectorSearchEndpoint": "vector-search-endpoints",
            # MLflow
            "mlflowExperiments": "experiments",
            # Genie
            "genieSpace": "genie",
        }
        
        # Check if this is a Unity Catalog object
        is_uc_object = object_type in uc_object_types
        
        # Get approved permissions from preapproved objects table
        print(f"  → Checking for pre-approved permissions in governance table")
        approved_perms = spark.sql(f"""
            SELECT permissions
            FROM {catalog}.{schema}.governance_preapproved_objects
            WHERE workspace_id = '{workspace_id}'
            AND object_id = '{object_id}'
            AND is_active = true
        """).collect()
        
        if is_uc_object:
            # Handle Unity Catalog objects using grants API
            return _revert_uc_permissions(client, object_id, object_type, approved_perms)
        else:
            # Handle workspace objects using permissions API
            return _revert_workspace_permissions(client, object_id, object_type, approved_perms, workspace_type_mapping)
        
    except Exception as e:
        return (False, str(e))


def _revert_uc_permissions(client, object_id: str, object_type: str, approved_perms) -> Tuple[bool, Optional[str]]:
    """
    Revert Unity Catalog object permissions using grants API.
    
    UC objects use a different permission model based on grants (OWNER, ALL PRIVILEGES, 
    USE CATALOG, USE SCHEMA, SELECT, MODIFY, etc.)
    
    This function:
    1. Gets current grants from UC
    2. Compares with pre-approved grants from governance table
    3. Revokes grants not in approved list
    4. Adds grants that are in approved but not current
    """
    try:
        from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege
        
        # Map object types to securable types
        securable_type_mapping = {
            "catalog": SecurableType.CATALOG,
            "schema": SecurableType.SCHEMA,
            "table": SecurableType.TABLE,
            "volume": SecurableType.VOLUME,
            "function": SecurableType.FUNCTION,
            "connection": SecurableType.CONNECTION,
            "externalLocation": SecurableType.EXTERNAL_LOCATION,
            "storageCredential": SecurableType.STORAGE_CREDENTIAL,
            "share": SecurableType.SHARE,
            "recipient": SecurableType.RECIPIENT,
            "provider": SecurableType.PROVIDER,
            "metastore": SecurableType.METASTORE,
            "ucRegisteredModel": SecurableType.FUNCTION,  # UC models use FUNCTION securable type
        }
        
        securable_type = securable_type_mapping.get(object_type)
        if not securable_type:
            return (False, f"Unsupported UC object type for permission revert: {object_type}")
        
        # Get current grants from Unity Catalog
        try:
            current_grants = client.grants.get(securable_type=securable_type, full_name=object_id)
        except Exception as get_error:
            return (False, f"Failed to get current UC grants: {str(get_error)}")
        
        # Build a map of current grants: {principal: set(privileges)}
        current_grants_map = {}
        owner_principal = None
        
        if current_grants.privilege_assignments:
            for assignment in current_grants.privilege_assignments:
                principal = assignment.principal
                privileges = set()
                
                if assignment.privileges:
                    for priv in assignment.privileges:
                        priv_name = priv.privilege.value if hasattr(priv.privilege, 'value') else str(priv.privilege)
                        privileges.add(priv_name)
                        
                        # Track the owner
                        if priv_name == 'OWNER' or priv_name == 'ALL_PRIVILEGES':
                            owner_principal = principal
                
                current_grants_map[principal] = privileges
        
        print(f"  → Current grants: {len(current_grants_map)} principals")
        
        if not approved_perms or not approved_perms[0].permissions:
            # No approved permissions - revoke all grants except owner
            print(f"  → No pre-approved permissions found for UC object")
            print(f"  → Revoking all explicit grants (keeping owner)")
            
            revoked_count = 0
            for principal, privileges in current_grants_map.items():
                # Skip owner - can't revoke ownership
                if 'OWNER' in privileges or principal == owner_principal:
                    print(f"    - Skipping owner: {principal}")
                    continue
                
                # Revoke all privileges for this principal
                privileges_to_revoke = [p for p in privileges if p != 'OWNER']
                if privileges_to_revoke:
                    try:
                        changes = [PermissionsChange(
                            remove=[Privilege[p] for p in privileges_to_revoke],
                            principal=principal
                        )]
                        client.grants.update(
                            securable_type=securable_type,
                            full_name=object_id,
                            changes=changes
                        )
                        revoked_count += 1
                        print(f"    - Revoked {len(privileges_to_revoke)} privileges from: {principal}")
                    except Exception as revoke_error:
                        print(f"    - Warning: Failed to revoke from {principal}: {str(revoke_error)}")
            
            return (True, f"Revoked grants from {revoked_count} principals (owner retained)")
        
        # Pre-approved permissions found - sync to approved state
        print(f"  → Pre-approved permissions found for UC object")
        print(f"  → Syncing to pre-approved grants state")
        
        # Build a map of approved grants: {principal: set(privileges)}
        approved_grants_map = {}
        permissions_list = approved_perms[0].permissions
        
        for perm in permissions_list:
            principal = perm['principal_email']
            privilege = perm['permission_level']
            
            if principal not in approved_grants_map:
                approved_grants_map[principal] = set()
            approved_grants_map[principal].add(privilege)
        
        print(f"  → Approved grants: {len(approved_grants_map)} principals")
        
        # Step 1: Revoke grants that are in current but not in approved
        revoked_count = 0
        for principal, current_privs in current_grants_map.items():
            # Skip owner
            if 'OWNER' in current_privs:
                continue
            
            approved_privs = approved_grants_map.get(principal, set())
            
            # Find privileges to revoke (in current but not in approved)
            privs_to_revoke = current_privs - approved_privs - {'OWNER'}
            
            if privs_to_revoke:
                try:
                    changes = [PermissionsChange(
                        remove=[Privilege[p] for p in privs_to_revoke],
                        principal=principal
                    )]
                    client.grants.update(
                        securable_type=securable_type,
                        full_name=object_id,
                        changes=changes
                    )
                    revoked_count += len(privs_to_revoke)
                    print(f"    - Revoked {privs_to_revoke} from: {principal}")
                except Exception as revoke_error:
                    print(f"    - Warning: Failed to revoke from {principal}: {str(revoke_error)}")
        
        # Step 2: Add grants that are in approved but not in current
        added_count = 0
        for principal, approved_privs in approved_grants_map.items():
            current_privs = current_grants_map.get(principal, set())
            
            # Find privileges to add (in approved but not in current)
            # Skip OWNER as it can't be granted this way
            privs_to_add = approved_privs - current_privs - {'OWNER', 'ALL_PRIVILEGES'}
            
            if privs_to_add:
                try:
                    changes = [PermissionsChange(
                        add=[Privilege[p] for p in privs_to_add],
                        principal=principal
                    )]
                    client.grants.update(
                        securable_type=securable_type,
                        full_name=object_id,
                        changes=changes
                    )
                    added_count += len(privs_to_add)
                    print(f"    - Added {privs_to_add} to: {principal}")
                except Exception as add_error:
                    print(f"    - Warning: Failed to add grants to {principal}: {str(add_error)}")
        
        # Step 3: Revoke all grants from principals not in approved list (except owner)
        removed_principals = 0
        for principal in current_grants_map.keys():
            if principal not in approved_grants_map and principal != owner_principal:
                current_privs = current_grants_map[principal]
                privs_to_revoke = current_privs - {'OWNER'}
                
                if privs_to_revoke:
                    try:
                        changes = [PermissionsChange(
                            remove=[Privilege[p] for p in privs_to_revoke],
                            principal=principal
                        )]
                        client.grants.update(
                            securable_type=securable_type,
                            full_name=object_id,
                            changes=changes
                        )
                        removed_principals += 1
                        print(f"    - Removed unapproved principal: {principal}")
                    except Exception as remove_error:
                        print(f"    - Warning: Failed to remove {principal}: {str(remove_error)}")
        
        summary = f"Synced UC grants: revoked {revoked_count} privileges, added {added_count} privileges, removed {removed_principals} unapproved principals"
        return (True, summary)
        
    except ImportError:
        return (False, "Unity Catalog SDK components not available")
    except Exception as e:
        return (False, f"UC permission revert failed: {str(e)}")


def _revert_workspace_permissions(client, object_id: str, object_type: str, approved_perms, type_mapping: dict) -> Tuple[bool, Optional[str]]:
    """
    Revert workspace object permissions using permissions API.
    
    Handles different principal types (users, groups, service principals) correctly.
    """
    try:
        permissions_object_type = type_mapping.get(object_type)
        if not permissions_object_type:
            return (False, f"Unsupported object type for permission revert: {object_type}")
        
        # Special handling for Lakeview dashboards - they may not support permissions API
        if object_type == 'lakeview_dashboard':
            print(f"  → Lakeview dashboards use workspace object permissions")
            print(f"  → Attempting to revert permissions via workspace path")
            # Lakeview dashboards permissions are managed differently
            # They typically inherit from the workspace folder they're in
            return (False, "Lakeview dashboards don't support direct permission revert. Permissions are inherited from workspace folder.")
        
        if not approved_perms or not approved_perms[0].permissions:
            # No approved permissions found - remove all explicit permissions
            print(f"  → No pre-approved permissions found")
            print(f"  → Removing all explicit permissions (keeping only inherited)")
            
            client.permissions.set(
                request_object_type=permissions_object_type,
                request_object_id=object_id,
                access_control_list=[]
            )
            return (True, "No pre-approved permissions found, removed all explicit permissions")
        
        # Pre-approved permissions found - reset to approved state
        print(f"  → Pre-approved permissions found")
        print(f"  → Resetting to pre-approved state")
        
        # Extract permissions
        permissions_list = approved_perms[0].permissions
        
        # Convert to SDK format
        from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
        
        acl_requests = []
        for perm in permissions_list:
            principal_email = perm['principal_email']
            permission_level = perm['permission_level']
            # Use stored principal_type if available, otherwise detect from naming conventions
            principal_type = perm.get('principal_type', None)
            
            request_kwargs = {
                'permission_level': PermissionLevel(permission_level)
            }
            
            if principal_type:
                # Use the stored principal type for accurate permission setting
                if principal_type == 'user':
                    request_kwargs['user_name'] = principal_email
                elif principal_type == 'service_principal':
                    request_kwargs['service_principal_name'] = principal_email
                elif principal_type == 'group':
                    request_kwargs['group_name'] = principal_email
                else:
                    # Unknown type, fall back to detection
                    principal_type = _detect_principal_type(principal_email)
                    if principal_type == 'user':
                        request_kwargs['user_name'] = principal_email
                    elif principal_type == 'service_principal':
                        request_kwargs['service_principal_name'] = principal_email
                    else:
                        request_kwargs['group_name'] = principal_email
            else:
                # Fallback: Detect principal type based on naming conventions
                detected_type = _detect_principal_type(principal_email)
                if detected_type == 'user':
                    request_kwargs['user_name'] = principal_email
                elif detected_type == 'service_principal':
                    request_kwargs['service_principal_name'] = principal_email
                else:
                    request_kwargs['group_name'] = principal_email
            
            request = AccessControlRequest(**request_kwargs)
            acl_requests.append(request)
        
        # Set permissions to pre-approved state
        client.permissions.set(
            request_object_type=permissions_object_type,
            request_object_id=object_id,
            access_control_list=acl_requests
        )
        
        return (True, f"Reset permissions to pre-approved state ({len(acl_requests)} ACLs)")
        
    except Exception as e:
        return (False, str(e))

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
        'retry_count': 0,
        'max_retries': max_retries,
        'last_retry_at': None,
        'completed_at': datetime.utcnow() if status == 'SUCCESS' else None,
        'notification_sent': False,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
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
        control_actions_table = f"{catalog}.{schema}.governance_control_actions"
        control_actions_df.write.mode("append").saveAsTable(control_actions_table)
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
    'timestamp': datetime.utcnow().isoformat()
}))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sjdatabricks.governance.governance_control_actions

# COMMAND ----------


