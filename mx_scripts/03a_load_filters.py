# Databricks notebook source
# MAGIC %md
# MAGIC # Load Governance Filters
# MAGIC
# MAGIC This notebook loads filter configurations into the governance_filters table.
# MAGIC These filters define how to extract object information from audit logs and what remediation actions to take.
# MAGIC
# MAGIC **Violation Types:**
# MAGIC - UNAPPROVED_CREATION: Unauthorized resource creation
# MAGIC - UNAUTHORIZED_PERMISSION_CHANGE: Unauthorized ACL changes
# MAGIC - UNAUTHORIZED_DELETION: Unauthorized resource deletion
# MAGIC - UNAUTHORIZED_ENTITLEMENT_CHANGE: Unauthorized entitlement changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

#dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
#dbutils.widgets.text("schema", "sch_mng_admon", "Schema Name")
#dbutils.widgets.dropdown("replace_existing", "true", ["true", "false"], "Replace Existing Filters")

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
from datetime import datetime
import json

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

try:
    replace_existing = dbutils.widgets.get("replace_existing").lower() == "true"
except Exception as e:
    replace_existing = False

try:
    load_filters = True if dbutils.widgets.get("load_filters") == "Y" or dbutils.widgets.get("load_filters") == "S" else False
except Exception as e:
    load_filters = False

if load_filters:
    replace_existing = True

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Replace Existing: {replace_existing}")

# COMMAND ----------

if load_filters == False:
    dbutils.notebook.exit(json.dumps({
    'status': 'SKIPPED',
    'reason': "Proceso no abanderado para realizar el discovery de objetos o actualizar los filtros",
    'load filters': load_filters,
    'timestamp': datetime.utcnow().isoformat()
}, indent=3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Filter Configurations
# MAGIC
# MAGIC ### Filter Structure:
# MAGIC - **filter_name**: Unique identifier for the filter
# MAGIC - **service_name**: Audit log service name
# MAGIC - **action_name**: Audit log action name
# MAGIC - **object_type**: Normalized object type for governance
# MAGIC - **object_id_expr**: SQL expression to extract object ID from audit log
# MAGIC - **object_name_expr**: SQL expression to extract object name from audit log
# MAGIC - **remediation_action**: Action to take (DELETE_RESOURCE, REVERT_PERMISSION, REPORT_DELETION)
# MAGIC - **extra_columns**: Additional columns needed for remediation (e.g., catalog, schema for tables)
# MAGIC - **is_active**: Whether the filter is active (True/False)
# MAGIC - **description**: Human-readable description

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Event Filters
# MAGIC
# MAGIC These filters detect unauthorized resource creation events.

# COMMAND ----------

# Create event filters - detect unauthorized resource creation
# Format: [filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, remediation_action, extra_columns, is_active, description]

create_filters_raw = [
    # Dashboards
    ["dashboards_create", "dashboards", "createDashboard", "dashboard", "request_params.dashboard_id", "request_params.dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard creation"],
    ["dashboards_clone", "dashboards", "cloneDashboard", "dashboard", "request_params.new_dashboard_id", "request_params.new_dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard clone"],
    
    # Genie Space
    ["genie_space_create", "aibiGenie", "createSpace", "genieSpace", 'get_json_object(response.result, "$.space_id")', 'get_json_object(response.result, "$.space_id")', "SKIP_REMEDIATION", {}, True, "AI/BI Genie Space creation"],
    
    # Alerts - Note: response.result contains the created alert details
    ["alerts_api_create", "alerts", "apiCreateAlert", "alert", 'COALESCE(get_json_object(response.result, "$.id"), request_params.alert_id)', 'COALESCE(get_json_object(response.result, "$.display_name"), get_json_object(response.result, "$.name"))', "SKIP_REMEDIATION", {}, True, "Alert creation via API"],
    ["alerts_create", "alerts", "createAlert", "alert", 'COALESCE(get_json_object(response.result, "$.id"), request_params.alert_id)', 'COALESCE(get_json_object(response.result, "$.display_name"), get_json_object(response.result, "$.name"))', "SKIP_REMEDIATION", {}, True, "Alert creation"],
    ["alerts_sql_create", "databrickssql", "createAlert", "alert", "request_params.alertId", "request_params.alertId", "SKIP_REMEDIATION", {}, True, "SQL Alert creation"],
    
    # Clusters
    ["clusters_create", "clusters", "createResult", "cluster", "get_json_object(response.result, '$.cluster_id')", "request_params.clusterName", "DELETE_RESOURCE", {}, True, "Cluster creation"],
    
    # Cluster Policies - policy_id is returned in response.result
    ["cluster_policies_create", "clusterPolicies", "create", "clusterPolicy", 'get_json_object(response.result, "$.policy_id")', "request_params.name", "DELETE_RESOURCE", {}, True, "Cluster policy creation"],
    
    # Apps - app name is in response.result or request_params.name
    ["apps_create", "apps", "createApp", "apps", 'get_json_object(response.result, "$.name")', 'get_json_object(response.result, "$.name")', "DELETE_RESOURCE", {}, True, "App creation"],
    
    # SQL Warehouse
    ["warehouse_create", "databrickssql", "createWarehouse", "warehouse", 'get_json_object(response.result, "$.id")', "request_params.name", "DELETE_RESOURCE", {}, True, "SQL Warehouse creation"],
    ["query_creation", "databrickssql", "createQuery", "query", "request_params.queryId", "request_params.queryId", "SKIP_REMEDIATION", {}, True, "SQL Query creation"],
    
    # Data Monitoring
    ["monitor_create", "dataMonitoring", "createMonitor", "monitors", "request_params.full_table_name_arg", "request_params.full_table_name_arg", "DELETE_RESOURCE", {"table_name": "request_params.full_table_name_arg"}, True, "Lakehouse Monitor creation"],
    
    # Feature Store
    ["feature_spec_create", "featureStore", "createFeatureSpec", "featureSpec", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Feature Spec creation"],
    ["feature_table_create", "featureStore", "createFeatureTable", "featureTable", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Feature Table creation"],
    
    # Instance Pools - instance_pool_id is returned in response.result
    ["instance_pools_create", "instancePools", "create", "instancePool", 'get_json_object(response.result, "$.instance_pool_id")', "request_params.instance_pool_name", "DELETE_RESOURCE", {}, True, "Instance Pool creation"],
    
    # Jobs
    ["jobs_create", "jobs", "create", "jobs", 'get_json_object(response.result, "$.job_id")', "request_params.name", "DELETE_RESOURCE", {}, True, "Job creation"],
    
    # Database Instances
    #["database_instance_create", "databaseInstances", "createDatabaseInstance", "databaseInstance", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Database Instance creation"],
    #["database_catalog_create", "databaseInstances", "createDatabaseCatalog", "databaseCatalog", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Database Catalog creation"],
    #["database_table_create", "databaseInstances", "createDatabaseTable", "databaseTable", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Database Table creation"],
    
    # Delta Pipelines (DLT)
    ["pipelines_create", "deltaPipelines", "create", "pipelines", "request_params.id", "request_params.name", "DELETE_RESOURCE", {}, True, "Delta Live Tables Pipeline creation"],
    
    # MLflow Experiments
    ["mlflow_experiment_create", "mlflowExperiment", "createMlflowExperiment", "mlflowExperiments", "request_params.experimentId", "request_params.experimentName", "DELETE_RESOURCE", {}, True, "MLflow Experiment creation"],
    
    # Model Registry
    # ["model_comment_create", "modelRegistry", "createComment", "modelComment", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Model Comment creation"],
    # ["model_version_create", "modelRegistry", "createModelVersion", "modelVersion", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Model Version creation"],
    # ["registered_model_create", "modelRegistry", "createRegisteredModel", "registeredModel", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Registered Model creation"],
    # ["registry_webhook_create", "modelRegistry", "createRegistryWebhook", "registryWebhook", "request_params.orgId", "request_params.orgId", "DELETE_RESOURCE", {}, True, "Registry Webhook creation"],
    # ["transition_request_create", "modelRegistry", "createTransitionRequest", "modelTransitionRequest", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Model Transition Request creation"],
    
    # Serving Endpoints
    ["serving_endpoint_create", "serverlessRealTimeInference", "createServingEndpoint", "servingEndpoint", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Model Serving Endpoint creation"],
    
    # Secrets
    ["secret_scope_create", "secrets", "createScope", "secretScope", "request_params.scope", "request_params.scope", "DELETE_RESOURCE", {}, True, "Secret Scope creation"],
    
    # Vector Search
    ["vector_search_endpoint_create", "vectorSearch", "createEndpoint", "vectorSearchEndpoint", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Vector Search Endpoint creation"],
    ["vector_index_create", "vectorSearch", "createVectorIndex", "vectorIndex", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Vector Index creation"],
    
    # Clean Rooms
    ["clean_room_create", "clean-room", "createCleanRoom", "cleanRoom", "request_params.clean_room_name", "request_params.clean_room_name", "DELETE_RESOURCE", {}, True, "Clean Room creation"],
    
    # Unity Catalog
    ["uc_recipient_create", "unityCatalog", "createRecipient", "recipient", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Unity Catalog Recipient creation"],
    ["uc_share_create", "unityCatalog", "createShare", "share", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Unity Catalog Share creation"],
    ["uc_provider_create", "unityCatalog", "createProvider", "provider", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Unity Catalog Provider creation"],
    ["uc_catalog_create", "unityCatalog", "createCatalog", "catalog", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Unity Catalog creation"],
    ["uc_schema_create", "unityCatalog", "createSchema", "schema", "concat_ws('.', request_params.catalog_name, request_params.name)", "concat_ws('.', request_params.catalog_name, request_params.name)", "DELETE_RESOURCE", {}, True, "Unity Catalog Schema creation"],
    ["uc_table_create", "unityCatalog", "createTable", "table", "concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)", "concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)", "DELETE_RESOURCE", {}, True, "Unity Catalog Table creation"],
    ["uc_constraint_create", "unityCatalog", "createConstraint", "tableConstraint", "request_params.full_name_arg", "request_params.full_name_arg", "DELETE_RESOURCE", {}, True, "Unity Catalog Table Constraint creation"],
    ["uc_volume_create", "unityCatalog", "createVolume", "volume", "concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)", "concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)", "DELETE_RESOURCE", {}, True, "Unity Catalog Volume creation"],
    ["uc_registered_model_create", "unityCatalog", "createRegisteredModel", "ucRegisteredModel", "concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)", "concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)", "DELETE_RESOURCE", {}, True, "Unity Catalog Registered Model creation"],
    ["uc_model_version_create", "unityCatalog", "createModelVersion", "ucModelVersion", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Unity Catalog Model Version creation"],
    ["uc_connection_create", "unityCatalog", "createConnection", "connection", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Unity Catalog Connection creation"],
    ["uc_function_create", "unityCatalog", "createFunction", "function", "request_params.function_info", "request_params.function_info", "DELETE_RESOURCE", {}, True, "Unity Catalog Function creation"],
    ["uc_policy_create", "unityCatalog", "createPolicy", "abacPolicy", "request_params.policy_info", "request_params.policy_info", "DELETE_RESOURCE", {}, True, "Unity Catalog ABAC Policy creation"],
    #Workspace / Notebook
    ["notebook_create", "notebook", "createNotebook", "notebook", "request_params.notebookId", "concat('/Workspace', request_params.path)", "DELETE_RESOURCE", {}, True, "Notebook creation in Workspace"],
    ["file_create", "workspace", "createFile", "file", "request_params.path", "concat('/Workspace', request_params.path)", "DELETE_RESOURCE", {}, True, "File creation in Workspace"],
]

print(f"Defined {len(create_filters_raw)} create event filters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACL Change Filters
# MAGIC
# MAGIC These filters detect unauthorized permission changes.

# COMMAND ----------

# ACL change filters - detect unauthorized permission changes
# Format: [filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, remediation_action, extra_columns, is_active, description]

acl_filters_raw = [
    # Cluster ACL
    ["cluster_acl_change", "clusters", "changeClusterAcl", "cluster", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Cluster ACL change"],
    
    # Cluster Policy ACL
    ["cluster_policy_acl_change", "clusterPolicies", "changeClusterPolicyAcl", "clusterPolicy", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Cluster Policy ACL change"],
    
    # Apps ACL
    ["apps_acl_change", "apps", "changeAppsAcl", "apps", "request_params.request_object_id", "request_params.request_object_id", "REVERT_PERMISSION", {}, True, "Apps ACL change"],
    
    # SQL Endpoint ACL
    ["endpoint_acl_change", "databrickssql", "changeEndpointAcls", "warehouse", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "SQL Warehouse/Endpoint ACL change"],
    
    # Feature Table ACL
    ["feature_table_acl_change", "featureStore", "changeFeatureTableAcl", "featureTable", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Feature Table ACL change"],
    
    # Instance Pool ACL
    ["instance_pool_acl_change", "instancePools", "changeInstancePoolAcl", "instancePool", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Instance Pool ACL change"],
    
    # Job ACL
    ["job_acl_change", "jobs", "changeJobAcl", "jobs", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Job ACL change"],
    
    # Database Instance ACL
    # ["database_instance_acl_change", "databaseInstances", "changeDatabaseInstanceAcl", "databaseInstance", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Database Instance ACL change"],
    
    # Pipeline ACL
    ["pipeline_acl_change", "deltaPipelines", "changePipelineAcls", "pipelines", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Delta Pipeline ACL change"],
    
    # Registered Model ACL
    ["registered_model_acl_change", "modelRegistry", "changeRegisteredModelAcl", "registeredModel", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Registered Model ACL change"],
    
    # Serving Endpoint ACL
    ["serving_endpoint_acl_change", "serverlessRealTimeInference", "changeInferenceEndpointAcl", "servingEndpoint", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Serving Endpoint ACL change"],
    
    # Vector Search Endpoint ACL
    ["vector_endpoint_acl_change", "vectorSearch", "changeEndpointAcl", "vectorSearchEndpoint", "request_params.request_object_id", "request_params.request_object_id", "REVERT_PERMISSION", {}, True, "Vector Search Endpoint ACL change"],

   # Secret Scope ACL - putAcl and deleteAcl operations both require permission revert
    ["secret_scope_acl_put", "secrets", "putAcl", "secretScope", "request_params.scope", "request_params.scope", "REVERT_PERMISSION", {}, True, "Secret Scope ACL put/change"],
    ["secret_scope_acl_delete", "secrets", "deleteAcl", "secretScope", "request_params.scope", "request_params.scope", "REVERT_PERMISSION", {}, True, "Secret Scope ACL deletion"],
    
    # Workspace ACL - object_type determined dynamically from aclChangeResourceName pattern
    # aclChangeResourceName patterns: /alerts/, /alertsv2/, /dashboards/, /dashboardsv3/, /datarooms/,
    # /directories/, /experiments/, /files/, /folders/, /genie/, /notebooks/, /projects/, /queries/, /repos/
    ["workspace_acl_change", "workspace", "changeWorkspaceAcl", 
     """CASE 
        WHEN request_params.aclChangeResourceName LIKE 'alerts/%' THEN 'alerts'
        WHEN request_params.aclChangeResourceName LIKE 'alertsv2/%' THEN 'alertsv2'
        WHEN request_params.aclChangeResourceName LIKE 'dashboards/%' THEN 'dashboard'
        WHEN request_params.aclChangeResourceName LIKE 'dashboardsv3/%' THEN 'lakeview_dashboard'
        WHEN request_params.aclChangeResourceName LIKE 'datarooms/%' THEN 'dataroom'
        WHEN request_params.aclChangeResourceName LIKE 'directories/%' THEN 'directory'
        WHEN request_params.aclChangeResourceName LIKE 'experiments/%' THEN 'mlflowExperiments'
        WHEN request_params.aclChangeResourceName LIKE 'files/%' THEN 'file'
        WHEN request_params.aclChangeResourceName LIKE 'folders/%' THEN 'folder'
        WHEN request_params.aclChangeResourceName LIKE 'genie/%' THEN 'genieSpace'
        WHEN request_params.aclChangeResourceName LIKE 'notebooks/%' THEN 'notebook'
        WHEN request_params.aclChangeResourceName LIKE 'projects/%' THEN 'project'
        WHEN request_params.aclChangeResourceName LIKE 'queries/%' THEN 'query'
        WHEN request_params.aclChangeResourceName LIKE 'repos/%' THEN 'repo'
        ELSE 'directory'
     END""", 
     "split_part(request_params.aclChangeResourceName, '/', 2)", "request_params.aclChangeResourceName", "REVERT_PERMISSION", 
     {"resource_path": "request_params.aclChangeResourceName"}, True, 
     "Workspace ACL change (notebooks, folders, dashboards, queries, repos, alerts, experiments, genie, files)"],
    
    # Unity Catalog Grants - object_type is dynamic from securable_type
    ["uc_grants_change", "unityCatalog", "updatePermissions", 
     "LOWER(request_params.securable_type)",  # Dynamic object type from securable_type
     "request_params.securable_full_name", "request_params.securable_full_name", "REVERT_PERMISSION", 
     {"securable_type": "request_params.securable_type", "changes": "request_params.changes"}, True, 
     "Unity Catalog Grants change (catalog, schema, table, volume, function, connection, externalLocation, storageCredential, share, recipient, provider, metastore, ucRegisteredModel, vectorIndex, featureTable)"]
]

print(f"Defined {len(acl_filters_raw)} ACL change filters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Event Filters
# MAGIC
# MAGIC These filters detect unauthorized resource deletions for security reporting.

# COMMAND ----------

# Delete event filters - detect unauthorized resource deletions
# Format: [filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, remediation_action, extra_columns, is_active, description]

delete_filters_raw = [
    # User deletion
    ["user_delete", "accounts", "delete", "user", "request_params.targetUserId", "request_params.targetUserId", "REPORT_DELETION", {}, True, "User deletion"],
    
    # Dashboard
    ["dashboard_trash", "dashboards", "trashDashboard", "dashboard", "request_params.dashboard_id", "request_params.dashboard_id", "REPORT_DELETION", {}, True, "Dashboard trashed"],
    ["dashboard_schedule_delete", "dashboards", "deleteSchedule", "dashboardSchedule", "request_params.dashboard_id", "request_params.dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard schedule deletion"],
    ["dashboard_subscription_delete", "dashboards", "deleteSubscription", "dashboardSubscription", "request_params.dashboard_id", "request_params.dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard subscription deletion"],
    
    # Genie Space
    ["genie_space_trash", "aibiGenie", "trashSpace", "genieSpace", "request_params.space_id", "request_params.space_id", "REPORT_DELETION", {}, True, "Genie Space trashed"],
    
    # Alert
    ["alert_trash", "alerts", "apiTrashAlert", "alert", "request_params.alert_id", "request_params.alert_id", "REPORT_DELETION", {}, True, "Alert trashed"],
    
    # Cluster
    ["cluster_delete", "clusters", "permanentDelete", "cluster", "request_params.cluster_id", "request_params.cluster_id", "REPORT_DELETION", {}, True, "Cluster permanent deletion"],
    
    # Cluster Policy
    ["cluster_policy_delete", "clusterPolicies", "delete", "clusterPolicy", "request_params.policy_id", "request_params.policy_id", "REPORT_DELETION", {}, True, "Cluster Policy deletion"],
    
    # Apps
    ["app_delete", "apps", "deleteApp", "apps", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "App deletion"],
    
    # SQL Warehouse/Query
    ["warehouse_delete", "databrickssql", "deleteWarehouse", "warehouse", "request_params.id", "request_params.id", "REPORT_DELETION", {}, True, "SQL Warehouse deletion"],
    ["query_delete", "databrickssql", "deleteQuery", "query", "request_params.queryId", "request_params.queryId", "REPORT_DELETION", {}, True, "SQL Query deletion"],
    
    # Data Monitoring
    ["monitor_delete", "dataMonitoring", "DeleteMonitor", "monitors", "request_params.full_table_name_arg", "request_params.full_table_name_arg", "REPORT_DELETION", {}, True, "Lakehouse Monitor deletion"],
    
    # Feature Store
    ["feature_table_delete", "featureStore", "deleteFeatureTable", "featureTable", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Feature Table deletion"],
    
    # Jobs
    ["job_delete", "jobs", "delete", "jobs", "request_params.job_id", "request_params.job_id", "REPORT_DELETION", {}, True, "Job deletion"],
    ["job_run_delete", "jobs", "deleteRun", "jobRun", "request_params.run_id", "request_params.run_id", "REPORT_DELETION", {}, True, "Job Run deletion"],
    
    # Pipeline
    ["pipeline_delete", "deltaPipelines", "delete", "pipelines", "request_params.pipeline_id", "request_params.pipeline_id", "REPORT_DELETION", {}, True, "Delta Pipeline deletion"],
    
    # MLflow Experiment
    ["mlflow_experiment_delete", "mlflowExperiment", "deleteMlflowExperiment", "mlflowExperiments", "request_params.experimentId", "request_params.experimentId", "REPORT_DELETION", {}, True, "MLflow Experiment deletion"],
    
    # Model Registry
    ["model_version_delete", "modelRegistry", "deleteModelVersion", "modelVersion", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Model Version deletion"],
    ["registered_model_delete", "modelRegistry", "deleteRegisteredModel", "registeredModel", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Registered Model deletion"],
    
    # Serving Endpoint
    ["serving_endpoint_delete", "serverlessRealTimeInference", "deleteServingEndpoint", "servingEndpoint", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Serving Endpoint deletion"],
    
    # Notebooks/Folders/Repos
    ["folder_delete", "notebook", "deleteFolder", "folder", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Folder deletion"],
    ["notebook_delete", "notebook", "deleteNotebook", "notebook", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Notebook deletion"],
    ["repo_delete", "notebook", "deleteRepo", "repo", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Repo deletion"],
    
    # Secrets    
																																								   
    ["secret_scope_delete", "secrets", "deleteScope", "secretScope", "request_params.scope", "request_params.scope", "REPORT_DELETION", {}, True, "Secret Scope deletion"],
    ["secret_delete", "secrets", "deleteSecret", "secret", "request_params.scope", "request_params.scope", "REPORT_DELETION", {"key": "request_params.key"}, True, "Secret deletion"],
    
    # Vector Search
    ["vector_endpoint_delete", "vectorSearch", "deleteEndpoint", "vectorSearchEndpoint", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Vector Search Endpoint deletion"],
    ["vector_index_delete", "vectorSearch", "deleteVectorIndex", "vectorIndex", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Vector Index deletion"],
    ["data_vector_index_delete", "vectorSearch", "deleteDataVectorIndex", "dataVectorIndex", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Data Vector Index deletion"],
    
    
    # Unity Catalog
    ["uc_metastore_delete", "unityCatalog", "deleteMetastore", "metastore", "request_params.metastore_id", "request_params.metastore_id", "REPORT_DELETION", {}, True, "Unity Catalog Metastore deletion"],
    ["uc_external_location_delete", "unityCatalog", "deleteExternalLocation", "externalLocation", "request_params.name_arg", "request_params.name_arg", "REPORT_DELETION", {}, True, "Unity Catalog External Location deletion"],
    ["uc_catalog_delete", "unityCatalog", "deleteCatalog", "catalog", "request_params.name_arg", "request_params.name_arg", "REPORT_DELETION", {}, True, "Unity Catalog deletion"],
    ["uc_schema_delete", "unityCatalog", "deleteSchema", "schema", "request_params.full_name_arg", "request_params.full_name_arg", "REPORT_DELETION", {}, True, "Unity Catalog Schema deletion"],
    ["uc_table_delete", "unityCatalog", "deleteTable", "table", "request_params.full_name_arg", "request_params.full_name_arg", "REPORT_DELETION", {}, True, "Unity Catalog Table deletion"],
    ["uc_storage_credential_delete", "unityCatalog", "deleteStorageCredential", "storageCredential", "request_params.name_arg", "request_params.name_arg", "REPORT_DELETION", {}, True, "Storage Credential deletion"],
    ["uc_constraint_delete", "unityCatalog", "deleteConstraint", "tableConstraint", "request_params.full_name_arg", "request_params.full_name_arg", "REPORT_DELETION", {}, True, "Table Constraint deletion"],
    ["uc_volume_delete", "unityCatalog", "deleteVolume", "volume", "request_params.volume_full_name", "request_params.volume_full_name", "REPORT_DELETION", {}, True, "Unity Catalog Volume deletion"],
    ["uc_connection_delete", "unityCatalog", "deleteConnection", "connection", "request_params.name_arg", "request_params.name_arg", "REPORT_DELETION", {}, True, "Unity Catalog Connection deletion"],
    ["uc_function_delete", "unityCatalog", "deleteFunction", "function", "request_params.full_name_arg", "request_params.full_name_arg", "REPORT_DELETION", {}, True, "Unity Catalog Function deletion"],
    ["uc_policy_delete", "unityCatalog", "deletePolicy", "abacPolicy", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Unity Catalog ABAC Policy deletion"],
    ["uc_credential_delete", "unityCatalog", "deleteCredential", "credential", "request_params.name_arg", "request_params.name_arg", "REPORT_DELETION", {}, True, "Unity Catalog Credential deletion"],
    ["uc_metastore_assignment_delete", "unityCatalog", "deleteMetastoreAssignment", "metastoreAssignment", "request_params.input_workspace_id", "request_params.input_workspace_id", "REPORT_DELETION", {}, True, "Metastore Assignment deletion"],
    
    #Workspace
    ["workspace_fileDelete", "workspace", "fileDelete", "files", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Workspace file deletion"],
]

print(f"Defined {len(delete_filters_raw)} delete event filters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workspace Admin Features
# MAGIC
# MAGIC These filters detect unauthorized entitlement changes.

# COMMAND ----------
# Workspace Admin features - detect entitlement changes
# Format: [filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, remediation_action, extra_columns, is_active, description]

workspace_admin_filters_raw = [
    ["workspace_any_users_workspace_grant_change", "accounts", "changeDatabricksWorkspaceAcl", "identity_replace", "request_params.targetUserId", "request_params.aclChangeResourceName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Groups/Users/SP workspace grant change"],
    ["workspace_any_users_dbsql_grant_change", "accounts", "changeDatabricksSqlAcl", "identity_replace", "request_params.targetUserId", "request_params.aclChangeResourceName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Groups/Users/SP dbsql grant change"],
    ["workspace_token_grant_change", "accounts", "changeDbTokenAcl", "tokensAcls", "concat_ws('/',workspace_id,'tokens')", "concat_ws('/',workspace_id,'tokens')", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Tokens Grants change"],
    ["workspace_user_set_admin", "accounts", "setAdmin", "users", "request_params.targetUserId", "request_params.targetUserName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace User Set Admin Grant"],
    ["workspace_user_remove_admin", "accounts", "removeAdmin", "users", "request_params.targetUserId", "request_params.targetUserName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace User Revoked Admin Grant"],
    ["any_file_grant_change", "sqlPermissions", "grantPermission", "any_file_permissions", "concat_ws('/',workspace_id,'any_files')", "request_params.permission", "REVERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Any Files ACL Grant"],
    ["any_file_revoke_change", "sqlPermissions", "revokePermission", "any_file_permissions", "concat_ws('/',workspace_id,'any_files')", "request_params.permission", "REVERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Any Files ACL Revoke"],
]
print(f"Defined {len(workspace_admin_filters_raw)} entitlements change filters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Filter Records

# COMMAND ----------

from datetime import datetime
import uuid

def convert_to_filter_record(raw_filter, violation_type):
    """Convert raw filter list to filter record dictionary."""
    return {
        'filter_id': str(uuid.uuid4()),
        'filter_name': raw_filter[0],
        'service_name': raw_filter[1],
        'action_name': raw_filter[2],
        'object_type': raw_filter[3],
        'object_id_expr': raw_filter[4],
        'object_name_expr': raw_filter[5],
        'extra_columns': raw_filter[7] if raw_filter[7] else {},
        'violation_type': violation_type,
        'remediation_action': raw_filter[6],
        'is_active': raw_filter[8],
        'description': raw_filter[9],
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }

# Convert raw filters to records
create_filters = [convert_to_filter_record(f, "UNAPPROVED_CREATION") for f in create_filters_raw]
acl_filters = [convert_to_filter_record(f, "UNAUTHORIZED_PERMISSION_CHANGE") for f in acl_filters_raw]
delete_filters = [convert_to_filter_record(f, "UNAUTHORIZED_DELETION") for f in delete_filters_raw]
workspace_admin_filters = [convert_to_filter_record(f, "UNAUTHORIZED_ENTITLEMENT_CHANGE") for f in workspace_admin_filters_raw]

print(f"Converted {len(create_filters)} create filters")
print(f"Converted {len(acl_filters)} ACL filters")
print(f"Converted {len(delete_filters)} delete filters")
print(f"Converted {len(workspace_admin_filters)} workspace admin filters")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Filters

# COMMAND ----------

all_filters = create_filters + acl_filters + delete_filters + workspace_admin_filters

print(f"\nTotal filters defined: {len(all_filters)}")
print(f"  - Create event filters: {len(create_filters)}")
print(f"  - ACL change filters: {len(acl_filters)}")
print(f"  - Delete event filters: {len(delete_filters)}")
print(f"  - Workspace admin filters: {len(workspace_admin_filters)}")

# Count active vs inactive
active_count = sum(1 for f in all_filters if f['is_active'])
inactive_count = len(all_filters) - active_count
print(f"\nActive filters: {active_count}")
print(f"Inactive filters: {inactive_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to Table

# COMMAND ----------

from pyspark.sql.types import *

# Define schema for filter records (removed priority column)
filter_schema = StructType([
    StructField('filter_id', StringType(), True),
    StructField('filter_name', StringType(), True),
    StructField('service_name', StringType(), True),
    StructField('action_name', StringType(), True),
    StructField('object_type', StringType(), True),
    StructField('object_id_expr', StringType(), True),
    StructField('object_name_expr', StringType(), True),
    StructField('extra_columns', MapType(StringType(), StringType()), True),
    StructField('violation_type', StringType(), True),
    StructField('remediation_action', StringType(), True),
    StructField('is_active', BooleanType(), True),
    StructField('description', StringType(), True),
    StructField('created_at', TimestampType(), True),
    StructField('updated_at', TimestampType(), True)
])

# Create DataFrame
df = spark.createDataFrame(all_filters, schema=filter_schema)

table_name = f"{catalog}.{schema}.governance_filters"

if replace_existing:
    # Truncate and reload
    spark.sql(f"TRUNCATE TABLE {table_name}")
    print(f"Truncated existing data in {table_name}")

# Create temp view for merge
df.createOrReplaceTempView("new_filters")

# Merge with existing data
spark.sql(f"""
    MERGE INTO {table_name} AS target
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

print(f"✓ Loaded {len(all_filters)} filters to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Loaded Data

# COMMAND ----------

# Count by violation type
summary_df = spark.sql(f"""
    SELECT 
        violation_type,
        COUNT(*) as filter_count,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_count,
        COUNT(DISTINCT service_name) as unique_services,
        COUNT(DISTINCT object_type) as unique_object_types
    FROM {table_name}
    GROUP BY violation_type
    ORDER BY violation_type
""")

print("Filter Summary by Violation Type:")
display(summary_df)

# Show all filters
all_filters_df = spark.sql(f"""
    SELECT 
        filter_name,
        service_name,
        action_name,
        object_type,
        violation_type,
        remediation_action,
        is_active
    FROM {table_name}
    ORDER BY violation_type, service_name, action_name
""")

print("\nAll Filters:")
display(all_filters_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════════════════════════╗
║                    GOVERNANCE FILTERS LOADED                              ║
╚══════════════════════════════════════════════════════════════════════════╝

Total Filters Loaded: {len(all_filters)}

Breakdown by Violation Type:
  - UNAPPROVED_CREATION:           {len(create_filters)} filters
  - UNAUTHORIZED_PERMISSION_CHANGE: {len(acl_filters)} filters
  - UNAUTHORIZED_DELETION:          {len(delete_filters)} filters
  - UNAUTHORIZED_ENTITLEMENT_CHANGE: {len(workspace_admin_filters)} filters

Active/Inactive:
  - Active filters:   {active_count}
  - Inactive filters: {inactive_count}

These filters are used by the watcher notebook to:
1. Dynamically generate audit log queries
2. Extract object IDs and names from audit events
3. Determine appropriate remediation actions

To update filters:
1. Modify the filter definitions in this notebook
2. Re-run the notebook with replace_existing = true

To add a new filter:
1. Add a new entry to the appropriate *_filters_raw list
2. Re-run the notebook to merge the new filter

To disable a filter:
- Change is_active to False in the filter definition and re-run
- Or run: UPDATE {table_name}
           SET is_active = false, updated_at = current_timestamp()
           WHERE filter_name = 'filter_name'
""")

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'filters_loaded': len(all_filters),
    'active_filters': active_count,
    'inactive_filters': inactive_count,
    'create_filters': len(create_filters),
    'acl_filters': len(acl_filters),
    'delete_filters': len(delete_filters),
    'workspace_admin_filters': len(workspace_admin_filters),
    'timestamp': datetime.now().isoformat()
}, indent=3))