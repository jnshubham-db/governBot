"""
Governance filter definitions exported from governBot/mx_scripts/03a_load_filters.py for parity.
Each raw entry: [filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, remediation_action, extra_columns_dict, is_active, description]
"""


def get_governance_filter_definitions():
    """Return dict with create_filters_raw, acl_filters_raw, delete_filters_raw, workspace_admin_filters_raw, object_changes_filters_raw (lists of raw filter tuples)."""
    return {
        "create_filters_raw": _create_filters_raw(),
        "acl_filters_raw": _acl_filters_raw(),
        "delete_filters_raw": _delete_filters_raw(),
        "workspace_admin_filters_raw": _workspace_admin_filters_raw(),
        "object_changes_filters_raw": _object_changes_filters_raw(),
    }


def _create_filters_raw():
    return [
        ["dashboards_create", "dashboards", "createDashboard", "dashboard", "request_params.dashboard_id", "request_params.dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard creation"],
        ["dashboards_clone", "dashboards", "cloneDashboard", "dashboard", "request_params.new_dashboard_id", "request_params.new_dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard clone"],
        ["genie_space_create", "aibiGenie", "createSpace", "genieSpace", 'get_json_object(response.result, "$.space_id")', 'get_json_object(response.result, "$.space_id")', "SKIP_REMEDIATION", {}, True, "AI/BI Genie Space creation"],
        ["alerts_api_create", "alerts", "apiCreateAlert", "alert", 'COALESCE(get_json_object(response.result, "$.id"), request_params.alert_id)', 'COALESCE(get_json_object(response.result, "$.display_name"), get_json_object(response.result, "$.name"))', "SKIP_REMEDIATION", {}, True, "Alert creation via API"],
        ["alerts_create", "alerts", "createAlert", "alert", 'COALESCE(get_json_object(response.result, "$.id"), request_params.alert_id)', 'COALESCE(get_json_object(response.result, "$.display_name"), get_json_object(response.result, "$.name"))', "SKIP_REMEDIATION", {}, True, "Alert creation"],
        ["alerts_sql_create", "databrickssql", "createAlert", "alert", "request_params.alertId", "request_params.alertId", "SKIP_REMEDIATION", {}, True, "SQL Alert creation"],
        ["clusters_create", "clusters", "createResult", "cluster", "get_json_object(response.result, '$.cluster_id')", "request_params.clusterName", "DELETE_RESOURCE", {}, True, "Cluster creation"],
        ["cluster_policies_create", "clusterPolicies", "create", "clusterPolicy", 'get_json_object(response.result, "$.policy_id")', "request_params.name", "DELETE_RESOURCE", {}, True, "Cluster policy creation"],
        ["apps_create", "apps", "createApp", "apps", 'get_json_object(response.result, "$.name")', 'get_json_object(response.result, "$.name")', "DELETE_RESOURCE", {}, True, "App creation"],
        ["warehouse_create", "databrickssql", "createWarehouse", "warehouse", 'get_json_object(response.result, "$.id")', "request_params.name", "DELETE_RESOURCE", {}, True, "SQL Warehouse creation"],
        ["query_creation", "databrickssql", "createQuery", "query", "request_params.queryId", "request_params.queryId", "SKIP_REMEDIATION", {}, True, "SQL Query creation"],
        ["monitor_create", "dataMonitoring", "createMonitor", "monitors", "request_params.full_table_name_arg", "request_params.full_table_name_arg", "DELETE_RESOURCE", {"table_name": "request_params.full_table_name_arg"}, True, "Lakehouse Monitor creation"],
        ["feature_spec_create", "featureStore", "createFeatureSpec", "featureSpec", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Feature Spec creation"],
        ["feature_table_create", "featureStore", "createFeatureTable", "featureTable", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Feature Table creation"],
        ["instance_pools_create", "instancePools", "create", "instancePool", 'get_json_object(response.result, "$.instance_pool_id")', "request_params.instance_pool_name", "DELETE_RESOURCE", {}, True, "Instance Pool creation"],
        ["jobs_create", "jobs", "create", "jobs", 'get_json_object(response.result, "$.job_id")', "request_params.name", "DELETE_RESOURCE", {}, True, "Job creation"],
        ["pipelines_create", "deltaPipelines", "create", "pipelines", "request_params.id", "request_params.name", "DELETE_RESOURCE", {}, True, "Delta Live Tables Pipeline creation"],
        ["mlflow_experiment_create", "mlflowExperiment", "createMlflowExperiment", "mlflowExperiments", "request_params.experimentId", "request_params.experimentName", "DELETE_RESOURCE", {}, True, "MLflow Experiment creation"],
        ["serving_endpoint_create", "serverlessRealTimeInference", "createServingEndpoint", "servingEndpoint", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Model Serving Endpoint creation"],
        ["secret_scope_create", "secrets", "createScope", "secretScope", "request_params.scope", "request_params.scope", "DELETE_RESOURCE", {}, True, "Secret Scope creation"],
        ["vector_search_endpoint_create", "vectorSearch", "createEndpoint", "vectorSearchEndpoint", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Vector Search Endpoint creation"],
        ["vector_index_create", "vectorSearch", "createVectorIndex", "vectorIndex", "request_params.name", "request_params.name", "DELETE_RESOURCE", {}, True, "Vector Index creation"],
        ["clean_room_create", "clean-room", "createCleanRoom", "cleanRoom", "request_params.clean_room_name", "request_params.clean_room_name", "DELETE_RESOURCE", {}, True, "Clean Room creation"],
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
        ["notebook_create", "notebook", "createNotebook", "notebook", "request_params.notebookId", "concat('/Workspace', request_params.path)", "DELETE_RESOURCE", {}, True, "Notebook creation in Workspace"],
        ["file_create", "workspace", "createFile", "file", "request_params.path", "concat('/Workspace', request_params.path)", "DELETE_RESOURCE", {}, True, "File creation in Workspace"],
        ["workspace_group_create", "accounts", "createGroup", "groups", "request_params.targetUserId", "request_params.targetUserName", "REPORT_SECURITY_TEAM", {}, True, "Workspace level group creation"],
        ["workspace_user_or_sp_create", "accounts", "add", "CASE WHEN request_params.targetUserName is not like '%@%' THEN 'users' ELSE 'service_principal' END", "request_params.targetUserId", "request_params.targetUserName", "REPORT_SECURITY_TEAM", {}, True, "Workspace level user or SP addition"],
    ]


def _acl_filters_raw():
    return [
        ["cluster_acl_change", "clusters", "changeClusterAcl", "cluster", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Cluster ACL change"],
        ["cluster_policy_acl_change", "clusterPolicies", "changeClusterPolicyAcl", "clusterPolicy", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Cluster Policy ACL change"],
        ["apps_acl_change", "apps", "changeAppsAcl", "apps", "request_params.request_object_id", "request_params.request_object_id", "REVERT_PERMISSION", {}, True, "Apps ACL change"],
        ["endpoint_acl_change", "databrickssql", "changeEndpointAcls", "warehouse", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "SQL Warehouse/Endpoint ACL change"],
        ["feature_table_acl_change", "featureStore", "changeFeatureTableAcl", "featureTable", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Feature Table ACL change"],
        ["instance_pool_acl_change", "instancePools", "changeInstancePoolAcl", "instancePool", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Instance Pool ACL change"],
        ["job_acl_change", "jobs", "changeJobAcl", "jobs", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Job ACL change"],
        ["pipeline_acl_change", "deltaPipelines", "changePipelineAcls", "pipelines", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Delta Pipeline ACL change"],
        ["registered_model_acl_change", "modelRegistry", "changeRegisteredModelAcl", "registeredModel", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Registered Model ACL change"],
        ["serving_endpoint_acl_change", "serverlessRealTimeInference", "changeInferenceEndpointAcl", "servingEndpoint", "request_params.resourceId", "request_params.resourceId", "REVERT_PERMISSION", {}, True, "Serving Endpoint ACL change"],
        ["vector_endpoint_acl_change", "vectorSearch", "changeEndpointAcl", "vectorSearchEndpoint", "request_params.request_object_id", "request_params.request_object_id", "REVERT_PERMISSION", {}, True, "Vector Search Endpoint ACL change"],
        ["secret_scope_acl_put", "secrets", "putAcl", "secretScope", "request_params.scope", "request_params.scope", "REVERT_PERMISSION", {}, True, "Secret Scope ACL put/change"],
        ["secret_scope_acl_delete", "secrets", "deleteAcl", "secretScope", "request_params.scope", "request_params.scope", "REVERT_PERMISSION", {}, True, "Secret Scope ACL deletion"],
        ["workspace_acl_change", "workspace", "changeWorkspaceAcl", "CASE WHEN request_params.aclChangeResourceName LIKE 'alerts/%' THEN 'alerts' WHEN request_params.aclChangeResourceName LIKE 'alertsv2/%' THEN 'alertsv2' WHEN request_params.aclChangeResourceName LIKE 'dashboards/%' THEN 'dashboard' WHEN request_params.aclChangeResourceName LIKE 'dashboardsv3/%' THEN 'lakeview_dashboard' WHEN request_params.aclChangeResourceName LIKE 'datarooms/%' THEN 'dataroom' WHEN request_params.aclChangeResourceName LIKE 'directories/%' THEN 'directory' WHEN request_params.aclChangeResourceName LIKE 'experiments/%' THEN 'mlflowExperiments' WHEN request_params.aclChangeResourceName LIKE 'files/%' THEN 'file' WHEN request_params.aclChangeResourceName LIKE 'folders/%' THEN 'folder' WHEN request_params.aclChangeResourceName LIKE 'genie/%' THEN 'genieSpace' WHEN request_params.aclChangeResourceName LIKE 'notebooks/%' THEN 'notebook' WHEN request_params.aclChangeResourceName LIKE 'projects/%' THEN 'project' WHEN request_params.aclChangeResourceName LIKE 'queries/%' THEN 'query' WHEN request_params.aclChangeResourceName LIKE 'repos/%' THEN 'repo' ELSE 'directory' END", "CASE WHEN request_params.aclChangeResourceName='folders/workspace' THEN '0' ELSE split_part(request_params.aclChangeResourceName, '/', 2) END", "request_params.aclChangeResourceName", "REVERT_PERMISSION", {"resource_path": "request_params.aclChangeResourceName"}, True, "Workspace ACL change (notebooks, folders, dashboards, queries, repos, alerts, experiments, genie, files)"],
        ["uc_grants_change", "unityCatalog", "updatePermissions", "LOWER(request_params.securable_type)", "request_params.securable_full_name", "request_params.securable_full_name", "REVERT_PERMISSION", {"securable_type": "request_params.securable_type", "changes": "request_params.changes"}, True, "Unity Catalog Grants change"],
    ]


def _delete_filters_raw():
    return [
        ["user_delete", "accounts", "delete", "user", "request_params.targetUserId", "request_params.targetUserId", "REPORT_DELETION", {}, True, "User deletion"],
        ["dashboard_trash", "dashboards", "trashDashboard", "dashboard", "request_params.dashboard_id", "request_params.dashboard_id", "REPORT_DELETION", {}, True, "Dashboard trashed"],
        ["dashboard_schedule_delete", "dashboards", "deleteSchedule", "dashboardSchedule", "request_params.dashboard_id", "request_params.dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard schedule deletion"],
        ["dashboard_subscription_delete", "dashboards", "deleteSubscription", "dashboardSubscription", "request_params.dashboard_id", "request_params.dashboard_id", "SKIP_REMEDIATION", {}, True, "Dashboard subscription deletion"],
        ["genie_space_trash", "aibiGenie", "trashSpace", "genieSpace", "request_params.space_id", "request_params.space_id", "REPORT_DELETION", {}, True, "Genie Space trashed"],
        ["alert_trash", "alerts", "apiTrashAlert", "alert", "request_params.alert_id", "request_params.alert_id", "REPORT_DELETION", {}, True, "Alert trashed"],
        ["cluster_delete", "clusters", "permanentDelete", "cluster", "request_params.cluster_id", "request_params.cluster_id", "REPORT_DELETION", {}, True, "Cluster permanent deletion"],
        ["cluster_policy_delete", "clusterPolicies", "delete", "clusterPolicy", "request_params.policy_id", "request_params.policy_id", "REPORT_DELETION", {}, True, "Cluster Policy deletion"],
        ["app_delete", "apps", "deleteApp", "apps", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "App deletion"],
        ["warehouse_delete", "databrickssql", "deleteWarehouse", "warehouse", "request_params.id", "request_params.id", "REPORT_DELETION", {}, True, "SQL Warehouse deletion"],
        ["query_delete", "databrickssql", "deleteQuery", "query", "request_params.queryId", "request_params.queryId", "REPORT_DELETION", {}, True, "SQL Query deletion"],
        ["monitor_delete", "dataMonitoring", "DeleteMonitor", "monitors", "request_params.full_table_name_arg", "request_params.full_table_name_arg", "REPORT_DELETION", {}, True, "Lakehouse Monitor deletion"],
        ["feature_table_delete", "featureStore", "deleteFeatureTable", "featureTable", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Feature Table deletion"],
        ["job_delete", "jobs", "delete", "jobs", "request_params.job_id", "request_params.job_id", "REPORT_DELETION", {}, True, "Job deletion"],
        ["job_run_delete", "jobs", "deleteRun", "jobRun", "request_params.run_id", "request_params.run_id", "REPORT_DELETION", {}, True, "Job Run deletion"],
        ["pipeline_delete", "deltaPipelines", "delete", "pipelines", "request_params.pipeline_id", "request_params.pipeline_id", "REPORT_DELETION", {}, True, "Delta Pipeline deletion"],
        ["mlflow_experiment_delete", "mlflowExperiment", "deleteMlflowExperiment", "mlflowExperiments", "request_params.experimentId", "request_params.experimentId", "REPORT_DELETION", {}, True, "MLflow Experiment deletion"],
        ["model_version_delete", "modelRegistry", "deleteModelVersion", "modelVersion", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Model Version deletion"],
        ["registered_model_delete", "modelRegistry", "deleteRegisteredModel", "registeredModel", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Registered Model deletion"],
        ["serving_endpoint_delete", "serverlessRealTimeInference", "deleteServingEndpoint", "servingEndpoint", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Serving Endpoint deletion"],
        ["folder_delete", "notebook", "deleteFolder", "folder", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Folder deletion"],
        ["notebook_delete", "notebook", "deleteNotebook", "notebook", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Notebook deletion"],
        ["repo_delete", "notebook", "deleteRepo", "repo", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Repo deletion"],
        ["secret_scope_delete", "secrets", "deleteScope", "secretScope", "request_params.scope", "request_params.scope", "REPORT_DELETION", {}, True, "Secret Scope deletion"],
        ["secret_delete", "secrets", "deleteSecret", "secret", "request_params.scope", "request_params.scope", "REPORT_DELETION", {"key": "request_params.key"}, True, "Secret deletion"],
        ["vector_endpoint_delete", "vectorSearch", "deleteEndpoint", "vectorSearchEndpoint", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Vector Search Endpoint deletion"],
        ["vector_index_delete", "vectorSearch", "deleteVectorIndex", "vectorIndex", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Vector Index deletion"],
        ["data_vector_index_delete", "vectorSearch", "deleteDataVectorIndex", "dataVectorIndex", "request_params.name", "request_params.name", "REPORT_DELETION", {}, True, "Data Vector Index deletion"],
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
        ["workspace_fileDelete", "workspace", "fileDelete", "files", "request_params.path", "concat('/Workspace', request_params.path)", "REPORT_DELETION", {}, True, "Workspace file deletion"],
    ]


def _workspace_admin_filters_raw():
    return [
        ["workspace_groups_deletion", "accounts", "removeGroup", "groups", "request_params.targetUserId", "request_params.targetGroupName", "REPORT_DELETION", {}, True, "Workspace Groups deletion"],
        ["workspace_user_deletion", "accounts", "delete", "identity_replace", "request_params.targetUserId", "request_params.targetUserName", "REPORT_DELETION", {}, True, "Workspace User deletion"],
        ["workspace_any_users_workspace_grant_change", "accounts", "changeDatabricksWorkspaceAcl", "identity_replace", "request_params.targetUserId", "request_params.aclChangeResourceName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Groups/Users/SP workspace grant change"],
        ["workspace_any_users_dbsql_grant_change", "accounts", "changeDatabricksSqlAcl", "identity_replace", "request_params.targetUserId", "request_params.aclChangeResourceName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Groups/Users/SP dbsql grant change"],
        ["workspace_token_grant_change", "accounts", "changeDbTokenAcl", "tokensAcls", "concat_ws('/',workspace_id,'tokens')", "concat_ws('/',workspace_id,'tokens')", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Tokens Grants change"],
        ["workspace_user_set_admin", "accounts", "setAdmin", "users", "request_params.targetUserId", "request_params.targetUserName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace User Set Admin Grant"],
        ["workspace_user_remove_admin", "accounts", "removeAdmin", "users", "request_params.targetUserId", "request_params.targetUserName", "ALERT_ENTITLEMENT_CHANGE", {}, True, "Workspace User Revoked Admin Grant"],
        ["any_file_grant_change", "sqlPermissions", "grantPermission", "any_file_permissions", "concat_ws('/',workspace_id,'any_files')", "request_params.permission", "REVERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Any Files ACL Grant"],
        ["any_file_revoke_change", "sqlPermissions", "revokePermission", "any_file_permissions", "concat_ws('/',workspace_id,'any_files')", "request_params.permission", "REVERT_ENTITLEMENT_CHANGE", {}, True, "Workspace Any Files ACL Revoke"],
    ]


def _object_changes_filters_raw():
    return [
        ["uc_table_update", "unityCatalog", "updateTables", "table", "request_params.full_name_arg", "from_json(response.result, 'full_name string').full_name", "REPORT_OBJECT_UPDATE", {}, True, "Unity Catalog Table name/schema has been updated"],
        ["uc_schema_update", "unityCatalog", "updateSchema", "schema", "request_params.full_name_arg", "from_json(response.result, 'full_name string').full_name", "REPORT_OBJECT_UPDATE", {}, True, "Unity Catalog Schema has been updated"],
        ["uc_catalog_update", "unityCatalog", "updateCatalog", "catalog", "request_params.name_arg", "request_params.name", "REPORT_OBJECT_UPDATE", {}, True, "Unity Catalog has change"],
        ["uc_registered_model_update", "unityCatalog", "updateRegisteredModel", "ucRegisteredModel", "request_params.full_name_arg", "concat_ws('.', split_part(request_params.full_name_arg, '.', 1),split_part(request_params.full_name_arg, '.', 2),coalesce(request_params.name, request_params.new_name))", "REPORT_OBJECT_UPDATE", {}, True, "UC Registered Model has been changed"],
        ["registered_model_name_update", "modelRegistry", "renameRegisteredModel", "registeredModel", "request_params.name", "request_params.new_name", "REPORT_OBJECT_UPDATE", {}, True, "Workspace Registered model name has been updated"],
    ]
