# Databricks Governance Automation System

## Complete Documentation

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Notebooks Reference](#notebooks-reference)
4. [Workflow Configuration](#workflow-configuration)
5. [Pre-requisites & Setup](#pre-requisites--setup)
6. [Object Types Supported](#object-types-supported)
7. [Edge Cases & Special Conditions](#edge-cases--special-conditions)
8. [Remediation Actions](#remediation-actions)
9. [Alerting & Notifications](#alerting--notifications)
10. [Troubleshooting](#troubleshooting)

---

## Overview

The **Databricks Governance Automation System** (GovernBot) is a comprehensive solution for monitoring, detecting, and remediating unauthorized changes in Databricks workspaces. It provides:

- **Automated Discovery**: Catalog all existing resources and their permissions
- **Real-time Monitoring**: Track audit logs for unauthorized activities
- **Automatic Remediation**: Delete unauthorized resources or revert permission changes
- **Compliance Reporting**: Generate reports for security teams

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Pre-Approved Objects** | Resources that existed before governance was enabled, or were created by authorized users |
| **Pre-Approved Identities** | Users, groups, or service principals authorized to make changes |
| **Violation** | Any unauthorized creation, deletion, or permission change |
| **Remediation** | The action taken to correct a violation |

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS GOVERNANCE SYSTEM                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         DELTA TABLES                                 │    │
│  ├─────────────────────────────────────────────────────────────────────┤    │
│  │  • governance_config_workspaces     - Workspace configurations       │    │
│  │  • governance_preapproved_objects   - Approved resources & perms     │    │
│  │  • governance_preapproved_identities - Authorized users/groups/SPs   │    │
│  │  • governance_filters               - Audit log filter rules         │    │
│  │  • governance_violations_staging    - Detected violations            │    │
│  │  • governance_control_actions       - Remediation audit trail        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      DATA SOURCES                                    │    │
│  ├─────────────────────────────────────────────────────────────────────┤    │
│  │  • system.access.audit              - Databricks Audit Logs          │    │
│  │  • Databricks SDK APIs              - Permissions, Grants, etc.      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
                           ONE-TIME SETUP
    ┌─────────────────────────────────────────────────────────┐
    │                                                          │
    │  01_setup_tables → 02_load_workspace → 03_approved_id   │
    │         ↓                                    ↓          │
    │         └──────────────┬────────────────────┘           │
    │                        ↓                                │
    │              03a_load_filters                           │
    │                        ↓                                │
    │              04_discover (Initial Discovery)            │
    │                                                          │
    └─────────────────────────────────────────────────────────┘
                              
                        DAILY WORKFLOW
    ┌─────────────────────────────────────────────────────────┐
    │                                                          │
    │     ┌──────────────┐                                    │
    │     │ 05_watcher   │  ← Detects violations from         │
    │     │              │    audit logs                      │
    │     └──────┬───────┘                                    │
    │            ↓                                            │
    │     ┌──────────────────┐                                │
    │     │04a_sync_approved │  ← Syncs approved user         │
    │     │     _changes     │    changes to baseline         │
    │     └──────┬───────────┘                                │
    │            ↓                                            │
    │     ┌──────────────┐                                    │
    │     │06_remediation│  ← Takes corrective actions        │
    │     │              │    (delete/revert)                 │
    │     └──────┬───────┘                                    │
    │            ↓                                            │
    │     ┌──────────────┐                                    │
    │     │07_validation │  ← Validates actions &             │
    │     │              │    generates reports               │
    │     └──────┬───────┘                                    │
    │            ↓                                            │
    │     ┌──────────────┐                                    │
    │     │   ALERTS     │  ← Sends notifications to          │
    │     │              │    security team                   │
    │     └──────────────┘                                    │
    │                                                          │
    └─────────────────────────────────────────────────────────┘
```

---

## Notebooks Reference

### One-Time Setup Notebooks

#### 1. `01_setup_tables.py` - Table Setup

**Purpose**: Creates all required Delta tables for the governance system.

**When to Run**: First step when setting up the governance system.

**Tables Created**:

| Table Name | Description |
|------------|-------------|
| `governance_config_workspaces` | Workspace configurations with enforcement settings |
| `governance_preapproved_objects` | Pre-approved objects and their permissions |
| `governance_preapproved_identities` | Authorized users, groups, and service principals |
| `governance_violations_staging` | Detected violations pending remediation |
| `governance_control_actions` | Audit trail of all remediation actions |
| `governance_filters` | Dynamic audit log filter configurations |

**Parameters**:
- `catalog`: Catalog name (default: `sjdatabricks`)
- `schema`: Schema name (default: `governance`)
- `drop_existing`: Whether to recreate tables (default: `true`)

---

#### 2. `02_load_workspace.py` - Workspace Configuration

**Purpose**: Loads workspace configuration into the governance system.

**When to Run**: Once per workspace to onboard it into governance.

**Configuration Fields**:
```python
{
    'workspace_id': '3592773542550038',
    'workspace_name': 'Production Workspace',
    'workspace_url': 'https://adb-xxx.azuredatabricks.net/',
    'enforcement_enabled': True,        # Enable/disable governance
    'notification_email': 'admin@example.com',
    'notification_slack_webhook': None,  # Optional Slack integration
    'enabled_object_types': ['notebook', 'query', 'dashboard', 'job', ...],
    'max_retry_attempts': 3,
    'created_by': 'admin'
}
```

**Parameters**:
- `catalog`: Catalog name
- `schema`: Schema name

---

#### 3. `03_approved_id.py` - Pre-Approved Identities

**Purpose**: Loads authorized identities (users, groups, service principals) who are permitted to make changes.

**When to Run**: Once initially, then whenever authorized identities change.

**Identity Configuration**:
```python
{
    "name": "admin@example.com",
    "type": "USER",                    # USER, GROUP, or SERVICE_PRINCIPAL
    "can_manage_resources": True,      # Can create/delete resources
    "can_manage_permissions": True,    # Can modify permissions
    "approved_actions": ["ALL"]        # Object types this identity can create
}
```

**Permission Flags Explained**:
- `can_manage_resources`: Allows creating/deleting jobs, pipelines, apps, experiments, monitors, etc.
- `can_manage_permissions`: Allows granting/revoking permissions (ACL changes)
- `approved_actions`: Specifies which object types the identity can create (granular control)

**Approved Actions Options**:
| Value | Description |
|-------|-------------|
| `["ALL"]` | Can create any object type (default, backward compatible) |
| `["table", "schema", "volume"]` | Can only create specific object types |
| `["UC_DATA_OBJECTS"]` | Group alias - expands to catalog, schema, table, volume, function |
| `["COMPUTE"]` | Group alias - expands to cluster, clusterPolicy, instancePool, warehouse |
| `["ML_AI"]` | Group alias - expands to ML/AI related objects |

**Group Aliases (Expand Automatically)**:
| Alias | Expands To |
|-------|------------|
| `ALL` | All object types (wildcard) |
| `UC_DATA_OBJECTS` | catalog, schema, table, volume, function, tableConstraint |
| `UC_SECURITY` | storageCredential, externalLocation, connection |
| `UC_ALL` | All Unity Catalog objects |
| `COMPUTE` | cluster, clusterPolicy, instancePool, warehouse |
| `ML_AI` | mlflowExperiments, servingEndpoint, registeredModel, featureSpec, featureTable, ucRegisteredModel, ucModelVersion |
| `DATA_SHARING` | share, recipient, provider |
| `DASHBOARDS_BI` | dashboard, genieSpace, alert, query |
| `ORCHESTRATION` | jobs, pipelines |
| `SECRETS` | secretScope |
| `VECTOR_SEARCH` | vectorSearchEndpoint, vectorIndex |
| `APPS` | apps |
| `MONITORING` | monitors |
| `CLEAN_ROOMS` | cleanRoom |

**Example Use Cases**:
```python
# Admin - can create anything
{"name": "admin@example.com", "type": "USER", "can_manage_resources": True, 
 "can_manage_permissions": True, "approved_actions": ["ALL"]}

# Data Engineer - can only create UC data objects (tables, schemas, volumes)
{"name": "data.engineer@example.com", "type": "USER", "can_manage_resources": True, 
 "can_manage_permissions": False, "approved_actions": ["UC_DATA_OBJECTS"]}

# ML Engineer - can only create ML/AI objects
{"name": "ml.engineer@example.com", "type": "USER", "can_manage_resources": True, 
 "can_manage_permissions": False, "approved_actions": ["ML_AI"]}

# Analyst - can only create dashboards and queries
{"name": "analyst@example.com", "type": "USER", "can_manage_resources": True, 
 "can_manage_permissions": False, "approved_actions": ["dashboard", "query", "alert"]}
```

**Supported Identity Types**:
- `USER`: Individual user emails
- `GROUP`: Databricks workspace groups (members are expanded automatically and inherit group's approved_actions)
- `SERVICE_PRINCIPAL`: Service principal application IDs (UUID format)

---

#### 4. `03a_load_filters.py` - Governance Filters

**Purpose**: Loads filter configurations that define how to extract object information from audit logs and what remediation actions to take.

**When to Run**: Once initially, then whenever filter rules need to be updated.

**Filter Structure**:
```python
{
    'filter_name': 'jobs_create',
    'service_name': 'jobs',
    'action_name': 'create',
    'object_type': 'jobs',
    'object_id_expr': 'get_json_object(response.result, "$.job_id")',
    'object_name_expr': 'request_params.name',
    'violation_type': 'UNAPPROVED_CREATION',
    'remediation_action': 'DELETE_RESOURCE',
    'is_active': True,
    'description': 'Job creation'
}
```

**Violation Types**:
| Type | Description |
|------|-------------|
| `UNAPPROVED_CREATION` | Unauthorized resource creation |
| `UNAUTHORIZED_PERMISSION_CHANGE` | Unauthorized ACL changes |
| `UNAUTHORIZED_DELETION` | Unauthorized resource deletion |

**Remediation Actions**:
| Action | Description |
|--------|-------------|
| `DELETE_RESOURCE` | Delete the unauthorized resource |
| `REVERT_PERMISSION` | Revert permissions to pre-approved state |
| `REPORT_DELETION` | Log for security reporting only (no action) |

---

#### 5. `04_discover.py` - Initial Discovery

**Purpose**: Discovers all existing resources in a workspace and catalogs them in the `governance_preapproved_objects` table.

**When to Run**: Once per workspace after initial setup to establish the baseline.

**Discoverable Object Types**:

| Category | Object Types |
|----------|--------------|
| **Workspace** | notebooks, directories, repos, files |
| **SQL** | queries, dashboards (Lakeview & Legacy), alerts, warehouses |
| **Compute** | clusters, cluster policies, instance pools, jobs, pipelines |
| **ML/AI** | MLflow experiments, serving endpoints, registered models, vector search endpoints |
| **Unity Catalog** | catalogs, schemas, tables, volumes, functions, connections, external locations, storage credentials |
| **Delta Sharing** | shares, recipients, providers |
| **Other** | secret scopes, apps, monitors, Genie spaces, clean rooms, metastores |

**Discovery Features**:
- Parallel processing with configurable `max_threads`
- Selective filtering for workspace objects
- Automatic permission fetching from appropriate APIs
- Exclusion of system catalogs (`__databricks_internal`, `samples`, `system`)
- Exclusion of system schemas (`information_schema`)
- Exclusion of system models (`system.ai.*`)

**Parameters**:
- `catalog`, `schema`: Target catalog/schema
- `workspace_id`, `workspace_url`: Workspace details
- `object_types`: Multiselect for types to discover
- `use_selective_filter`: Enable intelligent filtering (Y/N)
- `max_threads`: Parallel threads for discovery
- `debug_permissions`: Debug mode for permission fetching

---

### Workflow Notebooks (Daily Execution)

#### 6. `05_watcher.py` - Violation Detection (Workflow Step 1)

**Purpose**: Reads audit logs and detects violations by comparing against pre-approved objects and identities.

**Execution**: Runs on a schedule (typically every few hours or daily).

**Detection Logic**:

1. **Load Active Filters**: Reads from `governance_filters` table
2. **Expand Groups**: Resolves group memberships to individual users
3. **Query Audit Logs**: Uses dynamic SQL generated from filters
4. **Compare with Baseline**: Identifies objects not in pre-approved list
5. **Write Violations**: Stores detected violations in staging table

**Parameters**:
- `catalog`, `schema`: Configuration location
- `lookback_hours`: How far back to scan audit logs (default: 200 hours)

---

#### 7. `04a_sync_approved_changes.py` - Sync Approved Changes (Workflow Step 2)

**Purpose**: Monitors audit logs for changes made by pre-approved users and syncs those changes back to the baseline.

**Execution**: Runs after the watcher to keep the baseline current.

**Sync Operations**:
1. **Creation Sync**: Adds new objects created by approved users to pre-approved objects
2. **Permission Sync**: Updates permissions when approved users make changes

**Parameters**:
- `catalog`, `schema`: Configuration location
- `lookback_hours`: Audit log lookback period
- `sync_creations`: Enable/disable creation sync (Y/N)
- `sync_permissions`: Enable/disable permission sync (Y/N)

---

#### 8. `06_remediation.py` - Execute Remediation (Workflow Step 3)

**Purpose**: Processes violations from the staging table and executes appropriate remediation actions.

**Execution**: Runs after sync to take corrective actions.

**Remediation Process**:

1. **Read Pending Violations**: Gets violations with status `PENDING`
2. **Backup Resource**: Captures resource definition before action
3. **Execute Action**: Deletes resource or reverts permissions
4. **Update Status**: Records success/failure and details
5. **Create Audit Trail**: Logs action in `governance_control_actions`

**Supported Delete Operations**:
- Jobs, Clusters, Pipelines, Apps
- Dashboards (Lakeview & Legacy), Queries, Alerts
- MLflow Experiments, Serving Endpoints, Registered Models
- Secret Scopes, Vector Search Endpoints/Indexes
- Unity Catalog objects (catalogs, schemas, tables, volumes, functions, connections)
- Delta Sharing objects (shares, recipients, providers)
- Monitors, Genie Spaces, Clean Rooms

**Permission Reversion**:
- Workspace permissions: Uses `client.permissions.set()`
- Unity Catalog grants: Uses `client.grants.update()`
- Secret scope ACLs: Uses `client.secrets.put_acl()`

**Parameters**:
- `catalog`, `schema`: Configuration location
- `workspace_id`, `workspace_url`: Target workspace
- `dry_run`: Test mode without actual changes (true/false)
- `max_remediation_batch`: Limit on violations to process

---

#### 9. `07_validation.py` - Validation & Reporting (Workflow Step 4)

**Purpose**: Validates remediation results and generates compliance reports.

**Execution**: Final step in the workflow.

**Validation Checks**:
1. Verify resource deletion was successful
2. Confirm permission reversion matches expected state
3. Retry failed remediations with exponential backoff
4. Generate summary statistics

**Retry Logic**:
- Uses exponential backoff for failed operations
- Configurable maximum retry attempts per workspace
- Updates `last_retry_at` timestamp for tracking

---

## Workflow Configuration

### Databricks Workflow Definition

Create a Databricks Workflow with the following tasks:

```
Workflow: governance_daily_workflow
├── Task 1: 05_watcher
│   └── Parameters: lookback_hours=24
├── Task 2: 04a_sync_approved_changes (depends on Task 1)
│   └── Parameters: lookback_hours=24, sync_creations=Y, sync_permissions=Y
├── Task 3: 06_remediation (depends on Task 2)
│   └── Parameters: dry_run=false
└── Task 4: 07_validation (depends on Task 3)
```

### Recommended Schedule

| Scenario | Schedule | Lookback Hours |
|----------|----------|----------------|
| High Security | Every 4 hours | 6 |
| Standard | Daily | 26 |
| Low Security | Weekly | 170 |

---

## Pre-requisites & Setup

### Required Permissions

The service principal or user running the notebooks must have:

1. **Unity Catalog**: `CREATE`, `USAGE`, `SELECT`, `MODIFY` on the governance catalog/schema
2. **Audit Logs**: `SELECT` on `system.access.audit`
3. **Workspace**: `CAN_MANAGE` on resources for discovery and remediation
4. **Admin**: Workspace admin for group expansion and identity management

### SDK Requirements

```
databricks-sdk >= 0.20.0
```

### Initial Setup Steps

1. **Create Tables**: Run `01_setup_tables.py`
2. **Add Workspace**: Run `02_load_workspace.py` with workspace details
3. **Add Identities**: Run `03_approved_id.py` with authorized users
4. **Load Filters**: Run `03a_load_filters.py` to configure audit log filters
5. **Initial Discovery**: Run `04_discover.py` to catalog existing resources
6. **Create Workflow**: Set up the daily workflow with tasks 5-8
7. **Create Alert**: Configure Databricks alert for security notifications

---

## Object Types Supported

### Full Object Type Reference

| Object Type | Discovery | Remediation | Permission Type |
|-------------|-----------|-------------|-----------------|
| `notebook` | ✓ | ✓ | `notebooks` |
| `directory` | ✓ | ✓ | `directories` |
| `repo` | ✓ | ✓ | `repos` |
| `query` | ✓ | ✓ | `queries` |
| `dashboard` | ✓ | ✓ (Legacy) | `dbsql-dashboards` |
| `lakeview_dashboard` | ✓ | ✓ | `dashboards` |
| `alert` | ✓ | ✓ | `alerts` |
| `jobs` | ✓ | ✓ | `jobs` |
| `cluster` | ✓ | ✓ | `clusters` |
| `clusterPolicy` | ✓ | ✓ | `cluster-policies` |
| `instancePool` | ✓ | ✓ | `instance-pools` |
| `pipelines` | ✓ | ✓ | `pipelines` |
| `warehouse` | ✓ | ✓ | `warehouses` |
| `apps` | ✓ | ✓ | Apps API |
| `mlflowExperiments` | ✓ | ✓ | `experiments` |
| `servingEndpoint` | ✓ | ✓ | `serving-endpoints` |
| `registeredModel` | ✓ | ✓ | UC Grants (FUNCTION) |
| `secretScope` | ✓ | ✓ | Secrets API |
| `vectorSearchEndpoint` | ✓ | ✓ | `vector-search-endpoints` |
| `monitors` | ✓ | ✓ | UC Grants (TABLE) |
| `genieSpace` | ✓ | ✓ | `genie` |
| `catalog` | ✓ | ✓ | UC Grants (CATALOG) |
| `schema` | ✓ | ✓ | UC Grants (SCHEMA) |
| `table` | ✓ | ✓ | UC Grants (TABLE) |
| `volume` | ✓ | ✓ | UC Grants (VOLUME) |
| `function` | ✓ | ✓ | UC Grants (FUNCTION) |
| `connection` | ✓ | ✓ | UC Grants (CONNECTION) |
| `externalLocation` | ✓ | ✓ | UC Grants (EXTERNAL_LOCATION) |
| `storageCredential` | ✓ | ✓ | UC Grants (STORAGE_CREDENTIAL) |
| `share` | ✓ | ✓ | UC Grants (SHARE) |
| `recipient` | ✓ | ✓ | UC Grants (RECIPIENT) |
| `provider` | ✓ | ✓ | UC Grants (PROVIDER) |
| `metastore` | ✓ | ✓ | UC Grants (METASTORE) |
| `cleanRoom` | ✓ | ✓ | N/A |

---

## Edge Cases & Special Conditions

### Watcher Exclusions (05_watcher.py)

The watcher notebook contains several important exclusions to prevent false positives:

#### 1. Personal Workspace Exclusions

```sql
-- Exclude notebook/folder/repo deletions in personal workspace
AND NOT (
    service_name = 'notebook' 
    AND action_name IN ('deleteNotebook', 'deleteFolder', 'deleteRepo')
    AND request_params.path LIKE '/Workspace/Users/%'
)
```

**Reason**: Users should be able to manage their own personal workspace items.

#### 2. System Cluster Exclusions

```sql
-- Exclude job clusters
AND NOT NVL(request_params.acl_path_prefix,'x') LIKE '/clusters/jobs/%'

-- Exclude pipeline clusters
AND NOT NVL(request_params.acl_path_prefix,'x') LIKE '/clusters/pipelines/%'

-- Exclude serverless clusters
AND NOT (
    request_params.kind='SERVERLESS_SQL_WAREHOUSE' 
    AND request_params.cluster_creator='SQL_SERVICE'
    OR request_params.kind='SERVERLESS_PREVIEW' 
    AND request_params.cluster_creator='COMPUTE_GATEWAY_LAUNCHER'
    OR request_params.kind='SERVERLESS_REPL_VM' 
    AND request_params.cluster_creator='REPL_LAUNCHER'
)
```

**Reason**: These are system-managed clusters, not user-created.

#### 3. Cluster ACL Filtering

```sql
-- Only track ACL changes for user-created clusters
AND request_params.resourceId IN (
    SELECT DISTINCT cluster_id 
    FROM system.compute.clusters 
    WHERE cluster_source IN ('API','UI') 
    AND workspace_id IN (...)
)
```

**Reason**: Prevents flagging system cluster permission changes.

#### 4. DataFactory Job Owner Exclusion

```sql
-- Exclude owner assignments from Azure Data Factory
AND NOT (
    NVL(request_params.aclPermissionSet,'x') = 'Owner' 
    AND NVL(USER_AGENT,'x') = 'AzureDataFactory'
)
```

**Reason**: ADF commonly assigns job ownership during deployment.

#### 5. System User Exclusion

All queries filter out `System-User`:
```sql
AND user_identity.email != 'System-User'
```

#### 6. Unknown Object ID Exclusion

Events with null or 'unknown' object IDs are filtered:
```python
.filter((col("object_id") != "unknown") & (col("object_id").isNotNull()))
```

---

### Discovery Exclusions (04_discover.py)

#### 1. Catalog Exclusions

```python
EXCLUDED_CATALOGS = {'__databricks_internal', 'samples', 'system'}
```

**Reason**: These are system catalogs managed by Databricks.

#### 2. Schema Exclusions

```python
EXCLUDED_SCHEMAS = {'information_schema'}
```

**Reason**: Standard metadata schema, not user-managed.

#### 3. System Model Exclusions

```python
# Skip system models (e.g., system.ai.*)
if full_name.startswith('system.'):
    continue
```

**Reason**: Databricks-managed AI models.

#### 4. Foundation Model Endpoint Exclusions

```python
# Skip foundation model endpoints (databricks-* endpoints)
if endpoint_name.startswith('databricks-'):
    continue
```

**Reason**: Databricks-managed foundation model endpoints don't support permissions API.

#### 5. Workspace Object Filtering

When `use_selective_filter == "Y"`:
- `/Workspace/Repos/`: Permissions only for directories/repos, list files without permissions
- `/Workspace/Users/`: All objects with permissions
- `/Workspace/*`: All objects with permissions
- `.git` paths: Excluded entirely

---

### Remediation Special Cases (06_remediation.py)

#### 1. Principal Type Detection

The system intelligently detects principal types for permission handling:

```python
def _detect_principal_type(principal: str) -> str:
    # UUID pattern = Service Principal
    if re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-...$', principal):
        return 'service_principal'
    # Contains @ = User
    elif '@' in principal:
        return 'user'
    # Everything else = Group
    else:
        return 'group'
```

#### 2. Dashboard Type Handling

```python
# Legacy SQL dashboards use different deletion method
if object_type == 'dashboard':
    client.dashboards.delete(dashboard_id)
    
# Lakeview dashboards use trash
if object_type == 'lakeview_dashboard':
    client.lakeview.trash(dashboard_id)
```

#### 3. Unity Catalog SecurableType Handling

All UC grant operations use `.value` for the SecurableType:
```python
client.grants.get(
    securable_type=SecurableType.TABLE.value,
    full_name=full_name
)
```

---

## Remediation Actions

### DELETE_RESOURCE Actions

| Object Type | Deletion Method |
|-------------|-----------------|
| `jobs` | `client.jobs.delete(job_id)` |
| `cluster` | `client.clusters.permanent_delete(cluster_id)` |
| `pipelines` | `client.pipelines.delete(pipeline_id)` |
| `apps` | `client.apps.delete(app_name)` |
| `dashboard` | `client.dashboards.delete(dashboard_id)` |
| `lakeview_dashboard` | `client.lakeview.trash(dashboard_id)` |
| `query` | `client.queries.delete(query_id)` |
| `alert` | `client.alerts.delete(alert_id)` |
| `warehouse` | `client.warehouses.delete(warehouse_id)` |
| `clusterPolicy` | `client.cluster_policies.delete(policy_id)` |
| `instancePool` | `client.instance_pools.delete(pool_id)` |
| `mlflowExperiments` | `client.experiments.delete_experiment(experiment_id)` |
| `servingEndpoint` | `client.serving_endpoints.delete(name)` |
| `secretScope` | `client.secrets.delete_scope(scope)` |
| `vectorSearchEndpoint` | `client.vector_search_endpoints.delete_endpoint(name)` |
| `monitors` | `client.quality_monitors.delete(table_name)` |
| `catalog` | `client.catalogs.delete(name, force=True)` |
| `schema` | `client.schemas.delete(full_name, force=True)` |
| `table` | `client.tables.delete(full_name)` |
| `volume` | `client.volumes.delete(full_name)` |
| `function` | `client.functions.delete(full_name)` |
| `connection` | `client.connections.delete(name)` |
| `externalLocation` | `client.external_locations.delete(name)` |
| `storageCredential` | `client.storage_credentials.delete(name)` |
| `share` | `client.shares.delete(name)` |
| `recipient` | `client.recipients.delete(name)` |
| `provider` | `client.providers.delete(name)` |
| `genieSpace` | REST API: DELETE `/api/2.0/genie/spaces/{space_id}` |

### REVERT_PERMISSION Actions

| Permission Type | Reversion Method |
|-----------------|------------------|
| Workspace objects | `client.permissions.set(object_type, object_id, access_control_list)` |
| Unity Catalog | `client.grants.update(securable_type, full_name, changes)` |
| Secret Scopes | `client.secrets.put_acl(scope, principal, permission)` |
| Genie Spaces | `client.permissions.set("genie", space_id, access_control_list)` |

### REPORT_DELETION Actions

No automated action taken. The violation is recorded for:
- Security team review
- Compliance audit trails
- Incident investigation

---

## Alerting & Notifications

### Databricks SQL Alert Configuration

Create a Databricks SQL Alert to notify the security team:

**Query**:
```sql
SELECT 
    action_type,
    object_type,
    object_name,
    violator_email,
    remediation_status,
    remediation_details,
    created_at
FROM {catalog}.{schema}.governance_control_actions
WHERE created_at >= current_timestamp() - INTERVAL 24 HOUR
ORDER BY created_at DESC
```

**Alert Condition**: 
- Trigger when: `rows > 0`
- Refresh: Every 1 hour (or match workflow schedule)

**Notification Destination**:
- Email to security team distribution list
- Slack webhook for immediate notification

### Alert Content Should Include

1. **Summary Statistics**:
   - Total violations detected
   - Successful remediations
   - Failed remediations requiring manual review

2. **Detailed Actions**:
   - Object type and name
   - Violating user
   - Action taken (DELETE/REVERT/REPORT)
   - Status (SUCCESS/FAILED)

3. **Failed Actions**:
   - Error messages
   - Retry attempts remaining
   - Manual intervention required

---

## Troubleshooting

### Common Issues

#### 1. "Permission fetch failed" errors

**Cause**: Incorrect permission object type or object ID format.

**Solution**: 
- For serving endpoints: Use `endpoint.id`, not `endpoint.name`
- For vector search: Use `endpoint.id`, not `endpoint.name`
- For registered models: Use `SecurableType.FUNCTION.value`

#### 2. "Object not found" during remediation

**Cause**: Resource was already deleted or moved.

**Solution**: Mark as SUCCESS with note that resource no longer exists.

#### 3. High number of false positives

**Cause**: Missing exclusions or incorrect filter configuration.

**Solution**: 
- Review watcher edge cases
- Add appropriate exclusions to filters
- Verify approved identities list is complete

#### 4. Notebook code not updating

**Cause**: Databricks notebook caches Python code.

**Solution**: Restart Python environment:
```python
dbutils.library.restartPython()
```

#### 5. SDK version incompatibilities

**Cause**: Older SDK version missing APIs.

**Solution**: 
```python
%pip install -U databricks-sdk
%restart_python
```

### Debug Mode

Enable debug mode in discovery for verbose output:
```
debug_permissions = Y
```

This shows:
- Permission fetch attempts
- ACL contents
- Owner extraction logic

---

## Appendix

### A. Table Schemas

#### governance_preapproved_objects
```sql
CREATE TABLE governance_preapproved_objects (
    object_id STRING,
    workspace_id STRING,
    object_type STRING,
    object_name STRING,
    object_path STRING,
    owner_email STRING,
    permissions ARRAY<STRUCT<
        principal_email: STRING,
        principal_type: STRING,  -- 'user', 'group', 'service_principal'
        permission_level: STRING
    >>,
    metadata MAP<STRING, STRING>,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

#### governance_preapproved_identities
```sql
CREATE TABLE governance_preapproved_identities (
    identity_name STRING,
    identity_type STRING,  -- 'USER', 'GROUP', 'SERVICE_PRINCIPAL'
    display_name STRING,
    can_manage_resources BOOLEAN,
    can_manage_permissions BOOLEAN,
    approved_actions ARRAY<STRING>,  -- Object types this identity can create, e.g., ['table', 'schema'] or ['ALL']
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

**approved_actions Column Values**:
- `['ALL']`: Can create any object type (default for backward compatibility)
- `['table', 'schema', 'volume']`: Can only create specific object types listed
- Group aliases like `['UC_DATA_OBJECTS']` are expanded at load time to individual object types

### B. Filter Expression Examples

**Extract job_id from response**:
```sql
get_json_object(response.result, '$.job_id')
```

**Extract from request params**:
```sql
request_params.name
```

**Concatenate UC names**:
```sql
concat_ws('.', request_params.catalog_name, request_params.schema_name, request_params.name)
```

**Dynamic object type**:
```sql
CASE 
    WHEN request_params.aclChangeResourceName LIKE '%/notebooks/%' THEN 'notebook'
    WHEN request_params.aclChangeResourceName LIKE '%/dashboards/%' THEN 'dashboard'
    ELSE 'directory'
END
```

---

*Last Updated: December 2024*
*Version: 1.0*

