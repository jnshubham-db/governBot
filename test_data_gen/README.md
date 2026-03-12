# GovernBot Test Data Generator

Creates real assets in a Databricks workspace using two service principals (allowed + unapproved) to generate authentic audit log entries for testing GovernBot's three core workflows:

1. **sync_approved_changes** - syncs approved user actions into baseline
2. **watcher** - detects violations from unapproved users
3. **remediation** - auto-deletes/reverts unauthorized changes

## Prerequisites

- Databricks CLI configured with profile `e2-demo-west`
- Secrets stored in scope `karthik_test_datagen`:
  - `allowed_sp_client_id`, `allowed_sp_secret`
  - `unallowed_sp_client_id`, `unallowed_sp_secret`
- Both SPs must have workspace access to `fe-sandbox-classic-sandbox-kj1pbc`
- Python environment with `databricks-sdk` installed

## Usage

```bash
# Full run - creates all assets, records to tracking table, auto-cleans up
python test_data_gen/generate_test_data.py

# Leave resources for inspection (recommended for first run)
python test_data_gen/generate_test_data.py --no-cleanup --verbose

# Only generate specific categories
python test_data_gen/generate_test_data.py --categories uc_data compute workspace_extended

# Dry run - shows what would happen
python test_data_gen/generate_test_data.py --dry-run

# Clean up leftover gbot_test_* resources from previous runs
python test_data_gen/generate_test_data.py --cleanup-only

# Custom warehouse/catalog
python test_data_gen/generate_test_data.py --warehouse-id abc123 --catalog my_catalog --schema my_schema
```

## CLI Reference

| Flag | Description |
|------|-------------|
| `--dry-run` | Print what would be done without executing |
| `--cleanup-only` | Scan and delete all `gbot_test_*` resources |
| `--no-cleanup` | Leave test resources for inspection |
| `--categories` | Space-separated list (see Categories below) |
| `--warehouse-id` | SQL warehouse ID (auto-detected if not set) |
| `--catalog` | Catalog for tracking table (default: `main`) |
| `--schema` | Schema for tracking table (default: `govern_bot`) |
| `--verbose` | Extra output |

## Categories

| Category | Description |
|----------|-------------|
| `uc_data` | Unity Catalog objects: catalogs, schemas, tables, volumes, functions, UC registered models |
| `compute` | Clusters, cluster policies, instance pools, warehouses |
| `workspace` | Notebooks, dashboards, jobs, pipelines, experiments, secret scopes, queries |
| `ml_ai` | Serving endpoints, vector search endpoints, workspace registered models |
| `admin_security` | Groups, entitlements |
| `workspace_extended` | Apps, alerts, genie spaces, files, folders, secrets, additional workspace resources |
| `uc_data_extended` | Table constraints, UC model versions, object updates (rename/comment), UC grants, connections, shares, recipients, providers |
| `acl_extended` | Dashboard ACL/clone, query ACL, serving endpoint ACL, registered model ACL, VS endpoint ACL, warehouse ACL |
| `admin_extended` | Group lifecycle (create + delete), monitors |
| `ml_ai_extended` | Workspace registered model rename, monitor create/delete |

## Operations Per Asset (6 Phases)

For each asset type, the script runs:

| Phase | Actor | Action | Expected GovernBot Result |
|-------|-------|--------|--------------------------|
| 1 | Allowed SP | Create | sync_approved_changes syncs to baseline |
| 2 | Unapproved SP | Create | watcher detects `UNAPPROVED_CREATION` |
| 3 | Unapproved SP | Change permissions | watcher detects `UNAUTHORIZED_PERMISSION_CHANGE` |
| 4 | Unapproved SP | Delete own asset | watcher detects `UNAUTHORIZED_DELETION` |
| 5 | Allowed SP | Change permissions | sync_approved_changes syncs permission update |
| 6 | Allowed SP | Delete own asset | sync_approved_changes syncs deletion |

## Coverage Progress

**Last updated: 2026-03-12** | **Latest run IDs: `43e42755`, `eb0db69d`** | **Tracking table: `main.govern_bot.test_data_gen_operations`**

> **Note:** The tracking table records operations from the generator script. However, the `--cleanup-only` phase also deletes resources via real API calls, which generate authentic audit log entries even though they are not recorded in the tracking table. The coverage below accounts for both sources.

### Summary

| Metric | Count |
|--------|-------|
| Total governance filter combinations | 109 |
| Covered (tracked + cleanup audit logs) | 75 / 109 (69%) |
| Feasible remaining | 2 (workspace limitation) |
| Blocked / infeasible | 32 |

### Generated — Tracked in Table (85 combos) + Cleanup Audit Logs

| Asset Type | CREATE | DELETE | CHANGE_PERMISSION | UPDATE | CLONE | DELETE_ACL |
|------------|:------:|:------:|:-----------------:|:------:|:-----:|:----------:|
| alert | Y | Y | | | | |
| apps | Y | Y* | Y | | | |
| catalog | | | Y | Y | | |
| cleanRoom | Y | | | | | |
| cluster | Y | Y | Y | | | |
| clusterPolicy | Y | Y | Y | | | |
| connection | Y | Y | | | | |
| dashboard | Y | Y | Y | | Y | |
| directory | | | Y | | | |
| file | Y | Y | | | | |
| files (workspace) | | Y* | | | | |
| folder | Y | Y | | | | |
| function | Y | Y | | | | |
| genieSpace | Y | Y | | | | |
| groups | Y | Y | | | | |
| instancePool | Y | Y | Y | | | |
| jobs | Y | Y | Y | | | |
| mlflowExperiments | Y | Y | Y | | | |
| monitors | Y | Y | | | | |
| notebook | Y | Y | Y | | | |
| pipelines | Y | Y | Y | | | |
| provider | Y | Y | | | | |
| query | Y | Y | Y | | | |
| recipient | Y | Y | | | | |
| registeredModel | Y | Y | Y | Y | | |
| schema | Y | Y* | Y | Y | | |
| secret | Y | Y | | | | |
| secretScope | Y | Y | Y | | | Y |
| servingEndpoint | Y | Y | Y | | | |
| share | Y | Y | | | | |
| table | Y | Y* | | Y | | |
| tableConstraint | Y | Y | | | | |
| ucRegisteredModel | Y | | | Y | | |
| vectorSearchEndpoint | Y | Y* | Y | | | |
| volume | Y | Y | Y | | | |
| warehouse | Y | Y | Y | | | |

`Y*` = Not in tracking table but audit log generated via cleanup (`--cleanup-only`) which successfully deleted the resource.

- **apps DELETE**: Script recorded FAILED (20min cooldown), but cleanup later deleted 10 apps successfully
- **vectorSearchEndpoint DELETE**: Script recorded FAILED, but cleanup deleted 6 VS endpoints successfully
- **schema DELETE**: Schemas deleted implicitly during `uc_data` generator cascade drops and cleanup
- **table DELETE**: Tables deleted implicitly during `uc_data` generator cascade drops and cleanup
- **files DELETE**: Files deleted during cleanup via `workspace.delete()` which triggers `workspace.fileDelete` audit action
- **vectorSearchEndpoint CHANGE_PERMISSION**: Fixed in run `eb0db69d` — requires using endpoint **ID** (not name) with `permissions.update`

### Feasible Remaining (2 items — blocked by workspace limitation)

| Asset Type | Operation | Filter Name | Notes |
|------------|-----------|-------------|-------|
| `catalog` | CREATE | `uc_catalog_create` | Workspace has Default Storage enabled — requires UI-only catalog creation. Tested SDK, SQL, and SQL with MANAGED LOCATION — all fail. |
| `catalog` | DELETE | `uc_catalog_delete` | Depends on catalog CREATE succeeding first |

### Blocked / Infeasible (32 items)

#### Infeasible — requires account admin or cloud infrastructure

| Asset Type | Operation | Reason |
|------------|-----------|--------|
| `metastore` | DELETE | Account-admin-only operation |
| `metastoreAssignment` | DELETE | Account-admin-only operation |
| `externalLocation` | DELETE | Requires cloud storage URL + storage credential |
| `storageCredential` | DELETE | Requires cloud IAM role ARN |
| `credential` | DELETE | Requires cloud credential setup |

#### Needs special runtime or prerequisites

| Asset Type | Operation | Reason |
|------------|-----------|--------|
| `featureSpec` | CREATE | Feature Store runtime required |
| `featureTable` | CREATE / DELETE / ACL | Feature Store runtime required |
| `vectorIndex` | CREATE / DELETE | Requires running VS endpoint + Delta table source |
| `dataVectorIndex` | DELETE | Requires data vector index |
| `ucModelVersion` | CREATE | Requires MLflow model logging runtime |
| `modelVersion` | DELETE | Requires MLflow model version |
| `abacPolicy` | CREATE / DELETE | Requires ABAC preview feature |
| `dashboardSchedule` | DELETE | Requires dashboard with active schedule |
| `dashboardSubscription` | DELETE | Requires dashboard with subscription |
| `jobRun` | DELETE | Requires a completed job run |
| `repo` | DELETE | Requires git credentials configured |

#### Sensitive — affects real identities or workspace security

| Asset Type | Operation | Reason |
|------------|-----------|--------|
| `users_or_sp` | CREATE | `accounts.add` — adds real user/SP to workspace |
| `user` | DELETE | Deletes real user |
| `identity_replace` | ADMIN_DELETE / ACL_CHANGE | Workspace user deletion, grant changes |
| `tokensAcls` | ACL_CHANGE | Workspace token grant changes |
| `users` | ADMIN_SET / ADMIN_REMOVE | Admin grant changes |
| `any_file_permissions` | GRANT / REVOKE | Workspace-level any-file ACL |

#### Partially covered by existing data

| Asset Type | Operation | Notes |
|------------|-----------|-------|
| `workspace_acl` | CHANGE_PERMISSION | Umbrella filter — individual types (notebook, dashboard, query, experiment, directory) already covered |
| `uc_grants` | CHANGE_PERMISSION | Umbrella filter — schema, catalog, volume grants already covered |

## Tracking Table

All operations are recorded in `main.govern_bot.test_data_gen_operations`:

| Column | Description |
|--------|-------------|
| `operation_id` | UUID per operation |
| `timestamp` | When executed |
| `run_id` | Shared across all ops in one script run |
| `sp_role` | `allowed` or `unapproved` |
| `sp_client_id` | Service principal client ID |
| `asset_category` | Category name (see Categories above) |
| `asset_type` | e.g., `table`, `cluster`, `jobs` |
| `operation` | `CREATE`, `DELETE`, `CHANGE_PERMISSION`, `UPDATE`, `CLONE`, `DELETE_ACL` |
| `asset_name` | Full name/path |
| `asset_id` | ID returned from creation |
| `status` | `SUCCESS`, `FAILED`, `SKIPPED` |
| `error_message` | Error details if failed |
| `expected_violation_type` | Expected GovernBot violation type |
| `expected_remediation` | Expected remediation action |

### Query Examples

```sql
-- All operations from a specific run
SELECT * FROM main.govern_bot.test_data_gen_operations
WHERE run_id = '43e42755'
ORDER BY timestamp;

-- Summary by status
SELECT asset_category, asset_type, operation, status, COUNT(*) as cnt
FROM main.govern_bot.test_data_gen_operations
GROUP BY ALL
ORDER BY asset_category, asset_type, operation;

-- Coverage: distinct successful asset_type + operation
SELECT DISTINCT asset_type, operation
FROM main.govern_bot.test_data_gen_operations
WHERE status = 'SUCCESS'
ORDER BY asset_type, operation;

-- Failed operations only
SELECT asset_type, operation, error_message
FROM main.govern_bot.test_data_gen_operations
WHERE status = 'FAILED'
ORDER BY asset_type, operation;
```

## Naming Convention

All resources: `gbot_test_{YYYYMMDD_HHMMSS}_{asset_type}_{sp_role}`

Example: `gbot_test_20260311_143022_cluster_allowed`

## Key Implementation Details

- **Sentinel pattern**: `safe_execute()` uses a `_SENTINEL_FAIL` object to distinguish void SDK calls (return `None`) from actual failures. The `succeeded()` helper checks results.
- **Cleanup**: `--cleanup-only` scans for all `gbot_test_*` prefixed resources across workspace, UC, compute, apps, alerts, VS endpoints, and secret scopes.
- **UC objects use `main` catalog**: The test workspace doesn't support standalone catalog creation via API, so all UC test objects are created under `main.govern_bot_test_*` schemas.

## Verification

1. Run: `python test_data_gen/generate_test_data.py --no-cleanup --verbose`
2. Check script output for success/failure counts
3. Query tracking table to cross-reference with audit logs
4. Run GovernBot watcher notebook to verify violations are detected
5. Clean up: `python test_data_gen/generate_test_data.py --cleanup-only`
