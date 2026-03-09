# GovernBot Reusable Design — Project Description

**Audience:** Humans and AI agents. Use this document to understand scope, concepts, and structure before making changes or answering questions about the project.

---

## 1. Purpose and Scope

This repository provides a **reusable governance automation system** for Databricks workspaces (GovernBot). It consists of:

- **governbot_core** — A Python package (installable wheel) that implements config, client factory, filters, identities, audit query builders, permissions helpers, and remediation logic. No hardcoded workspace IDs or catalog names; all environment-specific values come from environment variables.
- **Notebooks** — Thin Databricks notebooks that use the package and optionally widget overrides. They run setup, discovery, sync, watcher, remediation, and validation.

The system:

- **Monitors** workspaces using `system.access.audit`.
- **Maintains a baseline** of pre-approved objects and identities in Unity Catalog Delta tables.
- **Detects violations** (unauthorized creations, permission changes, deletions, entitlement/object changes).
- **Syncs** approved-user changes into the baseline (04a).
- **Remediates** by deleting unauthorized resources, reverting permissions, or reporting (06).
- **Validates** and reports for compliance (07).

**Relationship to governBot/mx_scripts:** The `governBot/mx_scripts` folder contains the original notebook suite with inline logic and workspace-specific config. The GovernBot package and notebooks are a refactor for portability: config from env, shared logic in the wheel, parity with mx_scripts behavior where applicable.

---

## 2. Key Concepts

| Concept | Meaning |
|--------|---------|
| **Pre-approved objects** | Resources (and their permissions) that are allowed — populated by discovery or by syncing approved-user creations. Stored in `governance_preapproved_objects`. |
| **Pre-approved identities** | Users, groups, or service principals allowed to create resources or change permissions. Stored in `governance_preapproved_identities`. |
| **Violation** | An audit event that is not allowed (e.g. creation by non-approved user, unauthorized permission change). Written to `governance_violations_staging`. |
| **Remediation** | Corrective action: `DELETE_RESOURCE`, `REVERT_PERMISSION`, `REPORT_DELETION`, `REPORT_OBJECT_UPDATE`, `ALERT_ENTITLEMENT_CHANGE`, etc. Recorded in `governance_control_actions`. |
| **Catalog / schema** | Governance tables live in one Unity Catalog catalog and one schema (e.g. `my_catalog.sch_mng_admon`). Set via `GOVERNANCE_CATALOG` and `GOVERNANCE_SCHEMA`. |

---

## 3. Governance Tables

All tables live in `{catalog}.{schema}` (from `GovernanceConfig.from_env()`).

| Table | Role |
|-------|------|
| `governance_config_workspaces` | Workspace list, enforcement flag, notification settings, warehouse_id, enabled_object_types. |
| `governance_preapproved_objects` | Baseline: object_id, workspace_id, object_type, object_name, permissions, is_active, metadata. |
| `governance_preapproved_identities` | Approved users/groups/service principals; can_manage_resources, can_manage_permissions, approved_actions. |
| `governance_filters` | Audit-log filter rules: service_name, action_name, object_type, object_id_expr, object_name_expr, violation_type, remediation_action. |
| `governance_violations_staging` | Violations detected by the watcher; processing_status (PENDING, PENDING_REPORT, COMPLETED). |
| `governance_control_actions` | Audit trail of remediation: action_id, violation_id, remediation_status, backup_definition, error_message. |

---

## 4. Package Layout (governbot_core)

```
GovernBot/
├── pyproject.toml
├── src/governbot_core/
│   ├── __init__.py          # Public API: Config, ClientFactory, load_filters, execute_remediation, etc.
│   ├── config.py            # GovernanceConfig.from_env()
│   ├── constants.py         # Table names, violation types, remediation actions
│   ├── clients.py           # ClientFactory (workspace + account)
│   ├── filters.py           # load_filters(spark, catalog, schema)
│   ├── identities.py        # load_preapproved_identities, parse_identity_lists, expand_group_members
│   ├── audit_queries.py     # build_audit_query_from_filters, build_approved_user_query
│   ├── object_types.py     # DELETE_HANDLERS registry, get_delete_handler
│   ├── permissions.py      # detect_principal_type, build_access_control_request
│   ├── remediation.py      # delete_* handlers, get_resource_definition, revert_permissions, execute_remediation
│   ├── discovery_impl.py   # Discovery logic (run_discovery; used by 02_one_time_discover)
│   ├── sync_impl.py        # Permission fetch, entitlement/budget helpers (used by 04a)
│   └── governance_filter_definitions.py  # Default filter set (get_governance_filter_definitions)
├── notebooks/
│   ├── 01_setup_environment.py   # Tables + workspace + identities + filters (combined setup)
│   ├── 02_one_time_discover.py   # Initial discovery into preapproved_objects
│   ├── 04a_sync_approved_changes.py  # Sync creations/permissions/deletions/entitlements/object changes
│   ├── 05_watcher.py              # Detect violations → violations_staging
│   ├── 06_remediation.py          # Process staging → execute_remediation → control_actions
│   └── 07_validation.py           # Validate and report
├── PROJECT_DESCRIPTION.md   # This file
├── IMPLEMENTATION.md
├── DEPLOYMENT.md
└── README.md
```

---

## 5. Notebook Roles and Order

### One-time setup

| Notebook | Responsibility |
|----------|----------------|
| `01_setup_environment.py` | Creates all Delta tables; loads workspace config, approved identities, and governance filters. Single entry point for environment setup. |
| `02_one_time_discover.py` | Initial discovery: scans workspaces and writes existing resources into `governance_preapproved_objects`. |

### Scheduled workflow (run in order)

| Notebook | Responsibility |
|----------|----------------|
| `05_watcher.py` | Reads audit logs (build_audit_query_from_filters), detects violations (events not from approved identities), writes to `governance_violations_staging`. |
| `04a_sync_approved_changes.py` | Syncs creations/permissions/deletions/entitlements/object changes by approved users into `governance_preapproved_objects`; serverless budget policies; control_actions MERGE for inactive objects. |
| `06_remediation.py` | Reads pending violations, calls `execute_remediation` per row, writes to `governance_control_actions`, updates staging status. |
| `07_validation.py` | Validates remediation outcomes and exits with JSON (counts, validation_issues). |

---

## 6. Data and Control Flow

- **Inputs:** `system.access.audit`, Databricks SDK (Workspace + Account APIs where needed), governance tables.
- **Outputs:** Governance tables updated; notebooks exit with JSON (`status`, counts, `timestamp`) for orchestrators.
- **Config:** `GovernanceConfig.from_env()`; secrets via a `get_secret(scope, key)` callback (e.g. `dbutils.secrets.get` in Databricks). No catalog/schema or secrets in code.
- **Auth:** Azure client-secret or PAT; keys and scope names come from config/env.

---

## 7. Technology and Conventions

- **Runtime:** Databricks (notebooks); PySpark and Python 3.9+.
- **SDK:** `databricks-sdk` (WorkspaceClient, AccountClient for account-level APIs).
- **Parameters:** Widgets for overrides (e.g. `lookback_hours`, `dry_run`, `sync_creations`); env for catalog, schema, KV scope/keys. Notebooks use try/except on widgets and fall back to defaults.
- **Exits:** Workflow notebooks use `dbutils.notebook.exit(json.dumps({...}))` with `status`, optional `reason`, counts, and `timestamp`.

---

## 8. Where to Look Next

- **Implementation rules and code conventions:** [IMPLEMENTATION.md](IMPLEMENTATION.md).
- **Build, install, env, and running jobs:** [DEPLOYMENT.md](DEPLOYMENT.md).
- **Package usage and config:** [README.md](README.md).
- **Original mx_scripts semantics:** [governBot/mx_scripts/PROJECT_DESCRIPTION.md](../governBot/mx_scripts/PROJECT_DESCRIPTION.md) and [governBot/DOCUMENTATION.md](../governBot/DOCUMENTATION.md).

Agents should read this file for context and IMPLEMENTATION.md before editing code.
