# mx_scripts — Project Description (Agent Reference)

This document describes the **mx_scripts** codebase for AI agents and developers. Use it to understand scope, concepts, and structure before making changes. For full product docs see `../DOCUMENTATION.md` and `../QUICK_REFERENCE.md`.

---

## 1. Purpose and Scope

**mx_scripts** is the notebook suite for **GovernBot** (Databricks Governance Automation). It:

- **Monitors** Databricks workspaces for unauthorized changes using `system.access.audit`.
- **Maintains a baseline** of pre-approved objects and identities.
- **Detects violations** (unauthorized creations, permission changes, deletions).
- **Syncs** approved-user changes back into the baseline.
- **Remediates** by deleting unauthorized resources or reverting permissions.
- **Validates** and reports for compliance.

All notebooks are **Databricks notebooks** (`.py` with `# Databricks notebook source` and `# MAGIC %md` / `# COMMAND ----------`). They run in a single governance catalog/schema and support **Azure (client-secret or PAT)** authentication.

---

## 2. Key Concepts

| Concept | Meaning |
|--------|---------|
| **Pre-approved objects** | Resources (and their permissions) that are allowed — baseline from discovery or approved-user changes. |
| **Pre-approved identities** | Users, groups, or service principals allowed to create resources or change permissions. |
| **Violation** | An action in audit logs that is not allowed (e.g. creation by non-approved user, unauthorized permission change). |
| **Remediation** | Corrective action: `DELETE_RESOURCE`, `REVERT_PERMISSION`, or `REPORT_DELETION`. |
| **Catalog / schema** | Governance tables live in one catalog and one schema (e.g. `qadl.sch_mng_admon` or `dlprod.sch_mng_admon`). |

---

## 3. Tables (Governance State)

All in `{catalog}.{schema}` (e.g. `qadl.sch_mng_admon` or `dlprod.sch_mng_admon`):

| Table | Role |
|-------|------|
| `governance_config_workspaces` | Workspace list, enforcement flag, notification settings, warehouse_id. |
| `governance_preapproved_objects` | Baseline: object_id, workspace_id, object_type, permissions, etc. |
| `governance_preapproved_identities` | Approved users/groups/service principals and their capabilities. |
| `governance_filters` | Audit-log filter rules (service_name, action_name, object_type, remediation_action). |
| `governance_violations_staging` | Violations detected by the watcher; status (e.g. PENDING) for remediation. |
| `governance_control_actions` | Audit trail of remediation actions (what was done, by whom, when). |

---

## 4. Notebooks and Responsibilities

### One-time setup (order matters)

| Notebook | Responsibility |
|----------|----------------|
| `01_setup_tables.py` | Creates all governance Delta tables; optional drop_existing. |
| `02_load_workspace.py` | Inserts/updates workspace config in `governance_config_workspaces`. |
| `03_approved_id.py` | Loads pre-approved identities into `governance_preapproved_identities`. |
| `03a_load_filters.py` | Loads filter rules into `governance_filters`. |
| `04_discover.py` | Initial discovery: catalogs existing workspace/UC/etc. resources into `governance_preapproved_objects`. |

### Scheduled workflow (run in order)

| Notebook | Responsibility |
|----------|----------------|
| `05_watcher.py` | Reads audit logs, detects violations, writes to `governance_violations_staging`. |
| `04a_sync_approved_changes.py` | Syncs creations/permissions/entitlements by approved users into `governance_preapproved_objects`. |
| `06_remediation.py` | Processes staging violations: DELETE_RESOURCE / REVERT_PERMISSION / REPORT_DELETION; writes to `governance_control_actions`. |
| `07_validation.py` | Validates remediation outcomes, retries, reporting. |

### Utility

| Notebook | Responsibility |
|----------|----------------|
| `create_workflow.py` | Defines the Databricks workflow (tasks, dependencies, notebook paths). |

---

## 5. Data and Control Flow

- **Inputs:** `system.access.audit`, Databricks SDK (Workspace + Account APIs), governance tables.
- **Outputs:** Governance tables updated; workflow notebooks exit with JSON (status, counts, errors).
- **Auth:** Azure client-secret or PAT; secrets from Databricks secret scopes (Key Vault–backed); `workspace_id` determines catalog and scope names (QA vs prod).
- **Environment:** Catalog/schema and KV scope are chosen by `workspaceId` (e.g. `4126527463676543` → QA/catalog `qadl`; else prod/catalog `dlprod`). Timezone used: `America/Mexico_City`.

---

## 6. Technology and Conventions

- **Runtime:** Databricks (notebooks); PySpark and Python 3.
- **SDK:** `databricks-sdk` (WorkspaceClient, AccountClient where needed).
- **Parameters:** Many widgets are commented out; catalog/schema/lookback etc. are often set from `workspaceId` or defaults. Active widgets include `auth_type`, `load_filters`, `enable_discover`, `dry_run` (remediation), etc.
- **Exits:** Workflow notebooks use `dbutils.notebook.exit(json.dumps({...}))` with status, reason, counts, and timestamp for the orchestrator.

---

## 7. Where to Look for More

- **Architecture and flows:** `../DOCUMENTATION.md` (architecture, data flow, object types, edge cases).
- **Quick lookup:** `../QUICK_REFERENCE.md` (notebook order, violation types, exclusions, tables).
- **Implementation rules:** `IMPLEMENTATION.md` in this folder (how to change or add code in mx_scripts).

Agents should read **PROJECT_DESCRIPTION.md** (this file) for context and **IMPLEMENTATION.md** before editing any mx_scripts code.
