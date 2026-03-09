# mx_scripts — Implementation Guide for Agents

This document defines **how to implement or change code** in mx_scripts. Agents must follow these rules when adding, modifying, or refactoring notebooks.

---

## 1. Before You Start

1. **Read** `PROJECT_DESCRIPTION.md` for scope, concepts, tables, and notebook roles.
2. **Use** `../DOCUMENTATION.md` and `../QUICK_REFERENCE.md` for product behavior, object types, and edge cases.
3. **Preserve** existing behavior unless the user explicitly requests a change; follow current patterns.

---

## 2. Notebook Format and Structure

- **First line:** `# Databricks notebook source`
- **Sections:** Use `# MAGIC %md` for markdown and `# COMMAND ----------` to separate runnable cells.
- **Docstring:** Start with a short `# MAGIC %md` title and purpose; document parameters and main behavior in markdown cells.
- **Order:** Parameters → config resolution → imports → main logic. Keep parameter and config blocks at the top.

---

## 3. Parameters and Configuration

- **Widgets:** Prefer reusing existing widget names and semantics so workflows keep working.
  - `auth_type`: `"azure-client-secret"` or `"pat"` (often the only active widget in workflow notebooks).
  - `load_filters`, `enable_discover`, `load_approved_id`: `"Y"` / `"N"` / `"S"`; treat `"Y"` and `"S"` as true when a notebook can be skipped (e.g. run discovery or load filters instead).
  - `dry_run`: remediation only; `"true"` / `"false"`.
  - `lookback_hours`, `account_id`, etc.: many are passed by workflow; widgets may be commented out and values set from `workspaceId` or defaults.
- **Resolution:** Use try/except around `dbutils.widgets.get(...)` and fall back to safe defaults (e.g. `auth_type = "azure-client-secret"`, `lookback_hours = 24`). Do not assume widgets exist when notebooks are run from workflows.
- **Catalog and schema:** Derived from `get_context().workspaceId` (e.g. QA workspace → `qadl` / `sch_mng_admon`; prod → `dlprod` / `sch_mng_admon`). Do not introduce new catalog/schema names without aligning with existing env logic.

---

## 4. Authentication and Workspace Context

- **Context:** Use `from dbruntime.databricks_repl_context import get_context` and `get_context().workspaceId` for workspace-specific config.
- **Secrets:** Use `dbutils.secrets.get(scope=kv_scope, key=kv_*_key)`. Do not hardcode credentials. Scope and key names are set per workspace (e.g. `kv_scope`, `kv_client_id_key`, `kv_client_secret_key`, `kv_tenant_id_key` for Azure; or PAT keys).
- **Clients:** Use `WorkspaceClient` (and `AccountClient` only when account-level APIs are needed). Support both `azure-client-secret` and `pat` via the existing pattern (see `04a_sync_approved_changes.py` or `05_watcher.py`).

---

## 5. Early Exits (Workflow Notebooks)

- When the notebook should do nothing (e.g. load_filters/enable_discover set, no enabled workspaces), call `dbutils.notebook.exit(...)` with a **JSON string** so the workflow can parse it.
- **Contract:** Exit payload must be valid JSON and include at least:
  - `status`: e.g. `"SKIPPED"`, `"SUCCESS"`, `"FAILED"`
  - `reason` or message when status is not SUCCESS
  - Counts or details when relevant (e.g. `violations_detected`, `remediated_count`)
  - `timestamp` in ISO format (use `datetime.now(tz).isoformat()` with project timezone).
- Prefer `json.dumps({...})` for complex payloads; keep string payloads valid JSON (e.g. `'{"status": "SKIPPED", "reason": "..."}'`).

---

## 6. Error Handling and Logging

- **Widgets and config:** Use try/except with sensible defaults; avoid failing the whole run on a missing optional widget.
- **External calls:** Catch exceptions around SDK/API and Spark operations; log or record errors and, where appropriate, set violation/action status to failure and continue or exit with status `FAILED` and reason.
- **No silent swallows:** Prefer explicit handling (log, return, exit with reason) over bare `except: pass`.

---

## 7. Table and Schema Contracts

- **Writes:** Only write to the governance tables defined in `PROJECT_DESCRIPTION.md`. Use the existing column names and types (see `../DOCUMENTATION.md` appendix for schemas).
- **New columns:** Do not add columns to existing tables without aligning with `01_setup_tables.py` and all readers/writers of that table.
- **Object types and filters:** When adding support for a new object type or audit event, follow the existing filter and remediation patterns; update `03a_load_filters` and remediation logic consistently and document in `../DOCUMENTATION.md` if required.

---

## 8. Code Style and Dependencies

- **Imports:** Place after parameter/config blocks. Use `databricks-sdk`, PySpark, `pytz`, `json`, `datetime` as in existing notebooks; add new dependencies only when needed and document in the notebook or README.
- **Naming:** Use snake_case for variables and functions; align with existing names (e.g. `workspace_id`, `object_type`, `preapproved_objects`).
- **Comments:** Comment non-obvious business rules (e.g. exclusions, principal-type detection, dashboard vs lakeview_dashboard handling).

---

## 9. Testing and Validation

- **Workflows:** After changing a workflow notebook, ensure it still fits the job definition in `create_workflow.py` (task order, parameters, notebook paths).
- **Exits:** Verify exit JSON is parseable and contains the expected keys for the orchestrator.
- **Backward compatibility:** Do not remove or rename widgets/parameters that the workflow or other notebooks pass; extend instead.

---

## 10. Checklist Before Submitting Changes

- [ ] PROJECT_DESCRIPTION.md and IMPLEMENTATION.md read; change aligns with project scope and tables.
- [ ] Notebook format (Databricks notebook source, COMMAND cells, %md) preserved.
- [ ] Parameters and auth follow existing widget and workspace-based config pattern.
- [ ] Early exits return valid JSON with status/reason/timestamp (and counts where applicable).
- [ ] No new hardcoded secrets; Key Vault scope/keys used as in existing notebooks.
- [ ] Table writes use existing schema; no unintended schema drift.
- [ ] Error handling is explicit (no silent swallow); failures are reflected in exit status or staging/control tables.
- [ ] If adding object types or filters, DOCUMENTATION.md and QUICK_REFERENCE.md updated as needed.

---

Agents must follow this implementation guide when editing any file under mx_scripts. When in doubt, mimic the patterns in `04a_sync_approved_changes.py`, `05_watcher.py`, and `06_remediation.py` for parameters, auth, and exit contracts.
