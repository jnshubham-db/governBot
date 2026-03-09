# GovernBot Reusable Design — Implementation Guide

**Audience:** Humans and AI agents. Follow these rules when adding, modifying, or refactoring code in the package or notebooks.

---

## 1. Before You Start

1. **Read** [PROJECT_DESCRIPTION.md](PROJECT_DESCRIPTION.md) for scope, concepts, tables, and notebook roles.
2. **Preserve** existing behavior unless the user explicitly requests a change; follow current patterns in the codebase.
3. **Do not** hardcode catalog, schema, workspace IDs, or secret values; use `GovernanceConfig.from_env()` and the provided `get_secret` callback.

---

## 2. Package (governbot_core) Conventions

### Module layout

- **config.py** — Only reads `os.environ` (and optional `.env` via python-dotenv). No `dbutils` or Spark.
- **clients.py** — `ClientFactory` takes `(config, get_secret)`. `get_secret(scope, key)` is supplied by the caller (e.g. notebook with `dbutils.secrets.get`).
- **filters.py, identities.py, audit_queries.py** — Accept `spark`, `catalog`, `schema` (or filter lists) as arguments; no global Spark session assumption in the package API.
- **remediation.py** — Pure Python + SDK; no Spark. `execute_remediation` takes `get_approved_permissions` callback for REVERT_PERMISSION (caller provides Spark-backed lookup if needed).
- **constants.py** — Single source of truth for table base names and violation/remediation constants. Do not duplicate these strings elsewhere.

### Adding a new object type (remediation)

1. Implement a `delete_<type>(client, object_id) -> (bool, Optional[str])` function in `remediation.py`.
2. Register it: `register_delete_handler("object_type", handler)` inside `_register_all_handlers()`.
3. If backup before delete is required, add a branch in `get_resource_definition()` in `remediation.py` for that object type.
4. Ensure the filter set (e.g. in `governance_filter_definitions.py` or 03a) includes the corresponding create/delete/ACL filters if they should be tracked.

### Adding a new audit filter or violation type

1. Add the filter row to the appropriate list in `governance_filter_definitions.py` (or pass via widget/env in 03a/setup_environment). Filter format: `[filter_name, service_name, action_name, object_type, object_id_expr, object_name_expr, remediation_action, extra_columns, is_active, description]`.
2. If a new violation type constant is needed, add it in `constants.py` and use it in audit_queries and watcher/remediation logic consistently.
3. If remediation behavior for a new action is needed, extend `execute_remediation()` in `remediation.py` and keep return signature `(status, details, error, backup_definition)`.

### Code style

- **Naming:** snake_case for variables and functions; align with existing names (`workspace_id`, `object_type`, `preapproved_objects`).
- **Types:** Use type hints for public functions; `Optional`, `List`, `Dict`, `Tuple` from `typing` where helpful.
- **Imports:** Prefer standard library first, then third-party, then local. In remediation, delay heavy SDK imports inside functions if needed for optional code paths.
- **Errors:** Let `ResourceDoesNotExist` (or equivalent) propagate where the caller is responsible for handling it; catch and return `(False, str(e))` in delete/revert helpers.

---

## 3. Notebook Format and Structure

- **First line:** `# Databricks notebook source`
- **Sections:** Use `# MAGIC %md` for markdown and `# COMMAND ----------` to separate runnable cells.
- **Docstring:** Start with a short `# MAGIC %md` title and purpose; document parameters and main behavior in markdown cells.
- **Order:** Parameters (widgets / env) → config resolution → imports → main logic. Keep parameter and config blocks at the top.

### Parameters and configuration

- **Widgets:** Use try/except around `dbutils.widgets.get(...)` and fall back to safe defaults. Do not assume widgets exist when notebooks are run from workflows.
- **Config:** Call `GovernanceConfig.from_env()` once; use `config.catalog`, `config.schema`, and pass them into package APIs. In Databricks, provide `get_secret = lambda scope, key: dbutils.secrets.get(scope=scope, key=key)` to `ClientFactory`.
- **Optional env overrides:** Support `WORKSPACE_CONFIGS_JSON`, `APPROVED_IDENTITIES_JSON`, `GOVERNANCE_FILTER_DEFINITIONS_JSON` etc. when documented for a notebook; parse with `json.loads` and validate before use.

### Early exits

- When the notebook should do nothing (e.g. no enabled workspaces, load_filters/enable_discover set), call `dbutils.notebook.exit(json.dumps({...}))`.
- **Contract:** Exit payload must be valid JSON and include at least:
  - `status`: e.g. `"SKIPPED"`, `"SUCCESS"`, `"FAILED"`
  - `reason` or message when status is not SUCCESS
  - Counts or details when relevant (e.g. `violations_detected`, `creations_synced`)
  - `timestamp` in ISO format.
- Use `json.dumps(..., indent=2)` for readability when the output is consumed by humans or logs.

---

## 4. Table and Schema Contracts

- **Writes:** Only write to the governance tables defined in PROJECT_DESCRIPTION.md. Use existing column names and types; match the DDL in `01_setup_environment.py` (or the equivalent setup notebook).
- **New columns:** Do not add columns to existing tables without updating the setup notebook and all readers/writers of that table.
- **MERGE keys:** Preserve existing MERGE key semantics (e.g. `event_id` for violations_staging, `workspace_id` + `object_id` for preapproved_objects) so idempotency and deduplication remain correct.

---

## 5. Error Handling and Logging

- **Widgets and config:** Use try/except with sensible defaults; avoid failing the whole run on a missing optional widget.
- **External calls:** Catch exceptions around SDK/API and Spark operations; record errors in control_actions or exit with status `FAILED` and reason. Do not silently swallow exceptions; prefer explicit handling (log, return, exit with reason).
- **Remediation:** `execute_remediation` returns `(status, details, error, backup_definition)`. The notebook is responsible for writing these to `governance_control_actions` and updating staging status.

---

## 6. Testing and Validation

- **Wheel build:** After changing the package, run `uv build --wheel` (or `pip install -e .`) and reinstall the wheel in the environment used by the notebooks.
- **Exits:** Verify exit JSON is parseable and contains the keys expected by any orchestrator or downstream notebook.
- **Backward compatibility:** Do not remove or rename widget/parameter names that workflows or other notebooks pass; extend instead. When adding new optional parameters, use defaults so existing callers continue to work.

---

## 7. Checklist Before Submitting Changes

- [ ] PROJECT_DESCRIPTION.md read; change aligns with project scope and tables.
- [ ] No hardcoded catalog, schema, workspace IDs, or secrets; config and secrets from env/callback.
- [ ] New object types: delete handler registered and get_resource_definition updated if backup is required.
- [ ] New filters or violation types: constants and filter definitions updated; audit_queries and watcher/remediation consistent.
- [ ] Notebook format (Databricks notebook source, COMMAND cells, %md) preserved; early exits return valid JSON with status/reason/timestamp.
- [ ] Table writes use existing schema; MERGE keys unchanged unless intentionally evolving the design.
- [ ] Error handling is explicit; failures reflected in exit status or control_actions/staging.

Agents must follow this implementation guide when editing code under GovernBot. When in doubt, mimic the patterns in `04a_sync_approved_changes.py`, `05_watcher.py`, and `06_remediation.py` for parameters, config, and exit contracts.
