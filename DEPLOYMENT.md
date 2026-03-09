# GovernBot Reusable Design — Deployment Guide

**Audience:** Humans and AI agents. Use this document to build, install, configure, and run the governance system in Databricks (or locally for package development).

---

## 1. Prerequisites

- **Python:** 3.9 or higher.
- **Databricks:** Workspace with Unity Catalog enabled; access to `system.access.audit` and secret scopes (e.g. Key Vault–backed).
- **Auth:** Azure service principal (client ID, client secret, tenant ID) or Databricks PAT. Secrets must be stored in a Databricks secret scope; the package never reads secrets from code, only via a `get_secret(scope, key)` callback.

---

## 2. Build the Package

From the `governBot` directory:

```bash
cd governBot
uv build --wheel
```

Or with pip:

```bash
pip install build
python -m build --wheel
```

Output: `dist/governbot_core-0.1.0-py3-none-any.whl` (version from `pyproject.toml`).

Optional dependencies (e.g. PySpark for local tests):

```bash
pip install governbot-core[pyspark]
```

---

## 3. Install in Databricks

### Option A: Install from workspace path

Upload the wheel to a workspace path (e.g. `/Workspace/Users/<you>/governBot/dist/governbot_core-0.1.0-py3-none-any.whl`). In the first cell of a notebook or in the cluster init script:

```python
%pip install /Workspace/Users/<you>/governBot/dist/governbot_core-0.1.0-py3-none-any.whl
```

### Option B: Install from DBFS or artifact

If the wheel is on DBFS or a volume:

```python
%pip install /dbfs/path/to/governbot_core-0.1.0-py3-none-any.whl
```

### Option C: Cluster library

Attach the wheel as a cluster library (Workspace or DBFS path). No `%pip` in the notebook required.

After install, restart the Python process if the notebook was already attached (`%restart_python` or detach/reattach cluster).

---

## 4. Environment Configuration

All environment-specific values are read from the environment. In Databricks jobs, set environment variables via job parameters (e.g. pass as `env` in the job task) or from a secret scope.

### Required (minimal)

| Variable | Description | Example |
|----------|-------------|---------|
| `GOVERNANCE_CATALOG` | Unity Catalog catalog for governance tables | `my_catalog` |
| `GOVERNANCE_SCHEMA` | Schema under that catalog | `sch_mng_admon` |
| `KV_SCOPE` | Databricks secret scope name (e.g. Key Vault–backed) | `my-kv-scope` |
| `KV_CLIENT_ID_KEY` | Secret key name for Azure client ID (or PAT key name) | `client-id-key` |
| `KV_CLIENT_SECRET_KEY` | Secret key name for Azure client secret (or PAT) | `client-secret-key` |
| `KV_TENANT_ID_KEY` | Secret key name for Azure tenant ID (not used for PAT) | `tenant-id-key` |

### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `AUTH_TYPE` | `azure-client-secret` or `pat` | `azure-client-secret` |
| `ACCOUNT_ID` | Databricks account ID (for account-level APIs, e.g. serverless budget) | — |
| `ACCOUNT_URL` | Account API URL | — |
| `LOOKBACK_HOURS_DEFAULT` | Default lookback for audit queries (hours) | `24` |
| `GOVERNANCE_EXCLUDED_OBJECT_TYPES` | Comma-separated object types to exclude from tracking | — |
| `WORKSPACE_CATALOG_OVERRIDES` | JSON map workspace_id → catalog for overrides | — |
| `KV_CLIENT_SECRET_KEY2` | Alternate secret key (e.g. for PAT key 2) | — |
| `WORKSPACE_IDS_USE_PAT_KEY2` | Comma-separated workspace IDs that use key2 | — |

Notebooks may also accept JSON via widgets or env for one-off data (e.g. `WORKSPACE_CONFIGS_JSON`, `APPROVED_IDENTITIES_JSON`, `GOVERNANCE_FILTER_DEFINITIONS_JSON`) as documented in the notebook or [README.md](README.md).

---

## 5. Secret Scope Setup

1. Create a secret scope in Databricks (e.g. backed by Azure Key Vault).
2. Store the following secrets (key names must match the `KV_*` env vars above):
   - **Azure:** Client ID, client secret, tenant ID.
   - **PAT:** Databricks PAT (and optionally a second PAT key for selected workspaces).
3. Ensure the cluster or job has access to the scope. Do not put secret values in code or in repo; the package only receives them via `dbutils.secrets.get(scope, key)` in the notebook-provided callback.

---

## 6. Running the Notebooks

### One-time setup (order matters)

1. **01_setup_environment.py**  
   Creates all Delta tables and optionally loads workspace config, approved identities, and filters.  
   - Widgets (optional): `drop_existing`, `workspace_configs`, `load_approved_id`, `approved_identities`, `load_filters`, `replace_existing`, `filter_definitions_json`.  
   - Env: `GOVERNANCE_CATALOG`, `GOVERNANCE_SCHEMA`, and KV scope/keys; optionally `WORKSPACE_CONFIGS_JSON`, `APPROVED_IDENTITIES_JSON`, `GOVERNANCE_FILTER_DEFINITIONS_JSON`.

2. **02_one_time_discover.py**  
   Runs initial discovery and populates `governance_preapproved_objects`.  
   - Requires enabled workspaces and (optionally) filters; config from env.

### Scheduled workflow (run in order)

1. **05_watcher.py** — Detect violations from audit logs; writes to `governance_violations_staging`.  
2. **04a_sync_approved_changes.py** — Sync approved-user changes into `governance_preapproved_objects`; update control_actions for inactive objects.  
3. **06_remediation.py** — Process pending violations; execute remediation; write to `governance_control_actions`; update staging status.  
4. **07_validation.py** — Validate and report; exit with JSON (counts, validation_issues).

Widgets (e.g. `lookback_hours`, `dry_run`, `sync_creations`) are optional and have defaults; see each notebook’s parameter cell.

---

## 7. Workflow Definition (Databricks Workflows)

Define a job with tasks in this order:

1. **setup** (optional, one-time): Run `01_setup_environment.py`.  
2. **discover** (optional, one-time): Run `02_one_time_discover.py`.  
3. **watcher**: Run `05_watcher.py`.  
4. **sync**: Run `04a_sync_approved_changes.py`.  
5. **remediation**: Run `06_remediation.py`.  
6. **validation**: Run `07_validation.py`.

Set the job’s environment variables (or pass them per task) as in section 4. Ensure the cluster has the governbot_core wheel installed and access to the secret scope.

---

## 8. Local Development

- **Config:** Copy `.env.example` to `.env` in `governBot` and set `GOVERNANCE_CATALOG`, `GOVERNANCE_SCHEMA`, and KV scope/keys. For local runs without Databricks, you can stub `get_secret` to return test values.
- **Tests:** Install with dev deps: `pip install -e ".[dev]"`. Run tests with pytest from the package root.
- **Notebooks:** Run on Databricks; they depend on `spark`, `dbutils`, and (for remediation) a real or mock workspace client. There is no local Spark requirement for the package itself unless you use `load_filters` or `load_enabled_workspaces` outside Databricks (then install `[pyspark]`).

---

## 9. Troubleshooting

| Issue | What to check |
|-------|----------------|
| **ImportError: governbot_core** | Wheel not installed or wrong interpreter; re-run `%pip install ...` and restart Python. |
| **Missing config / empty catalog** | Ensure `GOVERNANCE_CATALOG`, `GOVERNANCE_SCHEMA`, and KV_* are set in the job or cluster env. |
| **Secret not found** | Verify scope name and key names match env vars; cluster has permission to the scope. |
| **No violations / no sync** | Confirm `system.access.audit` is available; check lookback_hours and workspace_ids; ensure filters and identities are loaded (01_setup_environment or 03a). |
| **Remediation FAILED** | Check control_actions for error_message; ensure workspace client and (for REVERT_PERMISSION) get_approved_permissions callback are correct. |
| **Exit JSON parse error** | Ensure notebooks use `json.dumps(...)` and never print non-JSON before `dbutils.notebook.exit(...)`. |

For more detail on architecture and object types, see [governBot/DOCUMENTATION.md](../governBot/DOCUMENTATION.md) and [PROJECT_DESCRIPTION.md](PROJECT_DESCRIPTION.md).
