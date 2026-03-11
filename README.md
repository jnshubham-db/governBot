# GovernBot Core

Reusable library for the GovernBot (Databricks Governance Automation) system. Provides config, client factory, filters, workspaces, identities, permissions, audit query builders, and remediation in a single installable wheel.

## Build

```bash
uv build --wheel
```

Output: `dist/governbot_core-*.whl`

## Install

- Local: `pip install ./dist/governbot_core-*.whl`
- Databricks notebook: `%pip install /Workspace/path/to/GovernBot/dist/governbot_core-*.whl`

Optional: `pip install governbot-core[pyspark]` if you need PySpark for load_filters/load_enabled_workspaces in non-Databricks environments.

## Configuration (.env)

Copy `.env.example` to `.env` and set values. All environment-specific values (catalog, schema, KV scope/key names, account URL, etc.) are read from the environment; no hardcoded values in the library.

- **GOVERNANCE_CATALOG**, **GOVERNANCE_SCHEMA**: Unity Catalog location for governance tables.
- **AUTH_TYPE**: `azure-client-secret` or `pat`.
- **KV_SCOPE**, **KV_CLIENT_ID_KEY**, **KV_CLIENT_SECRET_KEY**, **KV_TENANT_ID_KEY**: Secret scope and key names (values stored in Databricks secrets or env).
- **WORKSPACE_IDS_USE_PAT_KEY2**: Optional comma-separated workspace IDs that use `KV_CLIENT_SECRET_KEY2` for PAT.
- **GOVERNANCE_EXCLUDED_OBJECT_TYPES**: Optional comma-separated object types to exclude from tracking (opt-out).

In Databricks, set env vars from job parameters or secrets; the package does not use `dbutils` and only reads `os.environ`.

## Usage (minimal)

```python
from governbot_core import GovernanceConfig, ClientFactory, load_enabled_workspaces, load_filters

config = GovernanceConfig.from_env()
get_secret = lambda scope, key: dbutils.secrets.get(scope=scope, key=key)  # in Databricks
factory = ClientFactory(config, get_secret)
client = factory.create_workspace_client("https://adb-xxx.azuredatabricks.net/")

rows = load_enabled_workspaces(spark, config.catalog, config.schema)
filters_by_type = load_filters(spark, config.catalog, config.schema)
```

## Adding or disabling object types

- **Object-type registry**: `governbot_core.object_types` defines supported types and a delete-handler dispatcher. Adding a new type: implement a delete function and call `register_delete_handler(object_type, handler)` in `remediation.py`.
- **Opt-out**: Set `GOVERNANCE_EXCLUDED_OBJECT_TYPES` in .env or pass `enabled_object_types` to `execute_remediation` so only listed types are remediated.

## Documentation (humans and agents)

| Document | Purpose |
|----------|---------|
| [PROJECT_DESCRIPTION.md](PROJECT_DESCRIPTION.md) | Scope, concepts, tables, package layout, notebook roles, data flow. |
| [IMPLEMENTATION.md](IMPLEMENTATION.md) | How to implement or change code: package conventions, notebook format, table contracts, checklist. |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Build, install, env config, secret scope, running notebooks, workflow order, troubleshooting. |

## Deploy app with Databricks Asset Bundles (DABs)

The app under `app/` (React + FastAPI) can be deployed using [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/).

1. **Set workspace host** — Edit `databricks.yml` and set `targets.dev.workspace.host` (and `prod` if needed) to your workspace URL.
2. **Authenticate** — `databricks auth login --host https://your-workspace.cloud.databricks.com`
3. **Deploy** (from governBot repo root):
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   ```
4. **Run the app** — `databricks bundle run governbot_app -t dev` or start it from the Apps UI.

See `app/README.md` for app config (catalog, schema, warehouse, env vars).

## References

- [governBot/mx_scripts](../governBot/mx_scripts) and [governBot/DOCUMENTATION.md](../governBot/DOCUMENTATION.md) for original semantics and product details.
