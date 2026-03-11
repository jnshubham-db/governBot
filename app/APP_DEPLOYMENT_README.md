# GovernBot App — Databricks Deployment

This guide covers deploying the GovernBot React + FastAPI app with **Databricks Asset Bundles (DABs)** so the full UI and API are available in your workspace.

## Prerequisites

- Node.js 18+ and npm (for building the frontend)
- Python 3.10+ (for backend; used by Databricks at runtime)
- Databricks CLI configured: `databricks auth login --host <your-workspace-url>`
- Target workspace has a SQL warehouse; you will set its HTTP path as app env

## 1. Build the React frontend

The app serves the React UI from `frontend/dist`. If you skip this step, the deployed app will still run the API, but opening the app URL will show a placeholder page instead of the Summary / Actions Center / Configs UI.

From the **repository root**:

```bash
cd app/frontend
npm install    # or npm ci if package-lock.json exists
npm run build
cd ../..
```

This creates `app/frontend/dist/` (and `app/frontend/dist/assets/`). The bundle deploy will upload everything under `app/`, including `frontend/dist`, so the UI is served after deployment.

**Including the build in git (optional):**  
By default, `dist/` is often gitignored. To commit the build so CI or others can deploy without building locally, ensure `app/frontend/dist` is not ignored (e.g. add `!app/frontend/dist/` in `.gitignore` if your root rule is `dist/`). Otherwise, run the build step on the same machine (or in CI) right before `databricks bundle deploy`.

## 2. Configure the bundle target

Edit `databricks.yml` in the repo root and set the workspace host for your target:

- **dev:** `targets.dev.workspace.host` (e.g. `https://e2-demo-field-eng.cloud.databricks.com`)
- **prod:** `targets.prod.workspace.host`

## 3. Deploy the app

From the **repository root**:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Use `-t prod` for production. The app code under `app/` (including `app.yaml`, `backend/`, and, if present, `frontend/dist/`) is deployed to the workspace.

## 4. Run the app and set environment variables

- **Run the app:**  
  `databricks bundle run governbot_app -t dev`  
  or start it from the Databricks workspace **Apps** UI.

- **Set environment variables** for the app (workspace App UI or by editing `app/app.yaml` before deploy):

  | Variable | Description | Example |
  |----------|-------------|--------|
  | `GOVERNANCE_CATALOG` | Unity Catalog name for governance tables | `karthik_auto_validator_1` |
  | `GOVERNANCE_SCHEMA` | Schema name | `govern_bot` |
  | `DATABRICKS_APP_WAREHOUSE_HTTP_PATH` | SQL warehouse HTTP path | `/sql/1.0/warehouses/<warehouse-id>` |

  Optional:

  | Variable | Description |
  |----------|-------------|
  | `DATABRICKS_CONFIG_PROFILE` | CLI profile for auth (e.g. `e2-demo-west`) if used |
  | `DATABRICKS_TLS_NO_VERIFY` | Set to `1` to disable SSL verification (e.g. behind corporate proxy) |

The app’s identity (user or service principal) must have **CAN USE** on the SQL warehouse and **SELECT** / **MODIFY** on the governance tables in that catalog and schema.

---

## Test in a local environment

Before deploying, you can run the app locally to verify the UI and API against your catalog, schema, and warehouse.

### 1. Backend (API)

From the **repository root**:

```bash
cd app
pip install -r backend/requirements.txt
```

Set environment variables (use your catalog, schema, and SQL warehouse HTTP path):

```bash
export GOVERNANCE_CATALOG=your_catalog
export GOVERNANCE_SCHEMA=your_schema
export DATABRICKS_APP_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/<your-warehouse-id>
export DATABRICKS_CONFIG_PROFILE=your-profile   # optional, for CLI profile auth
export DATABRICKS_TLS_NO_VERIFY=1              # optional, if you see SSL errors
```

Start the API server (you should still be in the `app` directory):

```bash
uvicorn backend.main:app --reload --port 8000
```

The API will be available at http://localhost:8000 (and http://localhost:8000/api/...). Without the frontend build, the root URL may show a simple placeholder.

### 2. Frontend (React dev server)

In a **second terminal**, from the **repository root**:

```bash
cd app/frontend
npm install
npm run dev
```

The Vite dev server runs at http://localhost:5173 and proxies `/api` to http://127.0.0.1:8000. Open http://localhost:5173 to use the React UI (Summary, Actions Center, Configs) against your local backend.

**Config in the UI:** On the Configs page you can set catalog, schema, and warehouse HTTP path; click “Save and reload data.” Values are stored in `localStorage` and sent as headers with every API request.

### 3. Production-like local test (optional)

To test the same setup as in Databricks (single process serving the built frontend + API), from the **repository root**:

```bash
cd app/frontend && npm run build && cd ../..
cd app && uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 to see the built React app and use the API. No separate frontend dev server is needed.

---

## 5. Open the app

Open the app URL shown in the Apps UI (or after `bundle run`). You should see:

- **Summary** — Violation summary and charts (set catalog/schema/warehouse in Configs if not set via env).
- **Actions Center** — Pending violations and approve/reject/note.
- **Configs** — Catalog, schema, warehouse path (stored in the browser); workspaces, identities, and filters from the API.

If you see a plain “GovernBot API” page instead of the React UI, the `frontend/dist` folder was not included in the deploy. Re-run the frontend build (step 1) and deploy again.

## Troubleshooting

| Issue | What to do |
|-------|------------|
| `{"detail": "Not Found"}` when opening the app URL | Ensure the catch-all is deployed (backend always serves index or a placeholder). If you want the full UI, build the frontend and redeploy so `frontend/dist` is present. |
| API returns 400 “Set X-Catalog…” | Set `GOVERNANCE_CATALOG`, `GOVERNANCE_SCHEMA`, and `DATABRICKS_APP_WAREHOUSE_HTTP_PATH` in the app’s environment (App UI or `app/app.yaml`). |
| API returns 502 “Could not connect to Databricks” | Check warehouse HTTP path and that the app identity has **CAN USE** on the warehouse and correct auth (e.g. OAuth or token). |
| SSL certificate errors | Set `DATABRICKS_TLS_NO_VERIFY=1` only if required (e.g. corporate proxy). |

## File reference

| Path | Purpose |
|------|--------|
| `databricks.yml` | Bundle definition; app resource and targets. |
| `app/app.yaml` | App command (`uvicorn backend.main:app`) and default env. |
| `app/backend/main.py` | FastAPI app and API routes; serves `frontend/dist` when present. |
| `app/frontend/` | React app; build output is `frontend/dist/`. |
| `app/README.md` | Local setup, API overview, and build-for-production steps. |
