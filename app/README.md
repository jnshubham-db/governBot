# GovernBot App (React + FastAPI)

React frontend and FastAPI backend for GovernBot: violation summary, actions center, and configuration.

## Stack

- **Frontend:** React 18, TypeScript, Vite, React Router, Recharts. Electric theme (primary `#00D4FF`, dark background).
- **Backend:** FastAPI; reads/writes governance tables via Databricks SQL Connector. All API routes accept optional headers `X-Catalog`, `X-Schema`, `X-Warehouse-HTTP-Path` (or use env defaults).

## Setup

### Backend (API)

- From the **app** directory (so `config` and `backend` are on the path):
  ```bash
  pip install -r backend/requirements.txt
  export GOVERNANCE_CATALOG=karthik_auto_validator_1
  export GOVERNANCE_SCHEMA=govern_bot
  export DATABRICKS_APP_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/...
  export DATABRICKS_CONFIG_PROFILE=e2-demo-west   # optional, for CLI profile auth
  export DATABRICKS_TLS_NO_VERIFY=1              # optional, if you see SSL errors
  cd governBot/app && uvicorn backend.main:app --reload --port 8000
  ```

### Frontend (React)

- From the **app** directory:
  ```bash
  cd frontend && npm install && npm run dev
  ```
  Dev server runs at http://localhost:5173 and proxies `/api` to http://127.0.0.1:8000.

- **Config in the UI:** On the Configs page, set catalog, schema, and warehouse HTTP path; click “Save and reload data.” Values are stored in `localStorage` and sent as headers with every API request.

## Build for production

```bash
cd frontend && npm run build
```

Then run the backend from **app**; it will serve the built React app from `frontend/dist` and the API under `/api`:

```bash
cd governBot/app && uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 for the React app.

## Databricks App deployment

- Build the frontend before deploying (e.g. in CI or locally: `cd frontend && npm ci && npm run build`).
- Use `app.yaml`: command runs `uvicorn backend.main:app --host 0.0.0.0 --port 8000`. Set env vars for catalog, schema, and warehouse path.
- **Privileges:** App identity needs **CAN USE** on the SQL warehouse and **SELECT** / **MODIFY** on the governance tables.

## API overview

- `GET /api/config` — default catalog, schema, warehouse path from env.
- `GET /api/summary?hours=24` — violation summary (KPIs, by_type, by_object_type, latest).
- `GET /api/actions` — pending violations (optional query: workspace, violation_type, remediation_action).
- `POST /api/actions/approve` — body `{ note }`, header `X-Violation-Id`.
- `POST /api/actions/reject` — body `{ reason }`, header `X-Violation-Id`.
- `POST /api/actions/note` — body `{ note }`, header `X-Violation-Id`.
- `GET/POST/PUT/DELETE /api/configs/workspaces` — list, create, update, delete.
- `GET/POST/PUT/DELETE /api/configs/identities` — list, create, update, delete (identity update/delete use header `X-Identity-Type`).
- `GET/POST/PUT/DELETE /api/configs/filters` — list, create, update, delete.

All configs/ and summary/actions routes use `X-Catalog`, `X-Schema`, `X-Warehouse-HTTP-Path` when set; otherwise env defaults.

## Actions Center — Approvals

- **Approve:** Inserts into `governance_pending_approvals`.
- **Reject:** Inserts a row into `governance_control_actions` with `remediation_status = 'SKIPPED'` and sets staging `processing_status = 'COMPLETED'`.
- **Add note:** Updates `remediation_details` on the corresponding `governance_control_actions` row.
