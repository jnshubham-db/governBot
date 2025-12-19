# Databricks Governance Automation - Quick Reference

## Overview

GovernBot automatically monitors Databricks workspaces for unauthorized changes and takes corrective actions.

---

## Flow Diagram

### Complete System Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   GOVERNANCE SYSTEM FLOW                                 │
└─────────────────────────────────────────────────────────────────────────────────────────┘

                              ╔═══════════════════════════════╗
                              ║     ONE-TIME SETUP (Initial)   ║
                              ╚═══════════════════════════════╝
                                            │
            ┌───────────────────────────────┼───────────────────────────────┐
            │                               │                               │
            ▼                               ▼                               ▼
    ┌───────────────┐              ┌───────────────┐              ┌───────────────┐
    │ 01_setup      │              │ 02_load       │              │ 03_approved   │
    │ _tables       │───────────▶  │ _workspace    │───────────▶  │ _id           │
    │               │              │               │              │               │
    │ Creates Delta │              │ Configure     │              │ Add approved  │
    │ tables        │              │ workspace     │              │ users/groups  │
    └───────────────┘              └───────────────┘              └───────┬───────┘
                                                                          │
                                                                          ▼
                                                                  ┌───────────────┐
                                                                  │ 03a_load      │
                                                                  │ _filters      │
                                                                  │               │
                                                                  │ Configure     │
                                                                  │ audit filters │
                                                                  └───────┬───────┘
                                                                          │
                                                                          ▼
                                                                  ┌───────────────┐
                                                                  │ 04_discover   │
                                                                  │               │
                                                                  │ Catalog all   │
                                                                  │ existing      │
                                                                  │ resources     │
                                                                  └───────────────┘


                              ╔═══════════════════════════════╗
                              ║   DAILY WORKFLOW (Scheduled)   ║
                              ╚═══════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                          │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│    │                              AUDIT LOGS                                          │  │
│    │                        (system.access.audit)                                     │  │
│    └─────────────────────────────────────────┬───────────────────────────────────────┘  │
│                                              │                                           │
│                                              ▼                                           │
│                                    ┌─────────────────┐                                   │
│                                    │  05_watcher     │                                   │
│                                    │  ─────────────  │                                   │
│                                    │  Detect         │                                   │
│                                    │  Violations     │                                   │
│                                    └────────┬────────┘                                   │
│                                             │                                            │
│              ┌──────────────────────────────┴──────────────────────────────┐            │
│              │                                                              │            │
│              ▼                                                              ▼            │
│    ┌─────────────────┐                                            ┌─────────────────┐   │
│    │  VIOLATIONS     │                                            │ 04a_sync        │   │
│    │  ─────────────  │                                            │ _approved       │   │
│    │  • Unauthorized │                                            │ ─────────────   │   │
│    │    creations    │                                            │ Sync approved   │   │
│    │  • Unauthorized │                                            │ user changes    │   │
│    │    deletions    │                                            │ to baseline     │   │
│    │  • Permission   │                                            └─────────────────┘   │
│    │    changes      │                                                                   │
│    └────────┬────────┘                                                                   │
│             │                                                                            │
│             ▼                                                                            │
│    ┌─────────────────┐                                                                   │
│    │  06_remediation │                                                                   │
│    │  ─────────────  │                                                                   │
│    │  • Delete       │──────────────────────────────────────────────────────────────┐   │
│    │    resources    │                                                               │   │
│    │  • Revert       │                                                               │   │
│    │    permissions  │                                                               │   │
│    │  • Report only  │                                                               │   │
│    └────────┬────────┘                                                               │   │
│             │                                                                        │   │
│             ▼                                                                        ▼   │
│    ┌─────────────────┐                                                   ┌───────────┐  │
│    │  07_validation  │                                                   │  CONTROL  │  │
│    │  ─────────────  │                                                   │  ACTIONS  │  │
│    │  • Verify       │                                                   │  TABLE    │  │
│    │    actions      │                                                   │           │  │
│    │  • Retry failed │                                                   │  (Audit   │  │
│    │  • Generate     │                                                   │   Trail)  │  │
│    │    reports      │                                                   └───────────┘  │
│    └────────┬────────┘                                                                   │
│             │                                                                            │
│             ▼                                                                            │
│    ┌─────────────────┐                                                                   │
│    │  DATABRICKS     │                                                                   │
│    │  SQL ALERT      │                                                                   │
│    │  ─────────────  │                                                                   │
│    │  Email/Slack    │                                                                   │
│    │  to Security    │                                                                   │
│    │  Team           │                                                                   │
│    └─────────────────┘                                                                   │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Notebooks Quick Reference

### Setup Notebooks (Run Once)

| Notebook | Purpose | Order |
|----------|---------|-------|
| `01_setup_tables.py` | Create governance Delta tables | 1st |
| `02_load_workspace.py` | Add workspace configuration | 2nd |
| `03_approved_id.py` | Define authorized users/groups | 3rd |
| `03a_load_filters.py` | Configure audit log filters | 4th |
| `04_discover.py` | Catalog existing resources | 5th |

### Workflow Notebooks (Daily Schedule)

| Notebook | Purpose | Workflow Order |
|----------|---------|----------------|
| `05_watcher.py` | Detect violations from audit logs | Step 1 |
| `04a_sync_approved_changes.py` | Sync approved user changes | Step 2 |
| `06_remediation.py` | Execute corrective actions | Step 3 |
| `07_validation.py` | Validate & report | Step 4 |

---

## Violation Types & Actions

```
┌────────────────────────────────┬──────────────────────────────────────────────────────┐
│         VIOLATION TYPE         │                   REMEDIATION ACTION                  │
├────────────────────────────────┼──────────────────────────────────────────────────────┤
│  UNAPPROVED_CREATION           │  DELETE_RESOURCE                                     │
│  (Unauthorized new resource)   │  → Deletes the unauthorized resource                 │
├────────────────────────────────┼──────────────────────────────────────────────────────┤
│  UNAUTHORIZED_PERMISSION_CHANGE│  REVERT_PERMISSION                                   │
│  (Unauthorized ACL change)     │  → Restores permissions to pre-approved state        │
├────────────────────────────────┼──────────────────────────────────────────────────────┤
│  UNAUTHORIZED_DELETION         │  REPORT_DELETION                                     │
│  (Unauthorized resource delete)│  → Logs for security review (no auto-action)         │
└────────────────────────────────┴──────────────────────────────────────────────────────┘
```

---

## Key Exclusions Configured

### What Gets Ignored (Won't Trigger Violations)

| Exclusion | Reason |
|-----------|--------|
| `/Workspace/Users/*` deletions | Users can manage their personal workspace |
| Job clusters (`/clusters/jobs/%`) | System-managed clusters |
| Pipeline clusters (`/clusters/pipelines/%`) | System-managed clusters |
| Serverless SQL warehouses | System-managed compute |
| `System-User` actions | Databricks system operations |
| Azure DataFactory owner assignments | Standard ADF deployment pattern |
| Catalogs: `__databricks_internal`, `samples`, `system` | System catalogs |
| Schema: `information_schema` | System schema |
| Models: `system.ai.*` | Databricks foundation models |
| Endpoints: `databricks-*` | Databricks foundation model endpoints |

---

## Identity Types

```
┌─────────────────────┬─────────────────────────────────────────────────────┐
│     IDENTITY TYPE   │                    EXAMPLE                          │
├─────────────────────┼─────────────────────────────────────────────────────┤
│  USER               │  user@company.com                                   │
├─────────────────────┼─────────────────────────────────────────────────────┤
│  SERVICE_PRINCIPAL  │  d118594b-a1db-41b2-a6e2-a201377c2aec              │
├─────────────────────┼─────────────────────────────────────────────────────┤
│  GROUP              │  data-engineers, admin-team                         │
└─────────────────────┴─────────────────────────────────────────────────────┘
```

### Permission Flags

| Flag | Description |
|------|-------------|
| `can_manage_resources` | Can create/delete jobs, pipelines, apps, etc. |
| `can_manage_permissions` | Can grant/revoke ACL permissions |

---

## Tables Overview

```
┌──────────────────────────────────┬────────────────────────────────────────────────┐
│            TABLE NAME            │                   PURPOSE                       │
├──────────────────────────────────┼────────────────────────────────────────────────┤
│  governance_config_workspaces    │  Workspace configurations & settings           │
├──────────────────────────────────┼────────────────────────────────────────────────┤
│  governance_preapproved_objects  │  Baseline of approved resources & permissions  │
├──────────────────────────────────┼────────────────────────────────────────────────┤
│  governance_preapproved_identities│ Authorized users/groups/SPs                   │
├──────────────────────────────────┼────────────────────────────────────────────────┤
│  governance_filters              │  Audit log filter rules                        │
├──────────────────────────────────┼────────────────────────────────────────────────┤
│  governance_violations_staging   │  Detected violations pending action            │
├──────────────────────────────────┼────────────────────────────────────────────────┤
│  governance_control_actions      │  Audit trail of all remediation actions        │
└──────────────────────────────────┴────────────────────────────────────────────────┘
```

---

## Workflow Task Dependencies

```
┌─────────────────┐
│   05_watcher    │──────────┐
└─────────────────┘          │
                             ▼
                    ┌─────────────────────┐
                    │ 04a_sync_approved   │──────────┐
                    │     _changes        │          │
                    └─────────────────────┘          │
                                                     ▼
                                            ┌─────────────────┐
                                            │ 06_remediation  │──────────┐
                                            └─────────────────┘          │
                                                                         ▼
                                                                ┌─────────────────┐
                                                                │ 07_validation   │
                                                                └─────────────────┘
                                                                         │
                                                                         ▼
                                                                ┌─────────────────┐
                                                                │   SQL ALERT     │
                                                                │   (Email/Slack) │
                                                                └─────────────────┘
```

---

## Quick Commands

### Enable/Disable Governance for a Workspace

```sql
-- Enable
UPDATE catalog.schema.governance_config_workspaces
SET enforcement_enabled = true, updated_at = current_timestamp()
WHERE workspace_id = 'your_workspace_id';

-- Disable
UPDATE catalog.schema.governance_config_workspaces
SET enforcement_enabled = false, updated_at = current_timestamp()
WHERE workspace_id = 'your_workspace_id';
```

### Add/Remove Approved Identity

```sql
-- Add
INSERT INTO catalog.schema.governance_preapproved_identities
VALUES ('user@company.com', 'USER', 'display_name', true, true, true, current_timestamp(), current_timestamp());

-- Deactivate
UPDATE catalog.schema.governance_preapproved_identities
SET is_active = false, updated_at = current_timestamp()
WHERE identity_name = 'user@company.com';
```

### View Pending Violations

```sql
SELECT * FROM catalog.schema.governance_violations_staging
WHERE processing_status = 'PENDING'
ORDER BY created_at DESC;
```

### View Remediation History

```sql
SELECT * FROM catalog.schema.governance_control_actions
WHERE created_at >= current_date() - 7
ORDER BY created_at DESC;
```

---

## Support

For issues or questions:
1. Check `DOCUMENTATION.md` for detailed information
2. Enable `debug_permissions = Y` in discovery for verbose logging
3. Review `governance_control_actions` table for remediation errors

---

*Version: 1.0 | Last Updated: December 2024*

