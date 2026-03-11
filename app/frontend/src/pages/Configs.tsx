import { useState, useEffect } from 'react'
import {
  getConfig,
  getWorkspaces,
  getIdentities,
  getFilters,
  createWorkspace,
  updateWorkspace,
  deleteWorkspace,
  createIdentity,
  updateIdentity,
  deleteIdentity,
  createFilter,
  updateFilter,
  deleteFilter,
} from '../api'

const dialogOverlay: React.CSSProperties = {
  position: 'fixed',
  inset: 0,
  background: 'rgba(0,0,0,0.6)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  zIndex: 1000,
}
const dialogBox: React.CSSProperties = {
  background: 'var(--bg-secondary)',
  border: '1px solid #334155',
  borderRadius: 8,
  padding: '1.5rem',
  maxWidth: 520,
  width: '90%',
  maxHeight: '90vh',
  overflow: 'auto',
}
const formRow: React.CSSProperties = { marginBottom: '0.75rem' }
const formLabel: React.CSSProperties = { display: 'block', fontSize: '0.875rem', marginBottom: '0.25rem', color: 'var(--text-muted)' }
const formInput: React.CSSProperties = { width: '100%', padding: '0.5rem', background: 'var(--bg)', border: '1px solid #334155', borderRadius: 4, color: 'var(--text)' }
const formCheckbox: React.CSSProperties = { marginRight: '0.5rem' }
const buttonRow: React.CSSProperties = { display: 'flex', gap: '0.5rem', marginTop: '1rem', flexWrap: 'wrap' }
const btnPrimary: React.CSSProperties = { padding: '0.5rem 1rem', background: 'var(--primary)', color: 'var(--bg)', border: 'none', borderRadius: 6, cursor: 'pointer' }
const btnSecondary: React.CSSProperties = { padding: '0.5rem 1rem', background: 'transparent', color: 'var(--text)', border: '1px solid #334155', borderRadius: 6, cursor: 'pointer' }
const btnDanger: React.CSSProperties = { padding: '0.5rem 1rem', background: '#7f1d1d', color: '#fecaca', border: 'none', borderRadius: 6, cursor: 'pointer' }
const tableAction: React.CSSProperties = { marginRight: '0.5rem', padding: '0.25rem 0.5rem', fontSize: '0.8rem', cursor: 'pointer' }

function arrFrom(val: unknown): string[] {
  if (Array.isArray(val)) return val.map(String).filter(Boolean)
  if (typeof val === 'string') return val.split(',').map((s) => s.trim()).filter(Boolean)
  return []
}
function arrToStr(arr: string[]): string {
  return Array.isArray(arr) ? arr.join(', ') : ''
}

type WorkspaceRow = Record<string, unknown>
type IdentityRow = Record<string, unknown>
type FilterRow = Record<string, unknown>

export default function Configs() {
  const [config, setConfig] = useState({ catalog: '', schema: '', warehouse_http_path: '' })
  const [workspaces, setWorkspaces] = useState<WorkspaceRow[]>([])
  const [identities, setIdentities] = useState<IdentityRow[]>([])
  const [filters, setFilters] = useState<FilterRow[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const [wsDialog, setWsDialog] = useState<{ open: boolean; mode: 'add' | 'edit'; row?: WorkspaceRow }>({ open: false, mode: 'add' })
  const [idDialog, setIdDialog] = useState<{ open: boolean; mode: 'add' | 'edit'; row?: IdentityRow }>({ open: false, mode: 'add' })
  const [filterDialog, setFilterDialog] = useState<{ open: boolean; mode: 'add' | 'edit'; row?: FilterRow }>({ open: false, mode: 'add' })

  const saveConfig = () => {
    if (config.catalog) localStorage.setItem('governbot_catalog', config.catalog)
    if (config.schema) localStorage.setItem('governbot_schema', config.schema)
    if (config.warehouse_http_path) localStorage.setItem('governbot_warehouse_http_path', config.warehouse_http_path)
    loadAll()
  }

  const loadAll = () => {
    setLoading(true)
    setError(null)
    getConfig()
      .then((c) => {
        setConfig({ catalog: c.catalog || '', schema: c.schema || '', warehouse_http_path: c.warehouse_http_path || '' })
        return Promise.all([getWorkspaces(), getIdentities(), getFilters()])
      })
      .then(([ws, id, fl]) => {
        setWorkspaces(ws.rows || [])
        setIdentities(id.rows || [])
        setFilters(fl.rows || [])
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    getConfig()
      .then((c) => {
        setConfig({
          catalog: localStorage.getItem('governbot_catalog') || c.catalog || '',
          schema: localStorage.getItem('governbot_schema') || c.schema || '',
          warehouse_http_path: localStorage.getItem('governbot_warehouse_http_path') || c.warehouse_http_path || '',
        })
        return loadAll()
      })
      .catch(() => setLoading(false))
  }, [])

  if (loading && !workspaces.length && !identities.length && !filters.length) return <p>Loading...</p>

  return (
    <>
      <h1 style={{ marginTop: 0 }}>Configs</h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '1rem' }}>
        Catalog, schema, and warehouse path are sent with every API request. Add and edit workspaces, approved identities, and assets to track below.
      </p>
      {error && <div style={{ color: '#f87171', marginBottom: '1rem' }}>{error}</div>}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '1rem', marginBottom: '2rem' }}>
        <div style={formRow}>
          <label style={formLabel}>Governance catalog</label>
          <input
            value={config.catalog}
            onChange={(e) => setConfig((c) => ({ ...c, catalog: e.target.value }))}
            placeholder="e.g. karthik_auto_validator_1"
            style={formInput}
          />
        </div>
        <div style={formRow}>
          <label style={formLabel}>Governance schema</label>
          <input
            value={config.schema}
            onChange={(e) => setConfig((c) => ({ ...c, schema: e.target.value }))}
            placeholder="e.g. govern_bot"
            style={formInput}
          />
        </div>
        <div style={formRow}>
          <label style={formLabel}>Warehouse HTTP path</label>
          <input
            type="password"
            value={config.warehouse_http_path}
            onChange={(e) => setConfig((c) => ({ ...c, warehouse_http_path: e.target.value }))}
            placeholder="/sql/1.0/warehouses/..."
            style={formInput}
          />
        </div>
      </div>
      <button type="button" onClick={saveConfig} style={{ ...btnPrimary, marginBottom: '2rem' }}>Save and reload data</button>

      {/* --- Workspaces --- */}
      <h2 style={{ marginBottom: '0.5rem' }}>Workspaces to track</h2>
      <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', marginBottom: '0.5rem' }}>Table: governance_config_workspaces</p>
      <button type="button" onClick={() => setWsDialog({ open: true, mode: 'add' })} style={{ ...btnPrimary, marginBottom: '0.75rem' }}>Add workspace</button>
      {workspaces.length === 0 ? (
        <p style={{ color: 'var(--text-muted)', marginBottom: '2rem' }}>No workspaces. Add one or configure catalog/schema above and save.</p>
      ) : (
        <div style={{ overflowX: 'auto', marginBottom: '2rem' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>workspace_id</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>workspace_name</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>workspace_url</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>enforcement_enabled</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>max_retry_attempts</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {workspaces.map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #334155' }}>
                  <td style={{ padding: '0.5rem' }}>{String(row.workspace_id ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.workspace_name ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.workspace_url ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.enforcement_enabled ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.max_retry_attempts ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>
                    <button type="button" style={tableAction} onClick={() => setWsDialog({ open: true, mode: 'edit', row })}>Edit</button>
                    <button type="button" style={{ ...tableAction, ...btnDanger }} onClick={() => deleteWorkspace(String(row.workspace_id)).then(loadAll).catch((e) => setError(e.message))}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* --- Identities --- */}
      <h2 style={{ marginBottom: '0.5rem' }}>Approved identities</h2>
      <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', marginBottom: '0.5rem' }}>Table: governance_preapproved_identities</p>
      <button type="button" onClick={() => setIdDialog({ open: true, mode: 'add' })} style={{ ...btnPrimary, marginBottom: '0.75rem' }}>Add identity</button>
      {identities.length === 0 ? (
        <p style={{ color: 'var(--text-muted)', marginBottom: '2rem' }}>No identities.</p>
      ) : (
        <div style={{ overflowX: 'auto', marginBottom: '2rem' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>identity_name</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>identity_type</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>display_name</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>is_active</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {identities.map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #334155' }}>
                  <td style={{ padding: '0.5rem' }}>{String(row.identity_name ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.identity_type ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.display_name ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.is_active ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>
                    <button type="button" style={tableAction} onClick={() => setIdDialog({ open: true, mode: 'edit', row })}>Edit</button>
                    <button type="button" style={{ ...tableAction, ...btnDanger }} onClick={() => deleteIdentity(String(row.identity_name), String(row.identity_type)).then(loadAll).catch((e) => setError(e.message))}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* --- Filters (assets to track) --- */}
      <h2 style={{ marginBottom: '0.5rem' }}>Assets to track (filters)</h2>
      <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', marginBottom: '0.5rem' }}>Table: governance_filters</p>
      <button type="button" onClick={() => setFilterDialog({ open: true, mode: 'add' })} style={{ ...btnPrimary, marginBottom: '0.75rem' }}>Add filter</button>
      {filters.length === 0 ? (
        <p style={{ color: 'var(--text-muted)' }}>No filters.</p>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>filter_name</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>service_name</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>violation_type</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>remediation_action</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>is_active</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filters.map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #334155' }}>
                  <td style={{ padding: '0.5rem' }}>{String(row.filter_name ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.service_name ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.violation_type ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.remediation_action ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.is_active ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>
                    <button type="button" style={tableAction} onClick={() => setFilterDialog({ open: true, mode: 'edit', row })}>Edit</button>
                    <button type="button" style={{ ...tableAction, ...btnDanger }} onClick={() => deleteFilter(String(row.filter_id)).then(loadAll).catch((e) => setError(e.message))}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Workspace dialog */}
      {wsDialog.open && (
        <WorkspaceDialog
          mode={wsDialog.mode}
          row={wsDialog.row}
          onClose={() => setWsDialog({ open: false, mode: 'add' })}
          onSaved={() => { setWsDialog({ open: false, mode: 'add' }); loadAll() }}
          onError={setError}
        />
      )}
      {idDialog.open && (
        <IdentityDialog
          mode={idDialog.mode}
          row={idDialog.row}
          onClose={() => setIdDialog({ open: false, mode: 'add' })}
          onSaved={() => { setIdDialog({ open: false, mode: 'add' }); loadAll() }}
          onError={setError}
        />
      )}
      {filterDialog.open && (
        <FilterDialog
          mode={filterDialog.mode}
          row={filterDialog.row}
          onClose={() => setFilterDialog({ open: false, mode: 'add' })}
          onSaved={() => { setFilterDialog({ open: false, mode: 'add' }); loadAll() }}
          onError={setError}
        />
      )}
    </>
  )
}

function WorkspaceDialog({
  mode,
  row,
  onClose,
  onSaved,
  onError,
}: {
  mode: 'add' | 'edit'
  row?: WorkspaceRow
  onClose: () => void
  onSaved: () => void
  onError: (s: string | null) => void
}) {
  const [workspace_id, setWorkspaceId] = useState('')
  const [workspace_name, setWorkspaceName] = useState('')
  const [workspace_url, setWorkspaceUrl] = useState('')
  const [warehouse_id, setWarehouseId] = useState('')
  const [enforcement_enabled, setEnforcementEnabled] = useState(false)
  const [notification_email, setNotificationEmail] = useState('')
  const [notification_slack_webhook, setNotificationSlackWebhook] = useState('')
  const [enabled_object_types, setEnabledObjectTypes] = useState('')
  const [max_retry_attempts, setMaxRetryAttempts] = useState(3)
  const [created_by, setCreatedBy] = useState('api')
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    if (row) {
      setWorkspaceId(String(row.workspace_id ?? ''))
      setWorkspaceName(String(row.workspace_name ?? ''))
      setWorkspaceUrl(String(row.workspace_url ?? ''))
      setWarehouseId(String(row.warehouse_id ?? ''))
      setEnforcementEnabled(Boolean(row.enforcement_enabled))
      setNotificationEmail(String(row.notification_email ?? ''))
      setNotificationSlackWebhook(String(row.notification_slack_webhook ?? ''))
      setEnabledObjectTypes(arrToStr(arrFrom(row.enabled_object_types)))
      setMaxRetryAttempts(Number(row.max_retry_attempts) || 3)
      setCreatedBy(String(row.created_by ?? 'api'))
    } else {
      setWorkspaceId('')
      setWorkspaceName('')
      setWorkspaceUrl('')
      setWarehouseId('')
      setEnforcementEnabled(false)
      setNotificationEmail('')
      setNotificationSlackWebhook('')
      setEnabledObjectTypes('notebook, query, job')
      setMaxRetryAttempts(3)
      setCreatedBy('api')
    }
  }, [row])

  const handleSubmit = () => {
    const body = {
      workspace_id,
      workspace_name,
      workspace_url,
      warehouse_id: warehouse_id || null,
      enforcement_enabled,
      notification_email: notification_email || null,
      notification_slack_webhook: notification_slack_webhook || null,
      enabled_object_types: arrFrom(enabled_object_types),
      max_retry_attempts,
      created_by,
    }
    setSaving(true)
    onError(null)
    const p = mode === 'add' ? createWorkspace(body) : updateWorkspace(workspace_id, body)
    p.then(onSaved).catch((e) => onError(e.message)).finally(() => setSaving(false))
  }

  return (
    <div style={dialogOverlay} onClick={onClose}>
      <div style={dialogBox} onClick={(e) => e.stopPropagation()}>
        <h3 style={{ marginTop: 0 }}>{mode === 'add' ? 'Add workspace' : 'Edit workspace'}</h3>
        <div style={formRow}>
          <label style={formLabel}>workspace_id *</label>
          <input value={workspace_id} onChange={(e) => setWorkspaceId(e.target.value)} style={formInput} placeholder="e.g. ws-123" disabled={mode === 'edit'} />
        </div>
        <div style={formRow}>
          <label style={formLabel}>workspace_name *</label>
          <input value={workspace_name} onChange={(e) => setWorkspaceName(e.target.value)} style={formInput} placeholder="My workspace" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>workspace_url</label>
          <input value={workspace_url} onChange={(e) => setWorkspaceUrl(e.target.value)} style={formInput} placeholder="https://..." />
        </div>
        <div style={formRow}>
          <label style={formLabel}>warehouse_id</label>
          <input value={warehouse_id} onChange={(e) => setWarehouseId(e.target.value)} style={formInput} placeholder="optional" />
        </div>
        <div style={formRow}>
          <label style={{ ...formLabel, display: 'flex', alignItems: 'center' }}>
            <input type="checkbox" checked={enforcement_enabled} onChange={(e) => setEnforcementEnabled(e.target.checked)} style={formCheckbox} />
            enforcement_enabled
          </label>
        </div>
        <div style={formRow}>
          <label style={formLabel}>notification_email</label>
          <input type="email" value={notification_email} onChange={(e) => setNotificationEmail(e.target.value)} style={formInput} placeholder="optional" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>notification_slack_webhook</label>
          <input value={notification_slack_webhook} onChange={(e) => setNotificationSlackWebhook(e.target.value)} style={formInput} placeholder="optional" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>enabled_object_types (comma-separated)</label>
          <input value={enabled_object_types} onChange={(e) => setEnabledObjectTypes(e.target.value)} style={formInput} placeholder="notebook, query, job" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>max_retry_attempts</label>
          <input type="number" min={0} value={max_retry_attempts} onChange={(e) => setMaxRetryAttempts(Number(e.target.value) || 0)} style={formInput} />
        </div>
        <div style={formRow}>
          <label style={formLabel}>created_by</label>
          <input value={created_by} onChange={(e) => setCreatedBy(e.target.value)} style={formInput} />
        </div>
        <div style={buttonRow}>
          <button type="button" style={btnPrimary} onClick={handleSubmit} disabled={saving || !workspace_id.trim() || !workspace_name.trim()}>{saving ? 'Saving…' : 'Save'}</button>
          <button type="button" style={btnSecondary} onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}

function IdentityDialog({
  mode,
  row,
  onClose,
  onSaved,
  onError,
}: {
  mode: 'add' | 'edit'
  row?: IdentityRow
  onClose: () => void
  onSaved: () => void
  onError: (s: string | null) => void
}) {
  const [identity_name, setIdentityName] = useState('')
  const [identity_type, setIdentityType] = useState('USER')
  const [display_name, setDisplayName] = useState('')
  const [can_manage_resources, setCanManageResources] = useState(false)
  const [can_manage_permissions, setCanManagePermissions] = useState(false)
  const [approved_actions, setApprovedActions] = useState('')
  const [is_active, setIsActive] = useState(true)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    if (row) {
      setIdentityName(String(row.identity_name ?? ''))
      setIdentityType(String(row.identity_type ?? 'USER'))
      setDisplayName(String(row.display_name ?? ''))
      setCanManageResources(Boolean(row.can_manage_resources))
      setCanManagePermissions(Boolean(row.can_manage_permissions))
      setApprovedActions(arrToStr(arrFrom(row.approved_actions)))
      setIsActive(Boolean(row.is_active))
    } else {
      setIdentityName('')
      setIdentityType('USER')
      setDisplayName('')
      setCanManageResources(false)
      setCanManagePermissions(false)
      setApprovedActions('ALL')
      setIsActive(true)
    }
  }, [row])

  const handleSubmit = () => {
    const body = {
      identity_name,
      identity_type,
      display_name,
      can_manage_resources,
      can_manage_permissions,
      approved_actions: arrFrom(approved_actions),
      is_active,
    }
    setSaving(true)
    onError(null)
    const p = mode === 'add' ? createIdentity(body) : updateIdentity(identity_name, identity_type, body)
    p.then(onSaved).catch((e) => onError(e.message)).finally(() => setSaving(false))
  }

  return (
    <div style={dialogOverlay} onClick={onClose}>
      <div style={dialogBox} onClick={(e) => e.stopPropagation()}>
        <h3 style={{ marginTop: 0 }}>{mode === 'add' ? 'Add approved identity' : 'Edit identity'}</h3>
        <div style={formRow}>
          <label style={formLabel}>identity_name *</label>
          <input value={identity_name} onChange={(e) => setIdentityName(e.target.value)} style={formInput} placeholder="user@example.com" disabled={mode === 'edit'} />
        </div>
        <div style={formRow}>
          <label style={formLabel}>identity_type *</label>
          <select value={identity_type} onChange={(e) => setIdentityType(e.target.value)} style={formInput} disabled={mode === 'edit'}>
            <option value="USER">USER</option>
            <option value="GROUP">GROUP</option>
            <option value="SERVICE_PRINCIPAL">SERVICE_PRINCIPAL</option>
          </select>
        </div>
        <div style={formRow}>
          <label style={formLabel}>display_name</label>
          <input value={display_name} onChange={(e) => setDisplayName(e.target.value)} style={formInput} placeholder="optional" />
        </div>
        <div style={formRow}>
          <label style={{ ...formLabel, display: 'flex', alignItems: 'center' }}>
            <input type="checkbox" checked={can_manage_resources} onChange={(e) => setCanManageResources(e.target.checked)} style={formCheckbox} />
            can_manage_resources
          </label>
        </div>
        <div style={formRow}>
          <label style={{ ...formLabel, display: 'flex', alignItems: 'center' }}>
            <input type="checkbox" checked={can_manage_permissions} onChange={(e) => setCanManagePermissions(e.target.checked)} style={formCheckbox} />
            can_manage_permissions
          </label>
        </div>
        <div style={formRow}>
          <label style={formLabel}>approved_actions (comma-separated, e.g. ALL or notebook, query, job)</label>
          <input value={approved_actions} onChange={(e) => setApprovedActions(e.target.value)} style={formInput} placeholder="ALL" />
        </div>
        <div style={formRow}>
          <label style={{ ...formLabel, display: 'flex', alignItems: 'center' }}>
            <input type="checkbox" checked={is_active} onChange={(e) => setIsActive(e.target.checked)} style={formCheckbox} />
            is_active
          </label>
        </div>
        <div style={buttonRow}>
          <button type="button" style={btnPrimary} onClick={handleSubmit} disabled={saving || !identity_name.trim()}>{saving ? 'Saving…' : 'Save'}</button>
          <button type="button" style={btnSecondary} onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}

function FilterDialog({
  mode,
  row,
  onClose,
  onSaved,
  onError,
}: {
  mode: 'add' | 'edit'
  row?: FilterRow
  onClose: () => void
  onSaved: () => void
  onError: (s: string | null) => void
}) {
  const [filter_name, setFilterName] = useState('')
  const [service_name, setServiceName] = useState('')
  const [action_name, setActionName] = useState('')
  const [object_type, setObjectType] = useState('')
  const [object_id_expr, setObjectIdExpr] = useState('')
  const [object_name_expr, setObjectNameExpr] = useState('')
  const [violation_type, setViolationType] = useState('')
  const [remediation_action, setRemediationAction] = useState('')
  const [is_active, setIsActive] = useState(true)
  const [description, setDescription] = useState('')
  const [saving, setSaving] = useState(false)
  const editFilterId = row ? String(row.filter_id ?? '') : ''

  useEffect(() => {
    if (row) {
      setFilterName(String(row.filter_name ?? ''))
      setServiceName(String(row.service_name ?? ''))
      setActionName(String(row.action_name ?? ''))
      setObjectType(String(row.object_type ?? ''))
      setObjectIdExpr(String(row.object_id_expr ?? ''))
      setObjectNameExpr(String(row.object_name_expr ?? ''))
      setViolationType(String(row.violation_type ?? ''))
      setRemediationAction(String(row.remediation_action ?? ''))
      setIsActive(Boolean(row.is_active))
      setDescription(String(row.description ?? ''))
    } else {
      setFilterName('')
      setServiceName('')
      setActionName('')
      setObjectType('')
      setObjectIdExpr('')
      setObjectNameExpr('')
      setViolationType('')
      setRemediationAction('')
      setIsActive(true)
      setDescription('')
    }
  }, [row])

  const handleSubmit = () => {
    const body = {
      filter_name,
      service_name,
      action_name,
      object_type,
      object_id_expr,
      object_name_expr,
      violation_type,
      remediation_action,
      is_active,
      description,
    }
    setSaving(true)
    onError(null)
    const p = mode === 'add' ? createFilter(body) : updateFilter(editFilterId, body)
    p.then(onSaved).catch((e) => onError(e.message)).finally(() => setSaving(false))
  }

  return (
    <div style={dialogOverlay} onClick={onClose}>
      <div style={dialogBox} onClick={(e) => e.stopPropagation()}>
        <h3 style={{ marginTop: 0 }}>{mode === 'add' ? 'Add filter (asset to track)' : 'Edit filter'}</h3>
        {mode === 'edit' && (
          <div style={formRow}>
            <label style={formLabel}>filter_id (read-only)</label>
            <input value={editFilterId} style={{ ...formInput, opacity: 0.8 }} readOnly />
          </div>
        )}
        <div style={formRow}>
          <label style={formLabel}>filter_name *</label>
          <input value={filter_name} onChange={(e) => setFilterName(e.target.value)} style={formInput} placeholder="e.g. notebooks_create" disabled={mode === 'edit'} />
        </div>
        <div style={formRow}>
          <label style={formLabel}>service_name</label>
          <input value={service_name} onChange={(e) => setServiceName(e.target.value)} style={formInput} placeholder="e.g. notebooks" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>action_name</label>
          <input value={action_name} onChange={(e) => setActionName(e.target.value)} style={formInput} placeholder="e.g. create" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>object_type</label>
          <input value={object_type} onChange={(e) => setObjectType(e.target.value)} style={formInput} placeholder="e.g. notebook" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>object_id_expr</label>
          <input value={object_id_expr} onChange={(e) => setObjectIdExpr(e.target.value)} style={formInput} placeholder="optional regex/expr" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>object_name_expr</label>
          <input value={object_name_expr} onChange={(e) => setObjectNameExpr(e.target.value)} style={formInput} placeholder="optional" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>violation_type</label>
          <input value={violation_type} onChange={(e) => setViolationType(e.target.value)} style={formInput} placeholder="e.g. UNAPPROVED_CREATION" />
        </div>
        <div style={formRow}>
          <label style={formLabel}>remediation_action</label>
          <input value={remediation_action} onChange={(e) => setRemediationAction(e.target.value)} style={formInput} placeholder="e.g. DELETE_RESOURCE" />
        </div>
        <div style={formRow}>
          <label style={{ ...formLabel, display: 'flex', alignItems: 'center' }}>
            <input type="checkbox" checked={is_active} onChange={(e) => setIsActive(e.target.checked)} style={formCheckbox} />
            is_active
          </label>
        </div>
        <div style={formRow}>
          <label style={formLabel}>description</label>
          <textarea value={description} onChange={(e) => setDescription(e.target.value)} style={formInput} rows={2} placeholder="optional" />
        </div>
        <div style={buttonRow}>
          <button type="button" style={btnPrimary} onClick={handleSubmit} disabled={saving || !filter_name.trim()}>{saving ? 'Saving…' : 'Save'}</button>
          <button type="button" style={btnSecondary} onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}
