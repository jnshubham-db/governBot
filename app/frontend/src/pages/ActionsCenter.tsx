import { useState, useEffect } from 'react'
import { getActions, approveViolation, rejectViolation, addNoteViolation, type ActionRow } from '../api'

export default function ActionsCenter() {
  const [rows, setRows] = useState<ActionRow[]>([])
  const [workspaces, setWorkspaces] = useState<string[]>(['(all)'])
  const [violationTypes, setViolationTypes] = useState<string[]>(['(all)'])
  const [remediationActions, setRemediationActions] = useState<string[]>(['(all)'])
  const [filterWorkspace, setFilterWorkspace] = useState('(all)')
  const [filterViolationType, setFilterViolationType] = useState('(all)')
  const [filterRemediation, setFilterRemediation] = useState('(all)')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [actionModal, setActionModal] = useState<{ type: 'approve' | 'reject' | 'note'; row: ActionRow } | null>(null)
  const [noteText, setNoteText] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const load = () => {
    setLoading(true)
    setError(null)
    getActions({
      workspace: filterWorkspace !== '(all)' ? filterWorkspace : undefined,
      violation_type: filterViolationType !== '(all)' ? filterViolationType : undefined,
      remediation_action: filterRemediation !== '(all)' ? filterRemediation : undefined,
    })
      .then((res) => {
        setRows(res.rows)
        const ws = [...new Set(res.rows.map((r) => String(r.workspace_id || '')))].filter(Boolean)
        setWorkspaces(['(all)', ...ws].filter((w, i, a) => a.indexOf(w) === i))
        const vt = [...new Set(res.rows.map((r) => String(r.violation_type || '')))].filter(Boolean)
        setViolationTypes(['(all)', ...vt].filter((v, i, a) => a.indexOf(v) === i))
        const ra = [...new Set(res.rows.map((r) => String(r.remediation_action || '')))].filter(Boolean)
        setRemediationActions(['(all)', ...ra].filter((r, i, a) => a.indexOf(r) === i))
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
  }, [filterWorkspace, filterViolationType, filterRemediation])

  const handleApprove = async () => {
    if (!actionModal) return
    setSubmitting(true)
    try {
      await approveViolation(String(actionModal.row.violation_id), noteText)
      setActionModal(null)
      setNoteText('')
      load()
    } catch (e) {
      setError(String(e))
    } finally {
      setSubmitting(false)
    }
  }

  const handleReject = async () => {
    if (!actionModal) return
    setSubmitting(true)
    try {
      await rejectViolation(String(actionModal.row.violation_id), noteText || 'Rejected by user')
      setActionModal(null)
      setNoteText('')
      load()
    } catch (e) {
      setError(String(e))
    } finally {
      setSubmitting(false)
    }
  }

  const handleNote = async () => {
    if (!actionModal) return
    setSubmitting(true)
    try {
      await addNoteViolation(String(actionModal.row.violation_id), noteText)
      setActionModal(null)
      setNoteText('')
      load()
    } catch (e) {
      setError(String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <>
      <h1 style={{ marginTop: 0 }}>Actions Center</h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '1rem' }}>
        Pending violations: approve, reject, or add a note.
      </p>
      {error && <div style={{ color: '#f87171', marginBottom: '1rem' }}>{error}</div>}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1rem', marginBottom: '1rem' }}>
        <div>
          <label>Workspace</label>
          <select value={filterWorkspace} onChange={(e) => setFilterWorkspace(e.target.value)} style={{ width: '100%' }}>
            {workspaces.map((w) => (
              <option key={w} value={w}>{w}</option>
            ))}
          </select>
        </div>
        <div>
          <label>Violation type</label>
          <select value={filterViolationType} onChange={(e) => setFilterViolationType(e.target.value)} style={{ width: '100%' }}>
            {violationTypes.map((v) => (
              <option key={v} value={v}>{v}</option>
            ))}
          </select>
        </div>
        <div>
          <label>Remediation action</label>
          <select value={filterRemediation} onChange={(e) => setFilterRemediation(e.target.value)} style={{ width: '100%' }}>
            {remediationActions.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
      </div>
      {loading && !rows.length ? (
        <p>Loading...</p>
      ) : rows.length === 0 ? (
        <p style={{ color: 'var(--text-muted)' }}>No pending violations.</p>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Event time</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Type</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Object</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>User</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Status</th>
                <th style={{ textAlign: 'left', padding: '0.5rem' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #334155' }}>
                  <td style={{ padding: '0.5rem' }}>{String(row.event_time ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.violation_type ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.object_name ?? row.object_type ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.user_email ?? '')}</td>
                  <td style={{ padding: '0.5rem' }}>{String(row.ca_status ?? row.processing_status ?? '')}</td>
                  <td style={{ padding: '0.5rem', display: 'flex', gap: '0.5rem' }}>
                    <button type="button" onClick={() => setActionModal({ type: 'approve', row })}>Approve</button>
                    <button type="button" className="secondary" onClick={() => setActionModal({ type: 'reject', row })}>Reject</button>
                    <button type="button" className="secondary" onClick={() => { setNoteText(String(row.remediation_details ?? '')); setActionModal({ type: 'note', row }); }}>Add note</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {actionModal && (
        <div style={{
          position: 'fixed',
          inset: 0,
          background: 'rgba(0,0,0,0.6)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 100,
        }}>
          <div style={{
            background: 'var(--bg-secondary)',
            padding: '1.5rem',
            borderRadius: 8,
            minWidth: 360,
            border: '1px solid var(--primary)',
          }}>
            <h3 style={{ marginTop: 0 }}>{actionModal.type === 'approve' ? 'Approve' : actionModal.type === 'reject' ? 'Reject' : 'Add note'}</h3>
            <p style={{ fontSize: '0.875rem', color: 'var(--text-muted)' }}>Violation ID: {String(actionModal.row.violation_id)}</p>
            <label>Note / reason</label>
            <textarea value={noteText} onChange={(e) => setNoteText(e.target.value)} rows={3} style={{ width: '100%', marginBottom: '1rem' }} />
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
              <button type="button" className="secondary" onClick={() => { setActionModal(null); setNoteText(''); }}>Cancel</button>
              {actionModal.type === 'approve' && <button type="button" onClick={handleApprove} disabled={submitting}>{submitting ? 'Saving...' : 'Confirm Approve'}</button>}
              {actionModal.type === 'reject' && <button type="button" onClick={handleReject} disabled={submitting}>{submitting ? 'Saving...' : 'Confirm Reject'}</button>}
              {actionModal.type === 'note' && <button type="button" onClick={handleNote} disabled={submitting}>{submitting ? 'Saving...' : 'Save note'}</button>}
            </div>
          </div>
        </div>
      )}
    </>
  )
}
