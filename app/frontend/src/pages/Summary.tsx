import { useState, useEffect } from 'react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { getSummary, getSummaryTrend, type SummaryResponse } from '../api'

const HOURS_OPTIONS = [6, 12, 24, 72]
const PIE_COLORS = ['#00D4FF', '#22D3EE', '#06B6D4', '#0891B2', '#0E7490', '#155E75', '#164E63']

export default function Summary() {
  const [hours, setHours] = useState(24)
  const [data, setData] = useState<SummaryResponse | null>(null)
  const [trendData, setTrendData] = useState<{ period: string; generated: number; failed: number; completed: number }[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    setError(null)
    Promise.all([getSummary(hours), getSummaryTrend(hours)])
      .then(([summary, trend]) => {
        setData(summary)
        setTrendData(trend.trend || [])
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [hours])

  if (loading && !data) return <p>Loading...</p>
  if (error) return <div style={{ color: '#f87171' }}>Error: {error}</div>
  if (!data) return null

  return (
    <>
      <h1 style={{ marginTop: 0 }}>Summary</h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '1rem' }}>
        Violation summary for the last X hours. Set catalog, schema, and warehouse path in Configs.
      </p>
      <div style={{ marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        <label>Last</label>
        <select value={hours} onChange={(e) => setHours(Number(e.target.value))} style={{ width: 100 }}>
          {HOURS_OPTIONS.map((h) => (
            <option key={h} value={h}>{h} hours</option>
          ))}
        </select>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1rem', marginBottom: '2rem' }}>
        <div style={{ background: 'var(--bg-secondary)', padding: '1rem', borderRadius: 8 }}>
          <div style={{ color: 'var(--text-muted)', fontSize: '0.875rem' }}>Total violations</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700, color: 'var(--primary)' }}>{data.total}</div>
        </div>
        <div style={{ background: 'var(--bg-secondary)', padding: '1rem', borderRadius: 8 }}>
          <div style={{ color: 'var(--text-muted)', fontSize: '0.875rem' }}>Pending</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700 }}>{data.pending}</div>
        </div>
        <div style={{ background: 'var(--bg-secondary)', padding: '1rem', borderRadius: 8 }}>
          <div style={{ color: 'var(--text-muted)', fontSize: '0.875rem' }}>Completed</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700 }}>{data.completed}</div>
        </div>
      </div>
      <h2 style={{ marginBottom: '0.5rem' }}>Violations over time</h2>
      {trendData.length > 0 ? (
        <div style={{ height: 300, marginBottom: '2rem' }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={trendData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="period" stroke="#94A3B8" fontSize={12} tickFormatter={(v) => v.slice(5)} />
              <YAxis stroke="#94A3B8" fontSize={12} />
              <Tooltip contentStyle={{ background: 'var(--bg-secondary)', border: '1px solid #334155' }} />
              <Legend />
              <Line type="monotone" dataKey="generated" name="Generated" stroke="#00D4FF" strokeWidth={2} dot={{ r: 3 }} />
              <Line type="monotone" dataKey="failed" name="Failed" stroke="#F59E0B" strokeWidth={2} dot={{ r: 3 }} />
              <Line type="monotone" dataKey="completed" name="Completed" stroke="#2797a3" strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <p style={{ color: 'var(--text-muted)', marginBottom: '2rem' }}>No trend data in the selected period.</p>
      )}
      <h2 style={{ marginBottom: '0.5rem' }}>Recent Run Violations</h2>
      <h3 style={{ marginBottom: '0.5rem' }}>Violations by type</h3>
      {data.by_type.length > 0 ? (
        <div style={{ height: 280, marginBottom: '2rem' }}>
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={data.by_type}
                dataKey="count"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={100}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
              >
                {data.by_type.map((_, i) => (
                  <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: 'var(--bg-secondary)', border: '1px solid #334155' }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <p style={{ color: 'var(--text-muted)', marginBottom: '2rem' }}>No violations in the selected period.</p>
      )}
      {data.by_object_type.length > 0 && (
        <>
          <h3 style={{ marginBottom: '0.5rem' }}>Violations by object type</h3>
          <div style={{ height: 260, marginBottom: '2rem' }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.by_object_type}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis dataKey="name" stroke="#94A3B8" fontSize={12} />
                <YAxis stroke="#94A3B8" fontSize={12} />
                <Tooltip contentStyle={{ background: 'var(--bg-secondary)', border: '1px solid #334155' }} />
                <Bar dataKey="count" fill="var(--primary)" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
      <h3 style={{ marginBottom: '0.5rem' }}>Latest violations</h3>
      {data.latest.length > 0 ? (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                {['event_time', 'violation_type', 'object_type', 'object_name', 'user_email', 'processing_status'].map((k) => (
                  <th key={k} style={{ textAlign: 'left', padding: '0.5rem', fontSize: '0.875rem', color: 'var(--text-muted)' }}>{k}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.latest.slice(0, 20).map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #334155' }}>
                  {['event_time', 'violation_type', 'object_type', 'object_name', 'user_email', 'processing_status'].map((k) => (
                    <td key={k} style={{ padding: '0.5rem', fontSize: '0.875rem' }}>{String((row as Record<string, unknown>)[k] ?? '')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p style={{ color: 'var(--text-muted)' }}>No rows to show.</p>
      )}
    </>
  )
}
