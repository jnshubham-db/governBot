import { ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

const iconStyle = { width: 20, height: 20, flexShrink: 0 }

function IconSummary() {
  return (
    <svg style={iconStyle} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="18" y1="20" x2="18" y2="10" /><line x1="12" y1="20" x2="12" y2="4" /><line x1="6" y1="20" x2="6" y2="14" />
    </svg>
  )
}
function IconActions() {
  return (
    <svg style={iconStyle} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 11l3 3L22 4" /><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11" />
    </svg>
  )
}
function IconConfigs() {
  return (
    <svg style={iconStyle} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </svg>
  )
}

function NavItem({
  to,
  matchPaths,
  icon,
  label,
}: {
  to: string
  matchPaths: string[]
  icon: React.ReactNode
  label: string
}) {
  const location = useLocation()
  const isActive = matchPaths.some((p) => location.pathname === p || (p !== '/' && location.pathname.startsWith(p)))
  return (
    <NavLink
      to={to}
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.5rem',
        padding: '0.5rem 0.75rem',
        borderRadius: 6,
        color: isActive ? 'var(--bg)' : 'var(--text)',
        background: isActive ? 'var(--primary)' : 'transparent',
        textDecoration: 'none',
      }}
    >
      {icon}
      <span>{label}</span>
    </NavLink>
  )
}

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <aside style={{
        position: 'fixed',
        left: 0,
        top: 0,
        bottom: 0,
        width: 220,
        background: 'var(--bg-secondary)',
        padding: '1.5rem 1rem',
        borderRight: '1px solid #334155',
        overflowY: 'auto',
      }}>
        <h2 style={{ margin: '0 0 1rem', fontSize: '1.25rem' }}>🛡️ GovernBot</h2>
        <nav style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
          <NavItem to="/summary" matchPaths={['/', '/summary']} icon={<IconSummary />} label="Summary" />
          <NavItem to="/actions" matchPaths={['/actions']} icon={<IconActions />} label="Actions Center" />
          <NavItem to="/configs" matchPaths={['/configs']} icon={<IconConfigs />} label="Configs" />
        </nav>
      </aside>
      <main style={{ flex: 1, marginLeft: 220, padding: '1.5rem 2rem', overflow: 'auto', minHeight: '100vh' }}>
        {children}
      </main>
    </div>
  )
}
