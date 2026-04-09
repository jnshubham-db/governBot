import { lazy, Suspense } from 'react'
import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import { LoadingIndicator } from './components/LoadingIndicator'

const Summary = lazy(() => import('./pages/Summary'))
const ActionsCenter = lazy(() => import('./pages/ActionsCenter'))
const Configs = lazy(() => import('./pages/Configs'))

export default function App() {
  return (
    <Layout>
      <Suspense fallback={<LoadingIndicator size={36} color="white" />}>
        <Routes>
          <Route path="/" element={<Summary />} />
          <Route path="/summary" element={<Summary />} />
          <Route path="/actions" element={<ActionsCenter />} />
          <Route path="/configs" element={<Configs />} />
        </Routes>
      </Suspense>
    </Layout>
  )
}
