import { useState, useEffect } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import ChannelPerformance from './ChannelPerformance'
import AttributionComparison from './AttributionComparison'
import ConversionPaths from './ConversionPaths'
import ExpenseManager from './ExpenseManager'
import DataSources from './DataSources'

type Page = 'dashboard' | 'comparison' | 'paths' | 'expenses' | 'datasources' | 'mmm'

const NAV_ITEMS: { key: Page; label: string; color: string }[] = [
  { key: 'dashboard', label: 'Channel Performance', color: '#007bff' },
  { key: 'comparison', label: 'Attribution Models', color: '#6f42c1' },
  { key: 'paths', label: 'Conversion Paths', color: '#17a2b8' },
  { key: 'expenses', label: 'Expenses', color: '#28a745' },
  { key: 'datasources', label: 'Data Sources', color: '#fd7e14' },
  { key: 'mmm', label: 'MMM (Advanced)', color: '#6c757d' },
]

const ATTRIBUTION_MODELS = [
  { id: 'last_touch', label: 'Last Touch' },
  { id: 'first_touch', label: 'First Touch' },
  { id: 'linear', label: 'Linear' },
  { id: 'time_decay', label: 'Time Decay' },
  { id: 'position_based', label: 'Position Based' },
  { id: 'markov', label: 'Data-Driven (Markov)' },
]

export default function App() {
  const [page, setPage] = useState<Page>('dashboard')
  const [selectedModel, setSelectedModel] = useState('linear')

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/journeys')
      if (!res.ok) return { loaded: false, count: 0 }
      return res.json()
    },
    refetchInterval: 5000,
  })

  const loadSampleMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/attribution/journeys/load-sample', { method: 'POST' })
      if (!res.ok) throw new Error('Failed to load sample')
      return res.json()
    },
    onSuccess: () => {
      journeysQuery.refetch()
      runAllMutation.mutate()
    },
  })

  const runAllMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/attribution/run-all', { method: 'POST' })
      if (!res.ok) throw new Error('Failed to run models')
      return res.json()
    },
  })

  useEffect(() => {
    if (journeysQuery.data?.loaded && !runAllMutation.data && !runAllMutation.isPending) {
      runAllMutation.mutate()
    }
  }, [journeysQuery.data?.loaded])

  const journeysLoaded = journeysQuery.data?.loaded || false
  const journeyCount = journeysQuery.data?.count || 0
  const convertedCount = journeysQuery.data?.converted || 0
  const channels: string[] = journeysQuery.data?.channels || []

  return (
    <div style={{ fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif', minHeight: '100vh', backgroundColor: '#f4f6f9' }}>
      {/* Header */}
      <header style={{
        background: 'linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%)',
        color: 'white',
        padding: '20px 32px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <div>
          <h1 style={{ margin: 0, fontSize: '24px', fontWeight: '700', letterSpacing: '-0.5px' }}>
            Meiro Attribution Dashboard
          </h1>
          <p style={{ margin: '4px 0 0', fontSize: '13px', color: 'rgba(255,255,255,0.6)' }}>
            Multi-touch attribution & channel performance analytics
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <div style={{
            padding: '6px 14px',
            borderRadius: 20,
            fontSize: '13px',
            fontWeight: '600',
            backgroundColor: journeysLoaded ? 'rgba(40,167,69,0.2)' : 'rgba(255,193,7,0.2)',
            color: journeysLoaded ? '#82e89f' : '#ffc107',
            border: `1px solid ${journeysLoaded ? 'rgba(40,167,69,0.4)' : 'rgba(255,193,7,0.4)'}`,
          }}>
            {journeysLoaded ? `${journeyCount} journeys (${convertedCount} converted)` : 'No data loaded'}
          </div>
          {!journeysLoaded && (
            <button
              onClick={() => loadSampleMutation.mutate()}
              disabled={loadSampleMutation.isPending}
              style={{
                padding: '8px 16px', borderRadius: 6, fontSize: '13px', fontWeight: '600',
                backgroundColor: '#28a745', color: 'white', border: 'none',
                cursor: loadSampleMutation.isPending ? 'wait' : 'pointer',
                opacity: loadSampleMutation.isPending ? 0.7 : 1,
              }}
            >
              {loadSampleMutation.isPending ? 'Loading...' : 'Load Sample Data'}
            </button>
          )}
          {journeysLoaded && (
            <button
              onClick={() => runAllMutation.mutate()}
              disabled={runAllMutation.isPending}
              style={{
                padding: '8px 16px', borderRadius: 6, fontSize: '13px', fontWeight: '600',
                backgroundColor: '#6f42c1', color: 'white', border: 'none',
                cursor: runAllMutation.isPending ? 'wait' : 'pointer',
                opacity: runAllMutation.isPending ? 0.7 : 1,
              }}
            >
              {runAllMutation.isPending ? 'Running...' : 'Re-run All Models'}
            </button>
          )}
        </div>
      </header>

      {/* Navigation */}
      <nav style={{
        display: 'flex', gap: 4, padding: '12px 32px',
        backgroundColor: 'white', borderBottom: '1px solid #e9ecef',
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
      }}>
        {NAV_ITEMS.map(item => (
          <button
            key={item.key}
            onClick={() => setPage(item.key)}
            style={{
              padding: '10px 20px', fontSize: '14px',
              fontWeight: page === item.key ? '700' : '500',
              backgroundColor: page === item.key ? item.color : 'transparent',
              color: page === item.key ? 'white' : '#495057',
              border: page === item.key ? 'none' : '1px solid #dee2e6',
              borderRadius: 6, cursor: 'pointer', transition: 'all 0.2s',
            }}
          >
            {item.label}
          </button>
        ))}

        {(page === 'dashboard' || page === 'comparison') && (
          <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8 }}>
            <label style={{ fontSize: '13px', fontWeight: '600', color: '#6c757d' }}>Model:</label>
            <select
              value={selectedModel}
              onChange={(e) => setSelectedModel(e.target.value)}
              style={{
                padding: '8px 12px', fontSize: '13px', border: '2px solid #6f42c1',
                borderRadius: 6, fontWeight: '600', color: '#6f42c1', cursor: 'pointer',
              }}
            >
              {ATTRIBUTION_MODELS.map(m => (
                <option key={m.id} value={m.id}>{m.label}</option>
              ))}
            </select>
          </div>
        )}
      </nav>

      {/* Main Content */}
      <main style={{ padding: '24px 32px', maxWidth: 1400, margin: '0 auto' }}>
        {!journeysLoaded && page !== 'datasources' && page !== 'expenses' && (
          <div style={{
            padding: 40, textAlign: 'center', backgroundColor: 'white',
            borderRadius: 12, border: '1px solid #e9ecef',
          }}>
            <h2 style={{ fontSize: '22px', color: '#495057', marginBottom: 12 }}>Welcome to the Attribution Dashboard</h2>
            <p style={{ fontSize: '15px', color: '#6c757d', marginBottom: 24, maxWidth: 600, margin: '0 auto 24px' }}>
              Load conversion path data to start analyzing channel performance.
              You can load sample data to explore, upload your own JSON, or import from Meiro CDP.
            </p>
            <div style={{ display: 'flex', gap: 12, justifyContent: 'center' }}>
              <button
                onClick={() => loadSampleMutation.mutate()}
                disabled={loadSampleMutation.isPending}
                style={{
                  padding: '14px 28px', fontSize: '15px', fontWeight: '600',
                  backgroundColor: '#28a745', color: 'white', border: 'none',
                  borderRadius: 8, cursor: 'pointer',
                }}
              >
                Load Sample Data
              </button>
              <button
                onClick={() => setPage('datasources')}
                style={{
                  padding: '14px 28px', fontSize: '15px', fontWeight: '600',
                  backgroundColor: '#fd7e14', color: 'white', border: 'none',
                  borderRadius: 8, cursor: 'pointer',
                }}
              >
                Connect Data Sources
              </button>
            </div>
          </div>
        )}

        {(journeysLoaded || page === 'datasources' || page === 'expenses') && (
          <>
            {page === 'dashboard' && (
              <ChannelPerformance model={selectedModel} channels={channels} modelsReady={!!runAllMutation.data} />
            )}
            {page === 'comparison' && (
              <AttributionComparison selectedModel={selectedModel} onSelectModel={setSelectedModel} />
            )}
            {page === 'paths' && <ConversionPaths />}
            {page === 'expenses' && <ExpenseManager />}
            {page === 'datasources' && (
              <DataSources onJourneysImported={() => { journeysQuery.refetch(); runAllMutation.mutate() }} />
            )}
            {page === 'mmm' && (
              <div style={{ padding: 32, textAlign: 'center', backgroundColor: 'white', borderRadius: 12, border: '1px solid #e9ecef' }}>
                <h2 style={{ color: '#495057' }}>Marketing Mix Modeling (Advanced)</h2>
                <p style={{ color: '#6c757d' }}>
                  The Bayesian MMM engine (PyMC-Marketing) is available for aggregate-level media mix modeling.
                  Use the Dataset Wizard to upload weekly spend + KPI data and run a full Bayesian model.
                </p>
                <p style={{ fontSize: '13px', color: '#adb5bd', marginTop: 16 }}>
                  This feature complements attribution. While attribution traces individual
                  customer journeys, MMM provides aggregate-level insights about channel effectiveness.
                </p>
              </div>
            )}
          </>
        )}
      </main>
    </div>
  )
}
