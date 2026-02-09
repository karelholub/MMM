import { useState, useEffect, useCallback, useMemo, lazy, Suspense } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'

const ChannelPerformance = lazy(() => import('./ChannelPerformance'))
const AttributionComparison = lazy(() => import('./AttributionComparison'))
const ConversionPaths = lazy(() => import('./ConversionPaths'))
const ExpenseManager = lazy(() => import('./ExpenseManager'))
const DataSources = lazy(() => import('./DataSources'))
const DatasetUploader = lazy(() => import('./DatasetUploader'))
const MMMDashboard = lazy(() => import('./MMMDashboard'))
const BudgetOptimizer = lazy(() => import('./BudgetOptimizer'))
const CampaignPerformance = lazy(() => import('./CampaignPerformance'))

type Page = 'dashboard' | 'comparison' | 'paths' | 'campaigns' | 'expenses' | 'datasources' | 'mmm'

const PAGE_FALLBACK = (
  <div style={{ padding: 48, textAlign: 'center', color: '#64748b', fontSize: 14 }}>
    Loading…
  </div>
)

const NAV_ITEMS: { key: Page; label: string; color: string }[] = [
  { key: 'dashboard', label: 'Channel Performance', color: '#007bff' },
  { key: 'campaigns', label: 'Campaign Performance', color: '#0d9488' },
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

const LAYOUT_STYLES = {
  root: { fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif', minHeight: '100vh', backgroundColor: '#f4f6f9' as const },
  header: { background: 'linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%)', color: 'white', padding: '20px 32px', display: 'flex' as const, justifyContent: 'space-between' as const, alignItems: 'center' as const },
  nav: { display: 'flex' as const, flexWrap: 'wrap' as const, gap: 8, padding: '12px 32px', alignItems: 'center' as const, backgroundColor: 'white', borderBottom: '1px solid #e9ecef', boxShadow: '0 1px 3px rgba(0,0,0,0.05)' },
  main: { padding: '24px 32px', maxWidth: 1400, margin: '0 auto' as const },
} as const

export default function App() {
  const [page, setPage] = useState<Page>('dashboard')
  const [selectedModel, setSelectedModel] = useState('linear')
  const [mmmRunId, setMmmRunId] = useState<string | null>(null)
  const [mmmDatasetId, setMmmDatasetId] = useState<string | null>(null)

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/journeys')
      if (!res.ok) return { loaded: false, count: 0 }
      return res.json()
    },
    refetchInterval: 15 * 1000,
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

  const createMmmRunMutation = useMutation({
    mutationFn: async (config: {
      dataset_id: string
      kpi: string
      spend_channels: string[]
      covariates: string[]
    }) => {
      const res = await fetch('/api/models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dataset_id: config.dataset_id,
          frequency: 'W',
          kpi_mode: 'conversions',
          kpi: config.kpi,
          spend_channels: config.spend_channels,
          covariates: config.covariates || [],
        }),
      })
      if (!res.ok) throw new Error('Failed to start MMM run')
      return res.json()
    },
    onSuccess: (data, variables) => {
      setMmmRunId(data.run_id)
      setMmmDatasetId(variables.dataset_id)
    },
  })

  const mmmRunQuery = useQuery({
    queryKey: ['mmm-run', mmmRunId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${mmmRunId}`)
      if (!res.ok) throw new Error('Failed to fetch run')
      return res.json()
    },
    enabled: !!mmmRunId,
    refetchInterval: (query) => {
      const data = query.state?.data as { status?: string } | undefined
      return data?.status === 'finished' || data?.status === 'error' ? false : 2000
    },
  })

  useEffect(() => {
    if (journeysQuery.data?.loaded && !runAllMutation.data && !runAllMutation.isPending) {
      runAllMutation.mutate()
    }
  }, [journeysQuery.data?.loaded])

  const journeysLoaded = journeysQuery.data?.loaded ?? false
  const journeyCount = journeysQuery.data?.count ?? 0
  const convertedCount = journeysQuery.data?.converted ?? 0
  const channels = useMemo(() => journeysQuery.data?.channels ?? [], [journeysQuery.data?.channels])

  const handleSetPage = useCallback((p: Page) => setPage(p), [])
  const handleSetDatasources = useCallback(() => setPage('datasources'), [])
  const handleMmmStartOver = useCallback(() => {
    setMmmRunId(null)
    setMmmDatasetId(null)
  }, [])
  const onJourneysImported = useCallback(() => {
    void journeysQuery.refetch()
    runAllMutation.mutate()
  }, [])
  const onMmmMappingComplete = useCallback(
    (mapping: { dataset_id: string; columns: { kpi: string; spend_channels: string[]; covariates?: string[] } }) => {
      createMmmRunMutation.mutate({
        dataset_id: mapping.dataset_id,
        kpi: mapping.columns.kpi,
        spend_channels: mapping.columns.spend_channels,
        covariates: mapping.columns.covariates ?? [],
      })
    },
    [createMmmRunMutation],
  )

  return (
    <div style={LAYOUT_STYLES.root}>
      {/* Header */}
      <header style={LAYOUT_STYLES.header}>
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
      <nav style={LAYOUT_STYLES.nav}>
        {NAV_ITEMS.map(item => (
          <button
            key={item.key}
            onClick={() => handleSetPage(item.key)}
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

        {(page === 'dashboard' || page === 'comparison' || page === 'campaigns') && (
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
      <main style={LAYOUT_STYLES.main}>
        {!journeysLoaded && page !== 'datasources' && page !== 'expenses' && page !== 'mmm' && page !== 'comparison' && page !== 'paths' && page !== 'campaigns' && (
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
                onClick={handleSetDatasources}
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

        {(journeysLoaded || page === 'datasources' || page === 'expenses' || page === 'mmm' || page === 'comparison' || page === 'paths' || page === 'campaigns') && (
          <Suspense fallback={PAGE_FALLBACK}>
            {page === 'dashboard' && (
              <ChannelPerformance model={selectedModel} channels={channels} modelsReady={!!runAllMutation.data} />
            )}
            {page === 'campaigns' && (
              <CampaignPerformance model={selectedModel} modelsReady={!!runAllMutation.data} />
            )}
            {page === 'comparison' && (
              <AttributionComparison selectedModel={selectedModel} onSelectModel={setSelectedModel} />
            )}
            {page === 'paths' && <ConversionPaths />}
            {page === 'expenses' && <ExpenseManager />}
            {page === 'datasources' && (
              <DataSources onJourneysImported={onJourneysImported} />
            )}
            {page === 'mmm' && (
              <div style={{ maxWidth: 1400, margin: '0 auto' }}>
                {!mmmRunId ? (
                  <>
                    <div style={{ marginBottom: tokens.space.xl }}>
                      <h1 style={{ margin: 0, fontSize: tokens.font.size2xl, fontWeight: tokens.font.weightBold, color: tokens.color.text, letterSpacing: '-0.02em' }}>
                        Marketing Mix Modeling (MMM)
                      </h1>
                      <p style={{ margin: `${tokens.space.xs}px 0 0`, fontSize: tokens.font.sizeSm, color: tokens.color.textSecondary }}>
                        Upload weekly spend + KPI data, map columns, then run a Bayesian MMM. Results include ROI by channel, contributions, and budget optimization.
                      </p>
                    </div>
                    <DatasetUploader onMappingComplete={onMmmMappingComplete} />
                    {createMmmRunMutation.isPending && (
                      <div style={{ marginTop: tokens.space.xl, padding: tokens.space.xl, textAlign: 'center', backgroundColor: tokens.color.bg, borderRadius: tokens.radius.lg, border: `1px solid ${tokens.color.border}` }}>
                        <p style={{ fontWeight: tokens.font.weightSemibold, color: tokens.color.text, margin: 0 }}>Starting model run…</p>
                        <p style={{ fontSize: tokens.font.sizeSm, color: tokens.color.textSecondary, marginTop: tokens.space.sm }}>You will be taken to results when the run is queued.</p>
                      </div>
                    )}
                  </>
                ) : (
                  <>
                    {(mmmRunQuery.isLoading || mmmRunQuery.data?.status === 'queued' || mmmRunQuery.data?.status === 'running') && (
                      <div style={{ marginBottom: tokens.space.xl, padding: tokens.space.xl, textAlign: 'center', backgroundColor: tokens.color.warningMuted, borderRadius: tokens.radius.lg, border: `1px solid ${tokens.color.warning}` }}>
                        <p style={{ fontWeight: tokens.font.weightSemibold, color: tokens.color.warning, margin: 0 }}>Model {mmmRunQuery.data?.status === 'running' ? 'running' : 'queued'}…</p>
                        <p style={{ fontSize: tokens.font.sizeSm, color: tokens.color.warning, marginTop: tokens.space.sm }}>This may take a few minutes. The page will update automatically.</p>
                      </div>
                    )}
                    {mmmRunQuery.data?.status === 'error' && (
                      <div style={{ marginBottom: tokens.space.xl, padding: tokens.space.xl, backgroundColor: tokens.color.dangerMuted, borderRadius: tokens.radius.lg, border: `1px solid ${tokens.color.danger}` }}>
                        <p style={{ fontWeight: tokens.font.weightSemibold, color: tokens.color.danger, margin: 0 }}>Model run failed</p>
                        <p style={{ fontSize: tokens.font.sizeSm, color: tokens.color.danger, marginTop: tokens.space.sm }}>{String((mmmRunQuery.data as { detail?: string }).detail ?? 'Unknown error')}</p>
                        <button
                          type="button"
                          onClick={handleMmmStartOver}
                          style={{ marginTop: tokens.space.md, padding: `${tokens.space.sm}px ${tokens.space.lg}px`, fontSize: tokens.font.sizeSm, backgroundColor: tokens.color.textMuted, color: tokens.color.surface, border: 'none', borderRadius: tokens.radius.sm, cursor: 'pointer', fontWeight: tokens.font.weightMedium }}
                        >
                          Start over
                        </button>
                      </div>
                    )}
                    {mmmRunQuery.data?.status === 'finished' && (
                      <>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.xl, flexWrap: 'wrap', gap: tokens.space.md }}>
                          <h2 style={{ margin: 0, fontSize: tokens.font.size2xl, fontWeight: tokens.font.weightBold, color: tokens.color.text }}>MMM Results</h2>
                          <button
                            type="button"
                            onClick={handleMmmStartOver}
                            style={{ padding: `${tokens.space.sm}px ${tokens.space.lg}px`, fontSize: tokens.font.sizeSm, fontWeight: tokens.font.weightMedium, color: tokens.color.surface, backgroundColor: tokens.color.textSecondary, border: 'none', borderRadius: tokens.radius.sm, cursor: 'pointer' }}
                          >
                            New model run
                          </button>
                        </div>
                        <MMMDashboard runId={mmmRunId!} datasetId={mmmDatasetId ?? ''} />
                        {mmmRunQuery.data?.roi?.length > 0 && mmmRunQuery.data?.contrib?.length > 0 && (
                          <BudgetOptimizer
                            roiData={mmmRunQuery.data.roi}
                            contribData={mmmRunQuery.data.contrib}
                            runId={mmmRunId!}
                          />
                        )}
                      </>
                    )}
                  </>
                )}
              </div>
            )}
          </Suspense>
        )}
      </main>
    </div>
  )
}
