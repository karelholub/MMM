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
const SettingsPage = lazy(() => import('./Settings'))
const DataQuality = lazy(() => import('./DataQuality'))
const IncrementalityPage = lazy(() => import('./Incrementality'))
const PathArchetypes = lazy(() => import('./PathArchetypes'))

type Page =
  | 'dashboard'
  | 'comparison'
  | 'paths'
  | 'campaigns'
  | 'expenses'
  | 'datasources'
  | 'mmm'
  | 'settings'
  | 'dq'
  | 'incrementality'
  | 'path_archetypes'

const PAGE_FALLBACK = (
  <div style={{ padding: 48, textAlign: 'center', color: '#64748b', fontSize: 14 }}>
    Loading…
  </div>
)

const NAV_ITEMS: { key: Page; label: string }[] = [
  { key: 'dashboard', label: 'Channel performance' },
  { key: 'campaigns', label: 'Campaign performance' },
  { key: 'comparison', label: 'Attribution models' },
  { key: 'paths', label: 'Conversion paths' },
  { key: 'path_archetypes', label: 'Path archetypes' },
  { key: 'mmm', label: 'MMM (advanced)' },
  { key: 'incrementality', label: 'Incrementality' },
  { key: 'dq', label: 'Data quality' },
  { key: 'expenses', label: 'Expenses' },
  { key: 'datasources', label: 'Data sources' },
  { key: 'settings', label: 'Settings' },
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
  root: {
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    minHeight: '100vh',
    backgroundColor: tokens.color.bg as const,
  },
  header: {
    background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #1d4ed8 100%)',
    color: '#ffffff',
    padding: '16px 32px',
    display: 'flex' as const,
    justifyContent: 'space-between' as const,
    alignItems: 'center' as const,
    borderBottom: `1px solid rgba(148,163,184,0.4)`,
  },
  nav: {
    display: 'flex' as const,
    flexWrap: 'wrap' as const,
    gap: 8,
    padding: '10px 32px',
    alignItems: 'center' as const,
    backgroundColor: tokens.color.surface,
    borderBottom: `1px solid ${tokens.color.borderLight}`,
    boxShadow: tokens.shadowSm,
  },
  main: {
    padding: '24px 32px',
    maxWidth: 1400,
    margin: '0 auto' as const,
  },
} as const

export default function App() {
  const [page, setPage] = useState<Page>('dashboard')
  const [selectedModel, setSelectedModel] = useState('linear')
  const [mmmRunId, setMmmRunId] = useState<string | null>(null)
  const [mmmDatasetId, setMmmDatasetId] = useState<string | null>(null)
  const [selectedConfigId, setSelectedConfigId] = useState<string | null>(null)

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/journeys')
      if (!res.ok) return { loaded: false, count: 0 }
      return res.json()
    },
    refetchInterval: 15 * 1000,
  })

  const modelConfigsQuery = useQuery({
    queryKey: ['model-configs'],
    queryFn: async () => {
      const res = await fetch('/api/model-configs')
      if (!res.ok) throw new Error('Failed to load model configs')
      return res.json() as Promise<
        { id: string; name: string; status: string; version: number; activated_at?: string | null }[]
      >
    },
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
  const primaryKpiLabel: string | undefined = journeysQuery.data?.primary_kpi_label
  const primaryKpiCount: number | undefined = journeysQuery.data?.primary_kpi_count
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
          <h1
            style={{
              margin: 0,
              fontSize: tokens.font.size2xl,
              fontWeight: tokens.font.weightBold,
              letterSpacing: '-0.04em',
            }}
          >
            Meiro measurement workspace
          </h1>
          <p
            style={{
              margin: '4px 0 0',
              fontSize: tokens.font.sizeSm,
              color: 'rgba(226,232,240,0.8)',
            }}
          >
            Unified attribution, incrementality, and MMM for Meiro CDP data.
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          <div
            style={{
              padding: '6px 14px',
              borderRadius: 999,
              fontSize: tokens.font.sizeSm,
              fontWeight: tokens.font.weightSemibold,
              backgroundColor: journeysLoaded ? 'rgba(22,163,74,0.18)' : 'rgba(234,179,8,0.16)',
              color: journeysLoaded ? '#bbf7d0' : '#facc15',
              border: `1px solid ${journeysLoaded ? 'rgba(34,197,94,0.5)' : 'rgba(234,179,8,0.5)'}`,
              whiteSpace: 'nowrap',
            }}
          >
            {journeysLoaded
              ? primaryKpiLabel
                ? `${journeyCount} journeys · ${primaryKpiLabel}: ${primaryKpiCount ?? convertedCount}`
                : `${journeyCount} journeys · ${convertedCount} converted`
              : 'No journeys loaded'}
          </div>
          {!journeysLoaded && (
            <button
              onClick={() => loadSampleMutation.mutate()}
              disabled={loadSampleMutation.isPending}
              style={{
                padding: '8px 16px',
                borderRadius: 999,
                fontSize: tokens.font.sizeSm,
                fontWeight: tokens.font.weightSemibold,
                backgroundColor: tokens.color.accent,
                color: '#ffffff',
                border: 'none',
                cursor: loadSampleMutation.isPending ? 'wait' : 'pointer',
                opacity: loadSampleMutation.isPending ? 0.7 : 1,
              }}
            >
              {loadSampleMutation.isPending ? 'Loading sample…' : 'Load sample journeys'}
            </button>
          )}
          {journeysLoaded && (
            <button
              onClick={() => runAllMutation.mutate()}
              disabled={runAllMutation.isPending}
              style={{
                padding: '8px 16px',
                borderRadius: 999,
                fontSize: tokens.font.sizeSm,
                fontWeight: tokens.font.weightSemibold,
                backgroundColor: 'rgba(15,23,42,0.6)',
                color: '#e5e7eb',
                border: '1px solid rgba(148,163,184,0.6)',
                cursor: runAllMutation.isPending ? 'wait' : 'pointer',
                opacity: runAllMutation.isPending ? 0.7 : 1,
              }}
            >
              {runAllMutation.isPending ? 'Running models…' : 'Re-run attribution models'}
            </button>
          )}
        </div>
      </header>

      {/* Navigation */}
      <nav style={LAYOUT_STYLES.nav}>
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: 6,
            alignItems: 'center',
          }}
        >
          {NAV_ITEMS.map((item) => (
            <button
              key={item.key}
              onClick={() => handleSetPage(item.key)}
              style={{
                padding: '8px 14px',
                fontSize: tokens.font.sizeSm,
                fontWeight: page === item.key ? tokens.font.weightSemibold : tokens.font.weightMedium,
                backgroundColor: page === item.key ? tokens.color.accent : 'transparent',
                color: page === item.key ? '#ffffff' : tokens.color.textSecondary,
                borderRadius: tokens.radius.sm,
                border: page === item.key ? 'none' : `1px solid ${tokens.color.borderLight}`,
                cursor: 'pointer',
                transition: 'background-color 0.15s ease, color 0.15s ease, border-color 0.15s ease',
              }}
            >
              {item.label}
            </button>
          ))}
        </div>

        {(page === 'dashboard' || page === 'comparison' || page === 'campaigns') && (
          <div
            style={{
              marginLeft: 'auto',
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              flexWrap: 'wrap',
            }}
          >
            <label
              style={{
                fontSize: tokens.font.sizeSm,
                fontWeight: tokens.font.weightMedium,
                color: tokens.color.textSecondary,
              }}
            >
              Model
            </label>
            <select
              value={selectedModel}
              onChange={(e) => setSelectedModel(e.target.value)}
              style={{
                padding: '6px 10px',
                fontSize: tokens.font.sizeSm,
                border: `1px solid ${tokens.color.border}`,
                borderRadius: tokens.radius.sm,
                fontWeight: tokens.font.weightMedium,
                color: tokens.color.text,
                cursor: 'pointer',
                backgroundColor: '#ffffff',
              }}
            >
              {ATTRIBUTION_MODELS.map((m) => (
                <option key={m.id} value={m.id}>
                  {m.label}
                </option>
              ))}
            </select>
            <label
              style={{
                fontSize: tokens.font.sizeSm,
                fontWeight: tokens.font.weightMedium,
                color: tokens.color.textSecondary,
                marginLeft: 4,
              }}
            >
              Config
            </label>
            <select
              value={selectedConfigId ?? ''}
              onChange={(e) => setSelectedConfigId(e.target.value || null)}
              style={{
                padding: '6px 10px',
                fontSize: tokens.font.sizeSm,
                border: `1px solid ${tokens.color.border}`,
                borderRadius: tokens.radius.sm,
                fontWeight: tokens.font.weightMedium,
                color: tokens.color.text,
                cursor: 'pointer',
                maxWidth: 260,
                backgroundColor: '#ffffff',
              }}
            >
              <option value="">Default active</option>
              {(modelConfigsQuery.data ?? [])
                .filter((c) => c.status === 'active' || c.status === 'draft')
                .map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name} v{c.version} {c.status === 'active' ? '• active' : '(draft)'}
                  </option>
                ))}
            </select>
          </div>
        )}
      </nav>

      {/* Main Content */}
      <main style={LAYOUT_STYLES.main}>
        {!journeysLoaded && page !== 'datasources' && page !== 'expenses' && page !== 'mmm' && page !== 'comparison' && page !== 'paths' && page !== 'campaigns' && page !== 'settings' && page !== 'dq' && page !== 'incrementality' && (
          <div style={{
            padding: 40, maxWidth: 560, margin: '0 auto',
            backgroundColor: tokens.color.surface,
            borderRadius: tokens.radius.lg,
            border: `1px solid ${tokens.color.border}`,
            boxShadow: tokens.shadow,
          }}>
            <h2 style={{ fontSize: tokens.font.size2xl, color: tokens.color.text, marginBottom: tokens.space.sm, textAlign: 'center' }}>
              Welcome to the Attribution Dashboard
            </h2>
            <p style={{ fontSize: tokens.font.sizeBase, color: tokens.color.textSecondary, textAlign: 'center', marginBottom: tokens.space.xl }}>
              Get started in three steps:
            </p>
            <ol style={{ fontSize: tokens.font.sizeMd, color: tokens.color.text, marginBottom: tokens.space.xl, paddingLeft: 24, lineHeight: 1.8 }}>
              <li>Load conversion path data (sample, upload, or connect a data source).</li>
              <li>Run attribution models from the header, then open Channel or Campaign Performance.</li>
              <li>Use Conversion Paths for next-best-action and MMM for mix modeling.</li>
            </ol>
            <div style={{ display: 'flex', gap: tokens.space.md, justifyContent: 'center', flexWrap: 'wrap' }}>
              <button
                onClick={() => loadSampleMutation.mutate()}
                disabled={loadSampleMutation.isPending}
                style={{
                  padding: `${tokens.space.md}px ${tokens.space.xl}px`,
                  fontSize: tokens.font.sizeBase,
                  fontWeight: tokens.font.weightSemibold,
                  backgroundColor: tokens.color.success,
                  color: 'white',
                  border: 'none',
                  borderRadius: tokens.radius.sm,
                  cursor: loadSampleMutation.isPending ? 'wait' : 'pointer',
                }}
              >
                {loadSampleMutation.isPending ? 'Loading…' : 'Load sample data'}
              </button>
              <button
                onClick={handleSetDatasources}
                style={{
                  padding: `${tokens.space.md}px ${tokens.space.xl}px`,
                  fontSize: tokens.font.sizeBase,
                  fontWeight: tokens.font.weightSemibold,
                  backgroundColor: '#fd7e14',
                  color: 'white',
                  border: 'none',
                  borderRadius: tokens.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Connect data sources
              </button>
            </div>
          </div>
        )}

        {(journeysLoaded || page === 'datasources' || page === 'expenses' || page === 'mmm' || page === 'comparison' || page === 'paths' || page === 'campaigns' || page === 'settings' || page === 'dq' || page === 'incrementality') && (
          <Suspense fallback={PAGE_FALLBACK}>
            {page === 'dashboard' && (
              <ChannelPerformance model={selectedModel} channels={channels} modelsReady={!!runAllMutation.data} configId={selectedConfigId} />
            )}
            {page === 'campaigns' && (
              <CampaignPerformance model={selectedModel} modelsReady={!!runAllMutation.data} configId={selectedConfigId} />
            )}
            {page === 'comparison' && (
              <AttributionComparison selectedModel={selectedModel} onSelectModel={setSelectedModel} />
            )}
            {page === 'paths' && <ConversionPaths />}
            {page === 'path_archetypes' && <PathArchetypes />}
            {page === 'expenses' && <ExpenseManager />}
            {page === 'datasources' && (
              <DataSources onJourneysImported={onJourneysImported} />
            )}
            {page === 'settings' && <SettingsPage />}
            {page === 'dq' && <DataQuality />}
            {page === 'incrementality' && <IncrementalityPage />}
            {/* PathArchetypes can be linked from ConversionPaths via URL hash or future nav item */}
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
