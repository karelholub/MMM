import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, UseMutationResult, UseQueryResult } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import MMMDataSourceStep from './MMMDataSourceStep'
import MMMRunConfigStep from './MMMRunConfigStep'
import MMMDashboard from './MMMDashboard'
import BudgetOptimizer from './BudgetOptimizer'
import { MMMContextBar, KpiMode } from '../components/MMMContextBar'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import { apiGetJson } from '../lib/apiClient'
import { evaluateMMMRunQuality, MMMQualityLevel } from '../lib/mmmQuality'
import { navigateForRecommendedAction } from '../lib/recommendedActions'
import type { RecommendedActionItem } from '../components/RecommendedActionsList'

type MMMRunView = 'analysis' | 'budget'

type MMMPlatformDraft = {
  kpiTarget?: 'sales' | 'attribution'
  spendChannels?: string[]
  attributionModel?: string
  attributionConfigId?: string | null
  notice?: string
}

interface MMMWizardShellProps {
  primaryKpiLabel?: string
  currencyCode?: string
  onOpenDataQuality?: () => void
  mmmRunId: string | null
  mmmDatasetId: string | null
  pendingMmmMapping: { dataset_id: string; columns: { kpi: string; spend_channels: string[]; covariates?: string[] } } | null
  mmmRunQuery: UseQueryResult<any, Error>
  createMmmRunMutation: UseMutationResult<any, Error, any>
  onMmmMappingComplete: (mapping: { dataset_id: string; columns: { kpi: string; spend_channels: string[]; covariates?: string[] } }) => void
  onMmmStartRun: (config: {
    dataset_id: string
    kpi: string
    spend_channels: string[]
    covariates: string[]
    kpi_mode: string
    use_adstock: boolean
    use_saturation: boolean
    holdout_weeks: number
    random_seed: number | null
  }) => void
  onMmmSelectRun: (runId: string, datasetId: string) => void
  onStartOver: () => void
}

interface PersistedState {
  lastRunId: string | null
  lastDatasetId: string | null
  kpiMode: KpiMode
}

interface MMMRunSummary {
  run_id: string
  status: string
  created_at: string | null
  updated_at: string | null
  dataset_id: string | null
  dataset_available?: boolean
  config?: {
    dataset_id?: string
    kpi?: string
    spend_channels?: string[]
    covariates?: string[]
    kpi_mode?: string
    use_adstock?: boolean
    use_saturation?: boolean
    holdout_weeks?: number
    random_seed?: number | null
  }
  kpi: string | null
  kpi_mode?: string | null
  n_channels: number
  n_covariates?: number
  r2?: number
  engine?: string
  engine_version?: string | null
  stage?: string | null
  progress_pct?: number | null
  scenario_count?: number
  latest_scenario_at?: string | null
  stale_from_status?: string | null
  stale_reason?: string | null
  stale_at?: string | null
  quality?: {
    level?: MMMQualityLevel
    label?: string
    tone?: 'success' | 'warning' | 'danger'
    reasons?: string[]
    can_use_results?: boolean
    can_use_budget?: boolean
    canUseResults?: boolean
    canUseBudget?: boolean
  }
}

const STORAGE_KEY = 'mmm-wizard-state-v1'

const runCanUseResults = (run: MMMRunSummary) => run.quality?.can_use_results ?? run.quality?.canUseResults ?? run.status === 'finished'
const runCanUseBudget = (run: MMMRunSummary) =>
  run.quality?.can_use_budget ?? run.quality?.canUseBudget ?? (run.status === 'finished' && run.dataset_available !== false)
const runCanOpenBudgetReadout = (run: MMMRunSummary) =>
  run.status === 'finished' && runCanUseResults(run) && !!run.dataset_id && Number(run.n_channels || 0) > 0

type MMMStartConfig = Parameters<MMMWizardShellProps['onMmmStartRun']>[0]

export default function MMMWizardShell(props: MMMWizardShellProps) {
  const t = tokens
  const {
    primaryKpiLabel,
    currencyCode,
    onOpenDataQuality,
    mmmRunId,
    mmmDatasetId,
    pendingMmmMapping,
    mmmRunQuery,
    createMmmRunMutation,
    onMmmMappingComplete,
    onMmmStartRun,
    onMmmSelectRun,
    onStartOver,
  } = props

  const [kpiMode, setKpiMode] = useState<KpiMode>('sales')
  const [selectedRunView, setSelectedRunView] = useState<MMMRunView>(() => {
    if (typeof window === 'undefined') return 'analysis'
    return new URLSearchParams(window.location.search).get('mmm_view') === 'budget' ? 'budget' : 'analysis'
  })
  const [showNewModelWorkflow, setShowNewModelWorkflow] = useState(false)
  const [initialPlatformDraft, setInitialPlatformDraft] = useState<MMMPlatformDraft | null>(null)
  const didAutoSelectRunRef = useRef(false)
  const analysisSectionRef = useRef<HTMLDivElement | null>(null)
  const budgetSectionRef = useRef<HTMLDivElement | null>(null)
  const recentRunsQuery = useQuery<MMMRunSummary[]>({
    queryKey: ['mmm-runs'],
    queryFn: async () => apiGetJson<MMMRunSummary[]>('/api/models', { fallbackMessage: 'Failed to load MMM runs' }),
    refetchInterval: (query) => {
      const runs = (query.state?.data ?? []) as MMMRunSummary[]
      return runs.some((run) => run.status === 'queued' || run.status === 'running') ? 2000 : false
    },
  })

  // Load persisted per-session state
  useEffect(() => {
    try {
      const raw = window.sessionStorage.getItem(STORAGE_KEY)
      if (!raw) return
      const parsed = JSON.parse(raw) as PersistedState
      if (parsed.kpiMode) setKpiMode(parsed.kpiMode)
      if (!mmmRunId && parsed.lastRunId && parsed.lastDatasetId) {
        onMmmSelectRun(parsed.lastRunId, parsed.lastDatasetId)
      }
    } catch {
      // ignore
    }
  }, [mmmRunId, onMmmSelectRun])

  // Persist when key pieces change
  useEffect(() => {
    const state: PersistedState = {
      lastRunId: mmmRunId,
      lastDatasetId: mmmDatasetId,
      kpiMode,
    }
    try {
      window.sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } catch {
      // ignore
    }
  }, [mmmRunId, mmmDatasetId, kpiMode])

  const runStatus: string | undefined = (mmmRunQuery.data as any)?.status

  const activeConfigLabel = useMemo(() => {
    if (runStatus === 'finished' && (mmmRunQuery.data as any)?.config?.version != null) {
      const cfg = (mmmRunQuery.data as any).config
      return `Run v${cfg.version} • ${cfg.kpi ?? 'KPI'}`
    }
    if (mmmRunId) {
      return `Run ${mmmRunId.slice(0, 8)}…`
    }
    return undefined
  }, [mmmRunId, mmmRunQuery.data, runStatus])

  const recentRuns = useMemo(
    () =>
      (recentRunsQuery.data ?? [])
        .slice()
        .sort((a, b) => {
          const priority = (run: MMMRunSummary) => {
            if (run.status === 'finished' && run.quality?.level === 'ready' && run.dataset_available !== false) return 0
            if (run.status === 'finished' && runCanUseResults(run)) return 1
            if (run.status === 'queued' || run.status === 'running') return 2
            if (run.status === 'finished') return 3
            if (run.status === 'stale') return 4
            if (run.status === 'error') return 5
            return 5
          }
          const priorityDelta = priority(a) - priority(b)
          if (priorityDelta !== 0) return priorityDelta
          const aTime = new Date(a.updated_at || a.created_at || 0).getTime()
          const bTime = new Date(b.updated_at || b.created_at || 0).getTime()
          return bTime - aTime
        })
        .slice(0, 5),
    [recentRunsQuery.data],
  )
  const latestFinishedRun = useMemo(
    () =>
      recentRuns.find((run) => run.status === 'finished' && run.dataset_id && runCanUseResults(run) && run.dataset_available !== false) ??
      recentRuns.find((run) => run.status === 'finished' && run.dataset_id && runCanUseResults(run)) ??
      null,
    [recentRuns],
  )

  const formatRunDate = (value?: string | null) =>
    value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : '—'

  const jumpToMmmView = (view: MMMRunView) => {
    setSelectedRunView(view)
    if (typeof window !== 'undefined') {
      const params = new URLSearchParams(window.location.search)
      params.set('mmm_view', view)
      window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}${window.location.hash}`)
    }
    window.requestAnimationFrame(() => {
      const node = view === 'budget' ? budgetSectionRef.current : analysisSectionRef.current
      node?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
  }

  const openRunView = (run: MMMRunSummary, view: MMMRunView) => {
    if (!run.dataset_id) return
    setShowNewModelWorkflow(false)
    onMmmSelectRun(run.run_id, run.dataset_id)
    jumpToMmmView(view)
  }

  const compatibleDraftForRun = (runData: any): MMMPlatformDraft => {
    const config = runData?.config || {}
    const spendChannels = Array.isArray(config.spend_channels) ? config.spend_channels.map(String) : []
    const attributionModel = runData?.attribution_model || config.attribution_model || 'linear'
    const attributionConfigId = runData?.attribution_config_id ?? config.attribution_config_id ?? null
    return {
      kpiTarget: runData?.attribution_model || attributionConfigId ? 'attribution' : 'sales',
      spendChannels,
      attributionModel,
      attributionConfigId,
      notice:
        'The previous run can still be reviewed, but its linked dataset file is missing. These settings are prefilled from the saved run so you can rebuild a compatible source dataset before launching a replacement model.',
    }
  }

  const startConfigFromRun = (runData: any): MMMStartConfig | null => {
    const config = runData?.config || {}
    const datasetId = String(runData?.dataset_id || config.dataset_id || '').trim()
    const kpi = String(config.kpi || runData?.kpi || '').trim()
    const spendChannels = Array.isArray(config.spend_channels) ? config.spend_channels.map(String).filter(Boolean) : []
    const covariates = Array.isArray(config.covariates) ? config.covariates.map(String).filter(Boolean) : []
    if (!datasetId || !kpi || !spendChannels.length) return null
    return {
      dataset_id: datasetId,
      kpi,
      spend_channels: spendChannels,
      covariates,
      kpi_mode: String(config.kpi_mode || runData?.kpi_mode || 'conversions'),
      use_adstock: config.use_adstock !== false,
      use_saturation: config.use_saturation !== false,
      holdout_weeks: Number.isFinite(Number(config.holdout_weeks)) ? Number(config.holdout_weeks) : 8,
      random_seed: config.random_seed == null ? null : Number(config.random_seed),
    }
  }

  const rerunWithSameSetup = (runData: any) => {
    const cfg = startConfigFromRun(runData)
    if (!cfg) return
    setInitialPlatformDraft(null)
    setShowNewModelWorkflow(false)
    onMmmStartRun(cfg)
    window.requestAnimationFrame(() => {
      document.getElementById('mmm-workspace-top')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
  }

  const startNewModelWorkflow = (draft?: MMMPlatformDraft) => {
    setInitialPlatformDraft(draft ?? null)
    setShowNewModelWorkflow(true)
    onStartOver()
    if (typeof document === 'undefined') return
    window.requestAnimationFrame(() => {
      document.getElementById('mmm-new-model-workflow')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
  }

  useEffect(() => {
    if (recentRunsQuery.isLoading || recentRunsQuery.isError) return
    if (mmmRunId || pendingMmmMapping || showNewModelWorkflow) return
    if (!latestFinishedRun?.dataset_id) {
      if (!recentRuns.length) setShowNewModelWorkflow(true)
      return
    }
    if (didAutoSelectRunRef.current) return
    didAutoSelectRunRef.current = true
    onMmmSelectRun(latestFinishedRun.run_id, latestFinishedRun.dataset_id)
  }, [
    latestFinishedRun,
    mmmRunId,
    onMmmSelectRun,
    pendingMmmMapping,
    recentRuns.length,
    recentRunsQuery.isError,
    recentRunsQuery.isLoading,
    showNewModelWorkflow,
  ])

  useEffect(() => {
    if (pendingMmmMapping) setShowNewModelWorkflow(true)
  }, [pendingMmmMapping])

  useEffect(() => {
    if (!runStatus || runStatus === 'queued' || runStatus === 'running') return
    recentRunsQuery.refetch()
  }, [recentRunsQuery.refetch, runStatus])

  useEffect(() => {
    if (!mmmRunId) return
    recentRunsQuery.refetch()
  }, [mmmRunId, recentRunsQuery.refetch])

  useEffect(() => {
    if (!mmmRunId || typeof window === 'undefined') return
    const view = new URLSearchParams(window.location.search).get('mmm_view')
    if (view !== 'budget' && view !== 'analysis') return
    setSelectedRunView(view)
    const timer = window.setTimeout(() => {
      const node = view === 'budget' ? budgetSectionRef.current : analysisSectionRef.current
      node?.scrollIntoView({ behavior: 'auto', block: 'start' })
    }, 0)
    return () => window.clearTimeout(timer)
  }, [mmmRunId])

  const renderStepContent = () => {
    if (mmmRunId && (runStatus === 'queued' || runStatus === 'running' || createMmmRunMutation.isPending)) {
      return renderRunContent()
    }
    if (pendingMmmMapping) {
      return (
        <div>
          <SectionHeader
            title="Model run"
            subtitle="Review configuration, set transforms and options, then run. Or select a previous run from history."
          />
          <MMMRunConfigStep
            pendingMapping={pendingMmmMapping}
            onStartRun={onMmmStartRun}
            onSelectRun={onMmmSelectRun}
            currentRunId={mmmRunId}
            isRunning={createMmmRunMutation.isPending}
          />
        </div>
      )
    }
    if (showNewModelWorkflow) {
      return (
        <div>
          <SectionHeader
            title="Prepare MMM dataset"
            subtitle="Use platform data (journeys + expenses) or upload a CSV. Then proceed to mapping and run."
          />
          <MMMDataSourceStep
            onMappingComplete={onMmmMappingComplete}
            initialPlatformDraft={initialPlatformDraft ?? undefined}
          />
        </div>
      )
    }
    if (mmmRunId) {
      // Run lifecycle: queued / running / error / finished
      return renderRunContent()
    }
    return (
      <div>
        <SectionHeader
          title="Prepare MMM dataset"
          subtitle="Use platform data (journeys + expenses) or upload a CSV. Then proceed to mapping and run."
        />
        <MMMDataSourceStep
          onMappingComplete={onMmmMappingComplete}
          initialPlatformDraft={initialPlatformDraft ?? undefined}
        />
      </div>
    )
  }

  const renderRunContent = () => {

    if (!mmmRunQuery.data || runStatus === 'queued' || runStatus === 'running' || mmmRunQuery.isLoading) {
      const runData = (mmmRunQuery.data || {}) as any
      const config = runData?.config || pendingMmmMapping?.columns || {}
      const startedAt = runData?.created_at
        ? new Date(runData.created_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
        : null
      const updatedAt = runData?.updated_at
        ? new Date(runData.updated_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
        : null
      const statusText = runStatus === 'running' ? 'Fitting model' : createMmmRunMutation.isPending ? 'Creating run' : 'Queued'
      const progressPct = Math.max(5, Math.min(95, Number(runData?.progress_pct ?? (runStatus === 'running' ? 45 : 10))))
      const stageLabel = String(runData?.stage || statusText)
      const spendChannels = Array.isArray(config.spend_channels)
        ? config.spend_channels
        : pendingMmmMapping?.columns.spend_channels ?? []
      return (
        <div
          style={{
            padding: t.space.xl,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.warning}`,
            backgroundColor: t.color.warningMuted,
            display: 'grid',
            gap: t.space.lg,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.lg, alignItems: 'flex-start', flexWrap: 'wrap' }}>
            <div style={{ display: 'grid', gap: t.space.xs }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.warning, fontWeight: t.font.weightSemibold, textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                {stageLabel}
              </div>
              <h3 style={{ margin: 0, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                MMM run is in progress
              </h3>
              <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary, lineHeight: 1.5 }}>
                The backend is fitting the model from the generated weekly dataset. This page polls automatically and will switch to results when the run finishes.
              </p>
            </div>
            <span
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: 999,
                background: t.color.surface,
                color: t.color.warning,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightSemibold,
                textTransform: 'capitalize',
              }}
            >
              {runStatus || 'starting'}
            </span>
          </div>

          <div style={{ display: 'grid', gap: t.space.xs }}>
            <div
              role="progressbar"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={progressPct}
              aria-label="MMM run progress"
              style={{
                height: 10,
                borderRadius: 999,
                overflow: 'hidden',
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
              }}
            >
              <div
                style={{
                  height: '100%',
                  width: `${progressPct}%`,
                  background: t.color.warning,
                  transition: 'width 240ms ease',
                }}
              />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              <span>{stageLabel}</span>
              <span>{progressPct.toFixed(0)}%</span>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.md }}>
            {[
              { label: 'Run', value: mmmRunId ? mmmRunId.slice(0, 12) : 'Starting...', helper: startedAt ? `Started ${startedAt}` : 'Waiting for run id' },
              { label: 'Dataset', value: String(config.dataset_id || mmmDatasetId || 'Preparing'), helper: `${spendChannels.length.toLocaleString()} spend signals` },
              { label: 'KPI', value: String(config.kpi || pendingMmmMapping?.columns.kpi || 'KPI'), helper: String(config.kpi_mode || 'conversions') },
              { label: 'Last update', value: updatedAt || 'Polling...', helper: 'Refreshes automatically every 2 seconds' },
            ].map((item) => (
              <div key={item.label} style={{ padding: t.space.md, borderRadius: t.radius.md, border: `1px solid ${t.color.borderLight}`, background: t.color.surface, minWidth: 0 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{item.label}</div>
                <div style={{ marginTop: 4, fontSize: t.font.sizeMd, color: t.color.text, fontWeight: t.font.weightSemibold, overflowWrap: 'anywhere' }}>{item.value}</div>
                <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{item.helper}</div>
              </div>
            ))}
          </div>

          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
            <button
              type="button"
              onClick={() => mmmRunQuery.refetch()}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
                background: t.color.surface,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Check now
            </button>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              You can leave this page; the run remains available in MMM run history.
            </div>
          </div>
        </div>
      )
    }

    if (runStatus === 'error') {
      return (
        <div
          style={{
            padding: t.space.xl,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.danger}`,
            backgroundColor: t.color.dangerMuted,
          }}
        >
          <p
            style={{
              margin: 0,
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.danger,
            }}
          >
            Model run failed
          </p>
          <p
            style={{
              marginTop: t.space.sm,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            {String((mmmRunQuery.data as { detail?: string }).detail ?? 'Unknown error')}
          </p>
          <button
            type="button"
            onClick={() => startNewModelWorkflow()}
            style={{
              marginTop: t.space.md,
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              backgroundColor: t.color.textMuted,
              color: t.color.surface,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: 'pointer',
              fontWeight: t.font.weightMedium,
            }}
          >
            Start over
          </button>
        </div>
      )
    }

    if (runStatus === 'stale') {
      const runData = mmmRunQuery.data as any
      const staleAt = runData?.stale_at
        ? new Date(runData.stale_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
        : null
      return (
        <div
          style={{
            padding: t.space.xl,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.warning}`,
            backgroundColor: t.color.warningMuted,
            display: 'grid',
            gap: t.space.md,
          }}
        >
          <div>
            <p
              style={{
                margin: 0,
                fontSize: t.font.sizeMd,
                fontWeight: t.font.weightSemibold,
                color: t.color.warning,
              }}
            >
              Model run is stale
            </p>
            <p
              style={{
                margin: `${t.space.sm}px 0 0`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
                lineHeight: 1.5,
              }}
            >
              This run was marked stale because it was {runData?.stale_from_status || 'active'} but stopped
              updating. {staleAt ? `Marked stale ${staleAt}. ` : ''}
              Start a compatible replacement run if you still need this model basis.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={() => startNewModelWorkflow(compatibleDraftForRun(runData))}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                color: '#ffffff',
                background: t.color.accent,
                border: 'none',
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Create compatible new run
            </button>
            <button
              type="button"
              onClick={() => startNewModelWorkflow()}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                color: t.color.textSecondary,
                background: 'transparent',
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Start fresh
            </button>
          </div>
        </div>
      )
    }

    const runData = mmmRunQuery.data as any
    const hasBudgetView = (runData?.roi?.length ?? 0) > 0 && (runData?.contrib?.length ?? 0) > 0
    const runCreatedAt = runData?.created_at ? new Date(runData.created_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : ''
    const runUpdatedAt = runData?.updated_at ? new Date(runData.updated_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : ''
    const datasetId = runData?.dataset_id || runData?.config?.dataset_id || mmmDatasetId
    const datasetAvailable = runData?.dataset_available !== false
    const scenarioCount = Number(runData?.scenario_count || 0)
    const latestScenarioAt = runData?.latest_scenario_at
      ? new Date(runData.latest_scenario_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
      : null
    const r2Value = Number(runData?.r2)
    const hasR2 = Number.isFinite(r2Value)
    const diagnostics = runData?.diagnostics || {}
    const diagnosticsIssues = [
      diagnostics.rhat_max != null && diagnostics.rhat_max > 1.1 ? `R-hat ${Number(diagnostics.rhat_max).toFixed(2)}` : null,
      diagnostics.ess_bulk_min != null && diagnostics.ess_bulk_min < 200 ? `ESS ${Number(diagnostics.ess_bulk_min).toFixed(0)}` : null,
      diagnostics.divergences != null && diagnostics.divergences > 0 ? `${Number(diagnostics.divergences).toLocaleString()} divergences` : null,
    ].filter((item): item is string => Boolean(item))
    const totalSpendFromSummary = Array.isArray(runData?.channel_summary)
      ? runData.channel_summary.reduce((sum: number, row: any) => sum + (Number(row?.spend) || 0), 0)
      : undefined
    const runQuality = evaluateMMMRunQuality({
      status: runStatus,
      engineVersion: runData?.engine_version,
      datasetAvailable,
      r2: hasR2 ? r2Value : null,
      weeks: 0,
      channelsModeled: Array.isArray(runData?.config?.spend_channels) ? runData.config.spend_channels.length : 0,
      totalSpend: totalSpendFromSummary,
      roi: runData?.roi,
      contrib: runData?.contrib,
      diagnostics,
    })
    const canOpenBudgetView = hasBudgetView && runQuality.canUseResults
    const activeRunView: MMMRunView = selectedRunView === 'budget' && canOpenBudgetView ? 'budget' : 'analysis'
    const healthTone =
      runQuality.level === 'not_usable'
        ? 'danger'
        : runStatus === 'error'
        ? 'danger'
        : runStatus !== 'finished'
          ? 'warning'
          : !datasetAvailable || diagnosticsIssues.length || (hasR2 && r2Value < 0.3)
            ? 'warning'
            : 'success'
    const healthLabel =
      runQuality.level === 'not_usable'
        ? runQuality.label
        : runQuality.level === 'directional'
          ? runQuality.label
        : runStatus !== 'finished'
        ? String(runStatus || 'pending')
        : !datasetAvailable
          ? 'Readout only'
          : diagnosticsIssues.length
            ? 'Diagnostics warning'
            : hasR2 && r2Value < 0.3
              ? 'Directional fit'
              : 'Decision ready'
    const healthColor =
      healthTone === 'success' ? t.color.success : healthTone === 'danger' ? t.color.danger : t.color.warning
    const healthBg =
      healthTone === 'success' ? t.color.successMuted : healthTone === 'danger' ? t.color.dangerMuted : t.color.warningMuted
    const kpiSource = runData?.attribution_model
      ? `Attribution: ${String(runData.attribution_model).replace(/_/g, ' ')}`
      : 'Direct KPI'
    const sourceContract = runData?.source_contract || null
    const sourceContractLabel = sourceContract?.attribution_source || kpiSource
    const sourceContractHelper = sourceContract?.latest_event_replay?.events_loaded
      ? `${Number(sourceContract.latest_event_replay.events_loaded).toLocaleString()} Pipes events · ${(Number(sourceContract.latest_event_replay.journeys_persisted) || 0).toLocaleString()} persisted journeys`
      : sourceContract?.spend_source
        ? `Spend: ${sourceContract.spend_source}`
        : 'Source contract not persisted for this run'
    const audienceScopeLabel =
      sourceContract?.measurement_audience?.name ||
      sourceContract?.audience_scope ||
      'Workspace aggregate'
    const modelMeta = [
      runData?.engine ? String(runData.engine) : null,
      runData?.config?.frequency ? `Frequency ${runData.config.frequency}` : 'Weekly',
      Array.isArray(runData?.config?.spend_channels)
        ? `${runData.config.spend_channels.length.toLocaleString()} channels`
        : null,
    ].filter((item): item is string => Boolean(item))
    const canRerunActive = datasetAvailable && !!startConfigFromRun(runData) && !createMmmRunMutation.isPending
    const handleBudgetRecommendedAction = (action: RecommendedActionItem) => {
      if (action.id === 'rerun_mmm_same_setup') {
        rerunWithSameSetup(runData)
        return
      }
      if (action.id === 'rebuild_mmm_dataset') {
        startNewModelWorkflow(compatibleDraftForRun(runData))
        return
      }
      if (action.id === 'review_mmm_inputs') {
        jumpToMmmView('analysis')
        return
      }
      navigateForRecommendedAction(action, { defaultPage: 'mmm' })
    }
    return (
      <div>
        <SectionHeader
          title="MMM analysis"
          subtitle={
            primaryKpiLabel
              ? `Review modeled channel impact on ${primaryKpiLabel} and move directly into budget actions.`
              : 'Review model fit, contribution, and budget actions inside the shared workspace flow.'
          }
          trailing={
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              <button
                type="button"
                onClick={() => jumpToMmmView('analysis')}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  color: activeRunView === 'analysis' ? '#ffffff' : t.color.text,
                  backgroundColor: activeRunView === 'analysis' ? t.color.accent : t.color.surface,
                  border: `1px solid ${activeRunView === 'analysis' ? t.color.accent : t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Results
              </button>
              <button
                type="button"
                onClick={() => canOpenBudgetView && jumpToMmmView('budget')}
                disabled={!canOpenBudgetView}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  color: activeRunView === 'budget' ? '#ffffff' : canOpenBudgetView ? t.color.text : t.color.textMuted,
                  backgroundColor: activeRunView === 'budget' ? t.color.accent : t.color.surface,
                  border: `1px solid ${activeRunView === 'budget' ? t.color.accent : t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: canOpenBudgetView ? 'pointer' : 'not-allowed',
                }}
              >
                {runQuality.canUseBudget ? 'Budget actions' : 'Budget readout'}
              </button>
              <button
                type="button"
                onClick={() => rerunWithSameSetup(runData)}
                disabled={!canRerunActive}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  color: canRerunActive ? t.color.text : t.color.textMuted,
                  backgroundColor: t.color.surface,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: canRerunActive ? 'pointer' : 'not-allowed',
                }}
              >
                Re-run same setup
              </button>
              <button
                type="button"
                onClick={() => startNewModelWorkflow()}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  color: t.color.surface,
                  backgroundColor: t.color.textSecondary,
                  border: 'none',
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                New model run
              </button>
            </div>
          }
        />
        <div style={{ marginBottom: t.space.lg }}>
          <SectionCard
            title="Selected run context"
            subtitle="The active MMM run, its data contract, and whether it is safe to use for new budget decisions."
            actions={
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: t.space.xs,
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: 999,
                  background: healthBg,
                  color: healthColor,
                  fontSize: t.font.sizeXs,
                  fontWeight: t.font.weightSemibold,
                  textTransform: 'capitalize',
                }}
              >
                {healthLabel}
              </span>
            }
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                  gap: t.space.md,
                }}
              >
                {[
                  {
                    label: 'Run',
                    value: mmmRunId ? mmmRunId.slice(0, 12) : '—',
                    helper: runUpdatedAt ? `Updated ${runUpdatedAt}` : runCreatedAt ? `Created ${runCreatedAt}` : 'No timestamp',
                  },
                  {
                    label: 'Model health',
                    value: hasR2 ? `R² ${r2Value.toFixed(3)}` : healthLabel,
                    helper: runQuality.reasons[0] || (diagnosticsIssues.length ? diagnosticsIssues.join(', ') : modelMeta.join(' · ') || 'Diagnostics unavailable'),
                  },
                  {
                    label: 'KPI source',
                    value: runData?.config?.kpi || runData?.kpi || 'KPI',
                    helper: runData?.attribution_config_id
                      ? `${kpiSource}, config ${String(runData.attribution_config_id).slice(0, 8)}…`
                      : kpiSource,
                  },
                  {
                    label: 'Source contract',
                    value: sourceContractLabel,
                    helper: sourceContractHelper,
                  },
                  {
                    label: 'Audience scope',
                    value: audienceScopeLabel,
                    helper: sourceContract?.measurement_audience?.materialization_status
                      ? String(sourceContract.measurement_audience.materialization_status).replace(/_/g, ' ')
                      : 'No membership-backed audience scope attached',
                  },
                  {
                    label: 'Dataset',
                    value: datasetAvailable ? 'Available' : 'Readout only',
                    helper: datasetId ? String(datasetId) : 'No linked dataset',
                  },
                  {
                    label: 'Budget work',
                    value: `${scenarioCount.toLocaleString()} scenario${scenarioCount === 1 ? '' : 's'}`,
                    helper: latestScenarioAt ? `Latest ${latestScenarioAt}` : canOpenBudgetView ? 'Recommendations available' : runQuality.level === 'not_usable' ? 'Blocked by model quality' : 'No optimizer readout yet',
                  },
                ].map((item) => (
                  <div
                    key={item.label}
                    style={{
                      padding: t.space.md,
                      borderRadius: t.radius.md,
                      border: `1px solid ${t.color.borderLight}`,
                      background: t.color.bg,
                      display: 'grid',
                      gap: 4,
                      minWidth: 0,
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                      {item.label}
                    </div>
                    <div style={{ fontSize: t.font.sizeMd, color: t.color.text, fontWeight: t.font.weightSemibold, overflowWrap: 'anywhere' }}>
                      {item.value}
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, overflowWrap: 'anywhere' }}>
                      {item.helper}
                    </div>
                  </div>
                ))}
              </div>
              {runQuality.level === 'not_usable' ? (
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap', alignItems: 'center' }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.danger, lineHeight: 1.5, maxWidth: 820 }}>
                    This run is not usable for results or budget actions. {runQuality.reasons[0] || 'Create a new run after checking spend and KPI signal.'}
                  </div>
                  <button
                    type="button"
                    onClick={() => rerunWithSameSetup(runData)}
                    disabled={!canRerunActive}
                    style={{
                      padding: `${t.space.sm}px ${t.space.lg}px`,
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightSemibold,
                      color: canRerunActive ? '#ffffff' : t.color.textMuted,
                      background: canRerunActive ? t.color.accent : t.color.borderLight,
                      border: 'none',
                      borderRadius: t.radius.sm,
                      cursor: canRerunActive ? 'pointer' : 'not-allowed',
                    }}
                  >
                    Re-run with same setup
                  </button>
                </div>
              ) : !datasetAvailable ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.warning, lineHeight: 1.5 }}>
                  This run can be used for saved ROI, contribution, fit, and scenario readouts. New optimizer
                  recommendations require rebuilding or reattaching the linked dataset.
                </div>
              ) : healthTone === 'warning' ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.warning, lineHeight: 1.5 }}>
                  Treat this run directionally until the flagged fit or diagnostic issue is reviewed.
                </div>
              ) : (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, lineHeight: 1.5 }}>
                  This run has the source dataset available and can support results review plus budget actions.
                </div>
              )}
            </div>
          </SectionCard>
        </div>
        {runData?.dataset_available === false ? (
          <div style={{ marginBottom: t.space.lg }}>
            <SectionCard
              title="Dataset recovery needed"
              subtitle="Saved model readouts remain available, but the linked source dataset file is not available in this runtime."
            >
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  gap: t.space.lg,
                  alignItems: 'center',
                  flexWrap: 'wrap',
                }}
              >
                <div style={{ maxWidth: 720, fontSize: t.font.sizeSm, color: t.color.textSecondary, lineHeight: 1.5 }}>
                  You can still review fit, contribution, ROI, and saved scenarios. Dataset preview and optimizer
                  recommendations need the original source rows, so rebuild a compatible platform dataset before making
                  new budget decisions from this historical run.
                </div>
                <button
                  type="button"
                  onClick={() => startNewModelWorkflow(compatibleDraftForRun(runData))}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    color: '#ffffff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: 'pointer',
                  }}
                >
                  Create compatible new run
                </button>
              </div>
            </SectionCard>
          </div>
        ) : null}
        {activeRunView === 'analysis' ? (
          <div ref={analysisSectionRef}>
            <MMMDashboard
              runId={mmmRunId!}
              datasetId={mmmDatasetId ?? ''}
              runMetadata={{ attribution_model: runData?.attribution_model, attribution_config_id: runData?.attribution_config_id }}
              onOpenDataQuality={onOpenDataQuality}
              onOpenBudgetActions={canOpenBudgetView ? () => jumpToMmmView('budget') : undefined}
            />
          </div>
        ) : (
          <div ref={budgetSectionRef}>
            <BudgetOptimizer
              roiData={runData.roi}
              contribData={runData.contrib}
              runId={mmmRunId}
              datasetId={mmmDatasetId}
              datasetAvailable={runData?.dataset_available !== false}
              budgetActionsAvailable={runQuality.canUseBudget}
              budgetReadoutReason={runQuality.reasons[0] ?? null}
              onRecommendedActionClick={handleBudgetRecommendedAction}
            />
          </div>
        )}
      </div>
    )
  }

  const primaryKpi = primaryKpiLabel ?? (kpiMode === 'sales' ? 'Total sales' : 'Marketing-driven conversions')
  const workflowActions =
    recentRuns.length || mmmRunId || pendingMmmMapping || mmmDatasetId ? (
      <button
        type="button"
        onClick={() => startNewModelWorkflow()}
        style={{
          padding: `${t.space.sm}px ${t.space.lg}px`,
          fontSize: t.font.sizeSm,
          fontWeight: t.font.weightMedium,
          color: t.color.surface,
          backgroundColor: t.color.textSecondary,
          border: 'none',
          borderRadius: t.radius.sm,
          cursor: 'pointer',
        }}
      >
        Create new MMM run
      </button>
    ) : null
  const shouldShowNewWorkflow = showNewModelWorkflow || !!pendingMmmMapping || (!mmmRunId && !recentRuns.length)
  const setupRunInProgress = shouldShowNewWorkflow && !!mmmRunId && (runStatus === 'queued' || runStatus === 'running' || createMmmRunMutation.isPending)
  const workspaceSubtitle = mmmRunId
    ? 'Selected MMM run is the active workspace. Results, budget recommendations, saved scenarios, and rollout evidence stay attached to this run.'
    : 'Pick up prior MMM results and budget scenarios without starting a new model run.'
  const latestFinishedRunCanUseBudget = latestFinishedRun ? runCanUseBudget(latestFinishedRun) : false
  const latestFinishedRunCanOpenBudget = latestFinishedRun ? runCanOpenBudgetReadout(latestFinishedRun) : false
  const latestFinishedRunQualityLabel = latestFinishedRun?.quality?.label || (latestFinishedRun?.dataset_available === false ? 'Readout only' : 'Decision ready')
  const latestFinishedRunReason = latestFinishedRun?.quality?.reasons?.[0]
  const latestFinishedRunCanRerun =
    !!latestFinishedRun &&
    latestFinishedRun.dataset_available !== false &&
    !!startConfigFromRun(latestFinishedRun) &&
    !createMmmRunMutation.isPending

  return (
    <DashboardPage
      title="Marketing Mix Modeling"
      description={`Model incremental media impact on ${primaryKpi} and move directly from model setup into budget decisions.`}
      actions={workflowActions}
      filters={null}
      dateRange={null}
    >
      <div id="mmm-workspace-top" style={{ maxWidth: 1400, margin: '0 auto', display: 'grid', gap: t.space.xl }}>
        <SectionCard
          title="Model context"
          subtitle="Weekly MMM setup, KPI mode, and active config context for the current run."
        >
          <MMMContextBar
            periodLabel="Weekly (MMM model frequency: W)"
            periodReadOnly
            kpiMode={kpiMode}
            onKpiModeChange={setKpiMode}
            currencyCode={currencyCode ?? '—'}
            currencyReadOnly
            onOpenDataQuality={onOpenDataQuality}
            activeConfigLabel={activeConfigLabel}
          />
        </SectionCard>

        <SectionCard
          title="MMM workspace"
          subtitle={workspaceSubtitle}
          actions={
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              <button
                type="button"
                onClick={() => recentRunsQuery.refetch()}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Refresh runs
              </button>
              <button
                type="button"
                onClick={() => startNewModelWorkflow()}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  color: '#ffffff',
                  background: t.color.accent,
                  border: `1px solid ${t.color.accent}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Create new model
              </button>
            </div>
          }
        >
          {recentRunsQuery.isLoading ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading MMM runs…</div>
          ) : !recentRuns.length ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              No prior runs yet. Start a model below; finished results and budget work will appear here.
            </div>
          ) : (
            <div style={{ display: 'grid', gap: t.space.lg }}>
              {latestFinishedRun && (
                <div
                  style={{
                    padding: t.space.lg,
                    borderRadius: t.radius.lg,
                    border: `1px solid ${t.color.accent}`,
                    background: t.color.accentMuted,
                    display: 'flex',
                    justifyContent: 'space-between',
                    gap: t.space.lg,
                    alignItems: 'center',
                    flexWrap: 'wrap',
                  }}
                >
                  <div style={{ display: 'grid', gap: 4 }}>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                      {latestFinishedRunCanUseBudget ? 'Recommended MMM run' : latestFinishedRunQualityLabel}
                    </div>
                    <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                      {latestFinishedRun.kpi || 'MMM run'} · {formatRunDate(latestFinishedRun.updated_at || latestFinishedRun.created_at)}
                    </div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      {latestFinishedRun.n_channels.toLocaleString()} channels
                      {latestFinishedRun.r2 != null ? ` · R² ${latestFinishedRun.r2.toFixed(3)}` : ''}
                      {latestFinishedRun.dataset_id ? ` · dataset ${latestFinishedRun.dataset_id.slice(0, 8)}…` : ''}
                      {latestFinishedRun.dataset_available === false ? ' · readout only' : ''}
                      {latestFinishedRun.quality?.level === 'directional' && latestFinishedRun.dataset_available !== false ? ' · directional' : ''}
                      {Number(latestFinishedRun.scenario_count || 0) > 0
                        ? ` · ${Number(latestFinishedRun.scenario_count).toLocaleString()} saved budget scenario${Number(latestFinishedRun.scenario_count) === 1 ? '' : 's'}`
                        : ' · no saved budget scenarios yet'}
                    </div>
                    {latestFinishedRunReason ? (
                      <div style={{ fontSize: t.font.sizeXs, color: latestFinishedRunCanUseBudget ? t.color.textMuted : t.color.warning }}>
                        {latestFinishedRunReason}
                      </div>
                    ) : null}
                  </div>
                  <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                    <button
                      type="button"
                      onClick={() => openRunView(latestFinishedRun, 'analysis')}
                      style={{
                        padding: `${t.space.sm}px ${t.space.lg}px`,
                        borderRadius: t.radius.sm,
                        border: 'none',
                        background: t.color.accent,
                        color: '#ffffff',
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightSemibold,
                        cursor: 'pointer',
                      }}
                    >
                      Open results
                    </button>
                    <button
                      type="button"
                      onClick={() => openRunView(latestFinishedRun, 'budget')}
                      disabled={!latestFinishedRunCanOpenBudget}
                      style={{
                        padding: `${t.space.sm}px ${t.space.lg}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: t.color.surface,
                        color: latestFinishedRunCanOpenBudget ? t.color.text : t.color.textMuted,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightSemibold,
                        cursor: latestFinishedRunCanOpenBudget ? 'pointer' : 'not-allowed',
                      }}
                    >
                      {latestFinishedRunCanUseBudget ? 'Open budget actions' : 'Open budget readout'}
                    </button>
                    <button
                      type="button"
                      onClick={() => rerunWithSameSetup(latestFinishedRun)}
                      disabled={!latestFinishedRunCanRerun}
                      style={{
                        padding: `${t.space.sm}px ${t.space.lg}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: t.color.surface,
                        color: latestFinishedRunCanRerun ? t.color.textSecondary : t.color.textMuted,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightSemibold,
                        cursor: latestFinishedRunCanRerun ? 'pointer' : 'not-allowed',
                      }}
                    >
                      Re-run same setup
                    </button>
                  </div>
                </div>
              )}

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: t.space.md }}>
                {recentRuns.map((run) => {
                  const isSelected = run.run_id === mmmRunId
                  const isStale = run.status === 'stale'
                  const canUseBudget = runCanUseBudget(run)
                  const canOpenBudget = runCanOpenBudgetReadout(run)
                  const canRerun = run.dataset_available !== false && !!startConfigFromRun(run) && !createMmmRunMutation.isPending
                  const qualityTone = run.quality?.tone
                  const qualityLabel = run.quality?.label || run.status
                  const statusColor =
                    qualityTone === 'success'
                      ? t.color.success
                      : qualityTone === 'danger' || run.status === 'error'
                        ? t.color.danger
                        : t.color.warning
                  return (
                    <div
                      key={run.run_id}
                      style={{
                        padding: t.space.md,
                        borderRadius: t.radius.md,
                        border: `1px solid ${isSelected ? t.color.accent : isStale ? t.color.warning : t.color.borderLight}`,
                        background: isSelected ? t.color.accentMuted : isStale ? t.color.warningMuted : t.color.surface,
                        display: 'grid',
                        gap: 6,
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                          {run.kpi || 'MMM run'}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: statusColor, textTransform: 'capitalize' }}>
                          {qualityLabel}
                        </div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {formatRunDate(run.updated_at || run.created_at)}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        {run.n_channels.toLocaleString()} channels
                        {run.r2 != null ? ` · R² ${run.r2.toFixed(3)}` : ''}
                        {run.stage && (run.status === 'queued' || run.status === 'running') ? ` · ${run.stage}` : ''}
                        {run.dataset_id ? ` · dataset ${run.dataset_id.slice(0, 8)}…` : ''}
                        {run.dataset_available === false ? ' · readout only' : ''}
                        {run.quality?.level === 'directional' && run.dataset_available !== false ? ' · directional' : ''}
                        {run.quality?.level === 'not_usable' ? ' · not usable' : ''}
                        {isStale ? ' · recovery needed' : ''}
                        {Number(run.scenario_count || 0) > 0
                          ? ` · ${Number(run.scenario_count).toLocaleString()} saved scenario${Number(run.scenario_count) === 1 ? '' : 's'}`
                          : ''}
                      </div>
                      {isStale ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Stopped before completion. Open recovery to create a compatible new run or start fresh.
                        </div>
                      ) : null}
                      {run.quality?.level === 'not_usable' && run.quality.reasons?.[0] ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.danger, lineHeight: 1.4 }}>
                          {run.quality.reasons[0]}
                        </div>
                      ) : null}
                      <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                        <button
                          type="button"
                          onClick={() => openRunView(run, 'analysis')}
                          disabled={!run.dataset_id}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: t.color.bg,
                            color: t.color.accent,
                            fontSize: t.font.sizeXs,
                            cursor: run.dataset_id ? 'pointer' : 'not-allowed',
                          }}
                        >
                          {isStale ? 'Open recovery' : isSelected ? 'Current analysis' : 'Open analysis'}
                        </button>
                        <button
                          type="button"
                          onClick={() => openRunView(run, 'budget')}
                          disabled={!canOpenBudget}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: t.color.bg,
                            color: canOpenBudget ? t.color.textSecondary : t.color.textMuted,
                            fontSize: t.font.sizeXs,
                            cursor: canOpenBudget ? 'pointer' : 'not-allowed',
                          }}
                        >
                          {isSelected
                            ? canUseBudget
                              ? 'Current budget'
                              : 'Current readout'
                            : canUseBudget
                            ? 'Open budget actions'
                            : 'Open budget readout'}
                        </button>
                        <button
                          type="button"
                          onClick={() => rerunWithSameSetup(run)}
                          disabled={!canRerun}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: t.color.bg,
                            color: canRerun ? t.color.textSecondary : t.color.textMuted,
                            fontSize: t.font.sizeXs,
                            cursor: canRerun ? 'pointer' : 'not-allowed',
                          }}
                        >
                          Re-run
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </SectionCard>

        {mmmRunId && !shouldShowNewWorkflow ? renderRunContent() : null}

        <SectionCard
          title="Create or rebuild MMM model"
          subtitle={
            shouldShowNewWorkflow
              ? 'Prepare a new dataset, run a model, review results, and move into budget decisions.'
              : 'Use this only when you need a fresh model basis. Existing results and budget work stay available above.'
          }
          actions={
            mmmRunId || recentRuns.length ? (
              <button
                type="button"
                onClick={() => {
                  if (shouldShowNewWorkflow) {
                    setShowNewModelWorkflow(false)
                    setInitialPlatformDraft(null)
                  } else {
                    startNewModelWorkflow()
                  }
                }}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  color: shouldShowNewWorkflow ? t.color.textSecondary : '#ffffff',
                  background: shouldShowNewWorkflow ? 'transparent' : t.color.accent,
                  border: `1px solid ${shouldShowNewWorkflow ? t.color.border : t.color.accent}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                {shouldShowNewWorkflow ? 'Hide setup' : 'Create new model'}
              </button>
            ) : null
          }
        >
          <div id="mmm-new-model-workflow" style={{ display: 'grid', gap: t.space.lg }}>
            {!shouldShowNewWorkflow ? (
              <div
                style={{
                  padding: t.space.lg,
                  borderRadius: t.radius.md,
                  border: `1px dashed ${t.color.border}`,
                  background: t.color.bg,
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  gap: t.space.lg,
                  flexWrap: 'wrap',
                }}
              >
                <div style={{ display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    Setup is collapsed to keep previous results primary.
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Start a new run only when the dataset, KPI, spend mapping, or model assumptions need to change.
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() => startNewModelWorkflow()}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    color: '#ffffff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: 'pointer',
                  }}
                >
                  Start new run
                </button>
              </div>
            ) : (
              <SetupChecklist
                pendingMapping={pendingMmmMapping}
                runId={setupRunInProgress ? mmmRunId : null}
                runStatus={setupRunInProgress ? runStatus : undefined}
              />
            )}
          </div>
        </SectionCard>

        {shouldShowNewWorkflow ? renderStepContent() : null}
      </div>
    </DashboardPage>
  )
}

function SectionHeader(props: { title: string; subtitle?: string; trailing?: React.ReactNode }) {
  const t = tokens
  return (
    <div
      style={{
        marginBottom: t.space.lg,
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        flexWrap: 'wrap',
        gap: t.space.sm,
      }}
    >
      <div>
        <h2
          style={{
            margin: 0,
            fontSize: t.font.sizeXl,
            fontWeight: t.font.weightSemibold,
            color: t.color.text,
          }}
        >
          {props.title}
        </h2>
        {props.subtitle && (
          <p
            style={{
              margin: `${t.space.xs}px 0 0`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            {props.subtitle}
          </p>
        )}
      </div>
      {props.trailing}
    </div>
  )
}

function SetupChecklist(props: {
  pendingMapping: MMMWizardShellProps['pendingMmmMapping']
  runId: string | null
  runStatus?: string
}) {
  const t = tokens
  const hasDataset = Boolean(props.pendingMapping?.dataset_id || props.runId)
  const hasMapping = Boolean(props.pendingMapping)
  const hasRun = Boolean(props.runId)
  const channels = props.pendingMapping?.columns.spend_channels ?? []
  const cards = [
    {
      title: 'Dataset and KPI',
      status: hasDataset ? 'Ready' : 'Choose source',
      tone: hasDataset ? 'success' : 'accent',
      detail: props.pendingMapping
        ? `${props.pendingMapping.columns.kpi} from dataset ${props.pendingMapping.dataset_id.slice(0, 12)}...`
        : 'Pick platform data or upload a CSV, then confirm the KPI period.',
    },
    {
      title: 'Spend mapping',
      status: hasMapping ? `${channels.length} channel${channels.length === 1 ? '' : 's'}` : 'Not set',
      tone: hasMapping ? 'success' : 'muted',
      detail: hasMapping
        ? channels.join(', ') || 'No spend channels selected'
        : 'Choose paid channels and optional covariates before generating the model dataset.',
    },
    {
      title: 'Model assumptions',
      status: hasMapping ? 'Configure below' : 'Waiting for dataset',
      tone: hasMapping ? 'accent' : 'muted',
      detail: hasMapping
        ? 'Set KPI basis, carry-over, saturation, holdout, and reproducibility before launch.'
        : 'These settings become available after a dataset is generated.',
    },
    {
      title: 'Launch and review',
      status: hasRun ? String(props.runStatus || 'Queued') : hasMapping ? 'Ready to run' : 'Not started',
      tone: hasRun ? (props.runStatus === 'error' ? 'danger' : 'success') : hasMapping ? 'accent' : 'muted',
      detail: hasRun
        ? 'The active run will open results and budget actions when finished.'
        : 'Start the model once the setup summary matches the analysis question.',
    },
  ]

  const colorForTone = (tone: string) => {
    if (tone === 'success') return { bg: t.color.successMuted, fg: t.color.success, border: t.color.success }
    if (tone === 'danger') return { bg: t.color.dangerMuted, fg: t.color.danger, border: t.color.danger }
    if (tone === 'accent') return { bg: t.color.accentMuted, fg: t.color.accent, border: t.color.accent }
    return { bg: t.color.bg, fg: t.color.textMuted, border: t.color.borderLight }
  }

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ display: 'grid', gap: 4 }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Setup checklist
        </div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          No hidden wizard steps. Configure the dataset first, then review model assumptions and launch from the panel below.
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.md }}>
        {cards.map((card) => {
          const color = colorForTone(card.tone)
          return (
            <div
              key={card.title}
              style={{
                padding: t.space.md,
                borderRadius: t.radius.md,
                border: `1px solid ${color.border}`,
                background: color.bg,
                display: 'grid',
                gap: t.space.xs,
                minWidth: 0,
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center' }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {card.title}
                </div>
                <span
                  style={{
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    color: color.fg,
                    whiteSpace: 'nowrap',
                  }}
                >
                  {card.status}
                </span>
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, lineHeight: 1.45, overflowWrap: 'anywhere' }}>
                {card.detail}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
