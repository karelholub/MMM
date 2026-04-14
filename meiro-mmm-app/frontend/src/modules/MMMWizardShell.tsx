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

type StepKey = 'data_source' | 'mapping' | 'model_run' | 'results' | 'optimize'
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

interface StepState {
  key: StepKey
  label: string
  status: 'not_started' | 'ready' | 'completed'
}

interface PersistedState {
  currentStep: StepKey
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
  kpi: string | null
  kpi_mode?: string | null
  n_channels: number
  n_covariates?: number
  r2?: number
  engine?: string
  scenario_count?: number
  latest_scenario_at?: string | null
  stale_from_status?: string | null
  stale_reason?: string | null
  stale_at?: string | null
}

const STORAGE_KEY = 'mmm-wizard-state-v1'

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
  const [currentStep, setCurrentStep] = useState<StepKey>('data_source')
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
  })

  // Load persisted per-session state
  useEffect(() => {
    try {
      const raw = window.sessionStorage.getItem(STORAGE_KEY)
      if (!raw) return
      const parsed = JSON.parse(raw) as PersistedState
      if (parsed.currentStep) setCurrentStep(parsed.currentStep)
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
      currentStep,
      lastRunId: mmmRunId,
      lastDatasetId: mmmDatasetId,
      kpiMode,
    }
    try {
      window.sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } catch {
      // ignore
    }
  }, [currentStep, mmmRunId, mmmDatasetId, kpiMode])

  const runStatus: string | undefined = (mmmRunQuery.data as any)?.status

  const steps: StepState[] = useMemo(() => {
    const hasDataset = !!mmmDatasetId || !!pendingMmmMapping
    const hasMapping = !!pendingMmmMapping
    const hasRun = !!mmmRunId
    const finished = runStatus === 'finished'

    return [
      { key: 'data_source', label: 'Data source', status: hasDataset ? 'completed' : 'ready' },
      { key: 'mapping', label: 'Mapping', status: hasDataset ? 'completed' : 'not_started' },
      { key: 'model_run', label: 'Model run', status: hasRun ? (finished ? 'completed' : 'ready') : hasMapping ? 'ready' : 'not_started' },
      { key: 'results', label: 'Results', status: finished ? 'completed' : hasRun ? 'ready' : 'not_started' },
      {
        key: 'optimize',
        label: 'Optimize',
        status: finished && (mmmRunQuery.data as any)?.roi?.length ? 'ready' : finished ? 'ready' : 'not_started',
      },
    ]
  }, [mmmDatasetId, pendingMmmMapping, mmmRunId, runStatus, mmmRunQuery.data])

  const setStepSafely = (key: StepKey) => {
    // Only allow navigating to steps that are ready or completed to keep flow predictable
    const step = steps.find((s) => s.key === key)
    if (!step) return
    if (step.status === 'not_started') return
    setCurrentStep(key)
  }

  const activeStepIndex = steps.findIndex((s) => s.key === currentStep)

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
            if (run.status === 'finished' && run.dataset_available !== false) return 0
            if (run.status === 'finished') return 1
            if (run.status === 'queued' || run.status === 'running') return 2
            if (run.status === 'stale') return 3
            if (run.status === 'error') return 4
            return 4
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
      recentRuns.find((run) => run.status === 'finished' && run.dataset_available !== false && run.dataset_id) ??
      recentRuns.find((run) => run.status === 'finished' && run.dataset_id) ??
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
    setCurrentStep(view === 'budget' ? 'optimize' : 'results')
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

  const startNewModelWorkflow = (draft?: MMMPlatformDraft) => {
    setInitialPlatformDraft(draft ?? null)
    setShowNewModelWorkflow(true)
    onStartOver()
    setCurrentStep('data_source')
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
    setCurrentStep('results')
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

  useEffect(() => {
    if (!mmmRunId) return
    if (runStatus === 'finished') {
      setCurrentStep((step) => (step === 'optimize' || selectedRunView === 'budget' ? 'optimize' : 'results'))
    } else if (runStatus === 'queued' || runStatus === 'running') {
      setCurrentStep('model_run')
    }
  }, [mmmRunId, runStatus, selectedRunView])

  const renderStepContent = () => {
    if (mmmRunId) {
      // Run lifecycle: queued / running / error / finished
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
      return (
        <div
          style={{
            padding: t.space.xl,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.warning}`,
            backgroundColor: t.color.warningMuted,
            textAlign: 'center',
          }}
        >
          <p
            style={{
              margin: 0,
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.warning,
            }}
          >
            Model {runStatus === 'running' ? 'running' : 'queued'}…
          </p>
          <p
            style={{
              marginTop: t.space.xs,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            This may take a few minutes. You can navigate away and return to MMM; progress is preserved for this session.
          </p>
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
    const activeRunView: MMMRunView = selectedRunView === 'budget' && hasBudgetView ? 'budget' : 'analysis'
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
    const healthTone =
      runStatus === 'error'
        ? 'danger'
        : runStatus !== 'finished'
          ? 'warning'
          : !datasetAvailable || diagnosticsIssues.length || (hasR2 && r2Value < 0.3)
            ? 'warning'
            : 'success'
    const healthLabel =
      runStatus !== 'finished'
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
    const modelMeta = [
      runData?.engine ? String(runData.engine) : null,
      runData?.config?.frequency ? `Frequency ${runData.config.frequency}` : 'Weekly',
      Array.isArray(runData?.config?.spend_channels)
        ? `${runData.config.spend_channels.length.toLocaleString()} channels`
        : null,
    ].filter((item): item is string => Boolean(item))
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
                onClick={() => hasBudgetView && jumpToMmmView('budget')}
                disabled={!hasBudgetView}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  color: activeRunView === 'budget' ? '#ffffff' : hasBudgetView ? t.color.text : t.color.textMuted,
                  backgroundColor: activeRunView === 'budget' ? t.color.accent : t.color.surface,
                  border: `1px solid ${activeRunView === 'budget' ? t.color.accent : t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: hasBudgetView ? 'pointer' : 'not-allowed',
                }}
              >
                Budget actions
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
                    helper: diagnosticsIssues.length ? diagnosticsIssues.join(', ') : modelMeta.join(' · ') || 'Diagnostics unavailable',
                  },
                  {
                    label: 'KPI source',
                    value: runData?.config?.kpi || runData?.kpi || 'KPI',
                    helper: runData?.attribution_config_id
                      ? `${kpiSource}, config ${String(runData.attribution_config_id).slice(0, 8)}…`
                      : kpiSource,
                  },
                  {
                    label: 'Dataset',
                    value: datasetAvailable ? 'Available' : 'Readout only',
                    helper: datasetId ? String(datasetId) : 'No linked dataset',
                  },
                  {
                    label: 'Budget work',
                    value: `${scenarioCount.toLocaleString()} scenario${scenarioCount === 1 ? '' : 's'}`,
                    helper: latestScenarioAt ? `Latest ${latestScenarioAt}` : hasBudgetView ? 'Recommendations available' : 'No optimizer readout yet',
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
              {!datasetAvailable ? (
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
              onOpenBudgetActions={hasBudgetView ? () => jumpToMmmView('budget') : undefined}
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
            />
          </div>
        )}
      </div>
    )
  }

  const nextStep = () => {
    if (activeStepIndex < steps.length - 1) {
      const target = steps[activeStepIndex + 1]
      if (target.status !== 'not_started') {
        setCurrentStep(target.key)
      }
    }
  }

  const primaryKpi = primaryKpiLabel ?? (kpiMode === 'sales' ? 'Total sales' : 'Marketing-driven conversions')
  const activeStep = steps[activeStepIndex]
  const statusLabel =
    activeStep?.status === 'completed'
      ? 'Completed'
      : activeStep?.status === 'ready'
        ? 'Ready'
        : 'Not started'
  const canContinue =
    activeStepIndex < steps.length - 1 && steps[activeStepIndex + 1]?.status !== 'not_started'
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
  const workspaceSubtitle = mmmRunId
    ? 'Selected MMM run is the active workspace. Results, budget recommendations, saved scenarios, and rollout evidence stay attached to this run.'
    : 'Pick up prior MMM results and budget scenarios without starting a new model run.'

  return (
    <DashboardPage
      title="Marketing Mix Modeling"
      description={`Model incremental media impact on ${primaryKpi} and move directly from model setup into budget decisions.`}
      actions={workflowActions}
      filters={null}
      dateRange={null}
    >
      <div style={{ maxWidth: 1400, margin: '0 auto', display: 'grid', gap: t.space.xl }}>
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
                      Recommended MMM run
                    </div>
                    <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                      {latestFinishedRun.kpi || 'MMM run'} · {formatRunDate(latestFinishedRun.updated_at || latestFinishedRun.created_at)}
                    </div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      {latestFinishedRun.n_channels.toLocaleString()} channels
                      {latestFinishedRun.r2 != null ? ` · R² ${latestFinishedRun.r2.toFixed(3)}` : ''}
                      {latestFinishedRun.dataset_id ? ` · dataset ${latestFinishedRun.dataset_id.slice(0, 8)}…` : ''}
                      {latestFinishedRun.dataset_available === false ? ' · readout only' : ''}
                      {Number(latestFinishedRun.scenario_count || 0) > 0
                        ? ` · ${Number(latestFinishedRun.scenario_count).toLocaleString()} saved budget scenario${Number(latestFinishedRun.scenario_count) === 1 ? '' : 's'}`
                        : ' · no saved budget scenarios yet'}
                    </div>
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
                      style={{
                        padding: `${t.space.sm}px ${t.space.lg}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: t.color.surface,
                        color: t.color.text,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightSemibold,
                        cursor: 'pointer',
                      }}
                    >
                      Open budget actions
                    </button>
                  </div>
                </div>
              )}

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: t.space.md }}>
                {recentRuns.map((run) => {
                  const isSelected = run.run_id === mmmRunId
                  const isStale = run.status === 'stale'
                  const statusColor =
                    run.status === 'finished'
                      ? t.color.success
                      : run.status === 'error'
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
                          {run.status}
                        </div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {formatRunDate(run.updated_at || run.created_at)}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        {run.n_channels.toLocaleString()} channels
                        {run.r2 != null ? ` · R² ${run.r2.toFixed(3)}` : ''}
                        {run.dataset_id ? ` · dataset ${run.dataset_id.slice(0, 8)}…` : ''}
                        {run.dataset_available === false ? ' · readout only' : ''}
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
                          disabled={!run.dataset_id || run.status !== 'finished'}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: t.color.bg,
                            color: run.status === 'finished' ? t.color.textSecondary : t.color.textMuted,
                            fontSize: t.font.sizeXs,
                            cursor: run.dataset_id && run.status === 'finished' ? 'pointer' : 'not-allowed',
                          }}
                        >
                          {isSelected ? 'Current budget' : 'Open budget actions'}
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </SectionCard>

        {mmmRunId ? renderRunContent() : null}

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
              <>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: t.space.md,
                overflowX: 'auto',
                paddingBottom: t.space.xs,
              }}
            >
              {steps.map((step, idx) => {
                const isActive = step.key === currentStep
                const isCompleted = step.status === 'completed'
                const isClickable = step.status !== 'not_started'
                return (
                  <div
                    key={step.key}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: t.space.xs,
                    }}
                  >
                    <button
                      type="button"
                      onClick={() => setStepSafely(step.key)}
                      disabled={!isClickable}
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: 999,
                        border: `1px solid ${
                          isActive ? t.color.accent : isCompleted ? t.color.success : t.color.border
                        }`,
                        backgroundColor: isActive
                          ? t.color.accent
                          : isCompleted
                            ? t.color.successMuted
                            : t.color.surface,
                        color: isActive
                          ? '#ffffff'
                          : isCompleted
                            ? t.color.success
                            : t.color.textSecondary,
                        cursor: isClickable ? 'pointer' : 'default',
                        fontSize: t.font.sizeSm,
                        fontWeight: isActive ? t.font.weightSemibold : t.font.weightMedium,
                        display: 'flex',
                        alignItems: 'center',
                        gap: t.space.xs,
                        whiteSpace: 'nowrap',
                      }}
                    >
                      <span
                        style={{
                          width: 18,
                          height: 18,
                          borderRadius: 999,
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          fontSize: t.font.sizeXs,
                          backgroundColor: isCompleted
                            ? t.color.success
                            : isActive
                              ? 'rgba(15,23,42,0.15)'
                              : t.color.bg,
                          color: isCompleted ? '#ffffff' : isActive ? '#ffffff' : t.color.textMuted,
                        }}
                      >
                        {idx + 1}
                      </span>
                      <span>{step.label}</span>
                    </button>
                    {idx < steps.length - 1 && (
                      <div
                        style={{
                          width: 32,
                          height: 1,
                          backgroundColor: t.color.borderLight,
                          flexShrink: 0,
                        }}
                      />
                    )}
                  </div>
                )
              })}
            </div>

            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: t.space.sm,
              }}
            >
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <strong style={{ color: t.color.text }}>Step {activeStepIndex + 1}:</strong> {activeStep?.label}{' '}
                <span style={{ color: t.color.textMuted }}>({statusLabel})</span>
              </div>
              <button
                type="button"
                onClick={nextStep}
                disabled={!canContinue}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  backgroundColor: canContinue ? t.color.accent : t.color.border,
                  color: '#ffffff',
                  cursor: canContinue ? 'pointer' : 'not-allowed',
                }}
              >
                Continue
              </button>
            </div>
              </>
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
