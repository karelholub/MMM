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
  kpi: string | null
  n_channels: number
  r2?: number
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
          const aTime = new Date(a.updated_at || a.created_at || 0).getTime()
          const bTime = new Date(b.updated_at || b.created_at || 0).getTime()
          return bTime - aTime
        })
        .slice(0, 5),
    [recentRunsQuery.data],
  )

  const formatRunDate = (value?: string | null) =>
    value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : '—'

  const jumpToMmmView = (view: 'analysis' | 'budget') => {
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

  useEffect(() => {
    if (!mmmRunId || typeof window === 'undefined') return
    const view = new URLSearchParams(window.location.search).get('mmm_view')
    if (view !== 'budget' && view !== 'analysis') return
    const timer = window.setTimeout(() => {
      const node = view === 'budget' ? budgetSectionRef.current : analysisSectionRef.current
      node?.scrollIntoView({ behavior: 'auto', block: 'start' })
    }, 0)
    return () => window.clearTimeout(timer)
  }, [mmmRunId])

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
        <MMMDataSourceStep onMappingComplete={onMmmMappingComplete} />
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
            onClick={() => {
              onStartOver()
              setCurrentStep('data_source')
            }}
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

    const runData = mmmRunQuery.data as any
    const runCreatedAt = runData?.created_at ? new Date(runData.created_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : ''
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
            <button
              type="button"
              onClick={() => {
                onStartOver()
                setCurrentStep('data_source')
              }}
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
          }
        />
        <div style={{ marginBottom: t.space.lg, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Run <code style={{ background: t.color.bg, padding: '2px 6px', borderRadius: t.radius.sm }}>{mmmRunId}</code>
          {runCreatedAt && ` · ${runCreatedAt}`}
          {runData?.config?.kpi && ` · KPI: ${runData.config.kpi}`}
          {runData?.r2 != null && ` · R² ${runData.r2.toFixed(3)}`}
          {runData?.attribution_model ? (
            <> · KPI source: <strong>Attribution ({runData.attribution_model.replace(/_/g, ' ')}</strong>{runData?.attribution_config_id ? `, config ${runData.attribution_config_id.slice(0, 8)}…` : ''})</>
          ) : (
            <> · KPI source: <strong>Direct</strong> (independent of attribution)</>
          )}
        </div>
        <div ref={analysisSectionRef}>
          <MMMDashboard
            runId={mmmRunId!}
            datasetId={mmmDatasetId ?? ''}
            runMetadata={{ attribution_model: runData?.attribution_model, attribution_config_id: runData?.attribution_config_id }}
            onOpenDataQuality={onOpenDataQuality}
          />
        </div>
        {(mmmRunQuery.data as any)?.roi?.length > 0 &&
          (mmmRunQuery.data as any)?.contrib?.length > 0 && (
            <div ref={budgetSectionRef}>
              <BudgetOptimizer
                roiData={(mmmRunQuery.data as any).roi}
                contribData={(mmmRunQuery.data as any).contrib}
                runId={mmmRunId}
                datasetId={mmmDatasetId}
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
    mmmRunId || pendingMmmMapping || mmmDatasetId ? (
      <button
        type="button"
        onClick={() => {
          onStartOver()
          setCurrentStep('data_source')
        }}
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
        Start new MMM run
      </button>
    ) : null

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
          subtitle="MMM now follows the same workspace shell, trust language, and action model as the rest of the app."
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
          title="Workflow"
          subtitle="Prepare the dataset, run the model, review results, and move into optimization without leaving the shared workspace flow."
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
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
          </div>
        </SectionCard>

        <SectionCard
          title="Recent MMM runs"
          subtitle="Reopen prior MMM results and budget work directly from the shared workspace."
          actions={
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
          }
        >
          {recentRunsQuery.isLoading ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading MMM runs…</div>
          ) : !recentRuns.length ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              No prior runs yet. Once a model finishes, it will be reopenable here without starting a new setup flow.
            </div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: t.space.md }}>
              {recentRuns.map((run) => {
                const isSelected = run.run_id === mmmRunId
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
                      border: `1px solid ${isSelected ? t.color.accent : t.color.borderLight}`,
                      background: isSelected ? t.color.accentMuted : t.color.surface,
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
                      {run.r2 != null ? ` • R² ${run.r2.toFixed(3)}` : ''}
                      {run.dataset_id ? ` • dataset ${run.dataset_id.slice(0, 8)}…` : ''}
                    </div>
                    <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                      <button
                        type="button"
                        onClick={() => {
                          if (!run.dataset_id) return
                          onMmmSelectRun(run.run_id, run.dataset_id)
                          jumpToMmmView('analysis')
                        }}
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
                        {isSelected ? 'Current analysis' : 'Open analysis'}
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          if (!run.dataset_id) return
                          onMmmSelectRun(run.run_id, run.dataset_id)
                          jumpToMmmView('budget')
                        }}
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
          )}
        </SectionCard>

        {renderStepContent()}
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
