import { useEffect, useMemo, useState } from 'react'
import { UseMutationResult, UseQueryResult } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import MMMDataSourceStep from './MMMDataSourceStep'
import MMMRunConfigStep from './MMMRunConfigStep'
import MMMDashboard from './MMMDashboard'
import BudgetOptimizer from './BudgetOptimizer'
import { MMMContextBar, KpiMode } from '../components/MMMContextBar'

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

  // Load persisted per-session state
  useEffect(() => {
    try {
      const raw = window.sessionStorage.getItem(STORAGE_KEY)
      if (!raw) return
      const parsed = JSON.parse(raw) as PersistedState
      if (parsed.currentStep) setCurrentStep(parsed.currentStep)
      if (parsed.kpiMode) setKpiMode(parsed.kpiMode)
    } catch {
      // ignore
    }
  }, [])

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
          title="MMM results"
          subtitle={
            primaryKpiLabel
              ? `ROI and contributions for ${primaryKpiLabel}.`
              : 'Bayesian MMM results: channel ROI, contribution share, and time series.'
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
        <MMMDashboard
          runId={mmmRunId!}
          datasetId={mmmDatasetId ?? ''}
          runMetadata={{ attribution_model: runData?.attribution_model, attribution_config_id: runData?.attribution_config_id }}
        />
        {(mmmRunQuery.data as any)?.roi?.length > 0 &&
          (mmmRunQuery.data as any)?.contrib?.length > 0 && (
            <BudgetOptimizer
              roiData={(mmmRunQuery.data as any).roi}
              contribData={(mmmRunQuery.data as any).contrib}
              runId={mmmRunId}
              datasetId={mmmDatasetId}
            />
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

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      {/* Page header */}
      <div
        style={{
          marginBottom: t.space.md,
          display: 'flex',
          flexWrap: 'wrap',
          justifyContent: 'space-between',
          gap: t.space.md,
        }}
      >
        <div>
          <h1
            style={{
              margin: 0,
              fontSize: t.font.size2xl,
              fontWeight: t.font.weightBold,
              color: t.color.text,
              letterSpacing: '-0.02em',
            }}
          >
            Marketing Mix Modeling
          </h1>
          <p
            style={{
              margin: `${t.space.xs}px 0 0`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Measure the incremental impact of media channels on{' '}
            <strong style={{ color: t.color.text }}>{primaryKpi}</strong> and optimize budget allocation.
          </p>
        </div>
      </div>

      {/* Measurement context bar */}
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

      {/* Stepper */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: t.space.md,
          marginBottom: t.space.lg,
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

      {/* Step CTA row */}
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
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          <strong style={{ color: t.color.text }}>Step {activeStepIndex + 1}:</strong>{' '}
          {steps[activeStepIndex]?.label}{' '}
          <span style={{ color: t.color.textMuted }}>
            ({steps[activeStepIndex]?.status === 'completed'
              ? 'Completed'
              : steps[activeStepIndex]?.status === 'ready'
                ? 'Ready'
                : 'Not started'}
            )
          </span>
        </div>
        <button
          type="button"
          onClick={nextStep}
          disabled={activeStepIndex >= steps.length - 1 || steps[activeStepIndex + 1]?.status === 'not_started'}
          style={{
            padding: `${t.space.sm}px ${t.space.lg}px`,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightMedium,
            borderRadius: t.radius.sm,
            border: 'none',
            backgroundColor:
              activeStepIndex >= steps.length - 1 || steps[activeStepIndex + 1]?.status === 'not_started'
                ? t.color.border
                : t.color.accent,
            color: '#ffffff',
            cursor:
              activeStepIndex >= steps.length - 1 || steps[activeStepIndex + 1]?.status === 'not_started'
                ? 'not-allowed'
                : 'pointer',
          }}
        >
          Continue
        </button>
      </div>

      {/* Step content */}
      {renderStepContent()}
    </div>
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

