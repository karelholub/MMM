import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import { apiGetJson } from '../lib/apiClient'

const t = tokens

interface RunSummary {
  run_id: string
  status: string
  created_at: string | null
  updated_at: string | null
  dataset_id: string | null
  kpi_mode: string | null
  kpi: string | null
  n_channels: number
  n_covariates: number
  r2?: number
  engine?: string
  stage?: string | null
  progress_pct?: number | null
}

interface PendingMapping {
  dataset_id: string
  columns: { kpi: string; spend_channels: string[]; covariates?: string[] }
}

interface MMMRunConfigStepProps {
  pendingMapping: PendingMapping
  onStartRun: (config: {
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
  onSelectRun: (runId: string, datasetId: string) => void
  currentRunId: string | null
  isRunning: boolean
}

export default function MMMRunConfigStep({
  pendingMapping,
  onStartRun,
  onSelectRun,
  currentRunId,
  isRunning,
}: MMMRunConfigStepProps) {
  const initialKpiMode = /sales|revenue|value/i.test(pendingMapping.columns.kpi) ? 'sales' : 'conversions'
  const [kpiMode, setKpiMode] = useState(initialKpiMode)
  const [useAdstock, setUseAdstock] = useState(true)
  const [useSaturation, setUseSaturation] = useState(true)
  const [holdoutWeeks, setHoldoutWeeks] = useState(8)
  const [seed, setSeed] = useState<string>('')

  useEffect(() => {
    setKpiMode(initialKpiMode)
  }, [initialKpiMode])

  const { data: runs = [], refetch } = useQuery<RunSummary[]>({
    queryKey: ['mmm-runs'],
    queryFn: async () => apiGetJson<RunSummary[]>('/api/models', { fallbackMessage: 'Failed to load runs' }),
    refetchInterval: (query) => {
      const current = (query.state?.data ?? []) as RunSummary[]
      return current.some((run) => run.status === 'queued' || run.status === 'running') ? 2000 : false
    },
  })

  const handleRun = () => {
    onStartRun({
      dataset_id: pendingMapping.dataset_id,
      kpi: pendingMapping.columns.kpi,
      spend_channels: pendingMapping.columns.spend_channels,
      covariates: pendingMapping.columns.covariates || [],
      kpi_mode: kpiMode,
      use_adstock: useAdstock,
      use_saturation: useSaturation,
      holdout_weeks: holdoutWeeks,
      random_seed: seed === '' ? null : parseInt(seed, 10) || null,
    })
  }

  const formatDate = (iso: string | null) => (iso ? new Date(iso).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' }) : '—')
  const hasEnoughChannels = pendingMapping.columns.spend_channels.length > 0
  const modelShape = useAdstock && useSaturation
    ? 'Carry-over + diminishing returns'
    : useAdstock
      ? 'Carry-over only'
      : useSaturation
        ? 'Diminishing returns only'
        : 'Simple linear ridge'

  return (
    <div style={{ maxWidth: 1180, margin: '0 auto' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: t.space.xl }}>
        {/* Left: Model run configuration */}
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            padding: t.space.xl,
            boxShadow: t.shadowSm,
          }}
        >
          <div style={{ marginBottom: t.space.lg }}>
            <h3 style={{ margin: `0 0 ${t.space.xs}px`, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Launch MMM model
            </h3>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary, lineHeight: 1.5 }}>
              Confirm the analysis question, selected spend signals, and model assumptions before creating the run.
            </p>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: t.space.md, marginBottom: t.space.xl }}>
            {[
              { label: 'Dataset', value: pendingMapping.dataset_id.slice(0, 14), helper: 'Generated source table' },
              { label: 'Target KPI column', value: pendingMapping.columns.kpi, helper: 'What the model explains' },
              { label: 'Spend signals', value: `${pendingMapping.columns.spend_channels.length}`, helper: pendingMapping.columns.spend_channels.join(', ') || 'No channels selected' },
              { label: 'Covariates', value: `${(pendingMapping.columns.covariates || []).length}`, helper: (pendingMapping.columns.covariates || []).join(', ') || 'None' },
            ].map((item) => (
              <div key={item.label} style={{ padding: t.space.md, borderRadius: t.radius.md, border: `1px solid ${t.color.borderLight}`, background: t.color.bg, minWidth: 0 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{item.label}</div>
                <div style={{ marginTop: 4, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>{item.value}</div>
                <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary, overflowWrap: 'anywhere' }}>{item.helper}</div>
              </div>
            ))}
          </div>

          <div style={{ display: 'grid', gap: t.space.lg }}>
            <section style={{ padding: t.space.lg, borderRadius: t.radius.lg, border: `1px solid ${t.color.borderLight}`, background: t.color.surface }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>
                1. Analysis target
              </div>
              <label style={{ display: 'block', fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xs }}>KPI basis</label>
              <select
                value={kpiMode}
                onChange={(e) => setKpiMode(e.target.value)}
                style={{ width: '100%', maxWidth: 320, padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
              >
                <option value="conversions">Conversions / attributed outcomes</option>
                <option value="sales">Revenue / sales value</option>
                <option value="profit">Profit</option>
              </select>
              <p style={{ margin: `${t.space.sm}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted, lineHeight: 1.45 }}>
                This labels the MMM run and keeps results, budget recommendations, and comparisons aligned with the business question.
              </p>
            </section>

            <section style={{ padding: t.space.lg, borderRadius: t.radius.lg, border: `1px solid ${t.color.borderLight}`, background: t.color.surface }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, alignItems: 'baseline', flexWrap: 'wrap', marginBottom: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  2. Media response assumptions
                </div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.accent, fontWeight: t.font.weightSemibold }}>{modelShape}</div>
              </div>
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <label style={{ display: 'flex', alignItems: 'flex-start', gap: t.space.sm, cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text }}>
                  <input type="checkbox" checked={useAdstock} onChange={(e) => setUseAdstock(e.target.checked)} style={{ marginTop: 3 }} />
                  <span>
                    <strong>Carry-over effect</strong>
                    <span style={{ display: 'block', color: t.color.textSecondary, fontSize: t.font.sizeXs, marginTop: 2 }}>
                      Lets prior spend influence later KPI movement. Keep on for most paid media.
                    </span>
                  </span>
                </label>
                <label style={{ display: 'flex', alignItems: 'flex-start', gap: t.space.sm, cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text }}>
                  <input type="checkbox" checked={useSaturation} onChange={(e) => setUseSaturation(e.target.checked)} style={{ marginTop: 3 }} />
                  <span>
                    <strong>Diminishing returns</strong>
                    <span style={{ display: 'block', color: t.color.textSecondary, fontSize: t.font.sizeXs, marginTop: 2 }}>
                      Prevents the model from assuming every extra dollar scales linearly forever.
                    </span>
                  </span>
                </label>
              </div>
            </section>

            <section style={{ padding: t.space.lg, borderRadius: t.radius.lg, border: `1px solid ${t.color.borderLight}`, background: t.color.surface }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>
                3. Validation and reproducibility
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.md }}>
                <div>
                  <label style={{ display: 'block', fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xs }}>Holdout weeks</label>
                  <input
                    type="number"
                    min={0}
                    max={52}
                    value={holdoutWeeks}
                    onChange={(e) => setHoldoutWeeks(parseInt(e.target.value, 10) || 0)}
                    style={{ width: '100%', padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
                  />
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeXs, color: t.color.textMuted }}>Most recent weeks reserved for validation.</div>
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xs }}>Random seed</label>
                  <input
                    type="number"
                    placeholder="Optional"
                    value={seed}
                    onChange={(e) => setSeed(e.target.value)}
                    style={{ width: '100%', padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
                  />
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeXs, color: t.color.textMuted }}>Set this when you need repeatable run output.</div>
                </div>
              </div>
            </section>
          </div>

          {!hasEnoughChannels && (
            <div style={{ marginTop: t.space.lg, padding: t.space.md, borderRadius: t.radius.md, border: `1px solid ${t.color.danger}`, background: t.color.dangerMuted, color: t.color.danger, fontSize: t.font.sizeSm }}>
              Select at least one spend channel before running MMM.
            </div>
          )}

          <button
            type="button"
            onClick={handleRun}
            disabled={isRunning || !hasEnoughChannels}
            style={{
              marginTop: t.space.xl,
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              color: '#fff',
              background: isRunning || !hasEnoughChannels ? t.color.border : t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: isRunning || !hasEnoughChannels ? 'not-allowed' : 'pointer',
            }}
          >
            {isRunning ? 'Starting model...' : 'Create MMM run'}
          </button>
        </div>

        {/* Right: Run history */}
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            padding: t.space.lg,
            boxShadow: t.shadowSm,
            maxHeight: 520,
            overflow: 'auto',
          }}
        >
          <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Run history
          </h3>
          {runs.length === 0 ? (
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>No runs yet. Run a model to see it here.</p>
          ) : (
            <ul style={{ listStyle: 'none', margin: 0, padding: 0 }}>
              {runs.map((run) => {
                const isSelected = run.run_id === currentRunId
                const statusColor = run.status === 'finished' ? t.color.success : run.status === 'error' ? t.color.danger : t.color.warning
                const isActive = run.status === 'queued' || run.status === 'running'
                const progressPct = Math.max(5, Math.min(95, Number(run.progress_pct ?? (run.status === 'running' ? 45 : 10))))
                return (
                  <li key={run.run_id} style={{ marginBottom: t.space.sm }}>
                    <button
                      type="button"
                      onClick={() => run.dataset_id && onSelectRun(run.run_id, run.dataset_id)}
                      style={{
                        width: '100%',
                        textAlign: 'left',
                        padding: t.space.md,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${isSelected ? t.color.accent : t.color.borderLight}`,
                        background: isSelected ? t.color.accentMuted : t.color.bg,
                        cursor: 'pointer',
                        fontSize: t.font.sizeSm,
                        color: t.color.text,
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 2 }}>
                        <span style={{ fontWeight: t.font.weightMedium }}>{formatDate(run.created_at)}</span>
                        <span style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: statusColor }}>{run.status}</span>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {run.kpi} · {run.n_channels} ch · {run.dataset_id ?? '—'}
                        {run.r2 != null && ` · R² ${run.r2.toFixed(3)}`}
                      </div>
                      {isActive ? (
                        <div style={{ display: 'grid', gap: 4, marginTop: t.space.sm }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, color: t.color.textMuted, fontSize: t.font.sizeXs }}>
                            <span>{run.stage || 'Processing'}</span>
                            <span>{progressPct.toFixed(0)}%</span>
                          </div>
                          <div style={{ height: 6, borderRadius: 999, background: t.color.surface, overflow: 'hidden', border: `1px solid ${t.color.borderLight}` }}>
                            <div style={{ width: `${progressPct}%`, height: '100%', background: t.color.warning, transition: 'width 240ms ease' }} />
                          </div>
                        </div>
                      ) : null}
                    </button>
                  </li>
                )
              })}
            </ul>
          )}
          <div style={{ display: 'flex', gap: t.space.sm, marginTop: t.space.sm, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={() => refetch()}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                color: t.color.textSecondary,
                background: 'transparent',
                border: 'none',
                cursor: 'pointer',
              }}
            >
              Refresh
            </button>
            <button
              type="button"
              disabled
              title="Compare runs (coming soon)"
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                color: t.color.textMuted,
                background: 'transparent',
                border: 'none',
                cursor: 'not-allowed',
              }}
            >
              Compare runs
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
