import { useState } from 'react'
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
  const [useAdstock, setUseAdstock] = useState(true)
  const [useSaturation, setUseSaturation] = useState(true)
  const [holdoutWeeks, setHoldoutWeeks] = useState(8)
  const [seed, setSeed] = useState<string>('')

  const { data: runs = [], refetch } = useQuery<RunSummary[]>({
    queryKey: ['mmm-runs'],
    queryFn: async () => apiGetJson<RunSummary[]>('/api/models', { fallbackMessage: 'Failed to load runs' }),
  })

  const handleRun = () => {
    onStartRun({
      dataset_id: pendingMapping.dataset_id,
      kpi: pendingMapping.columns.kpi,
      spend_channels: pendingMapping.columns.spend_channels,
      covariates: pendingMapping.columns.covariates || [],
      kpi_mode: 'conversions',
      use_adstock: useAdstock,
      use_saturation: useSaturation,
      holdout_weeks: holdoutWeeks,
      random_seed: seed === '' ? null : parseInt(seed, 10) || null,
    })
  }

  const formatDate = (iso: string | null) => (iso ? new Date(iso).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' }) : '—')

  return (
    <div style={{ maxWidth: 960, margin: '0 auto' }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 340px', gap: t.space.xl }}>
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
          <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Model run configuration
          </h3>

          <div style={{ marginBottom: t.space.lg }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: t.space.xs }}>Target KPI</div>
            <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightMedium, color: t.color.text }}>{pendingMapping.columns.kpi}</div>
          </div>
          <div style={{ marginBottom: t.space.lg }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: t.space.xs }}>Channels ({pendingMapping.columns.spend_channels.length})</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{pendingMapping.columns.spend_channels.join(', ')}</div>
          </div>
          <div style={{ marginBottom: t.space.lg }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: t.space.xs }}>Covariates ({(pendingMapping.columns.covariates || []).length})</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{(pendingMapping.columns.covariates || []).length ? (pendingMapping.columns.covariates || []).join(', ') : 'None'}</div>
          </div>

          <div style={{ borderTop: `1px solid ${t.color.borderLight}`, paddingTop: t.space.lg, marginTop: t.space.lg }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text, marginBottom: t.space.sm }}>Transforms</div>
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, marginBottom: t.space.sm, cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="checkbox" checked={useAdstock} onChange={(e) => setUseAdstock(e.target.checked)} />
              Adstock (carry-over effect)
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, marginBottom: t.space.sm, cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="checkbox" checked={useSaturation} onChange={(e) => setUseSaturation(e.target.checked)} />
              Saturation (diminishing returns)
            </label>
            <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textMuted }}>If both off, a simple Ridge model is used.</p>
          </div>

          <div style={{ marginTop: t.space.lg }}>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xs }}>Holdout weeks (optional)</label>
            <input
              type="number"
              min={0}
              max={52}
              value={holdoutWeeks}
              onChange={(e) => setHoldoutWeeks(parseInt(e.target.value, 10) || 0)}
              style={{ width: 80, padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
            />
            <span style={{ marginLeft: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textMuted }}>Last N weeks reserved (e.g. for validation)</span>
          </div>
          <div style={{ marginTop: t.space.md }}>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xs }}>Random seed (optional)</label>
            <input
              type="number"
              placeholder="e.g. 42"
              value={seed}
              onChange={(e) => setSeed(e.target.value)}
              style={{ width: 100, padding: t.space.sm, fontSize: t.font.sizeSm, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
            />
          </div>

          <button
            type="button"
            onClick={handleRun}
            disabled={isRunning}
            style={{
              marginTop: t.space.xl,
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              color: '#fff',
              background: isRunning ? t.color.border : t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: isRunning ? 'not-allowed' : 'pointer',
            }}
          >
            {isRunning ? 'Running…' : 'Run model'}
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
