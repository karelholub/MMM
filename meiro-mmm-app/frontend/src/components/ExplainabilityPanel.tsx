import { useQuery } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'

interface ExplainabilityDriver {
  metric: string
  delta: number
  current_value: number
  previous_value: number
  top_contributors: { id: string; delta: number; current_value: number; previous_value: number }[]
}

interface ExplainabilitySummary {
  period: {
    current: { from: string; to: string }
    previous: { from: string; to: string }
  }
  drivers: ExplainabilityDriver[]
  data_health: {
    confidence?: {
      score: number
      label: string
      components?: Record<string, number>
    } | null
    notes: string[]
  }
  config: {
    config_id?: string | null
    version?: number | null
    changes: { at: string; actor: string; action: string }[]
  }
  mechanics: {
    model: string
    windows?: {
      click_lookback_days?: number
      impression_lookback_days?: number
      session_timeout_minutes?: number
      conversion_latency_days?: number
    } | null
    eligibility?: Record<string, unknown>
  }
}

interface Props {
  scope: 'channel' | 'campaign' | 'paths'
  scopeId?: string
  configId?: string | null
}

export default function ExplainabilityPanel({ scope, scopeId, configId }: Props) {
  const now = new Date()
  const endIso = now.toISOString()
  const start = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)
  const startIso = start.toISOString()

  const query = useQuery<ExplainabilitySummary>({
    queryKey: ['explainability', scope, scopeId ?? '', configId ?? '', startIso, endIso],
    queryFn: async () => {
      const params = new URLSearchParams({
        scope,
        from: startIso,
        to: endIso,
      })
      if (scopeId) params.append('scope_id', scopeId)
      if (configId) params.append('config_id', configId)
      const res = await fetch(`/api/explainability/summary?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load explainability summary')
      return res.json()
    },
  })

  if (query.isLoading) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.md,
          boxShadow: t.shadowSm,
          fontSize: t.font.sizeSm,
          color: t.color.textSecondary,
        }}
      >
        Computing explanation…
      </div>
    )
  }

  if (query.isError || !query.data) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.danger}`,
          borderRadius: t.radius.lg,
          padding: t.space.md,
          boxShadow: t.shadowSm,
          fontSize: t.font.sizeSm,
          color: t.color.danger,
        }}
      >
        {(query.error as Error)?.message || 'Could not compute explanation.'}
      </div>
    )
  }

  const s = query.data
  const driver = s.drivers[0]

  return (
    <div
      style={{
        background: t.color.surface,
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.lg,
        padding: t.space.lg,
        boxShadow: t.shadowSm,
        fontSize: t.font.sizeSm,
        color: t.color.textSecondary,
      }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: t.space.sm,
          gap: t.space.sm,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <div
            style={{
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            Why did this change?
          </div>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Comparing last 30 days vs the 30 days before that.
          </div>
        </div>
      </div>

      {driver && (
        <div style={{ marginBottom: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted }}>
            Key driver: {driver.metric}
          </div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            {driver.metric} changed by{' '}
            <strong>
              {driver.delta >= 0 ? '+' : ''}
              {driver.delta.toFixed(2)}
            </strong>{' '}
            (from {driver.previous_value.toFixed(2)} to {driver.current_value.toFixed(2)}).
          </div>
          {driver.top_contributors && driver.top_contributors.length > 0 && (
            <div style={{ marginTop: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 2 }}>
                Top contributing channels/campaigns:
              </div>
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {driver.top_contributors.map((c) => (
                  <li key={c.id}>
                    <span style={{ color: t.color.text }}>{c.id}</span>{' '}
                    <span style={{ color: t.color.textSecondary }}>
                      ({c.delta >= 0 ? '+' : ''}
                      {c.delta.toFixed(2)})
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      <div style={{ marginBottom: t.space.md }}>
        <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 2 }}>
          Data health
        </div>
        {s.data_health.confidence ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            Confidence: <strong>{s.data_health.confidence.label}</strong> ({s.data_health.confidence.score.toFixed(0)}
            /100)
          </div>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No confidence snapshot available yet.</div>
        )}
        {s.data_health.notes && s.data_health.notes.length > 0 && (
          <ul style={{ margin: t.space.xs, paddingLeft: 18 }}>
            {s.data_health.notes.map((n, i) => (
              <li key={i}>{n}</li>
            ))}
          </ul>
        )}
      </div>

      <div style={{ marginBottom: t.space.md }}>
        <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 2 }}>
          Config & windows
        </div>
        {s.config.config_id ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            Using config <code>{s.config.config_id}</code>
            {s.config.version != null && (
              <>
                {' '}v<strong>{s.config.version}</strong>
              </>
            )}
            {s.config.changes && s.config.changes.length > 0 && (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: 2 }}>
                Recent changes: {s.config.changes.length} (see Settings → Measurement model configs for details).
              </div>
            )}
          </div>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            No versioned model config attached; using global settings.
          </div>
        )}
        {s.mechanics.windows && (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: 4 }}>
            Click lookback: {s.mechanics.windows.click_lookback_days ?? '—'}d · Impression lookback:{' '}
            {s.mechanics.windows.impression_lookback_days ?? '—'}d · Session timeout:{' '}
            {s.mechanics.windows.session_timeout_minutes ?? '—'}min
          </div>
        )}
      </div>
    </div>
  )
}

