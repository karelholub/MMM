import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { tokens as t } from '../theme/tokens'

interface ExplainabilityDriver {
  metric: string
  delta: number
  current_value: number
  previous_value: number
  contribution_pct?: number | null
  top_contributors: { id: string; delta: number; current_value: number; previous_value: number }[]
}

interface FeatureImportanceItem {
  id: string
  name: string
  importance: number
  share_pct: number
  delta: number
  direction: 'up' | 'down' | 'neutral'
}

interface NarrativeBlock {
  summary: string
  period_notes: string[]
  config_notes: string[]
  data_notes: string[]
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
  feature_importance?: FeatureImportanceItem[]
  narrative?: NarrativeBlock | null
}

interface Props {
  scope: 'channel' | 'campaign' | 'paths'
  scopeId?: string
  configId?: string | null
  model?: string
}

function formatDriverValue(metric: string, value: number): string {
  if (metric === 'attributed_value' || metric === 'total_value') return value.toLocaleString(undefined, { maximumFractionDigits: 0 })
  return value.toFixed(2)
}

export default function ExplainabilityPanel({ scope, scopeId, configId, model = 'linear' }: Props) {
  const [windowRange] = useState(() => {
    const now = new Date()
    const endIso = now.toISOString()
    const startIso = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString()
    return { startIso, endIso }
  })
  const { startIso, endIso } = windowRange

  const query = useQuery<ExplainabilitySummary>({
    queryKey: ['explainability', scope, scopeId ?? '', configId ?? '', model, startIso, endIso],
    queryFn: async () => {
      const params = new URLSearchParams({
        scope,
        from: startIso,
        to: endIso,
        model,
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
  const narrative = s.narrative
  const featureImportance = s.feature_importance ?? []

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
          marginBottom: t.space.md,
          gap: t.space.sm,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Why did this change?
          </div>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Comparing last 30 days vs the 30 days before. Model: <strong>{s.mechanics.model}</strong>.
          </div>
        </div>
      </div>

      {/* Narrative summary */}
      {narrative?.summary && (
        <div style={{ marginBottom: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 4 }}>
            Summary
          </div>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.text, lineHeight: 1.5 }}>
            {narrative.summary}
          </p>
        </div>
      )}

      {/* All drivers */}
      {s.drivers.length > 0 && (
        <div style={{ marginBottom: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 6 }}>
            Drivers
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.sm }}>
            {s.drivers.map((driver, idx) => (
              <div
                key={idx}
                style={{
                  padding: t.space.sm,
                  background: t.color.bg,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                }}
              >
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  {driver.metric.replace(/_/g, ' ')}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 2 }}>
                  {driver.delta >= 0 ? '+' : ''}
                  {formatDriverValue(driver.metric, driver.delta)} (from{' '}
                  {formatDriverValue(driver.metric, driver.previous_value)} to{' '}
                  {formatDriverValue(driver.metric, driver.current_value)})
                  {driver.contribution_pct != null && (
                    <span style={{ color: t.color.textMuted }}> · Top contributor: {driver.contribution_pct.toFixed(0)}% of change</span>
                  )}
                </div>
                {driver.top_contributors && driver.top_contributors.length > 0 && (
                  <div style={{ marginTop: 6, display: 'flex', flexWrap: 'wrap', gap: '6px 12px' }}>
                    {driver.top_contributors.slice(0, 8).map((c) => (
                      <span
                        key={c.id}
                        style={{
                          fontSize: t.font.sizeXs,
                          padding: '2px 6px',
                          borderRadius: t.radius.sm,
                          background: c.delta >= 0 ? t.color.successMuted ?? 'rgba(34,197,94,0.12)' : t.color.dangerMuted ?? 'rgba(239,68,68,0.12)',
                          color: t.color.text,
                        }}
                      >
                        {c.id}: {c.delta >= 0 ? '+' : ''}
                        {formatDriverValue(driver.metric, c.delta)}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Feature importance */}
      {featureImportance.length > 0 && (
        <div style={{ marginBottom: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 6 }}>
            Feature importance (share of attributed value)
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {featureImportance.slice(0, 12).map((f) => (
              <div key={f.id} style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
                <div style={{ minWidth: 120, fontSize: t.font.sizeSm, color: t.color.text }} title={f.id}>
                  {f.name.length > 24 ? f.name.slice(0, 22) + '…' : f.name}
                </div>
                <div
                  style={{
                    flex: 1,
                    height: 8,
                    maxWidth: 200,
                    background: t.color.borderLight,
                    borderRadius: t.radius.full,
                    overflow: 'hidden',
                  }}
                >
                  <div
                    style={{
                      width: `${Math.min(100, f.share_pct)}%`,
                      height: '100%',
                      background: f.direction === 'up' ? t.color.success : f.direction === 'down' ? t.color.danger : t.color.textMuted,
                      borderRadius: t.radius.full,
                    }}
                  />
                </div>
                <span style={{ fontSize: t.font.sizeXs, fontVariantNumeric: 'tabular-nums', color: t.color.textSecondary }}>
                  {f.share_pct.toFixed(1)}%
                </span>
                <span
                  style={{
                    fontSize: t.font.sizeXs,
                    fontVariantNumeric: 'tabular-nums',
                    color: f.delta >= 0 ? t.color.success : f.delta < 0 ? t.color.danger : t.color.textMuted,
                  }}
                >
                  {f.delta >= 0 ? '+' : ''}
                  {f.delta.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Narrative bullets (period, config, data) */}
      {narrative && (narrative.period_notes.length > 0 || narrative.config_notes.length > 0 || narrative.data_notes.length > 0) && (
        <div style={{ marginBottom: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 6 }}>
            Context (config & data)
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {narrative.period_notes.length > 0 && (
              <div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginBottom: 2 }}>Period</div>
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  {narrative.period_notes.map((n, i) => (
                    <li key={i} style={{ color: t.color.text }}>{n}</li>
                  ))}
                </ul>
              </div>
            )}
            {narrative.config_notes.length > 0 && (
              <div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginBottom: 2 }}>Config</div>
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  {narrative.config_notes.map((n, i) => (
                    <li key={i} style={{ color: t.color.text }}>{n}</li>
                  ))}
                </ul>
              </div>
            )}
            {narrative.data_notes.length > 0 && (
              <div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginBottom: 2 }}>Data quality</div>
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  {narrative.data_notes.map((n, i) => (
                    <li key={i} style={{ color: t.color.text }}>{n}</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Legacy data health (keep for confidence details) */}
      <div style={{ marginBottom: t.space.md }}>
        <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 2 }}>
          Data health
        </div>
        {s.data_health.confidence ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            Confidence: <strong>{s.data_health.confidence.label}</strong> ({s.data_health.confidence.score.toFixed(0)}/100)
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

      {/* Config & windows */}
      <div>
        <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 2 }}>
          Config & windows
        </div>
        {s.config.config_id ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            Using config <code>{s.config.config_id}</code>
            {s.config.version != null && <> v<strong>{s.config.version}</strong></>}
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
