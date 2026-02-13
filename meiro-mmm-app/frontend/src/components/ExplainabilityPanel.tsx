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

interface ChangeDecompositionBucket {
  key: 'volume' | 'mix' | 'rate' | string
  label: string
  contribution: number
  pct_of_total?: number | null
  is_estimated?: boolean
}

interface ChangeDecomposition {
  total_delta: number
  buckets: ChangeDecompositionBucket[]
  basis: string
  is_estimated?: boolean
}

interface ComparabilityWarning {
  severity: 'info' | 'warn' | 'critical' | string
  message: string
  action?: string | null
}

interface ComparabilitySummary {
  rating: 'high' | 'medium' | 'low' | string
  warnings: ComparabilityWarning[]
}

interface DataQualityMetricDelta {
  key: string
  label: string
  unit?: string
  current: number | null
  previous: number | null
  delta: number | null
}

interface ChannelBreakdownMetricBlock {
  current: number | null
  previous: number | null
  delta: number | null
}

interface ChannelBreakdown {
  channel: string
  spend?: ChannelBreakdownMetricBlock
  conversions?: ChannelBreakdownMetricBlock
  attributed_value?: ChannelBreakdownMetricBlock
  roas?: ChannelBreakdownMetricBlock
  cpa?: ChannelBreakdownMetricBlock
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
  change_decomposition?: ChangeDecomposition | null
  comparability?: ComparabilitySummary | null
  config_diff?: {
    has_changes?: boolean
    changes_count?: number
    lines?: string[]
  }
  data_quality_delta?: {
    metrics?: DataQualityMetricDelta[]
  }
  timeline?: { date: string; attributed_value: number; spend?: number | null }[]
  channel_breakdowns?: Record<string, ChannelBreakdown>
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
  const [expandedChannel, setExpandedChannel] = useState<string | null>(null)
  const [showConfigDiff, setShowConfigDiff] = useState(false)
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
  const changeDecomp = s.change_decomposition
  const comparability = s.comparability

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

      {/* Change decomposition: Volume vs Mix vs Rate/Value */}
      {changeDecomp && changeDecomp.buckets && changeDecomp.buckets.length > 0 ? (
        <div
          style={{
            marginBottom: t.space.md,
            padding: t.space.sm,
            borderRadius: t.radius.md,
            border: `1px solid ${t.color.borderLight}`,
            background: t.color.bg,
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: t.space.xs,
              gap: t.space.sm,
              flexWrap: 'wrap',
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted }}>
              Change decomposition {changeDecomp.is_estimated ? '(estimated)' : ''}
            </div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              Total change: <strong>{changeDecomp.total_delta >= 0 ? '+' : ''}{changeDecomp.total_delta.toLocaleString()}</strong>
            </div>
          </div>
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))',
              gap: t.space.sm,
            }}
          >
            {changeDecomp.buckets.map((b) => (
              <div
                key={b.key}
                style={{
                  padding: t.space.sm,
                  borderRadius: t.radius.sm,
                  background: t.color.surface,
                  border: `1px solid ${t.color.borderLight}`,
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  {b.label}
                </div>
                <div
                  style={{
                    marginTop: 4,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    color: b.contribution >= 0 ? t.color.success : t.color.danger,
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {b.contribution >= 0 ? '+' : ''}
                  {b.contribution.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                </div>
                <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  {b.pct_of_total != null ? `${b.pct_of_total.toFixed(1)}% of change` : '—'}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

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
          {scope === 'channel' && s.drivers[0] ? (
            <div
              style={{
                padding: t.space.sm,
                background: t.color.bg,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
              }}
            >
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text, marginBottom: 4 }}>
                Attributed value change
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                {s.drivers[0].delta >= 0 ? '+' : ''}
                {formatDriverValue(s.drivers[0].metric, s.drivers[0].delta)} (from{' '}
                {formatDriverValue(s.drivers[0].metric, s.drivers[0].previous_value)} to{' '}
                {formatDriverValue(s.drivers[0].metric, s.drivers[0].current_value)})
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px 8px' }}>
                {s.drivers[0].top_contributors.slice(0, 12).map((c) => {
                  const isExpanded = expandedChannel === c.id
                  const labelDelta = `${c.delta >= 0 ? '+' : ''}${formatDriverValue(s.drivers[0].metric, c.delta)}`
                  return (
                    <button
                      key={c.id}
                      type="button"
                      onClick={() => setExpandedChannel(isExpanded ? null : c.id)}
                      title={`Channel ${c.id}: ${labelDelta}`}
                      style={{
                        border: 'none',
                        cursor: 'pointer',
                        borderRadius: t.radius.full,
                        padding: '4px 8px',
                        fontSize: t.font.sizeXs,
                        fontWeight: t.font.weightMedium,
                        backgroundColor:
                          c.delta >= 0
                            ? t.color.successMuted ?? 'rgba(34,197,94,0.12)'
                            : t.color.dangerMuted ?? 'rgba(239,68,68,0.12)',
                        color: t.color.text,
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 6,
                      }}
                    >
                      <span>{c.id}</span>
                      <span style={{ fontVariantNumeric: 'tabular-nums' }}>{labelDelta}</span>
                      <span style={{ opacity: 0.7 }}>{isExpanded ? '▴' : '▾'}</span>
                    </button>
                  )
                })}
              </div>
              {expandedChannel && s.channel_breakdowns?.[expandedChannel] && (
                <div
                  style={{
                    marginTop: t.space.sm,
                    paddingTop: t.space.sm,
                    borderTop: `1px dashed ${t.color.borderLight}`,
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                  }}
                >
                  {(() => {
                    const b = s.channel_breakdowns![expandedChannel]
                    const renderBlock = (
                      label: string,
                      key: 'spend' | 'conversions' | 'attributed_value' | 'roas' | 'cpa',
                      fmt: (v: number) => string,
                    ) => {
                      const block = b[key]
                      if (!block) return null
                      const { current, previous, delta } = block
                      return (
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm }}>
                          <span>{label}</span>
                          <span style={{ fontVariantNumeric: 'tabular-nums' }}>
                            {previous != null ? fmt(previous) : 'N/A'} → {current != null ? fmt(current) : 'N/A'}{' '}
                            {delta != null && (
                              <span style={{ color: delta >= 0 ? t.color.success : t.color.danger }}>
                                ({delta >= 0 ? '+' : ''}
                                {fmt(delta)})
                              </span>
                            )}
                          </span>
                        </div>
                      )
                    }
                    return (
                      <div
                        style={{
                          display: 'grid',
                          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                          gap: t.space.sm,
                        }}
                      >
                        {renderBlock('Spend Δ', 'spend', (v) => v.toLocaleString(undefined, { maximumFractionDigits: 0 }))}
                        {renderBlock('Conversions Δ', 'conversions', (v) => v.toFixed(1))}
                        {renderBlock('Attributed value Δ', 'attributed_value', (v) =>
                          v.toLocaleString(undefined, { maximumFractionDigits: 0 }),
                        )}
                        {renderBlock('ROAS Δ', 'roas', (v) => v.toFixed(2))}
                        {renderBlock('CPA Δ', 'cpa', (v) => v.toFixed(2))}
                        {/* Path patterns and campaigns are not yet wired; show placeholder insufficient state */}
                        <div style={{ gridColumn: '1 / -1', marginTop: t.space.xs }}>
                          <span style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>Top campaigns & paths</span>
                          <div style={{ marginTop: 4, color: t.color.textSecondary }}>
                            Detailed campaign and path pattern breakdown for this channel is not yet available (insufficient data).
                          </div>
                        </div>
                      </div>
                    )
                  })()}
                </div>
              )}
            </div>
          ) : (
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
                      <span style={{ color: t.color.textMuted }}>
                        {' '}
                        · Top contributor: {driver.contribution_pct.toFixed(0)}% of change
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Attribution share & contribution to change (renamed from Feature importance) */}
      {featureImportance.length > 0 && (
        <div style={{ marginBottom: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 6 }}>
            Attribution share (current period)
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
                  title="Contribution to change (absolute delta)"
                >
                  {f.delta >= 0 ? '+' : ''}
                  {f.delta.toLocaleString(undefined, { maximumFractionDigits: 0 })}{' '}
                  <span style={{ color: t.color.textMuted }}>Δ</span>
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

      {/* Comparability & warnings card + data quality delta */}
      <div
        style={{
          marginBottom: t.space.md,
          padding: t.space.sm,
          borderRadius: t.radius.md,
          border: `1px solid ${t.color.borderLight}`,
          background: t.color.bg,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'flex-start',
            gap: t.space.sm,
            flexWrap: 'wrap',
            marginBottom: t.space.sm,
          }}
        >
          <div>
            <div
              style={{
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                color: t.color.textMuted,
                marginBottom: 4,
              }}
            >
              Comparability & warnings
            </div>
            {comparability ? (
              <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                <span
                  style={{
                    fontSize: t.font.sizeXs,
                    padding: '2px 8px',
                    borderRadius: t.radius.full,
                    fontWeight: t.font.weightSemibold,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                    backgroundColor:
                      comparability.rating === 'high'
                        ? t.color.successMuted
                        : comparability.rating === 'medium'
                        ? t.color.warningMuted
                        : t.color.dangerMuted,
                    color:
                      comparability.rating === 'high'
                        ? t.color.success
                        : comparability.rating === 'medium'
                        ? t.color.warning
                        : t.color.danger,
                  }}
                >
                  {comparability.rating.toUpperCase()} comparability
                </span>
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Comparability not scored (insufficient data).</div>
            )}
          </div>
        </div>
        {comparability && comparability.warnings && comparability.warnings.length > 0 && (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: t.font.sizeXs }}>
            {comparability.warnings.map((w, idx) => {
              const color =
                w.severity === 'critical' ? t.color.danger : w.severity === 'warn' ? t.color.warning : t.color.textSecondary
              return (
                <li key={idx} style={{ marginBottom: 2 }}>
                  <span style={{ color, fontWeight: t.font.weightMedium }}>{w.severity}</span>{' '}
                  <span style={{ color: t.color.text }}>{w.message}</span>
                  {w.action && (
                    <span style={{ color: t.color.accent, marginLeft: 4, cursor: 'pointer' }}>
                      {/* This is rendered as a text CTA; linking to settings is left to router integration */}
                      {w.action}
                    </span>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {/* Data quality delta mini table */}
        <div style={{ marginTop: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 4 }}>
            Data quality change
          </div>
          {s.data_quality_delta?.metrics && s.data_quality_delta.metrics.length > 0 ? (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeXs }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', padding: '2px 4px', color: t.color.textSecondary }}>Metric</th>
                    <th style={{ textAlign: 'right', padding: '2px 4px', color: t.color.textSecondary }}>Current</th>
                    <th style={{ textAlign: 'right', padding: '2px 4px', color: t.color.textSecondary }}>Previous</th>
                    <th style={{ textAlign: 'right', padding: '2px 4px', color: t.color.textSecondary }}>Δ</th>
                  </tr>
                </thead>
                <tbody>
                  {s.data_quality_delta.metrics.map((m) => (
                    <tr key={m.key}>
                      <td style={{ padding: '2px 4px', color: t.color.text }}>{m.label}</td>
                      <td style={{ padding: '2px 4px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                        {m.current != null ? m.current.toFixed(3) : 'N/A'}
                      </td>
                      <td style={{ padding: '2px 4px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                        {m.previous != null ? m.previous.toFixed(3) : 'N/A'}
                      </td>
                      <td
                        style={{
                          padding: '2px 4px',
                          textAlign: 'right',
                          fontVariantNumeric: 'tabular-nums',
                          color: m.delta != null ? (m.delta >= 0 ? t.color.success : t.color.danger) : t.color.textSecondary,
                        }}
                      >
                        {m.delta != null ? (m.delta >= 0 ? '+' : '') + m.delta.toFixed(3) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No data quality delta snapshot available.</div>
          )}
        </div>
      </div>

      {/* Config & windows with inline diff */}
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
                Recent changes: {s.config.changes.length}.
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
        {s.config_diff && s.config_diff.lines && s.config_diff.lines.length > 0 && (
          <div style={{ marginTop: t.space.sm }}>
            <button
              type="button"
              onClick={() => setShowConfigDiff((v) => !v)}
              style={{
                border: 'none',
                padding: 0,
                background: 'transparent',
                color: t.color.accent,
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
            >
              {showConfigDiff ? 'Hide config changes' : `Show config changes (${s.config_diff.changes_count ?? s.config_diff.lines.length})`}
            </button>
            {showConfigDiff && (
              <ul style={{ marginTop: t.space.xs, paddingLeft: 18, fontSize: t.font.sizeXs }}>
                {s.config_diff.lines.map((line, idx) => (
                  <li key={idx}>{line}</li>
                ))}
                <li>
                  <span style={{ color: t.color.accent }}>Open Settings → Measurement model configs</span> for full details.
                </li>
              </ul>
            )}
          </div>
        )}
      </div>

      {/* Data health (keep existing confidence narrative) */}
      <div style={{ marginTop: t.space.md }}>
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

      {/* Timeline: when did it change? */}
      {s.timeline && s.timeline.length > 0 && (
        <div style={{ marginTop: t.space.md }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: 4 }}>
            Timeline
          </div>
          <div
            style={{
              position: 'relative',
              height: 80,
              background: t.color.bg,
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.borderLight}`,
              padding: '4px 6px',
              overflow: 'hidden',
            }}
          >
            {/* Simple sparkline using CSS (no chart lib) */}
            {(() => {
              const points = s.timeline!
              const maxVal = Math.max(...points.map((p) => p.attributed_value || 0), 1)
              const widthPct = points.length > 1 ? 100 / (points.length - 1) : 0
              return (
                <svg width="100%" height="100%" preserveAspectRatio="none">
                  <polyline
                    fill="none"
                    stroke={t.color.accent}
                    strokeWidth={1.5}
                    points={points
                      .map((p, idx) => {
                        const x = (idx * widthPct).toString()
                        const y = (100 - (p.attributed_value / maxVal) * 100).toString()
                        return `${x},${y}`
                      })
                      .join(' ')}
                  />
                </svg>
              )
            })()}
          </div>
        </div>
      )}
    </div>
  )
}
