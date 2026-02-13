import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'
import { apiGetJson } from '../lib/apiClient'

interface AttributionComparisonProps {
  selectedModel: string
  onSelectModel: (model: string) => void
}

interface JourneysSummary {
  loaded: boolean
  count: number
  converted: number
  non_converted: number
  channels: string[]
  total_value: number
  primary_kpi_id?: string | null
  primary_kpi_label?: string | null
  primary_kpi_count?: number
  kpi_counts?: Record<string, number>
  date_min?: string | null
  date_max?: string | null
}

type Reliability = 'ok' | 'warning' | 'unreliable'

interface MarkovDiagnostics {
  journeys_used?: number
  converted_journeys?: number
  unique_states?: number
  unique_transitions?: number
  top_transitions?: { from: string; to: string; count: number; share?: number }[]
  warnings?: string[]
  reliability?: Reliability
  insufficient_data?: boolean
  what_to_do_next?: string[]
}

interface ModelResult {
  model: string
  channel_credit: Record<string, number>
  total_conversions: number
  total_value: number
  channels: { channel: string; attributed_value: number; attributed_share: number; attributed_conversions: number }[]
  error?: string
  config?: {
    config_id?: string
    config_version?: number
    conversion_key?: string | null
    time_window?: {
      click_lookback_days?: number | null
      impression_lookback_days?: number | null
      session_timeout_minutes?: number | null
      conversion_latency_days?: number | null
    }
    eligible_touchpoints?: {
      include_channels?: string[] | null
      exclude_channels?: string[] | null
      include_event_types?: string[] | null
      exclude_event_types?: string[] | null
    }
  } | null
  diagnostics?: MarkovDiagnostics
}

const MODEL_LABELS: Record<string, string> = {
  last_touch: 'Last Touch',
  first_touch: 'First Touch',
  linear: 'Linear',
  time_decay: 'Time Decay',
  position_based: 'Position Based',
  markov: 'Data-Driven (Markov)',
}

const MODEL_COLORS: Record<string, string> = {
  last_touch: '#dc2626',
  first_touch: '#d97706',
  linear: '#3b82f6',
  time_decay: '#059669',
  position_based: '#7c3aed',
  markov: '#0ea5e9',
}

function formatCurrency(v: number): string {
  if (v >= 1000000) return `$${(v / 1000000).toFixed(1)}M`
  if (v >= 1000) return `$${(v / 1000).toFixed(0)}K`
  return `$${v.toFixed(0)}`
}

function exportComparisonCSV(
  comparisonData: Record<string, unknown>[],
  models: string[],
  opts: {
    mode: 'absolute' | 'delta'
    baselineModel?: string | null
    conversionKey?: string | null
    configVersion?: number | null
    directMode: 'include' | 'exclude_view'
  },
) {
  const headers = [
    'Channel',
    ...models.map((m) => `${MODEL_LABELS[m] || m} (%)`),
    'Mode',
    'Baseline model',
    'Conversion key',
    'Config version',
    'Direct handling',
  ]
  const rows = comparisonData.map((row) => [
    row.channel,
    ...models.map((m) => ((row[`${m}_share`] as number) || 0) * 100).map((v) => v.toFixed(1)),
    opts.mode,
    opts.mode === 'delta' && opts.baselineModel ? MODEL_LABELS[opts.baselineModel] || opts.baselineModel : '',
    opts.conversionKey || '',
    opts.configVersion != null ? String(opts.configVersion) : '',
    opts.directMode,
  ])
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `attribution-comparison-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export default function AttributionComparison({ selectedModel, onSelectModel }: AttributionComparisonProps) {
  const [sortBy, setSortBy] = useState<'channel' | string>('channel')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [comparisonMode, setComparisonMode] = useState<'absolute' | 'delta'>('absolute')
  const [baselineModel, setBaselineModel] = useState<string>('linear')
  const [showDeltaRow, setShowDeltaRow] = useState(false)
  const [directMode, setDirectMode] = useState<'include' | 'exclude_view'>('include')
  const [showMarkovDiagnostics, setShowMarkovDiagnostics] = useState(false)

  const resultsQuery = useQuery<Record<string, ModelResult>>({
    queryKey: ['attribution-results'],
    queryFn: async () => apiGetJson<Record<string, ModelResult>>('/api/attribution/results', {
      fallbackMessage: 'Failed to fetch results',
    }),
    refetchInterval: 3000,
  })

  const results = resultsQuery.data || {}
  const models = Object.keys(results).filter((k) => !results[k].error)
  const t = tokens

  const journeysQuery = useQuery<JourneysSummary>({
    queryKey: ['attribution-journeys-summary'],
    queryFn: async () => apiGetJson<JourneysSummary>('/api/attribution/journeys', {
      fallbackMessage: 'Failed to load journeys summary',
    }),
  })

  const anyResult: ModelResult | undefined = models.length ? results[models[0]] : undefined
  const configMeta = anyResult?.config ?? null
  const conversionKey = configMeta?.conversion_key ?? journeysQuery.data?.primary_kpi_id ?? null
  const configVersion = configMeta?.config_version ?? null

  const baselineKey = useMemo(() => {
    if (models.includes(baselineModel)) return baselineModel
    if (models.includes('linear')) return 'linear'
    return models[0] || ''
  }, [baselineModel, models])

  const allChannels = new Set<string>()
  for (const model of models) {
    const r = results[model]
    if (r?.channels) {
      for (const ch of r.channels) {
        if (directMode === 'exclude_view' && ch.channel.toLowerCase() === 'direct') continue
        allChannels.add(ch.channel)
      }
    }
  }
  const comparisonData = Array.from(allChannels).map((channel) => {
    const row: Record<string, unknown> = { channel }
    for (const model of models) {
      const r = results[model]
      const ch = r?.channels?.find((c: { channel: string }) => c.channel === channel)
      row[model] = ch ? ch.attributed_value : 0
      row[`${model}_share`] = ch ? ch.attributed_share : 0
    }
    return row
  })

  const deltaData = useMemo(() => {
    if (!baselineKey || comparisonMode === 'absolute') return comparisonData
    return comparisonData.map((row) => {
      const baseVal = Number(row[baselineKey] ?? 0)
      const next: Record<string, unknown> = { channel: row.channel }
      for (const m of models) {
        const v = Number(row[m] ?? 0)
        next[m] = v - baseVal
        next[`${m}_share`] = row[`${m}_share`]
      }
      return next
    })
  }, [baselineKey, comparisonMode, comparisonData, models])

  const chartData = comparisonMode === 'absolute' ? comparisonData : deltaData

  const sortedData = useMemo(() => {
    return [...comparisonMode === 'absolute' ? comparisonData : deltaData].sort((a, b) => {
      let va: number | string = a[sortBy] as number | string
      let vb: number | string = b[sortBy] as number | string
      if (sortBy === 'channel') {
        va = String(va)
        vb = String(vb)
        return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va)
      }
      va = Number(va ?? 0)
      vb = Number(vb ?? 0)
      return sortDir === 'asc' ? va - vb : vb - va
    })
  }, [comparisonData, deltaData, comparisonMode, sortBy, sortDir])

  const selectedResult = results[selectedModel]

  const markovResult: ModelResult | undefined = results['markov']
  const markovDiagnostics: MarkovDiagnostics | undefined = markovResult?.diagnostics

  const winnersLosers = useMemo(() => {
    if (comparisonMode !== 'delta' || !baselineKey || !models.length || !comparisonData.length) {
      return { winners: [] as { channel: string; delta: number }[], losers: [] as { channel: string; delta: number }[] }
    }
    if (!models.includes(selectedModel)) {
      return { winners: [], losers: [] }
    }
    const winners: { channel: string; delta: number }[] = []
    const losers: { channel: string; delta: number }[] = []
    for (const row of comparisonData) {
      const ch = String(row.channel)
      const baseVal = Number(row[baselineKey] ?? 0)
      const val = Number(row[selectedModel] ?? 0)
      const delta = val - baseVal
      if (delta > 0) winners.push({ channel: ch, delta })
      else if (delta < 0) losers.push({ channel: ch, delta })
    }
    winners.sort((a, b) => b.delta - a.delta)
    losers.sort((a, b) => a.delta - b.delta)
    return {
      winners: winners.slice(0, 3),
      losers: losers.slice(0, 3),
    }
  }, [baselineKey, comparisonData, comparisonMode, models, selectedModel])

  if (resultsQuery.isError) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.danger}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl,
          boxShadow: t.shadowSm,
        }}
      >
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.danger }}>
          Could not load attribution results
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          {(resultsQuery.error as Error)?.message || 'Backend may be unreachable. Check that the API is running and CORS/proxy is correct.'}
        </p>
      </div>
    )
  }

  if (resultsQuery.isLoading) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl * 2,
          textAlign: 'center',
          boxShadow: t.shadowSm,
        }}
      >
        <p style={{ fontSize: t.font.sizeBase, color: t.color.textSecondary, margin: 0 }}>
          Loading attribution results…
        </p>
      </div>
    )
  }

  if (models.length === 0) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl,
          boxShadow: t.shadowSm,
        }}
      >
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          No attribution results
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          Run models first: load journeys and click &quot;Re-run All Models&quot; in the header.
        </p>
      </div>
    )
  }

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <h1
        style={{
          margin: 0,
          fontSize: t.font.size2xl,
          fontWeight: t.font.weightBold,
          color: t.color.text,
          letterSpacing: '-0.02em',
        }}
      >
        Attribution Model Comparison
      </h1>
      <p style={{ margin: `${t.space.xs}px 0 ${t.space.xl}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        Compare how different attribution models distribute credit across channels.
      </p>

      {/* Measurement context header */}
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: t.space.md,
          alignItems: 'center',
          marginBottom: t.space.lg,
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.md,
          boxShadow: t.shadowSm,
        }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Conversion
          </span>
          <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            {conversionKey ? `Conversion: ${conversionKey}` : 'Conversion: N/A'}
          </span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Date range
          </span>
          <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            {journeysQuery.data?.date_min && journeysQuery.data?.date_max
              ? `${new Date(journeysQuery.data.date_min).toLocaleDateString()} – ${new Date(
                  journeysQuery.data.date_max,
                ).toLocaleDateString()}`
              : 'Current dataset (range not configured)'}
          </span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Direct handling (view filter)
          </span>
          <div
            style={{
              display: 'inline-flex',
              borderRadius: t.radius.full,
              border: `1px solid ${t.color.border}`,
              overflow: 'hidden',
            }}
          >
            <button
              type="button"
              onClick={() => setDirectMode('include')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                border: 'none',
                cursor: 'pointer',
                backgroundColor: directMode === 'include' ? t.color.accent : 'transparent',
                color: directMode === 'include' ? t.color.surface : t.color.textSecondary,
              }}
            >
              Include Direct
            </button>
            <button
              type="button"
              onClick={() => setDirectMode('exclude_view')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                border: 'none',
                cursor: 'pointer',
                backgroundColor: directMode === 'exclude_view' ? t.color.accent : 'transparent',
                color: directMode === 'exclude_view' ? t.color.surface : t.color.textSecondary,
              }}
            >
              Exclude Direct
            </button>
          </div>
        </div>

        <div
          style={{
            marginLeft: 'auto',
            fontSize: t.font.sizeXs,
            color: t.color.textMuted,
            maxWidth: 380,
          }}
        >
          {configMeta?.time_window ? (
            <>
              Click lookback: {configMeta.time_window.click_lookback_days ?? '—'}d · Impression lookback:{' '}
              {configMeta.time_window.impression_lookback_days ?? '—'}d · Session timeout:{' '}
              {configMeta.time_window.session_timeout_minutes ?? '—'}min
            </>
          ) : (
            <>Measurement windows not configured for this model.</>
          )}
          {configMeta?.eligible_touchpoints && (
            <div style={{ marginTop: 2 }}>
              Touchpoints:{' '}
              {[
                configMeta.eligible_touchpoints.include_channels?.length
                  ? `+${configMeta.eligible_touchpoints.include_channels.join(', ')}`
                  : null,
                configMeta.eligible_touchpoints.exclude_channels?.length
                  ? `excl. ${configMeta.eligible_touchpoints.exclude_channels.join(', ')}`
                  : null,
              ]
                .filter(Boolean)
                .join(' · ')}
            </div>
          )}
        </div>
      </div>

      {/* Summary strip */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            padding: `${t.space.lg}px ${t.space.xl}px`,
            boxShadow: t.shadowSm,
          }}
        >
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            Models
          </div>
          <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
            {models.length}
          </div>
        </div>
        {selectedResult && (
          <>
            <div
              style={{
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: `${t.space.lg}px ${t.space.xl}px`,
                boxShadow: t.shadowSm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Selected: {MODEL_LABELS[selectedModel] || selectedModel}
              </div>
              <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
                {formatCurrency(selectedResult.total_value)}
              </div>
            </div>
            <div
              style={{
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: `${t.space.lg}px ${t.space.xl}px`,
                boxShadow: t.shadowSm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Conversions
              </div>
              <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
                {selectedResult.total_conversions.toLocaleString()}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Comparison bar chart */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          marginBottom: t.space.xl,
          boxShadow: t.shadowSm,
        }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: t.space.md,
              gap: t.space.md,
              flexWrap: 'wrap',
            }}
          >
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              {comparisonMode === 'absolute' ? 'Attributed Revenue by Model & Channel' : 'Delta vs Baseline by Channel'}
            </h3>
            <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Comparison mode</span>
                <div
                  style={{
                    display: 'inline-flex',
                    borderRadius: t.radius.full,
                    border: `1px solid ${t.color.border}`,
                    overflow: 'hidden',
                  }}
                >
                  <button
                    type="button"
                    onClick={() => setComparisonMode('absolute')}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: 'none',
                      cursor: 'pointer',
                      backgroundColor: comparisonMode === 'absolute' ? t.color.accent : 'transparent',
                      color: comparisonMode === 'absolute' ? t.color.surface : t.color.textSecondary,
                    }}
                  >
                    Absolute
                  </button>
                  <button
                    type="button"
                    onClick={() => setComparisonMode('delta')}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: 'none',
                      cursor: 'pointer',
                      backgroundColor: comparisonMode === 'delta' ? t.color.accent : 'transparent',
                      color: comparisonMode === 'delta' ? t.color.surface : t.color.textSecondary,
                    }}
                  >
                    Delta vs baseline
                  </button>
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Baseline</span>
                <select
                  value={baselineKey}
                  onChange={(e) => setBaselineModel(e.target.value)}
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    fontSize: t.font.sizeXs,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                  }}
                >
                  {models.map((m) => (
                    <option key={m} value={m}>
                      {MODEL_LABELS[m] || m}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          {comparisonMode === 'delta' && winnersLosers.winners.length + winnersLosers.losers.length > 0 && (
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: '1fr 1fr',
                gap: t.space.md,
                marginBottom: t.space.md,
                fontSize: t.font.sizeXs,
              }}
            >
              <div>
                <div style={{ fontWeight: t.font.weightSemibold, color: t.color.success, marginBottom: 4 }}>
                  Winners vs baseline (selected model)
                </div>
                {winnersLosers.winners.length === 0 && (
                  <div style={{ color: t.color.textMuted }}>No channels gaining material credit.</div>
                )}
                {winnersLosers.winners.map((w) => (
                  <div key={w.channel} style={{ color: t.color.text }}>
                    {w.channel}: <span style={{ color: t.color.success }}>{formatCurrency(w.delta)}</span>
                  </div>
                ))}
              </div>
              <div>
                <div style={{ fontWeight: t.font.weightSemibold, color: t.color.danger, marginBottom: 4 }}>
                  Losers vs baseline (selected model)
                </div>
                {winnersLosers.losers.length === 0 && (
                  <div style={{ color: t.color.textMuted }}>No channels losing material credit.</div>
                )}
                {winnersLosers.losers.map((l) => (
                  <div key={l.channel} style={{ color: t.color.text }}>
                    {l.channel}: <span style={{ color: t.color.danger }}>{formatCurrency(l.delta)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          <ResponsiveContainer width="100%" height={360}>
            <BarChart data={chartData} margin={{ top: 8, right: 16, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
            <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
            <YAxis
              tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }}
              tickFormatter={(v) => formatCurrency(v)}
            />
            <Tooltip formatter={(value: number) => formatCurrency(value)} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
            {models.map((model) => (
              <Bar
                key={model}
                dataKey={model}
                name={MODEL_LABELS[model] || model}
                fill={MODEL_COLORS[model] || t.color.textMuted}
                radius={[4, 4, 0, 0]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Model cards */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
          gap: t.space.lg,
          marginBottom: t.space.xl,
        }}
      >
        {models.map((model) => {
          const r = results[model]
          const isSelected = model === selectedModel
          return (
            <div
              key={model}
              onClick={() => onSelectModel(model)}
              style={{
                background: t.color.surface,
                border: `2px solid ${isSelected ? MODEL_COLORS[model] : t.color.borderLight}`,
                borderRadius: t.radius.lg,
                padding: t.space.lg,
                boxShadow: t.shadowSm,
                cursor: 'pointer',
                transition: 'border-color 0.2s, box-shadow 0.2s',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.md }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <h4 style={{ margin: 0, fontSize: t.font.sizeBase, fontWeight: t.font.weightBold, color: MODEL_COLORS[model] }}>
                    {MODEL_LABELS[model] || model}
                  </h4>
                  {model === 'markov' && r.diagnostics && (
                    <span
                      style={{
                        alignSelf: 'flex-start',
                        padding: '2px 8px',
                        borderRadius: 999,
                        fontSize: t.font.sizeXs,
                        fontWeight: t.font.weightSemibold,
                        color:
                          r.diagnostics.reliability === 'unreliable'
                            ? t.color.danger
                            : r.diagnostics.reliability === 'warning'
                            ? t.color.warning
                            : t.color.success,
                        backgroundColor:
                          r.diagnostics.reliability === 'unreliable'
                            ? `${t.color.danger}18`
                            : r.diagnostics.reliability === 'warning'
                            ? `${t.color.warning}18`
                            : `${t.color.success}18`,
                      }}
                    >
                      Reliability:{' '}
                      {(r.diagnostics.reliability || 'ok').toUpperCase()}
                    </span>
                  )}
                </div>
                {isSelected && (
                  <span
                    style={{
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightBold,
                      color: t.color.surface,
                      backgroundColor: MODEL_COLORS[model],
                      padding: '2px 8px',
                      borderRadius: 10,
                    }}
                  >
                    ACTIVE
                  </span>
                )}
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <div style={{ marginBottom: 4 }}>
                  Conversions:{' '}
                  <strong style={{ color: t.color.text }}>{r.total_conversions}</strong>
                </div>
                <div style={{ marginBottom: 4 }}>
                  Total value:{' '}
                  <strong style={{ color: t.color.text }}>{formatCurrency(r.total_value)}</strong>
                </div>
                {model === 'markov' && r.diagnostics?.insufficient_data && (
                  <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.danger }}>
                    Insufficient data for stable Markov. Treat results as unreliable.
                  </div>
                )}
              </div>
              {model === 'markov' && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation()
                    setShowMarkovDiagnostics(true)
                  }}
                  style={{
                    marginTop: t.space.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    fontSize: t.font.sizeXs,
                    color: t.color.accent,
                    background: 'transparent',
                    border: 'none',
                    cursor: 'pointer',
                    textDecoration: 'underline',
                  }}
                >
                  View diagnostics
                </button>
              )}
              {model !== baselineKey && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation()
                    setBaselineModel(model)
                  }}
                  style={{
                    marginTop: t.space.xs,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                    background: 'transparent',
                    border: `1px dashed ${t.color.borderLight}`,
                    borderRadius: t.radius.full,
                    cursor: 'pointer',
                  }}
                >
                  Set as baseline
                </button>
              )}
              {r.channels && (
                <div style={{ fontSize: t.font.sizeSm }}>
                  {r.channels.slice(0, 4).map((ch) => (
                    <div key={ch.channel} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: `1px solid ${t.color.borderLight}` }}>
                      <span style={{ color: t.color.text }}>{ch.channel}</span>
                      <span style={{ fontWeight: t.font.weightSemibold, color: MODEL_COLORS[model] }}>{(ch.attributed_share * 100).toFixed(1)}%</span>
                    </div>
                  ))}
                  {r.channels.length > 4 && (
                    <div style={{ color: t.color.textMuted, fontSize: t.font.sizeXs, marginTop: 4 }}>+{r.channels.length - 4} more</div>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* Comparison table + export */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.lg, flexWrap: 'wrap', gap: t.space.md }}>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Attribution Share by Model (%)
          </h3>
          <label
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            <input
              type="checkbox"
              checked={showDeltaRow}
              onChange={(e) => setShowDeltaRow(e.target.checked)}
              disabled={comparisonMode !== 'delta' || !baselineKey}
            />
            Show delta vs baseline
          </label>
          <button
            type="button"
          onClick={() =>
              exportComparisonCSV(comparisonData, models, {
                mode: comparisonMode,
                baselineModel: comparisonMode === 'delta' ? baselineKey : null,
                conversionKey,
                configVersion,
                directMode,
              })
            }
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.accent,
              background: 'transparent',
              border: `1px solid ${t.color.accent}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Export CSV
          </button>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'left',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    cursor: 'pointer',
                    userSelect: 'none',
                  }}
                  onClick={() => {
                    setSortBy('channel')
                    setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                  }}
                >
                  Channel {sortBy === 'channel' && (sortDir === 'asc' ? '↑' : '↓')}
                </th>
                {models.map((model) => (
                  <th
                    key={model}
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'center',
                      fontWeight: t.font.weightSemibold,
                      color: MODEL_COLORS[model],
                      borderBottom: `2px solid ${t.color.border}`,
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => {
                      setSortBy(`${model}_share`)
                      setSortDir('desc')
                    }}
                  >
                    {MODEL_LABELS[model] || model}
                    {model === selectedModel && ' *'}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedData.map((row, idx) => (
                <tr
                  key={String(row.channel)}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                  }}
                >
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, fontWeight: t.font.weightMedium, color: t.color.text }}>
                    {String(row.channel)}
                  </td>
                  {models.map((model) => {
                    const share = ((row[`${model}_share`] as number) || 0) * 100
                    const maxShare = Math.max(...models.map((m) => ((row[`${m}_share`] as number) || 0) * 100))
                    const isMax = share > 0 && share === maxShare
                    const baseShare =
                      comparisonMode === 'delta' && baselineKey
                        ? (((row[`${baselineKey}_share`] as number) || 0) * 100)
                        : null
                    const deltaShare =
                      comparisonMode === 'delta' && baselineKey && baseShare != null
                        ? share - baseShare
                        : null
                    return (
                      <td
                        key={model}
                        style={{
                          padding: `${t.space.md}px ${t.space.lg}px`,
                          textAlign: 'center',
                          fontWeight: isMax ? t.font.weightBold : t.font.weightNormal,
                          color: isMax ? MODEL_COLORS[model] : t.color.text,
                          backgroundColor: share > 20 ? `${MODEL_COLORS[model]}18` : 'transparent',
                          fontVariantNumeric: 'tabular-nums',
                        }}
                      >
                        <div>{share.toFixed(1)}%</div>
                        {showDeltaRow && deltaShare != null && (
                          <div
                            style={{
                              fontSize: t.font.sizeXs,
                              color: deltaShare > 0 ? t.color.success : deltaShare < 0 ? t.color.danger : t.color.textMuted,
                            }}
                          >
                            {deltaShare > 0 ? '+' : ''}
                            {deltaShare.toFixed(1)}pp
                          </div>
                        )}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Markov diagnostics drawer */}
      {showMarkovDiagnostics && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            backgroundColor: '#00000055',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 40,
          }}
          onClick={() => setShowMarkovDiagnostics(false)}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              maxWidth: 520,
              width: '100%',
              maxHeight: '80vh',
              overflowY: 'auto',
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.border}`,
              padding: t.space.lg,
              boxShadow: t.shadowLg,
            }}
          >
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: t.space.sm,
              }}
            >
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Data-Driven (Markov) diagnostics
              </h3>
              <button
                type="button"
                onClick={() => setShowMarkovDiagnostics(false)}
                style={{
                  border: 'none',
                  background: 'transparent',
                  cursor: 'pointer',
                  fontSize: t.font.sizeBase,
                  color: t.color.textSecondary,
                }}
              >
                ✕
              </button>
            </div>
            {!markovDiagnostics && (
              <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, margin: 0 }}>
                Diagnostics are not available for this Markov run.
              </p>
            )}
            {markovDiagnostics && (
              <>
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
                    gap: t.space.md,
                    marginBottom: t.space.md,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Journeys used</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.journeys_used ?? 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Converted journeys</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.converted_journeys ?? 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Unique states</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.unique_states ?? 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Unique transitions</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.unique_transitions ?? 'N/A'}
                    </div>
                  </div>
                </div>

                <div style={{ marginBottom: t.space.md }}>
                  <div
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: 4,
                    }}
                  >
                    Top transitions
                  </div>
                  {!markovDiagnostics.top_transitions || markovDiagnostics.top_transitions.length === 0 ? (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>Not available.</div>
                  ) : (
                    <ul
                      style={{
                        listStyle: 'none',
                        padding: 0,
                        margin: 0,
                        fontSize: t.font.sizeSm,
                        color: t.color.text,
                      }}
                    >
                      {markovDiagnostics.top_transitions.map((tr, idx) => (
                        <li key={`${tr.from}-${tr.to}-${idx}`} style={{ marginBottom: 2 }}>
                          {tr.from} → {tr.to}{' '}
                          <span style={{ color: t.color.textSecondary }}>
                            ({tr.count} paths{tr.share != null ? ` • ${(tr.share * 100).toFixed(1)}%` : ''})
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div style={{ marginBottom: t.space.md }}>
                  <div
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: 4,
                    }}
                  >
                    Triggered warnings
                  </div>
                  {!markovDiagnostics.warnings || markovDiagnostics.warnings.length === 0 ? (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>No warnings triggered.</div>
                  ) : (
                    <ul
                      style={{
                        margin: 0,
                        paddingLeft: t.space.lg,
                        fontSize: t.font.sizeSm,
                        color: t.color.text,
                      }}
                    >
                      {markovDiagnostics.warnings.map((w, idx) => (
                        <li key={idx}>{w}</li>
                      ))}
                    </ul>
                  )}
                </div>

                <div>
                  <div
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: 4,
                    }}
                  >
                    What to do next
                  </div>
                  <ul
                    style={{
                      margin: 0,
                      paddingLeft: t.space.lg,
                      fontSize: t.font.sizeSm,
                      color: t.color.text,
                    }}
                  >
                    {(markovDiagnostics.what_to_do_next && markovDiagnostics.what_to_do_next.length
                      ? markovDiagnostics.what_to_do_next
                      : [
                          'Increase data window or volume to include more journeys.',
                          'Improve source and campaign mapping, especially for Direct-heavy traffic.',
                          'Compare Markov deltas against simpler models (Linear, Position-based).',
                          'Use simpler models until Markov reliability improves.',
                        ]
                    ).map((item, idx) => (
                      <li key={idx}>{item}</li>
                    ))}
                  </ul>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
