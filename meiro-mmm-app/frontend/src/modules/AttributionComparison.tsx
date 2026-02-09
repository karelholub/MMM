import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'

interface AttributionComparisonProps {
  selectedModel: string
  onSelectModel: (model: string) => void
}

interface ModelResult {
  model: string
  channel_credit: Record<string, number>
  total_conversions: number
  total_value: number
  channels: { channel: string; attributed_value: number; attributed_share: number; attributed_conversions: number }[]
  error?: string
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
) {
  const headers = ['Channel', ...models.map((m) => `${MODEL_LABELS[m] || m} (%)`)]
  const rows = comparisonData.map((row) => [
    row.channel,
    ...models.map((m) => ((row[`${m}_share`] as number) || 0) * 100).map((v) => v.toFixed(1)),
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

  const resultsQuery = useQuery<Record<string, ModelResult>>({
    queryKey: ['attribution-results'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/results')
      if (!res.ok) throw new Error('Failed to fetch results')
      return res.json()
    },
    refetchInterval: 3000,
  })

  const results = resultsQuery.data || {}
  const models = Object.keys(results).filter((k) => !results[k].error)
  const t = tokens

  const allChannels = new Set<string>()
  for (const model of models) {
    const r = results[model]
    if (r?.channels) for (const ch of r.channels) allChannels.add(ch.channel)
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

  const sortedData = useMemo(() => {
    return [...comparisonData].sort((a, b) => {
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
  }, [comparisonData, sortBy, sortDir])

  const selectedResult = results[selectedModel]

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
        <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Attributed Revenue by Model & Channel
        </h3>
        <ResponsiveContainer width="100%" height={360}>
          <BarChart data={comparisonData} margin={{ top: 8, right: 16, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
            <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
            <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => formatCurrency(v)} />
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
                <h4 style={{ margin: 0, fontSize: t.font.sizeBase, fontWeight: t.font.weightBold, color: MODEL_COLORS[model] }}>
                  {MODEL_LABELS[model] || model}
                </h4>
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
                <div style={{ marginBottom: 4 }}>Conversions: <strong style={{ color: t.color.text }}>{r.total_conversions}</strong></div>
                <div style={{ marginBottom: 8 }}>Total value: <strong style={{ color: t.color.text }}>{formatCurrency(r.total_value)}</strong></div>
              </div>
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
          <button
            type="button"
            onClick={() => exportComparisonCSV(comparisonData, models)}
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
                        {share.toFixed(1)}%
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
