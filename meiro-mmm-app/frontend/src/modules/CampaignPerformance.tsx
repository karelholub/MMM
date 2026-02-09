import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'

interface CampaignPerformanceProps {
  model: string
  modelsReady: boolean
}

interface SuggestedNext {
  channel: string
  campaign?: string
  conversion_rate: number
  count: number
  avg_value: number
}

interface CampaignData {
  campaign: string
  channel: string
  campaign_name: string | null
  attributed_value: number
  attributed_share: number
  attributed_conversions: number
  spend: number
  roi: number | null
  roas: number | null
  cpa: number | null
  suggested_next: SuggestedNext | null
}

interface CampaignPerformanceResponse {
  model: string
  campaigns: CampaignData[]
  total_conversions: number
  total_value: number
  total_spend?: number
  message?: string
}

const MODEL_LABELS: Record<string, string> = {
  last_touch: 'Last Touch',
  first_touch: 'First Touch',
  linear: 'Linear',
  time_decay: 'Time Decay',
  position_based: 'Position Based',
  markov: 'Data-Driven (Markov)',
}

const METRIC_DEFINITIONS: Record<string, string> = {
  'Total Spend': 'Sum of expenses by channel (campaigns inherit channel spend).',
  'Attributed Revenue': 'Revenue attributed to each campaign by the selected model.',
  'Conversions': 'Attributed conversion count.',
  'Suggested next': 'Next Best Action: recommended next channel/campaign after this one.',
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

function exportCampaignsCSV(campaigns: CampaignData[]) {
  const headers = ['Campaign', 'Channel', 'Attributed Revenue', 'Share %', 'Conversions', 'Spend', 'ROI %', 'ROAS', 'CPA', 'Suggested next']
  const rows = campaigns.map((c) => [
    c.campaign,
    c.channel,
    c.attributed_value.toFixed(2),
    (c.attributed_share * 100).toFixed(1),
    c.attributed_conversions.toFixed(1),
    c.spend.toFixed(2),
    c.roi != null ? (c.roi * 100).toFixed(0) : '',
    c.roas != null ? c.roas.toFixed(2) : '',
    c.cpa != null ? c.cpa.toFixed(2) : '',
    c.suggested_next ? (c.suggested_next.campaign != null ? `${c.suggested_next.channel}/${c.suggested_next.campaign}` : c.suggested_next.channel) : '',
  ])
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `campaign-performance-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

type SortKey = keyof CampaignData
type SortDir = 'asc' | 'desc'

export default function CampaignPerformance({ model, modelsReady }: CampaignPerformanceProps) {
  const [sortKey, setSortKey] = useState<SortKey>('attributed_value')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const perfQuery = useQuery<CampaignPerformanceResponse>({
    queryKey: ['campaign-performance', model],
    queryFn: async () => {
      const res = await fetch(`/api/attribution/campaign-performance?model=${model}`)
      if (!res.ok) throw new Error('Failed to fetch campaign performance')
      return res.json()
    },
    enabled: modelsReady,
    refetchInterval: false,
  })

  const data = perfQuery.data
  const loading = perfQuery.isLoading || !modelsReady
  const campaigns = data?.campaigns ?? []

  const sortedCampaigns = useMemo(() => {
    if (!campaigns.length) return []
    return [...campaigns].sort((a, b) => {
      const va = a[sortKey]
      const vb = b[sortKey]
      if (va == null && vb == null) return 0
      if (va == null) return sortDir === 'asc' ? 1 : -1
      if (vb == null) return sortDir === 'asc' ? -1 : 1
      const cmp = typeof va === 'number' && typeof vb === 'number' ? va - vb : String(va).localeCompare(String(vb))
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [campaigns, sortKey, sortDir])

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const t = tokens

  if (loading) {
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
          Loading campaign performance…
        </p>
      </div>
    )
  }

  if (perfQuery.isError) {
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
          Failed to load
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          {(perfQuery.error as Error)?.message}
        </p>
      </div>
    )
  }

  if (!data || !campaigns.length) {
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
          No campaign data
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          {data?.message || 'Load conversion journeys with a "campaign" field on touchpoints (e.g. load sample data), then run attribution models.'}
        </p>
      </div>
    )
  }

  const totalSpend = data.total_spend ?? campaigns.reduce((s, c) => s + c.spend, 0)
  const totalROAS = totalSpend > 0 ? data.total_value / totalSpend : 0
  const avgCPA = data.total_conversions > 0 ? totalSpend / data.total_conversions : 0

  const kpis = [
    { label: 'Total Spend', value: formatCurrency(totalSpend), def: METRIC_DEFINITIONS['Total Spend'] },
    { label: 'Attributed Revenue', value: formatCurrency(data.total_value), def: METRIC_DEFINITIONS['Attributed Revenue'] },
    { label: 'Conversions', value: data.total_conversions.toLocaleString(), def: '' },
    { label: 'ROAS', value: `${totalROAS.toFixed(2)}×`, def: '' },
    { label: 'Avg CPA', value: formatCurrency(avgCPA), def: '' },
  ]

  const chartData = campaigns.map((c) => ({
    name: c.campaign_name ? `${c.channel} / ${c.campaign_name}` : c.campaign,
    spend: c.spend,
    attributed_value: c.attributed_value,
  }))

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: t.space.xl, flexWrap: 'wrap', gap: t.space.md }}>
        <div>
          <h1 style={{ margin: 0, fontSize: t.font.size2xl, fontWeight: t.font.weightBold, color: t.color.text, letterSpacing: '-0.02em' }}>
            Campaign Performance
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Attribution model: <strong style={{ color: t.color.accent }}>{MODEL_LABELS[model] || model}</strong>. Suggested next from Next Best Action (NBA).
          </p>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: t.space.md, marginBottom: t.space.xl }}>
        {kpis.map((kpi) => (
          <div
            key={kpi.label}
            style={{
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              padding: `${t.space.lg}px ${t.space.xl}px`,
              boxShadow: t.shadowSm,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }} title={kpi.def}>
              {kpi.label}
            </div>
            <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
              {kpi.value}
            </div>
          </div>
        ))}
      </div>

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
          Spend vs. Attributed Revenue by Campaign
        </h3>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 16 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
            <XAxis type="number" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => formatCurrency(v)} />
            <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
            <Tooltip formatter={(value: number) => formatCurrency(value)} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
            <Bar dataKey="spend" fill={t.color.danger} name="Spend" radius={[0, 4, 4, 0]} />
            <Bar dataKey="attributed_value" fill={t.color.success} name="Attributed Revenue" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

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
            Campaign Detail
          </h3>
          <button
            type="button"
            onClick={() => exportCampaignsCSV(campaigns)}
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
                {[
                  { key: 'campaign' as SortKey, label: 'Campaign', align: 'left' },
                  { key: 'channel' as SortKey, label: 'Channel', align: 'left' },
                  { key: 'attributed_value' as SortKey, label: 'Attributed Revenue', align: 'right' },
                  { key: 'attributed_share' as SortKey, label: 'Share', align: 'right' },
                  { key: 'attributed_conversions' as SortKey, label: 'Conversions', align: 'right' },
                  { key: 'spend' as SortKey, label: 'Spend', align: 'right' },
                  { key: 'roi' as SortKey, label: 'ROI', align: 'right' },
                  { key: 'roas' as SortKey, label: 'ROAS', align: 'right' },
                  { key: 'cpa' as SortKey, label: 'CPA', align: 'right' },
                  { key: 'suggested_next' as SortKey, label: 'Suggested next', align: 'left' },
                ].map((col) => (
                  <th
                    key={col.key}
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: col.align as 'left' | 'right',
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      whiteSpace: 'nowrap',
                      cursor: col.key !== 'suggested_next' ? 'pointer' : 'default',
                      userSelect: 'none',
                    }}
                    onClick={col.key !== 'suggested_next' ? () => handleSort(col.key) : undefined}
                    title={col.key === 'suggested_next' ? METRIC_DEFINITIONS['Suggested next'] : `Sort by ${col.label}`}
                  >
                    {col.label}
                    {sortKey === col.key && col.key !== 'suggested_next' && (sortDir === 'asc' ? ' ↑' : ' ↓')}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedCampaigns.map((c, idx) => (
                <tr
                  key={c.campaign}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                  }}
                >
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, fontWeight: t.font.weightMedium, color: t.color.text }}>
                    {c.campaign_name ? `${c.channel} / ${c.campaign_name}` : c.campaign}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, color: t.color.textSecondary }}>{c.channel}</td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightMedium, color: t.color.success, fontVariantNumeric: 'tabular-nums' }}>
                    {formatCurrency(c.attributed_value)}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {(c.attributed_share * 100).toFixed(1)}%
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {c.attributed_conversions.toFixed(1)}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {formatCurrency(c.spend)}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: c.roi != null && c.roi >= 0 ? t.color.success : c.roi != null ? t.color.danger : t.color.textMuted }}>
                    {c.roi != null ? `${(c.roi * 100).toFixed(0)}%` : '—'}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {c.roas != null ? `${c.roas.toFixed(2)}×` : '—'}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {c.cpa != null ? formatCurrency(c.cpa) : '—'}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px` }}>
                    {c.suggested_next ? (
                      <span
                        title={`${c.suggested_next.count} journeys, ${(c.suggested_next.conversion_rate * 100).toFixed(1)}% conversion, avg $${c.suggested_next.avg_value}`}
                        style={{
                          display: 'inline-block',
                          padding: '2px 8px',
                          backgroundColor: t.color.accentMuted,
                          color: t.color.accent,
                          borderRadius: t.radius.sm,
                          fontSize: t.font.sizeXs,
                          fontWeight: t.font.weightSemibold,
                        }}
                      >
                        {c.suggested_next.campaign != null ? `${c.suggested_next.channel} / ${c.suggested_next.campaign}` : c.suggested_next.channel}
                        {' '}
                        ({(c.suggested_next.conversion_rate * 100).toFixed(0)}%)
                      </span>
                    ) : (
                      <span style={{ color: t.color.textMuted, fontSize: t.font.sizeXs }}>—</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
