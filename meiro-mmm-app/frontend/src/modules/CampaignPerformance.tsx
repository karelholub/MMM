import { useState, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from 'recharts'
import { tokens } from '../theme/tokens'
import ConfidenceBadge, { Confidence } from '../components/ConfidenceBadge'
import ExplainabilityPanel from '../components/ExplainabilityPanel'

interface CampaignPerformanceProps {
  model: string
  modelsReady: boolean
  configId?: string | null
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
  treatment_rate?: number
  holdout_rate?: number
  uplift_abs?: number
  uplift_rel?: number | null
  treatment_n?: number
  holdout_n?: number
  confidence?: Confidence
}

interface CampaignPerformanceResponse {
  model: string
  campaigns: CampaignData[]
  total_conversions: number
  total_value: number
  total_spend?: number
  message?: string
  config?: {
    id: string
    name: string
    version: number
  } | null
}

interface CampaignTrendPoint {
  date: string
  transactions: number
  revenue: number
}

interface CampaignTrendsResponse {
  campaigns: string[]
  dates: string[]
  series: Record<string, CampaignTrendPoint[]>
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

export default function CampaignPerformance({ model, modelsReady, configId }: CampaignPerformanceProps) {
  const [sortKey, setSortKey] = useState<SortKey>('attributed_value')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [search, setSearch] = useState('')
  const [channelFilter, setChannelFilter] = useState<string>('')
  const [campaignTargets, setCampaignTargets] = useState<Record<string, number>>({})
  const [selectedCampaign, setSelectedCampaign] = useState<string | null>(null)
  const [showWhy, setShowWhy] = useState(false)

  const perfQuery = useQuery<CampaignPerformanceResponse>({
    queryKey: ['campaign-performance', model, configId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({ model })
      if (configId) params.append('config_id', configId)
      const res = await fetch(`/api/attribution/campaign-performance?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to fetch campaign performance')
      return res.json()
    },
    enabled: modelsReady,
    refetchInterval: false,
  })

  const data = perfQuery.data
  const loading = perfQuery.isLoading || !modelsReady
  const campaigns = data?.campaigns ?? []

  const trendsQuery = useQuery<CampaignTrendsResponse>({
    queryKey: ['campaign-performance-trends'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/campaign-performance/trends')
      if (!res.ok) throw new Error('Failed to fetch campaign trends')
      return res.json()
    },
    enabled: !!campaigns.length,
    refetchInterval: false,
  })

  const filteredCampaigns = useMemo(() => {
    if (!campaigns.length) return []
    const q = search.trim().toLowerCase()
    const byChannel = channelFilter.trim()
    return campaigns.filter((c) => {
      const matchSearch = !q || c.campaign.toLowerCase().includes(q) || c.channel.toLowerCase().includes(q) || (c.campaign_name ?? '').toLowerCase().includes(q)
      const matchChannel = !byChannel || c.channel === byChannel
      return matchSearch && matchChannel
    })
  }, [campaigns, search, channelFilter])

  const channelsList = useMemo(() => Array.from(new Set(campaigns.map((c) => c.channel))).sort(), [campaigns])

  const sortedCampaigns = useMemo(() => {
    if (!filteredCampaigns.length) return []
    return [...filteredCampaigns].sort((a, b) => {
      const va = a[sortKey]
      const vb = b[sortKey]
      if (va == null && vb == null) return 0
      if (va == null) return sortDir === 'asc' ? 1 : -1
      if (vb == null) return sortDir === 'asc' ? -1 : 1
      const cmp = typeof va === 'number' && typeof vb === 'number' ? va - vb : String(va).localeCompare(String(vb))
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [filteredCampaigns, sortKey, sortDir])

  const setCampaignTarget = useCallback((campaign: string, value: number) => {
    setCampaignTargets((prev) => (value > 0 ? { ...prev, [campaign]: value } : (() => { const next = { ...prev }; delete next[campaign]; return next })()))
  }, [])

  const totalBudgetTarget = useMemo(() => Object.values(campaignTargets).reduce((s, v) => s + v, 0), [campaignTargets])

  const activeCampaignKey = useMemo(() => {
    if (selectedCampaign && filteredCampaigns.some((c) => c.campaign === selectedCampaign)) {
      return selectedCampaign
    }
    return filteredCampaigns[0]?.campaign ?? null
  }, [selectedCampaign, filteredCampaigns])

  const activeTrendSeries: CampaignTrendPoint[] =
    activeCampaignKey && trendsQuery.data?.series?.[activeCampaignKey]
      ? trendsQuery.data.series[activeCampaignKey]
      : []

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
  const filteredTotalSpend = filteredCampaigns.reduce((s, c) => s + c.spend, 0)
  const filteredTotalValue = filteredCampaigns.reduce((s, c) => s + c.attributed_value, 0)

  const kpis = [
    { label: 'Total Spend', value: formatCurrency(totalSpend), def: METRIC_DEFINITIONS['Total Spend'] },
    { label: 'Attributed Revenue', value: formatCurrency(data.total_value), def: METRIC_DEFINITIONS['Attributed Revenue'] },
    { label: 'Conversions', value: data.total_conversions.toLocaleString(), def: '' },
    { label: 'ROAS', value: `${totalROAS.toFixed(2)}×`, def: '' },
    { label: 'Avg CPA', value: formatCurrency(avgCPA), def: '' },
  ]

  const chartData = filteredCampaigns.map((c) => ({
    name: c.campaign_name ? `${c.channel} / ${c.campaign_name}` : c.campaign,
    spend: c.spend,
    attributed_value: c.attributed_value,
  }))

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: t.space.md, flexWrap: 'wrap', gap: t.space.md }}>
        <div>
          <h1 style={{ margin: 0, fontSize: t.font.size2xl, fontWeight: t.font.weightBold, color: t.color.text, letterSpacing: '-0.02em' }}>
            Campaign Performance
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Attribution model: <strong style={{ color: t.color.accent }}>{MODEL_LABELS[model] || model}</strong>. Search and filter below; set budget targets to compare vs actual spend.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowWhy((v) => !v)}
          style={{
            border: 'none',
            backgroundColor: showWhy ? t.color.accentMuted : 'transparent',
            color: t.color.accent,
            padding: `${t.space.xs}px ${t.space.sm}px`,
            borderRadius: t.radius.full,
            fontSize: t.font.sizeXs,
            fontWeight: t.font.weightSemibold,
            cursor: 'pointer',
            alignSelf: 'center',
          }}
        >
          Why?
        </button>
      </div>

      {showWhy && (
        <div style={{ marginBottom: t.space.lg }}>
          <ExplainabilityPanel scope="campaign" configId={configId ?? undefined} />
        </div>
      )}

      {/* Search and filter */}
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: t.space.md,
          alignItems: 'center',
          marginBottom: t.space.xl,
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.lg,
          boxShadow: t.shadowSm,
        }}
      >
        <label style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
          Search
        </label>
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Campaign or channel name..."
          style={{
            minWidth: 200,
            padding: `${t.space.sm}px ${t.space.md}px`,
            fontSize: t.font.sizeSm,
            border: `1px solid ${t.color.border}`,
            borderRadius: t.radius.sm,
            color: t.color.text,
          }}
        />
        <label style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
          Channel
        </label>
        <select
          value={channelFilter}
          onChange={(e) => setChannelFilter(e.target.value)}
          style={{
            padding: `${t.space.sm}px ${t.space.md}px`,
            fontSize: t.font.sizeSm,
            border: `1px solid ${t.color.border}`,
            borderRadius: t.radius.sm,
            color: t.color.text,
            background: t.color.surface,
          }}
        >
          <option value="">All channels</option>
          {channelsList.map((ch) => (
            <option key={ch} value={ch}>{ch}</option>
          ))}
        </select>
        {(search || channelFilter) && (
          <button
            type="button"
            onClick={() => { setSearch(''); setChannelFilter('') }}
            style={{
              padding: `${t.space.sm}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Clear filters
          </button>
        )}
        <span style={{ marginLeft: 'auto', fontSize: t.font.sizeSm, color: t.color.textMuted }}>
          Showing {filteredCampaigns.length} of {campaigns.length} campaigns
        </span>
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
          display: 'grid',
          gridTemplateColumns: '2fr 1.5fr',
          gap: t.space.xl,
          marginBottom: t.space.xl,
        }}
      >
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            padding: t.space.xl,
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
          <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            {activeCampaignKey ? `Trend for ${activeCampaignKey}` : 'Campaign trend'}
          </h3>
          {trendsQuery.isLoading && (
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, margin: 0 }}>Loading trends…</p>
          )}
          {!trendsQuery.isLoading && !activeCampaignKey && (
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, margin: 0 }}>Select a campaign from the table below to see its trend.</p>
          )}
          {!trendsQuery.isLoading && activeCampaignKey && activeTrendSeries.length === 0 && (
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, margin: 0 }}>No trend data for this campaign.</p>
          )}
          {!trendsQuery.isLoading && activeCampaignKey && activeTrendSeries.length > 0 && (
            <ResponsiveContainer width="100%" height={320}>
              <LineChart data={activeTrendSeries} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis dataKey="date" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <YAxis yAxisId="left" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <YAxis yAxisId="right" orientation="right" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <Tooltip
                  contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                  labelFormatter={(label) => `Date: ${label}`}
                  formatter={(value: number, name) =>
                    name === 'transactions'
                      ? [value.toFixed(0), 'Transactions']
                      : [formatCurrency(value), 'Revenue']
                  }
                />
                <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="transactions"
                  name="Transactions"
                  stroke={t.color.chart[0]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="revenue"
                  name="Revenue"
                  stroke={t.color.chart[2]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
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
            Campaign Detail {filteredCampaigns.length < campaigns.length ? `(${filteredCampaigns.length} shown)` : ''}
          </h3>
          <button
            type="button"
            onClick={() => exportCampaignsCSV(filteredCampaigns)}
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
                  { key: 'treatment_rate' as SortKey, label: 'Conv rate (treated)', align: 'right' },
                  { key: 'holdout_rate' as SortKey, label: 'Conv rate (holdout)', align: 'right' },
                  { key: 'uplift_abs' as SortKey, label: 'Uplift', align: 'right' },
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
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'right',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    whiteSpace: 'nowrap',
                  }}
                >
                  Confidence
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedCampaigns.map((c, idx) => (
                <tr
                  key={c.campaign}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor:
                      activeCampaignKey === c.campaign
                        ? t.color.accentMuted
                        : idx % 2 === 0
                        ? t.color.surface
                        : t.color.bg,
                    cursor: 'pointer',
                  }}
                  onClick={() => setSelectedCampaign(c.campaign)}
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
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {c.treatment_rate != null ? `${(c.treatment_rate * 100).toFixed(1)}%` : '—'}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {c.holdout_rate != null ? `${(c.holdout_rate * 100).toFixed(1)}%` : '—'}
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: c.uplift_abs != null && c.uplift_abs > 0 ? t.color.success : c.uplift_abs != null && c.uplift_abs < 0 ? t.color.danger : t.color.textMuted }}>
                    {c.uplift_abs != null ? `${(c.uplift_abs * 100).toFixed(1)}%` : '—'}
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
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right' }}>
                    <ConfidenceBadge confidence={c.confidence} compact />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Campaign-level budget: targets vs actual spend */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          marginTop: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Campaign budget targets
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.lg }}>
          Set optional budget targets per campaign to compare vs actual spend (from channel expenses). Variance = actual − target.
        </p>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Campaign</th>
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Actual spend</th>
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Target budget</th>
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Variance</th>
              </tr>
            </thead>
            <tbody>
              {filteredCampaigns.map((c, idx) => {
                const target = campaignTargets[c.campaign]
                const variance = target != null ? c.spend - target : null
                return (
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
                    <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                      {formatCurrency(c.spend)}
                    </td>
                    <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right' }}>
                      <input
                        type="number"
                        min={0}
                        step={100}
                        placeholder="—"
                        value={target ?? ''}
                        onChange={(e) => {
                          const v = parseFloat(e.target.value)
                          setCampaignTarget(c.campaign, Number.isFinite(v) ? v : 0)
                        }}
                        style={{
                          width: 100,
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          fontSize: t.font.sizeSm,
                          border: `1px solid ${t.color.border}`,
                          borderRadius: t.radius.sm,
                          textAlign: 'right',
                        }}
                      />
                    </td>
                    <td style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'right',
                      fontVariantNumeric: 'tabular-nums',
                      color: variance != null ? (variance > 0 ? t.color.danger : variance < 0 ? t.color.success : t.color.textMuted) : t.color.textMuted,
                    }}>
                      {variance != null ? `${variance >= 0 ? '+' : ''}${formatCurrency(variance)}` : '—'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        {totalBudgetTarget > 0 && (
          <p style={{ margin: `${t.space.md}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Total target budget: {formatCurrency(totalBudgetTarget)} · Total actual (filtered): {formatCurrency(filteredTotalSpend)} · Variance: {formatCurrency(filteredTotalSpend - totalBudgetTarget)}
          </p>
        )}
      </div>
    </div>
  )
}
