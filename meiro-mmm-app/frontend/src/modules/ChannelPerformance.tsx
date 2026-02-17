import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import ConfidenceBadge, { Confidence } from '../components/ConfidenceBadge'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import TrendPanel from '../components/dashboard/TrendPanel'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts'
import { tokens } from '../theme/tokens'
import { apiGetJson, withQuery } from '../lib/apiClient'
import { useWorkspaceContext } from '../components/WorkspaceContext'

interface ChannelPerformanceProps {
  model: string
  channels: string[]
  modelsReady: boolean
  configId?: string | null
}

interface ChannelData {
  channel: string
  spend: number
  attributed_value: number
  attributed_conversions: number
  attributed_share: number
  roi: number
  roas: number
  cpa: number
  confidence?: Confidence
}

interface ExplainabilityDriver {
  metric: string
  delta: number
  current_value: number
  previous_value: number
}

interface ExplainabilitySummaryLite {
  data_health: {
    confidence?: {
      score: number
      label: string
      components?: Record<string, number>
    } | null
    notes: string[]
  }
  drivers: ExplainabilityDriver[]
}

interface ChannelTrendRow {
  ts: string
  channel: string
  value: number | null
}

interface ChannelTrendResponse {
  current_period: { date_from: string; date_to: string; grain: 'daily' | 'weekly' }
  previous_period: { date_from: string; date_to: string }
  series: ChannelTrendRow[]
  series_prev?: ChannelTrendRow[]
}

interface ChannelSummaryItem {
  channel: string
  current: { spend: number; conversions: number; revenue: number }
  previous?: { spend: number; conversions: number; revenue: number } | null
  derived?: { roas?: number | null; cpa?: number | null }
  confidence?: Confidence | null
}

interface ChannelSummaryResponse {
  current_period: { date_from: string; date_to: string; grain?: string }
  previous_period: { date_from: string; date_to: string }
  items: ChannelSummaryItem[]
  totals?: {
    current: { spend: number; conversions: number; revenue: number }
    previous?: { spend: number; conversions: number; revenue: number } | null
  }
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
  } | null
  mapping_coverage?: {
    spend_mapped_pct: number
    value_mapped_pct: number
    spend_mapped: number
    spend_total: number
    value_mapped: number
    value_total: number
  } | null
  meta?: { query_context?: { compare?: boolean } }
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
  'Total Spend': 'Sum of expenses mapped to channels in the selected period.',
  'Attributed Revenue': 'Revenue attributed to each channel by the selected model.',
  'Conversions': 'Attributed conversion count.',
  'ROAS': 'Return on ad spend: attributed revenue ÷ spend.',
  'ROI': 'Return on investment: (attributed value − spend) ÷ spend.',
  'CPA': 'Cost per acquisition: spend ÷ attributed conversions.',
  'Share': 'Share of total attributed revenue.',
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

function exportTableCSV(
  channels: ChannelData[],
  opts: {
    model: string
    periodLabel: string
    conversionKey?: string | null
    configVersion?: number | null
    directMode: 'include' | 'exclude'
  },
) {
  const headers = [
    'Channel',
    'Spend',
    'Attributed Revenue',
    'Conversions',
    'Share %',
    'ROI %',
    'ROAS',
    'CPA',
    'Period label',
    'Conversion key',
    'Model',
    'Config version',
    'Direct handling',
  ]
  const rows = channels.map((ch) => [
    ch.channel,
    ch.spend.toFixed(2),
    ch.attributed_value.toFixed(2),
    ch.attributed_conversions.toFixed(1),
    (ch.attributed_share * 100).toFixed(1),
    (ch.roi * 100).toFixed(1),
    ch.roas.toFixed(2),
    ch.cpa > 0 ? ch.cpa.toFixed(2) : '',
    opts.periodLabel,
    opts.conversionKey || '',
    opts.model,
    opts.configVersion != null ? String(opts.configVersion) : '',
    opts.directMode,
  ])
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `channel-performance-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

type SortKey = keyof ChannelData | 'attributed_share'
type SortDir = 'asc' | 'desc'

export default function ChannelPerformance({ model, modelsReady, configId }: ChannelPerformanceProps) {
  const { globalDateFrom, globalDateTo } = useWorkspaceContext()
  const initialTrendParams = useMemo(() => {
    const params = new URLSearchParams(window.location.search)
    const kpiRaw = (params.get('kpi') || '').toLowerCase()
    const kpi = ['spend', 'conversions', 'revenue', 'cpa', 'roas'].includes(kpiRaw) ? kpiRaw : 'conversions'
    const grainRaw = (params.get('grain') || 'auto').toLowerCase()
    const grain = grainRaw === 'daily' || grainRaw === 'weekly' ? grainRaw : 'auto'
    const compare = params.get('compare') !== '0'
    return { kpi, grain: grain as 'auto' | 'daily' | 'weekly', compare }
  }, [])

  const [sortKey, setSortKey] = useState<SortKey>('attributed_value')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [showWhy, setShowWhy] = useState(false)

  const [directMode, setDirectMode] = useState<'include' | 'exclude'>('include')
  const [comparePrevious, setComparePrevious] = useState(initialTrendParams.compare)
  const [trendKpi, setTrendKpi] = useState(initialTrendParams.kpi)
  const [trendGrain, setTrendGrain] = useState<'auto' | 'daily' | 'weekly'>(initialTrendParams.grain)
  const [chartSortBy, setChartSortBy] = useState<'spend' | 'attributed_value' | 'roas'>('attributed_value')
  const [channelSearch, setChannelSearch] = useState('')
  const [onlyLowConfidence, setOnlyLowConfidence] = useState(false)
  const [selectedTrendChannels, setSelectedTrendChannels] = useState<string[]>([])

  const explainabilityQuery = useQuery<ExplainabilitySummaryLite>({
    queryKey: ['explainability-lite-channel', model, configId ?? 'default'],
    queryFn: async () => {
      const params: Record<string, string> = {
        scope: 'channel',
        model,
      }
      if (configId) params.config_id = configId
      return apiGetJson<ExplainabilitySummaryLite>(withQuery('/api/explainability/summary', params), {
        fallbackMessage: 'Failed to load explainability summary',
      })
    },
    enabled: modelsReady,
    refetchInterval: false,
  })

  const trendChannelsParam = selectedTrendChannels.length ? selectedTrendChannels.join(',') : 'all'
  const trendDateRange = useMemo(() => {
    const fallbackTo = new Date().toISOString().slice(0, 10)
    const fallbackFrom = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)
    const dateFrom = globalDateFrom || fallbackFrom
    const dateTo = globalDateTo || fallbackTo
    return { dateFrom, dateTo, fromJourneys: !!(globalDateFrom && globalDateTo) }
  }, [globalDateFrom, globalDateTo])
  const trendQuery = useQuery<ChannelTrendResponse>({
    queryKey: ['channel-trend-panel', trendKpi, trendGrain, comparePrevious, trendChannelsParam],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
        kpi_key: trendKpi,
        grain: trendGrain,
        compare: comparePrevious ? '1' : '0',
      })
      selectedTrendChannels.forEach((ch) => params.append('channels', ch))
      return apiGetJson<ChannelTrendResponse>(`/api/performance/channel/trend?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel trend',
      })
    },
    enabled: modelsReady && !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const summaryQuery = useQuery<ChannelSummaryResponse>({
    queryKey: ['channel-summary-panel', trendDateRange.dateFrom, trendDateRange.dateTo, comparePrevious, trendChannelsParam],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
        compare: comparePrevious ? '1' : '0',
      })
      selectedTrendChannels.forEach((ch) => params.append('channels', ch))
      return apiGetJson<ChannelSummaryResponse>(`/api/performance/channel/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel summary',
      })
    },
    enabled: modelsReady && !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const loading = !modelsReady || summaryQuery.isLoading

  const channelRows = useMemo(() => {
    const rows = summaryQuery.data?.items ?? []
    if (!rows.length) return []
    const revenueTotal = rows.reduce((sum, row) => sum + (row.current.revenue || 0), 0)
    return rows.map((row) => {
      const spend = row.current.spend || 0
      const revenue = row.current.revenue || 0
      const conversions = row.current.conversions || 0
      const roas = row.derived?.roas != null ? row.derived.roas : spend > 0 ? revenue / spend : 0
      const cpa = row.derived?.cpa != null ? row.derived.cpa : conversions > 0 ? spend / conversions : 0
      const roi = spend > 0 ? (revenue - spend) / spend : 0
      return {
        channel: row.channel,
        spend,
        attributed_value: revenue,
        attributed_conversions: conversions,
        attributed_share: revenueTotal > 0 ? revenue / revenueTotal : 0,
        roi,
        roas,
        cpa,
        confidence: row.confidence || undefined,
      } as ChannelData
    })
  }, [summaryQuery.data?.items])

  const sortedChannels = useMemo(() => {
    if (!channelRows.length) return []
    const q = channelSearch.trim().toLowerCase()
    const base = channelRows.filter((ch) => {
      if (directMode === 'exclude' && ch.channel === 'direct') return false
      if (onlyLowConfidence && (!ch.confidence || ch.confidence.score >= 70)) return false
      if (!q) return true
      return ch.channel.toLowerCase().includes(q)
    })
    const key = sortKey === 'attributed_share' ? 'attributed_share' : sortKey
    return [...base].sort((a, b) => {
      const va = a[key as keyof ChannelData] as number
      const vb = b[key as keyof ChannelData] as number
      const cmp = typeof va === 'number' && typeof vb === 'number' ? va - vb : String(va).localeCompare(String(vb))
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [channelRows, sortKey, sortDir, directMode, channelSearch, onlyLowConfidence])

  const filteredForCharts = useMemo(
    () => channelRows.filter((ch) => (directMode === 'exclude' && ch.channel === 'direct' ? false : true)),
    [channelRows, directMode],
  )

  useEffect(() => {
    if (!filteredForCharts.length) return
    if (selectedTrendChannels.length === 0) {
      setSelectedTrendChannels(filteredForCharts.map((ch) => ch.channel))
    }
  }, [filteredForCharts, selectedTrendChannels.length])

  useEffect(() => {
    if (!filteredForCharts.length) return
    const available = new Set(filteredForCharts.map((ch) => ch.channel))
    setSelectedTrendChannels((prev) => {
      const next = prev.filter((c) => available.has(c))
      return next.length ? next : Array.from(available)
    })
  }, [filteredForCharts])

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    params.set('kpi', trendKpi)
    params.set('grain', trendGrain)
    params.set('compare', comparePrevious ? '1' : '0')
    window.history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`)
  }, [trendKpi, trendGrain, comparePrevious])

  const chartData = useMemo(() => {
    if (!filteredForCharts.length) return []
    const clone = [...filteredForCharts]
    if (chartSortBy === 'spend') clone.sort((a, b) => a.spend - b.spend)
    else if (chartSortBy === 'roas') clone.sort((a, b) => a.roas - b.roas)
    else clone.sort((a, b) => a.attributed_value - b.attributed_value)
    return clone
  }, [filteredForCharts, chartSortBy])

  const trendMetrics = useMemo(() => {
    const rows = trendQuery.data?.series || []
    const prevRows = trendQuery.data?.series_prev || []
    if (!rows.length) return []
    const enabled = new Set(selectedTrendChannels)
    const byTs = new Map<string, number>()
    rows.forEach((r) => {
      if (!enabled.has(r.channel)) return
      if (typeof r.value !== 'number') return
      byTs.set(r.ts, (byTs.get(r.ts) || 0) + r.value)
    })
    const byTsPrev = new Map<string, number>()
    prevRows.forEach((r) => {
      if (!enabled.has(r.channel)) return
      if (typeof r.value !== 'number') return
      byTsPrev.set(r.ts, (byTsPrev.get(r.ts) || 0) + r.value)
    })
    const keys = Array.from(new Set(rows.map((r) => r.ts))).sort()
    const keysPrev = Array.from(new Set(prevRows.map((r) => r.ts))).sort()
    const currentSeries = keys.map((k) => ({ ts: k, value: byTs.has(k) ? byTs.get(k)! : null }))
    const prevSeries = keysPrev.map((k) => ({ ts: k, value: byTsPrev.has(k) ? byTsPrev.get(k)! : null }))
    return [
      {
        key: trendKpi,
        label: trendKpi.toUpperCase(),
        current: currentSeries,
        previous: prevSeries,
        summaryMode: trendKpi === 'cpa' || trendKpi === 'roas' ? ('avg' as const) : ('sum' as const),
        formatValue:
          trendKpi === 'conversions'
            ? (v: number) => v.toFixed(0)
            : trendKpi === 'roas'
            ? (v: number) => `${v.toFixed(2)}x`
            : formatCurrency,
      },
    ]
  }, [trendQuery.data, trendKpi, selectedTrendChannels])
  const availableTrendChannels = useMemo(
    () => filteredForCharts.map((ch) => ch.channel).sort(),
    [filteredForCharts],
  )

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
          Loading attribution data…
        </p>
        <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted, marginTop: t.space.sm }}>
          Models are being computed. This may take a moment.
        </p>
      </div>
    )
  }

  if (summaryQuery.isError && !channelRows.length) {
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
          {(summaryQuery.error as Error)?.message}
        </p>
      </div>
    )
  }

  if (!channelRows.length) {
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
          No performance data
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          Load conversion journeys and map expenses to channels, then run attribution models.
        </p>
      </div>
    )
  }

  const summaryCurrent = summaryQuery.data?.totals?.current ?? { spend: 0, revenue: 0, conversions: 0 }
  const summaryPrevious = summaryQuery.data?.totals?.previous ?? { spend: 0, revenue: 0, conversions: 0 }
  const totalSpend = summaryCurrent.spend
  const totalValue = summaryCurrent.revenue
  const totalConversions = summaryCurrent.conversions

  const totalROAS = totalSpend > 0 ? totalValue / totalSpend : 0
  const avgCPA = totalConversions > 0 ? totalSpend / totalConversions : 0

  const periodLabel =
    globalDateFrom && globalDateTo
      ? `${globalDateFrom} – ${globalDateTo}`
      : 'current dataset'

  const conversionLabel =
    summaryQuery.data?.config?.conversion_key ||
    'All conversions'

  const exp = explainabilityQuery.data
  const revDriver = exp?.drivers?.find((d) => d.metric === 'attributed_value')
  const revDelta = revDriver?.delta ?? null
  const revPrev = revDriver?.previous_value ?? null
  const revPct = revDelta != null && revPrev && Math.abs(revPrev) > 1e-9 ? (revDelta / revPrev) * 100 : null

  const kpiDeltas = comparePrevious
    ? {
        totalSpend: totalSpend - summaryPrevious.spend,
        totalSpendPct:
          Math.abs(summaryPrevious.spend) > 1e-9
            ? ((totalSpend - summaryPrevious.spend) / summaryPrevious.spend) * 100
            : null as number | null,
        totalValue: totalValue - summaryPrevious.revenue,
        totalValuePct:
          Math.abs(summaryPrevious.revenue) > 1e-9
            ? ((totalValue - summaryPrevious.revenue) / summaryPrevious.revenue) * 100
            : revPct,
        totalConversions: totalConversions - summaryPrevious.conversions,
        totalConversionsPct:
          Math.abs(summaryPrevious.conversions) > 1e-9
            ? ((totalConversions - summaryPrevious.conversions) / summaryPrevious.conversions) * 100
            : null as number | null,
        totalROAS: null as number | null,
        totalROASPct: null as number | null,
        avgCPA: null as number | null,
        avgCPAPct: null as number | null,
      }
    : null
  const kpis = [
    { label: 'Total Spend', value: formatCurrency(totalSpend), def: METRIC_DEFINITIONS['Total Spend'] },
    { label: 'Attributed Revenue', value: formatCurrency(totalValue), def: METRIC_DEFINITIONS['Attributed Revenue'] },
    { label: 'Conversions', value: totalConversions.toLocaleString(), def: METRIC_DEFINITIONS['Conversions'] },
    { label: 'ROAS', value: `${totalROAS.toFixed(2)}×`, def: METRIC_DEFINITIONS['ROAS'] },
    { label: 'Avg CPA', value: formatCurrency(avgCPA), def: METRIC_DEFINITIONS['CPA'] },
  ]

  const tableColumns: { key: SortKey; label: string; align: 'left' | 'right'; format: (ch: ChannelData) => string }[] = [
    { key: 'channel', label: 'Channel', align: 'left', format: (ch) => ch.channel },
    { key: 'spend', label: 'Spend', align: 'right', format: (ch) => formatCurrency(ch.spend) },
    { key: 'attributed_value', label: 'Attributed Revenue', align: 'right', format: (ch) => formatCurrency(ch.attributed_value) },
    { key: 'attributed_conversions', label: 'Conversions', align: 'right', format: (ch) => ch.attributed_conversions.toFixed(1) },
    { key: 'attributed_share', label: 'Share', align: 'right', format: (ch) => `${(ch.attributed_share * 100).toFixed(1)}%` },
    { key: 'roi', label: 'ROI', align: 'right', format: (ch) => `${(ch.roi * 100).toFixed(0)}%` },
    { key: 'roas', label: 'ROAS', align: 'right', format: (ch) => `${ch.roas.toFixed(2)}×` },
    { key: 'cpa', label: 'CPA', align: 'right', format: (ch) => (ch.cpa > 0 ? formatCurrency(ch.cpa) : '—') },
  ]

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.xl,
          flexWrap: 'wrap',
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
            Channel Performance
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Attribution model: <strong style={{ color: t.color.accent }}>{MODEL_LABELS[model] || model}</strong>
          </p>
        </div>
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: t.space.sm,
            alignItems: 'center',
            justifyContent: 'flex-end',
          }}
        >
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            <strong>Period:</strong> {periodLabel}
          </div>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            <strong>Conversion:</strong> {conversionLabel} (read‑only)
          </div>
          {summaryQuery.data?.config?.time_window && (
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              <strong>Config:</strong>{' '}
              {[
                summaryQuery.data.config.time_window.click_lookback_days != null
                  ? `Click ${summaryQuery.data.config.time_window.click_lookback_days}d`
                  : null,
                summaryQuery.data.config.time_window.impression_lookback_days != null
                  ? `Impr. ${summaryQuery.data.config.time_window.impression_lookback_days}d`
                  : null,
                summaryQuery.data.config.time_window.session_timeout_minutes != null
                  ? `Session ${summaryQuery.data.config.time_window.session_timeout_minutes}m`
                  : null,
              ]
                .filter(Boolean)
                .join(' · ')}
            </div>
          )}
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs }}>
            <span style={{ color: t.color.textSecondary }}>Direct handling:</span>
            <button
              type="button"
              onClick={() => setDirectMode((m) => (m === 'include' ? 'exclude' : 'include'))}
              style={{
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.full,
                padding: '2px 8px',
                fontSize: t.font.sizeXs,
                backgroundColor: t.color.bg,
                cursor: 'pointer',
              }}
              title="View filter only; underlying attribution is unchanged."
            >
              View filter: {directMode === 'include' ? 'Include Direct' : 'Exclude Direct'}
            </button>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer', color: t.color.textSecondary }}>
              <input
                type="checkbox"
                checked={comparePrevious}
                onChange={(e) => setComparePrevious(e.target.checked)}
                style={{ margin: 0 }}
              />
              Compare to previous period (KPI deltas)
            </label>
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
      </div>

      {summaryQuery.isError && (
        <div
          style={{
            marginBottom: t.space.md,
            padding: `${t.space.sm}px ${t.space.md}px`,
            borderRadius: t.radius.sm,
            border: `1px solid ${t.color.danger}`,
            background: t.color.dangerSubtle,
            color: t.color.danger,
            fontSize: t.font.sizeXs,
          }}
        >
          Unified channel summary is currently unavailable. KPI totals and deltas depend on `/api/performance/channel/summary`.
        </div>
      )}

      {showWhy && (
        <div style={{ marginBottom: t.space.lg }}>
          <ExplainabilityPanel scope="channel" configId={configId ?? undefined} model={model} />
        </div>
      )}

      <div style={{ marginBottom: t.space.xl }}>
        <div
          style={{
            marginBottom: t.space.sm,
            display: 'grid',
            gap: t.space.sm,
          }}
        >
          <div style={{ display: 'grid', gap: t.space.xs }}>
            <label style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Channels</label>
            <div
              style={{
                display: 'flex',
                gap: t.space.xs,
                alignItems: 'center',
                overflowX: 'auto',
                flexWrap: 'nowrap',
                whiteSpace: 'nowrap',
                paddingBottom: 2,
              }}
            >
              <button
                type="button"
                onClick={() => setSelectedTrendChannels(availableTrendChannels)}
                style={{
                  border: `1px solid ${selectedTrendChannels.length === availableTrendChannels.length ? t.color.accent : t.color.borderLight}`,
                  background: selectedTrendChannels.length === availableTrendChannels.length ? t.color.accentMuted : t.color.surface,
                  color: selectedTrendChannels.length === availableTrendChannels.length ? t.color.accent : t.color.textSecondary,
                  borderRadius: t.radius.full,
                  padding: '6px 10px',
                  cursor: 'pointer',
                  fontSize: t.font.sizeSm,
                  fontWeight: selectedTrendChannels.length === availableTrendChannels.length ? t.font.weightSemibold : t.font.weightMedium,
                  flex: '0 0 auto',
                }}
              >
                All
              </button>
              {availableTrendChannels.map((ch) => {
                const isActive = selectedTrendChannels.includes(ch)
                return (
                  <button
                    key={ch}
                    type="button"
                    onClick={() => {
                      const next = isActive
                        ? selectedTrendChannels.filter((value) => value !== ch)
                        : [...selectedTrendChannels, ch]
                      setSelectedTrendChannels(next.length ? next : availableTrendChannels)
                    }}
                    style={{
                      border: `1px solid ${isActive ? t.color.accent : t.color.borderLight}`,
                      background: isActive ? t.color.accentMuted : t.color.surface,
                      color: isActive ? t.color.accent : t.color.textSecondary,
                      borderRadius: t.radius.full,
                      padding: '6px 10px',
                      cursor: 'pointer',
                      fontSize: t.font.sizeSm,
                      fontWeight: isActive ? t.font.weightSemibold : t.font.weightMedium,
                      flex: '0 0 auto',
                    }}
                  >
                    {ch}
                  </button>
                )
              })}
            </div>
          </div>
        </div>
        <TrendPanel
          title="Trend"
          subtitle="Daily trend for selected period"
          metrics={trendMetrics}
          metricKey={trendKpi}
          onMetricKeyChange={(key) => setTrendKpi(key)}
          grain={trendGrain}
          onGrainChange={setTrendGrain}
          compare={comparePrevious}
          onCompareChange={setComparePrevious}
          showMetricSelector
          showGrainSelector
          showCompareToggle
          showTableToggle
          baselineLabel={`vs previous ${trendQuery.data?.current_period ? ((new Date(trendQuery.data.current_period.date_to).getTime() - new Date(trendQuery.data.current_period.date_from).getTime()) / (24 * 60 * 60 * 1000) + 1).toFixed(0) : '0'} days`}
          infoTooltip="Observed values for selected channels and KPI. Toggle channels above to verify composition."
          noDataMessage="No data for selected filters and date range"
          footerNote={
            trendDateRange.fromJourneys
              ? undefined
              : 'Using default last 30 days because journey date range is not available.'
          }
        />
      </div>

      {/* Metrics strip */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
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
            <div
              style={{
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                color: t.color.textMuted,
                textTransform: 'uppercase',
                letterSpacing: '0.04em',
                display: 'flex',
                alignItems: 'center',
                gap: 4,
              }}
              title={kpi.def}
            >
              {kpi.label}
              <span style={{ opacity: 0.7, cursor: 'help' }} aria-label="Definition">ⓘ</span>
            </div>
            <div
              style={{
                fontSize: t.font.sizeXl,
                fontWeight: t.font.weightBold,
                color: t.color.text,
                marginTop: t.space.xs,
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {kpi.value}
            </div>
            {comparePrevious && (
              <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary, fontVariantNumeric: 'tabular-nums' }}>
                {(() => {
                  if (!kpiDeltas) return null
                  if (kpi.label === 'Total Spend') {
                    return 'Δ N/A'
                  }
                  if (kpi.label === 'Attributed Revenue') {
                    if (kpiDeltas.totalValue == null) return 'Δ N/A'
                    const abs = kpiDeltas.totalValue
                    const pct = kpiDeltas.totalValuePct
                    const absLabel = formatCurrency(Math.abs(abs))
                    return `${abs >= 0 ? '+' : '-'}${absLabel} ${pct != null ? `/ ${pct.toFixed(1)}%` : ''}`
                  }
                  return 'Δ N/A'
                })()}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Data health mini indicator */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: t.space.lg,
          gap: t.space.sm,
          flexWrap: 'wrap',
          fontSize: t.font.sizeXs,
        }}
      >
        <div style={{ color: t.color.textSecondary }}>Measurement context reflects the currently loaded journeys and active attribution config.</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {exp?.data_health?.confidence ? (
            <span title={exp.data_health.notes?.slice(0, 2).join('\n') || 'Open Why? for full details.'}>
              <ConfidenceBadge confidence={exp.data_health.confidence} compact />
            </span>
          ) : (
            <span style={{ color: t.color.textMuted }}>Data health: N/A</span>
          )}
          <button
            type="button"
            onClick={() => setShowWhy(true)}
            style={{
              border: 'none',
              background: 'transparent',
              color: t.color.accent,
              cursor: 'pointer',
              padding: 0,
            }}
          >
            See Why?
          </button>
        </div>
      </div>

      {/* Charts */}
      <div
        className="cp-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1.6fr 1fr',
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
          <h3
            style={{
              margin: `0 0 ${t.space.lg}px`,
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            Spend vs. Attributed Revenue by Channel
          </h3>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: t.space.sm,
              fontSize: t.font.sizeXs,
              color: t.color.textSecondary,
            }}
          >
            <div>
              Sorted by:{' '}
              <select
                value={chartSortBy}
                onChange={(e) => setChartSortBy(e.target.value as 'spend' | 'attributed_value' | 'roas')}
                style={{
                  fontSize: t.font.sizeXs,
                  padding: '2px 6px',
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  backgroundColor: t.color.surface,
                  color: t.color.text,
                }}
              >
                <option value="spend">Spend</option>
                <option value="attributed_value">Attributed Revenue</option>
                <option value="roas">ROAS</option>
              </select>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis type="number" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => formatCurrency(v)} />
              <YAxis type="category" dataKey="channel" width={100} tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
              <Tooltip
                formatter={(value: number) => formatCurrency(value)}
                contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
              />
              <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
              <Bar dataKey="spend" fill={t.color.danger} name="Spend" radius={[0, 4, 4, 0]} />
              <Bar dataKey="attributed_value" fill={t.color.success} name="Attributed Revenue" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
          {directMode === 'include' && filteredForCharts.some((ch) => ch.channel === 'direct') && (
            <p style={{ marginTop: t.space.sm, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              Direct can represent true direct or unattributed/unknown referrer. See Why? for details.
            </p>
          )}
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
          <h3
            style={{
              margin: `0 0 ${t.space.lg}px`,
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            Revenue Attribution Share
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={filteredForCharts}
                dataKey="attributed_share"
                nameKey="channel"
                cx="50%"
                cy="50%"
                innerRadius={56}
                outerRadius={90}
                paddingAngle={1}
                label={({ channel, attributed_share }) => `${channel} ${(attributed_share * 100).toFixed(0)}%`}
              >
                {filteredForCharts.map((_, i) => (
                  <Cell key={`cell-${i}`} fill={t.color.chart[i % t.color.chart.length]} />
                ))}
              </Pie>
              <Tooltip formatter={(value: number) => `${(value * 100).toFixed(1)}%`} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ROI / ROAS chart */}
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
          ROI & ROAS by Channel
        </h3>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={filteredForCharts} margin={{ top: 8, right: 16, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
            <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
            <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
            <Tooltip formatter={(value: number) => value.toFixed(2)} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
            <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
            <Bar dataKey="roas" fill={t.color.chart[3]} name="ROAS" radius={[4, 4, 0, 0]} />
            <Bar dataKey="roi" fill={t.color.chart[4]} name="ROI" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Sortable detail table + export */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: t.space.lg,
            flexWrap: 'wrap',
            gap: t.space.md,
          }}
        >
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Channel Detail
          </h3>
          <button
            type="button"
            onClick={() =>
              exportTableCSV(filteredForCharts, {
                model,
                periodLabel,
                conversionKey: summaryQuery.data?.config?.conversion_key ?? null,
                configVersion: summaryQuery.data?.config?.config_version ?? null,
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
        <style>{`
          .cp-table tbody tr:hover { background: ${t.color.accentMuted} !important; }
          @media (max-width: 900px) { .cp-charts { grid-template-columns: 1fr !important; } }
        `}</style>
        <div style={{ overflowX: 'auto' }}>
          <table className="cp-table" style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                <th
                  colSpan={3}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    textAlign: 'left',
                    fontWeight: t.font.weightNormal,
                  }}
                >
                  <input
                    type="text"
                    value={channelSearch}
                    onChange={(e) => setChannelSearch(e.target.value)}
                    placeholder="Search channels…"
                    style={{
                      width: '100%',
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.borderLight}`,
                      fontSize: t.font.sizeXs,
                    }}
                  />
                </th>
                <th
                  colSpan={5}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    textAlign: 'right',
                    fontWeight: t.font.weightNormal,
                    color: t.color.textSecondary,
                  }}
                >
                  <label style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={onlyLowConfidence}
                      onChange={(e) => setOnlyLowConfidence(e.target.checked)}
                      style={{ margin: 0 }}
                    />
                    Show only low confidence (Conf. &lt; 70)
                  </label>
                </th>
                <th />
              </tr>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                {tableColumns.map((col) => (
                  <th
                    key={col.key}
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: col.align,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      whiteSpace: 'nowrap',
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => handleSort(col.key)}
                    title={`Sort by ${col.label}`}
                  >
                    {col.label}
                    {sortKey === col.key && (sortDir === 'asc' ? ' ↑' : ' ↓')}
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
              {sortedChannels.map((ch, idx) => (
                <tr
                  key={ch.channel}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                  }}
                >
                  {tableColumns.map((col) => {
                    const isChannel = col.key === 'channel'
                    const isRoi = col.key === 'roi'
                    const isValue = col.key === 'attributed_value'
                    return (
                      <td
                        key={col.key}
                        style={{
                          padding: `${t.space.md}px ${t.space.lg}px`,
                          textAlign: col.align,
                          fontWeight: isChannel || isValue || isRoi ? t.font.weightMedium : t.font.weightNormal,
                          color:
                            col.key === 'cpa' && ch.spend === 0
                              ? t.color.textSecondary
                              : isValue
                              ? t.color.success
                              : isRoi
                              ? ch.roi >= 0
                                ? t.color.success
                                : t.color.danger
                              : t.color.text,
                          fontVariantNumeric: col.align === 'right' ? 'tabular-nums' : undefined,
                        }}
                      >
                        {(() => {
                          if (col.key === 'roas' && ch.spend === 0) return 'N/A'
                          if (col.key === 'cpa' && ch.spend === 0) return 'N/A'
                          return col.format(ch)
                        })()}
                      </td>
                    )
                  })}
                  <td
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'right',
                    }}
                  >
                    <ConfidenceBadge confidence={ch.confidence} compact />
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
