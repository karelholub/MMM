import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import ConfidenceBadge, { Confidence } from '../components/ConfidenceBadge'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import SurfaceBasisNotice from '../components/dashboard/SurfaceBasisNotice'
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
import { buildJourneyHypothesisHref } from '../lib/journeyLinks'
import { buildIncrementalityPlannerHref } from '../lib/experimentLinks'
import { buildSettingsHref } from '../lib/settingsLinks'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import DecisionStatusCard from '../components/DecisionStatusCard'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import { AnalyticsTable, AnalyticsToolbar, ContextSummaryStrip, type AnalyticsTableColumn, SectionCard } from '../components/dashboard'
import AnalysisNarrativePanel from '../components/dashboard/AnalysisNarrativePanel'
import SegmentComparisonContextNote from '../components/segments/SegmentComparisonContextNote'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import LagInsightsPanel, { type LagInsightsResponse } from '../components/performance/LagInsightsPanel'
import MeiroTargetInstanceBadge from '../features/meiro/MeiroTargetInstanceBadge'
import MeiroMeasurementScopeNotice from '../features/meiro/MeiroMeasurementScopeNotice'
import { getMeiroConfig, type MeiroConfig } from '../connectors/meiroConnector'
import {
  isLocalAnalyticalSegment,
  localSegmentCompatibleWithDimensions,
  readLocalSegmentDefinition,
  segmentOptionLabel,
  type SegmentRegistryResponse,
} from '../lib/segments'

function hasBaselineValue(value: number | null | undefined): boolean {
  return value != null && Number.isFinite(value) && Math.abs(value) > 1e-9
}

interface ChannelPerformanceProps {
  model: string
  channels: string[]
  modelsReady: boolean
  configId?: string | null
}

interface SuggestedNext {
  channel: string
  campaign?: string
  conversion_rate: number
  count: number
  avg_value: number
  is_promoted_policy?: boolean
  promoted_policy_title?: string | null
  promoted_policy_hypothesis_id?: string | null
  promoted_policy_journey_definition_id?: string | null
}

interface ChannelData {
  channel: string
  spend: number
  visits: number
  attributed_value: number
  attributed_conversions: number
  attributed_share: number
  cvr: number
  cost_per_visit: number
  revenue_per_visit: number
  first_touch_conversions: number
  assist_conversions: number
  last_touch_conversions: number
  first_touch_revenue: number
  assist_revenue: number
  last_touch_revenue: number
  touch_journeys: number
  content_journeys: number
  checkout_journeys: number
  converted_journeys: number
  funnel_conversion_rate: number
  roi: number
  roas: number
  cpa: number
  suggested_next: SuggestedNext | null
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
  meta?: {
    conversion_key?: string | null
    site_scope?: SiteScopeMeta
    conversion_key_resolution?: {
      configured_conversion_key?: string | null
      applied_conversion_key?: string | null
      reason?: string
    } | null
  } | null
}

interface ChannelSummaryItem {
  channel: string
  current: { spend: number; visits: number; conversions: number; revenue: number }
  previous?: { spend: number; visits: number; conversions: number; revenue: number } | null
  derived?: {
    roas?: number | null
    cpa?: number | null
    cvr?: number | null
    cost_per_visit?: number | null
    revenue_per_visit?: number | null
  }
  previous_derived?: {
    cvr?: number | null
    cost_per_visit?: number | null
    revenue_per_visit?: number | null
  } | null
  diagnostics?: {
    roles?: {
      first_touch_conversions?: number
      last_touch_conversions?: number
      assist_conversions?: number
      first_touch_revenue?: number
      last_touch_revenue?: number
      assist_revenue?: number
    }
    funnel?: {
      touch_journeys?: number
      content_journeys?: number
      checkout_journeys?: number
      converted_journeys?: number
      conversion_rate?: number
    }
  }
  confidence?: Confidence | null
  outcomes?: {
    current?: Record<string, number>
    previous?: Record<string, number> | null
  }
}

interface SiteScopeMeta {
  strict?: boolean
  target_sites?: string[]
  journeys_total?: number
  journeys_kept?: number
  journeys_excluded?: number
  out_of_scope_hosts?: Array<{ host: string; count: number }>
}

interface MeiroMeasurementScopeMeta {
  strict?: boolean
  target_sites?: string[]
  source_scope?: { status?: string; target_host?: string; legacy_unverified_entries?: number; out_of_scope_entries?: number }
  event_archive_site_scope?: { target_site_events?: number; out_of_scope_site_events?: number; unknown_site_events?: number }
  out_of_scope_campaign_labels?: number
  campaign_rows_excluded?: number
  warnings?: string[]
}

interface ChannelSummaryResponse {
  current_period: { date_from: string; date_to: string; grain?: string }
  previous_period: { date_from: string; date_to: string }
  items: ChannelSummaryItem[]
  totals?: {
    current: { spend: number; visits: number; conversions: number; revenue: number }
    previous?: { spend: number; visits: number; conversions: number; revenue: number } | null
    outcomes_current?: Record<string, number>
    outcomes_previous?: Record<string, number> | null
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
  spend_quality?: {
    status?: string
    measured_spend?: number
    allocated_spend?: number
    allocated_share?: number
  } | null
  readiness?: {
    status: string
    blockers: string[]
    warnings: string[]
    details?: {
      latest_event_replay?: {
        diagnostics?: {
          events_loaded?: number
          profiles_reconstructed?: number
          touchpoints_reconstructed?: number
          conversions_reconstructed?: number
          attributable_profiles?: number
          journeys_persisted?: number
          warnings?: string[]
        }
      } | null
    }
  } | null
  consistency_warnings?: string[]
  meta?: {
    conversion_key?: string | null
    site_scope?: SiteScopeMeta
    meiro_measurement_scope?: MeiroMeasurementScopeMeta
    conversion_key_resolution?: {
      configured_conversion_key?: string | null
      applied_conversion_key?: string | null
      reason?: string
    } | null
    query_context?: { compare?: boolean }
  }
  notes?: string[]
}

interface ChannelSuggestionResponse {
  items: Record<string, SuggestedNext>
  level: string
  eligible_journeys: number
  reason?: string
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
  'Visits': 'Normalized touchpoint count observed for each channel in the selected period.',
  'Attributed Revenue': 'Revenue attributed to each channel by the selected model.',
  'Conversions': 'Attributed conversion count.',
  'CVR': 'Attributed conversions divided by observed visits.',
  'Cost / Visit': 'Spend divided by visits.',
  'Revenue / Visit': 'Attributed revenue divided by visits.',
  'ROAS': 'Return on ad spend: attributed revenue ÷ spend.',
  'ROI': 'Return on investment: (attributed value − spend) ÷ spend.',
  'CPA': 'Cost per acquisition: spend ÷ attributed conversions.',
  'Share': 'Share of total attributed revenue.',
  'Suggested next': 'Most likely next channel for journeys that currently end on this channel, filtered by NBA defaults.',
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

function formatPercent(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`
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
    'Visits',
    'CVR',
    'Cost / Visit',
    'Revenue / Visit',
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
    ch.visits.toFixed(0),
    (ch.cvr * 100).toFixed(2),
    ch.cost_per_visit.toFixed(4),
    ch.revenue_per_visit.toFixed(4),
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
  const { globalDateFrom, globalDateTo, journeysSummary } = useWorkspaceContext()
  const initialTrendParams = useMemo(() => {
    const params = new URLSearchParams(window.location.search)
    const kpiRaw = (params.get('kpi') || '').toLowerCase()
    const kpi = ['spend', 'visits', 'conversions', 'revenue', 'cpa', 'roas'].includes(kpiRaw) ? kpiRaw : 'conversions'
    const grainRaw = (params.get('grain') || 'auto').toLowerCase()
    const grain = grainRaw === 'daily' || grainRaw === 'weekly' ? grainRaw : 'auto'
    const compare = params.get('compare') !== '0'
    return { kpi, grain: grain as 'auto' | 'daily' | 'weekly', compare }
  }, [])

  const [sortKey, setSortKey] = useState<SortKey>('attributed_value')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [showWhy, setShowWhy] = usePersistentToggle('channel-performance:show-explainability', false)
  const [showLagInsights, setShowLagInsights] = usePersistentToggle('channel-performance:show-lag-insights', false)

  const [directMode, setDirectMode] = useState<'include' | 'exclude'>('include')
  const [comparePrevious, setComparePrevious] = useState(initialTrendParams.compare)
  const [trendKpi, setTrendKpi] = useState(initialTrendParams.kpi)
  const [trendGrain, setTrendGrain] = useState<'auto' | 'daily' | 'weekly'>(initialTrendParams.grain)
  const [chartSortBy, setChartSortBy] = useState<'spend' | 'visits' | 'attributed_value' | 'roas'>('attributed_value')
  const [channelSearch, setChannelSearch] = useState('')
  const [onlyLowConfidence, setOnlyLowConfidence] = useState(false)
  const [selectedSegmentId, setSelectedSegmentId] = useState('')
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
    queryKey: ['channel-trend-panel', model, trendDateRange.dateFrom, trendDateRange.dateTo, trendKpi, trendGrain, comparePrevious, trendChannelsParam, configId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
        kpi_key: trendKpi,
        model,
        grain: trendGrain,
        compare: comparePrevious ? '1' : '0',
      })
      if (configId) params.set('model_id', configId)
      selectedTrendChannels.forEach((ch) => params.append('channels', ch))
      return apiGetJson<ChannelTrendResponse>(`/api/performance/channel/trend?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel trend',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const summaryQuery = useQuery<ChannelSummaryResponse>({
    queryKey: ['channel-summary-panel', model, trendDateRange.dateFrom, trendDateRange.dateTo, comparePrevious, trendChannelsParam, configId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
        model,
        compare: comparePrevious ? '1' : '0',
      })
      if (configId) params.set('model_id', configId)
      selectedTrendChannels.forEach((ch) => params.append('channels', ch))
      return apiGetJson<ChannelSummaryResponse>(`/api/performance/channel/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel summary',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const suggestionsQuery = useQuery<ChannelSuggestionResponse>({
    queryKey: ['channel-suggestions-panel', trendDateRange.dateFrom, trendDateRange.dateTo, configId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
      })
      if (configId) params.set('model_id', configId)
      return apiGetJson<ChannelSuggestionResponse>(`/api/performance/channel/suggestions?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel suggestions',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const lagQuery = useQuery<LagInsightsResponse>({
    queryKey: ['channel-lag-panel', trendDateRange.dateFrom, trendDateRange.dateTo, trendChannelsParam],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
      })
      selectedTrendChannels.forEach((ch) => params.append('channels', ch))
      return apiGetJson<LagInsightsResponse>(`/api/performance/channel/lag?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel lag analysis',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const lagBaselineQuery = useQuery<LagInsightsResponse>({
    queryKey: ['channel-lag-panel-baseline', trendDateRange.dateFrom, trendDateRange.dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
      })
      return apiGetJson<LagInsightsResponse>(`/api/performance/channel/lag?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel lag baseline',
      })
    },
    enabled: !!selectedSegmentId && !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['segment-registry', 'channel-performance'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load segment registry',
      }),
  })
  const meiroConfigQuery = useQuery<MeiroConfig>({
    queryKey: ['meiro-config'],
    queryFn: getMeiroConfig,
  })

  const loading = summaryQuery.isLoading

  const channelRows = useMemo(() => {
    const rows = summaryQuery.data?.items ?? []
    if (!rows.length) return []
    const suggestionMap = suggestionsQuery.data?.items ?? {}
    const revenueTotal = rows.reduce((sum, row) => sum + (row.current.revenue || 0), 0)
    return rows.map((row) => {
      const spend = row.current.spend || 0
      const visits = row.current.visits || 0
      const revenue = row.current.revenue || 0
      const conversions = row.current.conversions || 0
      const roas = row.derived?.roas != null ? row.derived.roas : spend > 0 ? revenue / spend : 0
      const cpa = row.derived?.cpa != null ? row.derived.cpa : conversions > 0 ? spend / conversions : 0
      const roi = spend > 0 ? (revenue - spend) / spend : 0
      const cvr = row.derived?.cvr != null ? row.derived.cvr : visits > 0 ? conversions / visits : 0
      const costPerVisit = row.derived?.cost_per_visit != null ? row.derived.cost_per_visit : visits > 0 ? spend / visits : 0
      const revenuePerVisit = row.derived?.revenue_per_visit != null ? row.derived.revenue_per_visit : visits > 0 ? revenue / visits : 0
      const roles = row.diagnostics?.roles || {}
      const funnel = row.diagnostics?.funnel || {}
      return {
        channel: row.channel,
        spend,
        visits,
        attributed_value: revenue,
        attributed_conversions: conversions,
        attributed_share: revenueTotal > 0 ? revenue / revenueTotal : 0,
        cvr,
        cost_per_visit: costPerVisit,
        revenue_per_visit: revenuePerVisit,
        first_touch_conversions: roles.first_touch_conversions || 0,
        assist_conversions: roles.assist_conversions || 0,
        last_touch_conversions: roles.last_touch_conversions || 0,
        first_touch_revenue: roles.first_touch_revenue || 0,
        assist_revenue: roles.assist_revenue || 0,
        last_touch_revenue: roles.last_touch_revenue || 0,
        touch_journeys: funnel.touch_journeys || 0,
        content_journeys: funnel.content_journeys || 0,
        checkout_journeys: funnel.checkout_journeys || 0,
        converted_journeys: funnel.converted_journeys || 0,
        funnel_conversion_rate: funnel.conversion_rate || 0,
        roi,
        roas,
        cpa,
        suggested_next: suggestionMap[row.channel] ?? null,
        confidence: row.confidence || undefined,
      } as ChannelData
    })
  }, [summaryQuery.data?.items, suggestionsQuery.data?.items])

  const localSegments = useMemo(
    () => (segmentRegistryQuery.data?.items ?? []).filter(isLocalAnalyticalSegment),
    [segmentRegistryQuery.data?.items],
  )
  const compatibleSegments = useMemo(
    () => localSegments.filter((item) => localSegmentCompatibleWithDimensions(item, ['channel_group'])),
    [localSegments],
  )
  const selectedSegment = useMemo(
    () => compatibleSegments.find((item) => item.id === selectedSegmentId) ?? null,
    [compatibleSegments, selectedSegmentId],
  )
  const selectedSegmentDefinition = useMemo(
    () => readLocalSegmentDefinition(selectedSegment),
    [selectedSegment],
  )

  const sortedChannels = useMemo(() => {
    if (!channelRows.length) return []
    const q = channelSearch.trim().toLowerCase()
    const base = channelRows.filter((ch) => {
      if (selectedSegmentDefinition.channel_group && ch.channel !== selectedSegmentDefinition.channel_group) return false
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
  }, [channelRows, sortKey, sortDir, directMode, channelSearch, onlyLowConfidence, selectedSegmentDefinition.channel_group])

  const filteredForCharts = useMemo(
    () =>
      channelRows.filter((ch) => {
        if (selectedSegmentDefinition.channel_group && ch.channel !== selectedSegmentDefinition.channel_group) return false
        if (directMode === 'exclude' && ch.channel === 'direct') return false
        if (onlyLowConfidence && (!ch.confidence || ch.confidence.score >= 70)) return false
        return true
      }),
    [channelRows, directMode, onlyLowConfidence, selectedSegmentDefinition.channel_group],
  )
  const workspaceRows = useMemo(
    () =>
      channelRows.filter((ch) => {
        if (directMode === 'exclude' && ch.channel === 'direct') return false
        if (onlyLowConfidence && (!ch.confidence || ch.confidence.score >= 70)) return false
        return true
      }),
    [channelRows, directMode, onlyLowConfidence],
  )
  const focusedSegmentRows = useMemo(
    () =>
      workspaceRows.filter((ch) => {
        if (selectedSegmentDefinition.channel_group && ch.channel !== selectedSegmentDefinition.channel_group) return false
        return true
      }),
    [workspaceRows, selectedSegmentDefinition.channel_group],
  )
  const latestEventReplay = summaryQuery.data?.readiness?.details?.latest_event_replay
  const latestEventReplayDiagnostics = latestEventReplay?.diagnostics
  const diagnosticRoleConversions = useMemo(
    () =>
      channelRows.reduce(
        (sum, row) => sum + row.first_touch_conversions + row.assist_conversions + row.last_touch_conversions,
        0,
      ),
    [channelRows],
  )
  const diagnosticFunnelConvertedJourneys = useMemo(
    () => channelRows.reduce((sum, row) => sum + row.converted_journeys, 0),
    [channelRows],
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
    if (!selectedSegmentId) return
    if (compatibleSegments.some((item) => item.id === selectedSegmentId)) return
    setSelectedSegmentId('')
  }, [compatibleSegments, selectedSegmentId])

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
    else if (chartSortBy === 'visits') clone.sort((a, b) => a.visits - b.visits)
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
          trendKpi === 'conversions' || trendKpi === 'visits'
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

  const summaryCurrent = summaryQuery.data?.totals?.current ?? { spend: 0, visits: 0, revenue: 0, conversions: 0 }
  const summaryPrevious = summaryQuery.data?.totals?.previous ?? { spend: 0, visits: 0, revenue: 0, conversions: 0 }
  const summaryOutcomesCurrent = summaryQuery.data?.totals?.outcomes_current ?? {}
  const totalSpend = summaryCurrent.spend
  const totalVisits = summaryCurrent.visits
  const totalValue = summaryCurrent.revenue
  const totalConversions = summaryCurrent.conversions

  const totalROAS = totalSpend > 0 ? totalValue / totalSpend : 0
  const avgCPA = totalConversions > 0 ? totalSpend / totalConversions : 0
  const totalCVR = totalVisits > 0 ? totalConversions / totalVisits : 0
  const totalCostPerVisit = totalVisits > 0 ? totalSpend / totalVisits : 0
  const totalRevenuePerVisit = totalVisits > 0 ? totalValue / totalVisits : 0
  const spendQuality = summaryQuery.data?.spend_quality ?? null
  const spendSignalWeak = totalSpend < 50
  const spendAllocatedOnly = (spendQuality?.status || '') === 'allocated_only'
  const showSpendConfidentSummary = !spendSignalWeak && !spendAllocatedOnly && totalSpend > 0
  const lagSummaryConversions = lagQuery.data?.summary?.conversions ?? 0
  const mixedBasisActivityWarning =
    Boolean(configId) &&
    totalConversions <= 0 &&
    (diagnosticRoleConversions > 0 || diagnosticFunnelConvertedJourneys > 0 || lagSummaryConversions > 0)

  const periodLabel =
    globalDateFrom && globalDateTo
      ? `${globalDateFrom} – ${globalDateTo}`
      : 'current dataset'

  const conversionLabel =
    summaryQuery.data?.meta?.conversion_key ||
    trendQuery.data?.meta?.conversion_key ||
    'All conversions'
  const measurementWindowLabel = summaryQuery.data?.config?.time_window
    ? [
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
        .join(' · ')
    : 'Not configured'
  const meiroScope = summaryQuery.data?.meta?.meiro_measurement_scope
  const meiroScopeStatus = String(meiroScope?.source_scope?.status || 'unknown').replace(/_/g, ' ')
  const meiroScopeFilterLabel = `${Number(meiroScope?.campaign_rows_excluded || 0).toLocaleString()} rows excluded · ${Number(meiroScope?.event_archive_site_scope?.out_of_scope_site_events || 0).toLocaleString()} out-of-scope events`
  const meiroScopeWarnings = [
    ...(meiroScope?.warnings || []),
    ...(Number(meiroScope?.campaign_rows_excluded || 0) > 0
      ? [`${Number(meiroScope?.campaign_rows_excluded || 0).toLocaleString()} campaign rows were excluded from this channel view because their evidence only matched out-of-scope Meiro archive data.`]
      : []),
  ]

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
        totalVisits: totalVisits - summaryPrevious.visits,
        totalVisitsPct:
          Math.abs(summaryPrevious.visits) > 1e-9
            ? ((totalVisits - summaryPrevious.visits) / summaryPrevious.visits) * 100
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
    { label: 'Visits', value: totalVisits.toLocaleString(), def: METRIC_DEFINITIONS['Visits'] },
    { label: 'Attributed Revenue', value: formatCurrency(totalValue), def: METRIC_DEFINITIONS['Attributed Revenue'] },
    { label: 'Conversions', value: totalConversions.toLocaleString(), def: METRIC_DEFINITIONS['Conversions'] },
    { label: 'CVR', value: `${(totalCVR * 100).toFixed(2)}%`, def: METRIC_DEFINITIONS['CVR'] },
    { label: 'Cost / Visit', value: formatCurrency(totalCostPerVisit), def: METRIC_DEFINITIONS['Cost / Visit'] },
    { label: 'Revenue / Visit', value: formatCurrency(totalRevenuePerVisit), def: METRIC_DEFINITIONS['Revenue / Visit'] },
    { label: 'ROAS', value: `${totalROAS.toFixed(2)}×`, def: METRIC_DEFINITIONS['ROAS'] },
    { label: 'Avg CPA', value: formatCurrency(avgCPA), def: METRIC_DEFINITIONS['CPA'] },
    { label: 'Net Revenue', value: formatCurrency(Number(summaryOutcomesCurrent.net_revenue || 0)), def: 'Revenue after refunds, cancellations, and invalidation.' },
    { label: 'Net Conversions', value: Number(summaryOutcomesCurrent.net_conversions || 0).toLocaleString(), def: 'Conversions remaining valid after post-conversion adjustments.' },
  ]
  const focusedSegmentSummary = useMemo(() => {
    if (!selectedSegment || !focusedSegmentRows.length) return null
    const spend = focusedSegmentRows.reduce((sum, row) => sum + row.spend, 0)
    const visits = focusedSegmentRows.reduce((sum, row) => sum + row.visits, 0)
    const conversions = focusedSegmentRows.reduce((sum, row) => sum + row.attributed_conversions, 0)
    const revenue = focusedSegmentRows.reduce((sum, row) => sum + row.attributed_value, 0)
    const cvr = visits > 0 ? conversions / visits : 0
    const roas = spend > 0 ? revenue / spend : 0
    const revenuePerVisit = visits > 0 ? revenue / visits : 0
    const spendShare = totalSpend > 0 ? (spend / totalSpend) * 100 : null
    const visitShare = totalVisits > 0 ? (visits / totalVisits) * 100 : null
    const conversionShare = totalConversions > 0 ? (conversions / totalConversions) * 100 : null
    const revenueShare = totalValue > 0 ? (revenue / totalValue) * 100 : null
    const focusedLagItem =
      (selectedSegmentDefinition.channel_group
        ? (lagBaselineQuery.data?.items ?? []).find(
            (item) =>
              item.channel === selectedSegmentDefinition.channel_group ||
              item.label === selectedSegmentDefinition.channel_group,
          ) ?? null
        : null) ?? null
    const focusedMedianLag = focusedLagItem?.p50_days_from_first_touch ?? null
    const workspaceMedianLag = lagBaselineQuery.data?.summary?.median_days_from_first_touch ?? null
    return {
      spend,
      visits,
      conversions,
      revenue,
      cvr,
      roas,
      revenuePerVisit,
      spendShare,
      visitShare,
      conversionShare,
      revenueShare,
      cvrDeltaPct: totalCVR > 0 ? ((cvr - totalCVR) / totalCVR) * 100 : null,
      roasDeltaPct: totalROAS > 0 ? ((roas - totalROAS) / totalROAS) * 100 : null,
      revenuePerVisitDeltaPct: totalRevenuePerVisit > 0 ? ((revenuePerVisit - totalRevenuePerVisit) / totalRevenuePerVisit) * 100 : null,
      focusedMedianLag,
      workspaceMedianLag,
      lagDeltaDays:
        focusedMedianLag != null && workspaceMedianLag != null ? focusedMedianLag - workspaceMedianLag : null,
    }
  }, [
    focusedSegmentRows,
    lagBaselineQuery.data?.items,
    lagBaselineQuery.data?.summary?.median_days_from_first_touch,
    selectedSegment,
    selectedSegmentDefinition.channel_group,
    totalConversions,
    totalCVR,
    totalROAS,
    totalRevenuePerVisit,
    totalSpend,
    totalValue,
    totalVisits,
  ])
  const channelNarrative = useMemo(() => {
    const topRevenueChannel = [...filteredForCharts].sort((a, b) => b.attributed_value - a.attributed_value)[0] ?? null
    const topRoasChannel = [...filteredForCharts].filter((item) => item.spend > 0).sort((a, b) => b.roas - a.roas)[0] ?? null
    const lagRiskChannel = [...(lagBaselineQuery.data?.items ?? [])]
      .filter((item) => item.conversions > 0)
      .sort((a, b) => (b.lag_buckets.over_7d / b.conversions) - (a.lag_buckets.over_7d / a.conversions))[0] ?? null
    const headline =
      kpiDeltas?.totalValuePct != null
        ? `Attributed revenue ${kpiDeltas.totalValuePct >= 0 ? 'rose' : 'fell'} ${Math.abs(kpiDeltas.totalValuePct).toFixed(1)}% vs the previous period.`
        : comparePrevious && !hasBaselineValue(summaryPrevious.revenue)
          ? 'Channel performance is loaded, but the previous period has no revenue baseline for a reliable percentage comparison.'
          : 'Channel performance is loaded for the current period.'
    const items = [
      topRevenueChannel
        ? `${topRevenueChannel.channel} is currently the largest revenue channel at ${formatCurrency(topRevenueChannel.attributed_value)}.`
        : null,
      showSpendConfidentSummary && topRoasChannel
        ? `${topRoasChannel.channel} is the most efficient visible channel at ${topRoasChannel.roas.toFixed(2)}× ROAS.`
        : null,
      lagRiskChannel
        ? `${lagRiskChannel.label} has the heaviest long-lag exposure, with ${formatPercent(
            lagRiskChannel.conversions > 0 ? (lagRiskChannel.lag_buckets.over_7d / lagRiskChannel.conversions) * 100 : null,
          )} of conversions taking more than 7 days.`
        : null,
      focusedSegmentSummary && selectedSegment && showSpendConfidentSummary
        ? `${selectedSegment.name} contributes ${focusedSegmentSummary.revenueShare?.toFixed(1) ?? '—'}% of revenue and runs ${formatPercent(focusedSegmentSummary.cvrDeltaPct)} vs workspace CVR.`
        : focusedSegmentSummary && selectedSegment
        ? `${selectedSegment.name} contributes ${focusedSegmentSummary.revenueShare?.toFixed(1) ?? '—'}% of revenue. Spend-based ROAS comparisons are muted because channel spend is ${spendAllocatedOnly ? 'allocated only in this slice' : 'too weak in this slice'}.`
        : null,
    ].filter((item): item is string => Boolean(item))
    return { headline, items }
  }, [
    comparePrevious,
    filteredForCharts,
    focusedSegmentSummary,
    kpiDeltas?.totalValuePct,
    lagBaselineQuery.data?.items,
    selectedSegment,
    showSpendConfidentSummary,
    spendAllocatedOnly,
    summaryPrevious.revenue,
  ])

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
        {latestEventReplayDiagnostics ? (
          <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Latest raw-event replay diagnosis</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Events {Number(latestEventReplayDiagnostics.events_loaded || 0).toLocaleString()} · reconstructed profiles {Number(latestEventReplayDiagnostics.profiles_reconstructed || 0).toLocaleString()} · touchpoints {Number(latestEventReplayDiagnostics.touchpoints_reconstructed || 0).toLocaleString()} · conversions {Number(latestEventReplayDiagnostics.conversions_reconstructed || 0).toLocaleString()} · attributable profiles {Number(latestEventReplayDiagnostics.attributable_profiles || 0).toLocaleString()} · persisted journeys {Number(latestEventReplayDiagnostics.journeys_persisted || 0).toLocaleString()}
            </div>
            {!!latestEventReplayDiagnostics.warnings?.length && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                {latestEventReplayDiagnostics.warnings.join(' · ')}
              </div>
            )}
          </div>
        ) : null}
      </div>
    )
  }

  const tableColumns: { key: SortKey; label: string; align: 'left' | 'right'; format: (ch: ChannelData) => string }[] = [
    { key: 'channel', label: 'Channel', align: 'left', format: (ch) => ch.channel },
    { key: 'spend', label: 'Spend', align: 'right', format: (ch) => formatCurrency(ch.spend) },
    { key: 'visits', label: 'Visits', align: 'right', format: (ch) => ch.visits.toLocaleString() },
    { key: 'cvr', label: 'CVR', align: 'right', format: (ch) => `${(ch.cvr * 100).toFixed(2)}%` },
    { key: 'cost_per_visit', label: 'Cost / Visit', align: 'right', format: (ch) => formatCurrency(ch.cost_per_visit) },
    { key: 'revenue_per_visit', label: 'Revenue / Visit', align: 'right', format: (ch) => formatCurrency(ch.revenue_per_visit) },
    { key: 'attributed_value', label: 'Attributed Revenue', align: 'right', format: (ch) => formatCurrency(ch.attributed_value) },
    { key: 'attributed_conversions', label: 'Conversions', align: 'right', format: (ch) => ch.attributed_conversions.toFixed(1) },
    { key: 'attributed_share', label: 'Share', align: 'right', format: (ch) => `${(ch.attributed_share * 100).toFixed(1)}%` },
    { key: 'roi', label: 'ROI', align: 'right', format: (ch) => `${(ch.roi * 100).toFixed(0)}%` },
    { key: 'roas', label: 'ROAS', align: 'right', format: (ch) => `${ch.roas.toFixed(2)}×` },
    { key: 'cpa', label: 'CPA', align: 'right', format: (ch) => (ch.cpa > 0 ? formatCurrency(ch.cpa) : '—') },
  ]
  const roleRows = [...sortedChannels]
    .sort((a, b) => b.last_touch_revenue + b.assist_revenue + b.first_touch_revenue - (a.last_touch_revenue + a.assist_revenue + a.first_touch_revenue))
    .slice(0, 8)
  const funnelRows = [...sortedChannels]
    .sort((a, b) => b.touch_journeys - a.touch_journeys)
    .slice(0, 8)
  const channelTableColumns: AnalyticsTableColumn<ChannelData>[] = [
    ...tableColumns.map((column) => ({
      key: String(column.key),
      label: column.label,
      hideable: column.key !== 'channel',
      align: column.align,
      sortable: true,
      sortDirection: sortKey === column.key ? sortDir : null,
      onSort: () => handleSort(column.key),
      title: `Sort by ${column.label}`,
      render: (channel: ChannelData) => {
        if (column.key === 'roas' && channel.spend === 0) return 'N/A'
        if (column.key === 'cpa' && channel.spend === 0) return 'N/A'
        return column.format(channel)
      },
      cellStyle: (channel: ChannelData) => {
        const isChannel = column.key === 'channel'
        const isRoi = column.key === 'roi'
        const isValue = column.key === 'attributed_value'
        return {
          fontWeight: isChannel || isValue || isRoi ? t.font.weightMedium : t.font.weightNormal,
          color:
            column.key === 'cpa' && channel.spend === 0
              ? t.color.textSecondary
              : isValue
              ? t.color.success
              : isRoi
              ? channel.roi >= 0
                ? t.color.success
                : t.color.danger
              : t.color.text,
        }
      },
    })),
    {
      key: 'suggested_next',
      label: 'Suggested next',
      hideable: true,
      title: METRIC_DEFINITIONS['Suggested next'],
      render: (channel) =>
        channel.suggested_next ? (
          <div style={{ display: 'inline-flex', gap: t.space.xs, alignItems: 'center', flexWrap: 'wrap' }}>
            <span
              title={`${channel.suggested_next.count} journeys, ${(channel.suggested_next.conversion_rate * 100).toFixed(1)}% conversion, avg $${channel.suggested_next.avg_value}`}
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
              {channel.suggested_next.channel}
              {channel.suggested_next.campaign ? ` / ${channel.suggested_next.campaign}` : ''}{' '}
              ({(channel.suggested_next.conversion_rate * 100).toFixed(0)}%)
            </span>
            {channel.suggested_next.is_promoted_policy ? (
              <>
                <span
                  title={channel.suggested_next.promoted_policy_title ?? 'Promoted Journey Lab policy'}
                  style={{
                    display: 'inline-block',
                    padding: '2px 8px',
                    border: `1px solid ${t.color.warning}`,
                    color: t.color.warning,
                    borderRadius: t.radius.full,
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                  }}
                >
                  Deployed policy
                </span>
                {buildJourneyHypothesisHref({
                  journeyDefinitionId: channel.suggested_next.promoted_policy_journey_definition_id,
                  hypothesisId: channel.suggested_next.promoted_policy_hypothesis_id,
                }) ? (
                  <a
                    href={buildJourneyHypothesisHref({
                      journeyDefinitionId: channel.suggested_next.promoted_policy_journey_definition_id,
                      hypothesisId: channel.suggested_next.promoted_policy_hypothesis_id,
                    }) || '#'}
                    style={{ fontSize: t.font.sizeXs, color: t.color.accent, textDecoration: 'none' }}
                  >
                    Open policy
                  </a>
                ) : null}
              </>
            ) : null}
          </div>
        ) : (
          <span style={{ color: t.color.textMuted, fontSize: t.font.sizeXs }}>—</span>
        ),
    },
    {
      key: 'confidence',
      label: 'Confidence',
      align: 'right',
      render: (channel) => <ConfidenceBadge confidence={channel.confidence} compact />,
    },
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
      <div style={{ marginBottom: t.space.md }}>
        <ContextSummaryStrip
          items={[
            { label: 'Source basis', value: latestEventReplayDiagnostics?.events_loaded ? 'Pipes raw events -> live attribution' : 'Config-aware performance summary' },
            { label: 'Target instance', value: meiroConfigQuery.data?.target_instance_host || 'meiro-internal.eu.pipes.meiro.io' },
            {
              label: 'Site scope',
              value: `${(summaryQuery.data?.meta?.site_scope?.target_sites || ['meiro.io', 'meir.store']).join(', ')}${Number(summaryQuery.data?.meta?.site_scope?.journeys_excluded || 0) > 0 ? ` · ${Number(summaryQuery.data?.meta?.site_scope?.journeys_excluded || 0).toLocaleString()} excluded` : ''}`,
            },
            { label: 'Archive scope', value: meiroScopeStatus },
            { label: 'Scope filter', value: meiroScopeFilterLabel },
            { label: 'Period', value: periodLabel },
            { label: 'Conversion', value: `${conversionLabel} (read-only)` },
            {
              label: 'Config basis',
              value: configId ? `Selected config ${configId.slice(0, 8)}… applied` : 'Default active config',
            },
            { label: 'Direct handling', value: directMode === 'include' ? 'Include Direct' : 'Exclude Direct' },
            { label: 'Compare previous', value: comparePrevious ? 'Enabled' : 'Disabled' },
            { label: 'Measurement window', value: measurementWindowLabel },
          ]}
        />
      </div>
      <div style={{ marginBottom: t.space.md }}>
        <MeiroTargetInstanceBadge config={meiroConfigQuery.data} compact />
      </div>
      <div style={{ marginBottom: t.space.md }}>
        <MeiroMeasurementScopeNotice compact />
      </div>
      {meiroScopeWarnings.length ? (
        <SurfaceBasisNotice marginBottom={t.space.md}>
          {meiroScopeWarnings.slice(0, 3).join(' ')}
        </SurfaceBasisNotice>
      ) : null}
      {mixedBasisActivityWarning ? (
        <SurfaceBasisNotice marginBottom={t.space.md}>
          The selected config <strong>{configId?.slice(0, 8)}…</strong> currently yields no config-scoped channel conversions in the KPI totals above, but supporting role, funnel, or lag diagnostics still show <strong>workspace-period activity</strong>. Read those lower panels as diagnostic context, not as proof that the selected config produced visible channel conversions in this slice.
        </SurfaceBasisNotice>
      ) : null}
      {!!summaryQuery.data?.notes?.length && (
        <div style={{ marginBottom: t.space.md, display: 'grid', gap: t.space.xs }}>
          {summaryQuery.data.notes.map((note, idx) => (
            <div key={`${note}-${idx}`} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, padding: `${t.space.sm}px ${t.space.md}px`, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, background: t.color.surface }}>
              {note}
            </div>
          ))}
        </div>
      )}
      <div style={{ marginBottom: t.space.md, display: 'flex', gap: t.space.md, flexWrap: 'wrap', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        <span>Click-through: {Number(summaryOutcomesCurrent.click_through_conversions || 0).toLocaleString()}</span>
        <span>View-through: {Number(summaryOutcomesCurrent.view_through_conversions || 0).toLocaleString()}</span>
        <span>Mixed paths: {Number(summaryOutcomesCurrent.mixed_path_conversions || 0).toLocaleString()}</span>
        <span>Refunded value: {formatCurrency(Number(summaryOutcomesCurrent.refunded_value || 0))}</span>
        <span>Invalid leads: {Number(summaryOutcomesCurrent.invalid_leads || 0).toLocaleString()}</span>
      </div>
      {focusedSegmentSummary ? (
        <div style={{ marginBottom: t.space.xl }}>
        <SectionCard
          title={`Segment vs workspace baseline: ${selectedSegment?.name ?? 'Saved segment'}`}
          subtitle={`Focused on ${selectedSegmentDefinition.channel_group || 'selected channels'} with the same direct-handling view as the page.`}
          overflow="visible"
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
          <SegmentComparisonContextNote
            mode="exact_filter"
            pageLabel="visible channel rows"
            basisLabel="visible channel summary rows under the current direct-handling mode"
            primaryLabel={selectedSegment?.name || 'Selected audience'}
            primaryRows={focusedSegmentRows.length}
            baselineRows={workspaceRows.length}
          />
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
              gap: t.space.md,
            }}
          >
            {[
              {
                label: 'Revenue share',
                value: focusedSegmentSummary.revenueShare != null ? `${focusedSegmentSummary.revenueShare.toFixed(1)}%` : '—',
                note: `${formatCurrency(focusedSegmentSummary.revenue)} of ${formatCurrency(totalValue)}`,
              },
              {
                label: 'Conversion share',
                value: focusedSegmentSummary.conversionShare != null ? `${focusedSegmentSummary.conversionShare.toFixed(1)}%` : '—',
                note: `${focusedSegmentSummary.conversions.toLocaleString()} of ${totalConversions.toLocaleString()}`,
              },
              {
                label: 'Visits share',
                value: focusedSegmentSummary.visitShare != null ? `${focusedSegmentSummary.visitShare.toFixed(1)}%` : '—',
                note: `${focusedSegmentSummary.visits.toLocaleString()} of ${totalVisits.toLocaleString()}`,
              },
              {
                label: 'CVR vs workspace',
                value: `${(focusedSegmentSummary.cvr * 100).toFixed(2)}%`,
                note: `${formatPercent(focusedSegmentSummary.cvrDeltaPct)} vs ${(totalCVR * 100).toFixed(2)}% overall`,
              },
              {
                label: 'Median lag vs workspace',
                value:
                  focusedSegmentSummary.focusedMedianLag != null
                    ? `${focusedSegmentSummary.focusedMedianLag.toFixed(1)}d`
                    : '—',
                note:
                  focusedSegmentSummary.workspaceMedianLag != null
                    ? `${focusedSegmentSummary.lagDeltaDays != null && focusedSegmentSummary.lagDeltaDays >= 0 ? '+' : ''}${focusedSegmentSummary.lagDeltaDays?.toFixed(1) ?? '0.0'}d vs ${focusedSegmentSummary.workspaceMedianLag.toFixed(1)}d overall`
                    : 'Lag baseline unavailable',
              },
              ...(showSpendConfidentSummary
                ? [
                    {
                      label: 'ROAS vs workspace',
                      value: focusedSegmentSummary.roas.toFixed(2),
                      note: `${formatPercent(focusedSegmentSummary.roasDeltaPct)} vs ${totalROAS.toFixed(2)} overall`,
                    },
                  ]
                : [
                    {
                      label: 'Spend trust',
                      value: spendAllocatedOnly ? 'Allocated only' : 'Too weak',
                      note: spendAllocatedOnly
                        ? 'Channel spend in this slice is allocation-derived, so ROAS comparisons are directional.'
                        : 'This slice has too little measured spend for confident ROAS comparisons.',
                    },
                  ]),
            ].map((item) => (
              <div
                key={item.label}
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.md,
                  padding: t.space.md,
                  background: t.color.surface,
                  minWidth: 0,
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{item.label}</div>
                <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.value}</div>
                <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{item.note}</div>
              </div>
            ))}
          </div>
          </div>
        </SectionCard>
        </div>
      ) : null}

      <div style={{ marginBottom: t.space.xl }}>
        <AnalysisNarrativePanel
          title="What changed"
          subtitle="A short readout of the current channel mix before you move into charts and tables."
          headline={channelNarrative.headline}
          items={channelNarrative.items}
        />
      </div>

      {summaryQuery.data?.readiness && (summaryQuery.data.readiness.status === 'blocked' || summaryQuery.data.readiness.warnings.length > 0) ? (
        <DecisionStatusCard
          title="Live Attribution Reliability"
          status={summaryQuery.data.readiness.status}
          blockers={summaryQuery.data.readiness.blockers}
          warnings={summaryQuery.data.readiness.warnings.slice(0, 3)}
        />
      ) : null}

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

      <div style={{ marginBottom: t.space.lg }}>
        <CollapsiblePanel
          title="Method & Explainability"
          subtitle="How the channel view is computed and what assumptions drive the metrics."
          open={showWhy}
          onToggle={() => setShowWhy((v) => !v)}
        >
          <ExplainabilityPanel scope="channel" configId={configId ?? undefined} model={model} />
        </CollapsiblePanel>
      </div>

      <div style={{ marginBottom: t.space.lg }}>
        {configId ? (
          <SurfaceBasisNotice marginBottom={t.space.sm}>
            Channel summary and trend panels are using the selected config <strong>{configId.slice(0, 8)}…</strong>. The lag panel below still reflects workspace diagnostic facts for the selected period, so read lag directionally rather than as a config-scoped view.
          </SurfaceBasisNotice>
        ) : null}
        <CollapsiblePanel
          title="Conversion Lag Analysis"
          subtitle="How quickly each channel tends to convert after first touch and last touch."
          open={showLagInsights}
          onToggle={() => setShowLagInsights((value) => !value)}
        >
          <LagInsightsPanel
            title="Channel lag analysis"
            subtitle={`Selected period ${periodLabel}${selectedTrendChannels.length ? ` · filtered to ${selectedTrendChannels.join(', ')}` : ''}`}
            data={lagQuery.data}
            loading={lagQuery.isLoading}
            error={lagQuery.isError ? (lagQuery.error as Error)?.message || 'Failed to load lag analysis' : null}
            emptyLabel="No channel lag data is available for the selected period."
            selectedActions={(item) => [
              {
                label: 'Open in Incrementality',
                href: buildIncrementalityPlannerHref({
                  channel: item.channel || item.label,
                  conversionKey:
                    summaryQuery.data?.meta?.conversion_key ||
                    trendQuery.data?.meta?.conversion_key ||
                    journeysSummary?.primary_kpi_id ||
                    null,
                  startAt: trendDateRange.dateFrom,
                  endAt: trendDateRange.dateTo,
                  name: `Lag test: ${item.label}`,
                  notes: `Investigate lag-heavy channel ${item.label}. P50 first-touch lag ${item.p50_days_from_first_touch != null ? `${item.p50_days_from_first_touch.toFixed(1)}d` : 'n/a'}.`,
                }),
              },
            ]}
          />
        </CollapsiblePanel>
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <div
          style={{
            marginBottom: t.space.sm,
            display: 'grid',
            gap: t.space.sm,
          }}
        >
          <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
            <label style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Trend metric</label>
            <select
              value={trendKpi}
              onChange={(e) => setTrendKpi(e.target.value)}
              style={{
                fontSize: t.font.sizeSm,
                padding: '6px 10px',
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                backgroundColor: t.color.surface,
                color: t.color.text,
              }}
            >
              <option value="spend">Spend</option>
              <option value="visits">Visits</option>
              <option value="conversions">Conversions</option>
              <option value="revenue">Revenue</option>
              <option value="cpa">CPA</option>
              <option value="roas">ROAS</option>
            </select>
          </div>
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
                  if (kpi.label === 'Visits') {
                    if (kpiDeltas.totalVisits == null) return 'Δ N/A'
                    const abs = kpiDeltas.totalVisits
                    const pct = kpiDeltas.totalVisitsPct
                    return `${abs >= 0 ? '+' : ''}${abs.toFixed(0)} ${pct != null ? `/ ${pct.toFixed(1)}%` : comparePrevious && !hasBaselineValue(summaryPrevious.visits) ? '/ No prior data' : ''}`
                  }
                  if (kpi.label === 'Attributed Revenue') {
                    if (kpiDeltas.totalValue == null) return 'Δ N/A'
                    const abs = kpiDeltas.totalValue
                    const pct = kpiDeltas.totalValuePct
                    const absLabel = formatCurrency(Math.abs(abs))
                    return `${abs >= 0 ? '+' : '-'}${absLabel} ${pct != null ? `/ ${pct.toFixed(1)}%` : comparePrevious && !hasBaselineValue(summaryPrevious.revenue) ? '/ No prior data' : ''}`
                  }
                  if (kpi.label === 'Conversions') {
                    if (kpiDeltas.totalConversions == null) return 'Δ N/A'
                    const abs = kpiDeltas.totalConversions
                    const pct = kpiDeltas.totalConversionsPct
                    return `${abs >= 0 ? '+' : ''}${abs.toFixed(0)} ${pct != null ? `/ ${pct.toFixed(1)}%` : comparePrevious && !hasBaselineValue(summaryPrevious.conversions) ? '/ No prior data' : ''}`
                  }
                  return 'Δ N/A'
                })()}
              </div>
            )}
          </div>
        ))}
      </div>

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))',
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
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'start', flexWrap: 'wrap', marginBottom: t.space.md }}>
            <div style={{ display: 'grid', gap: 4 }}>
              <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                Attribution Role Split
              </h3>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Use the dedicated Attribution Roles page for deeper introducer / assister / closer analysis.
              </div>
            </div>
            <a
              href="/?page=roles"
              style={{
                alignSelf: 'center',
                color: t.color.accent,
                textDecoration: 'none',
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
              }}
            >
              Open roles workspace
            </a>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.color.border}` }}>
                  <th style={{ textAlign: 'left', padding: `${t.space.sm}px 0` }}>Channel</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>First</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Assist</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Last</th>
                </tr>
              </thead>
              <tbody>
                {roleRows.map((row) => (
                  <tr key={row.channel} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: `${t.space.sm}px 0`, fontWeight: t.font.weightMedium }}>{row.channel}</td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                      {row.first_touch_conversions.toFixed(0)} / {formatCurrency(row.first_touch_revenue)}
                    </td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                      {row.assist_conversions.toFixed(0)} / {formatCurrency(row.assist_revenue)}
                    </td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                      {row.last_touch_conversions.toFixed(0)} / {formatCurrency(row.last_touch_revenue)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
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
          <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Funnel Progression
          </h3>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.color.border}` }}>
                  <th style={{ textAlign: 'left', padding: `${t.space.sm}px 0` }}>Channel</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Touched</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Content</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Checkout</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Converted</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Conv rate</th>
                </tr>
              </thead>
              <tbody>
                {funnelRows.map((row) => (
                  <tr key={row.channel} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: `${t.space.sm}px 0`, fontWeight: t.font.weightMedium }}>{row.channel}</td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>{row.touch_journeys.toLocaleString()}</td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>{row.content_journeys.toLocaleString()}</td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>{row.checkout_journeys.toLocaleString()}</td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>{row.converted_journeys.toLocaleString()}</td>
                    <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>{(row.funnel_conversion_rate * 100).toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
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
                onChange={(e) => setChartSortBy(e.target.value as 'spend' | 'visits' | 'attributed_value' | 'roas')}
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
                <option value="visits">Visits</option>
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
      <SectionCard
        title="Channel Detail"
        actions={
          <button
            type="button"
            onClick={() =>
              exportTableCSV(filteredForCharts, {
                model,
                periodLabel,
                conversionKey:
                  summaryQuery.data?.meta?.conversion_key ??
                  trendQuery.data?.meta?.conversion_key ??
                  null,
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
        }
        overflow="visible"
      >
        <style>{`
          @media (max-width: 900px) { .cp-charts { grid-template-columns: 1fr !important; } }
        `}</style>
        <AnalyticsTable
          columns={channelTableColumns}
          rows={sortedChannels}
          rowKey={(channel) => channel.channel}
          tableLabel="Channel detail"
          stickyFirstColumn
          virtualized
          virtualizationThreshold={40}
          virtualizationHeight={680}
          virtualRowHeight={50}
          allowColumnHiding
          allowDensityToggle
          persistKey="channel-detail-table"
          defaultHiddenColumnKeys={['cost_per_visit', 'revenue_per_visit', 'attributed_share', 'confidence']}
          presets={[
            {
              key: 'overview',
              label: 'Overview',
              visibleColumnKeys: ['channel', 'visits', 'attributed_conversions', 'attributed_value', 'spend', 'roas', 'cpa', 'suggested_next'],
            },
            {
              key: 'efficiency',
              label: 'Efficiency',
              visibleColumnKeys: ['channel', 'visits', 'cvr', 'cost_per_visit', 'revenue_per_visit', 'roi', 'roas', 'cpa', 'confidence'],
            },
            {
              key: 'mix',
              label: 'Mix',
              visibleColumnKeys: ['channel', 'visits', 'attributed_conversions', 'attributed_value', 'attributed_share', 'spend', 'suggested_next', 'confidence'],
            },
          ]}
          defaultPresetKey="overview"
          toolbar={
            <AnalyticsToolbar
              searchValue={channelSearch}
              onSearchChange={setChannelSearch}
              searchPlaceholder="Search channels…"
              filters={
                <>
                  <select
                    value={selectedSegmentId}
                    onChange={(event) => setSelectedSegmentId(event.target.value)}
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      fontSize: t.font.sizeSm,
                      border: `1px solid ${t.color.border}`,
                      borderRadius: t.radius.sm,
                      color: t.color.text,
                      background: t.color.surface,
                    }}
                  >
                    <option value="">All channels / no analytical segment</option>
                    {compatibleSegments.map((segment) => (
                      <option key={segment.id} value={segment.id}>
                        {segmentOptionLabel(segment)}
                      </option>
                    ))}
                  </select>
                  <label
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 6,
                      cursor: 'pointer',
                      fontSize: t.font.sizeSm,
                      color: t.color.textSecondary,
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={onlyLowConfidence}
                      onChange={(event) => setOnlyLowConfidence(event.target.checked)}
                      style={{ margin: 0 }}
                    />
                    Show only low confidence (Conf. &lt; 70)
                  </label>
                  <a
                    href={buildSettingsHref('segments')}
                    style={{ fontSize: t.font.sizeSm, color: t.color.accent, textDecoration: 'none' }}
                  >
                    Manage segments
                  </a>
                </>
              }
            />
          }
          emptyState="No channels match the current filters."
        />
      </SectionCard>
    </div>
  )
}
