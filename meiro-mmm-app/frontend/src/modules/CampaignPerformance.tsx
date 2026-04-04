import { useState, useMemo, useCallback, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ComposedChart, Line, ScatterChart, Scatter, ZAxis } from 'recharts'
import { tokens } from '../theme/tokens'
import ConfidenceBadge, { Confidence } from '../components/ConfidenceBadge'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import TrendPanel from '../components/dashboard/TrendPanel'
import { AnalyticsTable, AnalyticsToolbar, type AnalyticsTableColumn, SectionCard } from '../components/dashboard'
import { apiGetJson } from '../lib/apiClient'
import { buildJourneyHypothesisHref } from '../lib/journeyLinks'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import AdsActionsDrawer from '../components/ads/AdsActionsDrawer'
import DecisionStatusCard from '../components/DecisionStatusCard'
import { getAdsDeepLink, type AdsProviderKey } from '../connectors/adsManagerConnector'

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
  is_promoted_policy?: boolean
  promoted_policy_title?: string | null
  promoted_policy_hypothesis_id?: string | null
  promoted_policy_journey_definition_id?: string | null
}

interface CampaignSuggestionResponse {
  items: Record<string, SuggestedNext>
  level: string
  eligible_journeys: number
  reason?: string
}

interface CampaignData {
  campaign: string
  channel: string
  campaign_name: string | null
  visits: number
  attributed_value: number
  attributed_share: number
  attributed_conversions: number
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
  confidence_score?: number
}

interface CampaignTrendV2Row {
  ts: string
  campaign_id: string
  campaign_name?: string | null
  channel: string
  platform?: string | null
  value: number | null
}

interface CampaignTrendV2Response {
  current_period: { date_from: string; date_to: string; grain: 'daily' | 'weekly' }
  previous_period: { date_from: string; date_to: string }
  series: CampaignTrendV2Row[]
  series_prev?: CampaignTrendV2Row[]
  meta?: {
    conversion_key?: string | null
    conversion_key_resolution?: {
      configured_conversion_key?: string | null
      applied_conversion_key?: string | null
      reason?: string
    } | null
  } | null
}

interface CampaignSummaryItem {
  campaign_id: string
  campaign_name?: string | null
  channel: string
  platform?: string | null
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

interface CampaignSummaryResponse {
  current_period: { date_from: string; date_to: string; grain?: string }
  previous_period: { date_from: string; date_to: string }
  items: CampaignSummaryItem[]
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
  notes?: string[]
  meta?: {
    conversion_key?: string | null
    conversion_key_resolution?: {
      configured_conversion_key?: string | null
      applied_conversion_key?: string | null
      reason?: string
    } | null
  } | null
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
  'Visits': 'Normalized touchpoint count observed for each campaign in the selected period.',
  'Attributed Revenue': 'Revenue attributed to each campaign by the selected model.',
  'Conversions': 'Attributed conversion count.',
  'CVR': 'Attributed conversions divided by observed visits.',
  'Cost / Visit': 'Spend divided by visits.',
  'Revenue / Visit': 'Attributed revenue divided by visits.',
  'Suggested next': 'Next Best Action: recommended next channel/campaign after this one.',
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

function providerFromChannel(channel: string): AdsProviderKey | null {
  const key = (channel || '').toLowerCase()
  if (key.includes('google')) return 'google_ads'
  if (key.includes('meta') || key.includes('facebook') || key.includes('fb')) return 'meta_ads'
  if (key.includes('linkedin')) return 'linkedin_ads'
  return null
}

function exportCampaignsCSV(
  campaigns: CampaignData[],
  opts: {
    conversionKey?: string
    configVersion?: number | null
    directMode: 'include' | 'exclude'
  },
) {
  const headers = [
    'Campaign',
    'Channel',
    'Visits',
    'CVR',
    'Cost / Visit',
    'Revenue / Visit',
    'Attributed Revenue',
    'Share %',
    'Conversions',
    'Spend',
    'ROI %',
    'ROAS',
    'CPA',
    'Suggested next',
    'Period label',
    'Conversion key',
    'Config version',
    'Direct handling',
  ]
  const periodLabel = 'current_dataset'
  const rows = campaigns.map((c) => [
    c.campaign,
    c.channel,
    c.visits.toFixed(0),
    (c.cvr * 100).toFixed(2),
    c.cost_per_visit.toFixed(4),
    c.revenue_per_visit.toFixed(4),
    c.attributed_value.toFixed(2),
    (c.attributed_share * 100).toFixed(1),
    c.attributed_conversions.toFixed(1),
    c.spend.toFixed(2),
    c.roi != null ? (c.roi * 100).toFixed(0) : '',
    c.roas != null ? c.roas.toFixed(2) : '',
    c.cpa != null ? c.cpa.toFixed(2) : '',
    c.suggested_next ? (c.suggested_next.campaign != null ? `${c.suggested_next.channel}/${c.suggested_next.campaign}` : c.suggested_next.channel) : '',
    periodLabel,
    opts.conversionKey || '',
    opts.configVersion != null ? String(opts.configVersion) : '',
    opts.directMode,
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
  const [search, setSearch] = useState('')
  const [channelFilter, setChannelFilter] = useState<string>('')
  const [campaignTargets, setCampaignTargets] = useState<Record<string, number>>({})
  const [selectedCampaign, setSelectedCampaign] = useState<string | null>(null)
  const [showWhy, setShowWhy] = useState(false)
  const [conversionKey, setConversionKey] = useState<string | ''>('')
  const [directMode, setDirectMode] = useState<'include' | 'exclude'>('include')
  const [comparePrevious, setComparePrevious] = useState(initialTrendParams.compare)
  const [trendKpi, setTrendKpi] = useState(initialTrendParams.kpi)
  const [trendGrain, setTrendGrain] = useState<'auto' | 'daily' | 'weekly'>(initialTrendParams.grain)
  const [trendCampaignSearch, setTrendCampaignSearch] = useState('')
  const [selectedTrendCampaign, setSelectedTrendCampaign] = useState<string>('all')
  const [adsDrawerContext, setAdsDrawerContext] = useState<{
    provider: AdsProviderKey
    accountId: string
    entityType: 'campaign'
    entityId: string
    entityName?: string | null
    previewMetrics?: {
      spend7d?: number | null
      conversions7d?: number | null
      revenue7d?: number | null
      roas?: number | null
      cpa?: number | null
    }
    decisionContext?: {
      source: 'performance_recommendation' | 'deployed_journey_policy'
      scope_label?: string | null
      recommended_channel?: string | null
      recommended_campaign?: string | null
      conversion_rate?: number | null
      journey_count?: number | null
      avg_value?: number | null
      policy_title?: string | null
      hypothesis_id?: string | null
      journey_definition_id?: string | null
    } | null
  } | null>(null)

  const trendDateRange = useMemo(() => {
    const fallbackTo = new Date().toISOString().slice(0, 10)
    const fallbackFrom = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)
    return { dateFrom: globalDateFrom || fallbackFrom, dateTo: globalDateTo || fallbackTo }
  }, [globalDateFrom, globalDateTo])

  const trendQuery = useQuery<CampaignTrendV2Response>({
    queryKey: ['campaign-performance-trend-v2', trendDateRange.dateFrom, trendDateRange.dateTo, trendKpi, trendGrain, comparePrevious, conversionKey || 'all'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
        kpi_key: trendKpi,
        conversion_key: conversionKey || '',
        grain: trendGrain,
        compare: comparePrevious ? '1' : '0',
      })
      return apiGetJson<CampaignTrendV2Response>(`/api/performance/campaign/trend?${params.toString()}`, {
        fallbackMessage: 'Failed to fetch campaign trends',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const summaryQuery = useQuery<CampaignSummaryResponse>({
    queryKey: ['campaign-summary-v1', trendDateRange.dateFrom, trendDateRange.dateTo, comparePrevious, conversionKey || 'all'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
        compare: comparePrevious ? '1' : '0',
      })
      if (conversionKey) params.set('conversion_key', conversionKey)
      return apiGetJson<CampaignSummaryResponse>(`/api/performance/campaign/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to fetch campaign summary',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const suggestionsQuery = useQuery<CampaignSuggestionResponse>({
    queryKey: ['campaign-suggestions-v1', trendDateRange.dateFrom, trendDateRange.dateTo, conversionKey || 'all', configId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: trendDateRange.dateFrom,
        date_to: trendDateRange.dateTo,
        timezone: 'UTC',
      })
      if (conversionKey) params.set('conversion_key', conversionKey)
      if (configId) params.set('model_id', configId)
      return apiGetJson<CampaignSuggestionResponse>(`/api/performance/campaign/suggestions?${params.toString()}`, {
        fallbackMessage: 'Failed to fetch campaign suggestions',
      })
    },
    enabled: !!trendDateRange.dateFrom && !!trendDateRange.dateTo,
    refetchInterval: false,
  })

  const campaigns = useMemo(() => {
    const items = summaryQuery.data?.items ?? []
    if (!items.length) return []
    const suggestionMap = suggestionsQuery.data?.items ?? {}
    const totalRevenue = items.reduce((sum, item) => sum + (item.current.revenue || 0), 0)
    return items.map((item) => {
      const channel = String(item.channel || 'unknown')
      const campaignId = String(item.campaign_id || item.campaign_name || `${channel}:unknown`)
      const spend = item.current.spend || 0
      const visits = item.current.visits || 0
      const revenue = item.current.revenue || 0
      const conversions = item.current.conversions || 0
      const roas = item.derived?.roas ?? (spend > 0 ? revenue / spend : null)
      const cpa = item.derived?.cpa ?? (conversions > 0 ? spend / conversions : null)
      const roi = spend > 0 ? (revenue - spend) / spend : null
      const cvr = item.derived?.cvr ?? (visits > 0 ? conversions / visits : 0)
      const costPerVisit = item.derived?.cost_per_visit ?? (visits > 0 ? spend / visits : 0)
      const revenuePerVisit = item.derived?.revenue_per_visit ?? (visits > 0 ? revenue / visits : 0)
      const roles = item.diagnostics?.roles || {}
      const funnel = item.diagnostics?.funnel || {}
      return {
        campaign: campaignId,
        channel,
        campaign_name: item.campaign_name ?? null,
        visits,
        attributed_value: revenue,
        attributed_share: totalRevenue > 0 ? revenue / totalRevenue : 0,
        attributed_conversions: conversions,
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
        spend,
        roi,
        roas,
        cpa,
        suggested_next: suggestionMap[campaignId] ?? null,
        treatment_rate: undefined,
        holdout_rate: undefined,
        uplift_abs: undefined,
        uplift_rel: null,
        treatment_n: undefined,
        holdout_n: undefined,
        confidence: item.confidence || undefined,
        confidence_score: item.confidence?.score,
      } as CampaignData
    })
  }, [summaryQuery.data?.items, suggestionsQuery.data?.items])
  const loading = summaryQuery.isLoading

  const filteredCampaigns = useMemo(() => {
    if (!campaigns.length) return []
    const q = search.trim().toLowerCase()
    const byChannel = channelFilter.trim()
    return campaigns.filter((c) => {
      if (directMode === 'exclude' && c.channel === 'direct') {
        return false
      }
      const matchSearch =
        !q ||
        c.campaign.toLowerCase().includes(q) ||
        c.channel.toLowerCase().includes(q) ||
        (c.campaign_name ?? '').toLowerCase().includes(q)
      const matchChannel = !byChannel || c.channel === byChannel
      return matchSearch && matchChannel
    })
  }, [campaigns, search, channelFilter, directMode])

  const channelsList = useMemo(() => Array.from(new Set(campaigns.map((c) => c.channel))).sort(), [campaigns])
  const latestEventReplayDiagnostics = summaryQuery.data?.readiness?.details?.latest_event_replay?.diagnostics

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
  const activeCampaignLabel = useMemo(() => {
    if (!activeCampaignKey) return 'none'
    const active = filteredCampaigns.find((c) => c.campaign === activeCampaignKey)
    if (!active) return activeCampaignKey
    return active.campaign_name ? `${active.channel} / ${active.campaign_name}` : active.campaign
  }, [activeCampaignKey, filteredCampaigns])
  const activeCampaignStats = useMemo(() => {
    if (!activeCampaignKey) return null
    return filteredCampaigns.find((c) => c.campaign === activeCampaignKey) ?? null
  }, [activeCampaignKey, filteredCampaigns])
  const trendCampaignOptions = useMemo(() => {
    const ranked = [...filteredCampaigns]
      .sort((a, b) => (b.spend || b.attributed_conversions) - (a.spend || a.attributed_conversions))
      .slice(0, 30)
    return ranked.map((c) => c.campaign)
  }, [filteredCampaigns])

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    params.set('kpi', trendKpi)
    params.set('grain', trendGrain)
    params.set('compare', comparePrevious ? '1' : '0')
    window.history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`)
  }, [trendKpi, trendGrain, comparePrevious])

  const campaignTrendMetrics = useMemo(() => {
    const rows = trendQuery.data?.series || []
    const prevRows = trendQuery.data?.series_prev || []
    if (!rows.length) return []
    const campaignFilter =
      selectedTrendCampaign === 'all'
        ? new Set(
            trendCampaignOptions.filter((c) =>
              c.toLowerCase().includes(trendCampaignSearch.trim().toLowerCase()),
            ),
          )
        : new Set([selectedTrendCampaign])

    const byTs = new Map<string, number>()
    rows.forEach((r) => {
      if (!campaignFilter.has(r.campaign_id)) return
      if (typeof r.value !== 'number') return
      byTs.set(r.ts, (byTs.get(r.ts) || 0) + r.value)
    })
    const byTsPrev = new Map<string, number>()
    prevRows.forEach((r) => {
      if (!campaignFilter.has(r.campaign_id)) return
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
  }, [trendQuery.data, trendKpi, selectedTrendCampaign, trendCampaignOptions, trendCampaignSearch])

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const openProviderLink = useCallback(async (campaign: CampaignData) => {
    const provider = providerFromChannel(campaign.channel)
    if (!provider || !campaign.campaign) return
    try {
      const deeplink = await getAdsDeepLink({
        provider,
        accountId: 'default',
        entityType: 'campaign',
        entityId: campaign.campaign,
      })
      window.open(deeplink.url, '_blank', 'noopener,noreferrer')
    } catch {
      // keep table interaction non-blocking
    }
  }, [])

  const openAdsDrawer = useCallback((campaign: CampaignData) => {
    const provider = providerFromChannel(campaign.channel)
    if (!provider || !campaign.campaign) return
    setAdsDrawerContext({
      provider,
      accountId: 'default',
      entityType: 'campaign',
      entityId: campaign.campaign,
      entityName: campaign.campaign_name || campaign.campaign,
      previewMetrics: {
        spend7d: campaign.spend,
        conversions7d: campaign.attributed_conversions,
        revenue7d: campaign.attributed_value,
        roas: campaign.roas,
        cpa: campaign.cpa,
      },
      decisionContext: campaign.suggested_next
        ? {
            source: campaign.suggested_next.is_promoted_policy ? 'deployed_journey_policy' : 'performance_recommendation',
            scope_label: campaign.campaign_name ? `${campaign.channel} / ${campaign.campaign_name}` : campaign.campaign,
            recommended_channel: campaign.suggested_next.channel,
            recommended_campaign: campaign.suggested_next.campaign ?? null,
            conversion_rate: campaign.suggested_next.conversion_rate,
            journey_count: campaign.suggested_next.count,
            avg_value: campaign.suggested_next.avg_value,
            policy_title: campaign.suggested_next.promoted_policy_title ?? null,
            hypothesis_id: campaign.suggested_next.promoted_policy_hypothesis_id ?? null,
            journey_definition_id: campaign.suggested_next.promoted_policy_journey_definition_id ?? null,
          }
        : null,
    })
  }, [])

  const t = tokens

  const journeysKpis = journeysSummary?.kpi_counts ?? {}
  const availableKpis = Object.keys(journeysKpis)
  const primaryKpiId = journeysSummary?.primary_kpi_id ?? null

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

  if (summaryQuery.isError && !campaigns.length) {
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

  if (!campaigns.length) {
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
          {'Load conversion journeys with a "campaign" field on touchpoints (e.g. load sample data), then run attribution models.'}
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

  const summaryCurrent = summaryQuery.data?.totals?.current ?? { spend: 0, visits: 0, revenue: 0, conversions: 0 }
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
  const filteredTotalSpend = filteredCampaigns.reduce((s, c) => s + c.spend, 0)
  const kpis = [
    { label: 'Total Spend', value: formatCurrency(totalSpend), def: METRIC_DEFINITIONS['Total Spend'] },
    { label: 'Visits', value: totalVisits.toLocaleString(), def: METRIC_DEFINITIONS['Visits'] },
    { label: 'Attributed Revenue', value: formatCurrency(totalValue), def: METRIC_DEFINITIONS['Attributed Revenue'] },
    { label: 'Conversions', value: totalConversions.toLocaleString(), def: '' },
    { label: 'CVR', value: `${(totalCVR * 100).toFixed(2)}%`, def: METRIC_DEFINITIONS['CVR'] },
    { label: 'Cost / Visit', value: formatCurrency(totalCostPerVisit), def: METRIC_DEFINITIONS['Cost / Visit'] },
    { label: 'Revenue / Visit', value: formatCurrency(totalRevenuePerVisit), def: METRIC_DEFINITIONS['Revenue / Visit'] },
    { label: 'ROAS', value: `${totalROAS.toFixed(2)}×`, def: '' },
    { label: 'Avg CPA', value: formatCurrency(avgCPA), def: '' },
    { label: 'Net Revenue', value: formatCurrency(Number(summaryOutcomesCurrent.net_revenue || 0)), def: 'Revenue after refunds, cancellations, and invalidation.' },
    { label: 'Net Conversions', value: Number(summaryOutcomesCurrent.net_conversions || 0).toLocaleString(), def: 'Conversions remaining valid after post-conversion adjustments.' },
  ]
  const coverage = summaryQuery.data?.mapping_coverage
  const roleRows = [...sortedCampaigns]
    .sort((a, b) => b.last_touch_revenue + b.assist_revenue + b.first_touch_revenue - (a.last_touch_revenue + a.assist_revenue + a.first_touch_revenue))
    .slice(0, 8)
  const funnelRows = [...sortedCampaigns]
    .sort((a, b) => b.touch_journeys - a.touch_journeys)
    .slice(0, 8)
  const campaignTableColumns: AnalyticsTableColumn<CampaignData>[] = [
    {
      key: 'campaign',
      label: 'Campaign',
      hideable: false,
      sortable: true,
      sortDirection: sortKey === 'campaign' ? sortDir : null,
      onSort: () => handleSort('campaign'),
      title: 'Sort by Campaign',
      render: (campaign) =>
        campaign.campaign_name ? `${campaign.channel} / ${campaign.campaign_name}` : campaign.campaign,
      cellStyle: { fontWeight: t.font.weightMedium, color: t.color.text },
    },
    {
      key: 'channel',
      label: 'Channel',
      sortable: true,
      sortDirection: sortKey === 'channel' ? sortDir : null,
      onSort: () => handleSort('channel'),
      title: 'Sort by Channel',
      render: (campaign) => campaign.channel,
      cellStyle: { color: t.color.textSecondary },
    },
    {
      key: 'visits',
      label: 'Visits',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'visits' ? sortDir : null,
      onSort: () => handleSort('visits'),
      title: 'Sort by Visits',
      render: (campaign) => campaign.visits.toFixed(0),
    },
    {
      key: 'cvr',
      label: 'CVR',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'cvr' ? sortDir : null,
      onSort: () => handleSort('cvr'),
      title: 'Sort by CVR',
      render: (campaign) => `${(campaign.cvr * 100).toFixed(2)}%`,
    },
    {
      key: 'cost_per_visit',
      label: 'Cost / Visit',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'cost_per_visit' ? sortDir : null,
      onSort: () => handleSort('cost_per_visit'),
      title: 'Sort by Cost / Visit',
      render: (campaign) => formatCurrency(campaign.cost_per_visit),
    },
    {
      key: 'revenue_per_visit',
      label: 'Revenue / Visit',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'revenue_per_visit' ? sortDir : null,
      onSort: () => handleSort('revenue_per_visit'),
      title: 'Sort by Revenue / Visit',
      render: (campaign) => formatCurrency(campaign.revenue_per_visit),
    },
    {
      key: 'attributed_value',
      label: 'Attributed Revenue',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'attributed_value' ? sortDir : null,
      onSort: () => handleSort('attributed_value'),
      title: 'Sort by Attributed Revenue',
      render: (campaign) => formatCurrency(campaign.attributed_value),
      cellStyle: { fontWeight: t.font.weightMedium, color: t.color.success },
    },
    {
      key: 'attributed_share',
      label: 'Share',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'attributed_share' ? sortDir : null,
      onSort: () => handleSort('attributed_share'),
      title: 'Sort by Share',
      render: (campaign) => `${(campaign.attributed_share * 100).toFixed(1)}%`,
    },
    {
      key: 'attributed_conversions',
      label: 'Conversions',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'attributed_conversions' ? sortDir : null,
      onSort: () => handleSort('attributed_conversions'),
      title: 'Sort by Conversions',
      render: (campaign) => campaign.attributed_conversions.toFixed(1),
    },
    {
      key: 'spend',
      label: 'Spend',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'spend' ? sortDir : null,
      onSort: () => handleSort('spend'),
      title: 'Sort by Spend',
      render: (campaign) => formatCurrency(campaign.spend),
    },
    {
      key: 'roi',
      label: 'ROI',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'roi' ? sortDir : null,
      onSort: () => handleSort('roi'),
      title: 'Sort by ROI',
      render: (campaign) => (campaign.roi != null ? `${(campaign.roi * 100).toFixed(0)}%` : '—'),
      cellStyle: (campaign) => ({
        color:
          campaign.roi != null && campaign.roi >= 0
            ? t.color.success
            : campaign.roi != null
            ? t.color.danger
            : t.color.textMuted,
      }),
    },
    {
      key: 'roas',
      label: 'ROAS',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'roas' ? sortDir : null,
      onSort: () => handleSort('roas'),
      title: 'Sort by ROAS',
      render: (campaign) => (campaign.roas != null ? `${campaign.roas.toFixed(2)}×` : '—'),
    },
    {
      key: 'cpa',
      label: 'CPA',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'cpa' ? sortDir : null,
      onSort: () => handleSort('cpa'),
      title: 'Sort by CPA',
      render: (campaign) => (campaign.cpa != null ? formatCurrency(campaign.cpa) : '—'),
    },
    {
      key: 'treatment_rate',
      label: 'Conv rate (treated)',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'treatment_rate' ? sortDir : null,
      onSort: () => handleSort('treatment_rate'),
      title: 'Sort by Conv rate (treated)',
      render: (campaign) =>
        campaign.treatment_rate != null ? `${(campaign.treatment_rate * 100).toFixed(1)}%` : '—',
    },
    {
      key: 'holdout_rate',
      label: 'Conv rate (holdout)',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'holdout_rate' ? sortDir : null,
      onSort: () => handleSort('holdout_rate'),
      title: 'Sort by Conv rate (holdout)',
      render: (campaign) =>
        campaign.holdout_rate != null ? `${(campaign.holdout_rate * 100).toFixed(1)}%` : '—',
    },
    {
      key: 'uplift_abs',
      label: 'Uplift',
      align: 'right',
      sortable: true,
      sortDirection: sortKey === 'uplift_abs' ? sortDir : null,
      onSort: () => handleSort('uplift_abs'),
      title: 'Sort by Uplift',
      render: (campaign) => (campaign.uplift_abs != null ? `${(campaign.uplift_abs * 100).toFixed(1)}%` : '—'),
      cellStyle: (campaign) => ({
        color:
          campaign.uplift_abs != null && campaign.uplift_abs > 0
            ? t.color.success
            : campaign.uplift_abs != null && campaign.uplift_abs < 0
            ? t.color.danger
            : t.color.textMuted,
      }),
    },
    {
      key: 'suggested_next',
      label: 'Suggested next',
      title: METRIC_DEFINITIONS['Suggested next'],
      render: (campaign) =>
        campaign.suggested_next ? (
          <div style={{ display: 'inline-flex', gap: t.space.xs, alignItems: 'center', flexWrap: 'wrap' }}>
            <span
              title={`${campaign.suggested_next.count} journeys, ${(campaign.suggested_next.conversion_rate * 100).toFixed(1)}% conversion, avg $${campaign.suggested_next.avg_value}`}
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
              {campaign.suggested_next.campaign != null
                ? `${campaign.suggested_next.channel} / ${campaign.suggested_next.campaign}`
                : campaign.suggested_next.channel}{' '}
              ({(campaign.suggested_next.conversion_rate * 100).toFixed(0)}%)
            </span>
            {campaign.suggested_next.is_promoted_policy ? (
              <>
                <span
                  title={campaign.suggested_next.promoted_policy_title ?? 'Promoted Journey Lab policy'}
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
                  journeyDefinitionId: campaign.suggested_next.promoted_policy_journey_definition_id,
                  hypothesisId: campaign.suggested_next.promoted_policy_hypothesis_id,
                }) ? (
                  <a
                    href={buildJourneyHypothesisHref({
                      journeyDefinitionId: campaign.suggested_next.promoted_policy_journey_definition_id,
                      hypothesisId: campaign.suggested_next.promoted_policy_hypothesis_id,
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
      key: 'actions',
      label: 'Actions',
      render: (campaign) =>
        providerFromChannel(campaign.channel) && campaign.campaign ? (
          <div style={{ display: 'inline-flex', gap: t.space.xs, whiteSpace: 'nowrap' }}>
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation()
                void openProviderLink(campaign)
              }}
              style={{
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.accent,
                borderRadius: t.radius.sm,
                padding: `2px ${t.space.xs}px`,
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
              title="Open this campaign in provider UI"
            >
              Open ↗
            </button>
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation()
                openAdsDrawer(campaign)
              }}
              style={{
                border: `1px solid ${t.color.border}`,
                background: t.color.bg,
                color: t.color.text,
                borderRadius: t.radius.sm,
                padding: `2px ${t.space.xs}px`,
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
              title="Propose pause/enable/budget changes"
            >
              Manage
            </button>
          </div>
        ) : (
          <span style={{ color: t.color.textMuted, fontSize: t.font.sizeXs }} title="Missing provider entity id">
            —
          </span>
        ),
    },
    {
      key: 'confidence',
      label: 'Confidence',
      align: 'right',
      render: (campaign) => <ConfidenceBadge confidence={campaign.confidence} compact />,
    },
  ]

  const chartData = filteredCampaigns.map((c) => ({
    name: c.campaign_name ? `${c.channel} / ${c.campaign_name}` : c.campaign,
    spend: c.spend,
    attributed_value: c.attributed_value,
  }))
  const topCampaignChartData = [...filteredCampaigns]
    .sort((a, b) => b.spend - a.spend || b.attributed_value - a.attributed_value)
    .slice(0, 12)
    .map((c) => ({
      name: c.campaign_name ? `${c.channel} / ${c.campaign_name}` : c.campaign,
      spend: c.spend,
      attributed_value: c.attributed_value,
    }))
  const paretoChartData = (() => {
    const ranked = [...filteredCampaigns]
      .sort((a, b) => b.spend - a.spend || b.attributed_value - a.attributed_value)
      .slice(0, 12)
    const totalSpendBase = ranked.reduce((sum, campaign) => sum + campaign.spend, 0)
    let cumulativeSpend = 0
    return ranked.map((campaign) => {
      cumulativeSpend += campaign.spend
      return {
        name: campaign.campaign_name ? `${campaign.channel} / ${campaign.campaign_name}` : campaign.campaign,
        spend: campaign.spend,
        cumulative_spend_share_pct: totalSpendBase > 0 ? (cumulativeSpend / totalSpendBase) * 100 : 0,
        revenue: campaign.attributed_value,
      }
    })
  })()
  const efficiencyScatterData = filteredCampaigns
    .filter((campaign) => campaign.spend > 0 || campaign.attributed_value > 0)
    .slice(0, 120)
    .map((campaign) => ({
      x: campaign.spend,
      y: campaign.roas ?? 0,
      z: Math.max(campaign.attributed_conversions, 1),
      name: campaign.campaign_name ? `${campaign.channel} / ${campaign.campaign_name}` : campaign.campaign,
      revenue: campaign.attributed_value,
      conversions: campaign.attributed_conversions,
      cpa: campaign.cpa,
      channel: campaign.channel,
    }))

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
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
      {/* Page title + measurement context bar */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.sm,
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
            Campaign Performance
          </h1>
          <p
            style={{
              margin: `${t.space.xs}px 0 0`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Attribution model:{' '}
            <strong style={{ color: t.color.accent }}>{MODEL_LABELS[model] || model}</strong>
            {(summaryQuery.data?.meta?.conversion_key || trendQuery.data?.meta?.conversion_key || summaryQuery.data?.config?.conversion_key) && (
              <>
                {' '}
                · Conversion:{' '}
                <strong>
                  {summaryQuery.data?.meta?.conversion_key || trendQuery.data?.meta?.conversion_key || summaryQuery.data?.config?.conversion_key}
                </strong>
              </>
            )}
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

      {summaryQuery.data?.readiness && (summaryQuery.data.readiness.status === 'blocked' || summaryQuery.data.readiness.warnings.length > 0) ? (
        <DecisionStatusCard
          title="Performance Reliability Warning"
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
          Unified campaign summary is currently unavailable. KPI totals and deltas depend on `/api/performance/campaign/summary`.
        </div>
      )}

      <div style={{ marginBottom: t.space.xl }}>
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center', marginBottom: t.space.sm }}>
          <label style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Trend metric</label>
          <select
            value={trendKpi}
            onChange={(e) => setTrendKpi(e.target.value)}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              fontSize: t.font.sizeSm,
              minWidth: 140,
            }}
          >
            <option value="spend">Spend</option>
            <option value="visits">Visits</option>
            <option value="conversions">Conversions</option>
            <option value="revenue">Revenue</option>
            <option value="cpa">CPA</option>
            <option value="roas">ROAS</option>
          </select>
          <label style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Campaign</label>
          <input
            type="text"
            placeholder="Search campaigns..."
            value={trendCampaignSearch}
            onChange={(e) => setTrendCampaignSearch(e.target.value)}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              fontSize: t.font.sizeSm,
              minWidth: 180,
            }}
          />
          <select
            value={selectedTrendCampaign}
            onChange={(e) => setSelectedTrendCampaign(e.target.value)}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              fontSize: t.font.sizeSm,
              minWidth: 240,
            }}
          >
            <option value="all">All campaigns (aggregated)</option>
            {trendCampaignOptions
              .filter((c) => c.toLowerCase().includes(trendCampaignSearch.trim().toLowerCase()))
              .map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
          </select>
        </div>
        <TrendPanel
          title="Trend"
          subtitle="Daily trend for selected period"
          metrics={campaignTrendMetrics}
          metricKey={trendKpi}
          onMetricKeyChange={setTrendKpi}
          grain={trendGrain}
          onGrainChange={setTrendGrain}
          compare={comparePrevious}
          onCompareChange={setComparePrevious}
          showMetricSelector
          showGrainSelector
          showCompareToggle
          showTableToggle
          infoTooltip="Observed values by date for selected campaign scope. Change vs previous period is shown in summary."
          noDataMessage="No data for selected filters and date range"
        />
      </div>

      {/* Measurement context controls */}
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
        {/* Conversion selector */}
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
          <select
            value={conversionKey}
            onChange={(e) => setConversionKey(e.target.value)}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              fontSize: t.font.sizeSm,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              minWidth: 140,
            }}
          >
            <option value="">All conversions</option>
            {availableKpis.map((k) => (
              <option key={k} value={k}>
                {k}
                {k === primaryKpiId ? ' (primary)' : ''}
              </option>
            ))}
          </select>
        </div>

        {/* Direct handling */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Direct handling
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
              onClick={() => setDirectMode('exclude')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                border: 'none',
                cursor: 'pointer',
                backgroundColor: directMode === 'exclude' ? t.color.accent : 'transparent',
                color: directMode === 'exclude' ? t.color.surface : t.color.textSecondary,
              }}
            >
              Exclude Direct
            </button>
          </div>
        </div>

        {/* Compare vs previous period toggle (UI only for now) */}
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
            checked={comparePrevious}
            onChange={(e) => setComparePrevious(e.target.checked)}
          />
          Compare to previous period
        </label>

        {/* Lookback / eligibility summary */}
        <div
          style={{
            marginLeft: 'auto',
            fontSize: t.font.sizeXs,
            color: t.color.textMuted,
            maxWidth: 360,
          }}
        >
          {summaryQuery.data?.config?.time_window ? (
            <>
              Click lookback:{' '}
              {summaryQuery.data.config.time_window.click_lookback_days ?? '—'}d · Impression lookback:{' '}
              {summaryQuery.data.config.time_window.impression_lookback_days ?? '—'}d · Session timeout:{' '}
              {summaryQuery.data.config.time_window.session_timeout_minutes ?? '—'}min
            </>
          ) : (
            <>Measurement windows not configured for this model.</>
          )}
        </div>
      </div>

      {/* KPI strip + mapping coverage */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
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
              }}
              title={kpi.def}
            >
              {kpi.label}
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
          </div>
        ))}
        <div
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
            }}
          >
            Mapping coverage
          </div>
          <div
            style={{
              marginTop: t.space.xs,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Spend mapped:{' '}
            <strong style={{ color: coverage && coverage.spend_mapped_pct < 95 ? t.color.warning : t.color.success }}>
              {coverage ? `${coverage.spend_mapped_pct.toFixed(1)}%` : '—'}
            </strong>
          </div>
          <div
            style={{
              marginTop: 2,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            Value mapped:{' '}
            <strong style={{ color: coverage && coverage.value_mapped_pct < 95 ? t.color.warning : t.color.success }}>
              {coverage ? `${coverage.value_mapped_pct.toFixed(1)}%` : '—'}
            </strong>
          </div>
          {coverage && (coverage.spend_mapped_pct < 99.5 || coverage.value_mapped_pct < 99.5) && (
            <div
              style={{
                marginTop: t.space.xs,
                fontSize: t.font.sizeXs,
                color: t.color.textMuted,
              }}
            >
              Some spend or conversions are not mapped to campaigns. Check Data Quality to fix taxonomy and UTM mappings.
            </div>
          )}
        </div>
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
          <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Attribution Role Split
          </h3>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.color.border}` }}>
                  <th style={{ textAlign: 'left', padding: `${t.space.sm}px 0` }}>Campaign</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>First</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Assist</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Last</th>
                </tr>
              </thead>
              <tbody>
                {roleRows.map((row) => (
                  <tr key={row.campaign} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: `${t.space.sm}px 0`, fontWeight: t.font.weightMedium }}>
                      {row.campaign_name ? `${row.channel} / ${row.campaign_name}` : row.campaign}
                    </td>
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
                  <th style={{ textAlign: 'left', padding: `${t.space.sm}px 0` }}>Campaign</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Touched</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Content</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Checkout</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Converted</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Conv rate</th>
                </tr>
              </thead>
              <tbody>
                {funnelRows.map((row) => (
                  <tr key={row.campaign} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: `${t.space.sm}px 0`, fontWeight: t.font.weightMedium }}>
                      {row.campaign_name ? `${row.channel} / ${row.campaign_name}` : row.campaign}
                    </td>
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

      {showWhy && (
        <div style={{ marginBottom: t.space.lg }}>
          <ExplainabilityPanel scope="campaign" configId={configId ?? undefined} model={model} />
        </div>
      )}

      {/* Search and filter */}
      <div style={{ marginBottom: t.space.xl }}>
        <AnalyticsToolbar
          searchLabel="Search"
          searchValue={search}
          onSearchChange={setSearch}
          searchPlaceholder="Campaign or channel name..."
          beforeFilters={
            <label style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
              Channel
            </label>
          }
          filters={
            <>
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
            </>
          }
          summary={`Showing ${filteredCampaigns.length} of ${campaigns.length} campaigns`}
          padded
        />
      </div>

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
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
            <BarChart data={topCampaignChartData} layout="vertical" margin={{ left: 8, right: 16 }}>
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
            Spend concentration (Pareto)
          </h3>
          <ResponsiveContainer width="100%" height={320}>
            <ComposedChart data={paretoChartData} margin={{ top: 8, right: 16, left: 8, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis dataKey="name" tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }} interval={0} angle={-18} textAnchor="end" height={72} />
              <YAxis yAxisId="left" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => formatCurrency(v)} />
              <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => `${v}%`} />
              <Tooltip
                contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                formatter={(value: number, key: string) => {
                  if (key === 'cumulative_spend_share_pct') return `${value.toFixed(1)}%`
                  return formatCurrency(value)
                }}
              />
              <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
              <Bar yAxisId="left" dataKey="spend" fill={t.color.chart[0]} name="Spend" radius={[4, 4, 0, 0]} />
              <Line yAxisId="right" type="monotone" dataKey="cumulative_spend_share_pct" stroke={t.color.accent} strokeWidth={2} dot={{ r: 3 }} name="Cumulative spend share" />
            </ComposedChart>
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
            Spend vs. ROAS scatter
          </h3>
          <ResponsiveContainer width="100%" height={320}>
            <ScatterChart margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis
                type="number"
                dataKey="x"
                name="Spend"
                tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }}
                tickFormatter={(v) => formatCurrency(v)}
              />
              <YAxis
                type="number"
                dataKey="y"
                name="ROAS"
                tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }}
                tickFormatter={(v) => `${v.toFixed(1)}×`}
              />
              <ZAxis type="number" dataKey="z" range={[60, 360]} name="Conversions" />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                formatter={(value: number, _key: string, payload: { payload?: typeof efficiencyScatterData[number] }) => {
                  const point = payload?.payload
                  if (!point) return value
                  return [
                    <div key="tooltip" style={{ display: 'grid', gap: 2 }}>
                      <strong>{point.name}</strong>
                      <span>Spend: {formatCurrency(point.x)}</span>
                      <span>Revenue: {formatCurrency(point.revenue)}</span>
                      <span>ROAS: {point.y.toFixed(2)}×</span>
                      <span>Conversions: {point.conversions.toFixed(1)}</span>
                      <span>CPA: {point.cpa != null ? formatCurrency(point.cpa) : '—'}</span>
                    </div>,
                    point.channel,
                  ]
                }}
              />
              <Scatter name="Campaign efficiency" data={efficiencyScatterData} fill={t.color.chart[3]} />
            </ScatterChart>
          </ResponsiveContainer>
          <p style={{ margin: `${t.space.md}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Bubble size reflects attributed conversions. High-spend points below the cluster are the first low-efficiency campaigns to inspect.
          </p>
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
            Campaign selection
          </h3>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Use the Trend section above to inspect KPI movement for all campaigns or one selected campaign.
          </p>
          <p style={{ margin: `${t.space.sm}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Current table selection: {activeCampaignLabel}
          </p>
          <div
            style={{
              marginTop: t.space.md,
              display: 'grid',
              gridTemplateColumns: 'repeat(2, minmax(120px, 1fr))',
              gap: t.space.sm,
            }}
          >
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Spend</div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {activeCampaignStats ? formatCurrency(activeCampaignStats.spend) : '—'}
              </div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Attributed Revenue</div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {activeCampaignStats ? formatCurrency(activeCampaignStats.attributed_value) : '—'}
              </div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Visits / CVR</div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {activeCampaignStats ? `${activeCampaignStats.visits.toFixed(0)} / ${(activeCampaignStats.cvr * 100).toFixed(2)}%` : '—'}
              </div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Conversions</div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {activeCampaignStats ? activeCampaignStats.attributed_conversions.toFixed(1) : '—'}
              </div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>ROAS / CPA</div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {activeCampaignStats
                  ? `${activeCampaignStats.roas != null ? `${activeCampaignStats.roas.toFixed(2)}×` : '—'} / ${activeCampaignStats.cpa != null ? formatCurrency(activeCampaignStats.cpa) : '—'}`
                  : '—'}
              </div>
            </div>
          </div>
        </div>
      </div>

      <SectionCard
        title={`Campaign Detail ${filteredCampaigns.length < campaigns.length ? `(${filteredCampaigns.length} shown)` : ''}`}
        actions={
          <button
            type="button"
            onClick={() =>
              exportCampaignsCSV(filteredCampaigns, {
                conversionKey,
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
        <AnalyticsTable
          columns={campaignTableColumns}
          rows={sortedCampaigns}
          rowKey={(campaign) => campaign.campaign}
          tableLabel="Campaign detail"
          stickyFirstColumn
          virtualized
          virtualizationThreshold={60}
          virtualizationHeight={680}
          virtualRowHeight={52}
          allowColumnHiding
          allowDensityToggle
          persistKey="campaign-detail-table"
          defaultHiddenColumnKeys={['treatment_rate', 'holdout_rate', 'uplift_abs', 'confidence']}
          presets={[
            {
              key: 'overview',
              label: 'Overview',
              visibleColumnKeys: ['campaign', 'channel', 'spend', 'visits', 'attributed_conversions', 'attributed_value', 'roas', 'cpa'],
            },
            {
              key: 'efficiency',
              label: 'Efficiency',
              visibleColumnKeys: ['campaign', 'channel', 'visits', 'cvr', 'cost_per_visit', 'revenue_per_visit', 'spend', 'roi', 'roas', 'cpa'],
            },
            {
              key: 'experiments',
              label: 'Experiments',
              visibleColumnKeys: ['campaign', 'channel', 'attributed_conversions', 'attributed_value', 'treatment_rate', 'holdout_rate', 'uplift_abs', 'suggested_next', 'actions'],
            },
          ]}
          defaultPresetKey="overview"
          onRowClick={(campaign) => setSelectedCampaign(campaign.campaign)}
          isRowActive={(campaign) => activeCampaignKey === campaign.campaign}
          emptyState="No campaigns match the current filters."
        />
      </SectionCard>

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
      {adsDrawerContext && (
        <AdsActionsDrawer
          open={!!adsDrawerContext}
          onClose={() => setAdsDrawerContext(null)}
          provider={adsDrawerContext.provider}
          accountId={adsDrawerContext.accountId}
          entityType={adsDrawerContext.entityType}
          entityId={adsDrawerContext.entityId}
          entityName={adsDrawerContext.entityName}
          previewMetrics={adsDrawerContext.previewMetrics}
          decisionContext={adsDrawerContext.decisionContext}
        />
      )}
    </div>
  )
}
