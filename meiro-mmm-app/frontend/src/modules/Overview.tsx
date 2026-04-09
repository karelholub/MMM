import { useEffect, useMemo, useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import DecisionStatusCard from '../components/DecisionStatusCard'
import RecommendedActionsList, { type RecommendedActionItem } from '../components/RecommendedActionsList'
import WorkspaceAssistantPanel from '../components/WorkspaceAssistantPanel'
import SegmentComparisonContextNote from '../components/segments/SegmentComparisonContextNote'
import SegmentOverlapNotice from '../components/segments/SegmentOverlapNotice'
import { buildSettingsHref } from '../lib/settingsLinks'
import { navigateForRecommendedAction } from '../lib/recommendedActions'
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'
import { buildIncrementalityPlannerHref } from '../lib/experimentLinks'
import {
  listJourneyAlertDefinitions,
  listJourneyAlertEvents,
  type JourneyAlertDefinitionItem,
  type JourneyAlertEventItem,
} from './alerts/api'
import {
  DashboardPage,
  AnalyticsTable,
  type AnalyticsTableColumn,
  KpiTile,
  SectionCard,
  InsightRow,
  KpiTileSkeleton,
  DataHealthCard,
  ContextSummaryStrip,
  AnalysisShareActions,
  AnalysisNarrativePanel,
} from '../components/dashboard'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import {
  isLocalAnalyticalSegment,
  localSegmentCompatibleWithDimensions,
  readLocalSegmentDefinition,
  segmentOptionLabel,
  buildSegmentComparisonHref,
  type SegmentAnalysisResponse,
  type SegmentComparisonResponse,
  type SegmentRegistryResponse,
} from '../lib/segments'

type PageKey =
  | 'overview'
  | 'alerts'
  | 'dashboard'
  | 'roles'
  | 'trust'
  | 'comparison'
  | 'paths'
  | 'campaigns'
  | 'expenses'
  | 'datasources'
  | 'meiro'
  | 'mmm'
  | 'settings'
  | 'dq'
  | 'incrementality'
  | 'path_archetypes'
  | 'datasets'

interface OverviewProps {
  lastPage: PageKey | null
  onNavigate: (page: PageKey) => void
  onConnectDataSources: () => void
}

// --- API types (overview summary + drivers + alerts) ---
interface KpiTileResponse {
  kpi_key: string
  value: number
  delta_abs?: number | null
  delta_pct?: number | null
  current_period?: {
    date_from: string
    date_to: string
    grain?: 'daily' | 'hourly' | string
  }
  previous_period?: {
    date_from: string
    date_to: string
  }
  series?: Array<{ ts: string; value: number | null }>
  series_prev?: Array<{ ts: string; value: number | null }>
  sparkline?: number[]
  confidence?: string
  confidence_score?: number
  confidence_level?: 'high' | 'medium' | 'low' | string
  confidence_reasons?: Array<{
    key: string
    label: string
    score: number
    detail: string
    weight?: number
  }>
}

interface HighlightItem {
  type: 'kpi_delta' | 'alert' | 'consistency_warning'
  kpi_key?: string
  message: string
  delta_pct?: number
  alert_id?: number
  severity?: string
  ts_detected?: string
}

interface FreshnessResponse {
  last_touchpoint_ts?: string | null
  last_conversion_ts?: string | null
  ingest_lag_minutes?: number | null
}

interface OverviewSummaryResponse {
  kpi_tiles: KpiTileResponse[]
  outcomes?: {
    current?: Record<string, number>
    previous?: Record<string, number>
  }
  highlights: HighlightItem[]
  freshness: FreshnessResponse
  readiness?: JourneyReadinessResponse
  attention_queue?: RecommendedActionItem[]
  consistency_warnings?: string[]
  current_period?: {
    date_from: string
    date_to: string
    grain?: 'daily' | 'hourly' | string
  }
  previous_period?: {
    date_from: string
    date_to: string
  }
  date_from: string
  date_to: string
}

interface ChannelDriver {
  channel: string
  spend: number
  visits: number
  conversions: number
  revenue: number
  delta_spend_pct?: number | null
  delta_visits_pct?: number | null
  delta_conversions_pct?: number | null
  delta_revenue_pct?: number | null
  outcomes?: Record<string, number>
}

interface CampaignDriver {
  campaign: string
  revenue: number
  conversions: number
  delta_revenue_pct?: number | null
  outcomes?: Record<string, number>
}

interface OverviewDriversResponse {
  by_channel: ChannelDriver[]
  by_campaign: CampaignDriver[]
  date_from: string
  date_to: string
}

interface OverviewFunnelItem {
  path: string
  path_display: string
  steps: string[]
  conversions: number
  share: number
  revenue: number
  revenue_per_conversion: number
  median_days_to_convert?: number | null
  path_length: number
  ends_with_direct: boolean
}

interface OverviewFunnelsResponse {
  date_from: string
  date_to: string
  summary: {
    total_conversions: number
    net_conversions?: number
    gross_conversions?: number
    net_revenue?: number
    gross_revenue?: number
    click_through_conversions?: number
    view_through_conversions?: number
    mixed_path_conversions?: number
    distinct_paths: number
    top_paths_conversion_share: number
    median_path_length: number
  }
  tabs: {
    conversions: OverviewFunnelItem[]
    revenue: OverviewFunnelItem[]
    speed: OverviewFunnelItem[]
  }
}

interface OverviewTrendRow {
  channel: string
  current_revenue: number
  previous_revenue: number
  delta_revenue: number
  delta_revenue_pct?: number | null
  sparkline: number[]
}

interface OverviewTrendsResponse {
  date_from: string
  date_to: string
  decomposition: {
    current: {
      visits: number
      conversions: number
      revenue: number
      cvr: number
      revenue_per_conversion: number
    }
    previous: {
      visits: number
      conversions: number
      revenue: number
      cvr: number
      revenue_per_conversion: number
    }
    revenue_delta: number
    factors: Array<{ key: string; label: string; value: number }>
  }
  momentum: {
    window_days: number
    rising: OverviewTrendRow[]
    falling: OverviewTrendRow[]
  }
  mix_shift: Array<{
    channel: string
    revenue_share_current: number
    revenue_share_previous: number
    revenue_share_delta_pp: number
    visit_share_current: number
    visit_share_previous: number
    visit_share_delta_pp: number
    conversion_share_current: number
    conversion_share_previous: number
    conversion_share_delta_pp: number
  }>
}

interface OverviewAlertItem {
  id: number
  rule_id: number
  rule_name?: string | null
  ts_detected: string
  severity: string
  title: string | null
  message: string
  status: string
  deep_link: string
}

interface OverviewAlertsResponse {
  alerts: OverviewAlertItem[]
  total: number
}

interface JourneyReadinessResponse {
  status: string
  summary: {
    primary_kpi_coverage: number
    taxonomy_unknown_share: number
    journeys_loaded?: number
    freshness_hours?: number | null
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
  blockers: string[]
  warnings: string[]
  recommended_actions?: RecommendedActionItem[]
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
}

interface JourneyOverviewAlertRow {
  id: string
  domain: 'journeys' | 'funnels'
  status: 'open' | 'pending_eval' | 'disabled'
  severity: string
  title: string
  summary: string
  triggered_at: string | null
}

function medianOf(values: Array<number | null | undefined>): number | null {
  const valid = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  if (!valid.length) return null
  const sorted = [...valid].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle]
}

interface RecentAlertRow {
  id: string
  source: 'legacy' | 'journey_funnel'
  severity: string
  status: string
  title: string
  summary: string
  ts: string | null
  deep_link?: string
  legacy_id?: number
}

// --- Helpers ---
function formatCurrency(val: number): string {
  if (!Number.isFinite(val)) return '—'
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`
  if (val >= 1_000) return `$${(val / 1_000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

function formatKpiValue(kpiKey: string, value: number): string {
  if (kpiKey === 'spend' || kpiKey === 'revenue') return formatCurrency(value)
  return value.toLocaleString()
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

function daysInPeriod(fromIso?: string, toIso?: string): number {
  if (!fromIso || !toIso) return 0
  const from = new Date(fromIso)
  const to = new Date(toIso)
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return 0
  const days = Math.floor((to.getTime() - from.getTime()) / (24 * 60 * 60 * 1000)) + 1
  return Math.max(1, days)
}

// --- Cover Dashboard ---
export default function Overview({ lastPage, onNavigate, onConnectDataSources }: OverviewProps) {
  const {
    globalDateFrom,
    globalDateTo,
    isLoadingSampleJourneys,
    loadSampleJourneys,
    selectedConfigId,
  } = useWorkspaceContext()
  const [funnelTab, setFunnelTab] = useState<'conversions' | 'revenue' | 'speed'>('conversions')
  const [assistantCollapsed, setAssistantCollapsed] = useState(true)
  const [showWorkspaceSignals, setShowWorkspaceSignals] = usePersistentToggle('overview:show-workspace-signals', false)
  const [selectedSegmentId, setSelectedSegmentId] = useState('')
  const [compareSegmentId, setCompareSegmentId] = useState('')

  const dateRange = useMemo(() => {
    const dateFrom = globalDateFrom || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)
    const dateTo = globalDateTo || new Date().toISOString().slice(0, 10)
    return { date_from: dateFrom, date_to: dateTo }
  }, [globalDateFrom, globalDateTo])

  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['segment-registry', 'overview'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load segment registry',
      }),
    refetchInterval: false,
  })

  const localSegments = useMemo(
    () => (segmentRegistryQuery.data?.items ?? []).filter(isLocalAnalyticalSegment),
    [segmentRegistryQuery.data?.items],
  )
  const compatibleSegments = useMemo(
    () => localSegments.filter((item) => localSegmentCompatibleWithDimensions(item, ['channel_group'])),
    [localSegments],
  )
  const selectedSegment = useMemo(
    () => localSegments.find((item) => item.id === selectedSegmentId) ?? null,
    [localSegments, selectedSegmentId],
  )
  const compareSegment = useMemo(
    () => localSegments.find((item) => item.id === compareSegmentId) ?? null,
    [localSegments, compareSegmentId],
  )
  const selectedSegmentDefinition = useMemo(
    () => readLocalSegmentDefinition(selectedSegment),
    [selectedSegment],
  )
  const selectedSegmentAutoCompatible = useMemo(
    () => localSegmentCompatibleWithDimensions(selectedSegment, ['channel_group']),
    [selectedSegment],
  )

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const segment = params.get('segment')
    if (segment) setSelectedSegmentId(segment)
    const compareSegment = params.get('compare_segment')
    if (compareSegment) setCompareSegmentId(compareSegment)
  }, [])

  useEffect(() => {
    if (!selectedSegmentId) return
    if (!segmentRegistryQuery.data) return
    if (localSegments.some((item) => item.id === selectedSegmentId)) return
    setSelectedSegmentId('')
  }, [localSegments, selectedSegmentId, segmentRegistryQuery.data])

  useEffect(() => {
    if (!compareSegmentId) return
    if (localSegments.some((item) => item.id === compareSegmentId && item.id !== selectedSegmentId)) return
    setCompareSegmentId('')
  }, [compareSegmentId, localSegments, selectedSegmentId])

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    if (selectedSegmentId) params.set('segment', selectedSegmentId)
    else params.delete('segment')
    if (compareSegmentId) params.set('compare_segment', compareSegmentId)
    else params.delete('compare_segment')
    window.history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`)
  }, [compareSegmentId, selectedSegmentId])

  const summaryQuery = useQuery<OverviewSummaryResponse>({
    queryKey: ['overview-summary', dateRange.date_from, dateRange.date_to, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      return apiGetJson<OverviewSummaryResponse>(withQuery('/api/overview/summary', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
        channel_group: selectedSegmentAutoCompatible ? selectedSegmentDefinition.channel_group : undefined,
      }), { fallbackMessage: 'Failed to load overview summary' })
    },
  })
  const baselineSummaryQuery = useQuery<OverviewSummaryResponse>({
    queryKey: ['overview-summary', dateRange.date_from, dateRange.date_to, 'all'],
    queryFn: async () => {
      return apiGetJson<OverviewSummaryResponse>(withQuery('/api/overview/summary', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
      }), { fallbackMessage: 'Failed to load workspace overview baseline' })
    },
    enabled: Boolean(selectedSegmentDefinition.channel_group) && selectedSegmentAutoCompatible,
  })

  const driversQuery = useQuery<OverviewDriversResponse>({
    queryKey: ['overview-drivers', dateRange.date_from, dateRange.date_to, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      return apiGetJson<OverviewDriversResponse>(withQuery('/api/overview/drivers', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
        channel_group: selectedSegmentAutoCompatible ? selectedSegmentDefinition.channel_group : undefined,
      }), { fallbackMessage: 'Failed to load drivers' })
    },
  })

  const alertsQuery = useQuery<OverviewAlertsResponse>({
    queryKey: ['overview-alerts', 'open'],
    queryFn: async () => {
      return apiGetJson<OverviewAlertsResponse>(withQuery('/api/overview/alerts', { status: 'open', limit: 20 }), {
        fallbackMessage: 'Failed to load alerts',
      })
    },
  })

  const funnelsQuery = useQuery<OverviewFunnelsResponse>({
    queryKey: ['overview-funnels', dateRange.date_from, dateRange.date_to, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      return apiGetJson<OverviewFunnelsResponse>(withQuery('/api/overview/funnels', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
        limit: 5,
        channel_group: selectedSegmentAutoCompatible ? selectedSegmentDefinition.channel_group : undefined,
      }), { fallbackMessage: 'Failed to load top funnels' })
    },
  })
  const baselineFunnelsQuery = useQuery<OverviewFunnelsResponse>({
    queryKey: ['overview-funnels', dateRange.date_from, dateRange.date_to, 'all'],
    queryFn: async () => {
      return apiGetJson<OverviewFunnelsResponse>(withQuery('/api/overview/funnels', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
        limit: 5,
      }), { fallbackMessage: 'Failed to load workspace funnel baseline' })
    },
    enabled: Boolean(selectedSegmentDefinition.channel_group) && selectedSegmentAutoCompatible,
  })

  const trendsQuery = useQuery<OverviewTrendsResponse>({
    queryKey: ['overview-trends', dateRange.date_from, dateRange.date_to, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      return apiGetJson<OverviewTrendsResponse>(withQuery('/api/overview/trends', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
        channel_group: selectedSegmentAutoCompatible ? selectedSegmentDefinition.channel_group : undefined,
      }), { fallbackMessage: 'Failed to load trend insights' })
    },
  })
  const segmentAnalysisQuery = useQuery<SegmentAnalysisResponse>({
    queryKey: ['overview-segment-analysis', selectedSegment?.id || 'none', dateRange.date_from, dateRange.date_to],
    queryFn: async () =>
      apiGetJson<SegmentAnalysisResponse>(
        withQuery(`/api/segments/local/${selectedSegment?.id}/analysis`, {
          date_from: dateRange.date_from,
          date_to: dateRange.date_to,
        }),
        { fallbackMessage: 'Failed to load segment audience analysis' },
      ),
    enabled: Boolean(selectedSegment),
  })
  const segmentCompareQuery = useQuery<SegmentComparisonResponse>({
    queryKey: ['overview-segment-compare', selectedSegment?.id || 'none', compareSegment?.id || 'none'],
    queryFn: async () =>
      apiGetJson<SegmentComparisonResponse>(
        `/api/segments/local/${selectedSegment?.id}/compare?other_segment_id=${encodeURIComponent(compareSegment?.id || '')}`,
        { fallbackMessage: 'Failed to compare saved analytical audiences' },
      ),
    enabled: Boolean(selectedSegment?.id && compareSegment?.id && selectedSegment?.id !== compareSegment?.id),
  })

  const journeyFunnelAlertsQuery = useQuery<{ defs: JourneyAlertDefinitionItem[]; events: JourneyAlertEventItem[] }>({
    queryKey: ['overview-journey-funnel-alerts'],
    queryFn: async () => {
      const [defsJourneys, defsFunnels, eventsJourneys, eventsFunnels] = await Promise.all([
        listJourneyAlertDefinitions('journeys', { page: 1, perPage: 100 }),
        listJourneyAlertDefinitions('funnels', { page: 1, perPage: 100 }),
        listJourneyAlertEvents('journeys', { page: 1, perPage: 200 }),
        listJourneyAlertEvents('funnels', { page: 1, perPage: 200 }),
      ])
      return {
        defs: [...(defsJourneys.items ?? []), ...(defsFunnels.items ?? [])],
        events: [...(eventsJourneys.items ?? []), ...(eventsFunnels.items ?? [])],
      }
    },
  })

  const updateAlertStatusMutation = useMutation({
    mutationFn: async ({ alertId, status, snooze_until }: { alertId: number; status: string; snooze_until?: string }) => {
      return apiSendJson<any>(`/api/overview/alerts/${alertId}/status`, 'POST', { status, snooze_until }, {
        fallbackMessage: 'Failed to update alert',
      })
    },
    onSuccess: () => {
      alertsQuery.refetch()
      summaryQuery.refetch()
    },
  })

  const summary = summaryQuery.data
  const drivers = driversQuery.data
  const openAlerts = alertsQuery.data?.alerts ?? []
  const journeyFunnelRows = useMemo<JourneyOverviewAlertRow[]>(() => {
    const defs = journeyFunnelAlertsQuery.data?.defs ?? []
    const events = journeyFunnelAlertsQuery.data?.events ?? []
    return defs
      .map((def) => {
        const latest = events
          .filter((ev) => ev.alert_definition_id === def.id)
          .sort((a, b) => new Date(b.triggered_at || 0).getTime() - new Date(a.triggered_at || 0).getTime())[0]
        const status: JourneyOverviewAlertRow['status'] = !def.is_enabled
          ? 'disabled'
          : latest
            ? 'open'
            : 'pending_eval'
        return {
          id: def.id,
          domain: def.domain,
          status,
          severity: latest?.severity ?? 'info',
          title: def.name,
          summary: latest?.summary ?? 'Definition created. Waiting for evaluation run.',
          triggered_at: latest?.triggered_at ?? def.updated_at ?? def.created_at ?? null,
        }
      })
      .sort((a, b) => new Date(b.triggered_at || 0).getTime() - new Date(a.triggered_at || 0).getTime())
  }, [journeyFunnelAlertsQuery.data?.defs, journeyFunnelAlertsQuery.data?.events])
  const needsAttentionCount = journeyFunnelRows.filter((a) => a.status !== 'disabled' && (a.severity === 'critical' || a.severity === 'warn')).length
  const recentAlerts = useMemo<RecentAlertRow[]>(() => {
    const legacyRows: RecentAlertRow[] = openAlerts.map((a) => ({
      id: `legacy-${a.id}`,
      source: 'legacy',
      severity: a.severity || 'info',
      status: a.status || 'open',
      title: a.title || a.rule_name || 'Alert',
      summary: a.message || a.title || 'Alert triggered',
      ts: a.ts_detected || null,
      deep_link: a.deep_link,
      legacy_id: a.id,
    }))
    const jfRows: RecentAlertRow[] = journeyFunnelRows
      .filter((row) => row.status === 'open' || row.status === 'pending_eval')
      .map((row) => ({
        id: `jf-${row.id}`,
        source: 'journey_funnel',
        severity: row.severity,
        status: row.status,
        title: row.title,
        summary: row.summary,
        ts: row.triggered_at,
      }))
    return [...legacyRows, ...jfRows]
      .sort((a, b) => new Date(b.ts || 0).getTime() - new Date(a.ts || 0).getTime())
      .slice(0, 12)
  }, [journeyFunnelRows, openAlerts])
  const kpiTiles = summary?.kpi_tiles ?? []
  const highlights = summary?.highlights ?? []
  const byChannel = drivers?.by_channel ?? []
  const byCampaign = drivers?.by_campaign ?? []
  const funnelSummary = funnelsQuery.data?.summary
  const funnelRows = funnelsQuery.data?.tabs?.[funnelTab] ?? []
  const baselineSummary = baselineSummaryQuery.data
  const baselineFunnels = baselineFunnelsQuery.data
  const selectedFunnel = funnelRows[0] ?? null
  const funnelMedianLag = useMemo(
    () => medianOf(funnelRows.map((row) => row.median_days_to_convert)),
    [funnelRows],
  )
  const baselineMedianLag = useMemo(
    () => medianOf((baselineFunnels?.tabs?.[funnelTab] ?? []).map((row) => row.median_days_to_convert)),
    [baselineFunnels?.tabs, funnelTab],
  )
  const freshness = summary?.freshness
  const trendInsights = trendsQuery.data
  const journeysReadiness = summary?.readiness
  const latestEventReplayDiagnostics = journeysReadiness?.details?.latest_event_replay?.diagnostics
  const overviewAttentionQueue = summary?.attention_queue ?? journeysReadiness?.recommended_actions ?? []
  const overviewPeriodLabel =
    summary?.current_period?.date_from && summary?.current_period?.date_to
      ? `${summary.current_period.date_from} – ${summary.current_period.date_to}`
      : 'Current workspace period'
  const freshnessLabel =
    freshness?.ingest_lag_minutes != null
      ? `${Math.round(Number(freshness.ingest_lag_minutes || 0))} min lag`
      : 'Freshness unavailable'
  const readinessCoverageLabel =
    journeysReadiness?.summary?.primary_kpi_coverage != null
      ? `${(Number(journeysReadiness.summary.primary_kpi_coverage || 0) * 100).toFixed(1)}% KPI coverage`
      : 'Coverage unavailable'
  const journeysLoadedLabel =
    journeysReadiness?.summary?.journeys_loaded != null
      ? Number(journeysReadiness.summary.journeys_loaded).toLocaleString()
      : '—'
  const lagPostureLabel =
    funnelMedianLag == null
      ? 'Lag unavailable'
      : funnelMedianLag <= 1
        ? `Short-lag (${funnelMedianLag.toFixed(1)}d median)`
        : funnelMedianLag <= 3
          ? `Typical (${funnelMedianLag.toFixed(1)}d median)`
          : `Long-lag (${funnelMedianLag.toFixed(1)}d median)`
  const focusSegmentLabel = selectedSegment ? selectedSegment.name : 'All journeys'
  const selectedTileMap = useMemo(
    () => Object.fromEntries(kpiTiles.map((tile) => [tile.kpi_key, tile])) as Record<string, KpiTileResponse>,
    [kpiTiles],
  )
  const baselineTileMap = useMemo(
    () => Object.fromEntries((baselineSummary?.kpi_tiles ?? []).map((tile) => [tile.kpi_key, tile])) as Record<string, KpiTileResponse>,
    [baselineSummary?.kpi_tiles],
  )
  const segmentComparison = useMemo(() => {
    if (!selectedSegment || !baselineSummary) return null
    const readBaseline = (key: string) => Number(baselineTileMap[key]?.value ?? 0)
    if (!selectedSegmentAutoCompatible) {
      const summary = segmentAnalysisQuery.data?.summary
      if (!summary) return null
      const workspaceConversions = readBaseline('conversions')
      const workspaceRevenue = readBaseline('revenue')
      const segmentConversions = Number(summary.conversions ?? 0)
      const segmentRevenue = Number(summary.revenue ?? 0)
      return {
        shares: [
          { label: 'Journey share', value: summary.share_of_rows ?? null },
          { label: 'Conversion share', value: workspaceConversions > 0 ? segmentConversions / workspaceConversions : null },
          { label: 'Revenue share', value: workspaceRevenue > 0 ? segmentRevenue / workspaceRevenue : null },
        ],
        rates: [
          { label: 'Median lag', segment: summary.median_lag_days ?? null, baseline: baselineMedianLag, delta: summary.median_lag_days != null && baselineMedianLag != null ? summary.median_lag_days - baselineMedianLag : null, percent: false, suffix: 'd' },
          { label: 'Average path length', segment: summary.avg_path_length ?? null, baseline: null, delta: null, percent: false, suffix: 'x' },
        ],
      }
    }
    const readValue = (key: string) => Number(selectedTileMap[key]?.value ?? 0)
    const segmentSpend = readValue('spend')
    const segmentVisits = readValue('visits')
    const segmentConversions = readValue('conversions')
    const segmentRevenue = readValue('revenue')
    const workspaceSpend = readBaseline('spend')
    const workspaceVisits = readBaseline('visits')
    const workspaceConversions = readBaseline('conversions')
    const workspaceRevenue = readBaseline('revenue')
    const segmentCvr = segmentVisits > 0 ? segmentConversions / segmentVisits : null
    const workspaceCvr = workspaceVisits > 0 ? workspaceConversions / workspaceVisits : null
    const segmentRevenuePerVisit = segmentVisits > 0 ? segmentRevenue / segmentVisits : null
    const workspaceRevenuePerVisit = workspaceVisits > 0 ? workspaceRevenue / workspaceVisits : null
    const segmentRevenuePerConversion = segmentConversions > 0 ? segmentRevenue / segmentConversions : null
    const workspaceRevenuePerConversion = workspaceConversions > 0 ? workspaceRevenue / workspaceConversions : null
    const topPathShare = Number(funnelSummary?.top_paths_conversion_share ?? 0)
    const workspaceTopPathShare = Number(baselineFunnels?.summary?.top_paths_conversion_share ?? 0)
    return {
      shares: [
        { label: 'Spend share', value: workspaceSpend > 0 ? segmentSpend / workspaceSpend : null },
        { label: 'Visit share', value: workspaceVisits > 0 ? segmentVisits / workspaceVisits : null },
        { label: 'Conversion share', value: workspaceConversions > 0 ? segmentConversions / workspaceConversions : null },
        { label: 'Revenue share', value: workspaceRevenue > 0 ? segmentRevenue / workspaceRevenue : null },
      ],
      rates: [
        { label: 'CVR', segment: segmentCvr, baseline: workspaceCvr, delta: segmentCvr != null && workspaceCvr != null ? segmentCvr - workspaceCvr : null, percent: true },
        { label: 'Revenue / visit', segment: segmentRevenuePerVisit, baseline: workspaceRevenuePerVisit, delta: segmentRevenuePerVisit != null && workspaceRevenuePerVisit != null ? segmentRevenuePerVisit - workspaceRevenuePerVisit : null, percent: false },
        { label: 'Revenue / conversion', segment: segmentRevenuePerConversion, baseline: workspaceRevenuePerConversion, delta: segmentRevenuePerConversion != null && workspaceRevenuePerConversion != null ? segmentRevenuePerConversion - workspaceRevenuePerConversion : null, percent: false },
        { label: 'Median lag', segment: funnelMedianLag, baseline: baselineMedianLag, delta: funnelMedianLag != null && baselineMedianLag != null ? funnelMedianLag - baselineMedianLag : null, percent: false, suffix: 'd' },
        { label: 'Top-path concentration', segment: topPathShare, baseline: workspaceTopPathShare, delta: topPathShare - workspaceTopPathShare, percent: true },
      ],
    }
  }, [
    selectedSegmentDefinition.channel_group,
    baselineSummary,
    baselineFunnels?.summary?.top_paths_conversion_share,
    baselineMedianLag,
    baselineTileMap,
    funnelMedianLag,
    funnelSummary?.top_paths_conversion_share,
    selectedTileMap,
    selectedSegment,
    selectedSegmentAutoCompatible,
    segmentAnalysisQuery.data?.summary,
  ])
  const handleOverviewAction = (action: RecommendedActionItem) => {
    navigateForRecommendedAction(action, { onNavigate, defaultPage: 'datasources' })
  }

  const hasAnyData =
    kpiTiles.some((k) => typeof k.value === 'number' && Number.isFinite(k.value) && k.value !== 0) ||
    byChannel.length > 0 ||
    recentAlerts.length > 0 ||
    highlights.length > 0
  const isEmpty = !summaryQuery.isLoading && !summaryQuery.error && !hasAnyData
  const periodDays = daysInPeriod(summary?.current_period?.date_from, summary?.current_period?.date_to)
  const baselineLabel = periodDays > 0 ? `vs previous ${periodDays} ${periodDays === 1 ? 'day' : 'days'}` : 'vs previous period'
  const tileOrder: Array<KpiTileResponse['kpi_key']> = ['spend', 'visits', 'conversions', 'revenue']
  const orderedKpiTiles = tileOrder
    .map((key) => kpiTiles.find((k) => k.kpi_key === key))
    .filter((k): k is KpiTileResponse => Boolean(k))
  const overviewNarrative = useMemo(() => {
    const biggestTileDelta = [...orderedKpiTiles]
      .filter((tile) => tile.delta_pct != null && Number.isFinite(tile.delta_pct))
      .sort((a, b) => Math.abs(Number(b.delta_pct || 0)) - Math.abs(Number(a.delta_pct || 0)))[0]
    const strongestMomentum = trendInsights?.momentum?.rising?.[0] ?? null
    const weakestMomentum = trendInsights?.momentum?.falling?.[0] ?? null
    const lagDelta = funnelMedianLag != null && baselineMedianLag != null ? funnelMedianLag - baselineMedianLag : null
    const tileLabel = biggestTileDelta
      ? biggestTileDelta.kpi_key === 'revenue'
        ? 'Revenue'
        : biggestTileDelta.kpi_key === 'conversions'
          ? 'Conversions'
          : biggestTileDelta.kpi_key === 'visits'
            ? 'Visits'
            : 'Spend'
      : null
    const journeyShare = segmentComparison?.shares.find((item) => item.label === 'Journey share')?.value ?? null
    const conversionShare = segmentComparison?.shares.find((item) => item.label === 'Conversion share')?.value ?? null
    const cvrRate = segmentComparison?.rates.find((item) => item.label === 'CVR')?.segment ?? null
    const medianLagRate = segmentComparison?.rates.find((item) => item.label === 'Median lag')?.segment ?? null
    const headline =
      biggestTileDelta?.delta_pct != null && tileLabel
        ? `${tileLabel} ${biggestTileDelta.delta_pct >= 0 ? 'rose' : 'fell'} ${Math.abs(biggestTileDelta.delta_pct).toFixed(1)}% ${baselineLabel}.`
        : 'This period is loaded, but no strong period-over-period movement stands out yet.'
    const items = [
      highlights[0]?.message ? `Top signal: ${highlights[0].message}` : null,
      strongestMomentum
        ? `${strongestMomentum.channel} is the strongest rising revenue driver with ${formatCurrency(strongestMomentum.delta_revenue)} change vs the previous period.`
        : null,
      weakestMomentum
        ? `${weakestMomentum.channel} is the strongest falling revenue driver with ${formatCurrency(Math.abs(weakestMomentum.delta_revenue))} reversal vs the previous period.`
        : null,
      lagDelta != null
        ? `Median time to convert is ${lagDelta >= 0 ? 'slower' : 'faster'} by ${Math.abs(lagDelta).toFixed(1)} days than the workspace baseline.`
        : funnelMedianLag != null
          ? `Median time to convert is ${funnelMedianLag.toFixed(1)} days in the current visible slice.`
          : null,
      selectedSegment && segmentComparison && selectedSegmentAutoCompatible
        ? `${selectedSegment.name} contributes ${formatPercent(conversionShare)} of visible conversions and runs at ${formatPercent(cvrRate)} CVR.`
        : null,
      selectedSegment && segmentComparison && !selectedSegmentAutoCompatible && segmentAnalysisQuery.data?.summary
        ? `${selectedSegment.name} matches ${formatPercent(journeyShare)} of visible journey rows and ${formatPercent(conversionShare)} of visible conversions, with ${medianLagRate != null ? `${medianLagRate.toFixed(1)}d` : 'unknown'} median lag.`
        : null,
      selectedSegment && compareSegment && segmentCompareQuery.data
        ? `${selectedSegment.name} vs ${compareSegment.name}: ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity. Revenue delta is ${segmentCompareQuery.data.deltas.revenue == null ? '—' : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : ''}${formatCurrency(Math.abs(segmentCompareQuery.data.deltas.revenue))}`}.`
        : null,
    ].filter((item): item is string => Boolean(item))
    return { headline, items }
  }, [
    baselineLabel,
    baselineMedianLag,
    funnelMedianLag,
    highlights,
    orderedKpiTiles,
    compareSegment,
    segmentCompareQuery.data,
    segmentComparison,
    selectedSegment,
    selectedSegmentAutoCompatible,
    segmentAnalysisQuery.data?.summary,
    trendInsights?.momentum?.falling,
    trendInsights?.momentum?.rising,
  ])
  const channelColumns: AnalyticsTableColumn<(typeof byChannel)[number]>[] = [
    {
      key: 'channel',
      label: 'Channel',
      hideable: false,
      render: (row) => row.channel,
      cellStyle: { fontWeight: t.font.weightMedium },
    },
    {
      key: 'spend',
      label: 'Spend',
      align: 'right',
      render: (row) => formatCurrency(row.spend),
    },
    {
      key: 'visits',
      label: 'Visits',
      align: 'right',
      render: (row) => row.visits.toLocaleString(),
    },
    {
      key: 'conversions',
      label: 'Conversions',
      align: 'right',
      render: (row) => row.conversions.toLocaleString(),
    },
    {
      key: 'revenue',
      label: 'Revenue',
      align: 'right',
      render: (row) => formatCurrency(row.revenue),
    },
    {
      key: 'delta',
      label: 'Δ%',
      align: 'right',
      render: (row) =>
        row.delta_revenue_pct != null
          ? `${row.delta_revenue_pct >= 0 ? '▲' : '▼'} ${Math.abs(row.delta_revenue_pct).toFixed(1)}%`
          : '—',
    },
  ]
  const campaignColumns: AnalyticsTableColumn<(typeof byCampaign)[number]>[] = [
    {
      key: 'campaign',
      label: 'Campaign',
      hideable: false,
      render: (row) => row.campaign,
      cellStyle: { fontWeight: t.font.weightMedium },
    },
    {
      key: 'revenue',
      label: 'Revenue',
      align: 'right',
      render: (row) => formatCurrency(row.revenue),
    },
    {
      key: 'delta',
      label: 'Δ%',
      align: 'right',
      render: (row) =>
        row.delta_revenue_pct != null
          ? `${row.delta_revenue_pct >= 0 ? '▲' : '▼'} ${Math.abs(row.delta_revenue_pct).toFixed(1)}%`
          : '—',
    },
  ]
  const mixShiftRows = trendInsights?.mix_shift ?? []
  const mixShiftColumns: AnalyticsTableColumn<(typeof mixShiftRows)[number]>[] = [
    {
      key: 'channel',
      label: 'Channel',
      hideable: false,
      render: (row) => row.channel,
      cellStyle: { fontWeight: t.font.weightMedium },
    },
    {
      key: 'revenue_share_delta_pp',
      label: 'Revenue share Δ',
      align: 'right',
      render: (row) => `${row.revenue_share_delta_pp >= 0 ? '+' : ''}${row.revenue_share_delta_pp.toFixed(2)} pp`,
      cellStyle: (row) => ({ color: row.revenue_share_delta_pp >= 0 ? t.color.success : t.color.danger }),
    },
    {
      key: 'visit_share_delta_pp',
      label: 'Visit share Δ',
      align: 'right',
      render: (row) => `${row.visit_share_delta_pp >= 0 ? '+' : ''}${row.visit_share_delta_pp.toFixed(2)} pp`,
      cellStyle: (row) => ({ color: row.visit_share_delta_pp >= 0 ? t.color.success : t.color.danger }),
    },
    {
      key: 'conversion_share_delta_pp',
      label: 'Conv share Δ',
      align: 'right',
      render: (row) => `${row.conversion_share_delta_pp >= 0 ? '+' : ''}${row.conversion_share_delta_pp.toFixed(2)} pp`,
      cellStyle: (row) => ({ color: row.conversion_share_delta_pp >= 0 ? t.color.success : t.color.danger }),
    },
  ]

  function trendLabelFor(tile: KpiTileResponse): string {
    const grain = tile.current_period?.grain ?? summary?.current_period?.grain ?? 'daily'
    return grain === 'hourly' ? 'Hourly trend (selected period)' : 'Daily trend (selected period)'
  }

  function confidenceFor(tile: KpiTileResponse) {
    const score = tile.confidence_score
    const level = tile.confidence_level || tile.confidence || 'low'
    if (score == null) return null
    return {
      score,
      level,
      reasons: tile.confidence_reasons || [],
    }
  }

  function infoTooltipFor(tile: KpiTileResponse): string {
    const grain = tile.current_period?.grain ?? summary?.current_period?.grain ?? 'daily'
    return [
      `Sparkline: observed ${tile.kpi_key} values (${grain}) for the selected period.`,
      `Comparison: ${baselineLabel}.`,
      'Caveats: conversions and revenue reflect observed conversion paths; spend reflects imported expenses and currency conversion settings.',
    ].join('\n')
  }

  function tileDrilldown(key: string): { label: string; go: () => void } {
    const pushTrendQuery = (kpi: string) => {
      const params = new URLSearchParams(window.location.search)
      params.set('kpi', kpi)
      params.set('grain', 'auto')
      params.set('compare', '1')
      window.history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`)
      onNavigate('dashboard')
    }
    if (key === 'spend') return { label: 'Click to open Channel performance', go: () => pushTrendQuery('spend') }
    if (key === 'visits') return { label: 'Click to open Channel performance', go: () => pushTrendQuery('visits') }
    if (key === 'conversions') return { label: 'Click to open Channel performance', go: () => pushTrendQuery('conversions') }
    return { label: 'Click to open Channel performance', go: () => pushTrendQuery('revenue') }
  }

  const isLoading = summaryQuery.isLoading || driversQuery.isLoading
  const isError = summaryQuery.isError || driversQuery.isError
  const errorMessage =
    (summaryQuery.error as Error)?.message ?? (driversQuery.error as Error)?.message ?? null

  const headerActions = (
    <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
      <AnalysisShareActions
        fileStem="cover-dashboard"
        summaryTitle="Cover Dashboard brief"
        summaryLines={[
          `Period: ${overviewPeriodLabel}`,
          `Focus: ${focusSegmentLabel}`,
          `Freshness: ${freshnessLabel}`,
          `KPI coverage: ${readinessCoverageLabel}`,
          `Journeys loaded: ${journeysLoadedLabel}`,
          `Lag posture: ${lagPostureLabel}`,
          `Highlights: ${highlights.slice(0, 3).map((item) => item.message).join(' · ') || 'No major highlights in the current slice.'}`,
        ]}
      />
      {lastPage && lastPage !== 'overview' && (
        <button
          type="button"
          onClick={() => onNavigate(lastPage)}
          style={{
            padding: `${t.space.sm}px ${t.space.md}px`,
            borderRadius: t.radius.sm,
            border: `1px solid ${t.color.border}`,
            background: t.color.surface,
            fontSize: t.font.sizeSm,
            cursor: 'pointer',
          }}
        >
          Back
        </button>
      )}
    </div>
  )

  // --- Empty state ---
  const emptyState = (
    <div
      style={{
        maxWidth: 720,
        margin: '0 auto',
        background: t.color.surface,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.border}`,
        boxShadow: t.shadow,
        padding: t.space.xxl,
        textAlign: 'center',
        display: 'grid',
        gap: t.space.lg,
      }}
    >
      <div>
        <h2
          style={{
            margin: 0,
            fontSize: t.font.size2xl,
            fontWeight: t.font.weightSemibold,
            color: t.color.text,
            letterSpacing: '-0.02em',
          }}
        >
          Cover Dashboard
        </h2>
        <p style={{ margin: `${t.space.sm}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Connect data sources and load journeys to see KPIs, drivers, and alerts here.
        </p>
      </div>
      {latestEventReplayDiagnostics ? (
        <div style={{ display: 'grid', gap: t.space.sm, textAlign: 'left', border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.lg, background: t.color.bgSubtle }}>
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
      <div style={{ display: 'flex', justifyContent: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
        <button
          type="button"
          onClick={() => loadSampleJourneys()}
          disabled={isLoadingSampleJourneys}
          style={{
            padding: `${t.space.md}px ${t.space.xl}px`,
            fontSize: t.font.sizeBase,
            fontWeight: t.font.weightSemibold,
            backgroundColor: t.color.success,
            color: '#ffffff',
            border: 'none',
            borderRadius: t.radius.sm,
            cursor: isLoadingSampleJourneys ? 'wait' : 'pointer',
          }}
        >
          {isLoadingSampleJourneys ? 'Loading sample…' : 'Load sample data'}
        </button>
        <button
          type="button"
          onClick={onConnectDataSources}
          style={{
            padding: `${t.space.md}px ${t.space.xl}px`,
            fontSize: t.font.sizeBase,
            fontWeight: t.font.weightSemibold,
            backgroundColor: '#fd7e14',
            color: '#ffffff',
            border: 'none',
            borderRadius: t.radius.sm,
            cursor: 'pointer',
          }}
        >
          Connect data sources
        </button>
      </div>
    </div>
  )

  // --- Loading skeleton ---
  const loadingState = (
    <div style={{ display: 'grid', gap: t.space.xl }}>
      <div
        style={{
          display: 'grid',
          gap: t.space.xl,
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
        }}
      >
        {[1, 2, 3, 4, 5, 6].map((i) => (
          <KpiTileSkeleton key={i} />
        ))}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: t.space.xl }}>
        <SectionCard title="What changed" subtitle="Top insights">
          <div style={{ display: 'grid', gap: t.space.sm }}>
            {[1, 2, 3].map((i) => (
              <div key={i} style={{ height: 56, background: t.color.borderLight, borderRadius: t.radius.md }} />
            ))}
          </div>
        </SectionCard>
        <SectionCard title="Recent alerts">
          <div style={{ display: 'grid', gap: t.space.sm }}>
            {[1, 2].map((i) => (
              <div key={i} style={{ height: 48, background: t.color.borderLight, borderRadius: t.radius.md }} />
            ))}
          </div>
        </SectionCard>
      </div>
    </div>
  )

  return (
    <DashboardPage
      title="Cover Dashboard"
      description="What happened, why it happened, and what to check next. Period, source, model, and config are controlled in the workspace header."
      dateRange={null}
      filters={null}
      actions={lastPage && lastPage !== 'overview' ? headerActions : null}
      isLoading={isLoading && !isEmpty}
      loadingState={loadingState}
      isError={isError}
      errorMessage={errorMessage}
      isEmpty={isEmpty}
      emptyState={emptyState}
    >
      <div style={{ display: 'grid', gap: t.space.xl }}>
        {overviewAttentionQueue.length ? (
          <SectionCard
            title="Workspace Assistant"
            subtitle={
              assistantCollapsed
                ? `Hidden by default. ${overviewAttentionQueue.length} ranked next ${overviewAttentionQueue.length === 1 ? 'step' : 'steps'} available.`
                : 'Ranked next steps across taxonomy, KPI, journeys, Meiro, and data sources.'
            }
            actions={
              <button
                type="button"
                onClick={() => setAssistantCollapsed((current) => !current)}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: assistantCollapsed ? t.color.surface : t.color.bg,
                  fontSize: t.font.sizeXs,
                  fontWeight: t.font.weightMedium,
                  cursor: 'pointer',
                }}
              >
                {assistantCollapsed ? 'Show assistant' : 'Hide assistant'}
              </button>
            }
          >
            {assistantCollapsed ? (
              <div
                style={{
                  display: 'grid',
                  gap: t.space.sm,
                  padding: `${t.space.sm}px 0`,
                }}
              >
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  The assistant is still available, but collapsed so the KPI and performance sections stay primary.
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                  Top pending item: <strong>{overviewAttentionQueue[0]?.label ?? 'Recommended follow-up available'}</strong>
                </div>
              </div>
            ) : (
              <WorkspaceAssistantPanel
                actions={overviewAttentionQueue}
                onActionClick={handleOverviewAction}
              />
            )}
          </SectionCard>
        ) : null}

        <SectionCard
          title="Analysis focus"
          subtitle="This page currently supports saved local analytical segments that define only channel group, so the summary, drivers, funnels, and trends remain internally consistent."
        >
          <div
            style={{
              display: 'flex',
              gap: t.space.md,
              flexWrap: 'wrap',
              alignItems: 'flex-end',
            }}
          >
            <div style={{ display: 'grid', gap: 6, minWidth: 260, flex: '1 1 260px' }}>
              <span style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
                Focus segment
              </span>
              <select
                value={selectedSegmentId}
                onChange={(event) => setSelectedSegmentId(event.target.value)}
                style={{
                  minWidth: 0,
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  color: t.color.text,
                  fontSize: t.font.sizeSm,
                }}
              >
                <option value="">All journeys / no saved segment</option>
                {localSegments.map((segment) => (
                  <option key={segment.id} value={segment.id}>
                    {segmentOptionLabel(segment)}
                  </option>
                ))}
              </select>
            </div>
            {selectedSegment ? (
              <div style={{ display: 'grid', gap: 6, minWidth: 260, flex: '1 1 260px' }}>
                <span style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
                  Compare with
                </span>
                <select
                  value={compareSegmentId}
                  onChange={(event) => setCompareSegmentId(event.target.value)}
                  style={{
                    minWidth: 0,
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    fontSize: t.font.sizeSm,
                  }}
                >
                  <option value="">No paired comparison</option>
                  {localSegments
                    .filter((segment) => segment.id !== selectedSegment.id)
                    .map((segment) => (
                      <option key={segment.id} value={segment.id}>
                        {segmentOptionLabel(segment)}
                      </option>
                    ))}
                </select>
              </div>
            ) : null}
            <div style={{ display: 'grid', gap: 6, minWidth: 200, flex: '1 1 200px' }}>
              <span style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
                Applied filter
              </span>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                {selectedSegmentDefinition.channel_group
                  ? `channel_group=${selectedSegmentDefinition.channel_group}`
                  : selectedSegment
                  ? 'Advanced analytical audience lens active'
                  : 'No saved segment filter applied'}
              </div>
            </div>
            <div style={{ marginLeft: 'auto', display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              <a
                href={buildSettingsHref('segments')}
                style={{
                  alignSelf: 'center',
                  color: t.color.accent,
                  textDecoration: 'none',
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                }}
              >
                Manage segments
              </a>
            </div>
          </div>
        </SectionCard>

        <SegmentOverlapNotice selectedSegment={selectedSegment} />

        {segmentComparison ? (
          <SectionCard
            title="Segment vs workspace baseline"
            subtitle={`How ${selectedSegment?.name || 'this focus segment'} compares with the unfiltered workspace for the same period.`}
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              <SegmentComparisonContextNote
                mode={selectedSegmentAutoCompatible ? 'exact_filter' : 'analytical_lens'}
                pageLabel="overview metrics"
                basisLabel="matched journey-instance rows"
                primaryLabel={selectedSegment?.name || 'Selected audience'}
                primaryRows={segmentAnalysisQuery.data?.summary.journey_rows}
                baselineRows={segmentAnalysisQuery.data?.baseline_summary.journey_rows}
              />
              <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
                {segmentComparison.shares.map((item) => (
                  <div
                    key={item.label}
                    style={{
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      padding: t.space.md,
                      background: t.color.bgSubtle,
                      display: 'grid',
                      gap: 4,
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                    <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                      {formatPercent(item.value, 1)}
                    </div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      Portion of workspace total captured by the current focus segment
                    </div>
                  </div>
                ))}
              </div>

              <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
                {segmentComparison.rates.map((item) => {
                  const deltaPositive = (item.delta ?? 0) >= 0
                  const renderValue = (value: number | null, usePercent: boolean, suffix?: string) => {
                    if (value == null || !Number.isFinite(value)) return '—'
                    if (usePercent) return formatPercent(value, 2)
                    if (suffix === 'd') return `${value.toFixed(1)}d`
                    if (suffix === 'x') return `${value.toFixed(1)} steps`
                    return formatCurrency(value)
                  }
                  const renderDelta = () => {
                    if (item.delta == null || !Number.isFinite(item.delta)) return '—'
                    if (item.percent) return `${deltaPositive ? '+' : ''}${(item.delta * 100).toFixed(2)} pp`
                    if (item.suffix === 'd') return `${deltaPositive ? '+' : ''}${item.delta.toFixed(1)}d`
                    if (item.suffix === 'x') return `${deltaPositive ? '+' : ''}${item.delta.toFixed(1)} steps`
                    return `${deltaPositive ? '+' : ''}${formatCurrency(item.delta)}`
                  }
                  return (
                    <div
                      key={item.label}
                      style={{
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.md,
                        padding: t.space.md,
                        background: t.color.surface,
                        display: 'grid',
                        gap: 6,
                      }}
                    >
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        Segment <strong style={{ color: t.color.text }}>{renderValue(item.segment, item.percent, item.suffix)}</strong> · workspace{' '}
                        <strong style={{ color: t.color.text }}>{renderValue(item.baseline, item.percent, item.suffix)}</strong>
                      </div>
                      <div
                        style={{
                          fontSize: t.font.sizeSm,
                          fontWeight: t.font.weightSemibold,
                          color: deltaPositive ? t.color.success : t.color.danger,
                        }}
                      >
                        Δ {renderDelta()}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          </SectionCard>
        ) : null}

        {selectedSegment && compareSegment && segmentCompareQuery.data ? (
          <SectionCard
            title="Segment vs segment"
            subtitle={`How ${selectedSegment.name} compares directly with ${compareSegment.name} in the same period.`}
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Relationship</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity · {segmentCompareQuery.data.overlap.overlap_rows.toLocaleString()} shared rows
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{selectedSegment.name}</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {(segmentCompareQuery.data.primary_summary.journey_rows ?? 0).toLocaleString()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    rows · median lag {segmentCompareQuery.data.primary_summary.median_lag_days != null ? `${segmentCompareQuery.data.primary_summary.median_lag_days}d` : '—'}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{compareSegment.name}</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {(segmentCompareQuery.data.other_summary.journey_rows ?? 0).toLocaleString()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    rows · median lag {segmentCompareQuery.data.other_summary.median_lag_days != null ? `${segmentCompareQuery.data.other_summary.median_lag_days}d` : '—'}
                  </div>
                </div>
              </div>
              <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
                {[
                  {
                    label: 'Revenue delta',
                    value:
                      segmentCompareQuery.data.deltas.revenue == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : '-'}${formatCurrency(Math.abs(segmentCompareQuery.data.deltas.revenue))}`,
                  },
                  {
                    label: 'Conversion delta',
                    value:
                      segmentCompareQuery.data.deltas.conversions == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.conversions >= 0 ? '+' : '-'}${Math.abs(segmentCompareQuery.data.deltas.conversions).toLocaleString()}`,
                  },
                  {
                    label: 'Median lag delta',
                    value:
                      segmentCompareQuery.data.deltas.median_lag_days == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.median_lag_days >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.median_lag_days}d`,
                  },
                  {
                    label: 'Avg. path length delta',
                    value:
                      segmentCompareQuery.data.deltas.avg_path_length == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.avg_path_length >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.avg_path_length.toFixed(1)} steps`,
                  },
                ].map((item) => (
                  <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface, display: 'grid', gap: 4 }}>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                    <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.value}</div>
                  </div>
                ))}
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <a
                  href={buildIncrementalityPlannerHref({
                    conversionKey: selectedTileMap.conversions?.kpi_key || 'conversions',
                    startAt: dateRange.date_from,
                    endAt: dateRange.date_to,
                    segmentId: selectedSegment.id,
                    name: `Audience test: ${selectedSegment.name} vs ${compareSegment.name}`,
                    notes: `Compare ${selectedSegment.name} against ${compareSegment.name}. Relationship ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity. Revenue delta ${segmentCompareQuery.data.deltas.revenue == null ? 'n/a' : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : '-'}${Math.abs(segmentCompareQuery.data.deltas.revenue).toLocaleString()}`}.`,
                  })}
                  style={{
                    border: `1px solid ${t.color.accent}`,
                    background: t.color.accent,
                    color: '#fff',
                    borderRadius: t.radius.sm,
                    padding: '8px 12px',
                    fontSize: t.font.sizeSm,
                    textDecoration: 'none',
                  }}
                >
                  Draft experiment
                </a>
                <a
                  href={buildSegmentComparisonHref(selectedSegment.id, compareSegment.id)}
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    padding: '8px 12px',
                    fontSize: t.font.sizeSm,
                    textDecoration: 'none',
                  }}
                >
                  Open segment compare
                </a>
              </div>
            </div>
          </SectionCard>
        ) : null}

        <ContextSummaryStrip
          items={[
            { label: 'Source', value: 'Workspace summary + journey health' },
            { label: 'Period', value: overviewPeriodLabel },
            {
              label: 'Config basis',
              value: selectedConfigId ? `Workspace facts · selected config ${selectedConfigId.slice(0, 8)}… not applied` : 'Workspace facts',
            },
            { label: 'Focus', value: focusSegmentLabel },
            { label: 'Freshness', value: freshnessLabel },
            { label: 'KPI coverage', value: readinessCoverageLabel },
            { label: 'Journeys loaded', value: journeysLoadedLabel },
            { label: 'Lag posture', value: lagPostureLabel },
          ]}
        />
        {selectedConfigId ? (
          <div style={{ marginTop: -t.space.md, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Overview remains a workspace-fact view. Use live attribution pages to inspect the currently selected model config directly.
          </div>
        ) : null}

        <AnalysisNarrativePanel
          title="What changed"
          subtitle="A short readout of the current period before you move into the detailed tiles and tables."
          headline={overviewNarrative.headline}
          items={overviewNarrative.items}
        />

        <SectionCard
          title="Conversion lag signal"
          subtitle="How quickly the current top paths tend to convert, and how exposed they are to tighter attribution windows."
        >
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.md }}>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Median lag across top paths</div>
              <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {funnelMedianLag != null ? `${funnelMedianLag.toFixed(1)}d` : '—'}
              </div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Fastest highlighted path</div>
              <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {funnelRows.length
                  ? `${Math.min(...funnelRows.map((row) => row.median_days_to_convert ?? Number.POSITIVE_INFINITY)).toFixed(1)}d`
                  : '—'}
              </div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Window-sensitivity read</div>
              <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.text }}>
                {funnelMedianLag == null
                  ? 'Not enough timing data in the current top paths.'
                  : funnelMedianLag > 3
                    ? 'This period leans long-lag. Tightening attribution windows will likely suppress more paths than usual.'
                    : funnelMedianLag > 1
                      ? 'This period looks mixed. Review channel and path lag before changing windows.'
                      : 'This period leans short-lag. Attribution windows are less likely to materially distort the top paths.'}
              </div>
            </div>
          </div>
          <div style={{ marginTop: t.space.md, display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={() => onNavigate('comparison')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.text,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Open Attribution Comparison
            </button>
            <button
              type="button"
              onClick={() => onNavigate('trust')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.text,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Open Attribution Trust
            </button>
            <button
              type="button"
              onClick={() => onNavigate('paths')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.accent}`,
                background: t.color.accentMuted,
                color: t.color.accent,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                cursor: 'pointer',
              }}
            >
              Open Conversion Paths
            </button>
            <button
              type="button"
              onClick={() => onNavigate('campaigns')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.text,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Open Campaign Performance
            </button>
            <button
              type="button"
              onClick={() => onNavigate('roles')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.text,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Open Attribution Roles
            </button>
            <button
              type="button"
              onClick={() => onNavigate('incrementality')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.text,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Open Incrementality
            </button>
          </div>
        </SectionCard>

        {(journeysReadiness || latestEventReplayDiagnostics) ? (
          <CollapsiblePanel
            title="Workspace Signals"
            subtitle="Canonical readiness, KPI coverage, journeys freshness, and latest raw-event replay diagnostics."
            open={showWorkspaceSignals}
            onToggle={() => setShowWorkspaceSignals((current) => !current)}
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              {journeysReadiness ? (
                <DecisionStatusCard
                  title="Readiness"
                  status={journeysReadiness.status}
                  blockers={journeysReadiness.blockers}
                  warnings={journeysReadiness.warnings.slice(0, 3)}
                  compact
                />
              ) : null}
              {latestEventReplayDiagnostics ? (
                <div
                  style={{
                    display: 'grid',
                    gap: t.space.sm,
                    textAlign: 'left',
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.lg,
                    background: t.color.bgSubtle,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    Latest raw-event replay diagnosis
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Events {Number(latestEventReplayDiagnostics.events_loaded || 0).toLocaleString()} · reconstructed profiles {Number(latestEventReplayDiagnostics.profiles_reconstructed || 0).toLocaleString()} · touchpoints {Number(latestEventReplayDiagnostics.touchpoints_reconstructed || 0).toLocaleString()} · conversions {Number(latestEventReplayDiagnostics.conversions_reconstructed || 0).toLocaleString()} · attributable profiles {Number(latestEventReplayDiagnostics.attributable_profiles || 0).toLocaleString()} · persisted journeys {Number(latestEventReplayDiagnostics.journeys_persisted || 0).toLocaleString()}
                  </div>
                  {!!latestEventReplayDiagnostics.warnings?.length ? (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                      {latestEventReplayDiagnostics.warnings.join(' · ')}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          </CollapsiblePanel>
        ) : null}

        {/* B) KPI Tiles row */}
        <div
          style={{
            display: 'grid',
            gap: t.space.xl,
            gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
            maxWidth: '100%',
            alignItems: 'stretch',
          }}
        >
          {orderedKpiTiles.map((kpi) => {
            const deepLink = tileDrilldown(kpi.kpi_key)
            return (
              <KpiTile
                key={kpi.kpi_key}
                label={kpi.kpi_key.charAt(0).toUpperCase() + kpi.kpi_key.slice(1)}
                value={formatKpiValue(kpi.kpi_key, kpi.value)}
                deltaPct={kpi.delta_pct ?? null}
                deltaLabel={baselineLabel}
                trendLabel={trendLabelFor(kpi)}
                series={kpi.series?.length ? kpi.series : undefined}
                seriesPrev={kpi.series_prev?.length ? kpi.series_prev : undefined}
                confidence={confidenceFor(kpi) ?? undefined}
                infoTooltip={infoTooltipFor(kpi)}
                onClick={deepLink.go}
                drilldownLabel={deepLink.label}
                formatTooltipValue={(val) => formatKpiValue(kpi.kpi_key, val)}
              />
            )
          })}
        </div>

        {/* C) What changed feed */}
        <SectionCard
          title="Alerts & Movers"
          subtitle="Top insights from alerts and largest KPI movers"
          actions={
            <button
              type="button"
              onClick={() => onNavigate('alerts')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
            >
              View all alerts →
            </button>
          }
        >
          <div style={{ display: 'grid', gap: t.space.sm }}>
            {highlights.length === 0 && (
              <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              No significant changes or alerts in this period. Insights will appear here when KPIs move or rules fire.
              </span>
            )}
            {highlights.slice(0, 8).map((h, idx) => (
              <InsightRow
                key={h.type === 'alert' ? `alert-${h.alert_id}` : `kpi-${idx}-${h.kpi_key}`}
                message={h.message}
                severity={h.severity ?? (h.delta_pct != null && h.delta_pct > 0 ? 'info' : 'warning')}
                timestamp={h.ts_detected ?? undefined}
                link={
                  h.type === 'alert' && h.alert_id
                    ? { label: 'Open in Alerts', href: '#' }
                    : undefined
                }
                onLinkClick={h.type === 'alert' ? () => onNavigate('alerts') : undefined}
              />
            ))}
          </div>
        </SectionCard>

        {/* D) Drivers: top channels + optional campaigns */}
        <div style={{ display: 'grid', gap: t.space.xl, gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))' }}>
          <SectionCard
            title="Top channels"
            subtitle="Spend, visits, conversions, revenue and period-over-period delta"
            overflow="auto"
            actions={
              <button
                type="button"
                onClick={() => onNavigate('dashboard')}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  fontSize: t.font.sizeXs,
                  cursor: 'pointer',
                }}
              >
                Channel performance →
              </button>
            }
          >
            <AnalyticsTable
              columns={channelColumns}
              rows={byChannel.slice(0, 10)}
              rowKey={(row) => row.channel}
              tableLabel="Top channels"
              minWidth={760}
              stickyFirstColumn
              allowColumnHiding
              allowDensityToggle
              persistKey="overview-top-channels"
              defaultHiddenColumnKeys={['spend']}
              presets={[
                {
                  key: 'overview',
                  label: 'Overview',
                  visibleColumnKeys: ['channel', 'visits', 'conversions', 'revenue', 'delta'],
                },
                {
                  key: 'efficiency',
                  label: 'Spend',
                  visibleColumnKeys: ['channel', 'spend', 'revenue', 'delta'],
                },
              ]}
              defaultPresetKey="overview"
              emptyState="No channel data for this period."
            />
          </SectionCard>

          <SectionCard
            title="Top campaigns"
            subtitle="By revenue (optional)"
            overflow="auto"
            actions={
              <button
                type="button"
                onClick={() => onNavigate('campaigns')}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  fontSize: t.font.sizeXs,
                  cursor: 'pointer',
                }}
              >
                Campaign performance →
              </button>
            }
          >
            <AnalyticsTable
              columns={campaignColumns}
              rows={byCampaign.slice(0, 5)}
              rowKey={(row) => row.campaign}
              tableLabel="Top campaigns"
              minWidth={520}
              stickyFirstColumn
              allowColumnHiding
              allowDensityToggle
              persistKey="overview-top-campaigns"
              presets={[
                {
                  key: 'overview',
                  label: 'Overview',
                  visibleColumnKeys: ['campaign', 'revenue', 'delta'],
                },
              ]}
              defaultPresetKey="overview"
              emptyState="No campaign data."
            />
          </SectionCard>
        </div>

        <SectionCard
          title="Top converting funnels"
          subtitle="Channel-normalized paths ranked by conversions, value, and speed"
          actions={
            <button
              type="button"
              onClick={() => onNavigate('paths')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
            >
              View all funnels →
            </button>
          }
        >
        <div style={{ display: 'grid', gap: t.space.lg }}>
          <div
            style={{
              display: 'grid',
                gap: t.space.md,
                gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
              }}
            >
              <div
                style={{
                  padding: `${t.space.md}px ${t.space.lg}px`,
                  borderRadius: t.radius.md,
                  background: t.color.bg,
                  border: `1px solid ${t.color.borderLight}`,
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  Concentration
                </div>
                <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {funnelSummary ? `${(funnelSummary.top_paths_conversion_share * 100).toFixed(0)}%` : '—'}
                </div>
                <div style={{ marginTop: 2, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Top 5 paths drive this share of all conversions
                </div>
              </div>
              <div
                style={{
                  padding: `${t.space.md}px ${t.space.lg}px`,
                  borderRadius: t.radius.md,
                  background: t.color.bg,
                  border: `1px solid ${t.color.borderLight}`,
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  Typical Path Depth
                </div>
                <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {funnelSummary ? `${funnelSummary.median_path_length.toFixed(1)} touches` : '—'}
                </div>
                <div style={{ marginTop: 2, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Weighted median path length across converted journeys
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              {[
                ['conversions', 'Top converting paths'],
                ['revenue', 'Top revenue paths'],
                ['speed', 'Fastest paths'],
              ].map(([key, label]) => (
                <button
                  key={key}
                  type="button"
                  onClick={() => setFunnelTab(key as 'conversions' | 'revenue' | 'speed')}
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderRadius: t.radius.full,
                    border: `1px solid ${funnelTab === key ? t.color.accent : t.color.border}`,
                    background: funnelTab === key ? t.color.accentMuted : t.color.surface,
                    color: funnelTab === key ? t.color.accent : t.color.textSecondary,
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  {label}
                </button>
              ))}
            </div>

            <div
              style={{
                display: 'grid',
                gap: t.space.lg,
                gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
              }}
            >
              <div style={{ display: 'grid', gap: t.space.sm }}>
                {funnelsQuery.isError && (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                    {(funnelsQuery.error as Error)?.message ?? 'Failed to load funnel insights.'}
                  </div>
                )}
                {!funnelsQuery.isError && funnelRows.length === 0 && (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    No converted funnel paths were found in this period.
                  </div>
                )}
                {funnelRows.map((row, index) => (
                  <div
                    key={`${funnelTab}-${row.path}`}
                    style={{
                      display: 'grid',
                      gap: t.space.xs,
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      borderRadius: t.radius.md,
                      background: t.color.bg,
                      border: `1px solid ${t.color.borderLight}`,
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap' }}>
                      <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', minWidth: 0 }}>
                        <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, minWidth: 18 }}>#{index + 1}</span>
                        <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, wordBreak: 'break-word' }}>
                          {row.path_display}
                        </span>
                      </div>
                      <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                        {row.ends_with_direct && (
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.warning, background: t.color.warningSubtle, padding: '2px 8px', borderRadius: t.radius.full }}>
                            Direct closer
                          </span>
                        )}
                        {index === 0 && funnelTab === 'revenue' && (
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.success, background: t.color.successMuted, padding: '2px 8px', borderRadius: t.radius.full }}>
                            Highest value
                          </span>
                        )}
                        {index === 0 && funnelTab === 'speed' && (
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.accent, background: t.color.accentMuted, padding: '2px 8px', borderRadius: t.radius.full }}>
                            Fastest
                          </span>
                        )}
                      </div>
                    </div>
                    <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      <span>{row.conversions.toLocaleString()} conv</span>
                      <span>{(row.share * 100).toFixed(1)}% share</span>
                      <span>{formatCurrency(row.revenue)}</span>
                      <span>{formatCurrency(row.revenue_per_conversion)} / conv</span>
                      <span>{row.median_days_to_convert != null ? `${row.median_days_to_convert.toFixed(1)}d median` : 'Latency n/a'}</span>
                    </div>
                  </div>
                ))}
              </div>

              <div
                style={{
                  padding: `${t.space.lg}px`,
                  borderRadius: t.radius.md,
                  background: t.color.bg,
                  border: `1px solid ${t.color.borderLight}`,
                  alignSelf: 'start',
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  Path Snapshot
                </div>
                <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {selectedFunnel?.path_display ?? 'No funnel selected'}
                </div>
                <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.sm }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                    <span style={{ color: t.color.textSecondary }}>Conversions</span>
                    <strong>{selectedFunnel ? selectedFunnel.conversions.toLocaleString() : '—'}</strong>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                    <span style={{ color: t.color.textSecondary }}>Revenue</span>
                    <strong>{selectedFunnel ? formatCurrency(selectedFunnel.revenue) : '—'}</strong>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                    <span style={{ color: t.color.textSecondary }}>Revenue / conversion</span>
                    <strong>{selectedFunnel ? formatCurrency(selectedFunnel.revenue_per_conversion) : '—'}</strong>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                    <span style={{ color: t.color.textSecondary }}>Median time to convert</span>
                    <strong>{selectedFunnel?.median_days_to_convert != null ? `${selectedFunnel.median_days_to_convert.toFixed(1)} days` : '—'}</strong>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                    <span style={{ color: t.color.textSecondary }}>Path length</span>
                    <strong>{selectedFunnel ? `${selectedFunnel.path_length} steps` : '—'}</strong>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() => onNavigate('paths')}
                  style={{
                    marginTop: t.space.lg,
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.accent}`,
                    background: t.color.accentMuted,
                    color: t.color.accent,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  Open Conversion Paths
                </button>
              </div>
            </div>
          </div>
        </SectionCard>

        <div style={{ display: 'grid', gap: t.space.xl, gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))' }}>
          <SectionCard
            title="Trend decomposition"
            subtitle="Why revenue moved vs the previous period"
          >
            {trendsQuery.isError ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                {(trendsQuery.error as Error)?.message ?? 'Failed to load decomposition.'}
              </div>
            ) : (
              <div style={{ display: 'grid', gap: t.space.md }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Revenue delta: <strong style={{ color: t.color.text }}>{formatCurrency(trendInsights?.decomposition.revenue_delta ?? 0)}</strong>
                </div>
                {(trendInsights?.decomposition.factors ?? []).map((factor) => (
                  <div key={factor.key} style={{ display: 'grid', gap: 4 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                      <span style={{ color: t.color.textSecondary }}>{factor.label}</span>
                      <strong style={{ color: factor.value >= 0 ? t.color.success : t.color.danger }}>
                        {factor.value >= 0 ? '+' : '-'}{formatCurrency(Math.abs(factor.value))}
                      </strong>
                    </div>
                    <div style={{ height: 8, borderRadius: t.radius.full, background: t.color.borderLight, overflow: 'hidden' }}>
                      <div
                        style={{
                          width: `${Math.min(100, Math.abs(factor.value) / Math.max(Math.abs(trendInsights?.decomposition.revenue_delta ?? 0), 1) * 100)}%`,
                          height: '100%',
                          background: factor.value >= 0 ? t.color.success : t.color.danger,
                        }}
                      />
                    </div>
                  </div>
                ))}
                <div style={{ display: 'grid', gap: 2, paddingTop: t.space.sm, borderTop: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <span>Visits: {(trendInsights?.decomposition.current.visits ?? 0).toLocaleString()} vs {(trendInsights?.decomposition.previous.visits ?? 0).toLocaleString()}</span>
                  <span>CVR: {((trendInsights?.decomposition.current.cvr ?? 0) * 100).toFixed(2)}% vs {((trendInsights?.decomposition.previous.cvr ?? 0) * 100).toFixed(2)}%</span>
                  <span>Revenue / conv: {formatCurrency(trendInsights?.decomposition.current.revenue_per_conversion ?? 0)} vs {formatCurrency(trendInsights?.decomposition.previous.revenue_per_conversion ?? 0)}</span>
                </div>
              </div>
            )}
          </SectionCard>

          <SectionCard
            title="Channel momentum"
            subtitle={trendInsights ? `Recent ${trendInsights.momentum.window_days}-day movement` : 'Recent movement'}
          >
            {trendsQuery.isError ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                {(trendsQuery.error as Error)?.message ?? 'Failed to load momentum.'}
              </div>
            ) : (
              <div style={{ display: 'grid', gap: t.space.lg }}>
                {([
                  ['Rising', trendInsights?.momentum.rising ?? [], t.color.success],
                  ['Falling', trendInsights?.momentum.falling ?? [], t.color.danger],
                ] as Array<[string, OverviewTrendRow[], string]>).map(([label, rows, color]) => (
                  <div key={label as string} style={{ display: 'grid', gap: t.space.sm }}>
                    <div style={{ fontSize: t.font.sizeXs, color, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label as string}</div>
                    {(rows as OverviewTrendRow[]).slice(0, 3).map((row) => (
                      <div key={`${label}-${row.channel}`} style={{ display: 'grid', gap: 4, padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.md, background: t.color.bg, border: `1px solid ${t.color.borderLight}` }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, fontSize: t.font.sizeSm }}>
                          <strong style={{ color: t.color.text }}>{row.channel}</strong>
                          <span style={{ color }}>{row.delta_revenue >= 0 ? '+' : '-'}{formatCurrency(Math.abs(row.delta_revenue))}</span>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          {row.delta_revenue_pct != null ? `${row.delta_revenue_pct >= 0 ? '▲' : '▼'} ${Math.abs(row.delta_revenue_pct).toFixed(1)}%` : 'No prior baseline'}
                        </div>
                        <div style={{ display: 'flex', gap: 2, alignItems: 'flex-end', height: 24 }}>
                          {(() => {
                            const maxVal = Math.max(...row.sparkline, 1)
                            return row.sparkline.map((value, idx) => (
                              <span
                                key={idx}
                                style={{
                                  flex: 1,
                                  height: `${Math.max(12, value / maxVal * 100)}%`,
                                  borderRadius: 2,
                                  background: color as string,
                                  opacity: 0.75,
                                }}
                              />
                            ))
                          })()}
                        </div>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            )}
          </SectionCard>

          <SectionCard
            title="Channel mix shift"
            subtitle="Share movement in visits, conversions, and revenue"
            overflow="auto"
          >
            {trendsQuery.isError ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                {(trendsQuery.error as Error)?.message ?? 'Failed to load mix shift.'}
              </div>
            ) : (
              <AnalyticsTable
                columns={mixShiftColumns}
                rows={mixShiftRows}
                rowKey={(row) => row.channel}
                tableLabel="Channel mix shift"
                minWidth={640}
                stickyFirstColumn
                allowColumnHiding
                allowDensityToggle
                persistKey="overview-mix-shift"
                presets={[
                  {
                    key: 'all',
                    label: 'All',
                    visibleColumnKeys: ['channel', 'revenue_share_delta_pp', 'visit_share_delta_pp', 'conversion_share_delta_pp'],
                  },
                  {
                    key: 'revenue',
                    label: 'Revenue',
                    visibleColumnKeys: ['channel', 'revenue_share_delta_pp'],
                  },
                ]}
                defaultPresetKey="all"
                emptyState="No mix shift data for this period."
              />
            )}
          </SectionCard>
        </div>

        {/* E) Data health + F) Recent alerts side by side */}
        <div style={{ display: 'grid', gap: t.space.xl, gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
          <SectionCard
            title="Journey/Funnel alerts"
            subtitle={`Needs attention: ${needsAttentionCount}`}
            actions={
              <button
                type="button"
                onClick={() => onNavigate('alerts')}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  fontSize: t.font.sizeXs,
                  cursor: 'pointer',
                }}
              >
                Open alerts →
              </button>
            }
          >
            <div style={{ display: 'grid', gap: t.space.sm }}>
              {journeyFunnelAlertsQuery.isError && (
                <span style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                  {(journeyFunnelAlertsQuery.error as Error)?.message ?? 'Failed to load Journey/Funnel alerts.'}
                </span>
              )}
              {!journeyFunnelAlertsQuery.isError && journeyFunnelRows.length === 0 && (
                <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  No recent Journeys/Funnels alerts.
                </span>
              )}
              {journeyFunnelRows.slice(0, 6).map((alert) => (
                <div
                  key={alert.id}
                  style={{
                    display: 'grid',
                    gap: 2,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.sm,
                    background: t.color.bg,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                    {alert.domain.toUpperCase()} · {alert.status === 'pending_eval' ? 'PENDING EVALUATION' : (alert.severity || 'info').toUpperCase()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{alert.title}</div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{alert.summary}</div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{alert.triggered_at ? new Date(alert.triggered_at).toLocaleString() : '—'}</div>
                </div>
              ))}
            </div>
          </SectionCard>
          <DataHealthCard
            freshness={freshness ?? undefined}
            onOpenDataSources={onConnectDataSources}
            onOpenDataQuality={() => onNavigate('dq')}
          />
          <SectionCard
            title="Recent alerts"
            subtitle="Open alerts with ack / snooze"
            actions={
              <button
                type="button"
                onClick={() => onNavigate('alerts')}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  fontSize: t.font.sizeXs,
                  cursor: 'pointer',
                }}
              >
                Alerts timeline →
              </button>
            }
          >
            <div style={{ display: 'grid', gap: t.space.sm }}>
              {alertsQuery.isError && (
                <span style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                  {(alertsQuery.error as Error)?.message ?? 'Failed to load alerts.'}
                </span>
              )}
              {!alertsQuery.isError && recentAlerts.length === 0 && (
                <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  No recent alerts. Create rules in Alerts to start monitoring.
                </span>
              )}
              {recentAlerts.map((alert) => (
                <div
                  key={alert.id}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: 'minmax(0, 1fr)',
                    gap: t.space.sm,
                    alignItems: 'center',
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    background: t.color.bg,
                    borderRadius: t.radius.md,
                    border: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  <div>
                    <span
                      style={{
                        fontSize: t.font.sizeXs,
                        fontWeight: t.font.weightMedium,
                        color:
                          alert.severity === 'critical'
                            ? t.color.danger
                            : alert.severity === 'warning' || alert.severity === 'warn'
                              ? t.color.warning
                              : t.color.accent,
                      }}
                    >
                      {(alert.severity ?? 'info').toUpperCase()}
                    </span>
                    <span style={{ marginLeft: t.space.sm, fontSize: t.font.sizeSm, color: t.color.text, wordBreak: 'break-word' }}>
                      {alert.title}
                    </span>
                    <span style={{ display: 'block', fontSize: t.font.sizeXs, color: t.color.textMuted, marginTop: 2 }}>
                      {alert.ts ? new Date(alert.ts).toLocaleString() : '—'}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                    {alert.source === 'legacy' && alert.legacy_id != null ? (
                      <>
                        <button
                          type="button"
                          onClick={() =>
                            updateAlertStatusMutation.mutate({ alertId: alert.legacy_id!, status: 'ack' })
                          }
                          disabled={updateAlertStatusMutation.isPending}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.border}`,
                            background: t.color.surface,
                            fontSize: t.font.sizeXs,
                            cursor: updateAlertStatusMutation.isPending ? 'wait' : 'pointer',
                          }}
                        >
                          Ack
                        </button>
                        <button
                          type="button"
                          onClick={() =>
                            updateAlertStatusMutation.mutate({
                              alertId: alert.legacy_id!,
                              status: 'snoozed',
                              snooze_until: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
                            })
                          }
                          disabled={updateAlertStatusMutation.isPending}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.border}`,
                            background: t.color.surface,
                            fontSize: t.font.sizeXs,
                            cursor: updateAlertStatusMutation.isPending ? 'wait' : 'pointer',
                          }}
                        >
                          Snooze 24h
                        </button>
                        {alert.deep_link ? (
                          <a
                            href={alert.deep_link}
                            style={{
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              borderRadius: t.radius.sm,
                              border: `1px solid ${t.color.accent}`,
                              background: t.color.accentMuted,
                              fontSize: t.font.sizeXs,
                              color: t.color.accent,
                              textDecoration: 'none',
                              fontWeight: t.font.weightMedium,
                            }}
                          >
                            Details
                          </a>
                        ) : null}
                      </>
                    ) : (
                      <button
                        type="button"
                        onClick={() => onNavigate('alerts')}
                        style={{
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.accent}`,
                          background: t.color.accentMuted,
                          fontSize: t.font.sizeXs,
                          color: t.color.accent,
                          cursor: 'pointer',
                          fontWeight: t.font.weightMedium,
                        }}
                      >
                        Open alerts
                      </button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </SectionCard>
        </div>
      </div>
    </DashboardPage>
  )
}
