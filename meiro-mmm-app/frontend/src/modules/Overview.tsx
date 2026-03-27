import { useMemo, useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import DecisionStatusCard from '../components/DecisionStatusCard'
import RecommendedActionsList, { type RecommendedActionItem } from '../components/RecommendedActionsList'
import WorkspaceAssistantPanel from '../components/WorkspaceAssistantPanel'
import { navigateForRecommendedAction } from '../lib/recommendedActions'
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'
import {
  listJourneyAlertDefinitions,
  listJourneyAlertEvents,
  type JourneyAlertDefinitionItem,
  type JourneyAlertEventItem,
} from './alerts/api'
import {
  DashboardPage,
  KpiTile,
  SectionCard,
  InsightRow,
  DashboardTable,
  KpiTileSkeleton,
  DataHealthCard,
} from '../components/dashboard'

type PageKey =
  | 'overview'
  | 'alerts'
  | 'dashboard'
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
  canCreateAlerts: boolean
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

function daysInPeriod(fromIso?: string, toIso?: string): number {
  if (!fromIso || !toIso) return 0
  const from = new Date(fromIso)
  const to = new Date(toIso)
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return 0
  const days = Math.floor((to.getTime() - from.getTime()) / (24 * 60 * 60 * 1000)) + 1
  return Math.max(1, days)
}

// --- Cover Dashboard ---
export default function Overview({ lastPage, onNavigate, onConnectDataSources, canCreateAlerts }: OverviewProps) {
  const {
    globalDateFrom,
    globalDateTo,
    isLoadingSampleJourneys,
    loadSampleJourneys,
  } = useWorkspaceContext()
  const [funnelTab, setFunnelTab] = useState<'conversions' | 'revenue' | 'speed'>('conversions')

  const dateRange = useMemo(() => {
    const dateFrom = globalDateFrom || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)
    const dateTo = globalDateTo || new Date().toISOString().slice(0, 10)
    return { date_from: dateFrom, date_to: dateTo }
  }, [globalDateFrom, globalDateTo])

  const summaryQuery = useQuery<OverviewSummaryResponse>({
    queryKey: ['overview-summary', dateRange.date_from, dateRange.date_to],
    queryFn: async () => {
      return apiGetJson<OverviewSummaryResponse>(withQuery('/api/overview/summary', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
      }), { fallbackMessage: 'Failed to load overview summary' })
    },
  })

  const driversQuery = useQuery<OverviewDriversResponse>({
    queryKey: ['overview-drivers', dateRange.date_from, dateRange.date_to],
    queryFn: async () => {
      return apiGetJson<OverviewDriversResponse>(withQuery('/api/overview/drivers', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
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
    queryKey: ['overview-funnels', dateRange.date_from, dateRange.date_to],
    queryFn: async () => {
      return apiGetJson<OverviewFunnelsResponse>(withQuery('/api/overview/funnels', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
        limit: 5,
      }), { fallbackMessage: 'Failed to load top funnels' })
    },
  })

  const trendsQuery = useQuery<OverviewTrendsResponse>({
    queryKey: ['overview-trends', dateRange.date_from, dateRange.date_to],
    queryFn: async () => {
      return apiGetJson<OverviewTrendsResponse>(withQuery('/api/overview/trends', {
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
      }), { fallbackMessage: 'Failed to load trend insights' })
    },
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
  const selectedFunnel = funnelRows[0] ?? null
  const freshness = summary?.freshness
  const trendInsights = trendsQuery.data
  const journeysReadiness = summary?.readiness
  const latestEventReplayDiagnostics = journeysReadiness?.details?.latest_event_replay?.diagnostics
  const overviewAttentionQueue = summary?.attention_queue ?? journeysReadiness?.recommended_actions ?? []

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

  // --- Header: date range + filters + Create alert ---
  const dateRangeNode = (
    <div
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: t.space.sm,
        padding: `${t.space.sm}px ${t.space.md}px`,
        borderRadius: t.radius.sm,
        border: `1px solid ${t.color.borderLight}`,
        background: t.color.surface,
        fontSize: t.font.sizeSm,
        color: t.color.textSecondary,
      }}
    >
      <span role="img" aria-hidden>📅</span>
      <span>
        {dateRange.date_from} – {dateRange.date_to}
      </span>
    </div>
  )

  const headerActions = (
    <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
      {canCreateAlerts && (
        <button
          type="button"
          onClick={() => onNavigate('alerts')}
          style={{
            padding: `${t.space.sm}px ${t.space.md}px`,
            borderRadius: t.radius.sm,
            border: 'none',
            background: t.color.accent,
            color: '#ffffff',
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightSemibold,
            cursor: 'pointer',
          }}
        >
          Create alert
        </button>
      )}
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
      description="What happened, why it happened, and what to check next."
      dateRange={dateRangeNode}
      filters={null}
      actions={headerActions}
      isLoading={isLoading && !isEmpty}
      loadingState={loadingState}
      isError={isError}
      errorMessage={errorMessage}
      isEmpty={isEmpty}
      emptyState={emptyState}
    >
      <div style={{ display: 'grid', gap: t.space.xl }}>
        {overviewAttentionQueue.length ? (
          <SectionCard title="Workspace Assistant" subtitle="Ranked next steps across taxonomy, KPI, journeys, Meiro, and data sources.">
            <WorkspaceAssistantPanel
              actions={overviewAttentionQueue}
              onActionClick={handleOverviewAction}
            />
          </SectionCard>
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
          title="What changed"
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
            <DashboardTable>
              <thead>
                <tr>
                  <th>Channel</th>
                  <th>Spend</th>
                  <th>Visits</th>
                  <th>Conversions</th>
                  <th>Revenue</th>
                  <th>Δ%</th>
                </tr>
              </thead>
              <tbody>
                {byChannel.slice(0, 10).map((row) => (
                  <tr key={row.channel}>
                    <td style={{ fontWeight: t.font.weightMedium }}>{row.channel}</td>
                    <td>{formatCurrency(row.spend)}</td>
                    <td>{row.visits.toLocaleString()}</td>
                    <td>{row.conversions.toLocaleString()}</td>
                    <td>{formatCurrency(row.revenue)}</td>
                    <td>
                      {row.delta_revenue_pct != null
                        ? `${row.delta_revenue_pct >= 0 ? '▲' : '▼'} ${Math.abs(row.delta_revenue_pct).toFixed(1)}%`
                        : '—'}
                    </td>
                  </tr>
                ))}
                {byChannel.length === 0 && (
                  <tr>
                    <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary }}>
                      No channel data for this period.
                    </td>
                  </tr>
                )}
              </tbody>
            </DashboardTable>
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
            <DashboardTable>
              <thead>
                <tr>
                  <th>Campaign</th>
                  <th>Revenue</th>
                  <th>Δ%</th>
                </tr>
              </thead>
              <tbody>
                {byCampaign.slice(0, 5).map((row) => (
                  <tr key={row.campaign}>
                    <td style={{ fontWeight: t.font.weightMedium }}>{row.campaign}</td>
                    <td>{formatCurrency(row.revenue)}</td>
                    <td>
                      {row.delta_revenue_pct != null
                        ? `${row.delta_revenue_pct >= 0 ? '▲' : '▼'} ${Math.abs(row.delta_revenue_pct).toFixed(1)}%`
                        : '—'}
                    </td>
                  </tr>
                ))}
                {byCampaign.length === 0 && (
                  <tr>
                    <td colSpan={3} style={{ textAlign: 'center', color: t.color.textSecondary }}>
                      No campaign data.
                    </td>
                  </tr>
                )}
              </tbody>
            </DashboardTable>
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
          {journeysReadiness && (journeysReadiness.status === 'blocked' || journeysReadiness.warnings.length > 0) ? (
            <SectionCard
              title="Readiness"
              subtitle="Canonical health signals from taxonomy, KPI coverage, and journeys freshness."
            >
              <DecisionStatusCard
                title="Readiness"
                status={journeysReadiness.status}
                blockers={journeysReadiness.blockers}
                warnings={journeysReadiness.warnings.slice(0, 3)}
                compact
              />
            </SectionCard>
          ) : null}
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
              <DashboardTable>
                <thead>
                  <tr>
                    <th>Channel</th>
                    <th>Revenue share Δ</th>
                    <th>Visit share Δ</th>
                    <th>Conv share Δ</th>
                  </tr>
                </thead>
                <tbody>
                  {(trendInsights?.mix_shift ?? []).map((row) => (
                    <tr key={row.channel}>
                      <td style={{ fontWeight: t.font.weightMedium }}>{row.channel}</td>
                      <td style={{ color: row.revenue_share_delta_pp >= 0 ? t.color.success : t.color.danger }}>
                        {row.revenue_share_delta_pp >= 0 ? '+' : ''}{row.revenue_share_delta_pp.toFixed(2)} pp
                      </td>
                      <td style={{ color: row.visit_share_delta_pp >= 0 ? t.color.success : t.color.danger }}>
                        {row.visit_share_delta_pp >= 0 ? '+' : ''}{row.visit_share_delta_pp.toFixed(2)} pp
                      </td>
                      <td style={{ color: row.conversion_share_delta_pp >= 0 ? t.color.success : t.color.danger }}>
                        {row.conversion_share_delta_pp >= 0 ? '+' : ''}{row.conversion_share_delta_pp.toFixed(2)} pp
                      </td>
                    </tr>
                  ))}
                  {(trendInsights?.mix_shift ?? []).length === 0 && (
                    <tr>
                      <td colSpan={4} style={{ textAlign: 'center', color: t.color.textSecondary }}>
                        No mix shift data for this period.
                      </td>
                    </tr>
                  )}
                </tbody>
              </DashboardTable>
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
