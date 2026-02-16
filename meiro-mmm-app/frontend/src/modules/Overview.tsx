import { useMemo } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { useWorkspaceContext } from '../components/WorkspaceContext'
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
  type: 'kpi_delta' | 'alert'
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
  highlights: HighlightItem[]
  freshness: FreshnessResponse
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
  conversions: number
  revenue: number
  delta_spend_pct?: number | null
  delta_conversions_pct?: number | null
  delta_revenue_pct?: number | null
}

interface CampaignDriver {
  campaign: string
  revenue: number
  conversions: number
  delta_revenue_pct?: number | null
}

interface OverviewDriversResponse {
  by_channel: ChannelDriver[]
  by_campaign: CampaignDriver[]
  date_from: string
  date_to: string
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

function getDefaultDateRange(): { date_from: string; date_to: string } {
  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - 30)
  return {
    date_from: start.toISOString().slice(0, 10),
    date_to: end.toISOString().slice(0, 10),
  }
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
    journeysSummary,
    isLoadingSampleJourneys,
    loadSampleJourneys,
  } = useWorkspaceContext()

  const dateRange = useMemo(() => {
    if (journeysSummary?.date_min && journeysSummary?.date_max) {
      return {
        date_from: journeysSummary.date_min.slice(0, 10),
        date_to: journeysSummary.date_max.slice(0, 10),
      }
    }
    return getDefaultDateRange()
  }, [journeysSummary?.date_min, journeysSummary?.date_max])

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
  const freshness = summary?.freshness

  const hasAnyData =
    kpiTiles.some((k) => typeof k.value === 'number' && Number.isFinite(k.value) && k.value !== 0) ||
    byChannel.length > 0 ||
    recentAlerts.length > 0 ||
    highlights.length > 0
  const isEmpty = !summaryQuery.isLoading && !summaryQuery.error && !hasAnyData
  const periodDays = daysInPeriod(summary?.current_period?.date_from, summary?.current_period?.date_to)
  const baselineLabel = periodDays > 0 ? `vs previous ${periodDays} ${periodDays === 1 ? 'day' : 'days'}` : 'vs previous period'
  const tileOrder: Array<KpiTileResponse['kpi_key']> = ['spend', 'conversions', 'revenue']
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
            subtitle="Spend, conversions, revenue and period-over-period delta"
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
                    <td colSpan={5} style={{ textAlign: 'center', color: t.color.textSecondary }}>
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
