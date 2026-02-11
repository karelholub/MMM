import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import {
  DashboardPage,
  KpiTile,
  SectionCard,
  InsightRow,
  DashboardTable,
  KpiTileSkeleton,
  DataHealthCard,
} from '../components/dashboard'
import type { Confidence } from '../components/ConfidenceBadge'

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

interface OverviewProps {
  lastPage: PageKey | null
  onNavigate: (page: PageKey) => void
  onConnectDataSources: () => void
}

// --- API types (overview summary + drivers + alerts) ---
interface KpiTileResponse {
  kpi_key: string
  value: number
  delta_pct?: number | null
  sparkline?: number[]
  confidence?: string
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

/** Map overview API confidence string to ConfidenceBadge shape */
function overviewConfidenceToBadge(confidence?: string | null): Confidence | null {
  if (!confidence) return null
  const map: Record<string, { score: number; label: string }> = {
    ok: { score: 85, label: 'high' },
    degraded: { score: 55, label: 'medium' },
    stale: { score: 25, label: 'low' },
    no_data: { score: 0, label: 'low' },
  }
  const c = map[confidence]
  return c ? { score: c.score, label: c.label } : null
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

// --- Cover Dashboard ---
export default function Overview({ lastPage, onNavigate, onConnectDataSources }: OverviewProps) {
  const {
    journeysSummary,
    journeysLoaded,
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
      const params = new URLSearchParams({
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
      })
      const res = await fetch(`/api/overview/summary?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load overview summary')
      return res.json()
    },
  })

  const driversQuery = useQuery<OverviewDriversResponse>({
    queryKey: ['overview-drivers', dateRange.date_from, dateRange.date_to],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateRange.date_from,
        date_to: dateRange.date_to,
      })
      const res = await fetch(`/api/overview/drivers?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load drivers')
      return res.json()
    },
  })

  const alertsQuery = useQuery<OverviewAlertsResponse>({
    queryKey: ['overview-alerts', 'open'],
    queryFn: async () => {
      const params = new URLSearchParams({ status: 'open', limit: '20' })
      const res = await fetch(`/api/overview/alerts?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load alerts')
      return res.json()
    },
  })

  const queryClient = useQueryClient()
  const updateAlertStatusMutation = useMutation({
    mutationFn: async ({ alertId, status, snooze_until }: { alertId: number; status: string; snooze_until?: string }) => {
      const res = await fetch(`/api/overview/alerts/${alertId}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status, snooze_until }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Failed to update alert')
      }
      return res.json()
    },
    onSuccess: () => {
      alertsQuery.refetch()
      summaryQuery.refetch()
    },
  })

  const summary = summaryQuery.data
  const drivers = driversQuery.data
  const openAlerts = alertsQuery.data?.alerts ?? []
  const kpiTiles = summary?.kpi_tiles ?? []
  const highlights = summary?.highlights ?? []
  const byChannel = drivers?.by_channel ?? []
  const byCampaign = drivers?.by_campaign ?? []
  const freshness = summary?.freshness

  const hasAnyData =
    kpiTiles.some((k) => typeof k.value === 'number' && Number.isFinite(k.value) && k.value !== 0) ||
    byChannel.length > 0 ||
    openAlerts.length > 0 ||
    highlights.length > 0
  const isEmpty = !summaryQuery.isLoading && !summaryQuery.error && !hasAnyData

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
          gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
        }}
      >
        {[1, 2, 3, 4, 5, 6].map((i) => (
          <KpiTileSkeleton key={i} />
        ))}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.xl }}>
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
            gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
            maxWidth: '100%',
          }}
        >
          {kpiTiles.slice(0, 10).map((kpi) => (
            <KpiTile
              key={kpi.kpi_key}
              label={kpi.kpi_key.charAt(0).toUpperCase() + kpi.kpi_key.slice(1)}
              value={formatKpiValue(kpi.kpi_key, kpi.value)}
              delta={
                kpi.delta_pct != null && Number.isFinite(kpi.delta_pct)
                  ? { value: kpi.delta_pct }
                  : undefined
              }
              sparkline={kpi.sparkline?.length ? kpi.sparkline : undefined}
              confidence={overviewConfidenceToBadge(kpi.confidence) ?? undefined}
            />
          ))}
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
        <div style={{ display: 'grid', gap: t.space.xl, gridTemplateColumns: 'minmax(0,2fr) minmax(0,1fr)' }}>
          <SectionCard
            title="Top channels"
            subtitle="Spend, conversions, revenue and period-over-period delta"
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
        <div style={{ display: 'grid', gap: t.space.xl, gridTemplateColumns: 'minmax(0,280px) 1fr' }}>
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
              {openAlerts.length === 0 && (
                <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  No open alerts. Resolved and acknowledged alerts appear on the Alerts page.
                </span>
              )}
              {openAlerts.slice(0, 8).map((alert) => (
                <div
                  key={alert.id}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr auto',
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
                            : alert.severity === 'warning'
                              ? t.color.warning
                              : t.color.accent,
                      }}
                    >
                      {(alert.severity ?? 'info').toUpperCase()}
                    </span>
                    <span style={{ marginLeft: t.space.sm, fontSize: t.font.sizeSm, color: t.color.text }}>
                      {alert.title || alert.message}
                    </span>
                    <span style={{ display: 'block', fontSize: t.font.sizeXs, color: t.color.textMuted, marginTop: 2 }}>
                      {new Date(alert.ts_detected).toLocaleString()}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                    <button
                      type="button"
                      onClick={() =>
                        updateAlertStatusMutation.mutate({ alertId: alert.id, status: 'ack' })
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
                          alertId: alert.id,
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
