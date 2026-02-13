import { useMemo, useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { DashboardPage, SectionCard, DashboardTable } from '../components/dashboard'
import AlertDetailDrawer from './alerts/AlertDetailDrawer'
import AlertRulesTab from './alerts/AlertRulesTab'
import { apiGetJson, apiSendJson } from '../lib/apiClient'

interface AlertListItem {
  id: number
  rule_id: number
  rule_name: string | null
  rule_type: string | null
  ts_detected: string | null
  severity: string
  title: string
  message: string
  status: string
  snooze_until: string | null
  deep_link: { entity_type: string; entity_id: string | null; url: string }
  created_at: string | null
}

interface JourneyAlertDefinitionItem {
  id: string
  name: string
  type: string
  domain: 'journeys' | 'funnels'
  scope: Record<string, unknown>
  metric: string
  is_enabled: boolean
}

interface JourneyAlertEventItem {
  id: string
  alert_definition_id: string
  domain: 'journeys' | 'funnels'
  triggered_at: string | null
  severity: string
  summary: string
}

interface DisplayAlertItem {
  id: number | string
  rule_id: number
  rule_name: string | null
  rule_type: string | null
  ts_detected: string | null
  severity: string
  title: string
  message: string
  status: string
  snooze_until: string | null
  deep_link: { entity_type: string; entity_id: string | null; url: string }
  created_at: string | null
}

type TabId = 'alerts' | 'rules' | 'journey_alerts'

const STATUS_OPTIONS = [
  { value: 'open', label: 'Open' },
  { value: 'ack', label: 'Acknowledged' },
  { value: 'snoozed', label: 'Snoozed' },
  { value: 'resolved', label: 'Resolved' },
  { value: 'all', label: 'All' },
]

const SEVERITY_OPTIONS = [
  { value: '', label: 'All severity' },
  { value: 'critical', label: 'Critical' },
  { value: 'warn', label: 'Warning' },
  { value: 'info', label: 'Info' },
]

const TYPE_OPTIONS = [
  { value: '', label: 'All types' },
  { value: 'anomaly_kpi', label: 'KPI anomaly' },
  { value: 'threshold', label: 'Threshold' },
  { value: 'data_freshness', label: 'Data freshness' },
  { value: 'pipeline_health', label: 'Pipeline health' },
]

function formatSeverity(s: string): string {
  if (s === 'warn') return 'Warning'
  return s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '—'
}

function formatTs(iso: string | null | undefined): string {
  if (!iso) return '—'
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString()
}

function severityColor(severity: string): string {
  switch (severity?.toLowerCase()) {
    case 'critical':
      return t.color.danger
    case 'warn':
    case 'warning':
      return t.color.warning
    default:
      return t.color.accent
  }
}

export default function Alerts() {
  const [tab, setTab] = useState<TabId>('alerts')
  const [statusFilter, setStatusFilter] = useState('open')
  const [severityFilter, setSeverityFilter] = useState('')
  const [typeFilter, setTypeFilter] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [detailAlertId, setDetailAlertId] = useState<number | null>(null)
  const [journeyAlertDomain, setJourneyAlertDomain] = useState<'journeys' | 'funnels'>('journeys')
  const queryClient = useQueryClient()

  const alertsQuery = useQuery<{ items: AlertListItem[]; total: number; page: number; per_page: number }>({
    queryKey: ['alerts-list', statusFilter, severityFilter, typeFilter, search, page],
    queryFn: async () => {
      const params = new URLSearchParams()
      params.set('status', statusFilter)
      params.set('page', String(page))
      params.set('per_page', '20')
      if (severityFilter) params.set('severity', severityFilter)
      if (typeFilter) params.set('rule_type', typeFilter)
      if (search.trim()) params.set('search', search.trim())
      return apiGetJson<{ items: AlertListItem[]; total: number; page: number; per_page: number }>(
        `/api/alerts?${params.toString()}`,
        { fallbackMessage: 'Failed to load alerts' },
      )
    },
    enabled: tab === 'alerts',
  })

  const ackMutation = useMutation({
    mutationFn: async (id: number) =>
      apiSendJson<any>(`/api/alerts/${id}/ack`, 'POST', undefined, { fallbackMessage: 'Failed to ack' }),
    onSuccess: (_, id) => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', id] })
    },
  })
  const snoozeMutation = useMutation({
    mutationFn: async ({ id, duration_minutes }: { id: number; duration_minutes: number }) => {
      return apiSendJson<any>(`/api/alerts/${id}/snooze`, 'POST', { duration_minutes }, {
        fallbackMessage: 'Failed to snooze',
      })
    },
    onSuccess: (_, { id }) => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', id] })
    },
  })
  const resolveMutation = useMutation({
    mutationFn: async (id: number) =>
      apiSendJson<any>(`/api/alerts/${id}/resolve`, 'POST', undefined, { fallbackMessage: 'Failed to resolve' }),
    onSuccess: (_, id) => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', id] })
    },
  })

  const openDetail = useCallback((id: number) => setDetailAlertId(id), [])
  const closeDetail = useCallback(() => setDetailAlertId(null), [])

  const journeyAlertDefsQuery = useQuery<{ items: JourneyAlertDefinitionItem[]; total: number; page: number; per_page: number }>({
    queryKey: ['journey-alert-definitions', journeyAlertDomain],
    queryFn: async () => {
      const params = new URLSearchParams({ domain: journeyAlertDomain, page: '1', per_page: '100' })
      return apiGetJson<{ items: JourneyAlertDefinitionItem[]; total: number; page: number; per_page: number }>(
        `/api/alerts?${params.toString()}`,
        { fallbackMessage: 'Failed to load journey/funnel alert definitions' },
      )
    },
    enabled: tab === 'journey_alerts',
  })

  const journeyAlertEventsQuery = useQuery<{ items: JourneyAlertEventItem[]; total: number; page: number; per_page: number }>({
    queryKey: ['journey-alert-events', journeyAlertDomain],
    queryFn: async () => {
      const params = new URLSearchParams({ domain: journeyAlertDomain, page: '1', per_page: '100' })
      return apiGetJson<{ items: JourneyAlertEventItem[]; total: number; page: number; per_page: number }>(
        `/api/alerts/events?${params.toString()}`,
        { fallbackMessage: 'Failed to load journey/funnel alert events' },
      )
    },
    enabled: tab === 'journey_alerts',
  })

  const journeyAlertsForMainQuery = useQuery<{ defs: JourneyAlertDefinitionItem[]; events: JourneyAlertEventItem[] }>({
    queryKey: ['journey-alerts-main-rows'],
    queryFn: async () => {
      const [defsJBody, defsFBody, evJBody, evFBody] = await Promise.all([
        apiGetJson<{ items: JourneyAlertDefinitionItem[] }>('/api/alerts?domain=journeys&page=1&per_page=100', {
          fallbackMessage: 'Failed to load journey alert definitions',
        }),
        apiGetJson<{ items: JourneyAlertDefinitionItem[] }>('/api/alerts?domain=funnels&page=1&per_page=100', {
          fallbackMessage: 'Failed to load funnel alert definitions',
        }),
        apiGetJson<{ items: JourneyAlertEventItem[] }>('/api/alerts/events?domain=journeys&page=1&per_page=200', {
          fallbackMessage: 'Failed to load journey alert events',
        }),
        apiGetJson<{ items: JourneyAlertEventItem[] }>('/api/alerts/events?domain=funnels&page=1&per_page=200', {
          fallbackMessage: 'Failed to load funnel alert events',
        }),
      ])
      return {
        defs: [...(defsJBody.items ?? []), ...(defsFBody.items ?? [])],
        events: [...(evJBody.items ?? []), ...(evFBody.items ?? [])],
      }
    },
    enabled: tab === 'alerts',
    staleTime: 15_000,
  })

  const filteredItems = useMemo<DisplayAlertItem[]>(() => {
    const nativeItems: DisplayAlertItem[] = (alertsQuery.data?.items ?? []).map((item) => ({ ...item }))
    const defs = journeyAlertsForMainQuery.data?.defs ?? []
    const events = journeyAlertsForMainQuery.data?.events ?? []
    const syntheticItems: DisplayAlertItem[] = defs.map((def) => {
      const latest = events
        .filter((ev) => ev.alert_definition_id === def.id)
        .sort((a, b) => new Date(b.triggered_at || 0).getTime() - new Date(a.triggered_at || 0).getTime())[0]
      const scope = def.scope || {}
      const entityId =
        (scope.path_hash as string | undefined) ||
        (scope.funnel_id as string | undefined) ||
        (scope.journey_definition_id as string | undefined) ||
        null
      const status = !def.is_enabled ? 'disabled' : latest ? 'open' : 'pending_eval'
      return {
        id: `journey-def-${def.id}`,
        rule_id: 0,
        rule_name: def.name,
        rule_type: def.type,
        ts_detected: latest?.triggered_at ?? null,
        severity: latest?.severity ?? 'info',
        title: def.name,
        message: latest?.summary ?? 'Definition created. Waiting for next evaluation run.',
        status,
        snooze_until: null,
        deep_link: { entity_type: def.domain, entity_id: entityId, url: '/analytics/journeys' },
        created_at: null,
      }
    })

    const combined = [...nativeItems, ...syntheticItems]
    const q = search.trim().toLowerCase()
    const filtered = combined.filter((a) => {
      if (severityFilter && a.severity !== severityFilter) return false
      if (typeFilter && a.rule_type !== typeFilter) return false
      if (statusFilter !== 'all' && a.status !== statusFilter) return false
      if (q && !`${a.title} ${a.message} ${a.rule_name || ''}`.toLowerCase().includes(q)) return false
      return true
    })
    if (!dateFrom) return filtered
    const fromTs = new Date(dateFrom).getTime()
    return filtered.filter((a) => {
      const ts = a.ts_detected ? new Date(a.ts_detected).getTime() : 0
      return ts >= fromTs
    })
  }, [
    alertsQuery.data?.items,
    journeyAlertsForMainQuery.data?.defs,
    journeyAlertsForMainQuery.data?.events,
    dateFrom,
    search,
    severityFilter,
    typeFilter,
    statusFilter,
  ])

  const perPage = alertsQuery.data?.per_page ?? 20
  const total = alertsQuery.data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / perPage))

  const filters = (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
      <select
        value={statusFilter}
        onChange={(e) => setStatusFilter(e.target.value)}
        style={{
          padding: `${t.space.xs}px ${t.space.sm}px`,
          borderRadius: t.radius.sm,
          border: `1px solid ${t.color.border}`,
          background: t.color.surface,
          fontSize: t.font.sizeSm,
        }}
      >
        {STATUS_OPTIONS.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
      <select
        value={severityFilter}
        onChange={(e) => setSeverityFilter(e.target.value)}
        style={{
          padding: `${t.space.xs}px ${t.space.sm}px`,
          borderRadius: t.radius.sm,
          border: `1px solid ${t.color.border}`,
          background: t.color.surface,
          fontSize: t.font.sizeSm,
        }}
      >
        {SEVERITY_OPTIONS.map((o) => (
          <option key={o.value || 'all'} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
      <select
        value={typeFilter}
        onChange={(e) => setTypeFilter(e.target.value)}
        style={{
          padding: `${t.space.xs}px ${t.space.sm}px`,
          borderRadius: t.radius.sm,
          border: `1px solid ${t.color.border}`,
          background: t.color.surface,
          fontSize: t.font.sizeSm,
        }}
      >
        {TYPE_OPTIONS.map((o) => (
          <option key={o.value || 'all'} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
      <input
        type="date"
        value={dateFrom}
        onChange={(e) => setDateFrom(e.target.value)}
        title="Detected from"
        style={{
          padding: `${t.space.xs}px ${t.space.sm}px`,
          borderRadius: t.radius.sm,
          border: `1px solid ${t.color.border}`,
          background: t.color.surface,
          fontSize: t.font.sizeSm,
        }}
      />
    </div>
  )

  return (
    <>
      <DashboardPage
        title="Alerts"
        description="View and manage measurement and data quality alerts. Acknowledge, snooze, or resolve; manage alert rules."
        filters={
          tab === 'alerts' ? (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.md, alignItems: 'center' }}>
              {filters}
            </div>
          ) : undefined
        }
        isLoading={
          (tab === 'alerts' && (alertsQuery.isLoading || journeyAlertsForMainQuery.isLoading)) ||
          (tab === 'journey_alerts' && (journeyAlertDefsQuery.isLoading || journeyAlertEventsQuery.isLoading))
        }
        isError={
          (tab === 'alerts' && (alertsQuery.isError || journeyAlertsForMainQuery.isError)) ||
          (tab === 'journey_alerts' && (journeyAlertDefsQuery.isError || journeyAlertEventsQuery.isError))
        }
        errorMessage={
          tab === 'alerts'
            ? (alertsQuery.error as Error)?.message || (journeyAlertsForMainQuery.error as Error)?.message || null
            : tab === 'journey_alerts'
              ? (journeyAlertDefsQuery.error as Error)?.message || (journeyAlertEventsQuery.error as Error)?.message || null
              : null
        }
        isEmpty={
          (tab === 'alerts' && filteredItems.length === 0) ||
          (tab === 'journey_alerts' && (journeyAlertDefsQuery.data?.items ?? []).length === 0)
        }
        emptyState={
          <div
            style={{
              padding: t.space.xl,
              textAlign: 'center',
              color: t.color.textSecondary,
              border: `1px dashed ${t.color.border}`,
              borderRadius: t.radius.md,
              background: t.color.surface,
            }}
          >
            No alerts match the current filters.
          </div>
        }
      >
        <div style={{ display: 'grid', gap: t.space.xl }}>
          <div
            style={{
              display: 'flex',
              gap: t.space.xs,
              borderBottom: `1px solid ${t.color.borderLight}`,
              paddingBottom: t.space.sm,
            }}
          >
            <button
              type="button"
              onClick={() => setTab('alerts')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: 'none',
                background: tab === 'alerts' ? t.color.accentMuted : 'transparent',
                color: tab === 'alerts' ? t.color.accent : t.color.textSecondary,
                fontSize: t.font.sizeSm,
                fontWeight: tab === 'alerts' ? t.font.weightSemibold : t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Alerts
            </button>
            <button
              type="button"
              onClick={() => setTab('rules')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: 'none',
                background: tab === 'rules' ? t.color.accentMuted : 'transparent',
                color: tab === 'rules' ? t.color.accent : t.color.textSecondary,
                fontSize: t.font.sizeSm,
                fontWeight: tab === 'rules' ? t.font.weightSemibold : t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Alert rules
            </button>
            <button
              type="button"
              onClick={() => setTab('journey_alerts')}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: 'none',
                background: tab === 'journey_alerts' ? t.color.accentMuted : 'transparent',
                color: tab === 'journey_alerts' ? t.color.accent : t.color.textSecondary,
                fontSize: t.font.sizeSm,
                fontWeight: tab === 'journey_alerts' ? t.font.weightSemibold : t.font.weightMedium,
                cursor: 'pointer',
              }}
            >
              Journeys/Funnels
            </button>
          </div>

          {tab === 'alerts' && (
            <SectionCard
              title="Alerts list"
              subtitle="Filter by status, severity, type, and date; search in title and message."
            >
              <DashboardTable
                search={{
                  value: search,
                  onChange: setSearch,
                  placeholder: 'Search title, message, rule…',
                }}
                pagination={
                  totalPages > 1 ? (
                    <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
                      <button
                        type="button"
                        onClick={() => setPage((p) => Math.max(1, p - 1))}
                        disabled={page <= 1}
                        style={{
                          padding: '4px 8px',
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.border}`,
                          background: t.color.surface,
                          fontSize: t.font.sizeSm,
                          cursor: page <= 1 ? 'not-allowed' : 'pointer',
                        }}
                      >
                        Previous
                      </button>
                      <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                        Page {page} of {totalPages}
                      </span>
                      <button
                        type="button"
                        onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                        disabled={page >= totalPages}
                        style={{
                          padding: '4px 8px',
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.border}`,
                          background: t.color.surface,
                          fontSize: t.font.sizeSm,
                          cursor: page >= totalPages ? 'not-allowed' : 'pointer',
                        }}
                      >
                        Next
                      </button>
                    </div>
                  ) : null
                }
              >
                <thead>
                  <tr>
                    <th>Severity</th>
                    <th>Title</th>
                    <th>Detected</th>
                    <th>Status</th>
                    <th>Entity</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredItems.map((alert) => {
                    const eventId = typeof alert.id === 'number' ? alert.id : null
                    return (
                    <tr key={alert.id}>
                      <td>
                        <span
                          style={{
                            fontWeight: t.font.weightMedium,
                            color: severityColor(alert.severity),
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          {formatSeverity(alert.severity)}
                        </span>
                      </td>
                      <td>
                        {eventId != null ? (
                          <button
                            type="button"
                            onClick={() => openDetail(eventId)}
                            style={{
                              border: 'none',
                              background: 'transparent',
                              padding: 0,
                              cursor: 'pointer',
                              fontSize: t.font.sizeSm,
                              color: t.color.accent,
                              textAlign: 'left',
                              textDecoration: 'underline',
                            }}
                          >
                            {alert.title}
                          </button>
                        ) : (
                          <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{alert.title}</span>
                        )}
                      </td>
                      <td style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        {formatTs(alert.ts_detected)}
                      </td>
                      <td>{alert.status}</td>
                      <td style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        {alert.deep_link?.entity_type ?? '—'}
                        {alert.deep_link?.entity_id ? ` · ${alert.deep_link.entity_id}` : ''}
                      </td>
                      <td>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.xs }}>
                          {eventId != null && (
                            <>
                              <button
                                type="button"
                                onClick={() => openDetail(eventId)}
                                style={{
                                  padding: '2px 6px',
                                  borderRadius: t.radius.sm,
                                  border: `1px solid ${t.color.border}`,
                                  background: t.color.surface,
                                  fontSize: t.font.sizeXs,
                                  cursor: 'pointer',
                                  color: t.color.text,
                                }}
                              >
                                Detail
                              </button>
                              {alert.status === 'open' && (
                                <>
                                  <button
                                    type="button"
                                    onClick={() => ackMutation.mutate(eventId)}
                                    disabled={ackMutation.isPending}
                                    style={{
                                      padding: '2px 6px',
                                      borderRadius: t.radius.sm,
                                      border: `1px solid ${t.color.border}`,
                                      background: t.color.surface,
                                      fontSize: t.font.sizeXs,
                                      cursor: ackMutation.isPending ? 'wait' : 'pointer',
                                      color: t.color.text,
                                    }}
                                  >
                                    Ack
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => snoozeMutation.mutate({ id: eventId, duration_minutes: 60 })}
                                    disabled={snoozeMutation.isPending}
                                    style={{
                                      padding: '2px 6px',
                                      borderRadius: t.radius.sm,
                                      border: `1px solid ${t.color.border}`,
                                      background: t.color.surface,
                                      fontSize: t.font.sizeXs,
                                      cursor: snoozeMutation.isPending ? 'wait' : 'pointer',
                                      color: t.color.text,
                                    }}
                                  >
                                    Snooze
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => resolveMutation.mutate(eventId)}
                                    disabled={resolveMutation.isPending}
                                    style={{
                                      padding: '2px 6px',
                                      borderRadius: t.radius.sm,
                                      border: 'none',
                                      background: t.color.accent,
                                      color: t.color.surface,
                                      fontSize: t.font.sizeXs,
                                      cursor: resolveMutation.isPending ? 'wait' : 'pointer',
                                    }}
                                  >
                                    Resolve
                                  </button>
                                </>
                              )}
                            </>
                          )}
                          {eventId == null && (
                            <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Pending evaluation</span>
                          )}
                        </div>
                      </td>
                    </tr>
                    )
                  })}
                  {filteredItems.length === 0 && (
                    <tr>
                      <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary, padding: t.space.lg }}>
                        No alerts found.
                      </td>
                    </tr>
                  )}
                </tbody>
              </DashboardTable>
            </SectionCard>
          )}

          {tab === 'rules' && <AlertRulesTab />}

          {tab === 'journey_alerts' && (
            <SectionCard
              title="Journeys/Funnels alerts"
              subtitle="Definitions and latest fired events from Journeys and Funnels."
              actions={
                <select
                  value={journeyAlertDomain}
                  onChange={(e) => setJourneyAlertDomain(e.target.value as 'journeys' | 'funnels')}
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    fontSize: t.font.sizeSm,
                  }}
                >
                  <option value="journeys">Journeys</option>
                  <option value="funnels">Funnels</option>
                </select>
              }
            >
              <DashboardTable>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Domain</th>
                    <th>Metric</th>
                    <th>Scope</th>
                    <th>Status</th>
                    <th>Last triggered</th>
                  </tr>
                </thead>
                <tbody>
                  {(journeyAlertDefsQuery.data?.items ?? []).map((def) => {
                    const latest = (journeyAlertEventsQuery.data?.items ?? []).find((ev) => ev.alert_definition_id === def.id)
                    const scope = def.scope || {}
                    const scopeLabel =
                      (scope.path_hash as string | undefined) ||
                      (scope.funnel_id as string | undefined) ||
                      (scope.journey_definition_id as string | undefined) ||
                      'workspace'
                    return (
                      <tr key={def.id}>
                        <td>{def.name}</td>
                        <td>{def.domain}</td>
                        <td>{def.metric}</td>
                        <td>{scopeLabel}</td>
                        <td>{def.is_enabled ? 'Enabled' : 'Disabled'}</td>
                        <td>{formatTs(latest?.triggered_at)}</td>
                      </tr>
                    )
                  })}
                  {(journeyAlertDefsQuery.data?.items ?? []).length === 0 && (
                    <tr>
                      <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary, padding: t.space.lg }}>
                        No definitions yet. Create an alert from the Journeys page.
                      </td>
                    </tr>
                  )}
                </tbody>
              </DashboardTable>
            </SectionCard>
          )}
        </div>
      </DashboardPage>

      <AlertDetailDrawer
        alertId={detailAlertId}
        onClose={closeDetail}
      />
    </>
  )
}
