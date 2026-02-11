import { useMemo, useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { DashboardPage, SectionCard, DashboardTable } from '../components/dashboard'
import AlertDetailDrawer from './alerts/AlertDetailDrawer'
import AlertRulesTab from './alerts/AlertRulesTab'

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

type TabId = 'alerts' | 'rules'

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
      const res = await fetch(`/api/alerts?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load alerts')
      return res.json()
    },
    enabled: tab === 'alerts',
  })

  const filteredItems = useMemo(() => {
    const items = alertsQuery.data?.items ?? []
    if (!dateFrom) return items
    const fromTs = new Date(dateFrom).getTime()
    return items.filter((a) => {
      const ts = a.ts_detected ? new Date(a.ts_detected).getTime() : 0
      return ts >= fromTs
    })
  }, [alertsQuery.data?.items, dateFrom])

  const ackMutation = useMutation({
    mutationFn: async (id: number) => {
      const res = await fetch(`/api/alerts/${id}/ack`, { method: 'POST' })
      if (!res.ok) throw new Error('Failed to ack')
      return res.json()
    },
    onSuccess: (_, id) => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', id] })
    },
  })
  const snoozeMutation = useMutation({
    mutationFn: async ({ id, duration_minutes }: { id: number; duration_minutes: number }) => {
      const res = await fetch(`/api/alerts/${id}/snooze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ duration_minutes }),
      })
      if (!res.ok) throw new Error('Failed to snooze')
      return res.json()
    },
    onSuccess: (_, { id }) => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', id] })
    },
  })
  const resolveMutation = useMutation({
    mutationFn: async (id: number) => {
      const res = await fetch(`/api/alerts/${id}/resolve`, { method: 'POST' })
      if (!res.ok) throw new Error('Failed to resolve')
      return res.json()
    },
    onSuccess: (_, id) => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', id] })
    },
  })

  const openDetail = useCallback((id: number) => setDetailAlertId(id), [])
  const closeDetail = useCallback(() => setDetailAlertId(null), [])

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
        isLoading={tab === 'alerts' && alertsQuery.isLoading}
        isError={tab === 'alerts' && alertsQuery.isError}
        errorMessage={alertsQuery.isError ? (alertsQuery.error as Error)?.message : null}
        isEmpty={tab === 'alerts' && filteredItems.length === 0}
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
                  {filteredItems.map((alert) => (
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
                        <button
                          type="button"
                          onClick={() => openDetail(alert.id)}
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
                          <button
                            type="button"
                            onClick={() => openDetail(alert.id)}
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
                                onClick={() => ackMutation.mutate(alert.id)}
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
                                onClick={() => snoozeMutation.mutate({ id: alert.id, duration_minutes: 60 })}
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
                                onClick={() => resolveMutation.mutate(alert.id)}
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
                        </div>
                      </td>
                    </tr>
                  ))}
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
        </div>
      </DashboardPage>

      <AlertDetailDrawer
        alertId={detailAlertId}
        onClose={closeDetail}
      />
    </>
  )
}
