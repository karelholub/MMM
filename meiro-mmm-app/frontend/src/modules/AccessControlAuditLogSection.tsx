import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import type { CSSProperties } from 'react'
import { usePermissions } from '../hooks/usePermissions'
import { apiGetJson } from '../lib/apiClient'

interface FeatureFlags {
  access_control_enabled: boolean
  audit_log_enabled: boolean
}

interface AuditLogItem {
  id: number
  workspace_id?: string | null
  actor_user_id?: string | null
  actor_name?: string | null
  actor_email?: string | null
  action_key: string
  target_type?: string | null
  target_id?: string | null
  metadata_json?: Record<string, unknown> | null
  ip?: string | null
  user_agent?: string | null
  created_at: string
}

interface AuditLogResponse {
  items: AuditLogItem[]
  page: number
  page_size: number
  total: number
}

function formatDateTime(value?: string | null): string {
  if (!value) return '—'
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return '—'
  return dt.toLocaleString()
}

function fromDatetimeLocalInput(value: string): string | undefined {
  if (!value) return undefined
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return undefined
  return dt.toISOString()
}

export default function AccessControlAuditLogSection({ featureFlags }: { featureFlags: FeatureFlags }) {
  const [actionFilter, setActionFilter] = useState('')
  const [actorFilter, setActorFilter] = useState('')
  const [dateFromInput, setDateFromInput] = useState('')
  const [dateToInput, setDateToInput] = useState('')
  const [page, setPage] = useState(1)
  const [selected, setSelected] = useState<AuditLogItem | null>(null)

  const cardStyle: CSSProperties = {
    border: `1px solid ${t.color.borderLight}`,
    borderRadius: t.radius.md,
    background: t.color.surface,
    padding: t.space.md,
    display: 'grid',
    gap: t.space.sm,
  }

  const permissions = usePermissions()
  const workspaceId = permissions.workspaceId || 'default'
  const canViewAudit = permissions.hasAnyPermission(['audit.view', 'settings.manage'])

  const auditQuery = useQuery<AuditLogResponse, Error>({
    queryKey: ['admin-audit-log', workspaceId, actionFilter, actorFilter, dateFromInput, dateToInput, page],
    queryFn: async () => {
      const params = new URLSearchParams({
        workspaceId,
        page: String(page),
        page_size: '50',
      })
      if (actionFilter.trim()) params.set('action', actionFilter.trim())
      if (actorFilter.trim()) params.set('actor', actorFilter.trim())
      const nextFrom = fromDatetimeLocalInput(dateFromInput)
      const nextTo = fromDatetimeLocalInput(dateToInput)
      if (nextFrom) params.set('date_from', nextFrom)
      if (nextTo) params.set('date_to', nextTo)
      return apiGetJson<AuditLogResponse>(`/api/admin/audit-log?${params.toString()}`, {
        fallbackMessage: 'Failed to load audit log',
      })
    },
    enabled: featureFlags.access_control_enabled && featureFlags.audit_log_enabled && canViewAudit,
  })

  const actionOptions = useMemo(() => {
    const keys = new Set<string>()
    for (const item of auditQuery.data?.items ?? []) keys.add(item.action_key)
    return [...keys].sort((a, b) => a.localeCompare(b))
  }, [auditQuery.data?.items])

  const totalPages = Math.max(1, Math.ceil((auditQuery.data?.total ?? 0) / 50))

  if (!featureFlags.access_control_enabled) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Audit log</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Access control is disabled for this workspace.
        </p>
      </div>
    )
  }

  if (!featureFlags.audit_log_enabled) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Audit log</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Audit log feature is disabled.
        </p>
      </div>
    )
  }

  if (!canViewAudit) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Audit log</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Your role cannot view the audit log.
        </p>
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gap: t.space.lg }}>
      <div style={cardStyle}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Audit log</h3>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Track security and admin actions across this workspace.
            </p>
          </div>
        </div>

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
            gap: t.space.sm,
            alignItems: 'end',
          }}
        >
          <label style={{ display: 'grid', gap: 4 }}>
            <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Action</span>
            <select
              value={actionFilter}
              onChange={(e) => {
                setActionFilter(e.target.value)
                setPage(1)
              }}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
                background: t.color.surface,
              }}
            >
              <option value="">All actions</option>
              {actionOptions.map((action) => (
                <option key={action} value={action}>
                  {action}
                </option>
              ))}
            </select>
          </label>

          <label style={{ display: 'grid', gap: 4 }}>
            <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Actor</span>
            <input
              value={actorFilter}
              placeholder="Name or email"
              onChange={(e) => {
                setActorFilter(e.target.value)
                setPage(1)
              }}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            />
          </label>

          <label style={{ display: 'grid', gap: 4 }}>
            <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Date from</span>
            <input
              type="datetime-local"
              value={dateFromInput}
              onChange={(e) => {
                setDateFromInput(e.target.value)
                setPage(1)
              }}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            />
          </label>

          <label style={{ display: 'grid', gap: 4 }}>
            <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Date to</span>
            <input
              type="datetime-local"
              value={dateToInput}
              onChange={(e) => {
                setDateToInput(e.target.value)
                setPage(1)
              }}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            />
          </label>
        </div>
      </div>

      <div style={cardStyle}>
        {auditQuery.isLoading ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading audit events…</div>
        ) : auditQuery.isError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>Failed to load audit log.</div>
        ) : (
          <>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>Timestamp</th>
                    <th style={{ textAlign: 'left', padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>Actor</th>
                    <th style={{ textAlign: 'left', padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>Action</th>
                    <th style={{ textAlign: 'left', padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>Target</th>
                    <th style={{ textAlign: 'left', padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {(auditQuery.data?.items ?? []).map((row: AuditLogItem) => (
                    <tr key={row.id}>
                      <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>{formatDateTime(row.created_at)}</td>
                      <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>{row.actor_name || row.actor_email || 'System'}</td>
                      <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>{row.action_key}</td>
                      <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>{row.target_type || '—'} {row.target_id ? `#${row.target_id}` : ''}</td>
                      <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>
                        <button
                          type="button"
                          onClick={() => setSelected(row)}
                          style={{
                            padding: '2px 8px',
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: 'transparent',
                            fontSize: t.font.sizeXs,
                            cursor: 'pointer',
                          }}
                        >
                          View metadata
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {(auditQuery.data?.items?.length ?? 0) === 0 && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>No audit events found for the selected filters.</div>
            )}

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm, flexWrap: 'wrap' }}>
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                {(auditQuery.data?.total ?? 0)} total events
              </span>
              <div style={{ display: 'flex', gap: t.space.xs, alignItems: 'center' }}>
                <button
                  type="button"
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                  disabled={page <= 1}
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                    background: 'transparent',
                    fontSize: t.font.sizeXs,
                    cursor: page <= 1 ? 'not-allowed' : 'pointer',
                    opacity: page <= 1 ? 0.6 : 1,
                  }}
                >
                  Previous
                </button>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  Page {page} / {totalPages}
                </span>
                <button
                  type="button"
                  onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                  disabled={page >= totalPages}
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                    background: 'transparent',
                    fontSize: t.font.sizeXs,
                    cursor: page >= totalPages ? 'not-allowed' : 'pointer',
                    opacity: page >= totalPages ? 0.6 : 1,
                  }}
                >
                  Next
                </button>
              </div>
            </div>
          </>
        )}
      </div>

      {selected && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            right: 0,
            width: 'min(560px, 100vw)',
            height: '100vh',
            zIndex: 40,
            background: t.color.surface,
            borderLeft: `1px solid ${t.color.borderLight}`,
            boxShadow: t.shadowLg,
            display: 'grid',
            gridTemplateRows: 'auto 1fr',
          }}
        >
          <div style={{ padding: t.space.md, borderBottom: `1px solid ${t.color.borderLight}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Audit event #{selected.id}</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{selected.action_key}</div>
            </div>
            <button
              type="button"
              onClick={() => setSelected(null)}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                background: 'transparent',
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
            >
              Close
            </button>
          </div>
          <div style={{ padding: t.space.md, overflow: 'auto', display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Timestamp: {formatDateTime(selected.created_at)}</div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Actor: {selected.actor_name || selected.actor_email || 'System'}</div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Target: {selected.target_type || '—'} {selected.target_id ? `#${selected.target_id}` : ''}</div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>IP: {selected.ip || '—'}</div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>User agent: {selected.user_agent || '—'}</div>
            <pre
              style={{
                margin: 0,
                padding: t.space.sm,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                background: t.color.bgSubtle,
                fontSize: t.font.sizeXs,
                overflow: 'auto',
              }}
            >
{JSON.stringify(selected.metadata_json || {}, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  )
}
