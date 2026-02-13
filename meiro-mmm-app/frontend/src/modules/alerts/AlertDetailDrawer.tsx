import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../../theme/tokens'
import { apiGetJson, apiSendJson } from '../../lib/apiClient'

export interface AlertDetail {
  id: number
  rule_id: number
  rule_name: string | null
  rule_type: string | null
  ts_detected: string | null
  severity: string
  title: string
  message: string
  status: string
  context_json: Record<string, unknown> | null
  related_entities: Record<string, unknown> | null
  deep_link: { entity_type: string; entity_id: string | null; url: string }
  snooze_until: string | null
  created_at: string | null
  updated_at: string | null
  updated_by: string | null
}

interface AlertDetailDrawerProps {
  alertId: number | null
  onClose: () => void
  onNavigate?: (url: string) => void
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

export default function AlertDetailDrawer({
  alertId,
  onClose,
  onNavigate,
}: AlertDetailDrawerProps) {
  const queryClient = useQueryClient()
  const { data: alert, isLoading, error } = useQuery<AlertDetail>({
    queryKey: ['alert-detail', alertId],
    queryFn: async () => apiGetJson<AlertDetail>(`/api/alerts/${alertId}`, { fallbackMessage: 'Failed to load alert' }),
    enabled: alertId != null,
  })

  const ackMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<any>(`/api/alerts/${alertId}/ack`, 'POST', undefined, { fallbackMessage: 'Failed to acknowledge' }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', alertId] })
    },
  })
  const snoozeMutation = useMutation({
    mutationFn: async (duration_minutes: number) => {
      return apiSendJson<any>(`/api/alerts/${alertId}/snooze`, 'POST', { duration_minutes }, {
        fallbackMessage: 'Failed to snooze',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', alertId] })
    },
  })
  const resolveMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<any>(`/api/alerts/${alertId}/resolve`, 'POST', undefined, { fallbackMessage: 'Failed to resolve' }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      queryClient.invalidateQueries({ queryKey: ['alert-detail', alertId] })
    },
  })

  if (alertId == null) return null

  const open = true
  const ctx = alert?.context_json as Record<string, unknown> | undefined
  const observed = ctx?.observed
  const expected = ctx?.expected
  const zscore = ctx?.zscore
  const dims = ctx?.dimensions ?? ctx?.dims
  const related = alert?.related_entities as Record<string, unknown> | undefined
  const link = alert?.deep_link

  return (
    <>
      <div
        role="presentation"
        onClick={onClose}
        style={{
          position: 'fixed',
          inset: 0,
          backgroundColor: 'rgba(15,23,42,0.4)',
          zIndex: 40,
          opacity: open ? 1 : 0,
          pointerEvents: open ? 'auto' : 'none',
          transition: 'opacity 0.2s',
        }}
      />
      <div
        role="dialog"
        aria-label="Alert detail"
        style={{
          position: 'fixed',
          top: 0,
          right: 0,
          width: 'min(440px, 100vw)',
          height: '100vh',
          backgroundColor: t.color.surface,
          borderLeft: `1px solid ${t.color.borderLight}`,
          boxShadow: t.shadow,
          zIndex: 41,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
          transform: open ? 'translateX(0)' : 'translateX(100%)',
          transition: 'transform 0.2s',
        }}
      >
        <div
          style={{
            padding: t.space.lg,
            borderBottom: `1px solid ${t.color.borderLight}`,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'flex-start',
            gap: t.space.md,
          }}
        >
          <h2 style={{ margin: 0, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Alert detail
          </h2>
          <button
            type="button"
            onClick={onClose}
            style={{
              border: 'none',
              background: 'transparent',
              cursor: 'pointer',
              padding: t.space.xs,
              fontSize: t.font.sizeLg,
              color: t.color.textSecondary,
              lineHeight: 1,
            }}
          >
            ×
          </button>
        </div>
        <div style={{ flex: 1, overflowY: 'auto', padding: t.space.lg }}>
          {isLoading && (
            <div style={{ padding: t.space.xl, textAlign: 'center', color: t.color.textSecondary }}>
              Loading…
            </div>
          )}
          {error && (
            <div style={{ padding: t.space.lg, color: t.color.danger, fontSize: t.font.sizeSm }}>
              {(error as Error).message}
            </div>
          )}
          {alert && !isLoading && (
            <div style={{ display: 'grid', gap: t.space.xl }}>
              <div>
                <span
                  style={{
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    color: severityColor(alert.severity),
                    textTransform: 'uppercase',
                  }}
                >
                  {alert.severity}
                </span>
                <h3 style={{ margin: '4px 0 0', fontSize: t.font.sizeBase, color: t.color.text }}>
                  {alert.title}
                </h3>
                <p style={{ margin: '8px 0 0', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  {alert.message}
                </p>
                <p style={{ margin: '4px 0 0', fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  {alert.rule_name && `${alert.rule_name} · `}
                  Detected {formatTs(alert.ts_detected)}
                </p>
              </div>

              {(observed !== undefined || expected !== undefined || zscore !== undefined) && (
                <div
                  style={{
                    padding: t.space.md,
                    background: t.color.bg,
                    borderRadius: t.radius.md,
                    border: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
                    Observed vs expected
                  </div>
                  <div style={{ display: 'grid', gap: t.space.xs, fontSize: t.font.sizeSm }}>
                    {observed !== undefined && (
                      <div>
                        <span style={{ color: t.color.textSecondary }}>Observed: </span>
                        <span style={{ fontWeight: t.font.weightMedium }}>{String(observed)}</span>
                      </div>
                    )}
                    {expected !== undefined && (
                      <div>
                        <span style={{ color: t.color.textSecondary }}>Expected: </span>
                        <span style={{ fontWeight: t.font.weightMedium }}>{String(expected)}</span>
                      </div>
                    )}
                    {zscore !== undefined && (
                      <div>
                        <span style={{ color: t.color.textSecondary }}>Z-score: </span>
                        <span style={{ fontWeight: t.font.weightMedium }}>{String(zscore)}</span>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {dims != null && typeof dims === 'object' && Object.keys(dims as object).length > 0 && (
                <div
                  style={{
                    padding: t.space.md,
                    background: t.color.bg,
                    borderRadius: t.radius.md,
                    border: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
                    Dimension context
                  </div>
                  <pre
                    style={{
                      margin: 0,
                      fontSize: t.font.sizeXs,
                      color: t.color.text,
                      whiteSpace: 'pre-wrap',
                      wordBreak: 'break-word',
                    }}
                  >
                    {JSON.stringify(dims, null, 2)}
                  </pre>
                </div>
              )}

              {(related != null && typeof related === 'object' && Object.keys(related).length > 0) && (
                <div
                  style={{
                    padding: t.space.md,
                    background: t.color.bg,
                    borderRadius: t.radius.md,
                    border: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
                    Related entities
                  </div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm }}>
                    {link?.url && (
                      <a
                        href={link.url}
                        onClick={(e) => {
                          if (onNavigate) {
                            e.preventDefault()
                            onNavigate(link.url)
                          }
                        }}
                        style={{
                          fontSize: t.font.sizeSm,
                          color: t.color.accent,
                          textDecoration: 'none',
                          fontWeight: t.font.weightMedium,
                        }}
                      >
                        Open {link.entity_type || 'entity'} →
                      </a>
                    )}
                    {Object.entries(related as Record<string, unknown>).map(([k, v]) =>
                      v != null && v !== '' ? (
                        <span
                          key={k}
                          style={{
                            fontSize: t.font.sizeXs,
                            padding: '2px 8px',
                            background: t.color.surface,
                            border: `1px solid ${t.color.borderLight}`,
                            borderRadius: t.radius.sm,
                            color: t.color.textSecondary,
                          }}
                        >
                          {k}: {String(v)}
                        </span>
                      ) : null,
                    )}
                  </div>
                </div>
              )}

              <div
                style={{
                  padding: t.space.md,
                  background: t.color.bg,
                  borderRadius: t.radius.md,
                  border: `1px solid ${t.color.borderLight}`,
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
                  History
                </div>
                <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeSm, color: t.color.text }}>
                  <li>Created {formatTs(alert.created_at)}</li>
                  {alert.updated_at && (
                    <li>
                      Last updated {formatTs(alert.updated_at)}
                      {alert.updated_by && ` by ${alert.updated_by}`}
                    </li>
                  )}
                  <li>Status: {alert.status}</li>
                  {alert.snooze_until && (
                    <li>Snoozed until {formatTs(alert.snooze_until)}</li>
                  )}
                </ul>
              </div>

              <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm }}>
                {alert.status === 'open' && (
                  <>
                    <button
                      type="button"
                      onClick={() => ackMutation.mutate()}
                      disabled={ackMutation.isPending}
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: t.color.surface,
                        fontSize: t.font.sizeSm,
                        cursor: ackMutation.isPending ? 'wait' : 'pointer',
                        color: t.color.text,
                      }}
                    >
                      Ack
                    </button>
                    <button
                      type="button"
                      onClick={() => snoozeMutation.mutate(60)}
                      disabled={snoozeMutation.isPending}
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: t.color.surface,
                        fontSize: t.font.sizeSm,
                        cursor: snoozeMutation.isPending ? 'wait' : 'pointer',
                        color: t.color.text,
                      }}
                    >
                      Snooze 1h
                    </button>
                    <button
                      type="button"
                      onClick={() => snoozeMutation.mutate(1440)}
                      disabled={snoozeMutation.isPending}
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: t.color.surface,
                        fontSize: t.font.sizeSm,
                        cursor: snoozeMutation.isPending ? 'wait' : 'pointer',
                        color: t.color.text,
                      }}
                    >
                      Snooze 24h
                    </button>
                  </>
                )}
                {alert.status !== 'resolved' && (
                  <button
                    type="button"
                    onClick={() => resolveMutation.mutate()}
                    disabled={resolveMutation.isPending}
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      borderRadius: t.radius.sm,
                      border: 'none',
                      background: t.color.accent,
                      color: t.color.surface,
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      cursor: resolveMutation.isPending ? 'wait' : 'pointer',
                    }}
                  >
                    Resolve
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  )
}
