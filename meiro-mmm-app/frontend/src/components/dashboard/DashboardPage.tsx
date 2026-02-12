import { ReactNode, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { tokens as t } from '../../theme/tokens'

type AlertSeverity = 'info' | 'warning' | 'critical' | string

interface DashboardAlert {
  id: number | string
  triggered_at: string
  message: string
  severity?: AlertSeverity | null
  link?: string | null
  source?: string | null
}

interface DashboardPageProps {
  title: string
  description?: string
  dateRange?: ReactNode
  filters?: ReactNode
  actions?: ReactNode
  children: ReactNode
  isLoading?: boolean
  loadingState?: ReactNode
  isError?: boolean
  errorMessage?: string | null
  errorState?: ReactNode
  isEmpty?: boolean
  emptyState?: ReactNode
  padding?: 'md' | 'lg'
}

const DEFAULT_LOADING = (
  <div
    style={{
      padding: t.space.xl,
      textAlign: 'center',
      color: t.color.textSecondary,
      fontSize: t.font.sizeSm,
    }}
  >
    Loading…
  </div>
)

const DEFAULT_ERROR = (message?: string | null) => (
  <div
    style={{
      padding: t.space.xl,
      textAlign: 'center',
      color: t.color.danger,
      fontSize: t.font.sizeSm,
      border: `1px solid ${t.color.danger}`,
      borderRadius: t.radius.md,
      background: t.color.dangerMuted,
    }}
  >
    {message || 'Something went wrong while loading this dashboard.'}
  </div>
)

const DEFAULT_EMPTY = (
  <div
    style={{
      padding: t.space.xl,
      textAlign: 'center',
      color: t.color.textSecondary,
      fontSize: t.font.sizeSm,
      border: `1px dashed ${t.color.border}`,
      borderRadius: t.radius.md,
      background: t.color.surface,
    }}
  >
    No data available for the current filters.
  </div>
)

function severityColor(severity: AlertSeverity | null | undefined): string {
  switch (severity) {
    case 'critical':
    case 'error':
      return t.color.danger
    case 'warning':
    case 'warn':
      return t.color.warning
    default:
      return t.color.accent
  }
}

function AlertsBell({ alerts }: { alerts: DashboardAlert[] }) {
  const [open, setOpen] = useState(false)
  const hasCritical = alerts.some((alert) => (alert.severity ?? '').toLowerCase() === 'critical')
  const hasAlerts = alerts.length > 0

  return (
    <div style={{ position: 'relative' }}>
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        aria-label="Show latest alerts"
        style={{
          position: 'relative',
          border: 'none',
          background: 'transparent',
          borderRadius: t.radius.sm,
          padding: t.space.sm,
          cursor: 'pointer',
          color: hasCritical ? t.color.danger : t.color.textSecondary,
        }}
      >
        <span
          aria-hidden
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 20,
            height: 20,
          }}
        >
          🔔
        </span>
        {hasAlerts && (
          <span
            style={{
              position: 'absolute',
              top: 4,
              right: 4,
              width: 8,
              height: 8,
              background: hasCritical ? t.color.danger : t.color.accent,
              borderRadius: 8,
            }}
          />
        )}
      </button>
      {open && (
        <div
          style={{
            position: 'absolute',
            top: 'calc(100% + 8px)',
            right: 0,
            width: 'min(320px, calc(100vw - 32px))',
            maxHeight: 360,
            overflowY: 'auto',
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            boxShadow: t.shadow,
            padding: t.space.md,
            display: 'grid',
            gap: t.space.sm,
            zIndex: 20,
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            <span>Latest alerts</span>
            <button
              type="button"
              onClick={() => setOpen(false)}
              style={{
                border: 'none',
                background: 'transparent',
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
                cursor: 'pointer',
              }}
            >
              Close
            </button>
          </div>
          {alerts.length === 0 && (
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>
              All clear. No alerts in the last runs.
            </span>
          )}
          {alerts.map((alert) => (
            <a
              key={alert.id}
              href={alert.link ?? undefined}
              style={{
                display: 'grid',
                gap: 4,
                padding: t.space.sm,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                textDecoration: 'none',
                background: t.color.bg,
                color: t.color.text,
              }}
            >
              <span
                style={{
                  fontSize: t.font.sizeXs,
                  fontWeight: t.font.weightMedium,
                  color: severityColor(alert.severity ?? null),
                }}
              >
                {(alert.severity ?? 'info').toString().toUpperCase()}
              </span>
              <span style={{ fontSize: t.font.sizeSm }}>{alert.message}</span>
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                {alert.triggered_at ? new Date(alert.triggered_at).toLocaleString() : ''}
              </span>
            </a>
          ))}
          <a
            href="/alerts"
            style={{
              textDecoration: 'none',
              fontSize: t.font.sizeSm,
              color: t.color.accent,
              justifySelf: 'flex-end',
              fontWeight: t.font.weightMedium,
            }}
            onClick={() => setOpen(false)}
          >
            View all alerts →
          </a>
        </div>
      )}
    </div>
  )
}

export function DashboardPage({
  title,
  description,
  dateRange,
  filters,
  actions,
  children,
  isLoading,
  loadingState,
  isError,
  errorMessage,
  errorState,
  isEmpty,
  emptyState,
  padding = 'lg',
}: DashboardPageProps) {
  const alertsQuery = useQuery<DashboardAlert[]>({
    queryKey: ['dashboard-alerts-launcher'],
    queryFn: async () => {
      const res = await fetch('/api/data-quality/alerts?limit=12')
      if (!res.ok) return []
      const data = (await res.json()) as DashboardAlert[]
      return Array.isArray(data) ? data : []
    },
  })

  const alerts = useMemo(
    () => (alertsQuery.data ?? []).slice(0, 12),
    [alertsQuery.data],
  )

  const bodyPadding = padding === 'lg' ? t.space.xl : t.space.lg

  let content: ReactNode = children
  if (isLoading) {
    content = loadingState ?? DEFAULT_LOADING
  } else if (isError) {
    content = errorState ?? DEFAULT_ERROR(errorMessage)
  } else if (isEmpty) {
    content = emptyState ?? DEFAULT_EMPTY
  }

  return (
    <div
      style={{
        display: 'grid',
        gap: t.space.xl,
        padding: `${bodyPadding}px`,
      }}
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
          gap: t.space.md,
          alignItems: 'start',
        }}
      >
        <div style={{ display: 'grid', gap: 4, minWidth: 0 }}>
          <h1
            style={{
              margin: 0,
              fontSize: t.font.size2xl,
              color: t.color.text,
              fontWeight: t.font.weightSemibold,
              letterSpacing: '-0.01em',
            }}
          >
            {title}
          </h1>
          {description && (
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {description}
            </p>
          )}
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: t.space.md,
            flexWrap: 'wrap',
            justifyContent: 'flex-start',
            minWidth: 0,
          }}
        >
          {dateRange && <div style={{ minWidth: 0, flex: '0 1 auto' }}>{dateRange}</div>}
          {filters && <div style={{ minWidth: 0, flex: '1 1 220px' }}>{filters}</div>}
          {actions && <div style={{ minWidth: 0, flex: '0 1 auto' }}>{actions}</div>}
          <div style={{ marginLeft: 'auto' }}>
            <AlertsBell alerts={alerts} />
          </div>
        </div>
      </div>
      <div style={{ minHeight: 240 }}>{content}</div>
    </div>
  )
}

export default DashboardPage
