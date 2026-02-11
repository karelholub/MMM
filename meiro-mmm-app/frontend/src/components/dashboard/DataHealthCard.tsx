import { tokens as t } from '../../theme/tokens'

export interface FreshnessData {
  last_touchpoint_ts?: string | null
  last_conversion_ts?: string | null
  ingest_lag_minutes?: number | null
}

export interface DataHealthCardProps {
  freshness?: FreshnessData | null
  /** Optional link handlers (e.g. navigate to page) */
  onOpenDataSources?: () => void
  onOpenDataQuality?: () => void
}

function formatLag(minutes: number | null | undefined): string {
  if (minutes == null) return '—'
  if (minutes < 60) return `${Math.round(minutes)}m ago`
  if (minutes < 60 * 24) return `${(minutes / 60).toFixed(1)}h ago`
  return `${(minutes / 60 / 24).toFixed(1)}d ago`
}

function formatTs(ts: string | null | undefined): string {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    return Number.isNaN(d.getTime()) ? '—' : d.toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' })
  } catch {
    return '—'
  }
}

export default function DataHealthCard({ freshness, onOpenDataSources, onOpenDataQuality }: DataHealthCardProps) {
  const lag = freshness?.ingest_lag_minutes
  const status =
    lag == null ? 'unknown' : lag > 60 * 24 ? 'stale' : lag > 60 * 4 ? 'degraded' : 'ok'
  const statusLabel = status === 'ok' ? 'Fresh' : status === 'degraded' ? 'Delayed' : status === 'stale' ? 'Stale' : 'Unknown'
  const statusColor =
    status === 'ok' ? t.color.success : status === 'degraded' ? t.color.warning : status === 'stale' ? t.color.danger : t.color.textMuted

  return (
    <div
      style={{
        display: 'grid',
        gap: t.space.md,
        padding: t.space.lg,
        background: t.color.surface,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.borderLight}`,
        boxShadow: t.shadowSm,
      }}
    >
      <h3
        style={{
          margin: 0,
          fontSize: t.font.sizeBase,
          fontWeight: t.font.weightSemibold,
          color: t.color.text,
        }}
      >
        Data health
      </h3>
      <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.sm }}>
          <span style={{ color: t.color.textSecondary }}>Last ingest</span>
          <span style={{ fontWeight: t.font.weightMedium, color: statusColor }}>{lag != null ? formatLag(lag) : '—'}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.sm }}>
          <span style={{ color: t.color.textSecondary }}>Status</span>
          <span
            style={{
              padding: '2px 8px',
              borderRadius: 999,
              border: `1px solid ${statusColor}`,
              color: statusColor,
              backgroundColor: status === 'ok' ? t.color.successMuted : status === 'degraded' ? t.color.warningMuted : status === 'stale' ? t.color.dangerMuted : t.color.bg,
              fontWeight: t.font.weightMedium,
              fontSize: t.font.sizeXs,
            }}
          >
            {statusLabel}
          </span>
        </div>
        {freshness?.last_conversion_ts && (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.sm }}>
            <span style={{ color: t.color.textSecondary }}>Last conversion</span>
            <span style={{ color: t.color.text }}>{formatTs(freshness.last_conversion_ts)}</span>
          </div>
        )}
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm }}>
        {onOpenDataSources && (
          <button
            type="button"
            onClick={onOpenDataSources}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              background: t.color.surface,
              fontSize: t.font.sizeXs,
              color: t.color.accent,
              cursor: 'pointer',
              fontWeight: t.font.weightMedium,
            }}
          >
            Data sources →
          </button>
        )}
        {onOpenDataQuality && (
          <button
            type="button"
            onClick={onOpenDataQuality}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              background: t.color.surface,
              fontSize: t.font.sizeXs,
              color: t.color.accent,
              cursor: 'pointer',
              fontWeight: t.font.weightMedium,
            }}
          >
            Data quality →
          </button>
        )}
      </div>
    </div>
  )
}
