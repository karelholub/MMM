import { memo, useMemo } from 'react'
import { tokens as t } from '../../theme/tokens'
import ConfidenceBadge, { Confidence } from '../ConfidenceBadge'

export interface KpiTileProps {
  label: string
  value: string
  delta?: { value: number; label?: string }
  sparkline?: number[]
  confidence?: Confidence | null
  annotation?: string | null
}

function buildSparkline(points: number[]): string {
  if (points.length === 0) return ''
  const max = Math.max(...points)
  const min = Math.min(...points)
  const range = max - min || 1
  const width = 100
  const height = 32
  return points
    .map((point, index) => {
      const x = (index / (points.length - 1 || 1)) * width
      const y = height - ((point - min) / range) * height
      return `${x},${y}`
    })
    .join(' ')
}

export const KpiTile = memo(function KpiTile({
  label,
  value,
  delta,
  sparkline,
  confidence,
  annotation,
}: KpiTileProps) {
  const points = useMemo(() => (sparkline && sparkline.length ? buildSparkline(sparkline) : ''), [sparkline])
  const deltaLabel =
    delta && Number.isFinite(delta.value)
      ? `${delta.value > 0 ? '▲' : delta.value < 0 ? '▼' : '—'} ${Math.abs(delta.value).toFixed(1)}%`
      : null

  return (
    <div
      style={{
        display: 'grid',
        gap: t.space.sm,
        padding: t.space.lg,
        background: t.color.surface,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.borderLight}`,
        boxShadow: t.shadowSm,
        minWidth: 220,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{label}</span>
        {confidence && <ConfidenceBadge confidence={confidence} />}
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: t.space.sm }}>
        <span
          style={{
            fontSize: 28,
            fontWeight: t.font.weightSemibold,
            color: t.color.text,
            letterSpacing: '-0.01em',
          }}
        >
          {value}
        </span>
        {deltaLabel && (
          <span
            style={{
              fontSize: t.font.sizeSm,
              color: delta && delta.value >= 0 ? t.color.success : t.color.danger,
              fontWeight: t.font.weightMedium,
            }}
            title={delta.label}
          >
            {deltaLabel}
          </span>
        )}
      </div>
      {annotation && (
        <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{annotation}</span>
      )}
      {points && (
        <svg viewBox="0 0 100 32" preserveAspectRatio="none" style={{ width: '100%', height: 32 }}>
          <polyline
            fill="none"
            stroke={t.color.accent}
            strokeWidth={2}
            strokeLinecap="round"
            strokeLinejoin="round"
            points={points}
          />
        </svg>
      )}
    </div>
  )
})

export default KpiTile
