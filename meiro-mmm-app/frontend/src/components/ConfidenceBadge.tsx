import { tokens as t } from '../theme/tokens'

export interface Confidence {
  score: number
  label: 'high' | 'medium' | 'low' | string
  components?: Record<string, number>
}

interface Props {
  confidence?: Confidence | null
  compact?: boolean
}

export default function ConfidenceBadge({ confidence, compact }: Props) {
  if (!confidence) return null
  const { score, label, components } = confidence

  const normalizedLabel = (label || '').toLowerCase() as 'high' | 'medium' | 'low'
  // Start from base semantic colors to keep TypeScript happy with as const tokens
  let color: string = t.color.textSecondary
  let bg: string = t.color.bg
  if (normalizedLabel === 'high') {
    color = t.color.success
    bg = t.color.successMuted
  } else if (normalizedLabel === 'medium') {
    color = t.color.warning
    bg = t.color.warningMuted
  } else if (normalizedLabel === 'low') {
    color = t.color.danger
    bg = t.color.dangerMuted
  }

  const tooltip =
    components && Object.keys(components).length
      ? Object.entries(components)
          .map(([k, v]) => `${k}: ${typeof v === 'number' ? v.toFixed(2) : String(v)}`)
          .join('\n')
      : 'Confidence combines match rate, join rate, missingness, freshness, duplication and consent coverage.'

  return (
    <span
      title={tooltip}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        padding: compact ? '2px 6px' : '3px 8px',
        borderRadius: 999,
        border: `1px solid ${color}`,
        color,
        backgroundColor: bg,
        fontSize: t.font.sizeXs,
        fontWeight: t.font.weightMedium,
        whiteSpace: 'nowrap',
      }}
    >
      <span>Conf.</span>
      <span style={{ fontVariantNumeric: 'tabular-nums' }}>{Math.round(score)}</span>
    </span>
  )
}

