import type { ReactNode } from 'react'
import { tokens as t } from '../../theme/tokens'

export interface ContextSummaryItem {
  label: string
  value: ReactNode
  valueColor?: string
}

interface ContextSummaryStripProps {
  items: ContextSummaryItem[]
  minItemWidth?: number
}

function normalizeSummaryValue(label: string, value: ReactNode): ReactNode {
  if (typeof value !== 'string') return value
  if (!/period/i.test(label)) return value
  const dateMatches = value.match(/\d{4}-\d{2}-\d{2}/g)
  if (!dateMatches?.length) return value
  if (dateMatches.length >= 2) return `${dateMatches[0]} – ${dateMatches[1]}`
  return dateMatches[0]
}

export default function ContextSummaryStrip({
  items,
  minItemWidth = 180,
}: ContextSummaryStripProps) {
  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: `repeat(auto-fit, minmax(${minItemWidth}px, 1fr))`,
        gap: t.space.md,
      }}
    >
      {items.map((item) => (
        <div
          key={item.label}
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            padding: t.space.md,
            boxShadow: t.shadowSm,
          }}
        >
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>
            {item.label}
          </div>
          <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: item.valueColor ?? t.color.text }}>
            {normalizeSummaryValue(item.label, item.value)}
          </div>
        </div>
      ))}
    </div>
  )
}
