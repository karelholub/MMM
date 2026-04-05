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
            {item.value}
          </div>
        </div>
      ))}
    </div>
  )
}
