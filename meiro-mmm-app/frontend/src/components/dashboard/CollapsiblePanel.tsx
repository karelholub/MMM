import type { ReactNode } from 'react'
import { tokens as t } from '../../theme/tokens'

interface CollapsiblePanelProps {
  title: string
  subtitle?: string
  open: boolean
  onToggle: () => void
  children: ReactNode
}

export default function CollapsiblePanel({
  title,
  subtitle,
  open,
  onToggle,
  children,
}: CollapsiblePanelProps) {
  return (
    <div
      style={{
        background: t.color.surface,
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.lg,
        boxShadow: t.shadowSm,
      }}
    >
      <button
        type="button"
        onClick={onToggle}
        style={{
          width: '100%',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: t.space.md,
          padding: t.space.lg,
          background: 'transparent',
          border: 'none',
          cursor: 'pointer',
          textAlign: 'left',
        }}
      >
        <div style={{ display: 'grid', gap: 4 }}>
          <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{title}</div>
          {subtitle ? <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{subtitle}</div> : null}
        </div>
        <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.accent }}>
          {open ? 'Hide' : 'Show'}
        </div>
      </button>
      {open ? <div style={{ padding: `0 ${t.space.lg}px ${t.space.lg}px` }}>{children}</div> : null}
    </div>
  )
}
