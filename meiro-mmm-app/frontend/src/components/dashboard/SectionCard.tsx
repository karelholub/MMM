import { ReactNode } from 'react'
import { tokens as t } from '../../theme/tokens'

export interface SectionCardProps {
  title: string
  subtitle?: string
  actions?: ReactNode
  footer?: ReactNode
  children: ReactNode
  overflow?: 'visible' | 'auto'
}

export default function SectionCard({
  title,
  subtitle,
  actions,
  footer,
  children,
  overflow = 'visible',
}: SectionCardProps) {
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
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          gap: t.space.md,
        }}
      >
        <div style={{ display: 'grid', gap: 4 }}>
          <h2
            style={{
              margin: 0,
              fontSize: t.font.sizeLg,
              color: t.color.text,
              fontWeight: t.font.weightSemibold,
            }}
          >
            {title}
          </h2>
          {subtitle && (
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{subtitle}</p>
          )}
        </div>
        {actions && <div style={{ display: 'flex', gap: t.space.sm }}>{actions}</div>}
      </div>
      <div style={{ overflow }}>{children}</div>
      {footer && (
        <div
          style={{
            borderTop: `1px solid ${t.color.borderLight}`,
            paddingTop: t.space.sm,
            fontSize: t.font.sizeXs,
            color: t.color.textMuted,
          }}
        >
          {footer}
        </div>
      )}
    </div>
  )
}
