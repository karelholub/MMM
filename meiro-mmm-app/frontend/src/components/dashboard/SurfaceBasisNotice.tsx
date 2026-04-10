import type { ReactNode } from 'react'
import { tokens } from '../../theme/tokens'

interface SurfaceBasisNoticeProps {
  children: ReactNode
  marginTop?: string | number
  marginBottom?: string | number
}

export default function SurfaceBasisNotice({
  children,
  marginTop,
  marginBottom,
}: SurfaceBasisNoticeProps) {
  return (
    <div
      style={{
        marginTop,
        marginBottom,
        padding: `${tokens.space.sm}px ${tokens.space.md}px`,
        borderRadius: tokens.radius.md,
        border: `1px solid ${tokens.color.borderLight}`,
        background: tokens.color.bgSubtle,
        fontSize: tokens.font.sizeSm,
        color: tokens.color.textSecondary,
      }}
    >
      {children}
    </div>
  )
}
