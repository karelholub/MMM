import { ReactNode } from 'react'
import { tokens } from '../theme/tokens'

interface ProtectedPageProps {
  blocked: boolean
  reason: string
  children: ReactNode
}

export default function ProtectedPage({ blocked, reason, children }: ProtectedPageProps) {
  if (blocked) {
    return (
      <div
        style={{
          border: `1px solid ${tokens.color.borderLight}`,
          borderRadius: tokens.radius.lg,
          background: tokens.color.surface,
          boxShadow: tokens.shadowXs,
          padding: 24,
          display: 'grid',
          gap: 10,
        }}
      >
        <h2
          style={{
            margin: 0,
            fontSize: tokens.font.sizeLg,
            fontWeight: tokens.font.weightSemibold,
            color: tokens.color.text,
          }}
        >
          No access
        </h2>
        <p style={{ margin: 0, color: tokens.color.textSecondary, fontSize: tokens.font.sizeSm }}>
          {reason}
        </p>
      </div>
    )
  }

  return <>{children}</>
}
