import { ReactNode } from 'react'
import { tokens as t } from '../../theme/tokens'

export interface InsightRowProps {
  icon?: ReactNode
  message: string
  severity?: 'info' | 'warning' | 'critical' | string
  timestamp?: string | null
  link?: { label: string; href: string } | null
  /** When set, link is rendered as a button calling this (for SPA navigation). */
  onLinkClick?: () => void
}

const SEVERITY_BG: Record<string, string> = {
  critical: t.color.dangerMuted,
  warning: t.color.warningMuted,
  info: t.color.accentMuted,
}

const SEVERITY_BORDER: Record<string, string> = {
  critical: t.color.danger,
  warning: t.color.warning,
  info: t.color.accent,
}

export default function InsightRow({ icon, message, severity = 'info', timestamp, link, onLinkClick }: InsightRowProps) {
  const key = severity.toLowerCase()
  const background = SEVERITY_BG[key] ?? t.color.bg
  const border = SEVERITY_BORDER[key] ?? t.color.borderLight

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'auto 1fr auto',
        gap: t.space.md,
        alignItems: 'center',
        background,
        borderRadius: t.radius.md,
        border: `1px solid ${border}`,
        padding: `${t.space.sm}px ${t.space.md}px`,
      }}
    >
      <div
        aria-hidden
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
          width: 28,
          height: 28,
          borderRadius: 14,
          background: '#ffffff55',
          color: border,
          fontSize: t.font.sizeSm,
        }}
      >
        {icon ?? '⚡️'}
      </div>
      <div style={{ display: 'grid', gap: 4 }}>
        <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{message}</span>
        {timestamp && (
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            {new Date(timestamp).toLocaleString()}
          </span>
        )}
      </div>
      {link &&
        (onLinkClick ? (
          <button
            type="button"
            onClick={onLinkClick}
            style={{
              fontSize: t.font.sizeSm,
              color: border,
              fontWeight: t.font.weightMedium,
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              padding: 0,
            }}
          >
            {link.label}
          </button>
        ) : (
          <a
            href={link.href}
            style={{
              fontSize: t.font.sizeSm,
              color: border,
              fontWeight: t.font.weightMedium,
              textDecoration: 'none',
            }}
          >
            {link.label}
          </a>
        ))}
    </div>
  )
}
