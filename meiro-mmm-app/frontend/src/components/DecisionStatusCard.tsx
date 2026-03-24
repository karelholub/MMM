import { tokens as t } from '../theme/tokens'
import RecommendedActionsList, { type RecommendedActionItem } from './RecommendedActionsList'

interface DecisionStatusCardProps {
  title: string
  status?: string | null
  subtitle?: string
  blockers?: string[]
  warnings?: string[]
  actions?: RecommendedActionItem[]
  onActionClick?: (action: RecommendedActionItem) => void
  compact?: boolean
}

export default function DecisionStatusCard({
  title,
  status,
  subtitle,
  blockers = [],
  warnings = [],
  actions = [],
  onActionClick,
  compact = false,
}: DecisionStatusCardProps) {
  const tone =
    status === 'blocked'
      ? { border: t.color.danger, fg: t.color.danger, bg: t.color.dangerSubtle }
      : { border: t.color.warning, fg: t.color.warning, bg: t.color.warningSubtle }

  return (
    <div
      style={{
        marginBottom: compact ? 0 : t.space.lg,
        background: tone.bg,
        border: `1px solid ${tone.border}`,
        borderRadius: t.radius.lg,
        padding: t.space.md,
        boxShadow: t.shadowSm,
        display: 'grid',
        gap: compact ? t.space.xs : t.space.sm,
      }}
    >
      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: tone.fg }}>
        {title}
      </div>
      {subtitle ? (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{subtitle}</div>
      ) : null}
      {blockers.map((item) => (
        <div key={`b:${item}`} style={{ fontSize: t.font.sizeXs, color: t.color.text }}>{item}</div>
      ))}
      {warnings.map((item) => (
        <div key={`w:${item}`} style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{item}</div>
      ))}
      {actions.length ? (
        <div style={{ display: 'grid', gap: t.space.xs, marginTop: t.space.xs }}>
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Recommended actions
          </div>
          <RecommendedActionsList actions={actions} onActionClick={onActionClick} />
        </div>
      ) : null}
    </div>
  )
}
