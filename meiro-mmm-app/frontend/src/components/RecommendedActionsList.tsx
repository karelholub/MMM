import { tokens as t } from '../theme/tokens'

export type RecommendedActionItem = {
  id: string
  label: string
  benefit?: string
  requires_review?: boolean
  domain?: string
  target_page?: string
  target_section?: string
  target_tab?: string
}

interface RecommendedActionsListProps {
  actions?: RecommendedActionItem[]
  emptyMessage?: string
  onActionClick?: (action: RecommendedActionItem) => void
}

function formatDomain(domain?: string) {
  if (!domain) return null
  return domain.replace(/_/g, ' ')
}

export default function RecommendedActionsList({
  actions,
  emptyMessage = 'No immediate actions suggested.',
  onActionClick,
}: RecommendedActionsListProps) {
  if (!actions?.length) {
    return <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{emptyMessage}</div>
  }

  return (
    <div style={{ display: 'grid', gap: t.space.sm }}>
      {actions.map((action) => {
        const clickable = Boolean(onActionClick)
        return (
          <button
            key={action.id}
            type="button"
            onClick={() => onActionClick?.(action)}
            style={{
              textAlign: 'left',
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.sm,
              padding: t.space.sm,
              background: t.color.bg,
              cursor: clickable ? 'pointer' : 'default',
              display: 'grid',
              gap: 4,
            }}
          >
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
              <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{action.label}</span>
              {action.requires_review ? (
                <span
                  style={{
                    padding: '2px 6px',
                    borderRadius: t.radius.full,
                    fontSize: t.font.sizeXs,
                    color: t.color.warning,
                    background: t.color.warningSubtle,
                    border: `1px solid ${t.color.warning}`,
                  }}
                >
                  Review
                </span>
              ) : null}
              {action.domain ? (
                <span
                  style={{
                    padding: '2px 6px',
                    borderRadius: t.radius.full,
                    fontSize: t.font.sizeXs,
                    color: t.color.textMuted,
                    background: t.color.surface,
                    border: `1px solid ${t.color.borderLight}`,
                    textTransform: 'capitalize',
                  }}
                >
                  {formatDomain(action.domain)}
                </span>
              ) : null}
            </div>
            {action.benefit ? <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{action.benefit}</div> : null}
          </button>
        )
      })}
    </div>
  )
}
