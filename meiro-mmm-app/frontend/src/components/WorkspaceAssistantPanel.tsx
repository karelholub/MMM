import DecisionStatusCard from './DecisionStatusCard'
import type { RecommendedActionItem } from './RecommendedActionsList'
import { tokens as t } from '../theme/tokens'

interface WorkspaceAssistantPanelProps {
  actions?: RecommendedActionItem[]
  onActionClick?: (action: RecommendedActionItem) => void
}

const GROUPS: Array<{
  key: 'blocked' | 'warning' | 'opportunity'
  title: string
  subtitle: string
}> = [
  {
    key: 'blocked',
    title: 'Blocked',
    subtitle: 'Critical actions that are limiting trustworthy reporting or ingestion.',
  },
  {
    key: 'warning',
    title: 'Warning',
    subtitle: 'Important follow-up items that improve confidence and coverage.',
  },
  {
    key: 'opportunity',
    title: 'Opportunity',
    subtitle: 'Lower-risk improvements that can still increase value from the platform.',
  },
]

function bucketForAction(action: RecommendedActionItem): 'blocked' | 'warning' | 'opportunity' {
  const status = String(action.source_status || '').toLowerCase()
  if (status === 'blocked') return 'blocked'
  if (status === 'warning' || status === 'stale') return 'warning'
  return 'opportunity'
}

export default function WorkspaceAssistantPanel({
  actions = [],
  onActionClick,
}: WorkspaceAssistantPanelProps) {
  const grouped = GROUPS.map((group) => ({
    ...group,
    actions: actions.filter((action) => bucketForAction(action) === group.key),
  })).filter((group) => group.actions.length > 0)

  if (!grouped.length) {
    return null
  }

  return (
    <div
      style={{
        display: 'grid',
        gap: t.space.lg,
        gridTemplateColumns: grouped.length === 1 ? '1fr' : 'repeat(auto-fit, minmax(280px, 1fr))',
      }}
    >
      {grouped.map((group) => (
        <DecisionStatusCard
          key={group.key}
          title={`${group.title} (${group.actions.length})`}
          subtitle={group.subtitle}
          status={group.key === 'opportunity' ? 'ready' : group.key}
          actions={group.actions}
          onActionClick={onActionClick}
          compact
        />
      ))}
    </div>
  )
}
