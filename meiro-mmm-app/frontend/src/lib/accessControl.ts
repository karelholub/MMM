export type AppPageKey =
  | 'overview'
  | 'alerts'
  | 'dashboard'
  | 'comparison'
  | 'paths'
  | 'campaigns'
  | 'expenses'
  | 'datasources'
  | 'mmm'
  | 'settings'
  | 'dq'
  | 'incrementality'
  | 'path_archetypes'
  | 'analytics_journeys'
  | 'datasets'

export type PermissionCheckContext = {
  rbacEnabled: boolean
  hasPermission: (key: string) => boolean
  hasAnyPermission: (keys: string[]) => boolean
}

export function canAccessPage(page: AppPageKey, context: PermissionCheckContext): boolean {
  if (!context.rbacEnabled) return true
  if (page === 'analytics_journeys' || page === 'paths' || page === 'path_archetypes') {
    return context.hasAnyPermission(['journeys.view', 'journeys.manage'])
  }
  if (page === 'alerts') return context.hasAnyPermission(['alerts.view', 'alerts.manage'])
  if (page === 'settings') return context.hasAnyPermission(['settings.view', 'settings.manage'])
  return true
}

export function canManageJourneyDefinitions(context: PermissionCheckContext): boolean {
  if (!context.rbacEnabled) return true
  return context.hasPermission('journeys.manage')
}
