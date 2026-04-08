export type SettingsSectionKey =
  | 'attribution'
  | 'segments'
  | 'kpi'
  | 'taxonomy'
  | 'measurement-models'
  | 'journeys'
  | 'access-control-users'
  | 'access-control-roles'
  | 'access-control-audit-log'
  | 'nba'
  | 'mmm'
  | 'notifications'

export type SettingsTabKey = 'overview' | 'suggestions' | 'preview' | 'advanced'

export function buildSettingsHref(
  section: SettingsSectionKey,
  options?: {
    tab?: SettingsTabKey | null
    searchParams?: Record<string, string | number | boolean | null | undefined>
  },
): string {
  const params = new URLSearchParams()
  params.set('page', 'settings')
  Object.entries(options?.searchParams ?? {}).forEach(([key, value]) => {
    if (value == null || value === '') return
    params.set(key, String(value))
  })
  const tab = options?.tab ? `/${options.tab}` : ''
  return `/?${params.toString()}#settings/${section}${tab}`
}
