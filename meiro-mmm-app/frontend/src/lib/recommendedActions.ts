import type { RecommendedActionItem } from '../components/RecommendedActionsList'
import { buildSettingsHref } from './settingsLinks'

export function navigateForRecommendedAction(
  action: RecommendedActionItem,
  options?: {
    onNavigate?: (page: any) => void
    defaultPage?: string
  },
) {
  const targetPage = action.target_page || options?.defaultPage
  if (targetPage && options?.onNavigate) {
    options.onNavigate(targetPage)
  }
  if (targetPage === 'settings' && action.target_section) {
    window.location.assign(
      buildSettingsHref(action.target_section as any, {
        tab: (action.target_tab as any) || null,
      }),
    )
    return
  }
  if (targetPage && !options?.onNavigate) {
    window.location.assign(`#${targetPage}`)
  }
}
