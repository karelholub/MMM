import type { RecommendedActionItem } from '../components/RecommendedActionsList'

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
    const hash = action.target_tab
      ? `#settings/${action.target_section}/${action.target_tab}`
      : `#settings/${action.target_section}`
    window.location.assign(hash)
    return
  }
  if (targetPage && !options?.onNavigate) {
    window.location.assign(`#${targetPage}`)
  }
}
