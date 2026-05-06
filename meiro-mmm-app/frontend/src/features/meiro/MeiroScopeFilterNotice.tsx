import SurfaceBasisNotice from '../../components/dashboard/SurfaceBasisNotice'
import { tokens as t } from '../../theme/tokens'
import type { MeiroScopeFilterMeta } from './scopeTypes'

interface MeiroScopeFilterNoticeProps {
  scopeFilter?: MeiroScopeFilterMeta | null
  context?: 'overview' | 'campaign_selectors' | 'campaign_rows'
  marginBottom?: number | string
  variant?: 'notice' | 'inline'
}

function plural(value: number, singular: string, pluralValue = `${singular}s`) {
  return value === 1 ? singular : pluralValue
}

export default function MeiroScopeFilterNotice({
  scopeFilter,
  context = 'campaign_rows',
  marginBottom,
  variant = 'notice',
}: MeiroScopeFilterNoticeProps) {
  const labels = Number(scopeFilter?.out_of_scope_campaign_labels || 0)
  const rows = Number(scopeFilter?.campaign_rows_excluded || 0)
  const selectorsFiltered = Boolean(scopeFilter?.campaign_selectors_filtered)
  if (labels <= 0 && rows <= 0 && !selectorsFiltered) return null

  const subject =
    context === 'overview'
      ? 'Top campaigns'
      : context === 'campaign_selectors'
        ? 'Campaign selectors'
        : 'Campaign rows'

  const content = (
    <>
      <span style={{ color: t.color.text }}>
        {subject} use the Meiro production scope.
      </span>{' '}
      {rows > 0 ? (
        <>
          {rows.toLocaleString()} campaign {plural(rows, 'row')} hidden
          {labels > 0 ? ' and ' : '.'}
        </>
      ) : null}
      {labels > 0 ? (
        <>
          {labels.toLocaleString()} archived {plural(labels, 'label')} excluded.
        </>
      ) : null}
    </>
  )

  if (variant === 'inline') {
    return <div style={{ color: t.color.warning }}>{content}</div>
  }

  return (
    <SurfaceBasisNotice marginBottom={marginBottom ?? t.space.md}>
      {content}
    </SurfaceBasisNotice>
  )
}
