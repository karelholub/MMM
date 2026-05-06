import { useQuery } from '@tanstack/react-query'
import { getMeiroMeasurementPipelineSummary, type MeiroMeasurementPipelineSummary } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'

interface MeiroMeasurementScopeNoticeProps {
  compact?: boolean
}

function formatStatus(value?: string | null) {
  return String(value || 'unknown').replace(/_/g, ' ')
}

export default function MeiroMeasurementScopeNotice({ compact = false }: MeiroMeasurementScopeNoticeProps) {
  const summaryQuery = useQuery<MeiroMeasurementPipelineSummary>({
    queryKey: ['meiro-measurement-pipeline-summary'],
    queryFn: getMeiroMeasurementPipelineSummary,
    staleTime: 30_000,
  })
  const summary = summaryQuery.data
  if (!summary && summaryQuery.isLoading && compact) return null

  const sourceScope = summary?.source.source_scope
  const siteScope = summary?.source.site_scope
  const status = formatStatus(sourceScope?.status)
  const outOfScopeEvents = Number(siteScope?.out_of_scope_site_events || 0)
  const targetEvents = Number(siteScope?.target_site_events || 0)
  const targetHost = summary?.target.instance_host || 'meiro-internal.eu.pipes.meiro.io'
  const targetSites = summary?.target.site_domains?.length ? summary.target.site_domains : ['meiro.io', 'meir.store']
  const warning = summary?.status === 'blocked' || summary?.status === 'warning' || outOfScopeEvents > 0 || status !== 'target verified'

  return (
    <div
      style={{
        border: `1px solid ${warning ? t.color.warning : t.color.borderLight}`,
        background: warning ? t.color.warningMuted : t.color.bg,
        borderRadius: t.radius.md,
        padding: compact ? t.space.sm : t.space.md,
        display: 'grid',
        gap: compact ? 6 : t.space.sm,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
        <div style={{ display: 'grid', gap: 3 }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro production scope</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Reporting basis is <strong style={{ color: t.color.text }}>{targetHost}</strong> for <strong style={{ color: t.color.text }}>{targetSites.join(', ')}</strong>.
          </div>
        </div>
        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          {[
            { label: 'Source', value: summary?.source.primary_ingest_source || 'events' },
            { label: 'Archive scope', value: status },
            { label: 'Target events', value: targetEvents.toLocaleString() },
            { label: 'Out-of-scope', value: outOfScopeEvents.toLocaleString() },
          ].map((item) => (
            <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 8px', minWidth: 112 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightSemibold }}>{item.value}</div>
            </div>
          ))}
        </div>
      </div>
      {warning ? (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
          Legacy or out-of-scope replay rows may still exist in archives. Treat campaign/channel rows as production evidence only when they are backed by target-scope raw events.
        </div>
      ) : null}
    </div>
  )
}
