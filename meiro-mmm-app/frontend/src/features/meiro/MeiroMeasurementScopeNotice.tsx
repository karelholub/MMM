import { useQuery } from '@tanstack/react-query'
import { getMeiroMeasurementPipelineSummary, type MeiroMeasurementPipelineSummary } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import type { MeiroMeasurementScopeMeta } from './scopeTypes'

interface MeiroMeasurementScopeNoticeProps {
  compact?: boolean
  scope?: MeiroMeasurementScopeMeta | null
  targetHost?: string | null
  source?: string | null
}

function formatStatus(value?: string | null) {
  return String(value || 'unknown').replace(/_/g, ' ')
}

export default function MeiroMeasurementScopeNotice({ compact = false, scope, targetHost: providedTargetHost, source }: MeiroMeasurementScopeNoticeProps) {
  const summaryQuery = useQuery<MeiroMeasurementPipelineSummary>({
    queryKey: ['meiro-measurement-pipeline-summary'],
    queryFn: getMeiroMeasurementPipelineSummary,
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
    enabled: !scope,
  })
  const summary = summaryQuery.data
  if (!summary && summaryQuery.isLoading && compact) return null

  const currentWindow = summary?.source.current_window
  const currentSourceScope = currentWindow?.window_batches ? currentWindow.source_scope : null
  const currentSiteScope = currentWindow?.window_batches ? currentWindow.site_scope : null
  const sourceScope = scope?.source_scope || summary?.source.source_scope
  const siteScope = scope?.event_archive_site_scope || summary?.source.site_scope
  const effectiveSourceScope = currentSourceScope || sourceScope
  const effectiveSiteScope = currentSiteScope || siteScope
  const status = formatStatus(effectiveSourceScope?.status)
  const archiveStatus = formatStatus(sourceScope?.status)
  const currentOutOfScopeEvents = Number(effectiveSiteScope?.out_of_scope_site_events || 0)
  const targetEvents = Number(effectiveSiteScope?.target_site_events || 0)
  const targetHost = providedTargetHost || effectiveSourceScope?.target_host || sourceScope?.target_host || summary?.target.instance_host || 'meiro-internal.eu.pipes.meiro.io'
  const targetSites = scope?.target_sites?.length ? scope.target_sites : summary?.target.site_domains?.length ? summary.target.site_domains : ['meiro.io', 'meir.store']
  const sourceLabel = source || summary?.source.primary_ingest_source || 'events'
  const productionStatus = summary?.production_readiness?.status || summary?.status || 'unknown'
  const productionChecks = summary?.production_readiness?.checks || []
  const warning = productionStatus === 'blocked' || productionStatus === 'warning' || currentOutOfScopeEvents > 0 || status !== 'target verified'
  const borderColor = productionStatus === 'blocked' ? t.color.danger : warning ? t.color.warning : t.color.borderLight
  const background = productionStatus === 'blocked' ? t.color.dangerSubtle : warning ? t.color.warningMuted : t.color.bg
  const statusColor = productionStatus === 'ready' ? t.color.success : productionStatus === 'blocked' ? t.color.danger : t.color.warning

  return (
    <div
      style={{
        border: `1px solid ${borderColor}`,
        background,
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
            { label: 'Production', value: formatStatus(productionStatus), color: statusColor },
            { label: 'Source', value: sourceLabel },
            { label: 'Current scope', value: status },
            { label: 'Target events', value: targetEvents.toLocaleString() },
            { label: 'Out-of-scope', value: currentOutOfScopeEvents.toLocaleString() },
          ].map((item) => (
            <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 8px', minWidth: 112 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
              <div style={{ fontSize: t.font.sizeSm, color: item.color || t.color.text, fontWeight: t.font.weightSemibold }}>{item.value}</div>
            </div>
          ))}
        </div>
      </div>
      {!compact && summary?.production_readiness?.summary ? (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{summary.production_readiness.summary}</div>
      ) : null}
      {!compact && productionChecks.length ? (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.xs }}>
          {productionChecks.map((check) => {
            const checkColor = check.status === 'ready' ? t.color.success : check.status === 'blocked' ? t.color.danger : t.color.warning
            return (
              <div key={check.key} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, background: t.color.surface, padding: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeXs, color: checkColor, fontWeight: t.font.weightSemibold }}>{check.label}: {formatStatus(check.status)}</div>
                <div style={{ marginTop: 3, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{check.detail}</div>
              </div>
            )
          })}
        </div>
      ) : null}
      {warning ? (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
          Legacy or out-of-scope replay rows may still exist in archives. Treat campaign/channel rows as production evidence only when they are backed by target-scope raw events.
        </div>
      ) : null}
      {archiveStatus !== status ? (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Full archive scope is {archiveStatus}; current production window is {status}. Historical archive warnings are retained for hygiene but do not replace current-window readiness.
        </div>
      ) : null}
    </div>
  )
}
