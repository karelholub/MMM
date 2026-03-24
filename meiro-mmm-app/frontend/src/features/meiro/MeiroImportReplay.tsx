import type { MeiroConfig, MeiroMappingState } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import type { DryRunResult, MeiroWebhookArchiveStatus } from './shared'

interface MeiroImportReplayProps {
  meiroConfig?: MeiroConfig
  meiroMappingState?: MeiroMappingState
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  meiroDryRunPending: boolean
  meiroDryRunData?: DryRunResult
  importFromMeiroPending: boolean
  reprocessWebhookArchivePending: boolean
  relativeTime: (iso?: string | null) => string
  onDryRun: () => void
  onImportFromMeiro: () => void
  onReplayArchive: () => void
}

export default function MeiroImportReplay({
  meiroConfig,
  meiroMappingState,
  meiroWebhookArchiveStatus,
  meiroDryRunPending,
  meiroDryRunData,
  importFromMeiroPending,
  reprocessWebhookArchivePending,
  relativeTime,
  onDryRun,
  onImportFromMeiro,
  onReplayArchive,
}: MeiroImportReplayProps) {
  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Import readiness</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          CDP {meiroConfig?.connected ? 'connected' : 'not connected'} · Pipes {(meiroConfig?.webhook_received_count || 0) > 0 ? 'has payloads' : 'no payloads yet'} · Mapping {meiroMappingState?.approval?.status || 'unreviewed'}
        </div>
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
          <button type="button" onClick={onDryRun} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            {meiroDryRunPending ? 'Running dry run…' : 'Run dry run'}
          </button>
          <button type="button" onClick={onImportFromMeiro} disabled={importFromMeiroPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: importFromMeiroPending ? 'wait' : 'pointer' }}>
            {importFromMeiroPending ? 'Importing…' : 'Import into attribution'}
          </button>
        </div>
        {meiroDryRunData ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Dry run found <strong>{meiroDryRunData.count}</strong> journeys.
            {meiroDryRunData.warnings?.length ? <> Warnings: {meiroDryRunData.warnings.join(' · ')}</> : <> Validation looks clean.</>}
          </div>
        ) : null}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Replay archived Pipes payloads</div>
        {meiroWebhookArchiveStatus?.available ? (
          <>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Archived batches: <strong>{meiroWebhookArchiveStatus.entries}</strong> · last received {relativeTime(meiroWebhookArchiveStatus.last_received_at)}
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Parser versions: <strong>{(meiroWebhookArchiveStatus.parser_versions || []).join(', ') || '—'}</strong>
            </div>
            <button type="button" onClick={onReplayArchive} disabled={reprocessWebhookArchivePending || meiroMappingState?.approval?.status !== 'approved'} style={{ justifySelf: 'flex-start', border: `1px solid ${t.color.accent}`, background: '#fff', color: t.color.accent, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', opacity: reprocessWebhookArchivePending || meiroMappingState?.approval?.status !== 'approved' ? 0.7 : 1 }}>
              {reprocessWebhookArchivePending ? 'Reprocessing…' : 'Replay archive into attribution'}
            </button>
            {meiroMappingState?.approval?.status !== 'approved' ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>Approve the current mapping before replaying archived payloads.</div>
            ) : null}
          </>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No archived Pipes payloads available yet.</div>
        )}
      </div>
    </div>
  )
}
