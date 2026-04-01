import { useEffect, useMemo, useState } from 'react'

import type { MeiroConfig, MeiroImportResult, MeiroMappingState, MeiroPullConfig, MeiroQuarantineReprocessResult, MeiroQuarantineRun, MeiroWebhookSuggestions } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import { DEFAULT_MEIRO_PULL_CONFIG, type DryRunResult, type MeiroWebhookArchiveStatus, type MeiroWebhookReprocessResult } from './shared'

interface MeiroImportReplayProps {
  meiroConfig?: MeiroConfig
  meiroPullDraft: MeiroPullConfig
  meiroMappingState?: MeiroMappingState
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  meiroEventArchiveStatus?: MeiroWebhookArchiveStatus
  meiroWebhookSuggestions?: MeiroWebhookSuggestions
  meiroDryRunPending: boolean
  meiroDryRunData?: DryRunResult
  importFromMeiroPending: boolean
  importFromMeiroResult?: MeiroImportResult | null
  reprocessWebhookArchivePending: boolean
  reprocessWebhookArchiveResult?: MeiroWebhookReprocessResult | null
  reprocessQuarantinePending: boolean
  reprocessQuarantineResult?: MeiroQuarantineReprocessResult | null
  reprocessQuarantineError?: string | null
  quarantineRuns?: { items: MeiroQuarantineRun[]; total: number }
  quarantineRunsLoading: boolean
  quarantineRunsError?: string | null
  selectedQuarantineRun?: MeiroQuarantineRun | null
  selectedQuarantineRunLoading: boolean
  selectedQuarantineRunError?: string | null
  relativeTime: (iso?: string | null) => string
  onDryRun: () => void
  onImportFromMeiro: () => void
  onReplayArchive: () => void
  onReprocessSelectedQuarantine: (recordIndices?: number[]) => void
  onSelectQuarantineRun: (runId: string) => void
}

type CleaningReportView = {
  fixed?: number
  dropped?: number
  ambiguous?: number
  duplicate_profiles?: number
  quality?: {
    average_score?: number
  }
  top_unresolved_patterns?: Array<{
    code?: string
    count?: number
  }>
}

function asCleaningReport(value: unknown): CleaningReportView | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  return value as CleaningReportView
}

export default function MeiroImportReplay({
  meiroConfig,
  meiroPullDraft,
  meiroMappingState,
  meiroWebhookArchiveStatus,
  meiroEventArchiveStatus,
  meiroWebhookSuggestions,
  meiroDryRunPending,
  meiroDryRunData,
  importFromMeiroPending,
  importFromMeiroResult,
  reprocessWebhookArchivePending,
  reprocessWebhookArchiveResult,
  reprocessQuarantinePending,
  reprocessQuarantineResult,
  reprocessQuarantineError,
  quarantineRuns,
  quarantineRunsLoading,
  quarantineRunsError,
  selectedQuarantineRun,
  selectedQuarantineRunLoading,
  selectedQuarantineRunError,
  relativeTime,
  onDryRun,
  onImportFromMeiro,
  onReplayArchive,
  onReprocessSelectedQuarantine,
  onSelectQuarantineRun,
}: MeiroImportReplayProps) {
  const [selectedRecordIndices, setSelectedRecordIndices] = useState<number[]>([])
  const [showResolvedRecords, setShowResolvedRecords] = useState(false)
  const latestImportSummary =
    importFromMeiroResult?.import_summary ||
    reprocessWebhookArchiveResult?.import_result?.import_summary ||
    meiroDryRunData?.import_summary
  const cleaningReport = asCleaningReport(latestImportSummary?.cleaning_report || meiroDryRunData?.cleaning_report)
  const replayMode = meiroPullDraft.replay_mode || DEFAULT_MEIRO_PULL_CONFIG.replay_mode
  const replaySource = meiroPullDraft.replay_archive_source || DEFAULT_MEIRO_PULL_CONFIG.replay_archive_source
  const replayScopeLabel =
    replayMode === 'all'
      ? 'Entire archive'
      : replayMode === 'date_range'
        ? `Date range${meiroPullDraft.replay_date_from ? ` from ${meiroPullDraft.replay_date_from}` : ''}${meiroPullDraft.replay_date_to ? ` to ${meiroPullDraft.replay_date_to}` : ''}`
        : `Last ${Number(meiroPullDraft.replay_archive_limit || DEFAULT_MEIRO_PULL_CONFIG.replay_archive_limit || 5000).toLocaleString()} archived batches`
  const replaySourceLabel =
    replaySource === 'events' ? 'Raw events archive' : replaySource === 'profiles' ? 'Profiles archive' : 'Auto (freshest archive)'
  const replayDiagnostics = reprocessWebhookArchiveResult?.event_reconstruction_diagnostics
  const indexedRecords = useMemo(
    () => (selectedQuarantineRun?.records || []).map((record, index) => ({ record, index })),
    [selectedQuarantineRun?.records],
  )
  const openRecordIndices = useMemo(
    () => indexedRecords
      .filter(({ record }) => String(record.remediation?.status || 'open') === 'open')
      .map(({ index }) => index),
    [indexedRecords],
  )
  const reprocessableRecordIndices = useMemo(
    () => indexedRecords
      .filter(({ record }) => !!record.original && typeof record.original === 'object')
      .map(({ index }) => index),
    [indexedRecords],
  )
  const recordsForDisplay = useMemo(
    () => indexedRecords.filter(({ record }) => {
      if (showResolvedRecords) return true
      return String(record.remediation?.status || 'open') === 'open'
    }),
    [indexedRecords, showResolvedRecords],
  )
  const visibleRecords = useMemo(() => recordsForDisplay.slice(0, 10), [recordsForDisplay])
  const visibleRecordIndices = useMemo(
    () => visibleRecords
      .filter(({ record }) => !!record.original && typeof record.original === 'object')
      .map(({ index }) => index),
    [visibleRecords],
  )
  const openRecordCount = openRecordIndices.length
  const remediatedRecordCount = Math.max(0, indexedRecords.length - openRecordCount)

  useEffect(() => {
    setSelectedRecordIndices([])
    setShowResolvedRecords(false)
  }, [selectedQuarantineRun?.id])

  useEffect(() => {
    setSelectedRecordIndices((prev) => {
      const next = prev.filter((index) => (
        reprocessableRecordIndices.includes(index) &&
        (
          showResolvedRecords ||
          String(indexedRecords[index]?.record?.remediation?.status || 'open') === 'open'
        )
      ))
      return next.length === prev.length && next.every((value, position) => value === prev[position]) ? prev : next
    })
  }, [indexedRecords, reprocessableRecordIndices, showResolvedRecords])

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
          <div style={{ display: 'grid', gap: 8 }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Dry run found <strong>{meiroDryRunData.count}</strong> journeys.
              {meiroDryRunData.warnings?.length ? <> Warnings: {meiroDryRunData.warnings.join(' · ')}</> : <> Validation looks clean.</>}
            </div>
            {cleaningReport ? (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: t.space.sm }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Fixed</div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(cleaningReport.fixed || 0).toLocaleString()}</div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Dropped / quarantined</div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(cleaningReport.dropped || latestImportSummary?.quarantined || 0).toLocaleString()}</div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Ambiguous</div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(cleaningReport.ambiguous || 0).toLocaleString()}</div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Duplicate profiles</div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(cleaningReport.duplicate_profiles || 0).toLocaleString()}</div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Avg quality</div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(cleaningReport.quality?.average_score || 0).toFixed(1)}</div>
                </div>
              </div>
            ) : null}
            {!!cleaningReport?.top_unresolved_patterns?.length && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Top unresolved: {cleaningReport.top_unresolved_patterns.map((item: any) => `${item.code} (${item.count})`).join(' · ')}
              </div>
            )}
          </div>
        ) : null}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Replay archived Pipes payloads</div>
        {(meiroWebhookArchiveStatus?.available || meiroEventArchiveStatus?.available) ? (
          <>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Profile payloads received: <strong>{Number(meiroConfig?.webhook_received_count || 0).toLocaleString()}</strong> · profile archive: <strong>{Number(meiroWebhookArchiveStatus?.entries || 0).toLocaleString()}</strong> batches / <strong>{Number(meiroWebhookArchiveStatus?.profiles_received || 0).toLocaleString()}</strong> payloads
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Raw events received: <strong>{Number(meiroConfig?.event_webhook_received_count || 0).toLocaleString()}</strong> · event archive: <strong>{Number(meiroEventArchiveStatus?.entries || 0).toLocaleString()}</strong> batches / <strong>{Number(meiroEventArchiveStatus?.events_received || 0).toLocaleString()}</strong> events
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Replay source: <strong>{replaySourceLabel}</strong> · replay scope: <strong>{replayScopeLabel}</strong>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Latest profile archive: <strong>{relativeTime(meiroWebhookArchiveStatus?.last_received_at)}</strong> · latest event archive: <strong>{relativeTime(meiroEventArchiveStatus?.last_received_at)}</strong>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Parser versions: <strong>{Array.from(new Set([...(meiroWebhookArchiveStatus?.parser_versions || []), ...(meiroEventArchiveStatus?.parser_versions || [])])).join(', ') || '—'}</strong>
            </div>
            {meiroWebhookSuggestions?.event_stream_diagnostics?.available ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, display: 'grid', gap: 6 }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Raw event archive quality</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Usable names <strong>{(meiroWebhookSuggestions.event_stream_diagnostics.usable_event_name_share * 100).toFixed(1)}%</strong>
                  {' '}· source/medium <strong>{(meiroWebhookSuggestions.event_stream_diagnostics.source_medium_share * 100).toFixed(1)}%</strong>
                  {' '}· referrer-only <strong>{(meiroWebhookSuggestions.event_stream_diagnostics.referrer_only_share * 100).toFixed(1)}%</strong>
                  {' '}· conversion linkage <strong>{(meiroWebhookSuggestions.event_stream_diagnostics.conversion_linkage_share * 100).toFixed(1)}%</strong>
                </div>
                {(meiroWebhookSuggestions.event_stream_diagnostics.warnings || []).length ? (
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>
                    {(meiroWebhookSuggestions.event_stream_diagnostics.warnings || []).join(' · ')}
                  </div>
                ) : null}
              </div>
            ) : null}
            {replayDiagnostics ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Latest event replay reconstruction</div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: t.space.sm }}>
                  {[
                    { label: 'Events loaded', value: Number(replayDiagnostics.events_loaded || 0).toLocaleString() },
                    { label: 'Profiles reconstructed', value: Number(replayDiagnostics.profiles_reconstructed || 0).toLocaleString() },
                    { label: 'Touchpoints reconstructed', value: Number(replayDiagnostics.touchpoints_reconstructed || 0).toLocaleString() },
                    { label: 'Conversions reconstructed', value: Number(replayDiagnostics.conversions_reconstructed || 0).toLocaleString() },
                    { label: 'Attributable profiles', value: Number(replayDiagnostics.attributable_profiles || 0).toLocaleString() },
                    { label: 'Valid journeys', value: Number(replayDiagnostics.journeys_valid || 0).toLocaleString() },
                    { label: 'Quarantined', value: Number(replayDiagnostics.journeys_quarantined || 0).toLocaleString() },
                    { label: 'Persisted', value: Number(replayDiagnostics.journeys_persisted || 0).toLocaleString() },
                    { label: 'Converted', value: Number(replayDiagnostics.journeys_converted || 0).toLocaleString() },
                  ].map((item) => (
                    <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                      <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{item.value}</div>
                    </div>
                  ))}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Average <strong>{Number(replayDiagnostics.avg_events_per_profile || 0).toFixed(2)}</strong> events per reconstructed profile in the latest raw-event replay.
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Average <strong>{Number(replayDiagnostics.avg_touchpoints_per_profile || 0).toFixed(2)}</strong> touchpoints and <strong>{Number(replayDiagnostics.avg_conversions_per_profile || 0).toFixed(2)}</strong> conversions per reconstructed profile.
                  {typeof replayDiagnostics.persisted_from_attributable_share === 'number' ? (
                    <> Persisted retention from attributable profiles: <strong>{(replayDiagnostics.persisted_from_attributable_share * 100).toFixed(1)}%</strong>.</>
                  ) : null}
                </div>
                {(replayDiagnostics.warnings || []).length ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                    {(replayDiagnostics.warnings || []).join(' · ')}
                  </div>
                ) : null}
              </div>
            ) : null}
            <button type="button" onClick={onReplayArchive} disabled={reprocessWebhookArchivePending || meiroMappingState?.approval?.status !== 'approved'} style={{ justifySelf: 'flex-start', border: `1px solid ${t.color.accent}`, background: '#fff', color: t.color.accent, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', opacity: reprocessWebhookArchivePending || meiroMappingState?.approval?.status !== 'approved' ? 0.7 : 1 }}>
              {reprocessWebhookArchivePending ? 'Reprocessing…' : 'Replay archive into attribution'}
            </button>
            {meiroMappingState?.approval?.status !== 'approved' ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>Approve the current mapping before replaying archived payloads.</div>
            ) : null}
          </>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No archived Pipes profiles or raw events available yet.</div>
        )}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Quarantine review</div>
        {quarantineRunsLoading ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading quarantine runs…</div>
        ) : quarantineRunsError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{quarantineRunsError}</div>
        ) : !(quarantineRuns?.items || []).length ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No quarantine runs recorded yet.</div>
        ) : (
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              {(quarantineRuns?.items || []).map((run) => (
                <button
                  key={run.id}
                  type="button"
                  onClick={() => onSelectQuarantineRun(run.id)}
                  style={{ border: `1px solid ${t.color.border}`, background: selectedQuarantineRun?.id === run.id ? t.color.accentMuted : t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', textAlign: 'left' }}
                >
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>{run.source}</div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{relativeTime(run.created_at)} · {Number((run.records || []).length).toLocaleString()} records</div>
                </button>
              ))}
            </div>

            {selectedQuarantineRunLoading ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading selected quarantine run…</div>
            ) : selectedQuarantineRunError ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{selectedQuarantineRunError}</div>
            ) : selectedQuarantineRun ? (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  {selectedQuarantineRun.source} · {relativeTime(selectedQuarantineRun.created_at)} · {openRecordCount} open · {remediatedRecordCount} remediated · {indexedRecords.length} total quarantined records
                </div>
                <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                  <button
                    type="button"
                    onClick={() => onReprocessSelectedQuarantine()}
                    disabled={reprocessQuarantinePending || reprocessableRecordIndices.length === 0}
                    style={{ border: `1px solid ${t.color.accent}`, background: '#fff', color: t.color.accent, borderRadius: t.radius.sm, padding: '8px 10px', cursor: reprocessQuarantinePending ? 'wait' : 'pointer', opacity: reprocessQuarantinePending || reprocessableRecordIndices.length === 0 ? 0.7 : 1 }}
                  >
                    {reprocessQuarantinePending ? 'Reprocessing run…' : 'Reprocess run into attribution'}
                  </button>
                  <button
                    type="button"
                    onClick={() => onReprocessSelectedQuarantine(selectedRecordIndices)}
                    disabled={reprocessQuarantinePending || selectedRecordIndices.length === 0}
                    style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '8px 10px', cursor: reprocessQuarantinePending ? 'wait' : 'pointer', opacity: reprocessQuarantinePending || selectedRecordIndices.length === 0 ? 0.7 : 1 }}
                  >
                    {reprocessQuarantinePending ? 'Reprocessing selected…' : `Reprocess selected (${selectedRecordIndices.length})`}
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedRecordIndices(visibleRecordIndices)}
                    disabled={visibleRecordIndices.length === 0}
                    style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
                  >
                    Select shown
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedRecordIndices([])}
                    disabled={selectedRecordIndices.length === 0}
                    style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
                  >
                    Clear selection
                  </button>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, alignSelf: 'center' }}>
                    Re-runs quarantined originals through the current mapping and sanitation rules, then appends recovered journeys to attribution.
                  </div>
                </div>
                {reprocessableRecordIndices.length === 0 ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                    This quarantine run has no original records available for reprocessing.
                  </div>
                ) : null}
                {reprocessQuarantineError ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{reprocessQuarantineError}</div>
                ) : null}
                <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.text }}>
                  <input type="checkbox" checked={showResolvedRecords} onChange={(e) => setShowResolvedRecords(e.target.checked)} />
                  Show remediated records
                </label>
                {reprocessQuarantineResult?.source_quarantine_run_id === selectedQuarantineRun.id ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Reprocessed <strong>{reprocessQuarantineResult.reprocessed_count}</strong> records and persisted <strong>{reprocessQuarantineResult.persisted_count}</strong> total journeys.
                    {reprocessQuarantineResult.quarantine_run_id ? <> Remaining failures were written to quarantine run <strong>{reprocessQuarantineResult.quarantine_run_id}</strong>.</> : null}
                  </div>
                ) : null}
                {!recordsForDisplay.length ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {showResolvedRecords ? 'No quarantined records in this run.' : 'No open quarantined records remain in this run.'}
                  </div>
                ) : null}
                {visibleRecords.map(({ record, index }) => (
                  <details key={`${record.journey_id || record.customer_id || 'record'}-${index}`} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface, opacity: String(record.remediation?.status || 'open') === 'open' ? 1 : 0.75 }}>
                    <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text, display: 'flex', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                      <input
                        type="checkbox"
                        checked={selectedRecordIndices.includes(index)}
                        disabled={
                          String(record.remediation?.status || 'open') !== 'open' ||
                          !(record.original && typeof record.original === 'object')
                        }
                        onChange={(e) => {
                          const checked = e.target.checked
                          setSelectedRecordIndices((prev) => (
                            checked
                              ? (prev.includes(index) ? prev : [...prev, index].sort((a, b) => a - b))
                              : prev.filter((value) => value !== index)
                          ))
                        }}
                        onClick={(e) => e.stopPropagation()}
                      />
                      <span>{(record.customer_id || record.journey_id || 'Record')} · {(record.reason_codes || []).join(', ')} · quality {record.quality?.score ?? '—'} ({record.quality?.band || 'n/a'}) · status {String(record.remediation?.status || 'open')}</span>
                    </summary>
                    <div style={{ display: 'grid', gap: t.space.sm, marginTop: t.space.sm }}>
                      {record.remediation?.updated_at ? (
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                          Remediation: {String(record.remediation.status || 'open')} · {relativeTime(record.remediation.updated_at)}
                          {record.remediation.note ? <> · {record.remediation.note}</> : null}
                        </div>
                      ) : null}
                      {!!record.remediation?.history?.length ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          History: {record.remediation.history.map((entry) => `${entry.status || 'open'}${entry.at ? ` at ${relativeTime(entry.at)}` : ''}${entry.note ? ` (${entry.note})` : ''}`).join(' · ')}
                        </div>
                      ) : null}
                      {!!record.reasons?.length && (
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                          {record.reasons.map((reason) => `${reason.code}: ${reason.message}`).join(' · ')}
                        </div>
                      )}
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: t.space.sm }}>
                        <div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 4 }}>Original</div>
                          <pre style={{ margin: 0, padding: t.space.sm, borderRadius: t.radius.sm, background: t.color.bgSubtle, overflowX: 'auto', fontSize: 11 }}>{JSON.stringify(record.original || {}, null, 2)}</pre>
                        </div>
                        <div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 4 }}>Normalized</div>
                          <pre style={{ margin: 0, padding: t.space.sm, borderRadius: t.radius.sm, background: t.color.bgSubtle, overflowX: 'auto', fontSize: 11 }}>{JSON.stringify(record.normalized || {}, null, 2)}</pre>
                        </div>
                      </div>
                    </div>
                  </details>
                ))}
                {recordsForDisplay.length > 10 ? (
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    Showing first 10 records. Use selection on the visible slice, or reprocess the full run.
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  )
}
