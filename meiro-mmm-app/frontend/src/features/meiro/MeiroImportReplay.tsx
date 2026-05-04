import { useEffect, useMemo, useState } from 'react'

import type { DeciEngineEventsImportPayload, DeciEngineEventsImportResult } from '../../connectors/deciengineConnector'
import type { MeiroConfig, MeiroImportResult, MeiroMappingState, MeiroPullConfig, MeiroQuarantineReprocessResult, MeiroQuarantineRun, MeiroWebhookSuggestions } from '../../connectors/meiroConnector'
import { apiGetJson, withQuery } from '../../lib/apiClient'
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
  deciEngineImportDraft: DeciEngineEventsImportPayload
  deciEngineImportPending: boolean
  deciEngineImportResult?: DeciEngineEventsImportResult | null
  deciEngineImportError?: string | null
  deciEngineConfigSaving?: boolean
  deciEngineConfigSaved?: boolean
  deciEngineConfigError?: string | null
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
  onDeciEngineImportDraftChange: (draft: DeciEngineEventsImportPayload) => void
  onImportDeciEngineEvents: () => void
  onSaveDeciEngineConfig: () => void
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

type ActivationMeasurementSummary = {
  object?: { type?: string; id?: string; match_aliases?: string[] }
  summary?: {
    matched_touchpoints?: number
    matched_journeys?: number
    matched_profiles?: number
    conversions?: number
    revenue?: number
    conversion_rate?: number | null
    activation_metadata_coverage?: number
    variants?: string[]
    experiments?: string[]
    placements?: string[]
  }
  evidence?: {
    data_quality?: { status?: string; warnings?: string[]; activation_metadata_coverage?: number }
  }
  recommended_actions?: Array<{ id?: string; label?: string; reason?: string }>
}

type ActivationMeasurementEvidence = {
  total_matches?: number
  limit?: number
  items?: Array<{
    journey_id?: string
    profile_id?: string
    touchpoint_ts?: string
    conversion_ts?: string
    converted?: boolean
    revenue?: number
    channel?: string
    campaign?: string
    campaign_id?: string
    activation?: Record<string, unknown>
  }>
}

const ACTIVATION_OBJECT_TYPES = ['campaign', 'asset', 'content', 'bundle', 'offer', 'decision', 'decision_stack', 'experiment', 'variant', 'placement', 'template']

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
  deciEngineImportDraft,
  deciEngineImportPending,
  deciEngineImportResult,
  deciEngineImportError,
  deciEngineConfigSaving = false,
  deciEngineConfigSaved = false,
  deciEngineConfigError,
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
  onDeciEngineImportDraftChange,
  onImportDeciEngineEvents,
  onSaveDeciEngineConfig,
  onReplayArchive,
  onReprocessSelectedQuarantine,
  onSelectQuarantineRun,
}: MeiroImportReplayProps) {
  const [selectedRecordIndices, setSelectedRecordIndices] = useState<number[]>([])
  const [showResolvedRecords, setShowResolvedRecords] = useState(false)
  const [measurementDraft, setMeasurementDraft] = useState({
    object_type: 'campaign',
    object_id: '',
    native_meiro_campaign_id: '',
    creative_asset_id: '',
    native_meiro_asset_id: '',
    offer_catalog_id: '',
    native_meiro_catalog_id: '',
  })
  const [measurementPending, setMeasurementPending] = useState(false)
  const [measurementError, setMeasurementError] = useState<string | null>(null)
  const [measurementResult, setMeasurementResult] = useState<ActivationMeasurementSummary | null>(null)
  const [measurementEvidence, setMeasurementEvidence] = useState<ActivationMeasurementEvidence | null>(null)
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
  const canImportDeciEngineEvents = Boolean(
    deciEngineImportDraft.source_url.trim() && (deciEngineImportDraft.user_email || '').trim(),
  )
  const updateDeciEngineImportDraft = (patch: Partial<DeciEngineEventsImportPayload>) => {
    onDeciEngineImportDraftChange({ ...deciEngineImportDraft, ...patch })
  }
  const runActivationMeasurement = async () => {
    setMeasurementPending(true)
    setMeasurementError(null)
    try {
      const result = await apiGetJson<ActivationMeasurementSummary>(
        withQuery('/api/measurement/activation-summary', measurementDraft),
        { fallbackMessage: 'Failed to load activation measurement summary' },
      )
      const evidence = await apiGetJson<ActivationMeasurementEvidence>(
        withQuery('/api/measurement/activation-evidence', { ...measurementDraft, limit: 5 }),
        { fallbackMessage: 'Failed to load activation measurement evidence' },
      )
      setMeasurementResult(result)
      setMeasurementEvidence(evidence)
    } catch (error) {
      setMeasurementError((error as Error)?.message || 'Failed to load activation measurement summary')
      setMeasurementEvidence(null)
    } finally {
      setMeasurementPending(false)
    }
  }

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
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Import deciEngine activation events</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Pull persisted in-app events from deciEngine and replace attribution journeys with that activation stream.</div>
          </div>
          <button
            type="button"
            onClick={onImportDeciEngineEvents}
            disabled={deciEngineImportPending || !canImportDeciEngineEvents}
            style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: deciEngineImportPending ? 'wait' : 'pointer', opacity: deciEngineImportPending || !canImportDeciEngineEvents ? 0.7 : 1 }}
          >
            {deciEngineImportPending ? 'Importing…' : canImportDeciEngineEvents ? 'Import activation events' : 'Enter user email'}
          </button>
          <button
            type="button"
            onClick={onSaveDeciEngineConfig}
            disabled={deciEngineConfigSaving || !deciEngineImportDraft.source_url.trim()}
            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: deciEngineConfigSaving ? 'wait' : 'pointer', opacity: deciEngineConfigSaving || !deciEngineImportDraft.source_url.trim() ? 0.7 : 1 }}
          >
            {deciEngineConfigSaving ? 'Saving…' : 'Save source settings'}
          </button>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Source URL
            <input value={deciEngineImportDraft.source_url} onChange={(e) => updateDeciEngineImportDraft({ source_url: e.target.value })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            User email
            <input type="email" value={deciEngineImportDraft.user_email || ''} onChange={(e) => updateDeciEngineImportDraft({ user_email: e.target.value })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Profile filter
            <input value={deciEngineImportDraft.profileId || ''} onChange={(e) => updateDeciEngineImportDraft({ profileId: e.target.value })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Campaign filter
            <input value={deciEngineImportDraft.campaignKey || ''} onChange={(e) => updateDeciEngineImportDraft({ campaignKey: e.target.value })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Limit
            <input type="number" min={1} max={500} value={deciEngineImportDraft.limit || 500} onChange={(e) => updateDeciEngineImportDraft({ limit: Math.max(1, Math.min(Number(e.target.value || 500), 500)) })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
        </div>
        {deciEngineImportError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{deciEngineImportError}</div>
        ) : deciEngineConfigError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{deciEngineConfigError}</div>
        ) : !canImportDeciEngineEvents ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>User email is required because deciEngine authorizes the event feed through `X-User-Email`.</div>
        ) : deciEngineConfigSaved ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.success }}>deciEngine event-source settings saved.</div>
        ) : deciEngineImportResult ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            {deciEngineImportResult.message || `Loaded ${Number(deciEngineImportResult.count || 0).toLocaleString()} journeys from deciEngine activation events`}.
            {deciEngineImportResult.import_summary ? <> Valid {Number(deciEngineImportResult.import_summary.valid || 0).toLocaleString()} · invalid {Number(deciEngineImportResult.import_summary.invalid || 0).toLocaleString()} · converted {Number(deciEngineImportResult.import_summary.converted || 0).toLocaleString()}</> : null}
          </div>
        ) : null}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Activation measurement lookup</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Check whether imported journeys can measure a campaign, asset, offer, or decision object by Prism/deciEngine IDs.</div>
          </div>
          <button
            type="button"
            onClick={() => void runActivationMeasurement()}
            disabled={measurementPending || !measurementDraft.object_id.trim()}
            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: measurementPending ? 'wait' : 'pointer', opacity: measurementPending || !measurementDraft.object_id.trim() ? 0.7 : 1 }}
          >
            {measurementPending ? 'Checking…' : 'Check measurement'}
          </button>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: t.space.sm }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Object type
            <select value={measurementDraft.object_type} onChange={(e) => setMeasurementDraft((prev) => ({ ...prev, object_type: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: t.color.surface }}>
              {ACTIVATION_OBJECT_TYPES.map((type) => <option key={type} value={type}>{type}</option>)}
            </select>
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Object ID
            <input value={measurementDraft.object_id} onChange={(e) => setMeasurementDraft((prev) => ({ ...prev, object_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Native campaign ID
            <input value={measurementDraft.native_meiro_campaign_id} onChange={(e) => setMeasurementDraft((prev) => ({ ...prev, native_meiro_campaign_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Creative asset ID
            <input value={measurementDraft.creative_asset_id} onChange={(e) => setMeasurementDraft((prev) => ({ ...prev, creative_asset_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.text }}>
            Offer catalog ID
            <input value={measurementDraft.offer_catalog_id} onChange={(e) => setMeasurementDraft((prev) => ({ ...prev, offer_catalog_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
          </label>
        </div>
        {measurementError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{measurementError}</div>
        ) : measurementResult?.summary ? (
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: t.space.sm }}>
              {[
                { label: 'Touchpoints', value: Number(measurementResult.summary.matched_touchpoints || 0).toLocaleString() },
                { label: 'Journeys', value: Number(measurementResult.summary.matched_journeys || 0).toLocaleString() },
                { label: 'Profiles', value: Number(measurementResult.summary.matched_profiles || 0).toLocaleString() },
                { label: 'Conversions', value: Number(measurementResult.summary.conversions || 0).toLocaleString() },
                { label: 'Revenue', value: Number(measurementResult.summary.revenue || 0).toLocaleString(undefined, { maximumFractionDigits: 2 }) },
                { label: 'Metadata coverage', value: `${(Number(measurementResult.summary.activation_metadata_coverage || 0) * 100).toFixed(1)}%` },
              ].map((item) => (
                <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{item.value}</div>
                </div>
              ))}
            </div>
            {!!measurementResult.recommended_actions?.length ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                {measurementResult.recommended_actions.map((action) => action.label || action.reason || action.id).join(' · ')}
              </div>
            ) : null}
            {!!measurementEvidence?.items?.length ? (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  Evidence rows ({Number(measurementEvidence.total_matches || 0).toLocaleString()} matched)
                </div>
                {measurementEvidence.items.map((item, index) => {
                  const activation = item.activation || {}
                  const key = `${item.journey_id || item.profile_id || 'journey'}-${index}`
                  const ids = [
                    activation.activation_campaign_id ? `activation ${activation.activation_campaign_id}` : '',
                    activation.native_meiro_campaign_id ? `native campaign ${activation.native_meiro_campaign_id}` : '',
                    activation.creative_asset_id ? `creative ${activation.creative_asset_id}` : '',
                    activation.native_meiro_asset_id ? `native asset ${activation.native_meiro_asset_id}` : '',
                    activation.offer_catalog_id ? `catalog ${activation.offer_catalog_id}` : '',
                    activation.native_meiro_catalog_id ? `native catalog ${activation.native_meiro_catalog_id}` : '',
                  ].filter(Boolean).join(' · ')
                  return (
                    <div key={key} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, display: 'grid', gap: 4 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap' }}>
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.profile_id || item.journey_id || 'Unknown profile'}</div>
                        <div style={{ fontSize: t.font.sizeXs, color: item.converted ? t.color.success : t.color.textMuted }}>{item.converted ? 'Converted' : 'Not converted'} · revenue {Number(item.revenue || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {item.channel || 'unknown channel'} · {item.campaign || item.campaign_id || 'unknown campaign'} · touchpoint {item.touchpoint_ts ? relativeTime(item.touchpoint_ts) : '—'}
                      </div>
                      {ids ? <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{ids}</div> : null}
                    </div>
                  )
                })}
              </div>
            ) : measurementEvidence ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No evidence rows matched this activation object.</div>
            ) : null}
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
