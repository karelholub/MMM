import { useEffect, useMemo, useState } from 'react'

import type { DeciEngineEventsImportPayload, DeciEngineEventsImportResult } from '../../connectors/deciengineConnector'
import type { MeiroConfig, MeiroImportResult, MeiroMappingState, MeiroPullConfig, MeiroQuarantineReprocessResult, MeiroQuarantineRun, MeiroWebhookSuggestions } from '../../connectors/meiroConnector'
import { apiGetJson, apiSendJson, withQuery } from '../../lib/apiClient'
import { buildJourneyHypothesisSeedHref } from '../../lib/journeyLinks'
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

type ActivationMeasurementObject = {
  object_type: string
  object_id: string
  label?: string
  aliases?: string[]
  matched_touchpoints?: number
  matched_journeys?: number
  matched_profiles?: number
  conversions?: number
  revenue?: number
  source_systems?: string[]
  last_touchpoint_at?: string | null
}

type ActivationFeedbackItem = {
  object?: {
    type?: string
    id?: string
    label?: string
    aliases?: string[]
    source_systems?: string[]
  }
  recommendation?: string
  status?: string
  title?: string
  reason?: string
  action?: { id?: string; label?: string; target?: string }
  evidence?: {
    matched_touchpoints?: number
    matched_journeys?: number
    conversions?: number
    conversion_rate?: number
    revenue?: number
    last_touchpoint_at?: string | null
  }
}

type ActivationFeedbackResponse = {
  items?: ActivationFeedbackItem[]
  decision?: { status?: string; subtitle?: string; warnings?: string[]; blockers?: string[] }
  summary?: { ready?: number; warning?: number; setup?: number }
}

type ActivationFeedbackExport = {
  schema_version?: string
  generated_at?: string
  generated_by?: string
  summary?: { signals?: number; total_candidates?: number; ready?: number; warning?: number; setup?: number }
  signals?: Array<{
    signal_id?: string
    object?: { type?: string; id?: string; label?: string; aliases?: string[]; source_systems?: string[] }
    recommendation?: string
    status?: string
    decision_engine_hint?: { suggested_action?: string; eligible_for_policy_input?: boolean; requires_human_review?: boolean }
  }>
}

type ActivationFeedbackExportRun = {
  id?: string
  created_at?: string | null
  created_by?: string | null
  schema_version?: string
  summary?: { signals?: number; total_candidates?: number; ready?: number; warning?: number; setup?: number }
  decision?: { status?: string; subtitle?: string; warnings?: string[]; blockers?: string[] }
  payload?: ActivationFeedbackExport
}

type MeiroSegmentImportResponse = {
  status?: string
  message?: string
  summary?: {
    source?: string
    meiro_pipes?: number
    cdp_segments?: number
    pipes_webhook_segments?: number
    activation_ready?: number
  }
  items?: Array<{
    id?: string
    name?: string
    source_label?: string
    size?: number | null
    external_segment_id?: string | null
  }>
}

type JourneyDefinitionsResponse = {
  items?: Array<{ id?: string; name?: string; conversion_kpi_id?: string | null }>
}

const ACTIVATION_OBJECT_TYPES = ['campaign', 'asset', 'content', 'bundle', 'offer', 'decision', 'decision_stack', 'experiment', 'variant', 'placement', 'template']

function initialMeasurementDraftFromUrl() {
  const fallback = {
    object_type: 'campaign',
    object_id: '',
    native_meiro_campaign_id: '',
    creative_asset_id: '',
    native_meiro_asset_id: '',
    offer_catalog_id: '',
    native_meiro_catalog_id: '',
  }
  if (typeof window === 'undefined') return fallback
  const params = new URLSearchParams(window.location.search)
  const objectType = params.get('activation_object_type') || fallback.object_type
  return {
    ...fallback,
    object_type: ACTIVATION_OBJECT_TYPES.includes(objectType) ? objectType : fallback.object_type,
    object_id: params.get('activation_object_id') || '',
    native_meiro_campaign_id: params.get('native_meiro_campaign_id') || '',
    creative_asset_id: params.get('creative_asset_id') || '',
    native_meiro_asset_id: params.get('native_meiro_asset_id') || '',
    offer_catalog_id: params.get('offer_catalog_id') || '',
    native_meiro_catalog_id: params.get('native_meiro_catalog_id') || '',
  }
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
  const [measurementDraft, setMeasurementDraft] = useState(initialMeasurementDraftFromUrl)
  const [measurementPending, setMeasurementPending] = useState(false)
  const [measurementError, setMeasurementError] = useState<string | null>(null)
  const [measurementResult, setMeasurementResult] = useState<ActivationMeasurementSummary | null>(null)
  const [measurementEvidence, setMeasurementEvidence] = useState<ActivationMeasurementEvidence | null>(null)
  const [autoMeasurementStarted, setAutoMeasurementStarted] = useState(false)
  const [activationObjects, setActivationObjects] = useState<ActivationMeasurementObject[]>([])
  const [activationObjectsPending, setActivationObjectsPending] = useState(false)
  const [activationObjectsError, setActivationObjectsError] = useState<string | null>(null)
  const [activationFeedback, setActivationFeedback] = useState<ActivationFeedbackResponse | null>(null)
  const [activationFeedbackExport, setActivationFeedbackExport] = useState<ActivationFeedbackExport | null>(null)
  const [activationFeedbackExportRuns, setActivationFeedbackExportRuns] = useState<ActivationFeedbackExportRun[]>([])
  const [activationFeedbackExportPending, setActivationFeedbackExportPending] = useState(false)
  const [activationFeedbackExportError, setActivationFeedbackExportError] = useState<string | null>(null)
  const [meiroSegmentImportSource, setMeiroSegmentImportSource] = useState<'pipes_webhook' | 'cdp'>('pipes_webhook')
  const [meiroSegmentImportPending, setMeiroSegmentImportPending] = useState(false)
  const [meiroSegmentImportError, setMeiroSegmentImportError] = useState<string | null>(null)
  const [meiroSegmentImportResult, setMeiroSegmentImportResult] = useState<MeiroSegmentImportResponse | null>(null)
  const [journeySeedDefinition, setJourneySeedDefinition] = useState<{ id?: string; name?: string } | null>(null)
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
  const measurementDraftForObject = (
    item: ActivationMeasurementObject,
    prev = measurementDraft,
  ) => {
    const alias = (item.aliases || []).find((value) => value.startsWith('meiro-')) || ''
    return {
      ...prev,
      object_type: item.object_type,
      object_id: item.object_id,
      native_meiro_campaign_id: item.object_type === 'campaign' ? alias : prev.native_meiro_campaign_id,
      creative_asset_id: item.object_type === 'asset' ? item.object_id : prev.creative_asset_id,
      native_meiro_asset_id: item.object_type === 'asset' ? alias : prev.native_meiro_asset_id,
      offer_catalog_id: item.object_type === 'bundle' ? item.object_id : prev.offer_catalog_id,
    }
  }
  const loadActivationObjects = async () => {
    setActivationObjectsPending(true)
    setActivationObjectsError(null)
    try {
      const [objects, feedback, exports] = await Promise.all([
        apiGetJson<{ items?: ActivationMeasurementObject[] }>(
          withQuery('/api/measurement/activation-objects', { limit: 12 }),
          { fallbackMessage: 'Failed to load measurable activation objects' },
        ),
        apiGetJson<ActivationFeedbackResponse>(
          withQuery('/api/measurement/activation-feedback', { limit: 5 }),
          { fallbackMessage: 'Failed to load activation feedback' },
        ),
        apiGetJson<{ items?: ActivationFeedbackExportRun[] }>(
          withQuery('/api/measurement/activation-feedback/exports', { limit: 3 }),
          { fallbackMessage: 'Failed to load activation feedback exports' },
        ).catch(() => ({ items: [] })),
      ])
      setActivationObjects(objects.items || [])
      setActivationFeedback(feedback)
      setActivationFeedbackExportRuns(exports.items || [])
    } catch (error) {
      setActivationObjectsError((error as Error)?.message || 'Failed to load measurable activation objects')
    } finally {
      setActivationObjectsPending(false)
    }
  }
  const selectActivationObject = (item: ActivationMeasurementObject) => {
    setMeasurementDraft((prev) => measurementDraftForObject(item, prev))
  }
  const loadActivationFeedbackExport = async () => {
    setActivationFeedbackExportPending(true)
    setActivationFeedbackExportError(null)
    try {
      const result = await apiGetJson<ActivationFeedbackExport>(
        withQuery('/api/measurement/activation-feedback/export', { limit: 20 }),
        { fallbackMessage: 'Failed to build activation feedback export' },
      )
      setActivationFeedbackExport(result)
    } catch (error) {
      setActivationFeedbackExportError((error as Error)?.message || 'Failed to build activation feedback export')
    } finally {
      setActivationFeedbackExportPending(false)
    }
  }
  const createActivationFeedbackExport = async () => {
    setActivationFeedbackExportPending(true)
    setActivationFeedbackExportError(null)
    try {
      const run = await apiSendJson<ActivationFeedbackExportRun>(
        withQuery('/api/measurement/activation-feedback/exports', { limit: 20 }),
        'POST',
        {},
        { fallbackMessage: 'Failed to create activation feedback export' },
      )
      setActivationFeedbackExport(run.payload || null)
      setActivationFeedbackExportRuns((prev) => [
        {
          id: run.id,
          created_at: run.created_at,
          created_by: run.created_by,
          schema_version: run.schema_version,
          summary: run.summary,
          decision: run.decision,
        },
        ...prev.filter((item) => item.id !== run.id),
      ].slice(0, 3))
    } catch (error) {
      setActivationFeedbackExportError((error as Error)?.message || 'Failed to create activation feedback export')
    } finally {
      setActivationFeedbackExportPending(false)
    }
  }
  const runActivationMeasurement = async (draft = measurementDraft) => {
    setMeasurementPending(true)
    setMeasurementError(null)
    try {
      const result = await apiGetJson<ActivationMeasurementSummary>(
        withQuery('/api/measurement/activation-summary', draft),
        { fallbackMessage: 'Failed to load activation measurement summary' },
      )
      const evidence = await apiGetJson<ActivationMeasurementEvidence>(
        withQuery('/api/measurement/activation-evidence', { ...draft, limit: 5 }),
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
  const measureActivationObject = (item: ActivationMeasurementObject) => {
    const nextDraft = measurementDraftForObject(item)
    setMeasurementDraft(nextDraft)
    void runActivationMeasurement(nextDraft)
  }
  const measureActivationFeedbackItem = (item: ActivationFeedbackItem) => {
    if (!item.object?.type || !item.object?.id) return
    measureActivationObject({
      object_type: item.object.type,
      object_id: item.object.id,
      label: item.object.label,
      aliases: item.object.aliases || [],
    })
  }
  const syncMeiroSegments = async () => {
    setMeiroSegmentImportPending(true)
    setMeiroSegmentImportError(null)
    try {
      const result = await apiSendJson<MeiroSegmentImportResponse>(
        '/api/segments/import/meiro',
        'POST',
        { source: meiroSegmentImportSource },
        { fallbackMessage: 'Failed to sync Meiro audiences into MMM segments' },
      )
      setMeiroSegmentImportResult(result)
    } catch (error) {
      setMeiroSegmentImportError((error as Error)?.message || 'Failed to sync Meiro audiences into MMM segments')
    } finally {
      setMeiroSegmentImportPending(false)
    }
  }

  useEffect(() => {
    setSelectedRecordIndices([])
    setShowResolvedRecords(false)
  }, [selectedQuarantineRun?.id])

  useEffect(() => {
    let cancelled = false
    apiGetJson<JourneyDefinitionsResponse>(
      withQuery('/api/journeys/definitions', { limit: 1, sort: 'desc' }),
      { fallbackMessage: 'Failed to load journey definitions' },
    )
      .then((result) => {
        if (cancelled) return
        const first = result.items?.[0] || null
        setJourneySeedDefinition(first ? { id: first.id, name: first.name } : null)
      })
      .catch(() => {
        if (!cancelled) setJourneySeedDefinition(null)
      })
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    void loadActivationObjects()
  }, [deciEngineImportResult?.count])

  useEffect(() => {
    if (autoMeasurementStarted || !measurementDraft.object_id.trim()) return
    setAutoMeasurementStarted(true)
    void runActivationMeasurement(measurementDraft)
  }, [autoMeasurementStarted, measurementDraft])

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

  const topActivationFeedback = activationFeedback?.items?.[0] || null
  const topActivationMeasured =
    !!topActivationFeedback?.object?.type &&
    !!topActivationFeedback.object.id &&
    measurementResult?.object?.type === topActivationFeedback.object.type &&
    measurementResult?.object?.id === topActivationFeedback.object.id
  const topActivationStatusColor =
    topActivationFeedback?.status === 'ready'
      ? t.color.success
      : topActivationFeedback?.status === 'warning'
        ? t.color.warning
        : t.color.textMuted
  const latestActivationFeedbackExportRun = activationFeedbackExportRuns[0] || null
  const measuredObjectLabel = measurementResult?.object?.id || measurementDraft.object_id
  const measuredObjectType = measurementResult?.object?.type || measurementDraft.object_type
  const activationDecisionDraftHref =
    journeySeedDefinition?.id && measuredObjectLabel && measurementResult?.summary
      ? buildJourneyHypothesisSeedHref({
          journeyDefinitionId: journeySeedDefinition.id,
          title: `Activation decision review: ${measuredObjectLabel}`,
          hypothesisText: `${measuredObjectType} ${measuredObjectLabel} has ${Number(measurementResult.summary.conversions || 0).toLocaleString()} measured conversions and ${Number(measurementResult.summary.matched_touchpoints || 0).toLocaleString()} matched activation touchpoints. Review whether deciEngine should change audience eligibility, decision priority, or asset treatment for this object.`,
          supportCount: Number(measurementResult.summary.matched_journeys || 0),
          baselineRate: measurementResult.summary.conversion_rate ?? null,
        })
      : null

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

      <div style={{ border: `1px solid ${topActivationFeedback ? t.color.accent : t.color.borderLight}`, borderRadius: t.radius.md, background: topActivationFeedback ? t.color.accentMuted : t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
          <div style={{ display: 'grid', gap: 4 }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Activation decision queue</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {topActivationFeedback
                ? topActivationFeedback.title || topActivationFeedback.object?.label || topActivationFeedback.object?.id
                : activationFeedback?.decision?.subtitle || 'Import activation events to surface the next campaign, asset, or decision to review.'}
            </div>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={() => void loadActivationObjects()}
              disabled={activationObjectsPending}
              style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: activationObjectsPending ? 'wait' : 'pointer', opacity: activationObjectsPending ? 0.7 : 1 }}
            >
              {activationObjectsPending ? 'Refreshing...' : 'Refresh queue'}
            </button>
            {topActivationFeedback ? (
              <>
                <button
                  type="button"
                  onClick={() => measureActivationFeedbackItem(topActivationFeedback)}
                  disabled={measurementPending}
                  style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: measurementPending ? 'wait' : 'pointer', opacity: measurementPending ? 0.75 : 1 }}
                >
                  {measurementPending ? 'Measuring...' : 'Measure top object'}
                </button>
                <button
                  type="button"
                  onClick={() => void createActivationFeedbackExport()}
                  disabled={activationFeedbackExportPending}
                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: activationFeedbackExportPending ? 'wait' : 'pointer', opacity: activationFeedbackExportPending ? 0.7 : 1 }}
                >
                  {activationFeedbackExportPending ? 'Creating...' : 'Create export run'}
                </button>
              </>
            ) : (
              <button
                type="button"
                onClick={onImportDeciEngineEvents}
                disabled={deciEngineImportPending || !canImportDeciEngineEvents}
                style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: deciEngineImportPending ? 'wait' : 'pointer', opacity: deciEngineImportPending || !canImportDeciEngineEvents ? 0.7 : 1 }}
              >
                {deciEngineImportPending ? 'Importing...' : canImportDeciEngineEvents ? 'Import activation events' : 'Enter user email'}
              </button>
            )}
          </div>
        </div>
        {topActivationFeedback ? (
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{topActivationFeedback.reason}</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Status</div>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: topActivationStatusColor }}>{topActivationFeedback.status || 'setup'} · {topActivationFeedback.recommendation || 'review'}</div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Evidence</div>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>{Number(topActivationFeedback.evidence?.matched_touchpoints || 0).toLocaleString()} touchpoints · {Number(topActivationFeedback.evidence?.conversions || 0).toLocaleString()} conversions</div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Object</div>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, overflowWrap: 'anywhere' }}>{topActivationFeedback.object?.type} · {topActivationFeedback.object?.id}</div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Last measurement</div>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                  {topActivationMeasured
                    ? `${Number(measurementResult?.summary?.matched_touchpoints || 0).toLocaleString()} matched · ${Number(measurementResult?.summary?.conversions || 0).toLocaleString()} conversions`
                    : 'Not measured in this session'}
                </div>
              </div>
            </div>
            {topActivationMeasured && measurementResult?.summary ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Current result: <strong style={{ color: t.color.text }}>{Number(measurementResult.summary.conversions || 0).toLocaleString()}</strong> conversions · revenue <strong style={{ color: t.color.text }}>{Number(measurementResult.summary.revenue || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong> · metadata coverage <strong style={{ color: t.color.text }}>{(Number(measurementResult.summary.activation_metadata_coverage || 0) * 100).toFixed(1)}%</strong>
              </div>
            ) : null}
            {latestActivationFeedbackExportRun ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface, display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <span>Latest export: <strong style={{ color: t.color.text }}>{latestActivationFeedbackExportRun.id}</strong> · {Number(latestActivationFeedbackExportRun.summary?.signals || 0).toLocaleString()} signals</span>
                <span>{latestActivationFeedbackExportRun.created_at ? relativeTime(latestActivationFeedbackExportRun.created_at) : 'recently'}</span>
              </div>
            ) : null}
            {activationFeedbackExportError ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{activationFeedbackExportError}</div>
            ) : null}
          </div>
        ) : null}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
          <div style={{ display: 'grid', gap: 4 }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Sync Meiro audiences into MMM segments</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Bring one audience source into the MMM segment registry so selectors can use it for experiment setup and operational audience alignment.
            </div>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
            <select
              value={meiroSegmentImportSource}
              onChange={(event) => setMeiroSegmentImportSource(event.target.value as 'pipes_webhook' | 'cdp')}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: t.color.surface }}
            >
              <option value="pipes_webhook">Pipes archive memberships</option>
              <option value="cdp">CDP connector segments</option>
            </select>
            <button
              type="button"
              onClick={() => void syncMeiroSegments()}
              disabled={meiroSegmentImportPending}
              style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: meiroSegmentImportPending ? 'wait' : 'pointer', opacity: meiroSegmentImportPending ? 0.75 : 1 }}
            >
              {meiroSegmentImportPending ? 'Syncing...' : 'Sync audiences'}
            </button>
          </div>
        </div>
        {meiroSegmentImportError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{meiroSegmentImportError}</div>
        ) : meiroSegmentImportResult ? (
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {meiroSegmentImportResult.message || 'Audience sync completed.'}
              {' '}Source <strong style={{ color: t.color.text }}>{meiroSegmentImportResult.summary?.source || meiroSegmentImportSource}</strong>
              {' '}· activation-ready <strong style={{ color: t.color.text }}>{Number(meiroSegmentImportResult.summary?.activation_ready || 0).toLocaleString()}</strong>
            </div>
            {meiroSegmentImportResult.items?.length ? (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
                {meiroSegmentImportResult.items.slice(0, 4).map((item) => (
                  <div key={item.id || item.external_segment_id || item.name} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface }}>
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>{item.name || item.external_segment_id || item.id}</div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      {item.source_label || 'Meiro'} · {item.size == null ? 'size unknown' : `${Number(item.size).toLocaleString()} profiles`}
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Use one source at a time: Pipes archive memberships for CDI-derived operational audiences, or CDP connector segments when Engage is the audience source of truth.
          </div>
        )}
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
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
            <div>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Measured objects</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Select a discovered campaign, asset, offer, or decision from the imported activation stream.</div>
            </div>
            <button
              type="button"
              onClick={() => void loadActivationObjects()}
              disabled={activationObjectsPending}
              style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 9px', cursor: activationObjectsPending ? 'wait' : 'pointer', opacity: activationObjectsPending ? 0.7 : 1 }}
            >
              {activationObjectsPending ? 'Refreshing...' : 'Refresh objects'}
            </button>
          </div>
          {activationObjectsError ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{activationObjectsError}</div>
          ) : activationObjectsPending && !activationObjects.length ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading measurable activation objects...</div>
          ) : activationObjects.length ? (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: t.space.sm }}>
              {activationObjects.map((item) => (
                <button
                  key={`${item.object_type}:${item.object_id}`}
                  type="button"
                  onClick={() => selectActivationObject(item)}
                  style={{
                    border: `1px solid ${measurementDraft.object_type === item.object_type && measurementDraft.object_id === item.object_id ? t.color.accent : t.color.borderLight}`,
                    background: measurementDraft.object_type === item.object_type && measurementDraft.object_id === item.object_id ? t.color.accentMuted : t.color.surface,
                    borderRadius: t.radius.sm,
                    padding: t.space.sm,
                    cursor: 'pointer',
                    textAlign: 'left',
                    display: 'grid',
                    gap: 4,
                  }}
                >
                  <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>{item.label || item.object_id}</span>
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    {item.object_type} · {Number(item.matched_touchpoints || 0).toLocaleString()} touchpoints · {Number(item.conversions || 0).toLocaleString()} conversions
                  </span>
                  {!!item.aliases?.length ? (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, overflowWrap: 'anywhere' }}>{item.aliases.slice(0, 2).join(' · ')}</span>
                  ) : null}
                </button>
              ))}
            </div>
          ) : (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No measurable activation objects found in the currently loaded journeys.</div>
          )}
          {activationFeedback?.items?.length ? (
            <div style={{ display: 'grid', gap: t.space.sm }}>
              <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Activation feedback</div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  Ready {Number(activationFeedback.summary?.ready || 0).toLocaleString()} · review {Number(activationFeedback.summary?.warning || 0).toLocaleString()} · setup {Number(activationFeedback.summary?.setup || 0).toLocaleString()}
                </div>
                <button
                  type="button"
                  onClick={() => void loadActivationFeedbackExport()}
                  disabled={activationFeedbackExportPending}
                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '5px 8px', cursor: activationFeedbackExportPending ? 'wait' : 'pointer', fontSize: t.font.sizeXs, opacity: activationFeedbackExportPending ? 0.7 : 1 }}
                >
                  {activationFeedbackExportPending ? 'Building...' : 'Preview export'}
                </button>
                <button
                  type="button"
                  onClick={() => void createActivationFeedbackExport()}
                  disabled={activationFeedbackExportPending}
                  style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '5px 8px', cursor: activationFeedbackExportPending ? 'wait' : 'pointer', fontSize: t.font.sizeXs, opacity: activationFeedbackExportPending ? 0.7 : 1 }}
                >
                  {activationFeedbackExportPending ? 'Creating...' : 'Create export run'}
                </button>
              </div>
              {activationFeedback.decision?.warnings?.length ? (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>{activationFeedback.decision.warnings.join(' · ')}</div>
              ) : null}
              {activationFeedbackExportError ? (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>{activationFeedbackExportError}</div>
              ) : activationFeedbackExport ? (
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    Export payload <strong style={{ color: t.color.text }}>{activationFeedbackExport.schema_version}</strong> · {Number(activationFeedbackExport.summary?.signals || 0).toLocaleString()} signals · generated {activationFeedbackExport.generated_at ? relativeTime(activationFeedbackExport.generated_at) : 'now'}
                  </div>
                  <pre style={{ margin: 0, maxHeight: 180, overflow: 'auto', fontSize: 11, background: t.color.bgSubtle, borderRadius: t.radius.sm, padding: t.space.sm }}>{JSON.stringify({ schema_version: activationFeedbackExport.schema_version, summary: activationFeedbackExport.summary, first_signal: activationFeedbackExport.signals?.[0] || null }, null, 2)}</pre>
                </div>
              ) : null}
              {activationFeedbackExportRuns.length ? (
                <div style={{ display: 'grid', gap: 5 }}>
                  <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.text }}>Recent export runs</div>
                  {activationFeedbackExportRuns.map((run) => (
                    <div key={run.id} style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap', fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      <span>{run.id} · {run.schema_version} · {Number(run.summary?.signals || 0).toLocaleString()} signals</span>
                      <span>{run.created_at ? relativeTime(run.created_at) : 'recently'}</span>
                    </div>
                  ))}
                </div>
              ) : null}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
                {activationFeedback.items.map((item, index) => {
                  const statusColor = item.status === 'ready' ? t.color.success : item.status === 'warning' ? t.color.warning : t.color.textMuted
                  return (
                    <button
                      key={`${item.object?.type || 'object'}:${item.object?.id || index}:feedback`}
                      type="button"
                      onClick={() => measureActivationFeedbackItem(item)}
                      disabled={measurementPending || !item.object?.id}
                      style={{ border: `1px solid ${t.color.borderLight}`, background: t.color.surface, borderRadius: t.radius.sm, padding: t.space.sm, cursor: measurementPending ? 'wait' : item.object?.id ? 'pointer' : 'default', display: 'grid', gap: 5, textAlign: 'left', opacity: measurementPending ? 0.82 : 1 }}
                    >
                      <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>{item.title || item.object?.label || item.object?.id || 'Activation feedback'}</span>
                      <span style={{ fontSize: t.font.sizeXs, color: statusColor, fontWeight: t.font.weightSemibold }}>{item.status || 'setup'} · {item.recommendation || 'review'}</span>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{item.reason}</span>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        {Number(item.evidence?.matched_touchpoints || 0).toLocaleString()} touchpoints · {Number(item.evidence?.conversions || 0).toLocaleString()} conversions · revenue {Number(item.evidence?.revenue || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </span>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.accent, fontWeight: t.font.weightSemibold }}>
                        Measure this object
                      </span>
                    </button>
                  )
                })}
              </div>
            </div>
          ) : activationFeedback?.decision?.status === 'blocked' ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{activationFeedback.decision.subtitle}</div>
          ) : null}
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
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Measurement result for <strong style={{ color: t.color.text }}>{measurementResult.object?.type || measurementDraft.object_type}</strong>{' '}
              <strong style={{ color: t.color.text }}>{measurementResult.object?.id || measurementDraft.object_id}</strong>
            </div>
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
            {activationDecisionDraftHref ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface, display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                <div style={{ display: 'grid', gap: 3 }}>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Decision draft ready</div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    Seed a journey hypothesis from this measured activation object{journeySeedDefinition?.name ? ` in ${journeySeedDefinition.name}` : ''}.
                  </div>
                </div>
                <a
                  href={activationDecisionDraftHref}
                  style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '7px 10px', textDecoration: 'none', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold }}
                >
                  Draft decision hypothesis
                </a>
              </div>
            ) : measurementResult?.summary ? (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Create a journey definition to turn this measurement into a decision hypothesis draft.</div>
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
