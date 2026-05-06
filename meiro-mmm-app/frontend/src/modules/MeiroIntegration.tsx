import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import DecisionStatusCard from '../components/DecisionStatusCard'
import type { RecommendedActionItem } from '../components/RecommendedActionsList'
import MeiroIntegrationPanel from '../features/meiro/MeiroIntegrationPanel'
import MeiroTargetInstanceBadge from '../features/meiro/MeiroTargetInstanceBadge'
import { tokens as t } from '../theme/tokens'
import {
  connectMeiroCDP,
  disconnectMeiroCDP,
  getMeiroEventArchive,
  getMeiroConfig,
  getMeiroMeasurementPipelineSummary,
  type MeiroQuarantineReprocessResult,
  getMeiroMapping,
  getMeiroPullConfig,
  getMeiroQuarantineRun,
  getMeiroQuarantineRuns,
  getMeiroWebhookEvents,
  getMeiroWebhookDiagnostics,
  getMeiroWebhookSuggestions,
  meiroDryRun,
  meiroPull,
  meiroRotateWebhookSecret,
  saveMeiroMapping,
  saveMeiroPullConfig,
  testMeiroConnection,
  type MeiroPullConfig,
  type MeiroQuarantineRun,
  type MeiroMeasurementPipelineSummary,
  type MeiroWebhookDiagnostics,
} from '../connectors/meiroConnector'
import { getDeciEngineEventsConfig, importDeciEngineActivationEvents, saveDeciEngineEventsConfig, type DeciEngineEventsImportPayload } from '../connectors/deciengineConnector'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import {
  DEFAULT_MEIRO_PULL_CONFIG,
  normalizeMeiroPullConfig,
  type MeiroTab,
  type MeiroWebhookArchiveStatus,
  type MeiroWebhookReprocessResult,
} from '../features/meiro/shared'
import { usePermissions } from '../hooks/usePermissions'

interface MeiroIntegrationPageProps {
  onJourneysImported: () => void
}

interface MeiroReadinessResponse {
  status: string
  confidence: { score: number; band: string }
  summary: {
    cdp_connected: boolean
    webhook_received_count: number
    event_webhook_received_count?: number
    webhook_has_secret: boolean
    mapping_status: string
    mapping_version: number
    archive_entries: number
    profile_archive_entries?: number
    event_archive_entries?: number
    last_test_at?: string | null
    last_webhook_received_at?: string | null
    last_event_webhook_received_at?: string | null
    conversion_selector?: string | null
    primary_ingest_source?: 'profiles' | 'events'
    replay_archive_source?: 'auto' | 'profiles' | 'events'
    dual_ingest_detected?: boolean
    raw_event_diagnostics?: {
      available: boolean
      batches_examined: number
      events_examined: number
      usable_event_name_share: number
      identity_share: number
      source_medium_share: number
      referrer_only_share: number
      touchpoint_like_events: number
      conversion_like_events: number
      conversion_linkage_share: number
      avg_reconstructed_profiles_per_event: number
      warnings?: string[]
    }
  }
  blockers: string[]
  warnings: string[]
  reasons: string[]
  recommended_actions: RecommendedActionItem[]
}

type MeiroSegmentImportSource = 'pipes_registry' | 'pipes_webhook' | 'cdp'

interface MeiroSegmentRegistryItem {
  id?: string
  external_segment_id?: string | null
  name?: string
  source?: string
  source_label?: string
  size?: number | null
}

interface SegmentRegistryResponse {
  items?: MeiroSegmentRegistryItem[]
  summary?: {
    local_analytical?: number
    meiro_pipes?: number
    analysis_ready?: number
    activation_ready?: number
  }
}

interface MeiroSegmentImportResponse {
  status?: string
  message?: string
  summary?: {
    source?: string
    meiro_pipes?: number
    cdp_segments?: number
    pipes_webhook_segments?: number
    pipes_registry_segments?: number
    activation_ready?: number
  }
  items?: MeiroSegmentRegistryItem[]
}

function relativeTime(iso?: string | null) {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return '—'
  const m = Math.floor((Date.now() - d.getTime()) / 60000)
  if (m < 1) return 'just now'
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

function pipelineTone(status: 'ready' | 'warning' | 'blocked' | 'idle') {
  if (status === 'ready') return { color: t.color.success, bg: t.color.successMuted, label: 'Ready' }
  if (status === 'warning') return { color: t.color.warning, bg: t.color.warningMuted, label: 'Review' }
  if (status === 'blocked') return { color: t.color.danger, bg: t.color.dangerMuted, label: 'Blocked' }
  return { color: t.color.textMuted, bg: t.color.bg, label: 'Waiting' }
}

function readInitialMeiroTab(): MeiroTab {
  if (typeof window === 'undefined') return 'overview'
  const value = new URLSearchParams(window.location.search).get('meiro_tab')
  return ['overview', 'cdp', 'pipes', 'normalization', 'import'].includes(value || '') ? (value as MeiroTab) : 'overview'
}

function keyForAudience(item: MeiroSegmentRegistryItem) {
  return String(item.id || item.external_segment_id || item.name || '').trim()
}

function audienceDisplayName(item: MeiroSegmentRegistryItem) {
  return String(item.name || item.external_segment_id || item.id || '').trim()
}

function dedupeAudienceOptions(items: MeiroSegmentRegistryItem[]) {
  const seen = new Set<string>()
  const deduped: MeiroSegmentRegistryItem[] = []
  for (const item of items) {
    const display = audienceDisplayName(item).toLowerCase()
    const source = String(item.source || '').trim().toLowerCase()
    const identity = display ? `${source}:${display}` : keyForAudience(item).toLowerCase()
    if (!identity || seen.has(identity)) continue
    seen.add(identity)
    deduped.push(item)
  }
  return deduped
}

export default function MeiroIntegrationPage({ onJourneysImported }: MeiroIntegrationPageProps) {
  const queryClient = useQueryClient()
  const permissions = usePermissions()
  const [meiroTab, setMeiroTabState] = useState<MeiroTab>(readInitialMeiroTab)
  const [detailsOpen, setDetailsOpen] = useState(() => readInitialMeiroTab() !== 'overview')
  const setMeiroTab = (tab: MeiroTab) => {
    setMeiroTabState(tab)
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    if (tab === 'overview') params.delete('meiro_tab')
    else params.set('meiro_tab', tab)
    const next = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ''}${window.location.hash}`
    window.history.replaceState(null, '', next)
  }
  const [meiroUrl, setMeiroUrl] = useState('')
  const [meiroKey, setMeiroKey] = useState('')
  const [webhookSecretValue, setWebhookSecretValue] = useState<string | null>(null)
  const [meiroPullDraft, setMeiroPullDraft] = useState<MeiroPullConfig>(DEFAULT_MEIRO_PULL_CONFIG)
  const [deciEngineImportDraft, setDeciEngineImportDraft] = useState<DeciEngineEventsImportPayload>({
    source_url: 'http://host.docker.internal:3001/v1/inapp/events',
    limit: 500,
  })
  const [oauthToast, setOauthToast] = useState<string | null>(null)
  const [selectedQuarantineRunId, setSelectedQuarantineRunId] = useState<string | null>(null)
  const [pipelineAudienceSource, setPipelineAudienceSource] = useState<MeiroSegmentImportSource>('pipes_registry')
  const [pipelineAudienceKey, setPipelineAudienceKey] = useState('')

  const meiroConfigQuery = useQuery({ queryKey: ['meiro-config'], queryFn: getMeiroConfig })
  const measurementPipelineQuery = useQuery<MeiroMeasurementPipelineSummary>({
    queryKey: ['meiro-measurement-pipeline-summary'],
    queryFn: getMeiroMeasurementPipelineSummary,
    staleTime: 30_000,
  })
  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['segment-registry', 'meiro-pipeline'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load MMM segment registry',
      }),
    staleTime: 30_000,
  })
  const meiroReadinessQuery = useQuery<MeiroReadinessResponse>({
    queryKey: ['meiro-readiness'],
    queryFn: async () =>
      apiGetJson<MeiroReadinessResponse>('/api/connectors/meiro/readiness', {
        fallbackMessage: 'Failed to load Meiro readiness',
      }),
    enabled: detailsOpen,
  })
  const meiroMappingQuery = useQuery({ queryKey: ['meiro-mapping'], queryFn: getMeiroMapping })
  const meiroPullConfigQuery = useQuery({ queryKey: ['meiro-pull-config'], queryFn: getMeiroPullConfig })
  const meiroWebhookSuggestionsQuery = useQuery({
    queryKey: ['meiro-webhook-suggestions-page'],
    queryFn: () => getMeiroWebhookSuggestions(100),
    enabled: detailsOpen,
  })
  const meiroWebhookEventsQuery = useQuery({
    queryKey: ['meiro-webhook-events-page'],
    queryFn: () => getMeiroWebhookEvents(100),
    enabled: detailsOpen,
  })
  const meiroEventArchiveQuery = useQuery({
    queryKey: ['meiro-event-archive-page'],
    queryFn: () => getMeiroEventArchive(25),
    enabled: detailsOpen,
  })
  const meiroWebhookDiagnosticsQuery = useQuery<MeiroWebhookDiagnostics>({
    queryKey: ['meiro-webhook-diagnostics-page'],
    queryFn: () => getMeiroWebhookDiagnostics(100),
    enabled: detailsOpen,
  })
  const meiroWebhookArchiveStatusQuery = useQuery<MeiroWebhookArchiveStatus>({
    queryKey: ['meiro-webhook-archive-status-page'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookArchiveStatus>('/api/connectors/meiro/webhook/archive-status', {
        fallbackMessage: 'Failed to load webhook archive status',
      }),
    enabled: detailsOpen,
  })
  const meiroEventArchiveStatusQuery = useQuery<MeiroWebhookArchiveStatus>({
    queryKey: ['meiro-event-archive-status-page'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookArchiveStatus>('/api/connectors/meiro/events/archive-status', {
        fallbackMessage: 'Failed to load event archive status',
      }),
    enabled: detailsOpen,
  })
  const meiroQuarantineRunsQuery = useQuery({
    queryKey: ['meiro-quarantine-runs'],
    queryFn: () => getMeiroQuarantineRuns(10),
    enabled: detailsOpen,
  })
  const meiroQuarantineRunQuery = useQuery<MeiroQuarantineRun>({
    queryKey: ['meiro-quarantine-run', selectedQuarantineRunId],
    queryFn: () => getMeiroQuarantineRun(String(selectedQuarantineRunId)),
    enabled: detailsOpen && Boolean(selectedQuarantineRunId),
  })
  const deciEngineEventsConfigQuery = useQuery({
    queryKey: ['deciengine-events-config-page'],
    queryFn: getDeciEngineEventsConfig,
  })

  useEffect(() => {
    const email = (permissions.auth?.user?.email || '').trim()
    if (!email || deciEngineImportDraft.user_email) return
    setDeciEngineImportDraft((prev) => ({ ...prev, user_email: email }))
  }, [permissions.auth?.user?.email, deciEngineImportDraft.user_email])

  useEffect(() => {
    if (!deciEngineEventsConfigQuery.data) return
    const email = deciEngineEventsConfigQuery.data.user_email || permissions.auth?.user?.email || ''
    setDeciEngineImportDraft({ ...deciEngineEventsConfigQuery.data, user_email: email, limit: deciEngineEventsConfigQuery.data.limit || 500 })
  }, [deciEngineEventsConfigQuery.data, permissions.auth?.user?.email])

  const invalidateJourneyState = async () => {
    onJourneysImported()
    await queryClient.invalidateQueries({ queryKey: ['journeys-summary'] })
    await queryClient.invalidateQueries({ queryKey: ['journeys-validation-summary'] })
    await queryClient.invalidateQueries({ queryKey: ['journeys-preview-20'] })
    await queryClient.invalidateQueries({ queryKey: ['import-log-recent'] })
  }

  const connectMeiroMutation = useMutation({
    mutationFn: async () => connectMeiroCDP({ api_base_url: meiroUrl, api_key: meiroKey }),
    onSuccess: async () => {
      setMeiroKey('')
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
    },
  })
  const testMeiroMutation = useMutation({
    mutationFn: async () =>
      testMeiroConnection({ api_base_url: meiroUrl || undefined, api_key: meiroKey || undefined }),
  })
  const disconnectMeiroMutation = useMutation({
    mutationFn: disconnectMeiroCDP,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
    },
  })
  const saveMeiroPullMutation = useMutation({
    mutationFn: saveMeiroPullConfig,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['meiro-pull-config'] }),
  })
  const pinRawEventsPrimaryMutation = useMutation({
    mutationFn: async () => {
      const nextConfig = normalizeMeiroPullConfig({
        ...meiroPullDraft,
        primary_ingest_source: 'events',
        replay_archive_source: 'events',
      })
      return saveMeiroPullConfig(nextConfig)
    },
    onSuccess: async (saved) => {
      const nextConfig = normalizeMeiroPullConfig(saved || { ...meiroPullDraft, primary_ingest_source: 'events', replay_archive_source: 'events' })
      setMeiroPullDraft(nextConfig)
      setOauthToast('Raw Pipes events are pinned as the attribution source. Profile/CDP streams should be treated as enrichment.')
      await queryClient.invalidateQueries({ queryKey: ['meiro-pull-config'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-readiness'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-measurement-pipeline-summary'] })
    },
    onError: (error) => setOauthToast((error as Error).message || 'Failed to pin raw events as primary source'),
  })
  const runMeiroPullMutation = useMutation({
    mutationFn: () => meiroPull(),
    onSuccess: async () => {
      await invalidateJourneyState()
    },
  })
  const saveMeiroMappingMutation = useMutation({
    mutationFn: saveMeiroMapping,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['meiro-mapping'] }),
  })
  const applyMeiroMappingSuggestionMutation = useMutation({
    mutationFn: async (payload: Record<string, unknown>) =>
      apiSendJson('/api/connectors/meiro/mapping', 'POST', payload, {
        fallbackMessage: 'Failed to apply Meiro mapping suggestions',
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['meiro-mapping'] })
    },
  })
  const updateMeiroMappingApprovalMutation = useMutation({
    mutationFn: async (payload: { status: string; note?: string }) =>
      apiSendJson('/api/connectors/meiro/mapping/approval', 'POST', payload, {
        fallbackMessage: 'Failed to update Meiro mapping approval',
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['meiro-mapping'] })
    },
  })
  const meiroDryRunMutation = useMutation({
    mutationFn: async () => meiroDryRun(100),
  })
  const importFromMeiroMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<any>('/api/attribution/journeys/from-cdp', 'POST', {}, {
        fallbackMessage: 'Import from Meiro failed',
      }),
    onSuccess: async () => {
      await invalidateJourneyState()
    },
  })
  const importDeciEngineEventsMutation = useMutation({
    mutationFn: async () => importDeciEngineActivationEvents(deciEngineImportDraft),
    onSuccess: async () => {
      await invalidateJourneyState()
    },
  })
  const saveDeciEngineEventsConfigMutation = useMutation({
    mutationFn: async () => saveDeciEngineEventsConfig(deciEngineImportDraft),
    onSuccess: async (config) => {
      setDeciEngineImportDraft(config)
      await queryClient.invalidateQueries({ queryKey: ['deciengine-events-config-page'] })
    },
  })
  const importMeiroSegmentsMutation = useMutation({
    mutationFn: async (source: MeiroSegmentImportSource) =>
      apiSendJson<MeiroSegmentImportResponse>(
        '/api/segments/import/meiro',
        'POST',
        { source },
        { fallbackMessage: 'Failed to sync Meiro audiences into MMM segments' },
      ),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['segment-registry'] })
    },
  })
  const buildReplayPayload = () => {
    const replayMode = meiroPullDraft.replay_mode || 'last_n'
    const toIso = (value?: string | null) => {
      if (!value) return undefined
      const parsed = new Date(value)
      return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString()
    }
    return {
      replay_mode: replayMode,
      archive_source: meiroPullDraft.replay_archive_source || 'auto',
      archive_limit: replayMode === 'last_n' ? (meiroPullDraft.replay_archive_limit || 5000) : undefined,
      date_from: replayMode === 'date_range' ? toIso(meiroPullDraft.replay_date_from) : undefined,
      date_to: replayMode === 'date_range' ? toIso(meiroPullDraft.replay_date_to) : undefined,
      persist_to_attribution: true,
      import_note: 'Reprocessed from webhook archive using current approved mapping',
    }
  }
  const reprocessWebhookArchiveMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<MeiroWebhookReprocessResult>('/api/connectors/meiro/webhook/reprocess', 'POST', buildReplayPayload(), {
        fallbackMessage: 'Failed to reprocess Meiro webhook archive',
      }),
    onSuccess: async () => {
      await invalidateJourneyState()
      await queryClient.invalidateQueries({ queryKey: ['meiro-webhook-events-page'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-webhook-archive-status-page'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-event-archive-status-page'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-measurement-pipeline-summary'] })
    },
  })
  const reprocessSelectedQuarantineMutation = useMutation({
    mutationFn: async (recordIndices?: number[]) => {
      if (!selectedQuarantineRunId) {
        throw new Error('No quarantine run selected')
      }
      return apiSendJson<MeiroQuarantineReprocessResult>(
        `/api/attribution/meiro/quarantine/${selectedQuarantineRunId}/reprocess`,
        'POST',
        {
          record_indices: recordIndices && recordIndices.length ? recordIndices : undefined,
          persist_to_attribution: true,
          replace_existing: false,
          import_note: `Reprocessed from quarantine run ${selectedQuarantineRunId}`,
        },
        { fallbackMessage: 'Failed to reprocess Meiro quarantine run' },
      )
    },
    onSuccess: async (data) => {
      await invalidateJourneyState()
      await queryClient.invalidateQueries({ queryKey: ['meiro-quarantine-runs'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-quarantine-run', data.source_quarantine_run_id] })
      if (data.quarantine_run_id) {
        await queryClient.invalidateQueries({ queryKey: ['meiro-quarantine-run', data.quarantine_run_id] })
      }
    },
  })
  const rotateWebhookSecretMutation = useMutation({
    mutationFn: meiroRotateWebhookSecret,
    onSuccess: async (res) => {
      setWebhookSecretValue(res.secret || null)
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
    },
    onError: (e) => setOauthToast((e as Error).message || 'Failed to rotate webhook secret'),
  })

  useEffect(() => {
    if (meiroPullConfigQuery.data) {
      setMeiroPullDraft(normalizeMeiroPullConfig(meiroPullConfigQuery.data))
    }
  }, [meiroPullConfigQuery.data])

  useEffect(() => {
    const firstId = meiroQuarantineRunsQuery.data?.items?.[0]?.id
    if (!selectedQuarantineRunId && firstId) {
      setSelectedQuarantineRunId(firstId)
    }
  }, [meiroQuarantineRunsQuery.data, selectedQuarantineRunId])

  const measurementSummary = measurementPipelineQuery.data
  const readiness = measurementSummary?.readiness || meiroReadinessQuery.data
  const readinessSummary = (readiness?.summary || {}) as MeiroReadinessResponse['summary'] & Record<string, unknown>
  const summarySource = measurementSummary?.source
  const summaryMapping = measurementSummary?.mapping
  const mappingStatus = (summaryMapping?.status || readinessSummary.mapping_status || meiroMappingQuery.data?.approval?.status || '').toLowerCase()
  const mappingNeedsReview = mappingStatus !== 'approved'

  const handleReadinessAction = (action: RecommendedActionItem) => {
    if (action.target_tab) {
      setDetailsOpen(true)
      setMeiroTab(action.target_tab as MeiroTab)
    }
  }

  const sourceMode = (summarySource?.primary_ingest_source || readinessSummary.primary_ingest_source || meiroConfigQuery.data?.primary_ingest_source || meiroPullDraft.primary_ingest_source || 'profiles') === 'events' ? 'events' : 'profiles'
  const profilePayloads = Math.max(
    Number(summarySource?.profile_payloads || 0),
    Number(readinessSummary.webhook_received_count || 0),
    Number(meiroConfigQuery.data?.webhook_received_count || 0),
    Number(meiroWebhookArchiveStatusQuery.data?.profiles_received || 0),
  )
  const rawEvents = Math.max(
    Number(summarySource?.raw_events || 0),
    Number(readinessSummary.event_webhook_received_count || 0),
    Number(meiroConfigQuery.data?.event_webhook_received_count || 0),
    Number(meiroEventArchiveStatusQuery.data?.events_received || 0),
  )
  const rawDiagnostics = measurementSummary?.quality.raw_event_diagnostics || (readinessSummary.raw_event_diagnostics as MeiroMeasurementPipelineSummary['quality']['raw_event_diagnostics'] | undefined)
  const cdpScopeStatus = summarySource?.cdp_instance_scope?.status || meiroConfigQuery.data?.cdp_instance_scope?.status
  const cdpOutOfScope = cdpScopeStatus === 'out_of_scope'
  const sourceMediumShare = Number(rawDiagnostics?.source_medium_share || 0)
  const conversionLinkageShare = Number(rawDiagnostics?.conversion_linkage_share || 0)
  const hasLivePipes = profilePayloads > 0 || rawEvents > 0
  const mappingApproved = mappingStatus === 'approved'
  const dualIngestDetected = Boolean(summarySource?.dual_ingest_detected ?? readinessSummary.dual_ingest_detected ?? (profilePayloads > 0 && rawEvents > 0))
  const replayBacklogCount = Math.max(
    Number(measurementSummary?.replay.backlog_entries || 0),
    Number(readinessSummary.archive_entries || 0),
    Number(sourceMode === 'events' ? meiroEventArchiveStatusQuery.data?.entries || 0 : meiroWebhookArchiveStatusQuery.data?.entries || 0),
  )
  const sourceScopeStatus = summarySource?.source_scope?.status || meiroEventArchiveStatusQuery.data?.source_scope?.status || 'unknown'
  const outOfScopeSiteEvents = Number(summarySource?.site_scope?.out_of_scope_site_events || 0)
  const targetSites = measurementSummary?.target.site_domains || summarySource?.site_scope?.target_sites || ['meiro.io', 'meir.store']
  const topTargetCampaigns = measurementSummary?.quality.top_target_campaigns || []
  const readinessActions = ((readiness?.recommended_actions || []) as Array<Partial<RecommendedActionItem>>)
    .filter((action): action is RecommendedActionItem => Boolean(action?.id && action?.label))
  const firstReadinessAction = readinessActions[0]
  const pipelineWarningCount = [
    dualIngestDetected,
    cdpOutOfScope,
    mappingNeedsReview,
    sourceMode === 'events' && sourceScopeStatus === 'out_of_scope',
    sourceMode === 'events' && outOfScopeSiteEvents > 0,
    sourceMode === 'events' && rawDiagnostics?.available && sourceMediumShare < 0.6,
    sourceMode === 'events' && rawDiagnostics?.available && conversionLinkageShare < 0.2,
  ].filter(Boolean).length
  const pipelineStatus = measurementSummary?.status === 'blocked'
    ? 'blocked'
    : measurementSummary?.status === 'ready' && hasLivePipes && pipelineWarningCount === 0
      ? 'ready'
      : !hasLivePipes
    ? 'blocked'
    : pipelineWarningCount > 0 || readiness?.status === 'warning' || readiness?.status === 'blocked'
      ? 'warning'
      : 'ready'
  const pipelineStatusTone = pipelineTone(pipelineStatus)
  const nextAction = firstReadinessAction
    ? { label: String(firstReadinessAction.label), tab: firstReadinessAction.target_tab as MeiroTab | undefined }
    : mappingNeedsReview
      ? { label: 'Review mapping', tab: 'normalization' as MeiroTab }
      : !hasLivePipes
        ? { label: 'Configure Pipes webhook', tab: 'pipes' as MeiroTab }
        : replayBacklogCount > 0
          ? { label: 'Review replay', tab: 'import' as MeiroTab }
          : { label: 'Measure activation evidence', tab: 'import' as MeiroTab }
  const pipelineStages = [
    {
      label: 'Connect',
      status: hasLivePipes ? 'ready' : 'blocked',
      value: hasLivePipes ? `${sourceMode === 'events' ? rawEvents.toLocaleString() : profilePayloads.toLocaleString()} ${sourceMode === 'events' ? 'raw events' : 'profile payloads'}` : 'No live payloads',
      detail: hasLivePipes ? `Last event ${relativeTime(readinessSummary.last_event_webhook_received_at || readinessSummary.last_webhook_received_at || meiroConfigQuery.data?.event_webhook_last_received_at || meiroConfigQuery.data?.webhook_last_received_at)}` : 'Configure the Pipes webhook endpoint',
      tab: 'pipes' as MeiroTab,
    },
    {
      label: 'Map',
      status: mappingApproved ? (sourceMode === 'events' && rawDiagnostics?.available && sourceMediumShare < 0.6 ? 'warning' : 'ready') : 'warning',
      value: mappingApproved ? 'Approved' : 'Needs approval',
      detail: sourceMode === 'events' && rawDiagnostics?.available
        ? `${(sourceMediumShare * 100).toFixed(0)}% source/medium coverage`
        : `Version ${readinessSummary.mapping_version || meiroMappingQuery.data?.version || 0}`,
      tab: 'normalization' as MeiroTab,
    },
    {
      label: 'Replay',
      status: replayBacklogCount > 0 ? 'warning' : 'ready',
      value: replayBacklogCount > 0 ? `${replayBacklogCount.toLocaleString()} archived batches` : 'No backlog',
      detail: `Source ${measurementSummary?.source.replay_archive_source || readinessSummary.replay_archive_source || meiroPullDraft.replay_archive_source || sourceMode}`,
      tab: 'import' as MeiroTab,
    },
    {
      label: 'Measure',
      status: mappingApproved && hasLivePipes ? 'ready' : 'idle',
      value: mappingApproved && hasLivePipes ? 'Evidence available' : 'Waiting for mapped journeys',
      detail: sourceMode === 'events' && rawDiagnostics?.available
        ? `${(conversionLinkageShare * 100).toFixed(0)}% conversion linkage`
        : 'Campaign, asset, offer, and decision lookup',
      tab: 'import' as MeiroTab,
    },
    {
      label: 'Handoff',
      status: mappingApproved && hasLivePipes ? 'ready' : 'idle',
      value: mappingApproved && hasLivePipes ? 'deciEngine ready' : 'Pending evidence',
      detail: 'Feedback exports and decision handoff',
      tab: 'import' as MeiroTab,
    },
  ] as const
  const pipesRegistrySegments = (segmentRegistryQuery.data?.items || []).filter((item) => item.source === 'meiro_pipes_registry')
  const pipesRegistryAudienceOptions = useMemo(() => dedupeAudienceOptions(pipesRegistrySegments), [pipesRegistrySegments])
  const meiroOperationalSegments = (segmentRegistryQuery.data?.items || []).filter((item) => String(item.source || '').startsWith('meiro'))
  const latestSegmentSync = importMeiroSegmentsMutation.data
  const pipelineAudienceOptions = useMemo(() => {
    let items: MeiroSegmentRegistryItem[]
    if (latestSegmentSync?.summary?.source === pipelineAudienceSource && latestSegmentSync.items?.length) {
      items = latestSegmentSync.items
    } else if (pipelineAudienceSource === 'pipes_registry') {
      items = pipesRegistryAudienceOptions
    } else if (pipelineAudienceSource === 'pipes_webhook') {
      items = meiroOperationalSegments.filter((item) => item.source !== 'meiro_pipes_registry')
    } else {
      items = []
    }
    return dedupeAudienceOptions(items)
  }, [latestSegmentSync, meiroOperationalSegments, pipelineAudienceSource, pipesRegistryAudienceOptions])
  const selectedPipelineAudience = pipelineAudienceOptions.find((item) => keyForAudience(item) === pipelineAudienceKey) || pipelineAudienceOptions[0] || null
  const audienceSourceLabel =
    pipelineAudienceSource === 'pipes_registry'
      ? 'Pipes audience registry'
      : pipelineAudienceSource === 'pipes_webhook'
        ? 'Pipes archive memberships'
        : 'CDP connector segments'
  const pipelineAudienceCount = pipelineAudienceOptions.length

  useEffect(() => {
    if (cdpOutOfScope && pipelineAudienceSource === 'cdp') {
      setPipelineAudienceSource('pipes_registry')
    }
  }, [cdpOutOfScope, pipelineAudienceSource])

  useEffect(() => {
    const firstKey = pipelineAudienceOptions[0] ? keyForAudience(pipelineAudienceOptions[0]) : ''
    if (!pipelineAudienceOptions.length) {
      if (pipelineAudienceKey) setPipelineAudienceKey('')
      return
    }
    if (!pipelineAudienceOptions.some((item) => keyForAudience(item) === pipelineAudienceKey)) {
      setPipelineAudienceKey(firstKey)
    }
  }, [pipelineAudienceKey, pipelineAudienceOptions])

  const openPipelineTab = (tab?: MeiroTab) => {
    if (!tab) return
    setDetailsOpen(true)
    setMeiroTab(tab)
  }

  return (
    <DashboardPage
      title="Meiro"
      description="Primary integration workspace for Meiro CDP pull, Meiro Pipes webhook ingestion, normalization, and replay."
    >
      <div style={{ display: 'grid', gap: t.space.xl }}>
        {oauthToast ? (
          <SectionCard title="Integration update">
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center' }}>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{oauthToast}</div>
              <button
                type="button"
                onClick={() => setOauthToast(null)}
                style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
              >
                Dismiss
              </button>
            </div>
          </SectionCard>
        ) : null}

        <SectionCard title="Meiro Measurement Pipeline">
          <div style={{ display: 'grid', gap: t.space.md }}>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(0, 1fr) auto',
                gap: t.space.md,
                alignItems: 'start',
              }}
            >
              <div style={{ display: 'grid', gap: 6 }}>
                <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                  <span
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      padding: '5px 9px',
                      borderRadius: t.radius.full,
                      background: pipelineStatusTone.bg,
                      color: pipelineStatusTone.color,
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                    }}
                  >
                    {pipelineStatusTone.label}
                  </span>
                  {readiness ? (
                    <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      Confidence {readiness.confidence.band} ({readiness.confidence.score}/100)
                    </span>
                  ) : null}
                </div>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {sourceMode === 'events' ? 'Raw events are the primary attribution source' : 'Profile journeys are the primary attribution source'}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, maxWidth: 820 }}>
                  {dualIngestDetected
                    ? 'Both profile and raw-event webhooks are receiving traffic. Keep one stream primary for attribution and use the other only for enrichment while validating.'
                    : sourceMode === 'events'
                      ? 'Pipes raw events are archived, mapped, replayed into journeys, measured, and handed back to deciEngine.'
                      : 'Pipes profile payloads are imported as assembled journeys, then measured and handed back to deciEngine.'}
                </div>
                <MeiroTargetInstanceBadge config={meiroConfigQuery.data} />
                {dualIngestDetected ? (
                  <div
                    style={{
                      border: `1px solid ${t.color.warning}`,
                      background: t.color.warningMuted,
                      color: t.color.text,
                      borderRadius: t.radius.md,
                      padding: t.space.md,
                      display: 'flex',
                      justifyContent: 'space-between',
                      gap: t.space.md,
                      alignItems: 'center',
                      flexWrap: 'wrap',
                      maxWidth: 920,
                    }}
                  >
                    <div style={{ display: 'grid', gap: 4, minWidth: 0 }}>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Dual ingest resolution</div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        Pin raw events for attribution and keep profile/CDP streams as enrichment. This also pins replay source to raw events.
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={() => pinRawEventsPrimaryMutation.mutate()}
                      disabled={pinRawEventsPrimaryMutation.isPending}
                      style={{
                        border: `1px solid ${t.color.warning}`,
                        background: t.color.warning,
                        color: '#fff',
                        borderRadius: t.radius.sm,
                        padding: '8px 10px',
                        cursor: pinRawEventsPrimaryMutation.isPending ? 'wait' : 'pointer',
                        fontWeight: t.font.weightSemibold,
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {pinRawEventsPrimaryMutation.isPending ? 'Pinning...' : 'Pin raw events'}
                    </button>
                  </div>
                ) : null}
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                <button
                  type="button"
                  onClick={() => openPipelineTab(nextAction.tab)}
                  style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '9px 12px', cursor: 'pointer', fontWeight: t.font.weightSemibold }}
                >
                  {nextAction.label}
                </button>
                <button
                  type="button"
                  onClick={() => setDetailsOpen((open) => !open)}
                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '9px 12px', cursor: 'pointer', fontWeight: t.font.weightMedium }}
                >
                  {detailsOpen ? 'Hide details' : 'Open details'}
                </button>
              </div>
            </div>

            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))',
                gap: t.space.sm,
              }}
            >
              {pipelineStages.map((item) => {
                const itemTone = pipelineTone(item.status)
                return (
                  <button
                  key={item.label}
                  type="button"
                  onClick={() => openPipelineTab(item.tab)}
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    background: t.color.surface,
                    padding: t.space.md,
                    display: 'grid',
                    gap: 6,
                    textAlign: 'left',
                    cursor: 'pointer',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center' }}>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>
                      {item.label}
                    </div>
                    <span style={{ borderRadius: t.radius.full, background: itemTone.bg, color: itemTone.color, padding: '2px 7px', fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold }}>
                      {itemTone.label}
                    </span>
                  </div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {item.value}
                  </div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    {item.detail}
                  </div>
                </button>
                )
              })}
            </div>

            <div
              style={{
                border: `1px solid ${sourceScopeStatus === 'out_of_scope' ? t.color.danger : t.color.borderLight}`,
                borderRadius: t.radius.md,
                background: sourceScopeStatus === 'out_of_scope' ? t.color.dangerMuted : t.color.bg,
                padding: t.space.md,
                display: 'grid',
                gap: t.space.sm,
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'start', flexWrap: 'wrap' }}>
                <div style={{ display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Production data guardrails</div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Target instance <strong style={{ color: t.color.text }}>{measurementSummary?.target.instance_host || meiroConfigQuery.data?.target_instance_host || 'meiro-internal.eu.pipes.meiro.io'}</strong>
                    {' '}and target sites <strong style={{ color: t.color.text }}>{targetSites.join(', ')}</strong>.
                  </div>
                </div>
                <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                  {[
                    { label: 'Instance scope', value: sourceScopeStatus.replace(/_/g, ' ') },
                    { label: 'Target-site events', value: Number(summarySource?.site_scope?.target_site_events || 0).toLocaleString() },
                    { label: 'Out-of-scope events', value: outOfScopeSiteEvents.toLocaleString() },
                  ].map((item) => (
                    <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, background: t.color.surface, padding: '7px 9px', minWidth: 130 }}>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.value}</div>
                    </div>
                  ))}
                </div>
              </div>
              <div style={{ display: 'grid', gap: 6 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Top target campaigns in raw events</div>
                {topTargetCampaigns.length ? (
                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                    {topTargetCampaigns.slice(0, 8).map((item) => (
                      <span
                        key={item.campaign}
                        title={`${item.campaign}: ${item.events.toLocaleString()} events`}
                        style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.full, background: t.color.surface, color: t.color.textSecondary, padding: '4px 8px', fontSize: t.font.sizeXs, maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                      >
                        {item.campaign} ({item.events.toLocaleString()})
                      </span>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    No target-site campaign labels are available in recent raw events yet.
                  </div>
                )}
              </div>
            </div>

            <div
              style={{
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                background: t.color.bg,
                padding: t.space.md,
                display: 'grid',
                gap: t.space.md,
              }}
            >
              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: t.space.md, alignItems: 'start' }}>
                <div style={{ display: 'grid', gap: 5 }}>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Audience scope</div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Sync a single Meiro audience source into MMM before measuring or preparing deciEngine feedback.
                  </div>
                </div>
                <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', justifyContent: 'flex-end', flexWrap: 'wrap' }}>
                  <select
                    value={pipelineAudienceSource}
                    onChange={(event) => setPipelineAudienceSource(event.target.value as MeiroSegmentImportSource)}
                    style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: t.color.surface }}
                  >
                    <option value="pipes_registry">Pipes audience registry</option>
                    <option value="pipes_webhook">Pipes archive memberships</option>
                    <option value="cdp" disabled={cdpOutOfScope}>
                      {cdpOutOfScope ? 'CDP connector segments (other instance)' : 'CDP connector segments'}
                    </option>
                  </select>
                  <select
                    value={selectedPipelineAudience ? keyForAudience(selectedPipelineAudience) : pipelineAudienceKey}
                    onChange={(event) => setPipelineAudienceKey(event.target.value)}
                    disabled={!pipelineAudienceOptions.length}
                    style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: t.color.surface, minWidth: 210 }}
                  >
                    {pipelineAudienceOptions.length ? (
                      pipelineAudienceOptions.slice(0, 200).map((item) => {
                        const key = keyForAudience(item)
                        return (
                          <option key={key} value={key}>
                            {audienceDisplayName(item)}
                          </option>
                        )
                      })
                    ) : (
                      <option value="">Sync source first</option>
                    )}
                  </select>
                  <button
                    type="button"
                    onClick={() => importMeiroSegmentsMutation.mutate(pipelineAudienceSource)}
                    disabled={importMeiroSegmentsMutation.isPending || (pipelineAudienceSource === 'cdp' && cdpOutOfScope)}
                    style={{
                      border: `1px solid ${t.color.accent}`,
                      background: t.color.accent,
                      color: '#fff',
                      borderRadius: t.radius.sm,
                      padding: '8px 10px',
                      cursor: importMeiroSegmentsMutation.isPending ? 'wait' : pipelineAudienceSource === 'cdp' && cdpOutOfScope ? 'not-allowed' : 'pointer',
                      opacity: importMeiroSegmentsMutation.isPending || (pipelineAudienceSource === 'cdp' && cdpOutOfScope) ? 0.75 : 1,
                      fontWeight: t.font.weightSemibold,
                    }}
                  >
                    {importMeiroSegmentsMutation.isPending ? 'Syncing...' : 'Sync audience source'}
                  </button>
                  <button
                    type="button"
                    onClick={() => openPipelineTab('import')}
                    style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', fontWeight: t.font.weightMedium }}
                  >
                    Measurement setup
                  </button>
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: t.space.sm }}>
                {[
                  { label: 'Selected source', value: audienceSourceLabel },
                  { label: 'Available now', value: segmentRegistryQuery.isLoading ? 'Loading...' : `${pipelineAudienceCount.toLocaleString()} audiences` },
                  { label: 'Selected audience', value: selectedPipelineAudience ? audienceDisplayName(selectedPipelineAudience) : 'Not selected' },
                  { label: 'Pipes registry cache', value: `${pipesRegistryAudienceOptions.length.toLocaleString()} audiences` },
                  {
                    label: 'Activation-ready records',
                    value: `${Number(latestSegmentSync?.summary?.activation_ready ?? segmentRegistryQuery.data?.summary?.activation_ready ?? 0).toLocaleString()} records`,
                  },
                ].map((item) => (
                  <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, background: t.color.surface, padding: t.space.sm, display: 'grid', gap: 4 }}>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>{item.value}</div>
                  </div>
                ))}
              </div>

              {importMeiroSegmentsMutation.error ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                  {(importMeiroSegmentsMutation.error as Error).message || 'Audience sync failed'}
                </div>
              ) : latestSegmentSync ? (
                <div style={{ display: 'grid', gap: t.space.sm }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {latestSegmentSync.message || 'Audience sync completed.'}
                    {' '}Source <strong style={{ color: t.color.text }}>{latestSegmentSync.summary?.source || pipelineAudienceSource}</strong>.
                  </div>
                  {latestSegmentSync.items?.length ? (
                    <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                      {latestSegmentSync.items.slice(0, 6).map((item) => (
                        <span
                          key={item.id || item.external_segment_id || item.name}
                          style={{
                            display: 'inline-flex',
                            border: `1px solid ${t.color.borderLight}`,
                            borderRadius: t.radius.full,
                            background: t.color.surface,
                            color: t.color.textSecondary,
                            padding: '4px 8px',
                            fontSize: t.font.sizeXs,
                            maxWidth: 260,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}
                          title={audienceDisplayName(item)}
                        >
                          {audienceDisplayName(item)}
                        </span>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : pipesRegistryAudienceOptions.length ? (
                <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                  {pipesRegistryAudienceOptions.slice(0, 6).map((item) => (
                    <span
                      key={item.id || item.external_segment_id || item.name}
                      style={{
                        display: 'inline-flex',
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.full,
                        background: t.color.surface,
                        color: t.color.textSecondary,
                        padding: '4px 8px',
                        fontSize: t.font.sizeXs,
                        maxWidth: 260,
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                      title={audienceDisplayName(item)}
                    >
                      {audienceDisplayName(item)}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>

            {readiness && (readiness.blockers.length || readiness.warnings.length || readiness.reasons.length || readinessActions.length) ? (
              <DecisionStatusCard
                title="Pipeline diagnostics"
                status={readiness.status}
                subtitle="Detailed health checks are secondary to the pipeline state above."
                blockers={readiness.blockers}
                warnings={[...readiness.warnings, ...readiness.reasons]}
                actions={readinessActions.slice(0, 3)}
                onActionClick={handleReadinessAction}
              />
            ) : null}
          </div>
        </SectionCard>

        <SectionCard title="Detailed workspace">
          <div style={{ display: 'grid', gap: t.space.md }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Advanced setup, replay, normalization, quarantine, and webhook diagnostics. The default workflow should run from the pipeline summary above.
              </div>
              <button
                type="button"
                onClick={() => setDetailsOpen((open) => !open)}
                style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', fontWeight: t.font.weightMedium }}
              >
                {detailsOpen ? 'Collapse detailed workspace' : 'Expand detailed workspace'}
              </button>
            </div>
            {detailsOpen ? (
              <MeiroIntegrationPanel
              meiroTab={meiroTab}
              setMeiroTab={setMeiroTab}
              meiroUrl={meiroUrl}
              setMeiroUrl={setMeiroUrl}
              meiroKey={meiroKey}
              setMeiroKey={setMeiroKey}
              webhookSecretValue={webhookSecretValue}
              meiroPullDraft={meiroPullDraft}
              setMeiroPullDraft={setMeiroPullDraft}
              meiroConfig={meiroConfigQuery.data}
              meiroMappingState={meiroMappingQuery.data}
              meiroWebhookSuggestions={meiroWebhookSuggestionsQuery.data}
              meiroWebhookEvents={meiroWebhookEventsQuery.data}
              meiroEventArchive={meiroEventArchiveQuery.data}
              meiroWebhookDiagnostics={meiroWebhookDiagnosticsQuery.data}
              meiroWebhookArchiveStatus={meiroWebhookArchiveStatusQuery.data}
              meiroEventArchiveStatus={meiroEventArchiveStatusQuery.data}
              meiroWebhookEventsLoading={meiroWebhookEventsQuery.isLoading}
              meiroWebhookEventsError={(meiroWebhookEventsQuery.error as Error | undefined)?.message || null}
              meiroWebhookDiagnosticsError={(meiroWebhookDiagnosticsQuery.error as Error | undefined)?.message || null}
              meiroWebhookSuggestionsLoading={meiroWebhookSuggestionsQuery.isLoading}
              meiroWebhookSuggestionsError={(meiroWebhookSuggestionsQuery.error as Error | undefined)?.message || null}
              testMeiroResult={testMeiroMutation.data}
              saveMeiroPullPending={saveMeiroPullMutation.isPending || pinRawEventsPrimaryMutation.isPending}
              runMeiroPullPending={runMeiroPullMutation.isPending}
              applyMeiroMappingSuggestionPending={applyMeiroMappingSuggestionMutation.isPending}
              updateMeiroMappingApprovalPending={updateMeiroMappingApprovalMutation.isPending}
              meiroDryRunPending={meiroDryRunMutation.isPending}
              meiroDryRunData={meiroDryRunMutation.data}
              importFromMeiroPending={importFromMeiroMutation.isPending}
              importFromMeiroResult={importFromMeiroMutation.data ?? null}
              deciEngineImportDraft={deciEngineImportDraft}
              deciEngineImportPending={importDeciEngineEventsMutation.isPending}
              deciEngineImportResult={importDeciEngineEventsMutation.data ?? null}
              deciEngineImportError={(importDeciEngineEventsMutation.error as Error | undefined)?.message || null}
              deciEngineConfigSaving={saveDeciEngineEventsConfigMutation.isPending}
              deciEngineConfigSaved={saveDeciEngineEventsConfigMutation.isSuccess}
              deciEngineConfigError={(saveDeciEngineEventsConfigMutation.error as Error | undefined)?.message || null}
              reprocessWebhookArchivePending={reprocessWebhookArchiveMutation.isPending}
              reprocessWebhookArchiveResult={reprocessWebhookArchiveMutation.data ?? null}
              reprocessQuarantinePending={reprocessSelectedQuarantineMutation.isPending}
              reprocessQuarantineResult={reprocessSelectedQuarantineMutation.data ?? null}
              reprocessQuarantineError={reprocessSelectedQuarantineMutation.error ? (reprocessSelectedQuarantineMutation.error as Error).message : null}
              quarantineRuns={meiroQuarantineRunsQuery.data}
              quarantineRunsLoading={meiroQuarantineRunsQuery.isLoading}
              quarantineRunsError={(meiroQuarantineRunsQuery.error as Error | undefined)?.message || null}
              selectedQuarantineRun={meiroQuarantineRunQuery.data ?? null}
              selectedQuarantineRunLoading={meiroQuarantineRunQuery.isLoading}
              selectedQuarantineRunError={(meiroQuarantineRunQuery.error as Error | undefined)?.message || null}
              pipelineAudienceScope={selectedPipelineAudience}
              relativeTime={relativeTime}
              setOauthToast={setOauthToast}
              onTestMeiro={() => testMeiroMutation.mutate()}
              onPinRawEventsSource={() => pinRawEventsPrimaryMutation.mutate()}
              onConnectMeiro={() => connectMeiroMutation.mutate()}
              onDisconnectMeiro={() => disconnectMeiroMutation.mutate()}
              onRotateWebhookSecret={() => rotateWebhookSecretMutation.mutate()}
              onSaveMeiroPull={() => saveMeiroPullMutation.mutate(normalizeMeiroPullConfig(meiroPullDraft))}
              onRunMeiroPull={() => runMeiroPullMutation.mutate()}
              onSaveMeiroMapping={(payload) => saveMeiroMappingMutation.mutate(payload)}
              onApplyMeiroMappingSuggestion={() => applyMeiroMappingSuggestionMutation.mutate((meiroWebhookSuggestionsQuery.data as any)?.apply_payloads?.mapping || {})}
              onApproveMeiroMapping={() => updateMeiroMappingApprovalMutation.mutate({ status: 'approved', note: 'Approved from normalization review' })}
              onRejectMeiroMapping={() => updateMeiroMappingApprovalMutation.mutate({ status: 'rejected', note: 'Rejected from normalization review' })}
              onDryRun={() => meiroDryRunMutation.mutate()}
              onImportFromMeiro={() => importFromMeiroMutation.mutate()}
              onDeciEngineImportDraftChange={setDeciEngineImportDraft}
              onImportDeciEngineEvents={() => {
                if (window.confirm('This import replaces existing journeys with deciEngine activation events. Continue?')) {
                  importDeciEngineEventsMutation.mutate()
                }
              }}
              onSaveDeciEngineConfig={() => saveDeciEngineEventsConfigMutation.mutate()}
              onReplayArchive={() => reprocessWebhookArchiveMutation.mutate()}
              onReprocessSelectedQuarantine={(recordIndices) => reprocessSelectedQuarantineMutation.mutate(recordIndices)}
              onSelectQuarantineRun={(runId) => setSelectedQuarantineRunId(runId)}
            />
            ) : null}
          </div>
        </SectionCard>
      </div>
    </DashboardPage>
  )
}
