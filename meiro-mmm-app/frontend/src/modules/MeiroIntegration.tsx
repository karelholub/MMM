import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import DecisionStatusCard from '../components/DecisionStatusCard'
import type { RecommendedActionItem } from '../components/RecommendedActionsList'
import MeiroIntegrationPanel from '../features/meiro/MeiroIntegrationPanel'
import { tokens as t } from '../theme/tokens'
import {
  connectMeiroCDP,
  disconnectMeiroCDP,
  getMeiroConfig,
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
  type MeiroWebhookDiagnostics,
} from '../connectors/meiroConnector'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import {
  DEFAULT_MEIRO_PULL_CONFIG,
  normalizeMeiroPullConfig,
  type MeiroTab,
  type MeiroWebhookArchiveStatus,
  type MeiroWebhookReprocessResult,
} from '../features/meiro/shared'

interface MeiroIntegrationPageProps {
  onJourneysImported: () => void
}

interface MeiroReadinessResponse {
  status: string
  confidence: { score: number; band: string }
  summary: {
    cdp_connected: boolean
    webhook_received_count: number
    webhook_has_secret: boolean
    mapping_status: string
    mapping_version: number
    archive_entries: number
    last_test_at?: string | null
    last_webhook_received_at?: string | null
    conversion_selector?: string | null
  }
  blockers: string[]
  warnings: string[]
  reasons: string[]
  recommended_actions: RecommendedActionItem[]
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

function statusTone(connected: boolean, warning: boolean) {
  if (!connected) return { color: t.color.danger, bg: t.color.dangerMuted, label: 'Disconnected' }
  if (warning) return { color: t.color.warning, bg: t.color.warningMuted, label: 'Needs review' }
  return { color: t.color.success, bg: t.color.successMuted, label: 'Ready' }
}

export default function MeiroIntegrationPage({ onJourneysImported }: MeiroIntegrationPageProps) {
  const queryClient = useQueryClient()
  const [meiroTab, setMeiroTab] = useState<MeiroTab>('overview')
  const [meiroUrl, setMeiroUrl] = useState('')
  const [meiroKey, setMeiroKey] = useState('')
  const [webhookSecretValue, setWebhookSecretValue] = useState<string | null>(null)
  const [meiroPullDraft, setMeiroPullDraft] = useState<MeiroPullConfig>(DEFAULT_MEIRO_PULL_CONFIG)
  const [oauthToast, setOauthToast] = useState<string | null>(null)
  const [selectedQuarantineRunId, setSelectedQuarantineRunId] = useState<string | null>(null)

  const meiroConfigQuery = useQuery({ queryKey: ['meiro-config'], queryFn: getMeiroConfig })
  const meiroReadinessQuery = useQuery<MeiroReadinessResponse>({
    queryKey: ['meiro-readiness'],
    queryFn: async () =>
      apiGetJson<MeiroReadinessResponse>('/api/connectors/meiro/readiness', {
        fallbackMessage: 'Failed to load Meiro readiness',
      }),
  })
  const meiroMappingQuery = useQuery({ queryKey: ['meiro-mapping'], queryFn: getMeiroMapping })
  const meiroPullConfigQuery = useQuery({ queryKey: ['meiro-pull-config'], queryFn: getMeiroPullConfig })
  const meiroWebhookSuggestionsQuery = useQuery({
    queryKey: ['meiro-webhook-suggestions-page'],
    queryFn: () => getMeiroWebhookSuggestions(100),
  })
  const meiroWebhookEventsQuery = useQuery({
    queryKey: ['meiro-webhook-events-page'],
    queryFn: () => getMeiroWebhookEvents(100),
  })
  const meiroWebhookDiagnosticsQuery = useQuery<MeiroWebhookDiagnostics>({
    queryKey: ['meiro-webhook-diagnostics-page'],
    queryFn: () => getMeiroWebhookDiagnostics(100),
  })
  const meiroWebhookArchiveStatusQuery = useQuery<MeiroWebhookArchiveStatus>({
    queryKey: ['meiro-webhook-archive-status-page'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookArchiveStatus>('/api/connectors/meiro/webhook/archive-status', {
        fallbackMessage: 'Failed to load webhook archive status',
      }),
  })
  const meiroEventArchiveStatusQuery = useQuery<MeiroWebhookArchiveStatus>({
    queryKey: ['meiro-event-archive-status-page'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookArchiveStatus>('/api/connectors/meiro/events/archive-status', {
        fallbackMessage: 'Failed to load event archive status',
      }),
  })
  const meiroQuarantineRunsQuery = useQuery({
    queryKey: ['meiro-quarantine-runs'],
    queryFn: () => getMeiroQuarantineRuns(10),
  })
  const meiroQuarantineRunQuery = useQuery<MeiroQuarantineRun>({
    queryKey: ['meiro-quarantine-run', selectedQuarantineRunId],
    queryFn: () => getMeiroQuarantineRun(String(selectedQuarantineRunId)),
    enabled: Boolean(selectedQuarantineRunId),
  })

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

  const readiness = meiroReadinessQuery.data
  const mappingStatus = (readiness?.summary.mapping_status || meiroMappingQuery.data?.approval?.status || '').toLowerCase()
  const mappingNeedsReview = mappingStatus !== 'approved'
  const archiveBacklog = Number(readiness?.summary.archive_entries || meiroWebhookArchiveStatusQuery.data?.entries || 0) > 0
  const tone = statusTone(
    readiness?.summary.cdp_connected ?? !!meiroConfigQuery.data?.connected,
    readiness?.status === 'warning' || readiness?.status === 'blocked' || mappingNeedsReview || archiveBacklog,
  )

  const handleReadinessAction = (action: RecommendedActionItem) => {
    if (action.target_tab) {
      setMeiroTab(action.target_tab as MeiroTab)
    }
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

        <SectionCard title="Integration readiness">
          <div style={{ display: 'grid', gap: t.space.md }}>
            {readiness ? (
              <DecisionStatusCard
                title="Operational readiness"
                status={readiness.status}
                subtitle={`Confidence ${readiness.confidence.band} (${readiness.confidence.score}/100)`}
                blockers={readiness.blockers}
                warnings={[...readiness.warnings, ...readiness.reasons]}
                actions={readiness.recommended_actions}
                onActionClick={handleReadinessAction}
              />
            ) : null}
            <div
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: t.space.sm,
                alignSelf: 'flex-start',
                padding: '6px 10px',
                borderRadius: t.radius.full,
                background: tone.bg,
                color: tone.color,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
              }}
            >
              {tone.label}
            </div>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                gap: t.space.sm,
              }}
            >
              {[
                {
                  label: 'CDP pull',
                  value: readiness?.summary.cdp_connected ? 'Connected' : 'Disconnected',
                  meta: readiness?.summary.last_test_at ? `Last test ${relativeTime(readiness.summary.last_test_at)}` : 'No successful test yet',
                },
                {
                  label: 'Pipes webhook',
                  value: webhookSecretValue || readiness?.summary.webhook_has_secret ? 'Configured' : 'Not configured',
                  meta: (readiness?.summary.webhook_received_count || 0) > 0 ? `${Number(readiness?.summary.webhook_received_count || 0).toLocaleString()} payloads received` : 'No recent webhook events',
                },
                {
                  label: 'Mapping approval',
                  value: readiness?.summary.mapping_status ? readiness.summary.mapping_status.replace('_', ' ') : 'Pending',
                  meta: meiroMappingQuery.data?.approval?.updated_at ? `Updated ${relativeTime(meiroMappingQuery.data.approval.updated_at)}` : 'No mapping decision yet',
                },
                {
                  label: 'Replay backlog',
                  value: String(readiness?.summary.archive_entries || 0),
                  meta: archiveBacklog ? 'Archived events available for replay' : 'No archived backlog detected',
                },
              ].map((item) => (
                <div
                  key={item.label}
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    background: t.color.surface,
                    padding: t.space.md,
                    display: 'grid',
                    gap: 4,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>
                    {item.label}
                  </div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {item.value}
                  </div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    {item.meta}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </SectionCard>

        <SectionCard title="Workspace">
          <div style={{ display: 'grid', gap: t.space.md }}>
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
              meiroWebhookDiagnostics={meiroWebhookDiagnosticsQuery.data}
              meiroWebhookArchiveStatus={meiroWebhookArchiveStatusQuery.data}
              meiroEventArchiveStatus={meiroEventArchiveStatusQuery.data}
              meiroWebhookEventsLoading={meiroWebhookEventsQuery.isLoading}
              meiroWebhookEventsError={(meiroWebhookEventsQuery.error as Error | undefined)?.message || null}
              meiroWebhookDiagnosticsError={(meiroWebhookDiagnosticsQuery.error as Error | undefined)?.message || null}
              meiroWebhookSuggestionsLoading={meiroWebhookSuggestionsQuery.isLoading}
              meiroWebhookSuggestionsError={(meiroWebhookSuggestionsQuery.error as Error | undefined)?.message || null}
              testMeiroResult={testMeiroMutation.data}
              saveMeiroPullPending={saveMeiroPullMutation.isPending}
              runMeiroPullPending={runMeiroPullMutation.isPending}
              applyMeiroMappingSuggestionPending={applyMeiroMappingSuggestionMutation.isPending}
              updateMeiroMappingApprovalPending={updateMeiroMappingApprovalMutation.isPending}
              meiroDryRunPending={meiroDryRunMutation.isPending}
              meiroDryRunData={meiroDryRunMutation.data}
              importFromMeiroPending={importFromMeiroMutation.isPending}
              importFromMeiroResult={importFromMeiroMutation.data ?? null}
              reprocessWebhookArchivePending={reprocessWebhookArchiveMutation.isPending}
              reprocessWebhookArchiveResult={reprocessWebhookArchiveMutation.data ?? null}
              reprocessQuarantinePending={reprocessSelectedQuarantineMutation.isPending}
              reprocessQuarantineResult={reprocessSelectedQuarantineMutation.data ?? null}
              quarantineRuns={meiroQuarantineRunsQuery.data}
              quarantineRunsLoading={meiroQuarantineRunsQuery.isLoading}
              quarantineRunsError={(meiroQuarantineRunsQuery.error as Error | undefined)?.message || null}
              selectedQuarantineRun={meiroQuarantineRunQuery.data ?? null}
              selectedQuarantineRunLoading={meiroQuarantineRunQuery.isLoading}
              selectedQuarantineRunError={(meiroQuarantineRunQuery.error as Error | undefined)?.message || null}
              relativeTime={relativeTime}
              setOauthToast={setOauthToast}
              onTestMeiro={() => testMeiroMutation.mutate()}
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
              onReplayArchive={() => reprocessWebhookArchiveMutation.mutate()}
              onReprocessSelectedQuarantine={(recordIndices) => reprocessSelectedQuarantineMutation.mutate(recordIndices)}
              onSelectQuarantineRun={(runId) => setSelectedQuarantineRunId(runId)}
            />
          </div>
        </SectionCard>
      </div>
    </DashboardPage>
  )
}
