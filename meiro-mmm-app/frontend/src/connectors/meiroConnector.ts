// Meiro CDP connector – frontend API wrapper
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

export interface MeiroConfig {
  connected: boolean
  api_base_url: string | null
  last_test_at: string | null
  has_key: boolean
  webhook_url: string
  event_webhook_url?: string
  primary_ingest_source?: 'profiles' | 'events'
  webhook_last_received_at: string | null
  webhook_received_count: number
  event_webhook_last_received_at?: string | null
  event_webhook_received_count?: number
  webhook_has_secret: boolean
  auto_replay_state?: {
    last_attempted_at?: string | null
    last_completed_at?: string | null
    last_status?: string | null
    last_reason?: string | null
    last_trigger?: string | null
    last_archive_entries_seen?: number
    last_archive_received_at?: string | null
    last_result_summary?: Record<string, unknown>
  }
}

export interface MeiroMapping {
  touchpoint_attr?: string
  value_attr?: string
  id_attr?: string
  channel_field?: string
  timestamp_field?: string
  source_field?: string
  medium_field?: string
  campaign_field?: string
  currency_field?: string
  channel_mapping?: Record<string, string>
}

export interface MeiroMappingState {
  mapping: MeiroMapping
  approval: {
    status: string
    note?: string | null
    updated_at?: string | null
  }
  history: Array<Record<string, unknown>>
  version: number
  presets: Record<string, unknown>
}

export interface MeiroWebhookEvent {
  received_at: string
  received_count: number
  stored_total: number
  replace: boolean
  ingest_kind?: 'profiles' | 'events' | string | null
  ip?: string | null
  user_agent?: string | null
  payload_shape?: string | null
  status_code?: number
  outcome?: string | null
  error_class?: string | null
  error_detail?: string | null
}

export interface MeiroEventArchiveBatch {
  received_at: string
  received_count: number
  parser_version?: string | null
  replace?: boolean
  payload_shape?: string | null
  events?: Array<Record<string, unknown>>
}

export interface MeiroWebhookDiagnostics {
  ok: boolean
  health_url: string
  received_count: number
  last_received_at?: string | null
  recent_success_count: number
  recent_error_count: number
  recent_error_classes?: Record<string, number>
  latest_success?: Record<string, unknown> | null
  latest_error?: Record<string, unknown> | null
  notes?: string[]
}

export interface MeiroPullConfig {
  lookback_days: number
  session_gap_minutes: number
  conversion_selector: string
  output_mode?: string
  dedup_interval_minutes: number
  dedup_mode: 'strict' | 'balanced' | 'aggressive'
  primary_dedup_key: 'auto' | 'conversion_id' | 'order_id' | 'event_id'
  fallback_dedup_keys: Array<'conversion_id' | 'order_id' | 'event_id'>
  strict_ingest: boolean
  quarantine_unknown_channels: boolean
  quarantine_missing_utm: boolean
  quarantine_duplicate_profiles?: boolean
  timestamp_fallback_policy: 'profile' | 'conversion' | 'quarantine'
  value_fallback_policy: 'default' | 'zero' | 'quarantine'
  currency_fallback_policy: 'default' | 'quarantine'
  replay_mode?: 'all' | 'last_n' | 'date_range'
  primary_ingest_source?: 'profiles' | 'events'
  replay_archive_source?: 'auto' | 'profiles' | 'events'
  replay_archive_limit?: number
  replay_date_from?: string | null
  replay_date_to?: string | null
  auto_replay_mode?: 'disabled' | 'interval' | 'after_batch'
  auto_replay_interval_minutes?: number
  auto_replay_require_mapping_approval?: boolean
  auto_replay_quarantine_spike_threshold_pct?: number
  conversion_event_aliases?: Record<string, string>
  touchpoint_interaction_aliases?: Record<string, string>
  adjustment_event_aliases?: Record<string, string>
  adjustment_linkage_keys?: Array<'conversion_id' | 'order_id' | 'lead_id' | 'event_id'>
}

export interface MeiroWebhookSuggestions {
  generated_at: string
  events_analyzed: number
  total_conversions_observed: number
  total_touchpoints_observed: number
  event_stream_diagnostics?: {
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
  dedup_key_suggestion: string
  dedup_key_candidates: Array<{
    key: string
    count: number
    coverage_pct: number
    recommended: boolean
  }>
  sanitation_suggestions?: Array<{
    id: string
    type: string
    title: string
    description: string
    impact_count?: number
    confidence?: { score?: number; band?: string }
    recommended_action?: string
    payload?: Record<string, unknown>
  }>
  apply_payloads?: {
    sanitation?: Record<string, unknown>
    [key: string]: unknown
  }
}

export interface MeiroQuarantineRecord {
  journey_id?: string
  customer_id?: string
  reason_codes: string[]
  reasons?: Array<{ code: string; severity: string; message: string }>
  quality?: { score?: number; band?: string }
  original?: Record<string, unknown> | null
  normalized?: Record<string, unknown> | null
  remediation?: {
    status?: string
    updated_at?: string | null
    note?: string | null
    metadata?: Record<string, unknown>
    history?: Array<{
      at?: string
      status?: string
      note?: string | null
      metadata?: Record<string, unknown>
    }>
  } | null
}

export interface MeiroQuarantineRun {
  id: string
  source: string
  created_at: string
  import_note?: string | null
  parser_version?: string | null
  summary?: Record<string, unknown>
  records?: MeiroQuarantineRecord[]
}

export interface MeiroImportResult {
  import_summary?: Record<string, unknown>
  quarantine_count?: number
  count?: number
  quarantine_run_id?: string | null
}

export interface MeiroQuarantineReprocessResult {
  source_quarantine_run_id: string
  selected_record_count: number
  reprocessed_count: number
  quarantine_count: number
  quarantine_run_id?: string | null
  persisted_to_attribution: boolean
  replace_existing: boolean
  existing_count: number
  persisted_count: number
  import_summary?: Record<string, unknown>
}

export async function connectMeiroCDP(params: { api_base_url: string; api_key: string }) {
  return apiSendJson<any>('/api/connectors/meiro/connect', 'POST', params, { fallbackMessage: 'Connection failed' })
}

export async function saveMeiroCDP(params: { api_base_url: string; api_key: string }) {
  return apiSendJson<any>('/api/connectors/meiro/save', 'POST', params, { fallbackMessage: 'Save failed' })
}

export async function testMeiroConnection(params?: {
  api_base_url?: string
  api_key?: string
  save_on_success?: boolean
}) {
  return apiSendJson<any>('/api/connectors/meiro/test', 'POST', params || {}, {
    fallbackMessage: 'Connection test failed',
  })
}

export async function disconnectMeiroCDP() {
  return apiSendJson<any>('/api/connectors/meiro', 'DELETE', undefined, { fallbackMessage: 'Disconnect failed' })
}

export async function getMeiroCDPStatus(): Promise<{ connected: boolean }> {
  return apiGetJson<{ connected: boolean }>('/api/connectors/meiro/status', {
    fallbackMessage: 'Failed to fetch Meiro CDP status',
  })
}

export async function getMeiroConfig(): Promise<MeiroConfig> {
  return apiGetJson<MeiroConfig>('/api/connectors/meiro/config', { fallbackMessage: 'Failed to fetch Meiro config' })
}

export async function getMeiroAttributes() {
  return apiGetJson<any>('/api/connectors/meiro/attributes', { fallbackMessage: 'Failed to fetch attributes' })
}

export async function getMeiroEvents() {
  return apiGetJson<any>('/api/connectors/meiro/events', { fallbackMessage: 'Failed to fetch events' })
}

export async function getMeiroSegments() {
  return apiGetJson<any>('/api/connectors/meiro/segments', { fallbackMessage: 'Failed to fetch segments' })
}

export async function fetchMeiroCDPData(params: {
  since: string
  until: string
  event_types?: string[]
  attributes?: string[]
  segment_id?: string
}) {
  return apiSendJson<any>('/api/connectors/meiro/fetch', 'POST', params, {
    fallbackMessage: 'Fetch failed',
  })
}

export async function getMeiroMapping(): Promise<MeiroMappingState> {
  return apiGetJson<MeiroMappingState>(
    '/api/connectors/meiro/mapping',
    { fallbackMessage: 'Failed to fetch mapping' },
  )
}

export async function saveMeiroMapping(mapping: MeiroMapping) {
  return apiSendJson<any>('/api/connectors/meiro/mapping', 'POST', mapping, {
    fallbackMessage: 'Save failed',
  })
}

export async function updateMeiroMappingApproval(payload: { status: string; note?: string }) {
  return apiSendJson<any>('/api/connectors/meiro/mapping/approval', 'POST', payload, {
    fallbackMessage: 'Failed to update mapping approval',
  })
}

export async function getMeiroPullConfig(): Promise<MeiroPullConfig> {
  return apiGetJson<MeiroPullConfig>('/api/connectors/meiro/pull-config', {
    fallbackMessage: 'Failed to fetch pull config',
  })
}

export async function saveMeiroPullConfig(config: MeiroPullConfig | Record<string, unknown>) {
  return apiSendJson<any>('/api/connectors/meiro/pull-config', 'POST', config, {
    fallbackMessage: 'Save failed',
  })
}

export async function getMeiroWebhookSuggestions(limit = 100): Promise<MeiroWebhookSuggestions> {
  return apiGetJson<MeiroWebhookSuggestions>(
    withQuery('/api/connectors/meiro/webhook/suggestions', { limit }),
    { fallbackMessage: 'Failed to fetch webhook suggestions' },
  )
}

export async function meiroPull(since?: string, until?: string) {
  return apiSendJson<any>(
    withQuery('/api/connectors/meiro/pull', { since, until }),
    'POST',
    undefined,
    { fallbackMessage: 'Pull failed' },
  )
}

export async function meiroRotateWebhookSecret(): Promise<{ secret: string }> {
  return apiSendJson<{ secret: string }>('/api/connectors/meiro/webhook/rotate-secret', 'POST', undefined, {
    fallbackMessage: 'Rotate failed',
  })
}

export async function getMeiroWebhookEvents(limit = 100): Promise<{ items: MeiroWebhookEvent[]; total: number }> {
  return apiGetJson<{ items: MeiroWebhookEvent[]; total: number }>(
    withQuery('/api/connectors/meiro/webhook/events', { limit }),
    { fallbackMessage: 'Failed to fetch webhook event log' },
  )
}

export async function getMeiroEventArchive(limit = 25): Promise<{ items: MeiroEventArchiveBatch[]; total: number }> {
  return apiGetJson<{ items: MeiroEventArchiveBatch[]; total: number }>(
    withQuery('/api/connectors/meiro/events/archive', { limit }),
    { fallbackMessage: 'Failed to fetch Meiro raw-event archive' },
  )
}

export async function getMeiroWebhookDiagnostics(limit = 100): Promise<MeiroWebhookDiagnostics> {
  return apiGetJson<MeiroWebhookDiagnostics>(
    withQuery('/api/connectors/meiro/webhook/diagnostics', { limit }),
    { fallbackMessage: 'Failed to fetch Meiro webhook diagnostics' },
  )
}

export async function meiroDryRun(limit = 100): Promise<{
  count: number
  preview: Array<{ id: string; touchpoints: number; value: number }>
  warnings: string[]
  validation: { ok?: boolean; error?: string }
}> {
  return apiSendJson<any>(withQuery('/api/connectors/meiro/dry-run', { limit }), 'POST', undefined, {
    fallbackMessage: 'Dry run failed',
  })
}

export async function getMeiroQuarantineRuns(limit = 10): Promise<{ items: MeiroQuarantineRun[]; total: number }> {
  return apiGetJson<{ items: MeiroQuarantineRun[]; total: number }>(
    withQuery('/api/connectors/meiro/quarantine', { limit }),
    { fallbackMessage: 'Failed to fetch Meiro quarantine runs' },
  )
}

export async function getMeiroQuarantineRun(runId: string): Promise<MeiroQuarantineRun> {
  return apiGetJson<MeiroQuarantineRun>(`/api/connectors/meiro/quarantine/${runId}`, {
    fallbackMessage: 'Failed to fetch Meiro quarantine run',
  })
}
