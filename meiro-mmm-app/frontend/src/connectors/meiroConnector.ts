// Meiro CDP connector – frontend API wrapper
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

export interface MeiroConfig {
  connected: boolean
  api_base_url: string | null
  target_instance_url?: string | null
  target_instance_host?: string | null
  strict_instance_scope?: boolean
  cdp_instance_scope?: {
    status: 'not_configured' | 'in_scope' | 'out_of_scope' | string
    configured_url?: string | null
    configured_host?: string | null
    target_url?: string | null
    target_host?: string | null
    reason?: string | null
  }
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
  analysis_source?: 'event_archive' | 'webhook_history' | string
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
  taxonomy_suggestions?: {
    channel_rules?: Array<Record<string, unknown>>
    source_aliases?: Record<string, string>
    medium_aliases?: Record<string, string>
    top_sources?: Array<{ source: string; count: number }>
    top_mediums?: Array<{ medium: string; count: number }>
    top_campaigns?: Array<{ campaign: string; count: number }>
    observed_pairs?: Array<{ source: string; medium: string; count: number }>
    unresolved_pairs?: Array<{ source: string; medium: string; count: number }>
  }
  mapping_suggestions?: {
    touchpoint_attr_candidates?: Array<{ path: string; count: number }>
    value_field_candidates?: Array<{ path: string; count: number }>
    currency_field_candidates?: Array<{ path: string; count: number }>
    channel_field_candidates?: Array<{ path: string; count: number }>
    source_field_candidates?: Array<{ path: string; count: number }>
    medium_field_candidates?: Array<{ path: string; count: number }>
    campaign_field_candidates?: Array<{ path: string; count: number }>
  }
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

export type MeiroNativeCampaignChannel = 'email' | 'push' | 'whatsapp'

export interface MeiroApiStatus {
  configured: boolean
  domain: string | null
  username: string | null
  has_password: boolean
  timeout_ms: number
  token_cached: boolean
  last_login_at?: string | null
}

export interface MeiroNativeCampaign {
  id: string
  channel: MeiroNativeCampaignChannel
  name: string
  deleted: boolean
  modifiedAt?: string | null
  lastActivationAt?: string | null
  schedules?: unknown[]
  segmentIds?: string[]
  frequencyCap?: unknown
  raw?: Record<string, unknown>
}

export interface MeiroWbsProfile {
  status: string
  customerEntityId?: string | null
  returnedAttributes: Record<string, unknown>
  data: Record<string, unknown>
  raw: Record<string, unknown>
}

export interface MeiroWbsSegments {
  status: string
  segmentIds: string[]
  raw: Record<string, unknown>
}

export async function connectMeiroCDP(params: { api_base_url: string; api_key: string }) {
  return apiSendJson<any>('/api/connectors/meiro/connect', 'POST', params, { fallbackMessage: 'Connection failed' })
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

export async function getMeiroConfig(): Promise<MeiroConfig> {
  return apiGetJson<MeiroConfig>('/api/connectors/meiro/config', { fallbackMessage: 'Failed to fetch Meiro config' })
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

export async function getMeiroApiStatus(): Promise<MeiroApiStatus> {
  return apiGetJson<MeiroApiStatus>('/v1/meiro/api/status', {
    fallbackMessage: 'Failed to fetch Meiro API status',
  })
}

export async function checkMeiroApiLogin(): Promise<MeiroApiStatus & { ok: boolean }> {
  return apiSendJson<MeiroApiStatus & { ok: boolean }>('/v1/meiro/api/check-login', 'POST', undefined, {
    fallbackMessage: 'Meiro API login check failed',
  })
}

export async function getMeiroNativeCampaigns(params: {
  channel?: MeiroNativeCampaignChannel
  limit?: number
  offset?: number
  q?: string
  includeDeleted?: boolean
} = {}): Promise<{ items: MeiroNativeCampaign[]; total: number; limit: number; offset: number }> {
  return apiGetJson<{ items: MeiroNativeCampaign[]; total: number; limit: number; offset: number }>(
    withQuery('/v1/meiro/native-campaigns', params),
    { fallbackMessage: 'Failed to fetch Meiro campaigns' },
  )
}

export async function getMeiroNativeCampaign(
  channel: MeiroNativeCampaignChannel,
  id: string,
): Promise<MeiroNativeCampaign> {
  return apiGetJson<MeiroNativeCampaign>(
    `/v1/meiro/native-campaigns/${encodeURIComponent(channel)}/${encodeURIComponent(id)}`,
    { fallbackMessage: 'Failed to fetch Meiro campaign' },
  )
}

export async function lookupMeiroWbsProfile(params: {
  attribute: string
  value: string
  categoryId?: string
}): Promise<MeiroWbsProfile> {
  return apiGetJson<MeiroWbsProfile>(
    withQuery('/v1/meiro/audience/profile', params),
    { fallbackMessage: 'Failed to fetch Meiro WBS profile' },
  )
}

export async function lookupMeiroWbsSegments(params: {
  attribute: string
  value: string
  tag?: string
}): Promise<MeiroWbsSegments> {
  return apiGetJson<MeiroWbsSegments>(
    withQuery('/v1/meiro/audience/segments', params),
    { fallbackMessage: 'Failed to fetch Meiro WBS segments' },
  )
}
