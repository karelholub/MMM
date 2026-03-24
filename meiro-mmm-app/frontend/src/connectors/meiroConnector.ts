// Meiro CDP connector – frontend API wrapper
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

export interface MeiroConfig {
  connected: boolean
  api_base_url: string | null
  last_test_at: string | null
  has_key: boolean
  webhook_url: string
  webhook_last_received_at: string | null
  webhook_received_count: number
  webhook_has_secret: boolean
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
  ip?: string | null
  user_agent?: string | null
  payload_shape?: string | null
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
}

export interface MeiroWebhookSuggestions {
  generated_at: string
  events_analyzed: number
  total_conversions_observed: number
  total_touchpoints_observed: number
  dedup_key_suggestion: string
  dedup_key_candidates: Array<{
    key: string
    count: number
    coverage_pct: number
    recommended: boolean
  }>
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
