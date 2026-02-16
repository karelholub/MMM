import { apiGetJson, apiSendJson, withQuery } from '../../lib/apiClient'
import { buildListQuery, type PaginatedResponse } from '../../lib/apiSchemas'

export type JourneyAlertDomain = 'journeys' | 'funnels'

export interface JourneyAlertDefinitionItem {
  id: string
  name: string
  type: string
  domain: JourneyAlertDomain
  scope: Record<string, unknown>
  metric: string
  condition?: Record<string, unknown>
  schedule?: Record<string, unknown>
  is_enabled: boolean
  created_at?: string | null
  updated_at?: string | null
}

export interface JourneyAlertEventItem {
  id: string
  alert_definition_id: string
  domain: JourneyAlertDomain
  triggered_at: string | null
  severity: string
  summary: string
  details?: Record<string, unknown>
}

export interface JourneyAlertCreatePayload {
  name: string
  type: string
  domain: JourneyAlertDomain
  scope: Record<string, unknown>
  metric: string
  condition: Record<string, unknown>
  schedule?: Record<string, unknown>
  is_enabled?: boolean
}

export interface JourneyAlertPreviewResponse {
  current_value: number | null
  baseline_value: number | null
  delta_pct: number | null
  window: {
    current_from: string
    current_to: string
    baseline_from: string
    baseline_to: string
  }
}

export async function listJourneyAlertDefinitions(
  domain: JourneyAlertDomain,
  opts: { page?: number; perPage?: number } = {},
): Promise<PaginatedResponse<JourneyAlertDefinitionItem>> {
  return apiGetJson<PaginatedResponse<JourneyAlertDefinitionItem>>(
    withQuery('/api/alerts', buildListQuery({ domain, page: opts.page ?? 1, perPage: opts.perPage ?? 100 })),
    { fallbackMessage: 'Failed to load alert definitions' },
  )
}

export async function listJourneyAlertEvents(
  domain: JourneyAlertDomain,
  opts: { page?: number; perPage?: number } = {},
): Promise<PaginatedResponse<JourneyAlertEventItem>> {
  return apiGetJson<PaginatedResponse<JourneyAlertEventItem>>(
    withQuery('/api/alerts/events', buildListQuery({ domain, page: opts.page ?? 1, perPage: opts.perPage ?? 100 }, 200)),
    { fallbackMessage: 'Failed to load alert events' },
  )
}

export async function createJourneyAlert(payload: JourneyAlertCreatePayload): Promise<JourneyAlertDefinitionItem> {
  return apiSendJson<JourneyAlertDefinitionItem>('/api/alerts', 'POST', payload, {
    fallbackMessage: 'Failed to create alert',
  })
}

export async function previewJourneyAlert(
  payload: { type: string; scope: Record<string, unknown>; metric: string; condition: Record<string, unknown> },
): Promise<JourneyAlertPreviewResponse> {
  return apiSendJson<JourneyAlertPreviewResponse>('/api/alerts/preview', 'POST', payload, {
    fallbackMessage: 'Failed to preview alert',
  })
}

