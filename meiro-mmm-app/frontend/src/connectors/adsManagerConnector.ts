import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

export type AdsProviderKey = 'google_ads' | 'meta_ads' | 'linkedin_ads'
export type AdsEntityType = 'campaign' | 'adset' | 'adgroup'
export type AdsActionType = 'pause' | 'enable' | 'update_budget'

export interface AdsEntityItem {
  id: string
  provider: AdsProviderKey
  account_id: string
  entity_type: AdsEntityType
  entity_id: string
  entity_name: string | null
  deep_link: string
  last_seen_ts?: string | null
}

export interface AdsEntityListResponse {
  items: AdsEntityItem[]
  total: number
}

export interface AdsStateResponse {
  provider: AdsProviderKey
  account_id: string
  entity_type: AdsEntityType
  entity_id: string
  deep_link: string
  status?: string
  budget?: number | null
  currency?: string | null
  name?: string | null
  needs_reauth?: boolean
  error?: { code?: string; message?: string } | null
}

export interface AdsChangeRequest {
  id: string
  provider: AdsProviderKey
  account_id: string
  entity_type: AdsEntityType
  entity_id: string
  action_type: AdsActionType
  action_payload: Record<string, unknown>
  status: 'draft' | 'pending_approval' | 'approved' | 'rejected' | 'applied' | 'failed' | 'cancelled'
  approval_required: boolean
  approved_by_user_id?: string | null
  approved_at?: string | null
  applied_at?: string | null
  error_message?: string | null
  created_at?: string | null
  updated_at?: string | null
}

export interface AdsChangeRequestListResponse {
  items: AdsChangeRequest[]
  total: number
}

export interface BudgetRecommendationTargetInput {
  channel: string
  provider: AdsProviderKey
  entity_id: string
  entity_name?: string | null
  account_id?: string | null
  delta_pct: number
  reason?: string | null
}

export interface BudgetRecommendationCreateResponse {
  items: AdsChangeRequest[]
  skipped: Array<Record<string, unknown>>
  total: number
  skipped_total: number
}

export interface AdsAuditItem {
  id: string
  actor_user_id: string
  provider: AdsProviderKey
  account_id: string
  entity_type: AdsEntityType
  entity_id: string
  event_type: string
  event_payload: Record<string, unknown>
  created_at?: string | null
}

export interface AdsAuditListResponse {
  items: AdsAuditItem[]
  total: number
}

export async function getAdsDeepLink(input: {
  provider: AdsProviderKey
  accountId?: string | null
  entityType: AdsEntityType
  entityId: string
}): Promise<{ provider: AdsProviderKey; entity_type: AdsEntityType; entity_id: string; url: string }> {
  return apiGetJson(withQuery('/api/ads/deeplink', {
    provider: input.provider,
    account_id: input.accountId || undefined,
    entity_type: input.entityType,
    entity_id: input.entityId,
  }), { fallbackMessage: 'Failed to build provider deep link' })
}

export async function getAdsState(input: {
  provider: AdsProviderKey
  accountId: string
  entityType: AdsEntityType
  entityId: string
}): Promise<AdsStateResponse> {
  return apiGetJson(withQuery('/api/ads/state', {
    provider: input.provider,
    account_id: input.accountId,
    entity_type: input.entityType,
    entity_id: input.entityId,
  }), { fallbackMessage: 'Failed to load ads entity state' })
}

export async function createAdsChangeRequest(input: {
  provider: AdsProviderKey
  accountId: string
  entityType: AdsEntityType
  entityId: string
  actionType: AdsActionType
  actionPayload?: Record<string, unknown>
}): Promise<AdsChangeRequest> {
  return apiSendJson('/api/ads/change-requests', 'POST', {
    provider: input.provider,
    account_id: input.accountId,
    entity_type: input.entityType,
    entity_id: input.entityId,
    action_type: input.actionType,
    action_payload: input.actionPayload || {},
  }, { fallbackMessage: 'Failed to create change request' })
}

export async function createAdsChangeRequestsFromBudgetRecommendation(input: {
  runId: string
  scenarioId: string
  recommendationId?: string | null
  currency?: string
  targets: BudgetRecommendationTargetInput[]
}): Promise<BudgetRecommendationCreateResponse> {
  return apiSendJson('/api/ads/change-requests/from-budget-recommendation', 'POST', {
    run_id: input.runId,
    scenario_id: input.scenarioId,
    recommendation_id: input.recommendationId || undefined,
    currency: input.currency || 'USD',
    targets: input.targets,
  }, { fallbackMessage: 'Failed to create recommendation change requests' })
}

export async function approveAdsChangeRequest(requestId: string): Promise<AdsChangeRequest> {
  return apiSendJson(`/api/ads/change-requests/${requestId}/approve`, 'POST', undefined, {
    fallbackMessage: 'Failed to approve request',
  })
}

export async function rejectAdsChangeRequest(requestId: string, reason?: string): Promise<AdsChangeRequest> {
  return apiSendJson(`/api/ads/change-requests/${requestId}/reject`, 'POST', { reason: reason || undefined }, {
    fallbackMessage: 'Failed to reject request',
  })
}

export async function applyAdsChangeRequest(requestId: string, adminOverride?: boolean): Promise<AdsChangeRequest> {
  return apiSendJson(`/api/ads/change-requests/${requestId}/apply`, 'POST', { admin_override: !!adminOverride }, {
    fallbackMessage: 'Failed to apply request',
  })
}

export async function listAdsChangeRequests(params?: { status?: string; provider?: string; limit?: number }): Promise<AdsChangeRequestListResponse> {
  return apiGetJson(withQuery('/api/ads/change-requests', {
    status: params?.status,
    provider: params?.provider,
    limit: params?.limit,
  }), { fallbackMessage: 'Failed to load ads change requests' })
}

export async function listAdsAudit(params?: { provider?: string; entityId?: string; limit?: number }): Promise<AdsAuditListResponse> {
  return apiGetJson(withQuery('/api/ads/audit', {
    provider: params?.provider,
    entity_id: params?.entityId,
    limit: params?.limit,
  }), { fallbackMessage: 'Failed to load ads audit log' })
}
