import { apiSendJson } from '../lib/apiClient'

export async function fetchMetaAds(params: { ad_account_id: string; since: string; until: string; avg_aov: number }) {
  return apiSendJson<any>('/api/connectors/meta', 'POST', params, {
    fallbackMessage: 'Meta fetch failed',
  })
}

