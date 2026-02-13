import { apiSendJson } from '../lib/apiClient'

export async function fetchLinkedInAds(params: { since: string; until: string }) {
  return apiSendJson<any>('/api/connectors/linkedin', 'POST', params, {
    fallbackMessage: 'LinkedIn fetch failed',
  })
}

