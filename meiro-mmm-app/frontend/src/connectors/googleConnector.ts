import { apiSendJson } from '../lib/apiClient'

export async function fetchGoogleAds(params: { from: string; to: string }) {
  return apiSendJson<any>('/api/connectors/google', 'POST', {
    segments_date_from: params.from,
    segments_date_to: params.to,
  }, { fallbackMessage: 'Google fetch failed' })
}

