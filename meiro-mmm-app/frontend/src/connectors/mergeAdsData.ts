import { apiGetJson, apiSendJson } from '../lib/apiClient'

export async function mergeAdsData() {
  return apiSendJson<any>('/api/connectors/merge', 'POST', undefined, {
    fallbackMessage: 'Merge failed',
  })
}

export async function getConnectorStatus() {
  return apiGetJson<any>('/api/connectors/status', { fallbackMessage: 'Status fetch failed' })
}

