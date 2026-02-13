import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

export interface DataSourceRecord {
  id: string
  workspace_id: string
  category: 'warehouse' | 'ad_platform' | 'cdp'
  type: string
  name: string
  status: 'connected' | 'error' | 'disabled'
  config_json: Record<string, unknown>
  has_secret: boolean
  secret_ref?: string | null
  last_tested_at?: string | null
  last_error?: string | null
  created_at?: string | null
  updated_at?: string | null
}

export interface DataSourceTestResponse {
  ok: boolean
  message: string
  latency_ms: number
  details: {
    sample_namespaces?: string[]
    sample_schemas?: string[]
    sample_tables?: string[]
  }
}

export async function listDataSources(category?: string): Promise<{ items: DataSourceRecord[]; total: number }> {
  return apiGetJson<{ items: DataSourceRecord[]; total: number }>(
    withQuery('/api/data-sources', { category }),
    { fallbackMessage: 'Failed to load data sources' },
  )
}

export async function testDataSource(payload: {
  type: string
  config_json: Record<string, unknown>
  secrets: Record<string, unknown>
}): Promise<DataSourceTestResponse> {
  return apiSendJson<DataSourceTestResponse>('/api/data-sources/test', 'POST', payload, {
    fallbackMessage: 'Connection test failed',
  })
}

export async function createDataSource(payload: {
  workspace_id?: string
  category: string
  type: string
  name: string
  config_json: Record<string, unknown>
  secrets: Record<string, unknown>
}): Promise<DataSourceRecord> {
  return apiSendJson<DataSourceRecord>('/api/data-sources', 'POST', payload, {
    fallbackMessage: 'Failed to create data source',
  })
}

export async function testSavedDataSource(id: string): Promise<DataSourceTestResponse> {
  return apiSendJson<DataSourceTestResponse>(`/api/data-sources/${id}/test`, 'POST', undefined, {
    fallbackMessage: 'Failed to test data source',
  })
}

export async function disableDataSource(id: string): Promise<void> {
  await apiSendJson<{ ok: boolean }>(`/api/data-sources/${id}/disable`, 'POST', undefined, {
    fallbackMessage: 'Failed to disable data source',
  })
}

export async function deleteDataSource(id: string): Promise<void> {
  await apiSendJson<{ ok: boolean }>(`/api/data-sources/${id}`, 'DELETE', undefined, {
    fallbackMessage: 'Failed to delete data source',
  })
}

export async function rotateDataSourceCredentials(id: string, secrets: Record<string, unknown>): Promise<DataSourceRecord> {
  return apiSendJson<DataSourceRecord>(`/api/data-sources/${id}/rotate-credentials`, 'POST', { secrets }, {
    fallbackMessage: 'Failed to rotate credentials',
  })
}
