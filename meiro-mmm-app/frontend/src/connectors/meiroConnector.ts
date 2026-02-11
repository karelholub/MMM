// Meiro CDP connector – frontend API wrapper

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
  channel_mapping?: Record<string, string>
}

export async function connectMeiroCDP(params: { api_base_url: string; api_key: string }) {
  const res = await fetch('/api/connectors/meiro/connect', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Connection failed' }))
    throw new Error(data.detail || 'Connection failed')
  }
  return res.json()
}

export async function saveMeiroCDP(params: { api_base_url: string; api_key: string }) {
  const res = await fetch('/api/connectors/meiro/save', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Save failed' }))
    throw new Error(data.detail || 'Save failed')
  }
  return res.json()
}

export async function testMeiroConnection(params?: {
  api_base_url?: string
  api_key?: string
  save_on_success?: boolean
}) {
  const res = await fetch('/api/connectors/meiro/test', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params || {}),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Connection test failed' }))
    throw new Error(data.detail || 'Connection test failed')
  }
  return res.json()
}

export async function disconnectMeiroCDP() {
  const res = await fetch('/api/connectors/meiro', { method: 'DELETE' })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Disconnect failed' }))
    throw new Error(data.detail || 'Disconnect failed')
  }
  return res.json()
}

export async function getMeiroCDPStatus(): Promise<{ connected: boolean }> {
  const res = await fetch('/api/connectors/meiro/status')
  if (!res.ok) throw new Error('Failed to fetch Meiro CDP status')
  return res.json()
}

export async function getMeiroConfig(): Promise<MeiroConfig> {
  const res = await fetch('/api/connectors/meiro/config')
  if (!res.ok) throw new Error('Failed to fetch Meiro config')
  return res.json()
}

export async function getMeiroAttributes() {
  const res = await fetch('/api/connectors/meiro/attributes')
  if (!res.ok) throw new Error('Failed to fetch attributes')
  return res.json()
}

export async function getMeiroEvents() {
  const res = await fetch('/api/connectors/meiro/events')
  if (!res.ok) throw new Error('Failed to fetch events')
  return res.json()
}

export async function getMeiroSegments() {
  const res = await fetch('/api/connectors/meiro/segments')
  if (!res.ok) throw new Error('Failed to fetch segments')
  return res.json()
}

export async function fetchMeiroCDPData(params: {
  since: string
  until: string
  event_types?: string[]
  attributes?: string[]
  segment_id?: string
}) {
  const res = await fetch('/api/connectors/meiro/fetch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Fetch failed' }))
    throw new Error(data.detail || 'Fetch failed')
  }
  return res.json()
}

export async function getMeiroMapping(): Promise<{ mapping: MeiroMapping; presets: Record<string, unknown> }> {
  const res = await fetch('/api/connectors/meiro/mapping')
  if (!res.ok) throw new Error('Failed to fetch mapping')
  return res.json()
}

export async function saveMeiroMapping(mapping: MeiroMapping) {
  const res = await fetch('/api/connectors/meiro/mapping', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(mapping),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Save failed' }))
    throw new Error(data.detail || 'Save failed')
  }
  return res.json()
}

export async function getMeiroPullConfig(): Promise<Record<string, unknown>> {
  const res = await fetch('/api/connectors/meiro/pull-config')
  if (!res.ok) throw new Error('Failed to fetch pull config')
  return res.json()
}

export async function saveMeiroPullConfig(config: Record<string, unknown>) {
  const res = await fetch('/api/connectors/meiro/pull-config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Save failed' }))
    throw new Error(data.detail || 'Save failed')
  }
  return res.json()
}

export async function meiroPull(since?: string, until?: string) {
  const params = new URLSearchParams()
  if (since) params.set('since', since)
  if (until) params.set('until', until)
  const qs = params.toString()
  const res = await fetch(`/api/connectors/meiro/pull${qs ? `?${qs}` : ''}`, { method: 'POST' })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Pull failed' }))
    throw new Error(data.detail || 'Pull failed')
  }
  return res.json()
}

export async function meiroRotateWebhookSecret(): Promise<{ secret: string }> {
  const res = await fetch('/api/connectors/meiro/webhook/rotate-secret', { method: 'POST' })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Rotate failed' }))
    throw new Error(data.detail || 'Rotate failed')
  }
  return res.json()
}

export async function meiroDryRun(limit = 100): Promise<{
  count: number
  preview: Array<{ id: string; touchpoints: number; value: number }>
  warnings: string[]
  validation: { ok?: boolean; error?: string }
}> {
  const res = await fetch(`/api/connectors/meiro/dry-run?limit=${limit}`, { method: 'POST' })
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: 'Dry run failed' }))
    throw new Error(data.detail || 'Dry run failed')
  }
  return res.json()
}
