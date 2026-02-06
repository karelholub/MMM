// Meiro CDP connector – frontend API wrapper

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
