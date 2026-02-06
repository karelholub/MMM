export async function fetchMetaAds(params: { ad_account_id: string; since: string; until: string; avg_aov: number }) {
  const res = await fetch('/api/connectors/meta', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params)
  })
  if (!res.ok) {
    const errorData = await res.json().catch(() => ({ detail: 'Meta fetch failed' }))
    throw new Error(errorData.detail || 'Meta fetch failed')
  }
  return res.json()
}


