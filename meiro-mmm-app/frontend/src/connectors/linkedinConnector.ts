export async function fetchLinkedInAds(params: { since: string; until: string }) {
  const res = await fetch('/api/connectors/linkedin', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params)
  })
  if (!res.ok) {
    const errorData = await res.json().catch(() => ({ detail: 'LinkedIn fetch failed' }))
    throw new Error(errorData.detail || 'LinkedIn fetch failed')
  }
  return res.json()
}


