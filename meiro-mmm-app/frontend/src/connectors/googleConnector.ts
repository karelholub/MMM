export async function fetchGoogleAds(params: { from: string; to: string }) {
  const res = await fetch('/api/connectors/google', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ segments_date_from: params.from, segments_date_to: params.to })
  })
  if (!res.ok) throw new Error('Google fetch failed')
  return res.json()
}


