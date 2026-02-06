export async function mergeAdsData() {
  const res = await fetch('/api/connectors/merge', { method: 'POST' })
  if (!res.ok) throw new Error('Merge failed')
  return res.json()
}

export async function getConnectorStatus() {
  const res = await fetch('/api/connectors/status')
  if (!res.ok) throw new Error('Status fetch failed')
  return res.json()
}


