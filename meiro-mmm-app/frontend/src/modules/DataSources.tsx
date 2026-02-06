import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchMetaAds } from '../connectors/metaConnector'
import { fetchGoogleAds } from '../connectors/googleConnector'
import { fetchLinkedInAds } from '../connectors/linkedinConnector'
import { mergeAdsData, getConnectorStatus } from '../connectors/mergeAdsData'
import { connectMeiroCDP, disconnectMeiroCDP, fetchMeiroCDPData } from '../connectors/meiroConnector'
import ConnectAccounts from '../components/ConnectAccounts'

interface AuthStatus {
  connected: string[]
}

async function getAuthStatus(): Promise<AuthStatus> {
  const res = await fetch('/api/auth/status')
  if (!res.ok) throw new Error('Failed to fetch auth status')
  return res.json()
}

export default function DataSources() {
  const [status, setStatus] = useState<any>({})
  const [metaAccount, setMetaAccount] = useState('')
  const [avgAov, setAvgAov] = useState(20)
  const [since, setSince] = useState('2024-01-01')
  const [until, setUntil] = useState('2024-01-31')
  const [gFrom, setGFrom] = useState('2024-01-01')
  const [gTo, setGTo] = useState('2024-01-31')
  const [busy, setBusy] = useState(false)
  const [showConnect, setShowConnect] = useState(false)
  // Meiro CDP state
  const [meiroUrl, setMeiroUrl] = useState('')
  const [meiroKey, setMeiroKey] = useState('')
  const [meiroSince, setMeiroSince] = useState('2024-01-01')
  const [meiroUntil, setMeiroUntil] = useState('2024-12-31')
  const [meiroMsg, setMeiroMsg] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  const { data: authStatus } = useQuery<AuthStatus>({
    queryKey: ['authStatus'],
    queryFn: getAuthStatus,
    refetchInterval: 3000,
  })

  const refresh = async () => {
    const s = await getConnectorStatus()
    setStatus(s)
  }

  useEffect(() => { refresh() }, [])

  const run = async (fn: () => Promise<any>) => {
    setBusy(true)
    try { await fn(); await refresh() } finally { setBusy(false) }
  }

  const isConnected = (platform: string) => authStatus?.connected?.includes(platform) || false

  if (showConnect) {
    return (
      <div>
        <button
          onClick={() => setShowConnect(false)}
          style={{
            marginBottom: 20,
            padding: '8px 16px',
            fontSize: '14px',
            backgroundColor: '#6c757d',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer'
          }}
        >
          ← Back to Data Sources
        </button>
        <ConnectAccounts />
      </div>
    )
  }

  return (
    <div style={{ maxWidth: 1000, margin: '0 auto', padding: 24 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h1 style={{ marginTop: 0, fontSize: '28px', fontWeight: '700', color: '#212529', marginBottom: 8 }}>Data Sources</h1>
          <p style={{ color: '#6c757d', margin: 0 }}>Connect ad platforms, fetch campaign data, and build a unified dataset for MMM.</p>
        </div>
        <button
          onClick={() => setShowConnect(true)}
          style={{
            padding: '10px 20px',
            fontSize: '14px',
            fontWeight: '600',
            backgroundColor: '#007bff',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer',
            boxShadow: '0 2px 4px rgba(0,123,255,0.3)'
          }}
        >
          Manage Connections
        </button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 16 }}>
        <div style={{ background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <h3 style={{ margin: 0, fontSize: '18px', fontWeight: '600' }}>Meta Ads</h3>
            {isConnected('meta') ? (
              <span style={{ padding: '4px 8px', backgroundColor: '#28a745', color: 'white', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Connected
              </span>
            ) : (
              <span style={{ padding: '4px 8px', backgroundColor: '#ffc107', color: '#333', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Not Connected
              </span>
            )}
          </div>
          {isConnected('meta') ? (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              <input 
                placeholder="Ad Account ID (act_...)" 
                value={metaAccount} 
                onChange={e=>setMetaAccount(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
              <input 
                type="date" 
                value={since} 
                onChange={e=>setSince(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
              <input 
                type="date" 
                value={until} 
                onChange={e=>setUntil(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
              <input 
                type="number" 
                step={0.01} 
                value={avgAov} 
                onChange={e=>setAvgAov(parseFloat(e.target.value))} 
                placeholder="Avg AOV"
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
            </div>
          ) : (
            <p style={{ color: '#6c757d', fontSize: '14px', marginBottom: 12 }}>
              Connect your Meta account to fetch data automatically. No manual token entry needed.
            </p>
          )}
          <button 
            disabled={busy || !isConnected('meta') || !metaAccount} 
            onClick={() => run(() => fetchMetaAds({ ad_account_id: metaAccount, since, until, avg_aov: avgAov }))} 
            style={{ 
              marginTop: 8,
              padding: '10px 20px',
              fontSize: '14px',
              fontWeight: '600',
              backgroundColor: (!busy && isConnected('meta') && metaAccount) ? '#28a745' : '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: (!busy && isConnected('meta') && metaAccount) ? 'pointer' : 'not-allowed'
            }}
          >
            Fetch Meta Ads
          </button>
        </div>

        <div style={{ background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <h3 style={{ margin: 0, fontSize: '18px', fontWeight: '600' }}>Google Ads</h3>
            {isConnected('google') ? (
              <span style={{ padding: '4px 8px', backgroundColor: '#28a745', color: 'white', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Connected
              </span>
            ) : (
              <span style={{ padding: '4px 8px', backgroundColor: '#ffc107', color: '#333', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Not Connected
              </span>
            )}
          </div>
          {isConnected('google') ? (
            <div style={{ display: 'flex', gap: 8 }}>
              <input 
                type="date" 
                value={gFrom} 
                onChange={e=>setGFrom(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
              <input 
                type="date" 
                value={gTo} 
                onChange={e=>setGTo(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
            </div>
          ) : (
            <p style={{ color: '#6c757d', fontSize: '14px', marginBottom: 12 }}>
              Connect your Google Ads account to fetch data automatically.
            </p>
          )}
          <button 
            disabled={busy || !isConnected('google')} 
            onClick={() => run(() => fetchGoogleAds({ from: gFrom, to: gTo }))} 
            style={{ 
              marginTop: 8,
              padding: '10px 20px',
              fontSize: '14px',
              fontWeight: '600',
              backgroundColor: (!busy && isConnected('google')) ? '#28a745' : '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: (!busy && isConnected('google')) ? 'pointer' : 'not-allowed'
            }}
          >
            Fetch Google Ads
          </button>
        </div>

        <div style={{ background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <h3 style={{ margin: 0, fontSize: '18px', fontWeight: '600' }}>LinkedIn Ads</h3>
            {isConnected('linkedin') ? (
              <span style={{ padding: '4px 8px', backgroundColor: '#28a745', color: 'white', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Connected
              </span>
            ) : (
              <span style={{ padding: '4px 8px', backgroundColor: '#ffc107', color: '#333', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Not Connected
              </span>
            )}
          </div>
          {isConnected('linkedin') ? (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              <input 
                type="date" 
                value={since} 
                onChange={e=>setSince(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
              <input 
                type="date" 
                value={until} 
                onChange={e=>setUntil(e.target.value)}
                style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
              />
            </div>
          ) : (
            <p style={{ color: '#6c757d', fontSize: '14px', marginBottom: 12 }}>
              Connect your LinkedIn account to fetch data automatically. No manual token entry needed.
            </p>
          )}
          <button 
            disabled={busy || !isConnected('linkedin')} 
            onClick={() => run(() => fetchLinkedInAds({ since, until }))} 
            style={{ 
              marginTop: 8,
              padding: '10px 20px',
              fontSize: '14px',
              fontWeight: '600',
              backgroundColor: (!busy && isConnected('linkedin')) ? '#28a745' : '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: (!busy && isConnected('linkedin')) ? 'pointer' : 'not-allowed'
            }}
          >
            Fetch LinkedIn Ads
          </button>
        </div>

        {/* Meiro CDP */}
        <div style={{ background: '#fff', border: isConnected('meiro_cdp') ? '2px solid #6f42c1' : '1px solid #e9ecef', borderRadius: 8, padding: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <h3 style={{ margin: 0, fontSize: '18px', fontWeight: '600' }}>Meiro CDP</h3>
            {isConnected('meiro_cdp') ? (
              <span style={{ padding: '4px 8px', backgroundColor: '#6f42c1', color: 'white', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Connected
              </span>
            ) : (
              <span style={{ padding: '4px 8px', backgroundColor: '#ffc107', color: '#333', borderRadius: '4px', fontSize: '12px', fontWeight: '600' }}>
                Not Connected
              </span>
            )}
          </div>

          {meiroMsg && (
            <div style={{
              padding: '8px 12px',
              marginBottom: 12,
              borderRadius: 4,
              backgroundColor: meiroMsg.type === 'success' ? '#d4edda' : '#f8d7da',
              color: meiroMsg.type === 'success' ? '#155724' : '#721c24',
              fontSize: '13px',
            }}>
              {meiroMsg.text}
            </div>
          )}

          {!isConnected('meiro_cdp') ? (
            <div>
              <p style={{ color: '#6c757d', fontSize: '14px', marginBottom: 12 }}>
                Connect your Meiro CDP instance to import customer attribution and event data.
              </p>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 8, marginBottom: 12 }}>
                <input
                  placeholder="API Base URL (e.g. https://your-instance.meiro.io)"
                  value={meiroUrl}
                  onChange={e => setMeiroUrl(e.target.value)}
                  style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
                />
                <input
                  placeholder="API Key"
                  type="password"
                  value={meiroKey}
                  onChange={e => setMeiroKey(e.target.value)}
                  style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
                />
              </div>
              <button
                disabled={busy || !meiroUrl || !meiroKey}
                onClick={async () => {
                  setBusy(true)
                  setMeiroMsg(null)
                  try {
                    await connectMeiroCDP({ api_base_url: meiroUrl, api_key: meiroKey })
                    setMeiroMsg({ type: 'success', text: 'Connected to Meiro CDP!' })
                    await refresh()
                  } catch (e: any) {
                    setMeiroMsg({ type: 'error', text: e.message || 'Connection failed' })
                  } finally {
                    setBusy(false)
                  }
                }}
                style={{
                  padding: '10px 20px',
                  fontSize: '14px',
                  fontWeight: '600',
                  backgroundColor: (!busy && meiroUrl && meiroKey) ? '#6f42c1' : '#6c757d',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  cursor: (!busy && meiroUrl && meiroKey) ? 'pointer' : 'not-allowed',
                }}
              >
                Connect Meiro CDP
              </button>
            </div>
          ) : (
            <div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 12 }}>
                <input
                  type="date"
                  value={meiroSince}
                  onChange={e => setMeiroSince(e.target.value)}
                  style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
                />
                <input
                  type="date"
                  value={meiroUntil}
                  onChange={e => setMeiroUntil(e.target.value)}
                  style={{ padding: '8px', border: '1px solid #ced4da', borderRadius: '4px' }}
                />
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button
                  disabled={busy}
                  onClick={async () => {
                    setBusy(true)
                    setMeiroMsg(null)
                    try {
                      const result = await fetchMeiroCDPData({ since: meiroSince, until: meiroUntil })
                      setMeiroMsg({ type: 'success', text: `Fetched ${result.rows} rows from Meiro CDP` })
                      await refresh()
                    } catch (e: any) {
                      setMeiroMsg({ type: 'error', text: e.message || 'Fetch failed' })
                    } finally {
                      setBusy(false)
                    }
                  }}
                  style={{
                    padding: '10px 20px',
                    fontSize: '14px',
                    fontWeight: '600',
                    backgroundColor: !busy ? '#6f42c1' : '#6c757d',
                    color: 'white',
                    border: 'none',
                    borderRadius: '6px',
                    cursor: !busy ? 'pointer' : 'not-allowed',
                  }}
                >
                  Fetch CDP Data
                </button>
                <button
                  disabled={busy}
                  onClick={async () => {
                    setBusy(true)
                    try {
                      await disconnectMeiroCDP()
                      setMeiroMsg({ type: 'success', text: 'Disconnected from Meiro CDP' })
                      await refresh()
                    } catch (e: any) {
                      setMeiroMsg({ type: 'error', text: e.message || 'Disconnect failed' })
                    } finally {
                      setBusy(false)
                    }
                  }}
                  style={{
                    padding: '10px 20px',
                    fontSize: '14px',
                    fontWeight: '600',
                    backgroundColor: '#dc3545',
                    color: 'white',
                    border: 'none',
                    borderRadius: '6px',
                    cursor: !busy ? 'pointer' : 'not-allowed',
                  }}
                >
                  Disconnect
                </button>
              </div>
            </div>
          )}
        </div>

        <div style={{ background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 20 }}>
          <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600' }}>Merge & Prepare</h3>
          <p style={{ color: '#6c757d', fontSize: '14px', marginBottom: 12 }}>
            Combine data from all connected platforms into a unified dataset.
          </p>
          <button 
            disabled={busy} 
            onClick={() => run(mergeAdsData)}
            style={{
              padding: '10px 20px',
              fontSize: '14px',
              fontWeight: '600',
              backgroundColor: !busy ? '#007bff' : '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: !busy ? 'pointer' : 'not-allowed'
            }}
          >
            Merge All & Build unified_ads.csv
          </button>
        </div>
      </div>

      <div style={{ marginTop: 24, background: '#fff', border: '1px solid #e9ecef', borderRadius: 8, padding: 20 }}>
        <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600' }}>Connector Status</h3>
        <pre style={{ background: '#f8f9fa', padding: 12, borderRadius: 6, overflow: 'auto', fontSize: '12px' }}>
          {JSON.stringify(status, null, 2)}
        </pre>
      </div>
    </div>
  )
}


