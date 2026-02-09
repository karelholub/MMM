import { useState, useRef } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { connectMeiroCDP, disconnectMeiroCDP, getMeiroCDPStatus } from '../connectors/meiroConnector'

interface DataSourcesProps {
  onJourneysImported: () => void
}

const card = {
  backgroundColor: '#fff',
  borderRadius: 12,
  border: '1px solid #e9ecef',
  padding: 24,
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
  marginBottom: 16,
} as const

const ADMIN_PLATFORMS = [
  { id: 'google', label: 'Google Ads', clientIdLabel: 'Client ID', clientSecretLabel: 'Client Secret', hasAppId: false },
  { id: 'meta', label: 'Meta Ads', clientIdLabel: 'App ID', clientSecretLabel: 'App Secret', hasAppId: true },
  { id: 'linkedin', label: 'LinkedIn Ads', clientIdLabel: 'Client ID', clientSecretLabel: 'Client Secret', hasAppId: false },
] as const

export default function DataSources({ onJourneysImported }: DataSourcesProps) {
  const fileRef = useRef<HTMLInputElement>(null)
  const [meiroUrl, setMeiroUrl] = useState('')
  const [meiroKey, setMeiroKey] = useState('')
  const [adminOpen, setAdminOpen] = useState(false)
  const [adminCreds, setAdminCreds] = useState<Record<string, { client_id?: string; client_secret?: string; app_id?: string; app_secret?: string }>>({
    google: {}, meta: {}, linkedin: {},
  })

  const configStatusQuery = useQuery({
    queryKey: ['admin-datasource-config'],
    queryFn: async () => {
      const res = await fetch('/api/admin/datasource-config')
      if (!res.ok) return { google: { configured: false }, meta: { configured: false }, linkedin: { configured: false } }
      return res.json()
    },
  })

  const authQuery = useQuery({
    queryKey: ['auth-status'],
    queryFn: async () => {
      const res = await fetch('/api/auth/status')
      if (!res.ok) return { connected: [] }
      return res.json()
    },
  })

  const meiroQuery = useQuery({
    queryKey: ['meiro-status'],
    queryFn: getMeiroCDPStatus,
  })

  const meiroProfilesQuery = useQuery({
    queryKey: ['meiro-profiles'],
    queryFn: async () => {
      const res = await fetch('/api/connectors/meiro/profiles')
      if (!res.ok) return { stored_count: 0, webhook_url: '' }
      return res.json()
    },
  })

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/journeys')
      if (!res.ok) return { loaded: false }
      return res.json()
    },
  })

  const uploadMutation = useMutation({
    mutationFn: async (file: File) => {
      const formData = new FormData()
      formData.append('file', file)
      const res = await fetch('/api/attribution/journeys/upload', { method: 'POST', body: formData })
      if (!res.ok) {
        const data = await res.json().catch(() => ({ detail: 'Upload failed' }))
        throw new Error(data.detail || 'Upload failed')
      }
      return res.json()
    },
    onSuccess: () => onJourneysImported(),
  })

  const loadSampleMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/attribution/journeys/load-sample', { method: 'POST' })
      if (!res.ok) throw new Error('Failed')
      return res.json()
    },
    onSuccess: () => onJourneysImported(),
  })

  const importCdpMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/attribution/journeys/from-cdp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      if (!res.ok) {
        const data = await res.json().catch(() => ({ detail: 'Import failed' }))
        throw new Error(data.detail || 'Import failed')
      }
      return res.json()
    },
    onSuccess: () => onJourneysImported(),
  })

  const connectMeiroMutation = useMutation({
    mutationFn: async () => connectMeiroCDP({ api_base_url: meiroUrl, api_key: meiroKey }),
    onSuccess: () => meiroQuery.refetch(),
  })

  const disconnectMeiroMutation = useMutation({
    mutationFn: disconnectMeiroCDP,
    onSuccess: () => meiroQuery.refetch(),
  })

  const saveConfigMutation = useMutation({
    mutationFn: async (payload: { platform: string; client_id?: string; client_secret?: string; app_id?: string; app_secret?: string }) => {
      const res = await fetch('/api/admin/datasource-config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          platform: payload.platform,
          client_id: payload.client_id || undefined,
          client_secret: payload.client_secret || undefined,
          app_id: payload.app_id || undefined,
          app_secret: payload.app_secret || undefined,
        }),
      })
      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        throw new Error(data.detail || 'Failed to save')
      }
      return res.json()
    },
    onSuccess: () => configStatusQuery.refetch(),
  })

  const connected: string[] = authQuery.data?.connected || []
  const meiroConnected = meiroQuery.data?.connected || false
  const journeysLoaded = journeysQuery.data?.loaded || false

  return (
    <div>
      <h2 style={{ margin: '0 0 8px', fontSize: '22px', fontWeight: '700', color: '#212529' }}>Data Sources</h2>
      <p style={{ margin: '0 0 24px', fontSize: '14px', color: '#6c757d' }}>
        Import conversion path data for attribution analysis. Upload JSON, connect to Meiro CDP, or use sample data.
      </p>

      {/* Current Data Status */}
      <div style={{ ...card, backgroundColor: journeysLoaded ? '#d4edda' : '#fff3cd', borderColor: journeysLoaded ? '#c3e6cb' : '#ffc107' }}>
        <h3 style={{ margin: '0 0 8px', fontSize: '16px', fontWeight: '600', color: journeysLoaded ? '#155724' : '#856404' }}>
          {journeysLoaded ? 'Data Loaded' : 'No Data Loaded'}
        </h3>
        {journeysLoaded ? (
          <p style={{ margin: 0, fontSize: '14px', color: '#155724' }}>
            {journeysQuery.data?.count} journeys loaded ({journeysQuery.data?.converted} converted).
            Channels: {journeysQuery.data?.channels?.join(', ')}
          </p>
        ) : (
          <p style={{ margin: 0, fontSize: '14px', color: '#856404' }}>
            Upload conversion path data or load sample data to begin analysis.
          </p>
        )}
      </div>

      {/* Administration – OAuth credentials */}
      <div style={{ ...card, borderColor: '#6c757d' }}>
        <button
          type="button"
          onClick={() => setAdminOpen(!adminOpen)}
          style={{
            width: '100%',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            padding: 0,
            border: 'none',
            background: 'none',
            cursor: 'pointer',
            fontSize: '16px',
            fontWeight: 600,
            color: '#495057',
            textAlign: 'left',
          }}
        >
          <span>Administration – OAuth credentials</span>
          <span style={{ fontSize: '14px', color: '#6c757d' }}>{adminOpen ? '▼' : '▶'}</span>
        </button>
        <p style={{ margin: '4px 0 0', fontSize: '13px', color: '#6c757d' }}>
          Configure Google, Meta, and LinkedIn OAuth so you can use “Connect” below. Stored encrypted on the server.
        </p>
        {adminOpen && (
          <div style={{ marginTop: 20, paddingTop: 20, borderTop: '1px solid #e9ecef' }}>
            {ADMIN_PLATFORMS.map(p => {
              const status = configStatusQuery.data?.[p.id]
              const configured = status?.configured ?? false
              const creds = adminCreds[p.id] || {}
              const idKey = p.hasAppId ? 'app_id' : 'client_id'
              const secretKey = p.hasAppId ? 'app_secret' : 'client_secret'
              const saving = saveConfigMutation.isPending && saveConfigMutation.variables?.platform === p.id
              return (
                <div key={p.id} style={{ marginBottom: 24, padding: 16, backgroundColor: '#f8f9fa', borderRadius: 8 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                    <strong style={{ fontSize: '14px', color: '#212529' }}>{p.label}</strong>
                    <span style={{ fontSize: '12px', fontWeight: 600, color: configured ? '#28a745' : '#dc3545' }}>
                      {configured ? 'Configured' : 'Not configured'}
                    </span>
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: 12, alignItems: 'end' }}>
                    <div>
                      <label style={{ display: 'block', fontSize: '12px', fontWeight: 600, color: '#6c757d', marginBottom: 4 }}>{p.clientIdLabel}</label>
                      <input
                        type="text"
                        placeholder={configured ? '••••••••' : 'Enter ' + p.clientIdLabel}
                        value={creds[idKey as keyof typeof creds] ?? ''}
                        onChange={e => setAdminCreds(prev => ({ ...prev, [p.id]: { ...prev[p.id], [idKey]: e.target.value } }))}
                        style={{ width: '100%', padding: '8px 10px', fontSize: '13px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                      />
                    </div>
                    <div>
                      <label style={{ display: 'block', fontSize: '12px', fontWeight: 600, color: '#6c757d', marginBottom: 4 }}>{p.clientSecretLabel}</label>
                      <input
                        type="password"
                        placeholder={configured ? '••••••••' : 'Enter ' + p.clientSecretLabel}
                        value={creds[secretKey as keyof typeof creds] ?? ''}
                        onChange={e => setAdminCreds(prev => ({ ...prev, [p.id]: { ...prev[p.id], [secretKey]: e.target.value } }))}
                        style={{ width: '100%', padding: '8px 10px', fontSize: '13px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
                      />
                    </div>
                    <button
                      type="button"
                      disabled={saving || (p.hasAppId ? !creds.app_id?.trim() && !creds.app_secret?.trim() : !creds.client_id?.trim() && !creds.client_secret?.trim())}
                      onClick={() => {
                        const payload: { platform: string; client_id?: string; client_secret?: string; app_id?: string; app_secret?: string } = { platform: p.id }
                        if (p.hasAppId) {
                          if (creds.app_id?.trim()) payload.app_id = creds.app_id.trim()
                          if (creds.app_secret?.trim()) payload.app_secret = creds.app_secret.trim()
                        } else {
                          if (creds.client_id?.trim()) payload.client_id = creds.client_id.trim()
                          if (creds.client_secret?.trim()) payload.client_secret = creds.client_secret.trim()
                        }
                        saveConfigMutation.mutate(payload)
                      }}
                      style={{
                        padding: '8px 20px',
                        fontSize: '13px',
                        fontWeight: 600,
                        backgroundColor: saving ? '#ccc' : '#6c757d',
                        color: 'white',
                        border: 'none',
                        borderRadius: 6,
                        cursor: saving ? 'not-allowed' : 'pointer',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {saving ? 'Saving…' : 'Save'}
                    </button>
                  </div>
                  {saveConfigMutation.isError && saveConfigMutation.variables?.platform === p.id && (
                    <p style={{ color: '#dc3545', fontSize: '12px', marginTop: 8 }}>{(saveConfigMutation.error as Error).message}</p>
                  )}
                  {saveConfigMutation.isSuccess && saveConfigMutation.variables?.platform === p.id && (
                    <p style={{ color: '#28a745', fontSize: '12px', marginTop: 8 }}>Saved. You can connect this platform below.</p>
                  )}
                </div>
              )
            })}
            <p style={{ fontSize: '12px', color: '#6c757d', marginTop: 8, marginBottom: 0 }}>
              Environment variables (GOOGLE_CLIENT_ID, META_APP_ID, etc.) override these values if set.
            </p>
          </div>
        )}
      </div>

      {/* Quick Actions */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginBottom: 24 }}>
        <div style={card}>
          <h3 style={{ margin: '0 0 8px', fontSize: '16px', fontWeight: '600', color: '#28a745' }}>Sample Data</h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 16 }}>
            Load 30 sample customer journeys with 6 channels (google_ads, meta_ads, linkedin_ads, email, whatsapp, direct).
          </p>
          <button
            onClick={() => loadSampleMutation.mutate()}
            disabled={loadSampleMutation.isPending}
            style={{ padding: '10px 24px', fontSize: '14px', fontWeight: '600', backgroundColor: '#28a745', color: 'white', border: 'none', borderRadius: 6, cursor: 'pointer', width: '100%' }}
          >
            {loadSampleMutation.isPending ? 'Loading...' : 'Load Sample Data'}
          </button>
        </div>

        <div style={card}>
          <h3 style={{ margin: '0 0 8px', fontSize: '16px', fontWeight: '600', color: '#007bff' }}>Upload JSON</h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 16 }}>
            Upload a JSON file with customer journeys. Expected: array of objects with touchpoints and conversion_value.
          </p>
          <input ref={fileRef} type="file" accept=".json" style={{ display: 'none' }}
            onChange={(e) => { const file = e.target.files?.[0]; if (file) uploadMutation.mutate(file) }} />
          <button
            onClick={() => fileRef.current?.click()}
            disabled={uploadMutation.isPending}
            style={{ padding: '10px 24px', fontSize: '14px', fontWeight: '600', backgroundColor: '#007bff', color: 'white', border: 'none', borderRadius: 6, cursor: 'pointer', width: '100%' }}
          >
            {uploadMutation.isPending ? 'Uploading...' : 'Upload JSON File'}
          </button>
          {uploadMutation.isError && <p style={{ color: '#dc3545', fontSize: '13px', marginTop: 8 }}>{(uploadMutation.error as Error).message}</p>}
        </div>

        <div style={card}>
          <h3 style={{ margin: '0 0 8px', fontSize: '16px', fontWeight: '600', color: '#6f42c1' }}>Import from CDP</h3>
          <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 16 }}>
            Parse CDP profile data into attribution journeys. Connect Meiro CDP first and fetch data.
          </p>
          <button
            onClick={() => importCdpMutation.mutate()}
            disabled={importCdpMutation.isPending}
            style={{ padding: '10px 24px', fontSize: '14px', fontWeight: '600', backgroundColor: '#6f42c1', color: 'white', border: 'none', borderRadius: 6, cursor: 'pointer', width: '100%' }}
          >
            {importCdpMutation.isPending ? 'Importing...' : 'Import from CDP'}
          </button>
          {importCdpMutation.isError && <p style={{ color: '#dc3545', fontSize: '13px', marginTop: 8 }}>{(importCdpMutation.error as Error).message}</p>}
        </div>
      </div>

      {/* Meiro CDP Connection */}
      <div style={{ ...card, borderColor: meiroConnected ? '#28a745' : '#e9ecef' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <div>
            <h3 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#495057' }}>Meiro CDP</h3>
            <p style={{ margin: '4px 0 0', fontSize: '13px', color: '#6c757d' }}>
              Connect to your Meiro CDP instance to fetch customer profile attributes with conversion path data.
            </p>
          </div>
          <span style={{
            padding: '4px 12px', borderRadius: 12, fontSize: '12px', fontWeight: '700',
            backgroundColor: meiroConnected ? '#d4edda' : '#f8f9fa',
            color: meiroConnected ? '#155724' : '#6c757d',
          }}>
            {meiroConnected ? 'Connected' : 'Not Connected'}
          </span>
        </div>

        {!meiroConnected ? (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: 12, alignItems: 'end' }}>
            <div>
              <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>API Base URL</label>
              <input type="text" value={meiroUrl} onChange={(e) => setMeiroUrl(e.target.value)}
                placeholder="https://your-cdp.meiro.io"
                style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }} />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>API Key</label>
              <input type="password" value={meiroKey} onChange={(e) => setMeiroKey(e.target.value)}
                placeholder="Your API key"
                style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }} />
            </div>
            <button
              onClick={() => connectMeiroMutation.mutate()}
              disabled={!meiroUrl || !meiroKey || connectMeiroMutation.isPending}
              style={{
                padding: '10px 24px', fontSize: '14px', fontWeight: '600',
                backgroundColor: (!meiroUrl || !meiroKey) ? '#ccc' : '#007bff',
                color: 'white', border: 'none', borderRadius: 6,
                cursor: (!meiroUrl || !meiroKey) ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap',
              }}
            >
              {connectMeiroMutation.isPending ? 'Connecting...' : 'Connect'}
            </button>
          </div>
        ) : (
          <button onClick={() => disconnectMeiroMutation.mutate()}
            style={{ padding: '8px 20px', fontSize: '13px', fontWeight: '600', backgroundColor: '#dc3545', color: 'white', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
            Disconnect
          </button>
        )}
        {connectMeiroMutation.isError && <p style={{ color: '#dc3545', fontSize: '13px', marginTop: 8 }}>{(connectMeiroMutation.error as Error).message}</p>}

        <div style={{ marginTop: 20, paddingTop: 20, borderTop: '1px solid #e9ecef' }}>
          <h4 style={{ margin: '0 0 8px', fontSize: '14px', fontWeight: '600', color: '#495057' }}>Receive profiles from Meiro CDP</h4>
          <p style={{ margin: 0, fontSize: '13px', color: '#6c757d' }}>
            Meiro CDP can push customer profiles to this app. Configure a webhook in Meiro to POST profiles to the URL below. Then use &quot;Import from CDP&quot; to load them into attribution.
          </p>
          {meiroProfilesQuery.data?.webhook_url && (
            <p style={{ margin: '8px 0 0', fontSize: '12px', fontFamily: 'monospace', backgroundColor: '#f8f9fa', padding: 8, borderRadius: 6, wordBreak: 'break-all' }}>
              {meiroProfilesQuery.data.webhook_url}
            </p>
          )}
          {meiroProfilesQuery.data && (
            <p style={{ margin: '8px 0 0', fontSize: '13px', color: '#495057' }}>
              Stored: <strong>{meiroProfilesQuery.data.stored_count ?? 0}</strong> profiles
            </p>
          )}
        </div>
      </div>

      {/* Ad Platform Connections */}
      <div style={card}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Ad Platform Connections</h3>
        <p style={{ fontSize: '13px', color: '#6c757d', marginBottom: 16 }}>
          OAuth connections for pulling expense data. Expenses auto-import when you fetch data.
        </p>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {[
            { platform: 'meta', label: 'Meta Ads', color: '#1877F2' },
            { platform: 'google', label: 'Google Ads', color: '#4285F4' },
            { platform: 'linkedin', label: 'LinkedIn Ads', color: '#0A66C2' },
          ].map(p => {
            const isConnected = connected.includes(p.platform)
            return (
              <div key={p.platform} style={{
                display: 'flex', alignItems: 'center', gap: 8, padding: '10px 16px', borderRadius: 8,
                backgroundColor: isConnected ? `${p.color}10` : '#f8f9fa',
                border: `1px solid ${isConnected ? p.color : '#dee2e6'}`,
              }}>
                <span style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: isConnected ? '#28a745' : '#dee2e6' }} />
                <span style={{ fontSize: '14px', fontWeight: '600', color: '#495057' }}>{p.label}</span>
                {isConnected ? (
                  <span style={{ fontSize: '12px', color: '#28a745', fontWeight: '600' }}>Connected</span>
                ) : (
                  <a href={`/api/auth/${p.platform}`}
                    style={{ fontSize: '12px', fontWeight: '600', color: p.color, textDecoration: 'none', padding: '2px 8px', border: `1px solid ${p.color}`, borderRadius: 4 }}>
                    Connect
                  </a>
                )}
              </div>
            )
          })}
        </div>
      </div>

      {/* Data Format Docs */}
      <div style={{ ...card, backgroundColor: '#f0f7ff', borderColor: '#b8daff' }}>
        <h3 style={{ margin: '0 0 12px', fontSize: '16px', fontWeight: '600', color: '#004085' }}>Expected JSON Format</h3>
        <pre style={{ backgroundColor: 'white', padding: 16, borderRadius: 8, fontSize: '12px', overflow: 'auto', border: '1px solid #e9ecef', color: '#495057' }}>
{`[
  {
    "customer_id": "c001",
    "touchpoints": [
      {"channel": "google_ads", "timestamp": "2024-01-02"},
      {"channel": "email", "timestamp": "2024-01-05"},
      {"channel": "direct", "timestamp": "2024-01-07"}
    ],
    "conversion_value": 120.00,
    "converted": true
  },
  ...
]`}
        </pre>
        <p style={{ fontSize: '13px', color: '#004085', marginTop: 12, marginBottom: 0 }}>
          CDP profile attributes with conversion paths will be automatically parsed using configurable field mapping.
        </p>
      </div>
    </div>
  )
}
