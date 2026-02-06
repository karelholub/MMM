import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'

interface AuthStatus {
  connected: string[]
}

async function getAuthStatus(): Promise<AuthStatus> {
  const res = await fetch('/api/auth/status')
  if (!res.ok) throw new Error('Failed to fetch auth status')
  return res.json()
}

async function disconnectPlatform(platform: string): Promise<void> {
  const res = await fetch(`/api/auth/${platform}`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`Failed to disconnect ${platform}`)
}

export default function ConnectAccounts() {
  const queryClient = useQueryClient()
  const [message, setMessage] = useState<{ type: 'success' | 'error', text: string } | null>(null)

  // Check URL params for OAuth callback messages
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const success = params.get('success')
    const error = params.get('error')
    const errorMsg = params.get('message')
    
    if (success) {
      setMessage({ type: 'success', text: `Successfully connected ${success}!` })
      // Clean URL
      window.history.replaceState({}, '', window.location.pathname)
    } else if (error) {
      setMessage({ 
        type: 'error', 
        text: `Failed to connect ${error}${errorMsg ? `: ${errorMsg}` : ''}` 
      })
      window.history.replaceState({}, '', window.location.pathname)
    }
  }, [])

  const { data: status, isLoading } = useQuery<AuthStatus>({
    queryKey: ['authStatus'],
    queryFn: getAuthStatus,
    refetchInterval: 2000, // Poll every 2 seconds to catch new connections
  })

  const disconnectMutation = useMutation({
    mutationFn: disconnectPlatform,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['authStatus'] })
      setMessage({ type: 'success', text: 'Platform disconnected successfully' })
    },
    onError: (error: Error) => {
      setMessage({ type: 'error', text: error.message })
    },
  })

  const handleConnect = (platform: string) => {
    window.location.href = `/api/auth/${platform}`
  }

  const handleDisconnect = (platform: string) => {
    if (window.confirm(`Are you sure you want to disconnect ${platform}?`)) {
      disconnectMutation.mutate(platform)
    }
  }

  const isConnected = (platform: string) => {
    return status?.connected?.includes(platform) || false
  }

  const platforms = [
    { 
      id: 'meta', 
      name: 'Meta Ads', 
      icon: '📘',
      description: 'Connect your Facebook/Meta Ads account to fetch campaign data automatically.' 
    },
    { 
      id: 'google', 
      name: 'Google Ads', 
      icon: '🔵',
      description: 'Connect your Google Ads account to fetch campaign data automatically.' 
    },
    { 
      id: 'linkedin', 
      name: 'LinkedIn Ads', 
      icon: '💼',
      description: 'Connect your LinkedIn Ads account to fetch campaign data automatically.' 
    },
  ]

  return (
    <div style={{ maxWidth: 900, margin: '0 auto', padding: 24 }}>
      <h1 style={{ fontSize: '28px', fontWeight: '700', color: '#212529', marginTop: 0, marginBottom: 8 }}>
        Connect Your Accounts
      </h1>
      <p style={{ fontSize: '15px', color: '#6c757d', marginBottom: 32 }}>
        Authorize access to your ad platform accounts to automatically fetch campaign data for MMM modeling.
      </p>

      {message && (
        <div style={{
          padding: 16,
          marginBottom: 24,
          borderRadius: 8,
          backgroundColor: message.type === 'success' ? '#d4edda' : '#f8d7da',
          color: message.type === 'success' ? '#155724' : '#721c24',
          border: `1px solid ${message.type === 'success' ? '#c3e6cb' : '#f5c6cb'}`,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <span>{message.text}</span>
          <button 
            onClick={() => setMessage(null)}
            style={{
              background: 'transparent',
              border: 'none',
              color: 'inherit',
              cursor: 'pointer',
              fontSize: '18px',
              padding: '0 8px'
            }}
          >
            ×
          </button>
        </div>
      )}

      {isLoading && (
        <div style={{ textAlign: 'center', padding: 40 }}>
          <p style={{ color: '#6c757d' }}>Loading connection status...</p>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 20 }}>
        {platforms.map((platform) => {
          const connected = isConnected(platform.id)
          
          return (
            <div 
              key={platform.id}
              style={{
                backgroundColor: '#fff',
                border: `2px solid ${connected ? '#28a745' : '#e9ecef'}`,
                borderRadius: '12px',
                padding: 24,
                boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
                transition: 'all 0.2s'
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  <span style={{ fontSize: '32px' }}>{platform.icon}</span>
                  <div>
                    <h3 style={{ margin: 0, fontSize: '20px', fontWeight: '600', color: '#212529' }}>
                      {platform.name}
                    </h3>
                    <p style={{ margin: '4px 0 0', fontSize: '14px', color: '#6c757d' }}>
                      {platform.description}
                    </p>
                  </div>
                </div>
                {connected && (
                  <span style={{
                    padding: '6px 12px',
                    backgroundColor: '#28a745',
                    color: 'white',
                    borderRadius: '6px',
                    fontSize: '12px',
                    fontWeight: '600',
                    textTransform: 'uppercase'
                  }}>
                    Connected
                  </span>
                )}
              </div>
              
              <div style={{ display: 'flex', gap: 8, marginTop: 16 }}>
                {connected ? (
                  <button
                    onClick={() => handleDisconnect(platform.id)}
                    disabled={disconnectMutation.isPending}
                    style={{
                      padding: '10px 20px',
                      fontSize: '14px',
                      fontWeight: '600',
                      backgroundColor: '#dc3545',
                      color: 'white',
                      border: 'none',
                      borderRadius: '6px',
                      cursor: disconnectMutation.isPending ? 'not-allowed' : 'pointer',
                      opacity: disconnectMutation.isPending ? 0.6 : 1,
                      boxShadow: '0 2px 4px rgba(220,53,69,0.3)',
                      transition: 'all 0.2s'
                    }}
                    onMouseOver={(e) => {
                      if (!disconnectMutation.isPending) {
                        e.currentTarget.style.backgroundColor = '#c82333'
                      }
                    }}
                    onMouseOut={(e) => {
                      if (!disconnectMutation.isPending) {
                        e.currentTarget.style.backgroundColor = '#dc3545'
                      }
                    }}
                  >
                    {disconnectMutation.isPending ? 'Disconnecting...' : 'Disconnect'}
                  </button>
                ) : (
                  <button
                    onClick={() => handleConnect(platform.id)}
                    style={{
                      padding: '10px 20px',
                      fontSize: '14px',
                      fontWeight: '600',
                      backgroundColor: '#007bff',
                      color: 'white',
                      border: 'none',
                      borderRadius: '6px',
                      cursor: 'pointer',
                      boxShadow: '0 2px 4px rgba(0,123,255,0.3)',
                      transition: 'all 0.2s'
                    }}
                    onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#0056b3'}
                    onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#007bff'}
                  >
                    Connect {platform.name}
                  </button>
                )}
              </div>
            </div>
          )
        })}
      </div>

      <div style={{
        marginTop: 32,
        padding: 20,
        backgroundColor: '#f8f9fa',
        borderRadius: '8px',
        border: '1px solid #e9ecef'
      }}>
        <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600', color: '#495057' }}>
          How it works
        </h3>
        <ul style={{ margin: 0, paddingLeft: 20, color: '#6c757d', fontSize: '14px' }}>
          <li>Click "Connect" to authorize access via OAuth 2.0</li>
          <li>You'll be redirected to the platform's login page</li>
          <li>After authorization, you'll be redirected back</li>
          <li>Tokens are encrypted and stored securely</li>
          <li>Use the Data Sources page to fetch campaign data with connected accounts</li>
        </ul>
      </div>
    </div>
  )
}

