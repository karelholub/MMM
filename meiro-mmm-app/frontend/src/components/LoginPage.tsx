import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import { apiSendJson } from '../lib/apiClient'

interface LoginResponse {
  csrf_token: string
  role?: string | null
}

export default function LoginPage() {
  const queryClient = useQueryClient()
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('admin')
  const [workspaceId, setWorkspaceId] = useState('default')
  const [error, setError] = useState<string | null>(null)

  const loginMutation = useMutation({
    mutationFn: async () => {
      return apiSendJson<LoginResponse>(
        '/api/auth/login',
        'POST',
        {
          provider: 'local_password',
          username: username.trim(),
          password,
          workspace_id: workspaceId.trim() || 'default',
        },
        { auth: false, fallbackMessage: 'Login failed' },
      )
    },
    onSuccess: async (body) => {
      setError(null)
      try {
        window.localStorage.setItem('mmm-csrf-token', body.csrf_token || '')
      } catch {
        // ignore storage errors
      }
      await queryClient.invalidateQueries({ queryKey: ['auth-me'] })
      window.location.reload()
    },
    onError: (e) => setError((e as Error).message || 'Login failed'),
  })

  return (
    <div
      style={{
        minHeight: '100vh',
        background: `linear-gradient(140deg, ${tokens.color.bg} 0%, ${tokens.color.surface} 100%)`,
        display: 'grid',
        placeItems: 'center',
        padding: 16,
      }}
    >
      <div
        style={{
          width: 'min(420px, 100%)',
          background: tokens.color.surface,
          border: `1px solid ${tokens.color.borderLight}`,
          borderRadius: tokens.radius.lg,
          boxShadow: tokens.shadow,
          padding: 24,
          display: 'grid',
          gap: 12,
        }}
      >
        <h1 style={{ margin: 0, fontSize: tokens.font.sizeXl, color: tokens.color.text }}>Sign in</h1>
        <p style={{ margin: 0, color: tokens.color.textSecondary, fontSize: tokens.font.sizeSm }}>
          Local credentials (extendable to Google / SSO providers).
        </p>

        <label style={{ display: 'grid', gap: 6, fontSize: tokens.font.sizeSm }}>
          Username or email
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="username"
            style={{ padding: '9px 10px', borderRadius: tokens.radius.sm, border: `1px solid ${tokens.color.border}` }}
          />
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: tokens.font.sizeSm }}>
          Password
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            style={{ padding: '9px 10px', borderRadius: tokens.radius.sm, border: `1px solid ${tokens.color.border}` }}
          />
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: tokens.font.sizeSm }}>
          Workspace
          <input
            value={workspaceId}
            onChange={(e) => setWorkspaceId(e.target.value)}
            style={{ padding: '9px 10px', borderRadius: tokens.radius.sm, border: `1px solid ${tokens.color.border}` }}
          />
        </label>

        {error && <div style={{ fontSize: tokens.font.sizeSm, color: tokens.color.danger }}>{error}</div>}

        <button
          type="button"
          onClick={() => loginMutation.mutate()}
          disabled={loginMutation.isPending || !username.trim() || !password}
          style={{
            marginTop: 6,
            border: `1px solid ${tokens.color.accent}`,
            background: tokens.color.accent,
            color: '#fff',
            borderRadius: tokens.radius.sm,
            padding: '10px 12px',
            cursor: loginMutation.isPending ? 'wait' : 'pointer',
            opacity: loginMutation.isPending ? 0.8 : 1,
            fontWeight: tokens.font.weightSemibold,
          }}
        >
          {loginMutation.isPending ? 'Signing in…' : 'Sign in'}
        </button>
      </div>
    </div>
  )
}
