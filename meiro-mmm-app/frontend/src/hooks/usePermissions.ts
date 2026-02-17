import { useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiGetJson } from '../lib/apiClient'

interface AuthWorkspace {
  id?: string
  slug?: string | null
  name?: string | null
}

interface AuthMeResponse {
  authenticated?: boolean
  user?: {
    id?: string
    email?: string | null
    username?: string | null
    name?: string | null
    role_name?: string | null
    status?: string | null
  }
  workspace?: AuthWorkspace
  permissions?: string[]
  csrf_token?: string | null
}

export function usePermissions() {
  const authQuery = useQuery<AuthMeResponse>({
    queryKey: ['auth-me'],
    queryFn: async () => {
      try {
        return await apiGetJson<AuthMeResponse>('/api/auth/me', {
          fallbackMessage: 'Failed to load auth context',
        })
      } catch {
        return { permissions: [] }
      }
    },
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    retry: 1,
  })

  const permissions = useMemo(() => new Set(authQuery.data?.permissions ?? []), [authQuery.data?.permissions])

  useEffect(() => {
    const csrf = (authQuery.data?.csrf_token || '').trim()
    if (!csrf || typeof window === 'undefined') return
    try {
      window.localStorage.setItem('mmm-csrf-token', csrf)
    } catch {
      // ignore storage errors
    }
  }, [authQuery.data?.csrf_token])

  const hasPermission = (key: string): boolean => permissions.has(key)
  const hasAnyPermission = (keys: string[]): boolean => keys.some((k) => permissions.has(k))

  return {
    isLoading: authQuery.isLoading,
    isError: authQuery.isError,
    permissions,
    permissionList: authQuery.data?.permissions ?? [],
    workspaceId: authQuery.data?.workspace?.id,
    auth: authQuery.data,
    hasPermission,
    hasAnyPermission,
  }
}
