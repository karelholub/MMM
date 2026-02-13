import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

interface AuthWorkspace {
  id?: string
  slug?: string | null
  name?: string | null
}

interface AuthMeResponse {
  user?: {
    id?: string
    email?: string | null
    name?: string | null
    role_name?: string | null
    status?: string | null
  }
  workspace?: AuthWorkspace
  permissions?: string[]
}

export function usePermissions() {
  const authQuery = useQuery<AuthMeResponse>({
    queryKey: ['auth-me'],
    queryFn: async () => {
      const res = await fetch('/api/auth/me')
      if (!res.ok) {
        return { permissions: [] }
      }
      return (await res.json()) as AuthMeResponse
    },
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    retry: 1,
  })

  const permissions = useMemo(() => new Set(authQuery.data?.permissions ?? []), [authQuery.data?.permissions])

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
