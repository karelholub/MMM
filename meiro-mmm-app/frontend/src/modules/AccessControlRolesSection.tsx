import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import type { CSSProperties } from 'react'
import { usePermissions } from '../hooks/usePermissions'
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

interface FeatureFlags {
  access_control_enabled: boolean
  custom_roles_enabled: boolean
}

interface RoleRow {
  id: string
  workspace_id?: string | null
  name: string
  description?: string | null
  is_system: boolean
  member_count: number
  permission_keys: string[]
}

interface PermissionRow {
  key: string
  description: string
  category: string
}

function summarizeAccess(permissionKeys: string[]): Array<{ label: string; level: 'Manage' | 'View' | 'None' }> {
  const check = (prefix: string): 'Manage' | 'View' | 'None' => {
    if (permissionKeys.includes(`${prefix}.manage`)) return 'Manage'
    if (permissionKeys.includes(`${prefix}.view`)) return 'View'
    return 'None'
  }
  return [
    { label: 'Journeys', level: check('journeys') },
    { label: 'Funnels', level: check('funnels') },
    { label: 'Attribution', level: check('attribution') },
    { label: 'Alerts', level: check('alerts') },
    { label: 'Settings', level: check('settings') },
  ]
}

export default function AccessControlRolesSection({ featureFlags }: { featureFlags: FeatureFlags }) {
  const queryClient = useQueryClient()
  const [selectedRoleId, setSelectedRoleId] = useState<string | null>(null)
  const [draftName, setDraftName] = useState('')
  const [draftDescription, setDraftDescription] = useState('')
  const [draftPermissions, setDraftPermissions] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const permissions = usePermissions()
  const workspaceId = permissions.workspaceId || 'default'
  const canManageRoles = permissions.hasPermission('roles.manage')

  const rolesQuery = useQuery<{ items: RoleRow[] }>({
    queryKey: ['admin-roles-ui', workspaceId],
    queryFn: async () => {
      return apiGetJson<{ items: RoleRow[] }>(withQuery('/api/admin/roles', { workspaceId }), {
        fallbackMessage: 'Failed to load roles',
      })
    },
    enabled: featureFlags.access_control_enabled && canManageRoles,
  })

  const permissionsQuery = useQuery<PermissionRow[]>({
    queryKey: ['admin-permissions-ui'],
    queryFn: async () => {
      return apiGetJson<PermissionRow[]>('/api/admin/permissions', {
        fallbackMessage: 'Failed to load permissions',
      })
    },
    enabled: featureFlags.access_control_enabled && canManageRoles,
  })

  const selectedRole = useMemo(() => {
    const roles = rolesQuery.data?.items ?? []
    return roles.find((r) => r.id === selectedRoleId) ?? roles[0] ?? null
  }, [rolesQuery.data?.items, selectedRoleId])

  useEffect(() => {
    if (!selectedRole && (rolesQuery.data?.items?.length ?? 0) > 0) {
      setSelectedRoleId(rolesQuery.data!.items[0].id)
    }
  }, [rolesQuery.data?.items, selectedRole])

  useEffect(() => {
    if (!selectedRole) return
    setDraftName(selectedRole.name)
    setDraftDescription(selectedRole.description || '')
    setDraftPermissions([...selectedRole.permission_keys])
  }, [selectedRole?.id])

  const permissionsByCategory = useMemo(() => {
    const grouped = new Map<string, PermissionRow[]>()
    for (const perm of permissionsQuery.data ?? []) {
      const key = perm.category || 'other'
      const arr = grouped.get(key) ?? []
      arr.push(perm)
      grouped.set(key, arr)
    }
    return [...grouped.entries()].sort((a, b) => a[0].localeCompare(b[0]))
  }, [permissionsQuery.data])

  const refreshRoles = () => {
    void queryClient.invalidateQueries({ queryKey: ['admin-roles-ui'] })
    void queryClient.invalidateQueries({ queryKey: ['admin-roles'] })
  }

  const createRoleMutation = useMutation({
    mutationFn: async (payload: { name: string; description?: string; permission_keys: string[] }) => {
      return apiSendJson<any>('/api/admin/roles', 'POST', { ...payload, workspace_id: workspaceId }, {
        fallbackMessage: 'Failed to create role',
      })
    },
    onSuccess: (next) => {
      setSuccess('Role created')
      setSelectedRoleId(next.id)
      refreshRoles()
    },
    onError: (err) => setError((err as Error).message),
  })

  const updateRoleMutation = useMutation({
    mutationFn: async (payload: { id: string; name: string; description?: string; permission_keys: string[] }) => {
      return apiSendJson<any>(
        `/api/admin/roles/${payload.id}`,
        'PUT',
        {
          name: payload.name,
          description: payload.description,
          permission_keys: payload.permission_keys,
        },
        { fallbackMessage: 'Failed to update role' },
      )
    },
    onSuccess: () => {
      setSuccess('Role updated')
      refreshRoles()
    },
    onError: (err) => setError((err as Error).message),
  })

  const deleteRoleMutation = useMutation({
    mutationFn: async (roleId: string) => {
      return apiSendJson<any>(`/api/admin/roles/${roleId}`, 'DELETE', undefined, {
        fallbackMessage: 'Failed to delete role',
      })
    },
    onSuccess: () => {
      setSuccess('Role deleted')
      setSelectedRoleId(null)
      refreshRoles()
    },
    onError: (err) => setError((err as Error).message),
  })

  const cloneSelectedRole = () => {
    if (!selectedRole) return
    createRoleMutation.mutate({
      name: `${selectedRole.name} (Copy)`,
      description: selectedRole.description || '',
      permission_keys: selectedRole.permission_keys,
    })
  }

  const togglePermission = (key: string) => {
    setDraftPermissions((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key],
    )
  }

  const cardStyle: CSSProperties = {
    border: `1px solid ${t.color.borderLight}`,
    borderRadius: t.radius.md,
    background: t.color.surface,
    padding: t.space.md,
    display: 'grid',
    gap: t.space.sm,
  }

  if (!featureFlags.access_control_enabled) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Roles & permissions</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Access control is disabled for this workspace.
        </p>
      </div>
    )
  }

  if (!canManageRoles) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Roles & permissions</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Your role cannot manage roles and permissions.
        </p>
      </div>
    )
  }

  if (rolesQuery.isLoading || permissionsQuery.isLoading) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Roles & permissions</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading roles and permissions…</p>
      </div>
    )
  }

  if (rolesQuery.isError || permissionsQuery.isError) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Roles & permissions</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.danger }}>
          Failed to load access-control roles data.
        </p>
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gap: t.space.xl }}>
      <div style={cardStyle}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Roles</h3>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              System roles are read-only. Custom roles are editable when enabled.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.xs }}>
            {featureFlags.custom_roles_enabled && canManageRoles && (
              <button
                type="button"
                onClick={() => {
                  setError(null)
                  setSuccess(null)
                  createRoleMutation.mutate({
                    name: 'New custom role',
                    description: '',
                    permission_keys: [],
                  })
                }}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.accent}`,
                  background: t.color.accentMuted,
                  color: t.color.accent,
                  fontSize: t.font.sizeXs,
                  cursor: 'pointer',
                }}
              >
                New role
              </button>
            )}
            {featureFlags.custom_roles_enabled && canManageRoles && (
              <button
                type="button"
                onClick={cloneSelectedRole}
                disabled={!selectedRole}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  background: 'transparent',
                  color: t.color.text,
                  fontSize: t.font.sizeXs,
                  cursor: selectedRole ? 'pointer' : 'not-allowed',
                  opacity: selectedRole ? 1 : 0.6,
                }}
              >
                Clone role
              </button>
            )}
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(260px, 360px) 1fr', gap: t.space.md }}>
          <div style={{ display: 'grid', gap: t.space.xs, alignContent: 'start' }}>
            {(rolesQuery.data?.items ?? []).map((role) => (
              <button
                key={role.id}
                type="button"
                onClick={() => {
                  setSelectedRoleId(role.id)
                  setError(null)
                  setSuccess(null)
                }}
                style={{
                  textAlign: 'left',
                  borderRadius: t.radius.sm,
                  border: `1px solid ${selectedRole?.id === role.id ? t.color.accent : t.color.borderLight}`,
                  background: selectedRole?.id === role.id ? t.color.accentMuted : t.color.surface,
                  padding: t.space.sm,
                  display: 'grid',
                  gap: 4,
                  cursor: 'pointer',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm }}>
                  <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>{role.name}</span>
                  <span style={{ fontSize: t.font.sizeXs, color: role.is_system ? t.color.textMuted : t.color.accent }}>
                    {role.is_system ? 'System' : 'Custom'}
                  </span>
                </div>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{role.description || 'No description'}</span>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{role.member_count} member(s)</span>
              </button>
            ))}
          </div>

          <div style={{ ...cardStyle, alignContent: 'start' }}>
            {!selectedRole ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>Select a role.</div>
            ) : (
              <>
                <div style={{ display: 'grid', gap: t.space.xs }}>
                  <label style={{ display: 'grid', gap: 4 }}>
                    <span style={{ fontSize: t.font.sizeXs }}>Role name</span>
                    <input
                      value={draftName}
                      onChange={(e) => setDraftName(e.target.value)}
                      disabled={selectedRole.is_system || !canManageRoles || !featureFlags.custom_roles_enabled}
                      style={{
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.borderLight}`,
                        fontSize: t.font.sizeSm,
                        background: selectedRole.is_system ? t.color.bgSubtle : t.color.surface,
                      }}
                    />
                  </label>
                  <label style={{ display: 'grid', gap: 4 }}>
                    <span style={{ fontSize: t.font.sizeXs }}>Description</span>
                    <input
                      value={draftDescription}
                      onChange={(e) => setDraftDescription(e.target.value)}
                      disabled={selectedRole.is_system || !canManageRoles || !featureFlags.custom_roles_enabled}
                      style={{
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.borderLight}`,
                        fontSize: t.font.sizeSm,
                        background: selectedRole.is_system ? t.color.bgSubtle : t.color.surface,
                      }}
                    />
                  </label>
                </div>

                <div style={{ display: 'grid', gap: t.space.xs }}>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Permission matrix</div>
                  {permissionsByCategory.map(([category, perms]) => (
                    <div
                      key={category}
                      style={{
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.sm,
                        padding: t.space.sm,
                        display: 'grid',
                        gap: t.space.xs,
                      }}
                    >
                      <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, textTransform: 'capitalize' }}>
                        {category}
                      </div>
                      {perms.map((perm) => {
                        const checked = draftPermissions.includes(perm.key)
                        return (
                          <label
                            key={perm.key}
                            style={{
                              display: 'flex',
                              alignItems: 'flex-start',
                              gap: 8,
                              fontSize: t.font.sizeXs,
                              color: t.color.textSecondary,
                            }}
                          >
                            <input
                              type="checkbox"
                              checked={checked}
                              disabled={selectedRole.is_system || !canManageRoles || !featureFlags.custom_roles_enabled}
                              onChange={() => togglePermission(perm.key)}
                            />
                            <span>
                              <strong style={{ color: t.color.text }}>{perm.key}</strong>
                              <span style={{ display: 'block' }}>{perm.description}</span>
                            </span>
                          </label>
                        )
                      })}
                    </div>
                  ))}
                </div>

                <div style={{ display: 'grid', gap: t.space.xs }}>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Effective access preview</div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.xs }}>
                    {summarizeAccess(draftPermissions).map((item) => (
                      <span
                        key={item.label}
                        style={{
                          padding: '2px 8px',
                          borderRadius: 999,
                          border: `1px solid ${item.level === 'Manage' ? t.color.success : item.level === 'View' ? t.color.accent : t.color.border}`,
                          color: item.level === 'Manage' ? t.color.success : item.level === 'View' ? t.color.accent : t.color.textMuted,
                          fontSize: t.font.sizeXs,
                        }}
                      >
                        {item.label}: {item.level}
                      </span>
                    ))}
                  </div>
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm, flexWrap: 'wrap' }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                    {selectedRole.is_system
                      ? 'System role is read-only.'
                      : selectedRole.name === 'Admin' && selectedRole.member_count <= 1
                      ? 'Guardrail: cannot remove last Admin from workspace.'
                      : 'Custom role is editable.'}
                  </div>
                  <div style={{ display: 'flex', gap: t.space.xs }}>
                    {!selectedRole.is_system && canManageRoles && featureFlags.custom_roles_enabled && (
                      <>
                        <button
                          type="button"
                          onClick={() =>
                            updateRoleMutation.mutate({
                              id: selectedRole.id,
                              name: draftName.trim() || selectedRole.name,
                              description: draftDescription,
                              permission_keys: draftPermissions,
                            })
                          }
                          disabled={updateRoleMutation.isPending}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: 'none',
                            background: t.color.accent,
                            color: t.color.surface,
                            fontSize: t.font.sizeXs,
                            cursor: updateRoleMutation.isPending ? 'wait' : 'pointer',
                          }}
                        >
                          {updateRoleMutation.isPending ? 'Saving…' : 'Save role'}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            const ok = window.confirm(`Delete role "${selectedRole.name}"?`)
                            if (!ok) return
                            deleteRoleMutation.mutate(selectedRole.id)
                          }}
                          disabled={deleteRoleMutation.isPending || (selectedRole.name === 'Admin' && selectedRole.member_count <= 1)}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: 'none',
                            background: t.color.dangerSubtle,
                            color: t.color.danger,
                            fontSize: t.font.sizeXs,
                            cursor: deleteRoleMutation.isPending ? 'wait' : 'pointer',
                            opacity: selectedRole.name === 'Admin' && selectedRole.member_count <= 1 ? 0.6 : 1,
                          }}
                        >
                          Delete role
                        </button>
                      </>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      {error && (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
          {error}
        </div>
      )}
      {success && (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.success }}>
          {success}
        </div>
      )}
    </div>
  )
}
