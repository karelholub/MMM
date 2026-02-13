import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { usePermissions } from '../hooks/usePermissions'
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

interface FeatureFlags {
  access_control_enabled: boolean
}

interface RoleRow {
  id: string
  name: string
  is_system: boolean
}

interface UserRow {
  id: string
  email: string
  name?: string | null
  status: string
  last_login_at?: string | null
  membership_id: string
  membership_status: string
  role_id?: string | null
  role_name?: string | null
}

interface InvitationRow {
  id: string
  workspace_id: string
  email: string
  role_id?: string | null
  role_name?: string | null
  expires_at?: string | null
  accepted_at?: string | null
  created_at?: string | null
  invited_by_name?: string | null
}

function formatDateTime(value?: string | null): string {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleString()
}

export default function AccessControlUsersSection({ featureFlags }: { featureFlags: FeatureFlags }) {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'disabled'>('all')
  const [roleFilter, setRoleFilter] = useState<'all' | string>('all')
  const [inviteOpen, setInviteOpen] = useState(false)
  const [inviteEmail, setInviteEmail] = useState('')
  const [inviteRoleId, setInviteRoleId] = useState('')
  const [inviteError, setInviteError] = useState<string | null>(null)
  const [lastSuccess, setLastSuccess] = useState<string | null>(null)

  const permissions = usePermissions()
  const workspaceId = permissions.workspaceId || 'default'
  const canManage = permissions.hasPermission('users.manage')
  const canReadUsers = canManage || permissions.hasPermission('settings.view')

  const rolesQuery = useQuery<{ items: RoleRow[] }>({
    queryKey: ['admin-roles', workspaceId],
    queryFn: async () => {
      return apiGetJson<{ items: RoleRow[] }>(withQuery('/api/admin/roles', { workspaceId }), {
        fallbackMessage: 'Failed to load roles',
      })
    },
    enabled: featureFlags.access_control_enabled && canReadUsers,
  })

  const usersQuery = useQuery<{ items: UserRow[] }>({
    queryKey: ['admin-users', workspaceId, search, statusFilter],
    queryFn: async () => {
      const params = new URLSearchParams({ workspaceId })
      if (search.trim()) params.set('search', search.trim())
      if (statusFilter !== 'all') params.set('status', statusFilter)
      return apiGetJson<{ items: UserRow[] }>(`/api/admin/users?${params.toString()}`, {
        fallbackMessage: 'Failed to load users',
      })
    },
    enabled: featureFlags.access_control_enabled && canReadUsers,
  })

  const invitesQuery = useQuery<{ items: InvitationRow[] }>({
    queryKey: ['admin-invitations', workspaceId],
    queryFn: async () => {
      return apiGetJson<{ items: InvitationRow[] }>(withQuery('/api/admin/invitations', { workspaceId }), {
        fallbackMessage: 'Failed to load invitations',
      })
    },
    enabled: featureFlags.access_control_enabled && canReadUsers,
  })

  const filteredUsers = useMemo(() => {
    const all = usersQuery.data?.items ?? []
    if (roleFilter === 'all') return all
    return all.filter((u) => u.role_id === roleFilter)
  }, [usersQuery.data?.items, roleFilter])

  const refreshAll = () => {
    void queryClient.invalidateQueries({ queryKey: ['admin-users'] })
    void queryClient.invalidateQueries({ queryKey: ['admin-invitations'] })
    void queryClient.invalidateQueries({ queryKey: ['admin-roles'] })
  }

  const updateMembershipMutation = useMutation({
    mutationFn: async (payload: { membershipId: string; roleId: string }) => {
      return apiSendJson<any>(`/api/admin/memberships/${payload.membershipId}`, 'PATCH', { role_id: payload.roleId }, {
        fallbackMessage: 'Failed to update membership role',
      })
    },
    onSuccess: () => {
      setLastSuccess('Role updated')
      refreshAll()
    },
  })

  const updateUserStatusMutation = useMutation({
    mutationFn: async (payload: { userId: string; status: 'active' | 'disabled' }) => {
      return apiSendJson<any>(`/api/admin/users/${payload.userId}`, 'PATCH', { status: payload.status }, {
        fallbackMessage: 'Failed to update user status',
      })
    },
    onSuccess: () => {
      setLastSuccess('User status updated')
      refreshAll()
    },
  })

  const removeMembershipMutation = useMutation({
    mutationFn: async (membershipId: string) => {
      return apiSendJson<any>(`/api/admin/memberships/${membershipId}`, 'DELETE', undefined, {
        fallbackMessage: 'Failed to remove membership',
      })
    },
    onSuccess: () => {
      setLastSuccess('Member removed from workspace')
      refreshAll()
    },
  })

  const inviteMutation = useMutation({
    mutationFn: async (payload: { email: string; roleId: string }) => {
      return apiSendJson<any>('/api/admin/invitations', 'POST', {
        email: payload.email.trim().toLowerCase(),
        role_id: payload.roleId,
        workspace_id: workspaceId,
      }, {
        fallbackMessage: 'Failed to create invitation',
      })
    },
    onSuccess: () => {
      setLastSuccess('Invitation created')
      setInviteOpen(false)
      setInviteEmail('')
      setInviteRoleId('')
      setInviteError(null)
      refreshAll()
    },
  })

  const resendInviteMutation = useMutation({
    mutationFn: async (invitationId: string) => {
      return apiSendJson<any>(`/api/admin/invitations/${invitationId}/resend`, 'POST', undefined, {
        fallbackMessage: 'Failed to resend invitation',
      })
    },
    onSuccess: () => {
      setLastSuccess('Invitation resent')
      refreshAll()
    },
  })

  const revokeInviteMutation = useMutation({
    mutationFn: async (invitationId: string) => {
      return apiSendJson<any>(`/api/admin/invitations/${invitationId}/revoke`, 'DELETE', undefined, {
        fallbackMessage: 'Failed to revoke invitation',
      })
    },
    onSuccess: () => {
      setLastSuccess('Invitation revoked')
      refreshAll()
    },
  })

  const cardStyle: React.CSSProperties = {
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
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Access control</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Access control is disabled for this workspace. Enable `access_control_enabled` in settings flags.
        </p>
      </div>
    )
  }

  if (!canReadUsers) {
    return (
      <div style={cardStyle}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Access control</h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Your role cannot view workspace user administration.
        </p>
      </div>
    )
  }

  return (
    <>
      <div style={{ display: 'grid', gap: t.space.xl }}>
        <div style={cardStyle}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
            <div>
              <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Users</h3>
              <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Manage workspace members, roles, and account status.
              </p>
            </div>
            {canManage && (
              <button
                type="button"
                onClick={() => setInviteOpen(true)}
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
                Invite user
              </button>
            )}
          </div>

          <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
            <input
              type="search"
              placeholder="Search name or email"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{
                minWidth: 240,
                flex: 1,
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            />
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value as any)}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            >
              <option value="all">All statuses</option>
              <option value="active">Active</option>
              <option value="disabled">Disabled</option>
            </select>
            <select
              value={roleFilter}
              onChange={(e) => setRoleFilter(e.target.value)}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            >
              <option value="all">All roles</option>
              {(rolesQuery.data?.items ?? []).map((r) => (
                <option key={r.id} value={r.id}>
                  {r.name}
                </option>
              ))}
            </select>
          </div>

          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Name</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Email</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Role</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Status</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Last login</th>
                  <th style={{ textAlign: 'right', padding: '8px 10px' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {usersQuery.isLoading && (
                  <tr><td colSpan={6} style={{ padding: '12px 10px', color: t.color.textMuted }}>Loading users…</td></tr>
                )}
                {!usersQuery.isLoading && filteredUsers.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: '12px 10px', color: t.color.textMuted }}>No users found.</td></tr>
                )}
                {filteredUsers.map((u) => (
                  <tr key={u.id} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: '8px 10px' }}>{u.name || '—'}</td>
                    <td style={{ padding: '8px 10px' }}>{u.email}</td>
                    <td style={{ padding: '8px 10px' }}>
                      {canManage ? (
                        <select
                          value={u.role_id || ''}
                          onChange={(e) =>
                            updateMembershipMutation.mutate({ membershipId: u.membership_id, roleId: e.target.value })
                          }
                          disabled={updateMembershipMutation.isPending || !u.membership_id}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          {(rolesQuery.data?.items ?? []).map((r) => (
                            <option key={r.id} value={r.id}>{r.name}</option>
                          ))}
                        </select>
                      ) : (
                        <span>{u.role_name || '—'}</span>
                      )}
                    </td>
                    <td style={{ padding: '8px 10px' }}>{u.status}</td>
                    <td style={{ padding: '8px 10px' }}>{formatDateTime(u.last_login_at)}</td>
                    <td style={{ padding: '8px 10px', textAlign: 'right' }}>
                      {canManage && (
                        <div style={{ display: 'inline-flex', gap: t.space.xs }}>
                          <button
                            type="button"
                            onClick={() =>
                              updateUserStatusMutation.mutate({
                                userId: u.id,
                                status: u.status === 'disabled' ? 'active' : 'disabled',
                              })
                            }
                            style={{
                              border: `1px solid ${t.color.borderLight}`,
                              background: 'transparent',
                              borderRadius: t.radius.sm,
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              fontSize: t.font.sizeXs,
                              cursor: 'pointer',
                            }}
                          >
                            {u.status === 'disabled' ? 'Enable' : 'Disable'}
                          </button>
                          <button
                            type="button"
                            onClick={() => removeMembershipMutation.mutate(u.membership_id)}
                            style={{
                              border: 'none',
                              background: t.color.dangerSubtle,
                              color: t.color.danger,
                              borderRadius: t.radius.sm,
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              fontSize: t.font.sizeXs,
                              cursor: 'pointer',
                            }}
                          >
                            Remove
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div style={cardStyle}>
          <div>
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Invitations</h3>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Pending and accepted workspace invitations.
            </p>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Email</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Role</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Invited by</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Created</th>
                  <th style={{ textAlign: 'left', padding: '8px 10px' }}>Expires</th>
                  <th style={{ textAlign: 'right', padding: '8px 10px' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {invitesQuery.isLoading && (
                  <tr><td colSpan={6} style={{ padding: '12px 10px', color: t.color.textMuted }}>Loading invitations…</td></tr>
                )}
                {!invitesQuery.isLoading && (invitesQuery.data?.items ?? []).length === 0 && (
                  <tr><td colSpan={6} style={{ padding: '12px 10px', color: t.color.textMuted }}>No invitations found.</td></tr>
                )}
                {(invitesQuery.data?.items ?? []).map((inv) => (
                  <tr key={inv.id} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: '8px 10px' }}>{inv.email}</td>
                    <td style={{ padding: '8px 10px' }}>{inv.role_name || '—'}</td>
                    <td style={{ padding: '8px 10px' }}>{inv.invited_by_name || '—'}</td>
                    <td style={{ padding: '8px 10px' }}>{formatDateTime(inv.created_at)}</td>
                    <td style={{ padding: '8px 10px' }}>{formatDateTime(inv.expires_at)}</td>
                    <td style={{ padding: '8px 10px', textAlign: 'right' }}>
                      {canManage && !inv.accepted_at && (
                        <div style={{ display: 'inline-flex', gap: t.space.xs }}>
                          <button
                            type="button"
                            onClick={() => resendInviteMutation.mutate(inv.id)}
                            style={{
                              border: `1px solid ${t.color.borderLight}`,
                              background: 'transparent',
                              borderRadius: t.radius.sm,
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              fontSize: t.font.sizeXs,
                              cursor: 'pointer',
                            }}
                          >
                            Resend
                          </button>
                          <button
                            type="button"
                            onClick={() => revokeInviteMutation.mutate(inv.id)}
                            style={{
                              border: 'none',
                              background: t.color.dangerSubtle,
                              color: t.color.danger,
                              borderRadius: t.radius.sm,
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              fontSize: t.font.sizeXs,
                              cursor: 'pointer',
                            }}
                          >
                            Revoke
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {lastSuccess && (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.success }}>
            {lastSuccess}
          </div>
        )}
      </div>

      {inviteOpen && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(15,23,42,0.45)',
            zIndex: 40,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: t.space.lg,
          }}
        >
          <div
            style={{
              width: 'min(480px, 100%)',
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.border}`,
              padding: t.space.lg,
              display: 'grid',
              gap: t.space.md,
            }}
          >
            <div>
              <h3 style={{ margin: 0, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold }}>Invite user</h3>
              <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Send an invitation with a workspace role.
              </p>
            </div>
            <label style={{ display: 'grid', gap: 4 }}>
              <span style={{ fontSize: t.font.sizeSm }}>Email</span>
              <input
                type="email"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.target.value)}
                placeholder="name@company.com"
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  fontSize: t.font.sizeSm,
                }}
              />
            </label>
            <label style={{ display: 'grid', gap: 4 }}>
              <span style={{ fontSize: t.font.sizeSm }}>Role</span>
              <select
                value={inviteRoleId}
                onChange={(e) => setInviteRoleId(e.target.value)}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  fontSize: t.font.sizeSm,
                }}
              >
                <option value="">Select role</option>
                {(rolesQuery.data?.items ?? []).map((r) => (
                  <option key={r.id} value={r.id}>{r.name}</option>
                ))}
              </select>
            </label>
            {inviteError && <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>{inviteError}</div>}
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
              <button
                type="button"
                onClick={() => {
                  setInviteOpen(false)
                  setInviteError(null)
                }}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: 'transparent',
                  color: t.color.text,
                  fontSize: t.font.sizeSm,
                  cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => {
                  setInviteError(null)
                  if (!inviteEmail.trim()) {
                    setInviteError('Email is required')
                    return
                  }
                  if (!inviteRoleId) {
                    setInviteError('Role is required')
                    return
                  }
                  inviteMutation.mutate({ email: inviteEmail, roleId: inviteRoleId })
                }}
                disabled={inviteMutation.isPending}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background: t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor: inviteMutation.isPending ? 'wait' : 'pointer',
                  opacity: inviteMutation.isPending ? 0.85 : 1,
                }}
              >
                {inviteMutation.isPending ? 'Sending…' : 'Send invite'}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
