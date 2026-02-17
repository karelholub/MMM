import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

export type OAuthProviderKey = 'google_ads' | 'meta_ads' | 'linkedin_ads'

export interface OAuthConnectionAccount {
  id: string
  name: string
  status?: string | number | null
  provider: OAuthProviderKey
}

export interface OAuthConnectionRecord {
  id?: string
  provider_key: OAuthProviderKey
  display_name: string
  status: 'not_connected' | 'connected' | 'needs_reauth' | 'error' | 'disabled'
  selected_accounts: string[]
  available_accounts: OAuthConnectionAccount[]
  selected_accounts_count: number
  last_tested_at?: string | null
  last_connected_at?: string | null
  last_refreshed_at?: string | null
  last_error?: string | null
  updated_at?: string | null
  can_start?: boolean
  missing_config_keys?: string[]
  missing_config_reason?: string | null
}

export async function listOAuthConnections(): Promise<{ items: OAuthConnectionRecord[]; total: number }> {
  return apiGetJson<{ items: OAuthConnectionRecord[]; total: number }>('/api/connections', {
    fallbackMessage: 'Failed to load OAuth connections',
  })
}

export async function startOAuthConnection(provider: OAuthProviderKey, returnUrl?: string): Promise<{ authorization_url: string }> {
  return apiSendJson<{ authorization_url: string }>(`/api/connections/${provider}/start`, 'POST', { return_url: returnUrl }, {
    fallbackMessage: `Failed to start ${provider} connection`,
  })
}

export async function reauthOAuthConnection(provider: OAuthProviderKey, returnUrl?: string): Promise<{ authorization_url: string }> {
  return apiSendJson<{ authorization_url: string }>(`/api/connections/${provider}/reauth`, 'POST', { return_url: returnUrl }, {
    fallbackMessage: `Failed to re-auth ${provider} connection`,
  })
}

export async function listOAuthProviderAccounts(provider: OAuthProviderKey): Promise<{ provider_key: OAuthProviderKey; accounts: OAuthConnectionAccount[] }> {
  return apiGetJson<{ provider_key: OAuthProviderKey; accounts: OAuthConnectionAccount[] }>(`/api/connections/${provider}/accounts`, {
    fallbackMessage: `Failed to list ${provider} accounts`,
  })
}

export async function selectOAuthProviderAccounts(provider: OAuthProviderKey, accountIds: string[]): Promise<OAuthConnectionRecord> {
  return apiSendJson<OAuthConnectionRecord>(`/api/connections/${provider}/select-accounts`, 'POST', { account_ids: accountIds }, {
    fallbackMessage: `Failed to save ${provider} account selection`,
  })
}

export async function testOAuthConnection(provider: OAuthProviderKey): Promise<any> {
  return apiSendJson<any>(`/api/connections/${provider}/test`, 'POST', undefined, {
    fallbackMessage: `Failed to test ${provider} connection`,
  })
}

export async function disconnectOAuthConnection(provider: OAuthProviderKey): Promise<{ ok: boolean; connection: OAuthConnectionRecord }> {
  return apiSendJson<{ ok: boolean; connection: OAuthConnectionRecord }>(`/api/connections/${provider}/disconnect`, 'POST', undefined, {
    fallbackMessage: `Failed to disconnect ${provider}`,
  })
}
