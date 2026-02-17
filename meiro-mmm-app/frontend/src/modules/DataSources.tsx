import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import DashboardTable from '../components/dashboard/DashboardTable'
import { tokens as t } from '../theme/tokens'
import {
  connectMeiroCDP,
  disconnectMeiroCDP,
  getMeiroConfig,
  getMeiroWebhookEvents,
  meiroRotateWebhookSecret,
  getMeiroMapping,
  getMeiroPullConfig,
  meiroPull,
  saveMeiroMapping,
  saveMeiroPullConfig,
  testMeiroConnection,
} from '../connectors/meiroConnector'
import {
  createDataSource,
  deleteDataSource,
  disableDataSource,
  listDataSources,
  rotateDataSourceCredentials,
  testDataSource,
  testSavedDataSource,
  type DataSourceTestResponse,
} from '../connectors/dataSourcesConnector'
import {
  disconnectOAuthConnection,
  listOAuthConnections,
  listOAuthProviderAccounts,
  reauthOAuthConnection,
  selectOAuthProviderAccounts,
  startOAuthConnection,
  testOAuthConnection,
  type OAuthConnectionRecord,
  type OAuthProviderKey,
} from '../connectors/oauthConnectionsConnector'
import { apiGetJson, apiRequest, apiSendJson } from '../lib/apiClient'
import { usePermissions } from '../hooks/usePermissions'

interface DataSourcesProps {
  onJourneysImported: () => void
}

type IngestionMethod = 'sample' | 'upload' | 'meiro'
type SystemsTab = 'warehouses' | 'ad_platforms' | 'cdp'
type WarehouseType = 'bigquery' | 'snowflake'
type ProviderModalState = { provider: OAuthProviderKey; displayName: string } | null
type ConnectPickerState = { open: boolean } 
type CredentialsModalState = { open: boolean; provider: OAuthProviderKey | null }

type WarehouseDraft = {
  type: WarehouseType
  name: string
  // BigQuery
  serviceAccountJson: string
  projectId: string
  defaultDataset: string
  // Snowflake
  account: string
  username: string
  privateKeyPem: string
  passphrase: string
  warehouse: string
  role: string
  defaultDatabase: string
  defaultSchema: string
}

const JSON_FORMAT_V2 = `{
  "schema_version": "2.0",
  "defaults": { "timezone": "UTC", "currency": "EUR" },
  "journeys": [
    {
      "journey_id": "j_001",
      "customer": { "id": "c001", "type": "profile_id" },
      "touchpoints": [
        {
          "id": "tp_1",
          "ts": "2024-01-02T10:12:00Z",
          "channel": "google_ads",
          "campaign": { "name": "Brand", "id": "987" }
        }
      ],
      "conversions": [
        { "id": "cv_1", "ts": "2024-01-07T09:00:00Z", "name": "purchase", "value": 120.0, "currency": "EUR" }
      ]
    }
  ]
}`

function statusBadge(status: string) {
  const s = (status || '').toLowerCase()
  const map: Record<string, { bg: string; color: string; label: string }> = {
    connected: { bg: t.color.successMuted, color: t.color.success, label: 'Connected' },
    error: { bg: t.color.dangerMuted, color: t.color.danger, label: 'Error' },
    needs_reauth: { bg: t.color.warningMuted, color: t.color.warning, label: 'Needs re-auth' },
    disabled: { bg: t.color.borderLight, color: t.color.textMuted, label: 'Disabled' },
    needs_attention: { bg: t.color.warningMuted, color: t.color.warning, label: 'Needs attention' },
    not_connected: { bg: t.color.borderLight, color: t.color.textMuted, label: 'Not connected' },
  }
  const v = map[s] || map.not_connected
  return (
    <span
      style={{
        border: `1px solid ${v.color}`,
        background: v.bg,
        color: v.color,
        borderRadius: t.radius.full,
        padding: '2px 8px',
        fontSize: t.font.sizeXs,
        fontWeight: t.font.weightSemibold,
      }}
    >
      {v.label}
    </span>
  )
}

function relativeTime(iso?: string | null) {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return '—'
  const m = Math.floor((Date.now() - d.getTime()) / 60000)
  if (m < 1) return 'just now'
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

function disabledReason(canManageSettings: boolean, item: { can_start?: boolean; missing_config_reason?: string | null }) {
  if (!canManageSettings) return 'You need Settings manage permission'
  if (item.can_start === false) return item.missing_config_reason || 'Provider is not fully configured'
  return undefined
}

function providerToPlatform(provider: OAuthProviderKey): 'google' | 'meta' | 'linkedin' {
  if (provider === 'google_ads') return 'google'
  if (provider === 'meta_ads') return 'meta'
  return 'linkedin'
}

function defaultWarehouseDraft(type: WarehouseType): WarehouseDraft {
  return {
    type,
    name: '',
    serviceAccountJson: '',
    projectId: '',
    defaultDataset: '',
    account: '',
    username: '',
    privateKeyPem: '',
    passphrase: '',
    warehouse: '',
    role: '',
    defaultDatabase: '',
    defaultSchema: '',
  }
}

const DEFAULT_AD_PROVIDER_ROWS: Array<{
  provider_key: OAuthProviderKey
  display_name: string
  status: 'not_connected'
  selected_accounts_count: number
  last_tested_at: null
  last_error: null
  selected_accounts: string[]
  can_start: boolean
  missing_config_keys: string[]
  missing_config_reason: string | null
}> = [
  {
    provider_key: 'google_ads',
    display_name: 'Google Ads',
    status: 'not_connected',
    selected_accounts_count: 0,
    last_tested_at: null,
    last_error: null,
    selected_accounts: [],
    can_start: true,
    missing_config_keys: [],
    missing_config_reason: null,
  },
  {
    provider_key: 'meta_ads',
    display_name: 'Meta Ads',
    status: 'not_connected',
    selected_accounts_count: 0,
    last_tested_at: null,
    last_error: null,
    selected_accounts: [],
    can_start: true,
    missing_config_keys: [],
    missing_config_reason: null,
  },
  {
    provider_key: 'linkedin_ads',
    display_name: 'LinkedIn Ads',
    status: 'not_connected',
    selected_accounts_count: 0,
    last_tested_at: null,
    last_error: null,
    selected_accounts: [],
    can_start: true,
    missing_config_keys: [],
    missing_config_reason: null,
  },
]

export default function DataSources({ onJourneysImported }: DataSourcesProps) {
  const queryClient = useQueryClient()
  const fileRef = useRef<HTMLInputElement>(null)
  const permissions = usePermissions()
  const canManageSettings = permissions.hasPermission('settings.manage')

  const [ingestionMethod, setIngestionMethod] = useState<IngestionMethod>('sample')
  const [systemsTab, setSystemsTab] = useState<SystemsTab>('warehouses')
  const [showJsonHelp, setShowJsonHelp] = useState(false)
  const [showPreviewValidation, setShowPreviewValidation] = useState(false)
  const [showImportLog, setShowImportLog] = useState(false)

  const [drawerOpen, setDrawerOpen] = useState(false)
  const [drawerConnector, setDrawerConnector] = useState<{ id: string; name: string; type: string; category: string } | null>(null)
  const [meiroTab, setMeiroTab] = useState<'connection' | 'pull' | 'mapping'>('connection')
  const [meiroUrl, setMeiroUrl] = useState('')
  const [meiroKey, setMeiroKey] = useState('')
  const [webhookSecretValue, setWebhookSecretValue] = useState<string | null>(null)

  const [wizardOpen, setWizardOpen] = useState(false)
  const [wizardStep, setWizardStep] = useState<1 | 2 | 3 | 4>(1)
  const [warehouseDraft, setWarehouseDraft] = useState<WarehouseDraft>(defaultWarehouseDraft('bigquery'))
  const [warehouseTestResult, setWarehouseTestResult] = useState<DataSourceTestResponse | null>(null)
  const [wizardError, setWizardError] = useState<string | null>(null)
  const [rotateSecretText, setRotateSecretText] = useState('')
  const [oauthToast, setOauthToast] = useState<string | null>(null)
  const [providerModal, setProviderModal] = useState<ProviderModalState>(null)
  const [selectedProviderAccounts, setSelectedProviderAccounts] = useState<string[]>([])
  const [connectPicker, setConnectPicker] = useState<ConnectPickerState>({ open: false })
  const [credentialsModal, setCredentialsModal] = useState<CredentialsModalState>({ open: false, provider: null })
  const [credentialFields, setCredentialFields] = useState<Record<string, string>>({
    client_id: '',
    client_secret: '',
    developer_token: '',
    app_id: '',
    app_secret: '',
  })

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () =>
      apiGetJson<any>('/api/attribution/journeys', { fallbackMessage: 'Failed to load journeys summary' })
        .catch(() => ({ loaded: false, count: 0, converted: 0 })),
  })

  const validationQuery = useQuery({
    queryKey: ['journeys-validation-summary'],
    queryFn: async () =>
      apiGetJson<any>('/api/attribution/journeys/validation', { fallbackMessage: 'Failed to load validation summary' })
        .catch(() => ({ error_count: 0, warn_count: 0, validation_items: [] })),
  })

  const previewQuery = useQuery({
    queryKey: ['journeys-preview-20'],
    queryFn: async () =>
      apiGetJson<any>('/api/attribution/journeys/preview?limit=20', { fallbackMessage: 'Failed to load preview' })
        .catch(() => ({ rows: [], columns: [], total: 0 })),
    enabled: showPreviewValidation,
  })

  const importLogQuery = useQuery({
    queryKey: ['import-log-recent'],
    queryFn: async () =>
      apiGetJson<any>('/api/attribution/import-log', { fallbackMessage: 'Failed to load import log' })
        .catch(() => ({ runs: [] })),
    enabled: showImportLog,
  })

  const inventoryQuery = useQuery({
    queryKey: ['data-sources-inventory'],
    queryFn: async () => listDataSources(),
  })

  const oauthConnectionsQuery = useQuery({
    queryKey: ['oauth-connections'],
    queryFn: async () => listOAuthConnections(),
  })

  const legacyConnectionsQuery = useQuery({
    queryKey: ['legacy-datasource-connections'],
    queryFn: async () =>
      apiGetJson<{ connections: Array<{ id: string; status: string; label?: string; selected_accounts_count?: number; last_error?: string | null; last_tested_at?: string | null }> }>(
        '/api/datasources/connections',
        { fallbackMessage: 'Failed to load datasource connections' },
      ),
  })

  const providerAccountsQuery = useQuery({
    queryKey: ['oauth-provider-accounts', providerModal?.provider],
    queryFn: async () => {
      if (!providerModal?.provider) return { provider_key: 'google_ads', accounts: [] }
      return listOAuthProviderAccounts(providerModal.provider)
    },
    enabled: !!providerModal?.provider,
  })

  const meiroConfigQuery = useQuery({ queryKey: ['meiro-config'], queryFn: getMeiroConfig })
  const meiroMappingQuery = useQuery({ queryKey: ['meiro-mapping'], queryFn: getMeiroMapping, enabled: drawerOpen && meiroTab === 'mapping' && drawerConnector?.type === 'meiro' })
  const meiroPullConfigQuery = useQuery({ queryKey: ['meiro-pull-config'], queryFn: getMeiroPullConfig, enabled: drawerOpen && meiroTab === 'pull' && drawerConnector?.type === 'meiro' })
  const meiroWebhookEventsQuery = useQuery({
    queryKey: ['meiro-webhook-events'],
    queryFn: () => getMeiroWebhookEvents(100),
    enabled: drawerOpen && meiroTab === 'connection' && drawerConnector?.type === 'meiro',
  })

  const uploadMutation = useMutation({
    mutationFn: async (file: File) => {
      const fd = new FormData()
      fd.append('file', file)
      const res = await apiRequest('/api/attribution/journeys/upload', {
        method: 'POST',
        body: fd,
        fallbackMessage: 'Upload failed',
      })
      return res.json()
    },
    onSuccess: async () => {
      onJourneysImported()
      await queryClient.invalidateQueries({ queryKey: ['journeys-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-validation-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-preview-20'] })
      await queryClient.invalidateQueries({ queryKey: ['import-log-recent'] })
    },
  })

  const loadSampleMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<any>('/api/attribution/journeys/load-sample', 'POST', {}, { fallbackMessage: 'Failed to load sample' }),
    onSuccess: async () => {
      onJourneysImported()
      await queryClient.invalidateQueries({ queryKey: ['journeys-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-validation-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-preview-20'] })
      await queryClient.invalidateQueries({ queryKey: ['import-log-recent'] })
    },
  })

  const importFromMeiroMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<any>('/api/attribution/journeys/from-cdp', 'POST', {}, { fallbackMessage: 'Import from Meiro failed' }),
    onSuccess: async () => {
      onJourneysImported()
      await queryClient.invalidateQueries({ queryKey: ['journeys-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-validation-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-preview-20'] })
      await queryClient.invalidateQueries({ queryKey: ['import-log-recent'] })
    },
  })

  const connectMeiroMutation = useMutation({
    mutationFn: async () => connectMeiroCDP({ api_base_url: meiroUrl, api_key: meiroKey }),
    onSuccess: async () => {
      setMeiroKey('')
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
      await queryClient.invalidateQueries({ queryKey: ['legacy-datasource-connections'] })
    },
  })

  const testMeiroMutation = useMutation({
    mutationFn: async () => testMeiroConnection({ api_base_url: meiroUrl || undefined, api_key: meiroKey || undefined }),
  })

  const disconnectMeiroMutation = useMutation({
    mutationFn: disconnectMeiroCDP,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
      await queryClient.invalidateQueries({ queryKey: ['legacy-datasource-connections'] })
    },
  })

  const saveMeiroPullMutation = useMutation({ mutationFn: saveMeiroPullConfig, onSuccess: () => queryClient.invalidateQueries({ queryKey: ['meiro-pull-config'] }) })
  const runMeiroPullMutation = useMutation({ mutationFn: () => meiroPull(), onSuccess: () => onJourneysImported() })
  const saveMeiroMappingMutation = useMutation({ mutationFn: saveMeiroMapping, onSuccess: () => queryClient.invalidateQueries({ queryKey: ['meiro-mapping'] }) })
  const rotateWebhookSecretMutation = useMutation({
    mutationFn: meiroRotateWebhookSecret,
    onSuccess: async (res) => {
      setWebhookSecretValue(res.secret || null)
      await queryClient.invalidateQueries({ queryKey: ['meiro-config'] })
    },
    onError: (e) => setOauthToast((e as Error).message || 'Failed to rotate webhook secret'),
  })

  const testWarehouseMutation = useMutation({
    mutationFn: async () => {
      const payload = warehouseDraft.type === 'bigquery'
        ? {
            type: 'bigquery',
            config_json: { name: warehouseDraft.name, project_id: warehouseDraft.projectId, default_dataset: warehouseDraft.defaultDataset },
            secrets: { service_account_json: warehouseDraft.serviceAccountJson },
          }
        : {
            type: 'snowflake',
            config_json: {
              name: warehouseDraft.name,
              account: warehouseDraft.account,
              username: warehouseDraft.username,
              warehouse: warehouseDraft.warehouse,
              role: warehouseDraft.role,
              default_database: warehouseDraft.defaultDatabase,
              default_schema: warehouseDraft.defaultSchema,
            },
            secrets: { private_key_pem: warehouseDraft.privateKeyPem, passphrase: warehouseDraft.passphrase },
          }
      return testDataSource(payload)
    },
    onSuccess: (res) => setWarehouseTestResult(res),
    onError: (e) => setWizardError((e as Error).message),
  })

  const createWarehouseMutation = useMutation({
    mutationFn: async () => {
      const payload = warehouseDraft.type === 'bigquery'
        ? {
            category: 'warehouse',
            type: 'bigquery',
            name: warehouseDraft.name,
            config_json: { project_id: warehouseDraft.projectId, default_dataset: warehouseDraft.defaultDataset },
            secrets: { service_account_json: warehouseDraft.serviceAccountJson },
          }
        : {
            category: 'warehouse',
            type: 'snowflake',
            name: warehouseDraft.name,
            config_json: {
              account: warehouseDraft.account,
              username: warehouseDraft.username,
              warehouse: warehouseDraft.warehouse,
              role: warehouseDraft.role,
              default_database: warehouseDraft.defaultDatabase,
              default_schema: warehouseDraft.defaultSchema,
            },
            secrets: { private_key_pem: warehouseDraft.privateKeyPem, passphrase: warehouseDraft.passphrase },
          }
      return createDataSource(payload)
    },
    onSuccess: async () => {
      setWizardOpen(false)
      setWizardStep(1)
      setWarehouseDraft(defaultWarehouseDraft('bigquery'))
      setWarehouseTestResult(null)
      setWizardError(null)
      await queryClient.invalidateQueries({ queryKey: ['data-sources-inventory'] })
      await queryClient.invalidateQueries({ queryKey: ['legacy-datasource-connections'] })
    },
    onError: (e) => setWizardError((e as Error).message),
  })

  const testSavedMutation = useMutation({ mutationFn: (id: string) => testSavedDataSource(id), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['data-sources-inventory'] }) })
  const disableMutation = useMutation({ mutationFn: (id: string) => disableDataSource(id), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['data-sources-inventory'] }) })
  const deleteMutation = useMutation({ mutationFn: (id: string) => deleteDataSource(id), onSuccess: () => { setDrawerOpen(false); setDrawerConnector(null); queryClient.invalidateQueries({ queryKey: ['data-sources-inventory'] }) } })
  const rotateMutation = useMutation({
    mutationFn: async (id: string) => {
      if (!rotateSecretText.trim()) throw new Error('Provide credential payload to rotate')
      if (drawerConnector?.type === 'bigquery') return rotateDataSourceCredentials(id, { service_account_json: rotateSecretText })
      if (drawerConnector?.type === 'snowflake') return rotateDataSourceCredentials(id, { private_key_pem: rotateSecretText })
      throw new Error('Credential rotation supported for warehouse connectors in MVP')
    },
    onSuccess: () => {
      setRotateSecretText('')
      queryClient.invalidateQueries({ queryKey: ['data-sources-inventory'] })
    },
  })

  const startOAuthMutation = useMutation({
    mutationFn: async (provider: OAuthProviderKey) => {
      return startOAuthConnection(provider, '/datasources')
    },
    onSuccess: (res) => {
      window.location.href = res.authorization_url
    },
    onError: (e) => {
      setOauthToast((e as Error).message || 'Failed to start OAuth connection')
    },
  })

  const reauthOAuthMutation = useMutation({
    mutationFn: async (provider: OAuthProviderKey) => {
      return reauthOAuthConnection(provider, '/datasources')
    },
    onSuccess: (res) => {
      window.location.href = res.authorization_url
    },
    onError: (e) => {
      setOauthToast((e as Error).message || 'Failed to re-auth OAuth connection')
    },
  })

  const testOAuthMutation = useMutation({
    mutationFn: async (provider: OAuthProviderKey) => testOAuthConnection(provider),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['oauth-connections'] })
    },
  })

  const disconnectOAuthMutation = useMutation({
    mutationFn: async (provider: OAuthProviderKey) => disconnectOAuthConnection(provider),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['oauth-connections'] })
    },
  })

  const selectAccountsMutation = useMutation({
    mutationFn: async (payload: { provider: OAuthProviderKey; accountIds: string[] }) =>
      selectOAuthProviderAccounts(payload.provider, payload.accountIds),
    onSuccess: async () => {
      setProviderModal(null)
      setSelectedProviderAccounts([])
      await queryClient.invalidateQueries({ queryKey: ['oauth-connections'] })
    },
  })

  const saveProviderCredentialsMutation = useMutation({
    mutationFn: async (payload: { platform: 'google' | 'meta' | 'linkedin'; body: Record<string, string> }) =>
      apiSendJson('/api/admin/datasource-config', 'POST', { platform: payload.platform, ...payload.body }, { fallbackMessage: 'Failed to save datasource credentials' }),
    onSuccess: async () => {
      setOauthToast('Provider credentials updated.')
      setCredentialsModal({ open: false, provider: null })
      await queryClient.invalidateQueries({ queryKey: ['oauth-connections'] })
      await queryClient.invalidateQueries({ queryKey: ['legacy-datasource-connections'] })
    },
    onError: (e) => setOauthToast((e as Error).message || 'Failed to save provider credentials'),
  })

  const readiness = useMemo(() => {
    const j = journeysQuery.data || {}
    const validation = validationQuery.data || { error_count: 0, warn_count: 0 }
    return [
      {
        key: 'journeys',
        label: 'Journeys',
        value: (j.count || 0).toLocaleString(),
        tone: j.loaded ? t.color.success : t.color.warning,
        onClick: () => setIngestionMethod('sample'),
      },
      {
        key: 'coverage',
        label: 'Spend coverage',
        value: j.loaded ? 'Tracked' : 'Not ready',
        tone: j.loaded ? t.color.success : t.color.textMuted,
        onClick: () => setSystemsTab('ad_platforms'),
      },
      {
        key: 'freshness',
        label: 'Freshness',
        value: j.data_freshness_hours == null ? '—' : `${Math.round(j.data_freshness_hours)}h`,
        tone: (j.data_freshness_hours ?? 9999) <= 24 ? t.color.success : t.color.warning,
        onClick: () => setSystemsTab('cdp'),
      },
      {
        key: 'validation',
        label: 'Validation issues',
        value: `${validation.error_count || 0} errors / ${validation.warn_count || 0} warnings`,
        tone: (validation.error_count || 0) > 0 ? t.color.danger : (validation.warn_count || 0) > 0 ? t.color.warning : t.color.success,
        onClick: () => setShowPreviewValidation(true),
      },
    ]
  }, [journeysQuery.data, validationQuery.data])

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const providerParam = (params.get('oauth_provider') || '').trim() as OAuthProviderKey
    const statusParam = (params.get('oauth_status') || '').trim()
    const errorParam = (params.get('oauth_error') || '').trim()
    const messageParam = (params.get('oauth_message') || '').trim()
    if (!providerParam) return
    if (statusParam === 'connected') {
      setOauthToast(`Connected to ${providerParam.replace('_', ' ')}.`)
      const providerRecord = (oauthConnectionsQuery.data?.items || []).find((i) => i.provider_key === providerParam)
      setProviderModal({
        provider: providerParam,
        displayName: providerRecord?.display_name || providerParam.replace('_', ' '),
      })
      setSelectedProviderAccounts((providerRecord?.selected_accounts || []).map((id) => String(id)))
    } else if (statusParam === 'error' || errorParam) {
      setOauthToast(`Connection issue: ${decodeURIComponent(messageParam || errorParam || 'OAuth failed')}`)
    }
    params.delete('oauth_provider')
    params.delete('oauth_status')
    params.delete('oauth_error')
    params.delete('oauth_message')
    params.delete('accounts')
    const next = params.toString()
    const url = `${window.location.pathname}${next ? `?${next}` : ''}`
    window.history.replaceState(null, '', url)
  }, [oauthConnectionsQuery.data?.items])

  const inventoryItems = inventoryQuery.data?.items ?? []
  const warehouseItems = inventoryItems.filter((i) => i.category === 'warehouse')
  const oauthConnections = oauthConnectionsQuery.data?.items ?? []
  const legacyRows = (legacyConnectionsQuery.data?.connections || [])
    .filter((c) => ['google', 'meta', 'linkedin'].includes(String(c.id)))
    .map((c) => {
      const map: Record<string, OAuthProviderKey> = {
        google: 'google_ads',
        meta: 'meta_ads',
        linkedin: 'linkedin_ads',
      }
      const providerKey = map[String(c.id)] || 'google_ads'
      return {
        provider_key: providerKey,
        display_name: c.label || providerKey,
        status: (c.status || 'not_connected') as OAuthConnectionRecord['status'],
        selected_accounts_count: Number(c.selected_accounts_count || 0),
        last_tested_at: c.last_tested_at || null,
        last_error: c.last_error || null,
        selected_accounts: [] as string[],
        can_start: c.status !== 'needs_attention',
        missing_config_keys: [] as string[],
        missing_config_reason: c.status === 'needs_attention' ? 'Provider credentials are not configured.' : null,
      }
    })
  const effectiveOauthRows =
    oauthConnections.length > 0 ? oauthConnections : legacyRows.length > 0 ? legacyRows : DEFAULT_AD_PROVIDER_ROWS

  const adPlatformItems = effectiveOauthRows.map((c: any) => ({
    id: c.id || c.provider_key,
    provider_key: c.provider_key,
    name: c.display_name,
    type: c.provider_key,
    category: 'ad_platform',
    status: c.status || 'not_connected',
    last_tested_at: c.last_tested_at || null,
    selected_accounts_count: c.selected_accounts_count || 0,
    note: c.last_error || c.missing_config_reason || `${c.selected_accounts_count || 0} selected account(s)`,
    selected_accounts: c.selected_accounts || [],
    can_start: c.can_start !== false,
    missing_config_reason: c.missing_config_reason || null,
    missing_config_keys: Array.isArray(c.missing_config_keys) ? c.missing_config_keys : [],
  }))

  const cdpItems = [
    {
      id: 'meiro',
      name: 'Meiro CDP',
      type: 'meiro',
      category: 'cdp',
      status: meiroConfigQuery.data?.connected ? 'connected' : 'not_connected',
      last_tested_at: meiroConfigQuery.data?.last_test_at || null,
      note: meiroConfigQuery.data?.api_base_url || 'Configure endpoint and key',
    },
  ]

  const activeSystemsItems = systemsTab === 'warehouses' ? warehouseItems : systemsTab === 'ad_platforms' ? adPlatformItems : cdpItems

  const activeDrawerWarehouse = drawerConnector ? warehouseItems.find((w) => w.id === drawerConnector.id) : null
  const activeDrawerAdPlatform = drawerConnector
    ? adPlatformItems.find((p: any) => p.id === drawerConnector.id || p.provider_key === drawerConnector.type || p.type === drawerConnector.type)
    : null

  const openCredentialsModal = (provider: OAuthProviderKey) => {
    setCredentialFields({
      client_id: '',
      client_secret: '',
      developer_token: '',
      app_id: '',
      app_secret: '',
    })
    setCredentialsModal({ open: true, provider })
  }

  return (
    <>
      <DashboardPage
        title="Data Sources"
        description="Data Ops control-plane for journeys ingestion and connected systems."
      >
        <div style={{ display: 'grid', gap: t.space.xl }}>
          {oauthToast && (
            <SectionCard title="Connection update">
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center' }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{oauthToast}</div>
                <button
                  type="button"
                  onClick={() => setOauthToast(null)}
                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                >
                  Dismiss
                </button>
              </div>
            </SectionCard>
          )}
          <SectionCard title="Data readiness" subtitle="Click any KPI to jump to the relevant area.">
            <div style={{ display: 'grid', gap: t.space.sm, gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))' }}>
              {readiness.map((kpi) => (
                <button
                  key={kpi.key}
                  type="button"
                  onClick={kpi.onClick}
                  style={{
                    textAlign: 'left',
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    background: t.color.surface,
                    padding: t.space.md,
                    cursor: 'pointer',
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{kpi.label}</div>
                  <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold, color: kpi.tone }}>{kpi.value}</div>
                </button>
              ))}
            </div>
          </SectionCard>

          <SectionCard
            title="Journeys ingestion"
            subtitle="Use one ingestion method at a time; each import replaces existing journey data."
            actions={
              <button
                type="button"
                onClick={() => setShowImportLog((s) => !s)}
                style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
              >
                Import log
              </button>
            }
          >
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', marginBottom: t.space.md }}>
              {([
                { id: 'sample', label: 'Sample data' },
                { id: 'upload', label: 'Upload JSON' },
                { id: 'meiro', label: 'Import from Meiro' },
              ] as Array<{ id: IngestionMethod; label: string }>).map((opt) => (
                <button
                  key={opt.id}
                  type="button"
                  onClick={() => setIngestionMethod(opt.id)}
                  style={{
                    border: `1px solid ${ingestionMethod === opt.id ? t.color.accent : t.color.borderLight}`,
                    background: ingestionMethod === opt.id ? t.color.accentMuted : t.color.surface,
                    color: ingestionMethod === opt.id ? t.color.accent : t.color.text,
                    borderRadius: t.radius.full,
                    padding: '6px 12px',
                    cursor: 'pointer',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  {opt.label}
                </button>
              ))}
            </div>

            {ingestionMethod === 'sample' && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Prerequisites: none. Loads platform sample journeys for quick evaluation.</div>
                <button
                  type="button"
                  onClick={() => {
                    if (window.confirm('This import replaces existing journeys. Continue?')) loadSampleMutation.mutate()
                  }}
                  disabled={loadSampleMutation.isPending}
                  style={{ alignSelf: 'flex-start', border: `1px solid ${t.color.success}`, background: t.color.success, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: loadSampleMutation.isPending ? 'wait' : 'pointer' }}
                >
                  {loadSampleMutation.isPending ? 'Loading…' : 'Load sample data'}
                </button>
              </div>
            )}

            {ingestionMethod === 'upload' && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Prerequisites: valid JSON envelope with `schema_version=2.0` and `journeys` array.</div>
                <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                  <button
                    type="button"
                    onClick={() => fileRef.current?.click()}
                    disabled={uploadMutation.isPending}
                    style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: uploadMutation.isPending ? 'wait' : 'pointer' }}
                  >
                    {uploadMutation.isPending ? 'Uploading…' : 'Upload JSON'}
                  </button>
                  <button
                    type="button"
                    onClick={() => setShowJsonHelp((s) => !s)}
                    style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}
                  >
                    {showJsonHelp ? 'Hide format help' : 'Show format help'}
                  </button>
                </div>
                {showJsonHelp && (
                  <pre style={{ margin: 0, overflowX: 'auto', background: t.color.bg, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs }}>{JSON_FORMAT_V2}</pre>
                )}
                {uploadMutation.isError && <div style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>{(uploadMutation.error as Error).message}</div>}
              </div>
            )}

            {ingestionMethod === 'meiro' && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Prerequisites: configure Meiro connector in Connected systems → CDP / Sources.</div>
                <button
                  type="button"
                  onClick={() => {
                    if (!meiroConfigQuery.data?.connected) {
                      setSystemsTab('cdp')
                      return
                    }
                    if (window.confirm('This import replaces existing journeys. Continue?')) importFromMeiroMutation.mutate()
                  }}
                  disabled={importFromMeiroMutation.isPending}
                  style={{ alignSelf: 'flex-start', border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: importFromMeiroMutation.isPending ? 'wait' : 'pointer' }}
                >
                  {importFromMeiroMutation.isPending ? 'Importing…' : meiroConfigQuery.data?.connected ? 'Import from Meiro' : 'Configure Meiro first'}
                </button>
                {importFromMeiroMutation.isError && <div style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>{(importFromMeiroMutation.error as Error).message}</div>}
              </div>
            )}

            {showImportLog && (
              <div style={{ marginTop: t.space.md }}>
                <DashboardTable density="compact">
                  <thead>
                    <tr>
                      <th>Started</th>
                      <th>Source</th>
                      <th>Status</th>
                      <th>Rows</th>
                      <th>Note</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(importLogQuery.data?.runs ?? []).slice(0, 10).map((run: any) => (
                      <tr key={run.id}>
                        <td>{relativeTime(run.started_at || run.created_at)}</td>
                        <td>{run.source || '—'}</td>
                        <td>{statusBadge(run.status || 'not_connected')}</td>
                        <td>{run.valid ?? run.count ?? 0}</td>
                        <td style={{ color: t.color.textSecondary }}>{run.error || '—'}</td>
                      </tr>
                    ))}
                    {(importLogQuery.data?.runs ?? []).length === 0 && (
                      <tr>
                        <td colSpan={5} style={{ color: t.color.textSecondary, textAlign: 'center' }}>No import runs yet.</td>
                      </tr>
                    )}
                  </tbody>
                </DashboardTable>
              </div>
            )}

            <input
              ref={fileRef}
              type="file"
              accept=".json"
              style={{ display: 'none' }}
              onChange={(e) => {
                const f = e.target.files?.[0]
                if (f) uploadMutation.mutate(f)
                e.target.value = ''
              }}
            />
          </SectionCard>

          <SectionCard
            title="Connected systems"
            subtitle="Connection inventory across warehouses, ad platforms, and CDP/sources."
            actions={
              systemsTab === 'warehouses' ? (
                <button
                  type="button"
                  onClick={() => {
                    setWizardOpen(true)
                    setWizardStep(1)
                    setWarehouseDraft(defaultWarehouseDraft('bigquery'))
                    setWarehouseTestResult(null)
                    setWizardError(null)
                  }}
                  style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}
                >
                  Connect warehouse
                </button>
              ) : systemsTab === 'ad_platforms' ? (
                <button
                  type="button"
                  onClick={() => setConnectPicker({ open: true })}
                  disabled={!canManageSettings}
                  title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                  style={{
                    border: `1px solid ${canManageSettings ? t.color.accent : t.color.border}`,
                    background: canManageSettings ? t.color.accent : t.color.surfaceMuted,
                    color: canManageSettings ? '#fff' : t.color.textMuted,
                    borderRadius: t.radius.sm,
                    padding: '8px 12px',
                    cursor: canManageSettings ? 'pointer' : 'not-allowed',
                  }}
                >
                  Connect new source
                </button>
              ) : null
            }
          >
            {systemsTab === 'ad_platforms' && oauthConnectionsQuery.isError && (
              <div style={{ marginBottom: t.space.md, padding: t.space.sm, border: `1px solid ${t.color.warning}`, borderRadius: t.radius.sm, background: t.color.warningSubtle, color: t.color.warning, fontSize: t.font.sizeSm }}>
                OAuth connection API is unavailable in this environment. Showing fallback provider rows so you can still start connections.
              </div>
            )}
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', marginBottom: t.space.md }}>
              {([
                { id: 'warehouses', label: 'Warehouses' },
                { id: 'ad_platforms', label: 'Ad platforms' },
                { id: 'cdp', label: 'CDP / Sources' },
              ] as Array<{ id: SystemsTab; label: string }>).map((tab) => (
                <button
                  key={tab.id}
                  type="button"
                  onClick={() => setSystemsTab(tab.id)}
                  style={{
                    border: `1px solid ${systemsTab === tab.id ? t.color.accent : t.color.borderLight}`,
                    background: systemsTab === tab.id ? t.color.accentMuted : t.color.surface,
                    color: systemsTab === tab.id ? t.color.accent : t.color.text,
                    borderRadius: t.radius.full,
                    padding: '6px 12px',
                    cursor: 'pointer',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  {tab.label}
                </button>
              ))}
            </div>

            <DashboardTable density="compact">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Last tested/synced</th>
                  <th>Note</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {activeSystemsItems.map((item: any) => (
                  <tr key={item.id}>
                    <td>{item.name}</td>
                    <td>{item.type}</td>
                    <td>{statusBadge(item.status || 'not_connected')}</td>
                    <td>{relativeTime(item.last_tested_at)}</td>
                    <td style={{ color: t.color.textSecondary }}>{item.note || item.last_error || '—'}</td>
                    <td>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        {item.category === 'ad_platform' && (
                          <>
                            {(item.status === 'not_connected' || item.status === 'disabled') && (
                              <button
                                type="button"
                                onClick={() => startOAuthMutation.mutate(item.provider_key as OAuthProviderKey)}
                                disabled={!!disabledReason(canManageSettings, item)}
                                title={disabledReason(canManageSettings, item)}
                                style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '4px 8px', cursor: disabledReason(canManageSettings, item) ? 'not-allowed' : 'pointer', opacity: disabledReason(canManageSettings, item) ? 0.7 : 1, fontSize: t.font.sizeXs }}
                              >
                                Connect
                              </button>
                            )}
                            {(item.status === 'needs_reauth' || item.status === 'error') && (
                              <button
                                type="button"
                                onClick={() => reauthOAuthMutation.mutate(item.provider_key as OAuthProviderKey)}
                                disabled={!!disabledReason(canManageSettings, item)}
                                title={disabledReason(canManageSettings, item)}
                                style={{ border: `1px solid ${t.color.warning}`, background: t.color.warningSubtle, color: t.color.warning, borderRadius: t.radius.sm, padding: '4px 8px', cursor: disabledReason(canManageSettings, item) ? 'not-allowed' : 'pointer', opacity: disabledReason(canManageSettings, item) ? 0.7 : 1, fontSize: t.font.sizeXs }}
                              >
                                Reconnect
                              </button>
                            )}
                            {item.status === 'connected' && (
                              <>
                                <button
                                  type="button"
                                  onClick={() => {
                                    setProviderModal({ provider: item.provider_key as OAuthProviderKey, displayName: item.name })
                                    setSelectedProviderAccounts((item.selected_accounts || []).map((id: string) => String(id)))
                                  }}
                                  disabled={!canManageSettings}
                                  title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                                >
                                  Select accounts
                                </button>
                                <button
                                  type="button"
                                  onClick={() => testOAuthMutation.mutate(item.provider_key as OAuthProviderKey)}
                                  disabled={!canManageSettings}
                                  title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                                >
                                  Test
                                </button>
                                <button
                                  type="button"
                                  onClick={() => disconnectOAuthMutation.mutate(item.provider_key as OAuthProviderKey)}
                                  disabled={!canManageSettings}
                                  title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                                  style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerMuted, color: t.color.danger, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                                >
                                  Disconnect
                                </button>
                              </>
                            )}
                          </>
                        )}
                        <button
                          type="button"
                          onClick={() => {
                            setDrawerConnector({ id: item.id, name: item.name, type: item.type, category: item.category })
                            setDrawerOpen(true)
                          }}
                          style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                        >
                          Configure
                        </button>
                        {item.category === 'warehouse' && (
                          <button
                            type="button"
                            onClick={() => testSavedMutation.mutate(item.id)}
                            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '4px 8px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                          >
                            Test
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
                {systemsTab === 'ad_platforms' && (oauthConnectionsQuery.isLoading || legacyConnectionsQuery.isLoading) && (
                  <tr>
                    <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary }}>Loading providers…</td>
                  </tr>
                )}
                {activeSystemsItems.length === 0 && !(systemsTab === 'ad_platforms' && (oauthConnectionsQuery.isLoading || legacyConnectionsQuery.isLoading)) && (
                  <tr>
                    <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary }}>No systems in this category.</td>
                  </tr>
                )}
              </tbody>
            </DashboardTable>
          </SectionCard>

          <SectionCard
            title="Data preview & validation"
            subtitle="Use this section only when you need a quick sanity check."
            actions={
              <div style={{ display: 'flex', gap: t.space.sm }}>
                <a href="/#data-quality" style={{ color: t.color.accent, fontSize: t.font.sizeSm, textDecoration: 'none' }}>Open Data quality</a>
                <button type="button" onClick={() => setShowPreviewValidation((s) => !s)} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 10px', cursor: 'pointer' }}>{showPreviewValidation ? 'Collapse' : 'Expand'}</button>
              </div>
            }
          >
            {showPreviewValidation ? (
              <DashboardTable density="compact">
                <thead>
                  <tr>
                    {(previewQuery.data?.columns ?? []).map((col: string) => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(previewQuery.data?.rows ?? []).slice(0, 12).map((row: any, idx: number) => (
                    <tr key={idx}>
                      {(previewQuery.data?.columns ?? []).map((col: string) => (
                        <td key={col}>{String(row?.[col] ?? '—')}</td>
                      ))}
                    </tr>
                  ))}
                  {(previewQuery.data?.rows ?? []).length === 0 && (
                    <tr>
                      <td style={{ color: t.color.textSecondary }}>No preview rows available.</td>
                    </tr>
                  )}
                </tbody>
              </DashboardTable>
            ) : (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Collapsed by default to reduce noise.</div>
            )}
          </SectionCard>
        </div>
      </DashboardPage>

      {connectPicker.open && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 61, background: 'rgba(15,23,42,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
          <div style={{ width: 'min(680px, 100%)', maxHeight: '90vh', overflow: 'auto', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
            <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Connect new source</h3>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Select a provider and start OAuth connection.
            </div>
            <div style={{ display: 'grid', gap: t.space.sm }}>
              {adPlatformItems.map((item: any) => {
                const reason = disabledReason(canManageSettings, item)
                const connecting = startOAuthMutation.isPending && startOAuthMutation.variables === item.provider_key
                return (
                  <div key={item.provider_key} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.sm, display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                    <div style={{ display: 'grid', gap: 2 }}>
                      <div style={{ fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.name}</div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        {item.missing_config_reason || `${item.selected_accounts_count || 0} selected account(s)`}
                      </div>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: t.space.xs }}>
                      {statusBadge(item.status || 'not_connected')}
                      {!!item.missing_config_reason && (
                        <button
                          type="button"
                          onClick={() => openCredentialsModal(item.provider_key as OAuthProviderKey)}
                          disabled={!canManageSettings}
                          title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                          style={{
                            border: `1px solid ${t.color.border}`,
                            background: t.color.surface,
                            borderRadius: t.radius.sm,
                            padding: '6px 10px',
                            cursor: !canManageSettings ? 'not-allowed' : 'pointer',
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          Configure credentials
                        </button>
                      )}
                      <button
                        type="button"
                        onClick={() => startOAuthMutation.mutate(item.provider_key as OAuthProviderKey)}
                        disabled={!!reason || connecting}
                        title={reason}
                        style={{
                          border: `1px solid ${t.color.accent}`,
                          background: t.color.accent,
                          color: '#fff',
                          borderRadius: t.radius.sm,
                          padding: '6px 10px',
                          cursor: !!reason || connecting ? 'not-allowed' : 'pointer',
                          fontSize: t.font.sizeXs,
                          opacity: !!reason || connecting ? 0.7 : 1,
                        }}
                      >
                        {connecting ? 'Redirecting…' : 'Connect'}
                      </button>
                    </div>
                  </div>
                )
              })}
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
              <button
                type="button"
                onClick={() => setConnectPicker({ open: false })}
                style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {credentialsModal.open && credentialsModal.provider && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 62, background: 'rgba(15,23,42,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
          <div style={{ width: 'min(680px, 100%)', maxHeight: '90vh', overflow: 'auto', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
            <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>
              Configure credentials: {credentialsModal.provider === 'google_ads' ? 'Google Ads' : credentialsModal.provider === 'meta_ads' ? 'Meta Ads' : 'LinkedIn Ads'}
            </h3>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Credentials are stored server-side only and never exposed to the UI after save.
            </div>

            {credentialsModal.provider === 'google_ads' && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Client ID
                  <input value={credentialFields.client_id} onChange={(e) => setCredentialFields((p) => ({ ...p, client_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Client Secret
                  <input type="password" value={credentialFields.client_secret} onChange={(e) => setCredentialFields((p) => ({ ...p, client_secret: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Developer Token (required for account listing)
                  <input value={credentialFields.developer_token} onChange={(e) => setCredentialFields((p) => ({ ...p, developer_token: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
              </div>
            )}

            {credentialsModal.provider === 'meta_ads' && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  App ID
                  <input value={credentialFields.app_id} onChange={(e) => setCredentialFields((p) => ({ ...p, app_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  App Secret
                  <input type="password" value={credentialFields.app_secret} onChange={(e) => setCredentialFields((p) => ({ ...p, app_secret: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
              </div>
            )}

            {credentialsModal.provider === 'linkedin_ads' && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Client ID
                  <input value={credentialFields.client_id} onChange={(e) => setCredentialFields((p) => ({ ...p, client_id: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Client Secret
                  <input type="password" value={credentialFields.client_secret} onChange={(e) => setCredentialFields((p) => ({ ...p, client_secret: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
              </div>
            )}

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
              <button
                type="button"
                onClick={() => setCredentialsModal({ open: false, provider: null })}
                style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={saveProviderCredentialsMutation.isPending || !canManageSettings}
                onClick={() => {
                  const provider = credentialsModal.provider as OAuthProviderKey
                  const platform = providerToPlatform(provider)
                  const body: Record<string, string> = {}
                  if (provider === 'google_ads') {
                    if (credentialFields.client_id.trim()) body.client_id = credentialFields.client_id.trim()
                    if (credentialFields.client_secret.trim()) body.client_secret = credentialFields.client_secret.trim()
                    if (credentialFields.developer_token.trim()) body.developer_token = credentialFields.developer_token.trim()
                  } else if (provider === 'meta_ads') {
                    if (credentialFields.app_id.trim()) body.app_id = credentialFields.app_id.trim()
                    if (credentialFields.app_secret.trim()) body.app_secret = credentialFields.app_secret.trim()
                  } else {
                    if (credentialFields.client_id.trim()) body.client_id = credentialFields.client_id.trim()
                    if (credentialFields.client_secret.trim()) body.client_secret = credentialFields.client_secret.trim()
                  }
                  if (Object.keys(body).length === 0) {
                    setOauthToast('Enter at least one credential field to update.')
                    return
                  }
                  saveProviderCredentialsMutation.mutate({ platform, body })
                }}
                title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: !canManageSettings ? 'not-allowed' : 'pointer' }}
              >
                {saveProviderCredentialsMutation.isPending ? 'Saving…' : 'Save credentials'}
              </button>
            </div>
          </div>
        </div>
      )}

      {providerModal && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 61, background: 'rgba(15,23,42,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
          <div style={{ width: 'min(680px, 100%)', maxHeight: '90vh', overflow: 'auto', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
            <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Select accounts for {providerModal.displayName}</h3>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Choose which ad accounts this workspace should use for sync and health checks.
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.sm, maxHeight: 320, overflow: 'auto' }}>
              {providerAccountsQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading accounts…</div>}
              {providerAccountsQuery.isError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(providerAccountsQuery.error as Error).message}</div>}
              {!providerAccountsQuery.isLoading && !providerAccountsQuery.isError && (providerAccountsQuery.data?.accounts || []).length === 0 && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No accounts returned by provider.</div>
              )}
              {(providerAccountsQuery.data?.accounts || []).map((acc) => {
                const checked = selectedProviderAccounts.includes(String(acc.id))
                return (
                  <label key={String(acc.id)} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 4px', fontSize: t.font.sizeSm }}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(e) => {
                        const id = String(acc.id)
                        setSelectedProviderAccounts((prev) => e.target.checked ? Array.from(new Set([...prev, id])) : prev.filter((v) => v !== id))
                      }}
                    />
                    <span style={{ color: t.color.text, fontWeight: t.font.weightMedium }}>{acc.name || acc.id}</span>
                    <span style={{ color: t.color.textMuted }}>({acc.id})</span>
                  </label>
                )
              })}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap' }}>
              <button
                type="button"
                onClick={() => testOAuthMutation.mutate(providerModal.provider)}
                style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
              >
                Test connection
              </button>
              <div style={{ display: 'flex', gap: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => {
                    setProviderModal(null)
                    setSelectedProviderAccounts([])
                  }}
                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => selectAccountsMutation.mutate({ provider: providerModal.provider, accountIds: selectedProviderAccounts })}
                  disabled={selectAccountsMutation.isPending}
                  style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: selectAccountsMutation.isPending ? 'wait' : 'pointer' }}
                >
                  {selectAccountsMutation.isPending ? 'Saving…' : 'Save selection'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {wizardOpen && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 60, background: 'rgba(15,23,42,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
          <div style={{ width: 'min(760px, 100%)', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
            <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Connect warehouse</h3>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Step {wizardStep} of 4</div>

            {wizardStep === 1 && (
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md }}>
                {([
                  { id: 'bigquery' as const, label: 'BigQuery', note: 'Service account JSON' },
                  { id: 'snowflake' as const, label: 'Snowflake', note: 'Key-pair auth' },
                ]).map((opt) => (
                  <button
                    key={opt.id}
                    type="button"
                    onClick={() => setWarehouseDraft((p) => ({ ...defaultWarehouseDraft(opt.id), name: p.name }))}
                    style={{
                      textAlign: 'left',
                      border: `1px solid ${warehouseDraft.type === opt.id ? t.color.accent : t.color.borderLight}`,
                      background: warehouseDraft.type === opt.id ? t.color.accentMuted : t.color.surface,
                      borderRadius: t.radius.md,
                      padding: t.space.md,
                      cursor: 'pointer',
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold, color: t.color.text }}>{opt.label}</div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{opt.note}</div>
                  </button>
                ))}
              </div>
            )}

            {wizardStep === 2 && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Connection name
                  <input value={warehouseDraft.name} onChange={(e) => setWarehouseDraft((p) => ({ ...p, name: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                {warehouseDraft.type === 'bigquery' ? (
                  <>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Service account JSON
                      <textarea value={warehouseDraft.serviceAccountJson} onChange={(e) => {
                        const val = e.target.value
                        setWarehouseDraft((p) => ({ ...p, serviceAccountJson: val }))
                        try {
                          const parsed = JSON.parse(val)
                          if (parsed?.project_id) setWarehouseDraft((p) => ({ ...p, projectId: p.projectId || parsed.project_id }))
                        } catch {
                          // ignore parse errors while typing
                        }
                      }} rows={8} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Project ID
                      <input value={warehouseDraft.projectId} onChange={(e) => setWarehouseDraft((p) => ({ ...p, projectId: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Default dataset (optional)
                      <input value={warehouseDraft.defaultDataset} onChange={(e) => setWarehouseDraft((p) => ({ ...p, defaultDataset: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                  </>
                ) : (
                  <>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Account identifier
                      <input value={warehouseDraft.account} onChange={(e) => setWarehouseDraft((p) => ({ ...p, account: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Username
                      <input value={warehouseDraft.username} onChange={(e) => setWarehouseDraft((p) => ({ ...p, username: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Private key PEM
                      <textarea value={warehouseDraft.privateKeyPem} onChange={(e) => setWarehouseDraft((p) => ({ ...p, privateKeyPem: e.target.value }))} rows={6} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Passphrase (optional)
                      <input type="password" value={warehouseDraft.passphrase} onChange={(e) => setWarehouseDraft((p) => ({ ...p, passphrase: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Warehouse
                      <input value={warehouseDraft.warehouse} onChange={(e) => setWarehouseDraft((p) => ({ ...p, warehouse: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Role (optional)
                      <input value={warehouseDraft.role} onChange={(e) => setWarehouseDraft((p) => ({ ...p, role: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Default database (optional)
                      <input value={warehouseDraft.defaultDatabase} onChange={(e) => setWarehouseDraft((p) => ({ ...p, defaultDatabase: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Default schema (optional)
                      <input value={warehouseDraft.defaultSchema} onChange={(e) => setWarehouseDraft((p) => ({ ...p, defaultSchema: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                  </>
                )}
              </div>
            )}

            {wizardStep === 3 && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => {
                    setWizardError(null)
                    setWarehouseTestResult(null)
                    testWarehouseMutation.mutate()
                  }}
                  disabled={testWarehouseMutation.isPending}
                  style={{ alignSelf: 'flex-start', border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: testWarehouseMutation.isPending ? 'wait' : 'pointer' }}
                >
                  {testWarehouseMutation.isPending ? 'Testing…' : 'Run test connection'}
                </button>
                {warehouseTestResult && (
                  <div style={{ border: `1px solid ${warehouseTestResult.ok ? t.color.success : t.color.danger}`, background: warehouseTestResult.ok ? t.color.successMuted : t.color.dangerMuted, color: warehouseTestResult.ok ? t.color.success : t.color.danger, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeSm }}>
                    {warehouseTestResult.message} · {warehouseTestResult.latency_ms}ms
                    <div style={{ marginTop: 6, color: t.color.textSecondary }}>
                      Namespaces: {(warehouseTestResult.details?.sample_namespaces || []).join(', ') || '—'}
                    </div>
                  </div>
                )}
              </div>
            )}

            {wizardStep === 4 && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Save this connection to Connected systems inventory.
              </div>
            )}

            {wizardError && <div style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>{wizardError}</div>}

            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm }}>
              <button type="button" onClick={() => setWizardOpen(false)} style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>Cancel</button>
              <div style={{ display: 'flex', gap: t.space.sm }}>
                {wizardStep > 1 && <button type="button" onClick={() => setWizardStep(wizardStep === 4 ? 3 : wizardStep === 3 ? 2 : 1)} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>Back</button>}
                {wizardStep < 4 && <button type="button" onClick={() => setWizardStep(wizardStep === 1 ? 2 : wizardStep === 2 ? 3 : 4)} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>Next</button>}
                {wizardStep === 4 && <button type="button" onClick={() => createWarehouseMutation.mutate()} disabled={createWarehouseMutation.isPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', cursor: createWarehouseMutation.isPending ? 'wait' : 'pointer' }}>{createWarehouseMutation.isPending ? 'Saving…' : 'Save connection'}</button>}
              </div>
            </div>
          </div>
        </div>
      )}

      {drawerOpen && drawerConnector && (
        <>
          <div role="presentation" onClick={() => setDrawerOpen(false)} style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.35)', zIndex: 58 }} />
          <div role="dialog" aria-label={`Configure ${drawerConnector.name}`} style={{ position: 'fixed', right: 0, top: 0, width: 'min(560px, 100vw)', height: '100vh', background: t.color.surface, borderLeft: `1px solid ${t.color.borderLight}`, zIndex: 59, display: 'grid', gridTemplateRows: 'auto 1fr auto' }}>
            <div style={{ padding: t.space.lg, borderBottom: `1px solid ${t.color.borderLight}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Configure {drawerConnector.name}</h3>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{drawerConnector.type}</div>
              </div>
              <button type="button" onClick={() => setDrawerOpen(false)} style={{ border: 'none', background: 'transparent', fontSize: t.font.sizeLg, cursor: 'pointer' }}>×</button>
            </div>

            <div style={{ overflowY: 'auto', padding: t.space.lg, display: 'grid', gap: t.space.md }}>
              {drawerConnector.type === 'meiro' ? (
                <>
                  <div
                    style={{
                      display: 'inline-flex',
                      gap: t.space.xs,
                      flexWrap: 'wrap',
                      padding: t.space.xs,
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      background: t.color.bgSubtle,
                    }}
                  >
                    {(
                      [
                        { id: 'connection', label: 'Connection' },
                        { id: 'pull', label: 'Pull' },
                        { id: 'mapping', label: 'Mapping' },
                      ] as const
                    ).map((tab) => (
                      <button
                        key={tab.id}
                        type="button"
                        onClick={() => setMeiroTab(tab.id)}
                        style={{
                          border: `1px solid ${meiroTab === tab.id ? t.color.accent : 'transparent'}`,
                          background: meiroTab === tab.id ? t.color.accentMuted : 'transparent',
                          color: meiroTab === tab.id ? t.color.accent : t.color.textSecondary,
                          borderRadius: t.radius.sm,
                          padding: '6px 12px',
                          cursor: 'pointer',
                          fontSize: t.font.sizeSm,
                          fontWeight: meiroTab === tab.id ? t.font.weightSemibold : t.font.weightMedium,
                        }}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </div>

                  {meiroTab === 'connection' && (
                    <div style={{ display: 'grid', gap: t.space.md }}>
                      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Connection</div>
                        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                          API base URL
                          <input value={meiroUrl || meiroConfigQuery.data?.api_base_url || ''} onChange={(e) => setMeiroUrl(e.target.value)} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                        </label>
                        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                          API key
                          <input type="password" value={meiroKey} onChange={(e) => setMeiroKey(e.target.value)} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                        </label>
                        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                          <button type="button" onClick={() => testMeiroMutation.mutate()} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Test</button>
                          <button type="button" onClick={() => connectMeiroMutation.mutate()} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Save</button>
                          <button type="button" onClick={() => disconnectMeiroMutation.mutate()} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Disconnect</button>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Status: {meiroConfigQuery.data?.connected ? 'Connected' : 'Not connected'} · Last test: {relativeTime(meiroConfigQuery.data?.last_test_at)}
                        </div>
                        {testMeiroMutation.data && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{testMeiroMutation.data.message || 'Test completed'}</div>}
                      </div>

                      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Webhook ingestion</div>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                          Send Meiro profile/conversion events to this endpoint:
                        </div>
                        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
                          <code style={{ display: 'block', padding: '6px 8px', borderRadius: t.radius.sm, background: t.color.surface, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeXs }}>
                            {meiroConfigQuery.data?.webhook_url || 'http://localhost:8000/api/connectors/meiro/profiles'}
                          </code>
                          <button
                            type="button"
                            onClick={async () => {
                              const val = meiroConfigQuery.data?.webhook_url || 'http://localhost:8000/api/connectors/meiro/profiles'
                              try {
                                await navigator.clipboard.writeText(val)
                                setOauthToast('Webhook URL copied.')
                              } catch {
                                setOauthToast(val)
                              }
                            }}
                            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 10px', cursor: 'pointer', fontSize: t.font.sizeXs }}
                          >
                            Copy URL
                          </button>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Received: {meiroConfigQuery.data?.webhook_received_count ?? 0} · Last received: {relativeTime(meiroConfigQuery.data?.webhook_last_received_at)}
                        </div>
                        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                          <button
                            type="button"
                            onClick={() => rotateWebhookSecretMutation.mutate()}
                            disabled={rotateWebhookSecretMutation.isPending}
                            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: rotateWebhookSecretMutation.isPending ? 'wait' : 'pointer' }}
                          >
                            {rotateWebhookSecretMutation.isPending ? 'Rotating…' : 'Rotate webhook secret'}
                          </button>
                        </div>
                        {webhookSecretValue && (
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                            New secret (shown once): <code>{webhookSecretValue}</code>
                          </div>
                        )}
                        <div style={{ marginTop: t.space.sm }}>
                          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: 6 }}>
                            Last webhook events (max 100)
                          </div>
                          {meiroWebhookEventsQuery.isLoading ? (
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading events…</div>
                          ) : meiroWebhookEventsQuery.isError ? (
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                              {(meiroWebhookEventsQuery.error as Error)?.message || 'Failed to load webhook events'}
                            </div>
                          ) : (
                            <DashboardTable density="compact">
                              <thead>
                                <tr>
                                  <th>Received</th>
                                  <th>Profiles</th>
                                  <th>Mode</th>
                                  <th>Stored total</th>
                                  <th>Source IP</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(meiroWebhookEventsQuery.data?.items || []).map((event: any, idx: number) => (
                                  <tr key={`${event.received_at || 'event'}-${idx}`}>
                                    <td>{relativeTime(event.received_at)}</td>
                                    <td>{Number(event.received_count || 0).toLocaleString()}</td>
                                    <td>{event.replace ? 'replace' : 'append'}</td>
                                    <td>{Number(event.stored_total || 0).toLocaleString()}</td>
                                    <td>{event.ip || '—'}</td>
                                  </tr>
                                ))}
                                {!(meiroWebhookEventsQuery.data?.items || []).length && (
                                  <tr>
                                    <td colSpan={5} style={{ textAlign: 'center', color: t.color.textSecondary }}>
                                      No webhook events received yet.
                                    </td>
                                  </tr>
                                )}
                              </tbody>
                            </DashboardTable>
                          )}
                        </div>
                      </div>
                    </div>
                  )}

                  {meiroTab === 'pull' && (
                    <div style={{ display: 'grid', gap: t.space.sm }}>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Saved pull config is editable in JSON for MVP.</div>
                      <textarea
                        defaultValue={JSON.stringify(meiroPullConfigQuery.data || {}, null, 2)}
                        rows={8}
                        onBlur={(e) => {
                          try {
                            const parsed = JSON.parse(e.target.value)
                            saveMeiroPullMutation.mutate(parsed)
                          } catch {
                            // ignore invalid json while editing
                          }
                        }}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontFamily: 'monospace' }}
                      />
                      <button type="button" onClick={() => runMeiroPullMutation.mutate()} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Run pull now</button>
                    </div>
                  )}

                  {meiroTab === 'mapping' && (
                    <div style={{ display: 'grid', gap: t.space.sm }}>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Field mapping editor (JSON).</div>
                      <textarea
                        defaultValue={JSON.stringify(meiroMappingQuery.data?.mapping || {}, null, 2)}
                        rows={10}
                        onBlur={(e) => {
                          try {
                            const parsed = JSON.parse(e.target.value)
                            saveMeiroMappingMutation.mutate(parsed)
                          } catch {
                            // ignore parse error in blur
                          }
                        }}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontFamily: 'monospace' }}
                      />
                    </div>
                  )}
                </>
              ) : drawerConnector.category === 'warehouse' && activeDrawerWarehouse ? (
                <>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Status: {statusBadge(activeDrawerWarehouse.status)} · Last tested: {relativeTime(activeDrawerWarehouse.last_tested_at)}
                  </div>
                  <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: t.font.sizeXs }}>{JSON.stringify(activeDrawerWarehouse.config_json || {}, null, 2)}</pre>
                  </div>
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Update credentials (paste JSON or PEM)
                      <textarea value={rotateSecretText} onChange={(e) => setRotateSecretText(e.target.value)} rows={5} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                    </label>
                  </div>
                </>
              ) : drawerConnector.category === 'ad_platform' && activeDrawerAdPlatform ? (
                <>
                  <div style={{ display: 'grid', gap: 4 }}>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      Status: {statusBadge(activeDrawerAdPlatform.status || 'not_connected')} · Last tested: {relativeTime(activeDrawerAdPlatform.last_tested_at)}
                    </div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      Selected accounts: {(activeDrawerAdPlatform.selected_accounts || []).length}
                    </div>
                    {activeDrawerAdPlatform.missing_config_reason && (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                        {activeDrawerAdPlatform.missing_config_reason}
                      </div>
                    )}
                    {activeDrawerAdPlatform.missing_config_reason && (
                      <div>
                        <button
                          type="button"
                          onClick={() => openCredentialsModal(activeDrawerAdPlatform.provider_key as OAuthProviderKey)}
                          disabled={!canManageSettings}
                          title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                          style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 10px', cursor: !canManageSettings ? 'not-allowed' : 'pointer', fontSize: t.font.sizeXs }}
                        >
                          Configure credentials
                        </button>
                      </div>
                    )}
                  </div>

                  {(activeDrawerAdPlatform.selected_accounts || []).length > 0 ? (
                    <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 6 }}>Selected account IDs</div>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        {(activeDrawerAdPlatform.selected_accounts || []).map((acc: string) => (
                          <span key={acc} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.full, padding: '2px 8px', fontSize: t.font.sizeXs }}>
                            {acc}
                          </span>
                        ))}
                      </div>
                    </div>
                  ) : (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      No accounts selected yet. Connect and choose accounts to enable scoped sync.
                    </div>
                  )}

                  <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                    <button
                      type="button"
                      onClick={() => {
                        setProviderModal({ provider: activeDrawerAdPlatform.provider_key as OAuthProviderKey, displayName: activeDrawerAdPlatform.name })
                        setSelectedProviderAccounts((activeDrawerAdPlatform.selected_accounts || []).map((id: string) => String(id)))
                      }}
                      disabled={!canManageSettings || activeDrawerAdPlatform.status !== 'connected'}
                      title={!canManageSettings ? 'You need Settings manage permission' : activeDrawerAdPlatform.status !== 'connected' ? 'Connect provider first' : undefined}
                      style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: !canManageSettings || activeDrawerAdPlatform.status !== 'connected' ? 'not-allowed' : 'pointer' }}
                    >
                      Manage account selection
                    </button>
                  </div>
                </>
              ) : (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Connector-specific configuration appears here.</div>
              )}
            </div>

            <div style={{ borderTop: `1px solid ${t.color.borderLight}`, padding: t.space.md, display: 'flex', gap: t.space.sm, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              {drawerConnector.category === 'warehouse' && (
                <>
                  <button type="button" onClick={() => testSavedMutation.mutate(drawerConnector.id)} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Test connection</button>
                  <button type="button" onClick={() => rotateMutation.mutate(drawerConnector.id)} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Update credentials</button>
                  <button type="button" onClick={() => disableMutation.mutate(drawerConnector.id)} style={{ border: `1px solid ${t.color.warning}`, background: t.color.warningSubtle, color: t.color.warning, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Disable</button>
                  <button type="button" onClick={() => { if (window.confirm('Delete this data source?')) deleteMutation.mutate(drawerConnector.id) }} style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerMuted, color: t.color.danger, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Delete</button>
                </>
              )}
              {drawerConnector.category === 'ad_platform' && activeDrawerAdPlatform && (
                <>
                  {(activeDrawerAdPlatform.status === 'not_connected' || activeDrawerAdPlatform.status === 'disabled') && (
                    <button
                      type="button"
                      onClick={() => startOAuthMutation.mutate(activeDrawerAdPlatform.provider_key as OAuthProviderKey)}
                      disabled={!!disabledReason(canManageSettings, activeDrawerAdPlatform)}
                      title={disabledReason(canManageSettings, activeDrawerAdPlatform)}
                      style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: disabledReason(canManageSettings, activeDrawerAdPlatform) ? 'not-allowed' : 'pointer', opacity: disabledReason(canManageSettings, activeDrawerAdPlatform) ? 0.7 : 1 }}
                    >
                      Connect
                    </button>
                  )}
                  {(activeDrawerAdPlatform.status === 'needs_reauth' || activeDrawerAdPlatform.status === 'error') && (
                    <button
                      type="button"
                      onClick={() => reauthOAuthMutation.mutate(activeDrawerAdPlatform.provider_key as OAuthProviderKey)}
                      disabled={!!disabledReason(canManageSettings, activeDrawerAdPlatform)}
                      title={disabledReason(canManageSettings, activeDrawerAdPlatform)}
                      style={{ border: `1px solid ${t.color.warning}`, background: t.color.warningSubtle, color: t.color.warning, borderRadius: t.radius.sm, padding: '8px 10px', cursor: disabledReason(canManageSettings, activeDrawerAdPlatform) ? 'not-allowed' : 'pointer', opacity: disabledReason(canManageSettings, activeDrawerAdPlatform) ? 0.7 : 1 }}
                    >
                      Reconnect
                    </button>
                  )}
                  {activeDrawerAdPlatform.status === 'connected' && (
                    <>
                      <button
                        type="button"
                        onClick={() => testOAuthMutation.mutate(activeDrawerAdPlatform.provider_key as OAuthProviderKey)}
                        disabled={!canManageSettings}
                        title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                        style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: !canManageSettings ? 'not-allowed' : 'pointer' }}
                      >
                        Test connection
                      </button>
                      <button
                        type="button"
                        onClick={() => disconnectOAuthMutation.mutate(activeDrawerAdPlatform.provider_key as OAuthProviderKey)}
                        disabled={!canManageSettings}
                        title={!canManageSettings ? 'You need Settings manage permission' : undefined}
                        style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerMuted, color: t.color.danger, borderRadius: t.radius.sm, padding: '8px 10px', cursor: !canManageSettings ? 'not-allowed' : 'pointer' }}
                      >
                        Disconnect
                      </button>
                    </>
                  )}
                </>
              )}
              <button type="button" onClick={() => setDrawerOpen(false)} style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Close</button>
            </div>
          </div>
        </>
      )}
    </>
  )
}
