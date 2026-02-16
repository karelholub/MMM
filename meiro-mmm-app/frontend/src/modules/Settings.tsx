import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from 'react'
import type { CSSProperties } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import JourneysSettingsSection from './JourneysSettingsSection'
import AccessControlUsersSection from './AccessControlUsersSection'
import AccessControlRolesSection from './AccessControlRolesSection'
import AccessControlAuditLogSection from './AccessControlAuditLogSection'
import RevenueKpiDefinitionCard from './RevenueKpiDefinitionCard'
import { usePermissions } from '../hooks/usePermissions'
import { apiGetJson, apiSendJson } from '../lib/apiClient'

export type SectionKey =
  | 'attribution'
  | 'kpi'
  | 'taxonomy'
  | 'measurement-models'
  | 'journeys'
  | 'access-control-users'
  | 'access-control-roles'
  | 'access-control-audit-log'
  | 'nba'
  | 'mmm'
  | 'notifications'

interface SectionMeta {
  title: string
  description: string
  icon: string
}

const SECTION_ORDER: SectionKey[] = [
  'attribution',
  'kpi',
  'taxonomy',
  'measurement-models',
  'journeys',
  'access-control-users',
  'access-control-roles',
  'access-control-audit-log',
  'nba',
  'mmm',
  'notifications',
]

const SECTION_META: Record<SectionKey, SectionMeta> = {
  attribution: {
    title: 'Attribution defaults',
    description:
      'Control default lookback, decay, and weighting applied across attribution models.',
    icon: 'AT',
  },
  kpi: {
    title: 'KPI & conversions',
    description:
      'Manage primary KPIs and micro conversions that drive reporting and optimization.',
    icon: 'KP',
  },
  taxonomy: {
    title: 'Taxonomy',
    description:
      'Standardize channels and campaign structure via rules and alias mappings.',
    icon: 'TX',
  },
  'measurement-models': {
    title: 'Measurement model configs',
    description:
      'Version, review, and activate measurement configs used in reporting and MMM.',
    icon: 'MM',
  },
  journeys: {
    title: 'Analytics → Journeys',
    description:
      'Version and validate defaults for journeys paths, flow, funnels, and diagnostics.',
    icon: 'JR',
  },
  'access-control-users': {
    title: 'Access Control → Users',
    description:
      'Manage workspace users, invitations, and membership roles.',
    icon: 'AC',
  },
  'access-control-roles': {
    title: 'Access Control → Roles',
    description:
      'Manage system and custom roles with permission bundles and access previews.',
    icon: 'RL',
  },
  'access-control-audit-log': {
    title: 'Access Control → Audit Log',
    description:
      'Review workspace security and administrative actions with filters and metadata details.',
    icon: 'AL',
  },
  nba: {
    title: 'NBA defaults',
    description:
      'Tune thresholds that govern Next Best Action recommendations.',
    icon: 'NB',
  },
  mmm: {
    title: 'MMM defaults',
    description: 'Set defaults for new MMM runs including aggregation cadence.',
    icon: 'MD',
  },
  notifications: {
    title: 'Notifications',
    description:
      'Configure notification channels (email, Slack) and preferences: severities, digest mode, and quiet hours.',
    icon: 'NT',
  },
}

interface AttributionSettings {
  lookback_window_days: number
  use_converted_flag: boolean
  min_conversion_value: number
  time_decay_half_life_days: number
  position_first_pct: number
  position_last_pct: number
  markov_min_paths: number
}

interface MMMSettings {
  frequency: string
}

interface NBASettings {
  min_prefix_support: number
  min_conversion_rate: number
  max_prefix_depth: number
  min_next_support: number
  max_suggestions_per_prefix: number
  min_uplift_pct: number | null
  excluded_channels: string[]
}

interface FeatureFlags {
  journeys_enabled: boolean
  journey_examples_enabled: boolean
  funnel_builder_enabled: boolean
  funnel_diagnostics_enabled: boolean
  access_control_enabled: boolean
  custom_roles_enabled: boolean
  audit_log_enabled: boolean
  scim_enabled: boolean
  sso_enabled: boolean
}

interface RevenueConfig {
  conversion_names: string[]
  value_field_path: string
  currency_field_path: string
  dedup_key: 'conversion_id' | 'order_id' | 'event_id'
  base_currency: string
  fx_enabled: boolean
  fx_mode: 'none' | 'static_rates'
  fx_rates_json: Record<string, number>
  source_type?: string
}

type NbaPresetKey = 'conservative' | 'balanced' | 'aggressive'

const NBA_PRESETS: Record<
  NbaPresetKey,
  Pick<
    NBASettings,
    'min_prefix_support' | 'min_conversion_rate' | 'max_prefix_depth'
  >
> = {
  conservative: {
    min_prefix_support: 10,
    min_conversion_rate: 0.025,
    max_prefix_depth: 3,
  },
  balanced: {
    min_prefix_support: 5,
    min_conversion_rate: 0.01,
    max_prefix_depth: 5,
  },
  aggressive: {
    min_prefix_support: 3,
    min_conversion_rate: 0.005,
    max_prefix_depth: 6,
  },
}

const NBA_PRESET_TOLERANCE = 0.0001

interface Settings {
  attribution: AttributionSettings
  mmm: MMMSettings
  nba: NBASettings
  feature_flags: FeatureFlags
  revenue_config: RevenueConfig
}

type MatchOperator = 'any' | 'contains' | 'equals' | 'regex'

interface MatchExpression {
  operator: MatchOperator
  value: string
}

interface ChannelRule {
  name: string
  channel: string
  priority: number
  enabled: boolean
  source: MatchExpression
  medium: MatchExpression
  campaign: MatchExpression
}

interface AliasRow {
  id: string
  alias: string
  canonical: string
}

interface Taxonomy {
  channel_rules: ChannelRule[]
  source_aliases: Record<string, string>
  medium_aliases: Record<string, string>
}

interface KpiDefinition {
  id: string
  label: string
  type: 'primary' | 'micro'
  event_name: string
  value_field?: string | null
  weight: number
  lookback_days?: number | null
}

interface KpiConfig {
  definitions: KpiDefinition[]
  primary_kpi_id?: string | null
}

interface KpiTestResult {
  testAvailable: boolean
  eventsMatched: number
  journeysMatched: number
  journeysTotal: number
  journeysPct: number
  missingValueChecks?: number
  missingValueCount?: number
  missingValuePct?: number | null
  message?: string | null
  reason?: string | null
}

type WindowDirection = 'tighten' | 'loosen' | 'none'
type ConvertedFlagDirection = 'more_included' | 'fewer_included' | 'none'

interface AttributionPreviewSummary {
  previewAvailable: boolean
  totalJourneys: number
  windowImpactCount: number
  windowDirection: WindowDirection
  useConvertedFlagImpact: number
  useConvertedFlagDirection: ConvertedFlagDirection
  reason?: string | null
}

interface NBAPreviewSummary {
  previewAvailable: boolean
  datasetJourneys: number
  totalPrefixes: number
  prefixesEligible: number
  totalRecommendations: number
  averageRecommendationsPerPrefix: number
  filteredBySupportPct: number
  filteredByConversionPct: number
  reason?: string | null
}

interface NBATestRecommendationRow {
  step: string
  channel: string
  campaign?: string | null
  count: number
  conversions: number
  conversion_rate: number
  avg_value: number
  avg_value_converted: number
  uplift_pct: number | null
}

interface NBATestResult {
  previewAvailable: boolean
  prefix: string
  level: string
  totalPrefixSupport: number
  baselineConversionRate: number
  recommendations: NBATestRecommendationRow[]
  reason?: string | null
}

interface ModelConfigSummary {
  id: string
  name: string
  status: string
  version: number
  parent_id?: string | null
  created_at?: string | null
  updated_at?: string | null
  activated_at?: string | null
  created_by?: string | null
  change_note?: string | null
}

interface ModelConfigDetail extends ModelConfigSummary {
  config_json: Record<string, unknown>
}

interface ModelConfigValidationResult {
  valid: boolean
  errors: string[]
  warnings: string[]
  missing_conversions: string[]
  schema_errors: string[]
}

interface ModelConfigPreviewMetrics {
  journeys: number
  touchpoints: number
  conversions: number
}

interface ModelConfigPreviewResult {
  preview_available: boolean
  reason?: string | null
  baseline?: ModelConfigPreviewMetrics | null
  draft?: ModelConfigPreviewMetrics | null
  deltas?: Record<string, number>
  deltas_pct?: Record<string, number | null>
  warnings?: string[]
  coverage_warning?: boolean
  changed_keys?: string[]
  active_config_id?: string | null
  active_version?: number | null
}

interface NotificationChannelRow {
  id: number
  type: 'email' | 'slack_webhook'
  config: { emails?: string[]; configured?: boolean }
  is_verified: boolean
  created_at: string | null
}

interface NotificationPrefRow {
  id: number
  user_id: string
  channel_id: number
  severities: string[]
  digest_mode: 'realtime' | 'daily'
  quiet_hours: { start?: string; end?: string; timezone?: string } | null
  is_enabled: boolean
  created_at: string | null
  updated_at: string | null
}

const DEFAULT_MODEL_CONFIG_JSON: Record<string, any> = {
  attribution: {
    eligible_touchpoints: {
      include_channels: ['paid_search', 'paid_social', 'email', 'affiliate'],
      exclude_channels: ['direct'],
      include_event_types: [
        'ad_click',
        'ad_impression',
        'email_click',
        'site_visit',
      ],
      exclude_event_types: [],
    },
    dedup_rules: {
      prefer_server_over_client: true,
      dedup_key_fields: [
        'event_id',
        'platform_event_id',
        'meiro_event_hash',
      ],
    },
  },
  windows: {
    click_lookback_days: 30,
    impression_lookback_days: 7,
    session_timeout_minutes: 30,
    conversion_latency_days: 7,
  },
  conversions: {
    primary_conversion_key: 'purchase',
    conversion_definitions: [
      {
        key: 'purchase',
        name: 'Purchase',
        event_name: 'order_completed',
        filters: [
          { field: 'currency', op: 'in', value: ['EUR', 'CZK'] },
          { field: 'revenue', op: '>', value: 0 },
        ],
        value_field: 'revenue',
        dedup_mode: 'order_id',
        attribution_model_default: 'data_driven',
      },
      {
        key: 'lead',
        name: 'Qualified Lead',
        event_name: 'lead_submitted',
        filters: [{ field: 'lead_quality', op: '>=', value: 60 }],
        value_field: null,
        dedup_mode: 'lead_id',
        attribution_model_default: 'position_based',
      },
    ],
  },
}

const DEFAULT_SETTINGS: Settings = {
  attribution: {
    lookback_window_days: 30,
    use_converted_flag: true,
    min_conversion_value: 0,
    time_decay_half_life_days: 7,
    position_first_pct: 0.4,
    position_last_pct: 0.4,
    markov_min_paths: 5,
  },
  mmm: {
    frequency: 'W',
  },
  nba: {
    min_prefix_support: 5,
    min_conversion_rate: 0.01,
    max_prefix_depth: 5,
    min_next_support: 5,
    max_suggestions_per_prefix: 3,
    min_uplift_pct: null,
    excluded_channels: ['direct'],
  },
  feature_flags: {
    journeys_enabled: false,
    journey_examples_enabled: false,
    funnel_builder_enabled: false,
    funnel_diagnostics_enabled: false,
    access_control_enabled: false,
    custom_roles_enabled: false,
    audit_log_enabled: false,
    scim_enabled: false,
    sso_enabled: false,
  },
  revenue_config: {
    conversion_names: ['purchase'],
    value_field_path: 'value',
    currency_field_path: 'currency',
    dedup_key: 'conversion_id',
    base_currency: 'EUR',
    fx_enabled: false,
    fx_mode: 'none',
    fx_rates_json: {},
    source_type: 'conversion_event',
  },
}

const EMPTY_TAXONOMY: Taxonomy = {
  channel_rules: [],
  source_aliases: {},
  medium_aliases: {},
}

const EMPTY_KPI_CONFIG: KpiConfig = {
  definitions: [],
  primary_kpi_id: undefined,
}

const SUGGESTED_CHANNELS = [
  'paid_search',
  'paid_social',
  'email',
  'affiliate',
  'display',
  'organic',
  'referral',
  'direct',
  'video',
  'tv',
]

const SUGGESTED_EVENT_TYPES = [
  'ad_click',
  'ad_impression',
  'email_click',
  'site_visit',
  'app_open',
  'call_center',
]

const TEMPLATE_REFERENCE_SNIPPET = JSON.stringify(DEFAULT_MODEL_CONFIG_JSON, null, 2)

const MATCH_OPERATORS: MatchOperator[] = ['contains', 'equals', 'regex', 'any']

const KNOWN_CHANNELS = [
  'paid_search',
  'paid_social',
  'email',
  'display',
  'affiliate',
  'organic_search',
  'organic_social',
  'referral',
  'direct',
  'video',
  'tv',
  'sms',
  'push',
  'app_store',
]

function ensureMatchExpression(
  input?: Partial<MatchExpression> | null,
): MatchExpression {
  const operator = (input?.operator ?? 'any').toLowerCase() as MatchOperator
  const normalizedOperator: MatchOperator = MATCH_OPERATORS.includes(operator)
    ? operator
    : 'any'
  return {
    operator: normalizedOperator,
    value: input?.value ?? '',
  }
}

function createEmptyRule(priority: number = 100): ChannelRule {
  return {
    name: '',
    channel: '',
    priority,
    enabled: true,
    source: ensureMatchExpression(),
    medium: ensureMatchExpression(),
    campaign: ensureMatchExpression(),
  }
}

function normalizeChannelRule(
  rule: Partial<ChannelRule>,
  fallbackPriority: number,
): ChannelRule {
  return {
    name: rule.name ?? '',
    channel: rule.channel ?? '',
    priority: Number.isFinite(rule.priority)
      ? Number(rule.priority)
      : fallbackPriority,
    enabled: rule.enabled !== false,
    source: ensureMatchExpression(rule.source),
    medium: ensureMatchExpression(rule.medium),
    campaign: ensureMatchExpression(rule.campaign),
  }
}

function describeMatchExpression(
  label: string,
  expr: MatchExpression,
): string {
  const { operator, value } = expr
  if (operator === 'any' || !value) {
    return `${label}: any`
  }
  if (operator === 'contains') {
    return `${label} contains "${value}"`
  }
  if (operator === 'equals') {
    return `${label} equals "${value}"`
  }
  return `${label} matches /${value}/`
}

function generateRowId(): string {
  if (
    typeof globalThis !== 'undefined' &&
    globalThis.crypto &&
    typeof globalThis.crypto.randomUUID === 'function'
  ) {
    return globalThis.crypto.randomUUID()
  }
  return `row_${Math.random().toString(36).slice(2, 10)}`
}

function toAliasRows(aliases: Record<string, string>): AliasRow[] {
  return Object.entries(aliases).map(([alias, canonical]) => ({
    id: generateRowId(),
    alias,
    canonical,
  }))
}

function buildAliasObject(rows: AliasRow[]): Record<string, string> {
  return rows.reduce<Record<string, string>>((acc, row) => {
    const alias = row.alias.trim().toLowerCase()
    const canonical = row.canonical.trim()
    if (alias && canonical) {
      acc[alias] = canonical
    }
    return acc
  }, {})
}

function analyzeAliasRows(rows: AliasRow[]) {
  const seen = new Map<string, number>()
  let hasEmptyAlias = false
  let hasEmptyCanonical = false

  rows.forEach((row) => {
    const alias = row.alias.trim().toLowerCase()
    const canonical = row.canonical.trim()
    if (!alias) {
      hasEmptyAlias = true
    }
    if (!canonical) {
      hasEmptyCanonical = true
    }
    if (alias) {
      seen.set(alias, (seen.get(alias) ?? 0) + 1)
    }
  })

  const duplicates = Array.from(seen.entries())
    .filter(([, count]) => count > 1)
    .map(([alias]) => alias)

  return {
    duplicates,
    hasEmptyAlias,
    hasEmptyCanonical,
  }
}

function formatDateTime(value?: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function deepEqual(a: any, b: any): boolean {
  if (a === b) return true
  if (typeof a !== typeof b) return false
  if (a === null || b === null) return false
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i += 1) {
      if (!deepEqual(a[i], b[i])) return false
    }
    return true
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const aKeys = Object.keys(a).sort()
    const bKeys = Object.keys(b).sort()
    if (aKeys.length !== bKeys.length) return false
    for (let i = 0; i < aKeys.length; i += 1) {
      if (aKeys[i] !== bKeys[i]) return false
      if (!deepEqual(a[aKeys[i]], b[bKeys[i]])) return false
    }
    return true
  }
  return false
}

function formatPercentInput(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return ''
  const percent = value * 100
  const decimals = Number.isInteger(percent) ? 0 : percent < 1 ? 2 : 1
  return percent.toFixed(decimals).replace(/\.?0+$/, '')
}

function normalizeNumericString(value: string): string {
  return value.replace(',', '.').trim()
}

export interface SettingsPageHandle {
  getDirtySections: () => SectionKey[]
  saveSections: (sections?: SectionKey[]) => Promise<boolean>
  discardSections: (sections?: SectionKey[]) => void
}

interface SettingsPageProps {
  onDirtySectionsChange?: (sections: SectionKey[]) => void
}

interface PendingSectionChange {
  target: SectionKey
}

const badgeStyle: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 4,
  padding: '2px 8px',
  borderRadius: 999,
  fontSize: t.font.sizeXs,
  fontWeight: t.font.weightMedium,
  backgroundColor: t.color.warningSubtle,
  color: t.color.warning,
  border: `1px solid ${t.color.warning}`,
}

const SettingsPage = forwardRef<SettingsPageHandle, SettingsPageProps>(
  ({ onDirtySectionsChange }, ref) => {
    const permissions = usePermissions()
    const initialSection: SectionKey =
      (typeof window !== 'undefined' &&
        (() => {
          const hash = window.location.hash.replace('#settings/', '')
          return SECTION_ORDER.includes(hash as SectionKey)
            ? (hash as SectionKey)
            : null
        })()) ||
      'attribution'

    const [activeSection, setActiveSection] =
      useState<SectionKey>(initialSection)
    const [pendingSectionChange, setPendingSectionChange] =
      useState<PendingSectionChange | null>(null)
    const [lastSavedAt, setLastSavedAt] = useState<
      Partial<Record<SectionKey, string>>
    >({})

    const [notificationsChannelsBaseline, setNotificationsChannelsBaseline] =
      useState<NotificationChannelRow[]>([])
    const [notificationsChannelsDraft, setNotificationsChannelsDraft] =
      useState<NotificationChannelRow[]>([])
    const [notificationsPrefsBaseline, setNotificationsPrefsBaseline] =
      useState<NotificationPrefRow[]>([])
    const [notificationsPrefsDraft, setNotificationsPrefsDraft] =
      useState<NotificationPrefRow[]>([])
    const [notificationsSlackWebhookInput, setNotificationsSlackWebhookInput] =
      useState('') // only for new/editing slack; never persisted in state to UI after save

    const settingsQuery = useQuery<Settings>({
      queryKey: ['settings'],
      queryFn: async () => apiGetJson<Settings>('/api/settings', { fallbackMessage: 'Failed to load settings' }),
    })
    const taxonomyQuery = useQuery<Taxonomy>({
      queryKey: ['taxonomy'],
      queryFn: async () => apiGetJson<Taxonomy>('/api/taxonomy', { fallbackMessage: 'Failed to load taxonomy' }),
    })
    const kpiQuery = useQuery<KpiConfig>({
      queryKey: ['kpis'],
      queryFn: async () => apiGetJson<KpiConfig>('/api/kpis', { fallbackMessage: 'Failed to load KPI config' }),
    })
    const currentFlags =
      settingsQuery.data?.feature_flags ??
      DEFAULT_SETTINGS.feature_flags
    const rbacEnabled = currentFlags.access_control_enabled
    const visibleSectionOrder = useMemo<SectionKey[]>(() => {
      const canManageSettings = !rbacEnabled || permissions.hasAnyPermission(['settings.manage', 'settings.view'])
      const canUsers = !rbacEnabled || permissions.hasAnyPermission(['users.manage', 'settings.manage'])
      const canRoles = !rbacEnabled || permissions.hasPermission('roles.manage')
      const canAudit =
        (!rbacEnabled || permissions.hasAnyPermission(['audit.view', 'settings.manage'])) &&
        currentFlags.audit_log_enabled
      return SECTION_ORDER.filter((section) => {
        if (section === 'journeys') return canManageSettings
        if (section === 'access-control-users') return canUsers
        if (section === 'access-control-roles') return canRoles
        if (section === 'access-control-audit-log') return canAudit
        return true
      })
    }, [currentFlags.audit_log_enabled, permissions, rbacEnabled])
    const modelConfigsQuery = useQuery<ModelConfigSummary[]>({
      queryKey: ['model-configs'],
      queryFn: async () => apiGetJson<ModelConfigSummary[]>('/api/model-configs', { fallbackMessage: 'Failed to load model configs' }),
    })

    const notificationChannelsQuery = useQuery<NotificationChannelRow[]>({
      queryKey: ['settings', 'notification-channels'],
      queryFn: async () =>
        apiGetJson<NotificationChannelRow[]>('/api/settings/notification-channels', {
          fallbackMessage: 'Failed to load notification channels',
        }),
    })

    const notificationPrefsQuery = useQuery<NotificationPrefRow[]>({
      queryKey: ['settings', 'notification-preferences'],
      queryFn: async () =>
        apiGetJson<NotificationPrefRow[]>('/api/settings/notification-preferences', {
          fallbackMessage: 'Failed to load notification preferences',
        }),
    })

    const saveNotificationsMutation = useMutation({
      mutationFn: async (payload: {
        channelsBaseline: NotificationChannelRow[]
        channelsDraft: NotificationChannelRow[]
        prefsDraft: NotificationPrefRow[]
        slackWebhookInput: string
      }) => {
        const {
          channelsBaseline,
          channelsDraft,
          prefsDraft,
          slackWebhookInput,
        } = payload
        const baselineIds = new Set(channelsBaseline.map((c) => c.id))
        const draftById = new Map(channelsDraft.map((c) => [c.id, c]))
        const newChannels = channelsDraft.filter((c) => !c.id || c.id < 0)
        const existingChannels = channelsDraft.filter((c) => c.id > 0)
        const toDelete = [...baselineIds].filter((id) => !draftById.has(id))
        const newIdMap = new Map<number, number>()
        for (const ch of newChannels) {
          const body: { type: string; config: Record<string, unknown>; slack_webhook_url?: string } = {
            type: ch.type,
            config: ch.config?.emails ? { emails: ch.config.emails } : {},
          }
          if (ch.type === 'slack_webhook' && slackWebhookInput?.trim()) {
            body.slack_webhook_url = slackWebhookInput.trim()
          }
          const created = await apiSendJson<NotificationChannelRow>('/api/settings/notification-channels', 'POST', body, {
            fallbackMessage: 'Failed to create channel',
          })
          newIdMap.set(ch.id, created.id)
        }
        for (const ch of existingChannels) {
          await apiSendJson<NotificationChannelRow>(
            `/api/settings/notification-channels/${ch.id}`,
            'PUT',
            {
              config: ch.config,
              slack_webhook_url:
                ch.type === 'slack_webhook' && slackWebhookInput?.trim()
                  ? slackWebhookInput.trim()
                  : undefined,
            },
            { fallbackMessage: 'Failed to update channel' },
          )
        }
        for (const id of toDelete) {
          await apiSendJson<{ ok: boolean }>(`/api/settings/notification-channels/${id}`, 'DELETE', undefined, {
            fallbackMessage: 'Failed to delete channel',
          })
        }
        const resolveChannelId = (cid: number): number =>
          cid > 0 ? cid : newIdMap.get(cid) ?? cid
        for (const pref of prefsDraft) {
          const channelId = resolveChannelId(pref.channel_id)
          if (channelId <= 0) continue
          await apiSendJson<NotificationPrefRow>('/api/settings/notification-preferences', 'POST', {
            channel_id: channelId,
            severities: pref.severities ?? [],
            digest_mode: pref.digest_mode ?? 'realtime',
            quiet_hours: pref.quiet_hours ?? null,
            is_enabled: pref.is_enabled ?? false,
          }, {
            fallbackMessage: 'Failed to save preference',
          })
        }
        return {
          channels: await apiGetJson<NotificationChannelRow[]>('/api/settings/notification-channels', {
            fallbackMessage: 'Failed to refetch channels',
          }),
          prefs: await apiGetJson<NotificationPrefRow[]>('/api/settings/notification-preferences', {
            fallbackMessage: 'Failed to refetch preferences',
          }),
        }
      },
      onSuccess: (data) => {
        setNotificationsChannelsBaseline(data.channels)
        setNotificationsChannelsDraft(data.channels)
        setNotificationsPrefsBaseline(data.prefs)
        setNotificationsPrefsDraft(data.prefs)
        setNotificationsSlackWebhookInput('')
        void notificationChannelsQuery.refetch()
        void notificationPrefsQuery.refetch()
      },
    })

    const saveSettingsMutation = useMutation({
      mutationFn: async (payload: Settings) =>
        apiSendJson<Settings>('/api/settings', 'POST', payload, { fallbackMessage: 'Failed to save settings' }),
    })

    const saveTaxonomyMutation = useMutation({
      mutationFn: async (payload: Taxonomy) =>
        apiSendJson<Taxonomy>('/api/taxonomy', 'POST', payload, { fallbackMessage: 'Failed to save taxonomy' }),
    })

    const saveKpiMutation = useMutation({
      mutationFn: async (payload: KpiConfig) =>
        apiSendJson<KpiConfig>('/api/kpis', 'POST', payload, { fallbackMessage: 'Failed to save KPI config' }),
    })

    const testKpiMutation = useMutation({
      mutationFn: async (payload: { definition: KpiDefinition }) =>
        apiSendJson<KpiTestResult>('/api/kpis/test', 'POST', payload, { fallbackMessage: 'Failed to test KPI definition' }),
    })

    const saveModelConfigMutation = useMutation({
      mutationFn: async (payload: {
        id: string
        config: Record<string, any>
        changeNote?: string
      }) =>
        apiSendJson<any>(`/api/model-configs/${payload.id}`, 'PATCH', {
          config_json: payload.config,
          actor: 'ui',
          change_note: payload.changeNote,
        }, { fallbackMessage: 'Failed to save config' }),
      onSuccess: () => {
        void modelConfigsQuery.refetch()
      },
    })

    const activateModelConfigMutation = useMutation({
      mutationFn: async (payload: { id: string; activationNote?: string }) =>
        apiSendJson<any>(`/api/model-configs/${payload.id}/activate`, 'POST', {
          actor: 'ui',
          set_as_default: true,
          activation_note: payload.activationNote,
        }, { fallbackMessage: 'Failed to activate config' }),
      onSuccess: () => {
        setShowActivationModal(false)
        setActivationNote('')
        setActivationSummary(null)
        setActivationWarning(null)
        setLastSavedAt((prev) => ({
          ...prev,
          'measurement-models': new Date().toLocaleTimeString(),
        }))
        void modelConfigsQuery.refetch()
      },
    })

    const createDefaultConfigMutation = useMutation({
      mutationFn: async () =>
        apiSendJson<ModelConfigSummary>('/api/model-configs', 'POST', {
          name: 'Default Paid Media Config',
          created_by: 'ui',
          change_note: 'Initial default config from Settings UI',
          config_json: DEFAULT_MODEL_CONFIG_JSON,
        }, { fallbackMessage: 'Failed to create config' }),
      onSuccess: async (created) => {
        await modelConfigsQuery.refetch()
        if (created?.id) {
          setSelectedModelConfigId(created.id)
        }
      },
    })

    const createModelConfigMutation = useMutation({
      mutationFn: async (payload: {
        name: string
        config: Record<string, any>
        changeNote?: string
        createdBy?: string
      }) =>
        apiSendJson<{ id: string }>('/api/model-configs', 'POST', {
          name: payload.name,
          config_json: payload.config,
          created_by: payload.createdBy ?? 'ui',
          change_note: payload.changeNote,
        }, { fallbackMessage: 'Failed to create config' }),
      onSuccess: async (created) => {
        await modelConfigsQuery.refetch()
        if (created?.id) {
          setSelectedModelConfigId(created.id)
        }
      },
    })

    const cloneModelConfigMutation = useMutation({
      mutationFn: async (payload: { id: string; actor?: string }) => {
        const params = new URLSearchParams()
        params.set('actor', payload.actor ?? 'ui')
        return apiSendJson<{ id: string }>(`/api/model-configs/${payload.id}/clone?${params.toString()}`, 'POST', undefined, {
          fallbackMessage: 'Failed to duplicate config',
        })
      },
      onSuccess: async (created) => {
        await modelConfigsQuery.refetch()
        if (created?.id) {
          setSelectedModelConfigId(created.id)
        }
      },
    })

    const archiveModelConfigMutation = useMutation({
      mutationFn: async (payload: { id: string; actor?: string }) => {
        const params = new URLSearchParams()
        params.set('actor', payload.actor ?? 'ui')
        return apiSendJson<any>(`/api/model-configs/${payload.id}/archive?${params.toString()}`, 'POST', undefined, {
          fallbackMessage: 'Failed to archive config',
        })
      },
      onSuccess: () => {
        void modelConfigsQuery.refetch()
      },
    })

    const validateModelConfigMutation = useMutation({
      mutationFn: async (payload: {
        id: string
        config: Record<string, any>
      }) =>
        apiSendJson<ModelConfigValidationResult>(`/api/model-configs/${payload.id}/validate`, 'POST', {
          config_json: payload.config,
        }, { fallbackMessage: 'Failed to validate config' }),
    })

    const previewModelConfigMutation = useMutation({
      mutationFn: async (payload: {
        id: string
        config: Record<string, any>
      }) =>
        apiSendJson<ModelConfigPreviewResult>(`/api/model-configs/${payload.id}/preview`, 'POST', {
          config_json: payload.config,
        }, { fallbackMessage: 'Failed to compute preview' }),
    })

    const [settingsBaseline, setSettingsBaseline] =
      useState<Settings | null>(null)
    const [attributionDraft, setAttributionDraft] = useState<AttributionSettings>(
      deepClone(DEFAULT_SETTINGS.attribution),
    )
    const [nbaDraft, setNbaDraft] = useState<NBASettings>(
      deepClone(DEFAULT_SETTINGS.nba),
    )
    const [mmmDraft, setMmmDraft] = useState<MMMSettings>(
      deepClone(DEFAULT_SETTINGS.mmm),
    )

    const [taxonomyBaseline, setTaxonomyBaseline] =
      useState<Taxonomy | null>(null)
    const [taxonomyDraft, setTaxonomyDraft] = useState<Taxonomy>(
      deepClone(EMPTY_TAXONOMY),
    )
    const [sourceAliasRows, setSourceAliasRows] = useState<AliasRow[]>([])
    const [mediumAliasRows, setMediumAliasRows] = useState<AliasRow[]>([])
    const [aliasImportErrors, setAliasImportErrors] = useState<{
      source?: string
      medium?: string
    }>({})
    const [aliasValidationErrors, setAliasValidationErrors] = useState<{
      source?: string
      medium?: string
    }>({})
    const [sourceAliasImportJson, setSourceAliasImportJson] =
      useState<string>('{}')
    const [mediumAliasImportJson, setMediumAliasImportJson] =
      useState<string>('{}')
    const [showRuleModal, setShowRuleModal] = useState(false)
    const [ruleModalMode, setRuleModalMode] = useState<'create' | 'edit'>(
      'create',
    )
    const [ruleModalIndex, setRuleModalIndex] = useState<number | null>(null)
    const [ruleModalDraft, setRuleModalDraft] = useState<ChannelRule | null>(
      null,
    )
    const [ruleModalErrors, setRuleModalErrors] = useState<{
      name?: string
      channel?: string
      priority?: string
      source?: string
      medium?: string
      campaign?: string
    }>({})
    const [taxonomyTestInput, setTaxonomyTestInput] = useState<{
      source: string
      medium: string
      campaign: string
      utm_source: string
      utm_medium: string
      utm_campaign: string
    }>({
      source: '',
      medium: '',
      campaign: '',
      utm_source: '',
      utm_medium: '',
      utm_campaign: '',
    })
    const [taxonomyTestResult, setTaxonomyTestResult] = useState<{
      channel: string
      matched_rule: string | null
      confidence: number
      fallback_reason: string | null
      source?: string | null
      medium?: string | null
    } | null>(null)
    const [taxonomyTestError, setTaxonomyTestError] = useState<string | null>(
      null,
    )
    const [isTestingTaxonomy, setIsTestingTaxonomy] = useState(false)
    const [isTestingDataset, setIsTestingDataset] = useState(false)
    const [datasetTestError, setDatasetTestError] = useState<string | null>(null)
    const [unknownPatterns, setUnknownPatterns] = useState<
      Array<{ source: string; medium: string; campaign?: string | null; count: number }>
    >([])
    const [datasetTestRan, setDatasetTestRan] = useState(false)

    const [attributionPreview, setAttributionPreview] =
      useState<AttributionPreviewSummary | null>(null)
    const [attributionPreviewStatus, setAttributionPreviewStatus] = useState<
      'idle' | 'loading' | 'ready' | 'error' | 'blocked' | 'unavailable'
    >('idle')
    const [attributionPreviewError, setAttributionPreviewError] = useState<
      string | null
    >(null)
    const previewAbortRef = useRef<AbortController | null>(null)

    const toastIdRef = useRef(0)
    const [toast, setToast] = useState<{
      id: number
      type: 'success' | 'error'
      message: string
    } | null>(null)

    const [showNbaAdvanced, setShowNbaAdvanced] = useState(false)
    const [nbaConversionRateInput, setNbaConversionRateInput] = useState<string>(
      () => (DEFAULT_SETTINGS.nba.min_conversion_rate * 100).toString(),
    )
    const [nbaUpliftInput, setNbaUpliftInput] = useState<string>('')
    const [nbaPreview, setNbaPreview] = useState<NBAPreviewSummary | null>(null)
    const [nbaPreviewStatus, setNbaPreviewStatus] = useState<
      'idle' | 'loading' | 'ready' | 'error' | 'blocked' | 'unavailable'
    >('idle')
    const [nbaPreviewError, setNbaPreviewError] = useState<string | null>(null)
    const nbaPreviewAbortRef = useRef<AbortController | null>(null)
    const [nbaTestPrefix, setNbaTestPrefix] = useState('')
    const [nbaTestLevel, setNbaTestLevel] =
      useState<'channel' | 'campaign'>('channel')
    const [nbaTestResult, setNbaTestResult] = useState<NBATestResult | null>(null)
    const [nbaTestError, setNbaTestError] = useState<string | null>(null)
    const [isTestingNba, setIsTestingNba] = useState(false)

    const attributionErrors = useMemo(() => {
      const errors: Partial<Record<keyof AttributionSettings, string>> = {}
      if (attributionDraft.lookback_window_days < 1) {
        errors.lookback_window_days = 'Must be at least 1 day'
      } else if (attributionDraft.lookback_window_days > 365) {
        errors.lookback_window_days = 'Keep within a year'
      }
      if (attributionDraft.min_conversion_value < 0) {
        errors.min_conversion_value = 'Cannot be negative'
      }
      if (attributionDraft.time_decay_half_life_days < 1) {
        errors.time_decay_half_life_days = 'Must be positive'
      } else if (attributionDraft.time_decay_half_life_days > 60) {
        errors.time_decay_half_life_days = 'Max 60 days'
      }
      if (
        attributionDraft.position_first_pct < 0 ||
        attributionDraft.position_first_pct > 1
      ) {
        errors.position_first_pct = 'Between 0 and 1'
      }
      if (
        attributionDraft.position_last_pct < 0 ||
        attributionDraft.position_last_pct > 1
      ) {
        errors.position_last_pct = 'Between 0 and 1'
      }
      if (
        attributionDraft.position_first_pct + attributionDraft.position_last_pct >
        1
      ) {
        errors.position_first_pct = 'First + last cannot exceed 1'
        errors.position_last_pct = 'First + last cannot exceed 1'
      }
      if (attributionDraft.markov_min_paths < 1) {
        errors.markov_min_paths = 'Minimum 1 path'
      }
      return errors
    }, [attributionDraft])

    const [kpiBaseline, setKpiBaseline] = useState<KpiConfig | null>(null)
    const [kpiDraft, setKpiDraft] = useState<KpiConfig>(
      deepClone(EMPTY_KPI_CONFIG),
    )
    const [showKpiModal, setShowKpiModal] = useState(false)
    const [kpiModalMode, setKpiModalMode] = useState<'create' | 'edit'>('create')
    const [kpiModalIndex, setKpiModalIndex] = useState<number | null>(null)
    const [kpiModalDraft, setKpiModalDraft] = useState<KpiDefinition | null>(null)
    const [kpiModalErrors, setKpiModalErrors] = useState<
      Partial<Record<keyof KpiDefinition | 'primary', string>>
    >({})
    const [kpiModalPrimarySelected, setKpiModalPrimarySelected] =
      useState<boolean>(false)
    const [kpiModalOriginalId, setKpiModalOriginalId] = useState<string | null>(null)
    const [kpiTestResult, setKpiTestResult] = useState<KpiTestResult | null>(null)
    const [kpiTestError, setKpiTestError] = useState<string | null>(null)
    const [kpiDeleteConfirm, setKpiDeleteConfirm] = useState<{
      index: number
      definition: KpiDefinition
    } | null>(null)

    const makeEmptyKpiDefinition = useCallback(
      (): KpiDefinition => ({
        id: '',
        label: '',
        type: 'micro',
        event_name: '',
        value_field: undefined,
        weight: 1,
        lookback_days: undefined,
      }),
      [],
    )

    const normalizeKpiDefinition = useCallback((definition: KpiDefinition) => {
      const parsedWeight = Number(definition.weight)
      return {
        ...definition,
        id: definition.id.trim(),
        label: definition.label.trim(),
        event_name: definition.event_name.trim(),
        value_field: definition.value_field
          ? definition.value_field.trim() || undefined
          : undefined,
        lookback_days:
          definition.lookback_days !== null &&
          definition.lookback_days !== undefined &&
          Number.isFinite(definition.lookback_days)
            ? Number(definition.lookback_days)
            : undefined,
        weight: Number.isFinite(parsedWeight) ? parsedWeight : Number.NaN,
      }
    }, [])

    const validateKpiDefinition = useCallback(
      (
        definition: KpiDefinition,
        options: {
          allowId?: string | null
          isPrimarySelected: boolean
        },
      ) => {
        const normalized = normalizeKpiDefinition(definition)
        const errors: Partial<Record<keyof KpiDefinition | 'primary', string>> =
          {}
        if (!normalized.id) {
          errors.id = 'ID is required'
        } else if (
          kpiDraft.definitions.some(
            (existing, idx) =>
              existing.id === normalized.id &&
              normalized.id !== options.allowId &&
              (!showKpiModal || idx !== kpiModalIndex),
          )
        ) {
          errors.id = 'ID must be unique'
        }
        if (!normalized.label) {
          errors.label = 'Label is required'
        }
        if (!normalized.event_name) {
          errors.event_name = 'Event name is required'
        }
        if (!Number.isFinite(normalized.weight) || normalized.weight <= 0) {
          errors.weight = 'Weight must be greater than 0'
        }
        if (
          normalized.lookback_days !== undefined &&
          normalized.lookback_days !== null &&
          normalized.lookback_days < 1
        ) {
          errors.lookback_days = 'Lookback must be at least 1 day'
        }
        if (
          options.isPrimarySelected &&
          normalized.type !== 'primary'
        ) {
          errors.primary = 'Primary KPI must have type set to Primary'
        }
        return { normalized, errors }
      },
      [kpiDraft.definitions, kpiModalIndex, normalizeKpiDefinition, showKpiModal],
    )

    const resetKpiModalState = useCallback(() => {
      setKpiModalErrors({})
      setKpiModalPrimarySelected(false)
      setKpiModalOriginalId(null)
      setKpiTestResult(null)
      setKpiTestError(null)
      setKpiModalDraft(null)
      testKpiMutation.reset()
    }, [testKpiMutation])

    const handleOpenCreateKpi = useCallback(() => {
      setKpiModalMode('create')
      setKpiModalIndex(null)
      const empty = makeEmptyKpiDefinition()
      setKpiModalDraft(empty)
      setKpiModalErrors({})
      setKpiModalPrimarySelected(!kpiDraft.primary_kpi_id)
      setKpiModalOriginalId(null)
      setKpiTestResult(null)
      setKpiTestError(null)
      testKpiMutation.reset()
      setShowKpiModal(true)
    }, [kpiDraft.primary_kpi_id, makeEmptyKpiDefinition, testKpiMutation])

    const handleOpenEditKpi = useCallback(
      (index: number) => {
        const definition = kpiDraft.definitions[index]
        if (!definition) return
        setKpiModalMode('edit')
        setKpiModalIndex(index)
        setKpiModalDraft(deepClone(definition))
        setKpiModalErrors({})
        setKpiModalPrimarySelected(kpiDraft.primary_kpi_id === definition.id)
        setKpiModalOriginalId(definition.id)
        setKpiTestResult(null)
        setKpiTestError(null)
        testKpiMutation.reset()
        setShowKpiModal(true)
      },
      [kpiDraft.definitions, kpiDraft.primary_kpi_id, testKpiMutation],
    )

    const handleCloseKpiModal = useCallback(() => {
      setShowKpiModal(false)
      resetKpiModalState()
    }, [resetKpiModalState])

    const handleKpiModalFieldChange = useCallback(
      <K extends keyof KpiDefinition>(field: K, value: KpiDefinition[K]) => {
        setKpiModalDraft((prev) => {
          if (!prev) return prev
          return {
            ...prev,
            [field]: value,
          }
        })
        if (field === 'type' && value === 'micro') {
          setKpiModalPrimarySelected(false)
        }
        setKpiTestResult(null)
        setKpiTestError(null)
        setKpiModalErrors((prev) => {
          if (!Object.keys(prev).length) return prev
          const next = { ...prev }
          delete next[field]
          if (field === 'type') delete next.primary
          return next
        })
      },
      [],
    )

    const handleTogglePrimary = useCallback((next: boolean) => {
      setKpiModalPrimarySelected(next)
      setKpiTestResult(null)
      setKpiTestError(null)
      setKpiModalErrors((prev) => {
        if (!prev.primary) return prev
        const { primary, ...rest } = prev
        return rest
      })
    }, [])

    const handleSubmitKpiModal = useCallback(() => {
      if (!kpiModalDraft) return
      const { normalized, errors } = validateKpiDefinition(kpiModalDraft, {
        allowId: kpiModalOriginalId,
        isPrimarySelected: kpiModalPrimarySelected,
      })
      if (Object.keys(errors).length > 0) {
        setKpiModalErrors(errors)
        return
      }

      setKpiDraft((prev) => {
        const definitions = [...prev.definitions]
        let primary_kpi_id = prev.primary_kpi_id
        if (kpiModalMode === 'create') {
          definitions.push(normalized)
        } else if (kpiModalIndex != null) {
          definitions[kpiModalIndex] = normalized
          if (primary_kpi_id === kpiModalOriginalId && !kpiModalPrimarySelected) {
            primary_kpi_id = null
          }
        }

        if (kpiModalPrimarySelected) {
          primary_kpi_id = normalized.id
        } else if (!primary_kpi_id) {
          const fallback = definitions.find((def) => def.type === 'primary')
          if (fallback) {
            primary_kpi_id = fallback.id
          }
        }

        return {
          definitions,
          primary_kpi_id,
        }
      })
      setShowKpiModal(false)
      resetKpiModalState()
    }, [
      kpiModalDraft,
      kpiModalIndex,
      kpiModalMode,
      kpiModalOriginalId,
      kpiModalPrimarySelected,
      resetKpiModalState,
      validateKpiDefinition,
    ])

    const handleDeleteKpi = useCallback(
      (index: number) => {
        const definition = kpiDraft.definitions[index]
        if (!definition) return
        setKpiDeleteConfirm({ index, definition })
      },
      [kpiDraft.definitions],
    )

    const handleConfirmDeleteKpi = useCallback(() => {
      if (!kpiDeleteConfirm) return
      setKpiDraft((prev) => {
        const definitions = prev.definitions.filter(
          (_def, idx) => idx !== kpiDeleteConfirm.index,
        )
        let primary_kpi_id = prev.primary_kpi_id
        if (primary_kpi_id === kpiDeleteConfirm.definition.id) {
          primary_kpi_id =
            definitions.find((def) => def.type === 'primary')?.id ?? null
        }
        return {
          definitions,
          primary_kpi_id,
        }
      })
      setKpiDeleteConfirm(null)
    }, [kpiDeleteConfirm])

    const handleCancelDeleteKpi = useCallback(() => {
      setKpiDeleteConfirm(null)
    }, [])

    const handleTestKpiDefinition = useCallback(async () => {
      if (!kpiModalDraft) return
      const { normalized, errors } = validateKpiDefinition(kpiModalDraft, {
        allowId: kpiModalOriginalId,
        isPrimarySelected: kpiModalPrimarySelected,
      })
      if (Object.keys(errors).length > 0) {
        setKpiModalErrors(errors)
        return
      }
      try {
        setKpiTestError(null)
        setKpiModalDraft(normalized)
        const result = await testKpiMutation.mutateAsync({
          definition: normalized,
        })
        setKpiTestResult(result)
      } catch (error) {
        setKpiTestResult(null)
        setKpiTestError((error as Error)?.message ?? 'Unable to test KPI definition')
      }
    }, [
      kpiModalDraft,
      kpiModalOriginalId,
      kpiModalPrimarySelected,
      testKpiMutation,
      validateKpiDefinition,
    ])

    const updateSourceAliases = useCallback(
      (updater: (rows: AliasRow[]) => AliasRow[]) => {
        setSourceAliasRows((prev) => {
          const updated = updater(prev)
          setTaxonomyDraft((prevDraft) => ({
            ...prevDraft,
            source_aliases: buildAliasObject(updated),
          }))
          setAliasValidationErrors((prev) => ({ ...prev, source: undefined }))
          return updated
        })
      },
      [],
    )

    const updateMediumAliases = useCallback(
      (updater: (rows: AliasRow[]) => AliasRow[]) => {
        setMediumAliasRows((prev) => {
          const updated = updater(prev)
          setTaxonomyDraft((prevDraft) => ({
            ...prevDraft,
            medium_aliases: buildAliasObject(updated),
          }))
          setAliasValidationErrors((prev) => ({ ...prev, medium: undefined }))
          return updated
        })
      },
      [],
    )

    const handleAddSourceAlias = useCallback(() => {
      updateSourceAliases((prev) => [
        ...prev,
        { id: generateRowId(), alias: '', canonical: '' },
      ])
    }, [updateSourceAliases])

    const handleAddMediumAlias = useCallback(() => {
      updateMediumAliases((prev) => [
        ...prev,
        { id: generateRowId(), alias: '', canonical: '' },
      ])
    }, [updateMediumAliases])

    const handleSourceAliasChange = useCallback(
      (id: string, field: 'alias' | 'canonical', value: string) => {
        updateSourceAliases((prev) =>
          prev.map((row) =>
            row.id === id
              ? {
                  ...row,
                  [field]: value,
                }
              : row,
          ),
        )
      },
      [updateSourceAliases],
    )

    const handleMediumAliasChange = useCallback(
      (id: string, field: 'alias' | 'canonical', value: string) => {
        updateMediumAliases((prev) =>
          prev.map((row) =>
            row.id === id
              ? {
                  ...row,
                  [field]: value,
                }
              : row,
          ),
        )
      },
      [updateMediumAliases],
    )

    const handleRemoveSourceAlias = useCallback(
      (id: string) => {
        updateSourceAliases((prev) => prev.filter((row) => row.id !== id))
      },
      [updateSourceAliases],
    )

    const handleRemoveMediumAlias = useCallback(
      (id: string) => {
        updateMediumAliases((prev) => prev.filter((row) => row.id !== id))
      },
      [updateMediumAliases],
    )

    const handleTaxonomyTestInputChange = useCallback(
      (field: keyof typeof taxonomyTestInput, value: string) => {
        setTaxonomyTestInput((prev) => ({
          ...prev,
          [field]: value,
        }))
      },
      [],
    )

    const handleRunTaxonomyTest = useCallback(async () => {
      setIsTestingTaxonomy(true)
      setTaxonomyTestError(null)
      try {
        const payload = {
          source:
            taxonomyTestInput.source || taxonomyTestInput.utm_source || '',
          medium:
            taxonomyTestInput.medium || taxonomyTestInput.utm_medium || '',
          campaign:
            taxonomyTestInput.campaign || taxonomyTestInput.utm_campaign || '',
          utm_source: taxonomyTestInput.utm_source,
          utm_medium: taxonomyTestInput.utm_medium,
          utm_campaign: taxonomyTestInput.utm_campaign,
        }
        const data = await apiSendJson<any>('/api/taxonomy/map-channel', 'POST', payload, {
          fallbackMessage: 'Failed to test taxonomy rule',
        })
        setTaxonomyTestResult({
          channel: data.channel,
          matched_rule: data.matched_rule ?? null,
          confidence: data.confidence ?? 0,
          fallback_reason: data.fallback_reason ?? null,
          source: data.source,
          medium: data.medium,
        })
      } catch (error) {
        setTaxonomyTestResult(null)
        setTaxonomyTestError(
          (error as Error)?.message ?? 'Unable to run taxonomy test',
        )
      } finally {
        setIsTestingTaxonomy(false)
      }
    }, [taxonomyTestInput])

    const handleTaxonomyDatasetTest = useCallback(async () => {
      setIsTestingDataset(true)
      setDatasetTestError(null)
      setDatasetTestRan(false)
      try {
        const data = await apiGetJson<any>('/api/taxonomy/unknown-share?limit=10', {
          fallbackMessage: 'Failed to analyse dataset',
        })
        setUnknownPatterns(data.top_unmapped_patterns ?? [])
        setDatasetTestRan(true)
      } catch (error) {
        setUnknownPatterns([])
        setDatasetTestError(
          (error as Error)?.message ?? 'Unable to analyse dataset',
        )
        setDatasetTestRan(false)
      } finally {
        setIsTestingDataset(false)
      }
    }, [])

    const handleImportSourceAliases = useCallback(() => {
      try {
        const parsed = JSON.parse(sourceAliasImportJson) as Record<string, string>
        if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
          throw new Error('Source aliases JSON must be an object mapping alias -> canonical.')
        }
        const rows = toAliasRows(parsed)
        updateSourceAliases(() => rows)
        setAliasImportErrors((prev) => ({ ...prev, source: undefined }))
      } catch (error) {
        setAliasImportErrors((prev) => ({
          ...prev,
          source: (error as Error)?.message ?? 'Invalid JSON structure',
        }))
      }
    }, [sourceAliasImportJson, updateSourceAliases])

    const handleImportMediumAliases = useCallback(() => {
      try {
        const parsed = JSON.parse(mediumAliasImportJson) as Record<string, string>
        if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
          throw new Error('Medium aliases JSON must be an object mapping alias -> canonical.')
        }
        const rows = toAliasRows(parsed)
        updateMediumAliases(() => rows)
        setAliasImportErrors((prev) => ({ ...prev, medium: undefined }))
      } catch (error) {
        setAliasImportErrors((prev) => ({
          ...prev,
          medium: (error as Error)?.message ?? 'Invalid JSON structure',
        }))
      }
    }, [mediumAliasImportJson, updateMediumAliases])

    useEffect(() => {
      setSourceAliasImportJson(
        JSON.stringify(buildAliasObject(sourceAliasRows), null, 2),
      )
    }, [sourceAliasRows])

    useEffect(() => {
      setMediumAliasImportJson(
        JSON.stringify(buildAliasObject(mediumAliasRows), null, 2),
      )
    }, [mediumAliasRows])

    useEffect(() => {
      if (!toast) return
      const timer = setTimeout(() => setToast(null), 4000)
      return () => clearTimeout(timer)
    }, [toast])

    const attributionPreviewKey = useMemo(() => {
      if (activeSection !== 'attribution') return null
      const {
        lookback_window_days,
        min_conversion_value,
        use_converted_flag,
        time_decay_half_life_days,
        position_first_pct,
        position_last_pct,
        markov_min_paths,
      } = attributionDraft
      return JSON.stringify({
        lookback_window_days,
        min_conversion_value,
        use_converted_flag,
        time_decay_half_life_days,
        position_first_pct,
        position_last_pct,
        markov_min_paths,
      })
    }, [activeSection, attributionDraft])

    useEffect(() => {
      if (activeSection !== 'attribution') {
        previewAbortRef.current?.abort()
        setAttributionPreviewStatus('idle')
        setAttributionPreview(null)
        setAttributionPreviewError(null)
        return
      }

      if (Object.keys(attributionErrors).length > 0) {
        previewAbortRef.current?.abort()
        setAttributionPreviewStatus('blocked')
        setAttributionPreview(null)
        setAttributionPreviewError(null)
        return
      }

      if (!attributionPreviewKey) return

      previewAbortRef.current?.abort()
      const controller = new AbortController()
      previewAbortRef.current = controller
      setAttributionPreviewStatus('loading')
      setAttributionPreviewError(null)

      const timeout = setTimeout(async () => {
        try {
          const data = await apiSendJson<AttributionPreviewSummary>(
            '/api/attribution/preview',
            'POST',
            { settings: attributionDraft },
            { fallbackMessage: 'Failed to calculate preview', signal: controller.signal },
          )
          if (controller.signal.aborted) return
          setAttributionPreview(data)
          if (data.previewAvailable) {
            setAttributionPreviewStatus('ready')
          } else {
            setAttributionPreviewStatus('unavailable')
          }
          setAttributionPreviewError(null)
        } catch (error) {
          if (controller.signal.aborted) return
          setAttributionPreview(null)
          setAttributionPreviewError(
            (error as Error)?.message ?? 'Failed to calculate preview',
          )
          setAttributionPreviewStatus('error')
        }
      }, 300)

      return () => {
        clearTimeout(timeout)
        controller.abort()
      }
    }, [
      activeSection,
      attributionDraft,
      attributionErrors,
      attributionPreviewKey,
    ])

    const getNextRulePriority = useCallback(() => {
      if (taxonomyDraft.channel_rules.length === 0) return 10
      const maxPriority = Math.max(
        ...taxonomyDraft.channel_rules.map((rule) =>
          Number.isFinite(rule.priority) ? Number(rule.priority) : 0,
        ),
      )
      return maxPriority + 10
    }, [taxonomyDraft.channel_rules])

    const handleOpenCreateRule = useCallback(() => {
      setRuleModalMode('create')
      setRuleModalIndex(null)
      setRuleModalDraft(createEmptyRule(getNextRulePriority()))
      setRuleModalErrors({})
      setShowRuleModal(true)
    }, [getNextRulePriority])

    const handleOpenEditRule = useCallback(
      (index: number) => {
        const rule = taxonomyDraft.channel_rules[index]
        if (!rule) return
        setRuleModalMode('edit')
        setRuleModalIndex(index)
        setRuleModalDraft({
          ...rule,
          source: ensureMatchExpression(rule.source),
          medium: ensureMatchExpression(rule.medium),
          campaign: ensureMatchExpression(rule.campaign),
        })
        setRuleModalErrors({})
        setShowRuleModal(true)
      },
      [taxonomyDraft.channel_rules],
    )

    const handleDuplicateRule = useCallback(
      (index: number) => {
        const rule = taxonomyDraft.channel_rules[index]
        if (!rule) return
        const duplicate: ChannelRule = {
          ...deepClone(rule),
          name: `${rule.name} copy`,
          priority: getNextRulePriority(),
        }
        setRuleModalMode('create')
        setRuleModalIndex(null)
        setRuleModalDraft(duplicate)
        setRuleModalErrors({})
        setShowRuleModal(true)
      },
      [getNextRulePriority, taxonomyDraft.channel_rules],
    )

    const handleToggleRuleEnabled = useCallback(
      (index: number) => {
        setTaxonomyDraft((prev) => ({
          ...prev,
          channel_rules: prev.channel_rules.map((rule, idx) =>
            idx === index ? { ...rule, enabled: !rule.enabled } : rule,
          ),
        }))
      },
      [],
    )

    const handleRuleModalFieldChange = useCallback(
      (field: 'name' | 'channel' | 'priority' | 'enabled', value: string | number | boolean) => {
        setRuleModalDraft((prev) => {
          if (!prev) return prev
          if (field === 'priority') {
            const numeric = Number(value)
            return {
              ...prev,
              priority: Number.isFinite(numeric) ? numeric : prev.priority,
            }
          }
          if (field === 'enabled') {
            return {
              ...prev,
              enabled: Boolean(value),
            }
          }
          return {
            ...prev,
            [field]: value,
          }
        })
        setRuleModalErrors((prev) => ({ ...prev, [field]: undefined }))
      },
      [],
    )

    const handleRuleExpressionChange = useCallback(
      (field: 'source' | 'medium' | 'campaign', updates: Partial<MatchExpression>) => {
        setRuleModalDraft((prev) => {
          if (!prev) return prev
          const current = ensureMatchExpression(prev[field])
          const nextOperator = (updates.operator ?? current.operator) as MatchOperator
          const nextValue =
            nextOperator === 'any'
              ? ''
              : updates.value !== undefined
              ? updates.value
              : current.value
          const next = ensureMatchExpression({
            operator: nextOperator,
            value: nextValue,
          })
          return {
            ...prev,
            [field]: next,
          }
        })
        setRuleModalErrors((prev) => ({ ...prev, [field]: undefined }))
      },
      [],
    )

    const handleSubmitRuleModal = useCallback(() => {
      if (!ruleModalDraft) return

      const errors: {
        name?: string
        channel?: string
        priority?: string
        source?: string
        medium?: string
        campaign?: string
      } = {}

      if (!ruleModalDraft.name.trim()) {
        errors.name = 'Rule name is required'
      }
      if (!ruleModalDraft.channel.trim()) {
        errors.channel = 'Channel is required'
      }
      if (!Number.isFinite(ruleModalDraft.priority)) {
        errors.priority = 'Priority must be a number'
      }

      const validateExpression = (
        expr: MatchExpression,
        field: 'source' | 'medium' | 'campaign',
      ) => {
        const normalized = ensureMatchExpression(expr)
        if (normalized.operator !== 'any' && !normalized.value.trim()) {
          errors[field] = 'Provide a value or set operator to Any'
        }
        if (normalized.operator === 'regex' && normalized.value.trim()) {
          try {
            // eslint-disable-next-line no-new
            new RegExp(normalized.value)
          } catch (error) {
            errors[field] = (error as Error).message ?? 'Invalid regex pattern'
          }
        }
      }

      validateExpression(ruleModalDraft.source, 'source')
      validateExpression(ruleModalDraft.medium, 'medium')
      validateExpression(ruleModalDraft.campaign, 'campaign')

      if (Object.keys(errors).length > 0) {
        setRuleModalErrors(errors)
        return
      }

      const sanitizedRule: ChannelRule = {
        name: ruleModalDraft.name.trim(),
        channel: ruleModalDraft.channel.trim(),
        priority: Number(ruleModalDraft.priority),
        enabled: ruleModalDraft.enabled !== false,
        source: ensureMatchExpression(ruleModalDraft.source),
        medium: ensureMatchExpression(ruleModalDraft.medium),
        campaign: ensureMatchExpression(ruleModalDraft.campaign),
      }

      setTaxonomyDraft((prev) => {
        let nextRules: ChannelRule[]
        if (ruleModalMode === 'create') {
          nextRules = [...prev.channel_rules, sanitizedRule]
        } else if (
          ruleModalMode === 'edit' &&
          ruleModalIndex != null &&
          prev.channel_rules[ruleModalIndex]
        ) {
          nextRules = prev.channel_rules.map((rule, idx) =>
            idx === ruleModalIndex ? sanitizedRule : rule,
          )
        } else {
          nextRules = [...prev.channel_rules]
        }

        nextRules = nextRules
          .map((rule, index) =>
            Number.isFinite(rule.priority)
              ? rule
              : { ...rule, priority: (index + 1) * 10 },
          )
          .sort(
            (a, b) => a.priority - b.priority || a.name.localeCompare(b.name),
          )

        return {
          ...prev,
          channel_rules: nextRules,
        }
      })

      setShowRuleModal(false)
      setRuleModalDraft(null)
      setRuleModalErrors({})
      setRuleModalIndex(null)
    }, [ruleModalDraft, ruleModalIndex, ruleModalMode])

    const handleCloseRuleModal = useCallback(() => {
      setShowRuleModal(false)
      setRuleModalDraft(null)
      setRuleModalErrors({})
      setRuleModalIndex(null)
    }, [])


    const [modelConfigs, setModelConfigs] = useState<ModelConfigSummary[]>([])
    const [selectedModelConfigId, setSelectedModelConfigId] = useState<
      string | null
    >(null)
    const [modelConfigJson, setModelConfigJson] = useState<string>('')
    const [modelConfigBaseline, setModelConfigBaseline] = useState<string>('')
    const [modelConfigError, setModelConfigError] = useState<string | null>(null)
    const [currentConfigObject, setCurrentConfigObject] = useState<
      Record<string, any> | null
    >(null)
    const [modelConfigChangeNote, setModelConfigChangeNote] = useState<string>('')
    const [modelConfigBaselineChangeNote, setModelConfigBaselineChangeNote] =
      useState<string>('')
    const [measurementEditorMode, setMeasurementEditorMode] = useState<
      'basic' | 'advanced'
    >('basic')
    const [modelConfigParseError, setModelConfigParseError] = useState<string | null>(null)
    const [modelConfigValidation, setModelConfigValidation] =
      useState<ModelConfigValidationResult | null>(null)
    const [modelConfigPreview, setModelConfigPreview] =
      useState<ModelConfigPreviewResult | null>(null)
    const [isValidatingConfig, setIsValidatingConfig] = useState<boolean>(false)
    const [isPreviewingConfig, setIsPreviewingConfig] = useState<boolean>(false)
    const [modelConfigSearch, setModelConfigSearch] = useState<string>('')
    const [modelConfigStatusFilter, setModelConfigStatusFilter] = useState<
      'all' | 'draft' | 'active' | 'archived'
    >('all')
    const [showActivationModal, setShowActivationModal] = useState(false)
    const [activationNote, setActivationNote] = useState<string>('')
    const [activationSummary, setActivationSummary] =
      useState<ModelConfigPreviewResult | null>(null)
    const [activationWarning, setActivationWarning] = useState<string | null>(null)
    const [impactUnavailableReason, setImpactUnavailableReason] = useState<string | null>(null)
    const [includeChannelInput, setIncludeChannelInput] = useState<string>('')
    const [excludeChannelInput, setExcludeChannelInput] = useState<string>('')
    const [includeEventInput, setIncludeEventInput] = useState<string>('')
    const [excludeEventInput, setExcludeEventInput] = useState<string>('')

    const updateConfigObject = useCallback(
      (updater: (prev: Record<string, any>) => Record<string, any>) => {
        setCurrentConfigObject((prev) => {
          const base = prev ? deepClone(prev) : {}
          const next = updater(base)
          const formatted = JSON.stringify(next, null, 2)
          setModelConfigJson(formatted)
          setModelConfigParseError(null)
          setModelConfigError(null)
          setModelConfigValidation(null)
          setModelConfigPreview(null)
          setActivationSummary(null)
          setActivationWarning(null)
          return next
        })
      },
      [],
    )

    const touchpointConfig = useMemo(() => {
      const eligible = (currentConfigObject?.eligible_touchpoints ?? {}) as Record<string, any>
      const ensureArray = (value: unknown) =>
        Array.isArray(value) ? (value as string[]) : []
      return {
        include_channels: ensureArray(eligible.include_channels),
        exclude_channels: ensureArray(eligible.exclude_channels),
        include_event_types: ensureArray(eligible.include_event_types),
        exclude_event_types: ensureArray(eligible.exclude_event_types),
      }
    }, [currentConfigObject])

    const windowsConfig = useMemo(() => {
      const windows = (currentConfigObject?.windows ?? {}) as Record<string, any>
      return {
        click_lookback_days: Number(windows.click_lookback_days ?? 30),
        impression_lookback_days: Number(windows.impression_lookback_days ?? 7),
        session_timeout_minutes: Number(windows.session_timeout_minutes ?? 30),
        conversion_latency_days: Number(windows.conversion_latency_days ?? 7),
      }
    }, [currentConfigObject])

    const conversionsConfig = useMemo(() => {
      const conversions = (currentConfigObject?.conversions ?? {}) as Record<string, any>
      const definitions = Array.isArray(conversions.conversion_definitions)
        ? (conversions.conversion_definitions as Array<Record<string, any>>)
        : []
      const selectedKeys = definitions
        .map((def) => (typeof def?.key === 'string' ? def.key : null))
        .filter((key): key is string => Boolean(key))
      return {
        definitions,
        selectedKeys,
        primaryKey:
          typeof conversions.primary_conversion_key === 'string'
            ? conversions.primary_conversion_key
            : null,
      }
    }, [currentConfigObject])

    const availableKpis = useMemo(
      () => kpiDraft.definitions ?? [],
      [kpiDraft],
    )

    const baselineConfigObject = useMemo(() => {
      if (!modelConfigBaseline) return {}
      try {
        return JSON.parse(modelConfigBaseline) as Record<string, any>
      } catch {
        return {}
      }
    }, [modelConfigBaseline])

    const baselineConversionDefinitions = useMemo(() => {
      const conversions = (baselineConfigObject.conversions ?? {}) as Record<string, any>
      const defs = Array.isArray(conversions.conversion_definitions)
        ? (conversions.conversion_definitions as Array<Record<string, any>>)
        : []
      const map = new Map<string, Record<string, any>>()
      defs.forEach((def) => {
        if (def && typeof def.key === 'string') {
          map.set(def.key, deepClone(def))
        }
      })
      return map
    }, [baselineConfigObject])

    const handleAddTouchpointValue = useCallback(
      (
        field:
          | 'include_channels'
          | 'exclude_channels'
          | 'include_event_types'
          | 'exclude_event_types',
        value: string,
      ) => {
        const trimmed = value.trim()
        if (!trimmed) return
        const currentValues = touchpointConfig[field]
        if (currentValues.includes(trimmed)) return
        updateConfigObject((prev) => {
          const eligible = { ...(prev.eligible_touchpoints ?? {}) }
          eligible[field] = [...currentValues, trimmed]
          return { ...prev, eligible_touchpoints: eligible }
        })
      },
      [touchpointConfig, updateConfigObject],
    )

    const handleRemoveTouchpointValue = useCallback(
      (
        field:
          | 'include_channels'
          | 'exclude_channels'
          | 'include_event_types'
          | 'exclude_event_types',
        value: string,
      ) => {
        const currentValues = touchpointConfig[field]
        updateConfigObject((prev) => {
          const eligible = { ...(prev.eligible_touchpoints ?? {}) }
          eligible[field] = currentValues.filter((item) => item !== value)
          return { ...prev, eligible_touchpoints: eligible }
        })
      },
      [touchpointConfig, updateConfigObject],
    )

    const handleWindowFieldChange = useCallback(
      (field: keyof typeof windowsConfig, value: number) => {
        updateConfigObject((prev) => {
          const windows = { ...(prev.windows ?? {}) }
          windows[field] = value
          return { ...prev, windows }
        })
      },
      [updateConfigObject],
    )

    const handleToggleConversion = useCallback(
      (key: string, enabled: boolean) => {
        updateConfigObject((prev) => {
          const conversions = { ...(prev.conversions ?? {}) }
          const defs = Array.isArray(conversions.conversion_definitions)
            ? [...conversions.conversion_definitions]
            : []
          const existingIndex = defs.findIndex(
            (def) => def && typeof def.key === 'string' && def.key === key,
          )
          if (enabled) {
            if (existingIndex === -1) {
              const baselineDef = baselineConversionDefinitions.get(key)
              const kpi = availableKpis.find((d) => d.id === key)
              const newDef =
                baselineDef ??
                {
                  key,
                  name: kpi?.label ?? key,
                  event_name: kpi?.event_name ?? '',
                  value_field: kpi?.value_field ?? undefined,
                }
              defs.push(deepClone(newDef))
            }
            if (!conversions.primary_conversion_key) {
              conversions.primary_conversion_key = key
            }
          } else {
            if (existingIndex !== -1) {
              defs.splice(existingIndex, 1)
            }
            if (conversions.primary_conversion_key === key) {
              conversions.primary_conversion_key = defs[0]?.key ?? null
            }
          }
          conversions.conversion_definitions = defs
          return { ...prev, conversions }
        })
      },
      [availableKpis, baselineConversionDefinitions, updateConfigObject],
    )

    const handleSetPrimaryConversion = useCallback(
      (key: string) => {
        updateConfigObject((prev) => {
          const conversions = { ...(prev.conversions ?? {}) }
          conversions.primary_conversion_key = key
          return { ...prev, conversions }
        })
      },
      [updateConfigObject],
    )

    const filteredModelConfigs = useMemo(() => {
      const term = modelConfigSearch.trim().toLowerCase()
      return modelConfigs.filter((cfg) => {
        if (modelConfigStatusFilter !== 'all' && cfg.status !== modelConfigStatusFilter) {
          return false
        }
        if (!term) return true
        const haystack = [
          cfg.name,
          cfg.change_note ?? '',
          cfg.status,
          `v${cfg.version}`,
        ]
          .join(' ')
          .toLowerCase()
        return haystack.includes(term)
      })
    }, [modelConfigs, modelConfigSearch, modelConfigStatusFilter])

    const activeConfig = useMemo(
      () => modelConfigs.find((cfg) => cfg.status === 'active') ?? null,
      [modelConfigs],
    )

    const selectedConfigSummary = useMemo(
      () => modelConfigs.find((cfg) => cfg.id === selectedModelConfigId) ?? null,
      [modelConfigs, selectedModelConfigId],
    )

    const handleCreateDraft = useCallback(async () => {
      if (createModelConfigMutation.isPending) return
      const nameInput = window.prompt('Name for the new measurement config draft:')
      if (!nameInput) return
      const name = nameInput.trim()
      if (!name) return
      const noteInput = window.prompt('Short description (optional):') ?? undefined
      try {
        await createModelConfigMutation.mutateAsync({
          name,
          config: deepClone(DEFAULT_MODEL_CONFIG_JSON),
          changeNote: noteInput?.trim() || undefined,
        })
      } catch (error) {
        window.alert((error as Error)?.message ?? 'Failed to create draft')
      }
    }, [createModelConfigMutation])

    const handleDuplicateFromActive = useCallback(async () => {
      if (!activeConfig || cloneModelConfigMutation.isPending) return
      try {
        await cloneModelConfigMutation.mutateAsync({ id: activeConfig.id })
      } catch (error) {
        window.alert((error as Error)?.message ?? 'Failed to duplicate config')
      }
    }, [activeConfig, cloneModelConfigMutation])

    const handleArchiveDraft = useCallback(
      async (cfg: ModelConfigSummary) => {
        if (cfg.status !== 'draft' || archiveModelConfigMutation.isPending) return
        const confirmed = window.confirm(
          `Archive draft "${cfg.name}" v${cfg.version}? This cannot be activated afterwards.`,
        )
        if (!confirmed) return
        try {
          await archiveModelConfigMutation.mutateAsync({ id: cfg.id })
          if (cfg.id === selectedModelConfigId) {
            setSelectedModelConfigId(null)
          }
        } catch (error) {
          window.alert((error as Error)?.message ?? 'Failed to archive draft')
        }
      },
      [
        archiveModelConfigMutation,
        selectedModelConfigId,
        setSelectedModelConfigId,
      ],
    )

    const handleAdvancedJsonChange = useCallback(
      (value: string) => {
        setModelConfigJson(value)
        try {
          const parsed = JSON.parse(value)
          setCurrentConfigObject(parsed)
          setModelConfigParseError(null)
          setModelConfigValidation(null)
          setModelConfigPreview(null)
          setActivationSummary(null)
          setActivationWarning(null)
        } catch (error) {
          setModelConfigParseError((error as Error)?.message ?? 'Invalid JSON')
        }
      },
      [],
    )

    const handleFormatAdvancedJson = useCallback(() => {
      if (modelConfigParseError) {
        setModelConfigError(modelConfigParseError)
        return
      }
      let source = currentConfigObject
      if (!source) {
        try {
          source = JSON.parse(modelConfigJson || '{}') as Record<string, any>
        } catch (error) {
          setModelConfigError((error as Error)?.message ?? 'Invalid JSON')
          return
        }
      }
      const formatted = JSON.stringify(source, null, 2)
      setModelConfigJson(formatted)
      setModelConfigParseError(null)
    }, [currentConfigObject, modelConfigJson, modelConfigParseError])

    const handleValidateDraft = useCallback(async () => {
      if (!selectedModelConfigId) return
      if (modelConfigParseError) {
        setModelConfigError(modelConfigParseError)
        return
      }
      let payloadConfig = currentConfigObject
      if (!payloadConfig) {
        try {
          payloadConfig = JSON.parse(modelConfigJson || '{}') as Record<string, any>
        } catch (error) {
          setModelConfigError((error as Error)?.message ?? 'Invalid JSON')
          return
        }
      }
      setIsValidatingConfig(true)
      try {
        const result = await validateModelConfigMutation.mutateAsync({
          id: selectedModelConfigId,
          config: payloadConfig,
        })
        setModelConfigValidation(result)
        if (result.valid) {
          setModelConfigError(null)
        } else {
          setModelConfigError('Validation found issues. Review details below.')
        }
      } catch (error) {
        setModelConfigError((error as Error)?.message ?? 'Validation failed')
      } finally {
        setIsValidatingConfig(false)
      }
    }, [
      currentConfigObject,
      modelConfigJson,
      modelConfigParseError,
      selectedModelConfigId,
      validateModelConfigMutation,
    ])

    const handlePreviewDraft = useCallback(async () => {
      if (!selectedModelConfigId) return
      if (modelConfigParseError) {
        setModelConfigError(modelConfigParseError)
        return
      }
      let payloadConfig = currentConfigObject
      if (!payloadConfig) {
        try {
          payloadConfig = JSON.parse(modelConfigJson || '{}') as Record<string, any>
        } catch (error) {
          setModelConfigError((error as Error)?.message ?? 'Invalid JSON')
          return
        }
      }
      setIsPreviewingConfig(true)
      try {
        const result = await previewModelConfigMutation.mutateAsync({
          id: selectedModelConfigId,
          config: payloadConfig,
        })
        setModelConfigPreview(result)
        setActivationSummary(result)
        if (result.preview_available) {
          setImpactUnavailableReason(null)
          if (result.coverage_warning) {
            setActivationWarning(
              'Projected conversions drop more than 10% compared to the active config.',
            )
          } else {
            setActivationWarning(null)
          }
        } else {
          setImpactUnavailableReason(result.reason ?? 'Preview unavailable')
          setActivationWarning(null)
        }
      } catch (error) {
        setModelConfigError((error as Error)?.message ?? 'Preview failed')
      } finally {
        setIsPreviewingConfig(false)
      }
    }, [
      currentConfigObject,
      modelConfigJson,
      modelConfigParseError,
      previewModelConfigMutation,
      selectedModelConfigId,
    ])

    const handleEditorModeChange = useCallback(
      (mode: 'basic' | 'advanced') => {
        setMeasurementEditorMode(mode)
      },
      [],
    )

    useEffect(() => {
      if (settingsQuery.data) {
        setSettingsBaseline((prev) => prev ?? settingsQuery.data)
        if (!settingsBaseline) {
          setAttributionDraft(deepClone(settingsQuery.data.attribution))
          setNbaDraft(deepClone(settingsQuery.data.nba))
          setMmmDraft(deepClone(settingsQuery.data.mmm))
        }
      }
    }, [settingsQuery.data, settingsBaseline])

    useEffect(() => {
      const ch = notificationChannelsQuery.data
      const pr = notificationPrefsQuery.data
      if (
        Array.isArray(ch) &&
        Array.isArray(pr) &&
        notificationsChannelsBaseline.length === 0
      ) {
        setNotificationsChannelsBaseline(deepClone(ch))
        setNotificationsChannelsDraft(deepClone(ch))
        setNotificationsPrefsBaseline(deepClone(pr))
        setNotificationsPrefsDraft(deepClone(pr))
      }
    }, [
      notificationChannelsQuery.data,
      notificationPrefsQuery.data,
      notificationsChannelsBaseline.length,
    ])

    const nbaErrors = useMemo(() => {
      const errors: Partial<Record<keyof NBASettings, string>> = {}
      if (nbaDraft.min_prefix_support < 1) {
        errors.min_prefix_support = 'At least 1 journey required'
      }
      const normalizedConversion = normalizeNumericString(
        nbaConversionRateInput,
      )
      if (!normalizedConversion) {
        errors.min_conversion_rate = 'Conversion rate is required'
      } else {
        const parsed = Number(normalizedConversion)
        if (!Number.isFinite(parsed)) {
          errors.min_conversion_rate = 'Enter a valid percentage'
        } else if (parsed < 0 || parsed > 100) {
          errors.min_conversion_rate = 'Between 0 and 100'
        }
      }
      if (nbaDraft.max_prefix_depth < 0 || nbaDraft.max_prefix_depth > 10) {
        errors.max_prefix_depth = 'Depth between 0 and 10'
      }
      if (nbaDraft.min_next_support < 1) {
        errors.min_next_support = 'Minimum 1 continuation required'
      }
      if (nbaDraft.max_suggestions_per_prefix < 1) {
        errors.max_suggestions_per_prefix = 'At least 1 suggestion required'
      } else if (nbaDraft.max_suggestions_per_prefix > 10) {
        errors.max_suggestions_per_prefix = 'Keep to 10 or fewer suggestions'
      }
      const normalizedUplift = normalizeNumericString(nbaUpliftInput)
      if (normalizedUplift) {
        const parsed = Number(normalizedUplift)
        if (!Number.isFinite(parsed)) {
          errors.min_uplift_pct = 'Enter a valid percentage'
        } else if (parsed < 0 || parsed > 100) {
          errors.min_uplift_pct = 'Between 0 and 100'
        }
      }
      return errors
    }, [nbaDraft, nbaConversionRateInput, nbaUpliftInput])

    const nbaPreviewKey = useMemo(() => {
      const sortedExcluded = [...(nbaDraft.excluded_channels ?? [])]
        .map((channel) => channel.trim().toLowerCase())
        .filter(Boolean)
        .sort()
      return JSON.stringify({
        min_prefix_support: nbaDraft.min_prefix_support,
        min_conversion_rate: Number(
          nbaDraft.min_conversion_rate.toFixed(4),
        ),
        max_prefix_depth: nbaDraft.max_prefix_depth,
        min_next_support: nbaDraft.min_next_support,
        max_suggestions_per_prefix: nbaDraft.max_suggestions_per_prefix,
        min_uplift_pct:
          nbaDraft.min_uplift_pct !== null
            ? Number(nbaDraft.min_uplift_pct.toFixed(4))
            : null,
        excluded_channels: sortedExcluded,
      })
    }, [
      nbaDraft.excluded_channels,
      nbaDraft.max_prefix_depth,
      nbaDraft.max_suggestions_per_prefix,
      nbaDraft.min_conversion_rate,
      nbaDraft.min_next_support,
      nbaDraft.min_prefix_support,
      nbaDraft.min_uplift_pct,
    ])

    useEffect(() => {
      const formattedConversion = formatPercentInput(
        nbaDraft.min_conversion_rate,
      )
      if (
        normalizeNumericString(nbaConversionRateInput) !==
        normalizeNumericString(formattedConversion)
      ) {
        setNbaConversionRateInput(formattedConversion)
      }
      const formattedUplift = formatPercentInput(
        nbaDraft.min_uplift_pct ?? null,
      )
      if (
        normalizeNumericString(nbaUpliftInput) !==
        normalizeNumericString(formattedUplift)
      ) {
        setNbaUpliftInput(formattedUplift)
      }
    }, [nbaDraft.min_conversion_rate, nbaDraft.min_uplift_pct, nbaConversionRateInput, nbaUpliftInput])

    useEffect(() => {
      if (activeSection !== 'nba') {
        nbaPreviewAbortRef.current?.abort()
        setNbaPreviewStatus('idle')
        setNbaPreview(null)
        setNbaPreviewError(null)
        return
      }

      if (Object.keys(nbaErrors).length > 0) {
        nbaPreviewAbortRef.current?.abort()
        setNbaPreviewStatus('blocked')
        setNbaPreview(null)
        setNbaPreviewError(null)
        return
      }

      if (!nbaPreviewKey) return

      nbaPreviewAbortRef.current?.abort()
      const controller = new AbortController()
      nbaPreviewAbortRef.current = controller
      setNbaPreviewStatus('loading')
      setNbaPreviewError(null)

      const timeout = setTimeout(async () => {
        try {
          const payload = {
            settings: {
              ...nbaDraft,
              excluded_channels: Array.from(
                new Set(
                  (nbaDraft.excluded_channels ?? [])
                    .map((channel) => channel.trim())
                    .filter(Boolean),
                ),
              ),
            },
            level: 'channel',
          }
          const data = await apiSendJson<NBAPreviewSummary>(
            '/api/nba/preview',
            'POST',
            payload,
            { fallbackMessage: 'Failed to calculate NBA preview', signal: controller.signal },
          )
          if (controller.signal.aborted) return
          setNbaPreview(data)
          if (data.previewAvailable) {
            setNbaPreviewStatus('ready')
            setNbaPreviewError(null)
          } else {
            setNbaPreviewStatus('unavailable')
            setNbaPreviewError(null)
          }
        } catch (error) {
          if (controller.signal.aborted) return
          setNbaPreview(null)
          setNbaPreviewStatus('error')
          setNbaPreviewError(
            (error as Error)?.message ?? 'Failed to calculate NBA preview',
          )
        }
      }, 300)

      return () => {
        clearTimeout(timeout)
        controller.abort()
      }
    }, [
      activeSection,
      nbaDraft.excluded_channels,
      nbaDraft.max_prefix_depth,
      nbaDraft.max_suggestions_per_prefix,
      nbaDraft.min_conversion_rate,
      nbaDraft.min_next_support,
      nbaDraft.min_prefix_support,
      nbaDraft.min_uplift_pct,
      nbaErrors,
      nbaPreviewKey,
    ])

    useEffect(() => {
      if (!taxonomyQuery.data) return

      const normalizedRules = (taxonomyQuery.data.channel_rules ?? []).map(
        (rule, index) => normalizeChannelRule(rule, (index + 1) * 10),
      )

      const normalized: Taxonomy = {
        channel_rules: normalizedRules.sort(
          (a, b) => a.priority - b.priority || a.name.localeCompare(b.name),
        ),
        source_aliases: taxonomyQuery.data.source_aliases ?? {},
        medium_aliases: taxonomyQuery.data.medium_aliases ?? {},
      }

      setTaxonomyBaseline((prev) => prev ?? deepClone(normalized))

      if (!taxonomyBaseline) {
        setTaxonomyDraft(deepClone(normalized))
        setSourceAliasRows(toAliasRows(normalized.source_aliases))
        setMediumAliasRows(toAliasRows(normalized.medium_aliases))
      }
    }, [taxonomyBaseline, taxonomyQuery.data])

    useEffect(() => {
      if (kpiQuery.data) {
        setKpiBaseline((prev) => prev ?? kpiQuery.data)
        if (!kpiBaseline) {
          setKpiDraft(deepClone(kpiQuery.data))
        }
      }
    }, [kpiQuery.data, kpiBaseline])

    useEffect(() => {
      if (modelConfigsQuery.data) {
        setModelConfigs(modelConfigsQuery.data)
      }
    }, [modelConfigsQuery.data])

    useEffect(() => {
      if (!selectedModelConfigId && modelConfigs.length > 0) {
        setSelectedModelConfigId(modelConfigs[0].id)
      }
    }, [modelConfigs, selectedModelConfigId])

    useEffect(() => {
      if (!selectedModelConfigId) {
        setModelConfigJson('')
        setModelConfigBaseline('')
        setCurrentConfigObject(null)
        setModelConfigChangeNote('')
        setModelConfigBaselineChangeNote('')
        setModelConfigValidation(null)
        setModelConfigPreview(null)
        setModelConfigParseError(null)
        return
      }
      ;(async () => {
        try {
          const data = await apiGetJson<ModelConfigDetail>(`/api/model-configs/${selectedModelConfigId}`, {
            fallbackMessage: 'Failed to load config detail',
          })
          const configObject = (data.config_json ?? {}) as Record<string, any>
          const json = JSON.stringify(configObject, null, 2)
          setCurrentConfigObject(configObject)
          setModelConfigJson(json)
          setModelConfigBaseline(json)
          const note = data.change_note ?? ''
          setModelConfigChangeNote(note)
          setModelConfigBaselineChangeNote(note)
          setModelConfigError(null)
          setModelConfigParseError(null)
          setMeasurementEditorMode('basic')
          setModelConfigValidation(null)
          setModelConfigPreview(null)
          setActivationNote('')
          setActivationSummary(null)
          setActivationWarning(null)
          setImpactUnavailableReason(null)
          setIncludeChannelInput('')
          setExcludeChannelInput('')
          setIncludeEventInput('')
          setExcludeEventInput('')
        } catch (error) {
          setModelConfigError((error as Error)?.message ?? 'Unknown error')
        }
      })()
    }, [selectedModelConfigId])

    useEffect(() => {
      if (!showActivationModal) return
      if (!selectedModelConfigId) return
      if (modelConfigParseError) return
      if (!modelConfigValidation) {
        void handleValidateDraft()
      }
      if (!modelConfigPreview) {
        void handlePreviewDraft()
      }
    }, [
      showActivationModal,
      selectedModelConfigId,
      modelConfigParseError,
      modelConfigValidation,
      modelConfigPreview,
      handleValidateDraft,
      handlePreviewDraft,
    ])

    useEffect(() => {
      if (typeof window === 'undefined') return
      const handler = () => {
        const hash = window.location.hash.replace('#settings/', '')
        if (SECTION_ORDER.includes(hash as SectionKey)) {
          setActiveSection(hash as SectionKey)
        }
      }
      window.addEventListener('hashchange', handler)
      return () => window.removeEventListener('hashchange', handler)
    }, [])

    useEffect(() => {
      if (typeof window === 'undefined') return
      const nextHash = `#settings/${activeSection}`
      if (window.location.hash !== nextHash) {
        window.history.replaceState(null, '', nextHash)
      }
    }, [activeSection])

    useEffect(() => {
      if (!visibleSectionOrder.includes(activeSection)) {
        setActiveSection(visibleSectionOrder[0] ?? 'attribution')
      }
    }, [activeSection, visibleSectionOrder])

    const nbaActivePreset = useMemo<NbaPresetKey | 'custom'>(() => {
      return (
        (Object.entries(NBA_PRESETS) as Array<[NbaPresetKey, typeof NBA_PRESETS.conservative]>).find(
          ([, preset]) =>
            preset.min_prefix_support === nbaDraft.min_prefix_support &&
            Math.abs(preset.min_conversion_rate - nbaDraft.min_conversion_rate) <
              NBA_PRESET_TOLERANCE &&
            preset.max_prefix_depth === nbaDraft.max_prefix_depth,
        )?.[0] ?? 'custom'
      )
    }, [
      nbaDraft.max_prefix_depth,
      nbaDraft.min_conversion_rate,
      nbaDraft.min_prefix_support,
    ])

    const kpiValidation = useMemo(() => {
      const rowErrors: Record<
        number,
        Partial<Record<keyof KpiDefinition | 'row', string>>
      > = {}
      const idCounts: Record<string, number> = {}
      kpiDraft.definitions.forEach((def, index) => {
        const errors: Partial<Record<keyof KpiDefinition | 'row', string>> = {}
        const id = def.id.trim()
        const label = def.label.trim()
        const eventName = def.event_name.trim()
        idCounts[id] = (idCounts[id] ?? 0) + 1
        if (!id) errors.id = 'ID is required'
        if (!label) errors.label = 'Label is required'
        if (!eventName) errors.event_name = 'Event is required'
        if (!Number.isFinite(def.weight) || def.weight <= 0) {
          errors.weight = 'Weight must be > 0'
        }
        if (
          def.lookback_days != null &&
          def.lookback_days !== undefined &&
          def.lookback_days < 1
        ) {
          errors.lookback_days = 'At least 1 day'
        }
        rowErrors[index] = errors
      })

      Object.entries(idCounts).forEach(([id, count]) => {
        if (id && count > 1) {
          kpiDraft.definitions.forEach((def, index) => {
            if (def.id.trim() === id) {
              rowErrors[index] = {
                ...rowErrors[index],
                id: 'Duplicate ID',
              }
            }
          })
        }
      })

      const primaryDefinition = kpiDraft.definitions.find(
        (def) => def.id === kpiDraft.primary_kpi_id,
      )
      let primaryError: string | null = null
      if (!kpiDraft.primary_kpi_id) {
        primaryError = 'Select a primary KPI'
      } else if (!primaryDefinition) {
        primaryError = 'Primary KPI must reference an existing definition'
      } else if (primaryDefinition.type !== 'primary') {
        primaryError = 'Primary KPI entry must have type set to Primary'
      }

      const hasRowErrors = Object.values(rowErrors).some(
        (errors) => Object.keys(errors).length > 0,
      )

      return {
        hasErrors: hasRowErrors || !!primaryError,
        rowErrors,
        primaryError,
      }
    }, [kpiDraft])

    const settingsDirty = useMemo(
      () => ({
        attribution:
          !!settingsBaseline &&
          !deepEqual(attributionDraft, settingsBaseline.attribution),
        nba:
          !!settingsBaseline && !deepEqual(nbaDraft, settingsBaseline.nba),
        mmm:
          !!settingsBaseline && !deepEqual(mmmDraft, settingsBaseline.mmm),
      }),
      [attributionDraft, mmmDraft, nbaDraft, settingsBaseline],
    )

    const sourceAliasIssues = useMemo(
      () => analyzeAliasRows(sourceAliasRows),
      [sourceAliasRows],
    )
    const mediumAliasIssues = useMemo(
      () => analyzeAliasRows(mediumAliasRows),
      [mediumAliasRows],
    )

    const taxonomyDirty = useMemo(() => {
      if (!taxonomyBaseline) return false

      const normalizedBaselineRules = [...taxonomyBaseline.channel_rules].sort(
        (a, b) => a.priority - b.priority || a.name.localeCompare(b.name),
      )
      const normalizedDraftRules = [...taxonomyDraft.channel_rules].sort(
        (a, b) => a.priority - b.priority || a.name.localeCompare(b.name),
      )

      return (
        !deepEqual(normalizedDraftRules, normalizedBaselineRules) ||
        !deepEqual(taxonomyDraft.source_aliases, taxonomyBaseline.source_aliases) ||
        !deepEqual(taxonomyDraft.medium_aliases, taxonomyBaseline.medium_aliases)
      )
    }, [taxonomyBaseline, taxonomyDraft.channel_rules, taxonomyDraft.medium_aliases, taxonomyDraft.source_aliases])

    const kpiDirty = useMemo(() => {
      if (!kpiBaseline) return false
      return !deepEqual(kpiDraft, kpiBaseline)
    }, [kpiBaseline, kpiDraft])

    const measurementDirty = useMemo(() => {
      if (!selectedModelConfigId) return false
      const jsonDirty = modelConfigJson.trim() !== modelConfigBaseline.trim()
      const noteDirty =
        modelConfigChangeNote.trim() !== modelConfigBaselineChangeNote.trim()
      return jsonDirty || noteDirty
    }, [
      modelConfigBaseline,
      modelConfigBaselineChangeNote,
      modelConfigChangeNote,
      modelConfigJson,
      selectedModelConfigId,
    ])

    const notificationsDirty = useMemo(() => {
      if (
        notificationsChannelsDraft.length !==
          notificationsChannelsBaseline.length ||
        notificationsPrefsDraft.length !== notificationsPrefsBaseline.length
      )
        return true
      const chSame =
        JSON.stringify(notificationsChannelsDraft) ===
        JSON.stringify(notificationsChannelsBaseline)
      const prSame =
        JSON.stringify(notificationsPrefsDraft) ===
        JSON.stringify(notificationsPrefsBaseline)
      return !chSame || !prSame
    }, [
      notificationsChannelsBaseline,
      notificationsChannelsDraft,
      notificationsPrefsBaseline,
      notificationsPrefsDraft,
    ])

    const dirtySections = useMemo(() => {
      const dirty: SectionKey[] = []
      if (settingsDirty.attribution) dirty.push('attribution')
      if (kpiDirty) dirty.push('kpi')
      if (taxonomyDirty) dirty.push('taxonomy')
      if (measurementDirty) dirty.push('measurement-models')
      if (settingsDirty.nba) dirty.push('nba')
      if (settingsDirty.mmm) dirty.push('mmm')
      if (notificationsDirty) dirty.push('notifications')
      return dirty
    }, [kpiDirty, measurementDirty, notificationsDirty, settingsDirty, taxonomyDirty])

    useEffect(() => {
      onDirtySectionsChange?.(dirtySections)
    }, [dirtySections, onDirtySectionsChange])

    useEffect(() => {
      if (typeof window === 'undefined') return
      const handler = (event: BeforeUnloadEvent) => {
        if (dirtySections.length > 0) {
          event.preventDefault()
          event.returnValue = ''
        }
      }
      window.addEventListener('beforeunload', handler)
      return () => window.removeEventListener('beforeunload', handler)
    }, [dirtySections.length])

    const settingsPayload = useCallback(
      (overrides: Partial<Settings>): Settings => ({
        attribution: deepClone(
          overrides.attribution ?? settingsBaseline?.attribution ?? DEFAULT_SETTINGS.attribution,
        ),
        nba: deepClone(
          overrides.nba ?? settingsBaseline?.nba ?? DEFAULT_SETTINGS.nba,
        ),
        mmm: deepClone(
          overrides.mmm ?? settingsBaseline?.mmm ?? DEFAULT_SETTINGS.mmm,
        ),
        feature_flags: deepClone(
          overrides.feature_flags ??
            settingsBaseline?.feature_flags ??
            DEFAULT_SETTINGS.feature_flags,
        ),
        revenue_config: deepClone(
          overrides.revenue_config ??
            settingsBaseline?.revenue_config ??
            DEFAULT_SETTINGS.revenue_config,
        ),
      }),
      [settingsBaseline],
    )

    const saveSectionsInternal = useCallback(
      async (sections?: SectionKey[]) => {
        const target = sections?.length ? sections : dirtySections
        if (!target.length) return true

        const settingsSections = target.filter((key) =>
          ['attribution', 'nba', 'mmm'].includes(key),
        ) as Array<'attribution' | 'nba' | 'mmm'>

        try {
          if (settingsSections.length > 0) {
            const payload = settingsPayload({})
            if (settingsSections.includes('attribution')) {
              payload.attribution = deepClone(attributionDraft)
            }
            if (settingsSections.includes('nba')) {
              payload.nba = deepClone(nbaDraft)
            }
            if (settingsSections.includes('mmm')) {
              payload.mmm = deepClone(mmmDraft)
            }
            const response = await saveSettingsMutation.mutateAsync(payload)
            const merged = response ?? payload
            setSettingsBaseline((prev) => ({
              attribution: settingsSections.includes('attribution')
                ? deepClone(merged.attribution)
                : prev?.attribution ?? deepClone(DEFAULT_SETTINGS.attribution),
              nba: settingsSections.includes('nba')
                ? deepClone(merged.nba)
                : prev?.nba ?? deepClone(DEFAULT_SETTINGS.nba),
              mmm: settingsSections.includes('mmm')
                ? deepClone(merged.mmm)
                : prev?.mmm ?? deepClone(DEFAULT_SETTINGS.mmm),
              feature_flags:
                prev?.feature_flags ?? deepClone(DEFAULT_SETTINGS.feature_flags),
              revenue_config:
                prev?.revenue_config ?? deepClone(DEFAULT_SETTINGS.revenue_config),
            }))
            if (settingsSections.includes('attribution')) {
              setAttributionDraft(deepClone(merged.attribution))
            }
            if (settingsSections.includes('nba')) {
              setNbaDraft(deepClone(merged.nba))
            }
            if (settingsSections.includes('mmm')) {
              setMmmDraft(deepClone(merged.mmm))
            }
            setLastSavedAt((prev) => ({
              ...prev,
              ...(settingsSections.includes('attribution')
                ? { attribution: new Date().toLocaleTimeString() }
                : {}),
              ...(settingsSections.includes('nba')
                ? { nba: new Date().toLocaleTimeString() }
                : {}),
              ...(settingsSections.includes('mmm')
                ? { mmm: new Date().toLocaleTimeString() }
                : {}),
            }))
          }

          for (const section of target) {
            if (settingsSections.includes(section as any)) continue
            if (section === 'taxonomy') {
              const nextSourceAliases = buildAliasObject(sourceAliasRows)
              const nextMediumAliases = buildAliasObject(mediumAliasRows)

              const nextValidationErrors: { source?: string; medium?: string } =
                {}
              if (sourceAliasIssues.duplicates.length > 0) {
                nextValidationErrors.source = `Duplicate aliases: ${sourceAliasIssues.duplicates.join(
                  ', ',
                )}`
              }
              if (mediumAliasIssues.duplicates.length > 0) {
                nextValidationErrors.medium = `Duplicate aliases: ${mediumAliasIssues.duplicates.join(
                  ', ',
                )}`
              }
              setAliasValidationErrors(nextValidationErrors)
              if (
                sourceAliasIssues.duplicates.length > 0 ||
                mediumAliasIssues.duplicates.length > 0
              ) {
                return false
              }

              const sanitizedRules = taxonomyDraft.channel_rules
                .map((rule, index) => {
                  const priority = Number.isFinite(rule.priority)
                    ? Number(rule.priority)
                    : (index + 1) * 10
                  return {
                    name: rule.name.trim(),
                    channel: rule.channel.trim(),
                    priority,
                    enabled: rule.enabled !== false,
                    source: {
                      operator: ensureMatchExpression(rule.source).operator,
                      value: ensureMatchExpression(rule.source).value.trim(),
                    },
                    medium: {
                      operator: ensureMatchExpression(rule.medium).operator,
                      value: ensureMatchExpression(rule.medium).value.trim(),
                    },
                    campaign: {
                      operator: ensureMatchExpression(rule.campaign).operator,
                      value: ensureMatchExpression(rule.campaign).value.trim(),
                    },
                  }
                })
                .sort(
                  (a, b) => a.priority - b.priority || a.name.localeCompare(b.name),
                )

              const payload: Taxonomy = {
                channel_rules: sanitizedRules as ChannelRule[],
                source_aliases: nextSourceAliases,
                medium_aliases: nextMediumAliases,
              }
              const response = await saveTaxonomyMutation.mutateAsync(payload)
              const saved = response ?? payload

              const normalizedRules = (saved.channel_rules ?? []).map(
                (rule, index) => normalizeChannelRule(rule, (index + 1) * 10),
              )
              const normalizedTaxonomy: Taxonomy = {
                channel_rules: normalizedRules,
                source_aliases: saved.source_aliases ?? {},
                medium_aliases: saved.medium_aliases ?? {},
              }

              setTaxonomyBaseline(deepClone(normalizedTaxonomy))
              setTaxonomyDraft(deepClone(normalizedTaxonomy))
              setSourceAliasRows(toAliasRows(normalizedTaxonomy.source_aliases))
              setMediumAliasRows(toAliasRows(normalizedTaxonomy.medium_aliases))
              setAliasImportErrors({})
              setAliasValidationErrors({})
              setLastSavedAt((prev) => ({
                ...prev,
                taxonomy: new Date().toLocaleTimeString(),
              }))
            }
            if (section === 'kpi') {
              if (kpiValidation.hasErrors) {
                return false
              }
              const response = await saveKpiMutation.mutateAsync(kpiDraft)
              const saved = response ?? kpiDraft
              setKpiBaseline(deepClone(saved))
              setKpiDraft(deepClone(saved))
              setLastSavedAt((prev) => ({
                ...prev,
                kpi: new Date().toLocaleTimeString(),
              }))
            }
            if (section === 'measurement-models') {
              if (!selectedModelConfigId) return false
              try {
                if (modelConfigParseError) {
                  setModelConfigError(modelConfigParseError)
                  return false
                }
                let parsed: Record<string, any>
                if (currentConfigObject) {
                  parsed = deepClone(currentConfigObject)
                } else {
                  parsed = JSON.parse(modelConfigJson || '{}')
                }
                await saveModelConfigMutation.mutateAsync({
                  id: selectedModelConfigId,
                  config: parsed,
                  changeNote: modelConfigChangeNote,
                })
                const formatted = JSON.stringify(parsed, null, 2)
                setCurrentConfigObject(parsed)
                setModelConfigJson(formatted)
                setModelConfigBaseline(formatted)
                setModelConfigBaselineChangeNote(modelConfigChangeNote)
                setModelConfigError(null)
                setModelConfigValidation(null)
                setModelConfigPreview(null)
                setLastSavedAt((prev) => ({
                  ...prev,
                  'measurement-models': new Date().toLocaleTimeString(),
                }))
              } catch (error) {
                setModelConfigError(
                  (error as Error)?.message ?? 'Config JSON is invalid',
                )
                return false
              }
            }
            if (section === 'notifications') {
              try {
                await saveNotificationsMutation.mutateAsync({
                  channelsBaseline: notificationsChannelsBaseline,
                  channelsDraft: notificationsChannelsDraft,
                  prefsDraft: notificationsPrefsDraft,
                  slackWebhookInput: notificationsSlackWebhookInput,
                })
                setLastSavedAt((prev) => ({
                  ...prev,
                  notifications: new Date().toLocaleTimeString(),
                }))
              } catch {
                return false
              }
            }
          }
          return true
        } catch (error) {
          console.error(error)
          return false
        }
      },
      [
        attributionDraft,
        dirtySections,
        kpiDraft,
        kpiValidation.hasErrors,
        modelConfigJson,
        saveKpiMutation,
        saveModelConfigMutation,
        saveNotificationsMutation,
        saveSettingsMutation,
        saveTaxonomyMutation,
        selectedModelConfigId,
        settingsPayload,
        taxonomyDraft.channel_rules,
        mediumAliasRows,
        sourceAliasRows,
        mediumAliasIssues,
        sourceAliasIssues,
        nbaDraft,
        mmmDraft,
        notificationsChannelsBaseline,
        notificationsChannelsDraft,
        notificationsPrefsDraft,
        notificationsSlackWebhookInput,
        modelConfigParseError,
        currentConfigObject,
      ],
    )

    const discardSectionsInternal = useCallback(
      (sections?: SectionKey[]) => {
        const target = sections?.length ? sections : dirtySections
        if (!target.length) return
        target.forEach((section) => {
          switch (section) {
            case 'attribution':
              if (settingsBaseline) {
                setAttributionDraft(deepClone(settingsBaseline.attribution))
              }
              break
            case 'nba':
              if (settingsBaseline) {
                setNbaDraft(deepClone(settingsBaseline.nba))
              }
              break
            case 'mmm':
              if (settingsBaseline) {
                setMmmDraft(deepClone(settingsBaseline.mmm))
              }
              break
            case 'taxonomy':
              if (taxonomyBaseline) {
              const restored = deepClone(taxonomyBaseline)
              setTaxonomyDraft(restored)
              setSourceAliasRows(toAliasRows(restored.source_aliases ?? {}))
              setMediumAliasRows(toAliasRows(restored.medium_aliases ?? {}))
              setAliasImportErrors({})
              setAliasValidationErrors({})
              }
              break
            case 'kpi':
              if (kpiBaseline) {
                setKpiDraft(deepClone(kpiBaseline))
              }
              break
            case 'measurement-models':
              setModelConfigJson(modelConfigBaseline)
              setModelConfigError(null)
            setModelConfigParseError(null)
            setModelConfigValidation(null)
            setModelConfigPreview(null)
            setMeasurementEditorMode('basic')
            setModelConfigChangeNote(modelConfigBaselineChangeNote)
            setActivationNote('')
            setActivationSummary(null)
            setActivationWarning(null)
            setImpactUnavailableReason(null)
            try {
              const parsedBaseline = modelConfigBaseline
                ? (JSON.parse(modelConfigBaseline) as Record<string, any>)
                : {}
              setCurrentConfigObject(parsedBaseline)
            } catch {
              setCurrentConfigObject(null)
            }
              break
            case 'notifications':
              setNotificationsChannelsDraft(deepClone(notificationsChannelsBaseline))
              setNotificationsPrefsDraft(deepClone(notificationsPrefsBaseline))
              setNotificationsSlackWebhookInput('')
              break
            default:
              break
          }
        })
      },
    [
      dirtySections,
      kpiBaseline,
      modelConfigBaseline,
      modelConfigBaselineChangeNote,
      notificationsChannelsBaseline,
      notificationsPrefsBaseline,
      settingsBaseline,
      taxonomyBaseline,
    ],
    )

    useImperativeHandle(
      ref,
      () => ({
        getDirtySections: () => dirtySections,
        saveSections: saveSectionsInternal,
        discardSections: discardSectionsInternal,
      }),
      [dirtySections, discardSectionsInternal, saveSectionsInternal],
    )

    const handleSectionNav = useCallback(
      (next: SectionKey) => {
        if (next === activeSection) return
        if (dirtySections.includes(activeSection)) {
          setPendingSectionChange({ target: next })
        } else {
          setActiveSection(next)
        }
      },
      [activeSection, dirtySections],
    )

    const handleConfirmSectionChange = useCallback(
      async (action: 'stay' | 'discard' | 'save') => {
        if (!pendingSectionChange) return
        if (action === 'stay') {
          setPendingSectionChange(null)
          return
        }
        if (action === 'discard') {
          discardSectionsInternal([activeSection])
          setActiveSection(pendingSectionChange.target)
          setPendingSectionChange(null)
          return
        }
        if (action === 'save') {
          const success = await saveSectionsInternal([activeSection])
          if (success) {
            setActiveSection(pendingSectionChange.target)
            setPendingSectionChange(null)
          }
        }
      },
      [activeSection, discardSectionsInternal, pendingSectionChange, saveSectionsInternal],
    )

    const sectionIsSaving = useCallback(
      (section: SectionKey) => {
        switch (section) {
          case 'attribution':
          case 'nba':
          case 'mmm':
            return saveSettingsMutation.isPending
          case 'taxonomy':
            return saveTaxonomyMutation.isPending
          case 'kpi':
            return saveKpiMutation.isPending
          case 'measurement-models':
            return (
              saveModelConfigMutation.isPending ||
              activateModelConfigMutation.isPending
            )
          case 'notifications':
            return saveNotificationsMutation.isPending
          default:
            return false
        }
      },
      [
        activateModelConfigMutation.isPending,
        saveKpiMutation.isPending,
        saveModelConfigMutation.isPending,
        saveNotificationsMutation.isPending,
        saveSettingsMutation.isPending,
        saveTaxonomyMutation.isPending,
      ],
    )

    const handleReset = useCallback(
      (section: SectionKey) => {
        discardSectionsInternal([section])
      },
      [discardSectionsInternal],
    )

    const handleSave = useCallback(
      async (section: SectionKey) => {
        const success = await saveSectionsInternal([section])
        toastIdRef.current += 1
        const successMessages: Partial<Record<SectionKey, string>> = {
          attribution: 'Attribution defaults saved',
          kpi: 'KPI changes saved',
          taxonomy: 'Taxonomy saved',
          'measurement-models': 'Measurement model updated',
          nba: 'NBA defaults saved',
          mmm: 'MMM defaults saved',
          notifications: 'Notification settings saved',
        }
        const errorMessages: Partial<Record<SectionKey, string>> = {
          attribution: 'Failed to save attribution defaults',
          kpi: 'Failed to save KPIs',
          taxonomy: 'Failed to save taxonomy',
          'measurement-models': 'Failed to save measurement model',
          nba: 'Failed to save NBA defaults',
          mmm: 'Failed to save MMM defaults',
          notifications: 'Failed to save notification settings',
        }
        setToast({
          id: toastIdRef.current,
          type: success ? 'success' : 'error',
          message: success
            ? successMessages[section] ?? 'Changes saved'
            : errorMessages[section] ?? 'Unable to save changes',
        })
        return success
      },
      [saveSectionsInternal],
    )

    const handleRunNbaTest = useCallback(async () => {
      setIsTestingNba(true)
      setNbaTestError(null)
      try {
        const payload = {
          settings: {
            ...nbaDraft,
            excluded_channels: Array.from(
              new Set(
                (nbaDraft.excluded_channels ?? [])
                  .map((channel) => channel.trim())
                  .filter(Boolean),
              ),
            ),
          },
          path_prefix: nbaTestPrefix,
          level: nbaTestLevel,
        }
        const data = await apiSendJson<NBATestResult>('/api/nba/test', 'POST', payload, {
          fallbackMessage: 'Failed to test recommendations',
        })
        setNbaTestResult(data)
        setNbaTestError(null)
      } catch (error) {
        setNbaTestResult(null)
        setNbaTestError(
          (error as Error)?.message ?? 'Failed to test recommendations',
        )
      } finally {
        setIsTestingNba(false)
      }
    }, [nbaDraft, nbaTestLevel, nbaTestPrefix])

    const activeMeta = SECTION_META[activeSection]

    if (
      settingsQuery.isLoading &&
      !settingsBaseline &&
      taxonomyQuery.isLoading &&
      !taxonomyBaseline
    ) {
      return (
        <div
          style={{
            padding: t.space.xxl,
            borderRadius: t.radius.lg,
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            textAlign: 'center',
            color: t.color.textSecondary,
            boxShadow: t.shadowSm,
          }}
        >
          Loading settings…
        </div>
      )
    }

    if (settingsQuery.isError && !settingsBaseline) {
      return (
        <div
          style={{
            padding: t.space.xxl,
            borderRadius: t.radius.lg,
            background: t.color.surface,
            border: `1px solid ${t.color.danger}`,
            color: t.color.danger,
          }}
        >
          Failed to load settings: {(settingsQuery.error as Error)?.message}
        </div>
      )
    }

    const renderAttribution = () => {
      const cardStyle: CSSProperties = {
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.md,
        padding: t.space.lg,
        display: 'grid',
        gap: t.space.md,
        background: t.color.surface,
        boxShadow: t.shadowSm,
      }
      const labelStyle: CSSProperties = {
        fontSize: t.font.sizeSm,
        fontWeight: t.font.weightMedium,
        color: t.color.textSecondary,
      }
      const helperTextStyle: CSSProperties = {
        fontSize: t.font.sizeXs,
        color: t.color.textMuted,
      }
      const inputBaseStyle: CSSProperties = {
        padding: `${t.space.sm}px ${t.space.md}px`,
        borderRadius: t.radius.sm,
        border: `1px solid ${t.color.border}`,
        fontSize: t.font.sizeSm,
        width: '100%',
      }

      const baselineAttribution =
        settingsBaseline?.attribution ?? DEFAULT_SETTINGS.attribution
      const attributionDirty = dirtySections.includes('attribution')
      const attributionSaving = sectionIsSaving('attribution')
      const attributionHasErrors = Object.keys(attributionErrors).length > 0
      const attributionLastSaved = lastSavedAt.attribution

      const firstWeight = Number.isFinite(attributionDraft.position_first_pct)
        ? Number(attributionDraft.position_first_pct)
        : 0
      const lastWeight = Number.isFinite(attributionDraft.position_last_pct)
        ? Number(attributionDraft.position_last_pct)
        : 0
      const middleShare = Math.max(0, 1 - firstWeight - lastWeight)

      const windowImpactText =
        attributionPreviewStatus === 'ready' && attributionPreview
          ? (() => {
              const from = baselineAttribution.lookback_window_days
              const to = attributionDraft.lookback_window_days
              const impact = attributionPreview.windowImpactCount
              if (impact === 0) {
                if (attributionPreview.windowDirection === 'tighten') {
                  return `Tightening the window from ${from} → ${to} days does not drop any existing journeys.`
                }
                if (attributionPreview.windowDirection === 'loosen') {
                  return `Expanding the window from ${from} → ${to} days keeps all current journeys eligible.`
                }
                return `Window remains ${to} days. No journeys change eligibility.`
              }
              if (attributionPreview.windowDirection === 'tighten') {
                return `${impact} journey${impact === 1 ? '' : 's'} would fall outside the ${to}-day window (current window: ${from} days).`
              }
              if (attributionPreview.windowDirection === 'loosen') {
                return `${impact} journey${impact === 1 ? '' : 's'} would become eligible with the ${to}-day window (current window: ${from} days).`
              }
              return `${impact} journey${impact === 1 ? '' : 's'} change eligibility under the proposed window.`
            })()
          : null

      const convertedImpactText =
        attributionPreviewStatus === 'ready' && attributionPreview
          ? (() => {
              const impact = attributionPreview.useConvertedFlagImpact
              if (attributionPreview.useConvertedFlagDirection === 'none') {
                return baselineAttribution.use_converted_flag
                  ? 'Converted flag remains enabled. Journeys marked as not converted stay excluded.'
                  : 'Converted flag remains disabled. All journeys continue to count as conversions.'
              }
              if (impact === 0) {
                return baselineAttribution.use_converted_flag
                  ? 'Disabling the converted flag would not add additional journeys (none are marked as unconverted).'
                  : 'Enabling the converted flag would not remove journeys (none are marked as unconverted).'
              }
              if (attributionPreview.useConvertedFlagDirection === 'more_included') {
                return `Disabling the converted flag would include ${impact} journey${impact === 1 ? '' : 's'} currently marked as not converted.`
              }
              return `Enabling the converted flag would exclude ${impact} journey${impact === 1 ? '' : 's'} that are currently included without the flag.`
            })()
          : null

      const previewUnavailableMessage =
        attributionPreviewStatus === 'unavailable'
          ? attributionPreview?.reason ?? 'Preview unavailable'
          : null

      const previewErrorMessage =
        attributionPreviewStatus === 'error' ? attributionPreviewError : null

      return (
        <div style={{ display: 'grid', gap: t.space.xl }}>
          <div style={{ display: 'grid', gap: t.space.lg }}>
            <div style={cardStyle}>
              <div style={{ display: 'grid', gap: t.space.xs }}>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeMd,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Windows & eligibility
                </h3>
                <p style={helperTextStyle}>
                  Define how far back attribution looks and which journeys qualify by
                  default.
                </p>
              </div>
              <div
                style={{
                  display: 'grid',
                  gap: t.space.md,
                  gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                }}
              >
                <label style={{ display: 'grid', gap: 6 }}>
                  <span style={labelStyle}>Attribution window (days)</span>
                  <input
                    type="number"
                    min={1}
                    max={365}
                    value={attributionDraft.lookback_window_days}
                    onChange={(e) =>
                      setAttributionDraft((prev) => ({
                        ...prev,
                        lookback_window_days: Number(e.target.value),
                      }))
                    }
                    style={{
                      ...inputBaseStyle,
                      border: `1px solid ${
                        attributionErrors.lookback_window_days
                          ? t.color.danger
                          : t.color.border
                      }`,
                    }}
                  />
                  <span style={helperTextStyle}>
                    Journeys whose conversions occur beyond this window are excluded
                    from default attribution.
                  </span>
                  {attributionErrors.lookback_window_days && (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {attributionErrors.lookback_window_days}
                    </span>
                  )}
                </label>
              </div>
              <label
                style={{
                  display: 'flex',
                  gap: t.space.sm,
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.sm,
                  background: t.color.bgSubtle,
                  alignItems: 'flex-start',
                }}
              >
                <input
                  id="use-converted-flag"
                  type="checkbox"
                  checked={attributionDraft.use_converted_flag}
                  onChange={(e) =>
                    setAttributionDraft((prev) => ({
                      ...prev,
                      use_converted_flag: e.target.checked,
                    }))
                  }
                  style={{ marginTop: 4 }}
                />
                <div style={{ display: 'grid', gap: 4 }}>
                  <label
                    htmlFor="use-converted-flag"
                    style={{ fontSize: t.font.sizeSm, color: t.color.text }}
                  >
                    Respect the <code>converted</code> flag
                  </label>
                  <span style={helperTextStyle}>
                    When enabled, journeys marked as not converted are excluded from
                    attribution. When disabled, every journey contributes regardless
                    of that flag.
                  </span>
                </div>
              </label>
            </div>

            <div style={cardStyle}>
              <div style={{ display: 'grid', gap: t.space.xs }}>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeMd,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Model parameters
                </h3>
                <p style={helperTextStyle}>
                  Fine-tune how credit decays, how first/last touches are weighted, and
                  how much journey data Markov requires.
                </p>
              </div>

              <div
                style={{
                  display: 'grid',
                  gap: t.space.md,
                  gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                }}
              >
                <label style={{ display: 'grid', gap: 6 }}>
                  <span style={labelStyle}>Time-decay half-life (days)</span>
                  <input
                    type="number"
                    min={1}
                    max={60}
                    value={attributionDraft.time_decay_half_life_days}
                    onChange={(e) =>
                      setAttributionDraft((prev) => ({
                        ...prev,
                        time_decay_half_life_days: Number(e.target.value),
                      }))
                    }
                    style={{
                      ...inputBaseStyle,
                      border: `1px solid ${
                        attributionErrors.time_decay_half_life_days
                          ? t.color.danger
                          : t.color.border
                      }`,
                    }}
                  />
                  <span style={helperTextStyle}>
                    Controls how quickly credit fades for mid-journey touches. Lower
                    values emphasise recent activity.
                  </span>
                  {attributionErrors.time_decay_half_life_days && (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {attributionErrors.time_decay_half_life_days}
                    </span>
                  )}
                </label>

                <div style={{ display: 'grid', gap: t.space.sm }}>
                  <span style={labelStyle}>Position-based weights</span>
                  <div style={{ display: 'flex', gap: t.space.sm }}>
                    <div style={{ flex: 1, display: 'grid', gap: 4 }}>
                      <input
                        type="number"
                        min={0}
                        max={1}
                        step={0.05}
                        value={attributionDraft.position_first_pct}
                        onChange={(e) =>
                          setAttributionDraft((prev) => ({
                            ...prev,
                            position_first_pct: Number(e.target.value),
                          }))
                        }
                        style={{
                          ...inputBaseStyle,
                          border: `1px solid ${
                            attributionErrors.position_first_pct
                              ? t.color.danger
                              : t.color.border
                          }`,
                        }}
                      />
                      <span style={helperTextStyle}>First-touch weight</span>
                      {attributionErrors.position_first_pct && (
                        <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                          {attributionErrors.position_first_pct}
                        </span>
                      )}
                    </div>
                    <div style={{ flex: 1, display: 'grid', gap: 4 }}>
                      <input
                        type="number"
                        min={0}
                        max={1}
                        step={0.05}
                        value={attributionDraft.position_last_pct}
                        onChange={(e) =>
                          setAttributionDraft((prev) => ({
                            ...prev,
                            position_last_pct: Number(e.target.value),
                          }))
                        }
                        style={{
                          ...inputBaseStyle,
                          border: `1px solid ${
                            attributionErrors.position_last_pct
                              ? t.color.danger
                              : t.color.border
                          }`,
                        }}
                      />
                      <span style={helperTextStyle}>Last-touch weight</span>
                      {attributionErrors.position_last_pct && (
                        <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                          {attributionErrors.position_last_pct}
                        </span>
                      )}
                    </div>
                  </div>
                  <span style={helperTextStyle}>
                    Remaining {middleShare.toFixed(2)} credit is distributed across
                    middle touches. First + last must stay at or below 1.
                  </span>
                </div>

                <label style={{ display: 'grid', gap: 6 }}>
                  <span style={{ ...labelStyle, display: 'flex', alignItems: 'center', gap: t.space.xs }}>
                    Markov minimum paths per channel
                    <span
                      title="Higher minimums reduce noise but may exclude low-volume channels from results."
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        width: 16,
                        height: 16,
                        borderRadius: 8,
                        border: `1px solid ${t.color.border}`,
                        fontSize: t.font.sizeXs,
                        lineHeight: '14px',
                        cursor: 'help',
                        color: t.color.textSecondary,
                      }}
                    >
                      ?
                    </span>
                  </span>
                  <input
                    type="number"
                    min={1}
                    value={attributionDraft.markov_min_paths}
                    onChange={(e) =>
                      setAttributionDraft((prev) => ({
                        ...prev,
                        markov_min_paths: Number(e.target.value),
                      }))
                    }
                    style={{
                      ...inputBaseStyle,
                      border: `1px solid ${
                        attributionErrors.markov_min_paths
                          ? t.color.danger
                          : t.color.border
                      }`,
                    }}
                  />
                  <span style={helperTextStyle}>
                    Channels with fewer journeys than this threshold are ignored in
                    Markov calculations.
                  </span>
                  {attributionErrors.markov_min_paths && (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {attributionErrors.markov_min_paths}
                    </span>
                  )}
                </label>
              </div>
            </div>

            <div style={cardStyle}>
              <div style={{ display: 'grid', gap: t.space.xs }}>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeMd,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Conversion value rules
                </h3>
                <p style={helperTextStyle}>
                  Set thresholds that guard against zero-value conversions skewing
                  attribution.
                </p>
              </div>
              <label style={{ display: 'grid', gap: 6 }}>
                <span style={labelStyle}>Minimum conversion value</span>
                <input
                  type="number"
                  min={0}
                  value={attributionDraft.min_conversion_value}
                  onChange={(e) =>
                    setAttributionDraft((prev) => ({
                      ...prev,
                      min_conversion_value: Number(e.target.value),
                    }))
                  }
                  style={{
                    ...inputBaseStyle,
                    border: `1px solid ${
                      attributionErrors.min_conversion_value
                        ? t.color.danger
                        : t.color.border
                    }`,
                  }}
                />
                <span style={helperTextStyle}>
                  Conversions below this value are ignored when attribution runs.
                </span>
                {attributionErrors.min_conversion_value && (
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                    {attributionErrors.min_conversion_value}
                  </span>
                )}
              </label>
            </div>
          </div>

          <div style={cardStyle}>
            <div style={{ display: 'grid', gap: t.space.xs }}>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Impact preview
              </h3>
              <p style={helperTextStyle}>
                This affects attribution runs going forward. Preview is based on the
                journeys currently loaded.
              </p>
            </div>

            {attributionPreviewStatus === 'idle' && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Adjust the defaults to estimate how many journeys would change.
              </span>
            )}

            {attributionPreviewStatus === 'loading' && (
              <span style={helperTextStyle}>Calculating preview…</span>
            )}

            {attributionPreviewStatus === 'blocked' && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Resolve validation errors to see the impact preview.
              </span>
            )}

            {previewUnavailableMessage && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                {previewUnavailableMessage}
              </span>
            )}

            {previewErrorMessage && (
              <span style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>
                {previewErrorMessage}
              </span>
            )}

            {attributionPreviewStatus === 'ready' && attributionPreview && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <ul
                  style={{
                    margin: 0,
                    paddingLeft: t.space.lg,
                    color: t.color.textSecondary,
                    fontSize: t.font.sizeSm,
                    display: 'grid',
                    gap: t.space.xs,
                  }}
                >
                  {windowImpactText && <li>{windowImpactText}</li>}
                  {convertedImpactText && <li>{convertedImpactText}</li>}
                  <li>
                    Total journeys analysed: {attributionPreview.totalJourneys.toLocaleString()}
                  </li>
                </ul>
              </div>
            )}
          </div>

          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              background: t.color.bgSubtle,
              padding: t.space.md,
              display: 'flex',
              flexWrap: 'wrap',
              justifyContent: 'space-between',
              gap: t.space.sm,
              alignItems: 'center',
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              {attributionLastSaved
                ? `Last saved at ${attributionLastSaved}`
                : 'Not saved yet'}
            </div>
            <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center' }}>
              <button
                type="button"
                onClick={() => handleReset('attribution')}
                disabled={!attributionDirty || attributionSaving}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: 'transparent',
                  color:
                    !attributionDirty || attributionSaving
                      ? t.color.textMuted
                      : t.color.text,
                  cursor:
                    !attributionDirty || attributionSaving ? 'not-allowed' : 'pointer',
                  fontSize: t.font.sizeSm,
                }}
              >
                Reset
              </button>
              <button
                type="button"
                onClick={() => void handleSave('attribution')}
                disabled={!attributionDirty || attributionSaving || attributionHasErrors}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background:
                    !attributionDirty || attributionSaving || attributionHasErrors
                      ? t.color.borderLight
                      : t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor:
                    !attributionDirty || attributionSaving || attributionHasErrors
                      ? 'not-allowed'
                      : 'pointer',
                  opacity: attributionSaving ? 0.7 : 1,
                }}
              >
                {attributionSaving ? 'Saving…' : 'Save defaults'}
              </button>
            </div>
          </div>

          {attributionHasErrors && (
            <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
              Resolve validation issues above before saving.
            </span>
          )}
        </div>
      )
    }

    const renderNba = () => {
      const cardStyle: CSSProperties = {
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.md,
        padding: t.space.lg,
        display: 'grid',
        gap: t.space.md,
        background: t.color.surface,
        boxShadow: t.shadowSm,
      }
      const labelStyle: CSSProperties = {
        fontSize: t.font.sizeSm,
        fontWeight: t.font.weightMedium,
        color: t.color.textSecondary,
      }
      const helperTextStyle: CSSProperties = {
        fontSize: t.font.sizeXs,
        color: t.color.textMuted,
      }
      const inputStyle: CSSProperties = {
        padding: `${t.space.sm}px ${t.space.md}px`,
        borderRadius: t.radius.sm,
        border: `1px solid ${t.color.border}`,
        fontSize: t.font.sizeSm,
        background: t.color.surface,
      }
      const advancedToggleStyle: CSSProperties = {
        alignSelf: 'flex-start',
        padding: `${t.space.sm}px ${t.space.md}px`,
        borderRadius: t.radius.sm,
        border: `1px solid ${t.color.border}`,
        background: 'transparent',
        color: t.color.text,
        fontSize: t.font.sizeXs,
        fontWeight: t.font.weightMedium,
        cursor: 'pointer',
      }
      const confidenceBadgeBase: CSSProperties = {
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        padding: '2px 10px',
        borderRadius: 999,
        fontSize: t.font.sizeXs,
        fontWeight: t.font.weightMedium,
      }

      const nbaDirty = dirtySections.includes('nba')
      const nbaSaving = sectionIsSaving('nba')
      const nbaHasErrors = Object.keys(nbaErrors).length > 0
      const nbaLastSaved = lastSavedAt.nba
      const normalizedExcluded = Array.from(
        new Set(
          (nbaDraft.excluded_channels ?? [])
            .map((channel) => channel.trim())
            .filter(Boolean),
        ),
      ).sort()
      const excludedOptions = Array.from(
        new Set([...SUGGESTED_CHANNELS, ...normalizedExcluded]),
      )

      const previewUnavailableMessage =
        nbaPreviewStatus === 'unavailable'
          ? nbaPreview?.reason ?? 'Preview unavailable'
          : null
      const previewErrorMessage =
        nbaPreviewStatus === 'error' ? nbaPreviewError : null
      const baselineRate = nbaTestResult?.baselineConversionRate ?? 0
      const testRecommendations = nbaTestResult?.recommendations ?? []

      const formatPercentDisplay = (value: number | null | undefined) => {
        if (value === null || value === undefined || Number.isNaN(value)) {
          return '—'
        }
        const percent = value * 100
        const decimals = Math.abs(percent) < 1 ? 2 : 1
        return `${percent.toFixed(decimals).replace(/\.?0+$/, '')}%`
      }

      const prettyChannel = (channel: string) =>
        channel
          .split('_')
          .map(
            (part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase(),
          )
          .join(' ')

      const handlePresetChange = (next: NbaPresetKey | 'custom') => {
        if (next === 'custom') {
          setShowNbaAdvanced(true)
          return
        }
        const preset = NBA_PRESETS[next]
        setNbaDraft((prev) => {
          const nextMinNextSupport =
            prev.min_next_support === prev.min_prefix_support
              ? preset.min_prefix_support
              : prev.min_next_support
          return {
            ...prev,
            ...preset,
            min_next_support: Math.max(1, nextMinNextSupport),
          }
        })
      }

      const toggleExcludedChannel = (channel: string) => {
        setNbaDraft((prev) => {
          const exists = prev.excluded_channels.includes(channel)
          const next = exists
            ? prev.excluded_channels.filter((item) => item !== channel)
            : [...prev.excluded_channels, channel]
          return {
            ...prev,
            excluded_channels: Array.from(
              new Set(next.map((item) => item.trim()).filter(Boolean)),
            ),
          }
        })
      }

      const confidenceFor = (rec: NBATestRecommendationRow) => {
        const uplift =
          rec.uplift_pct ??
          (baselineRate > 0
            ? (rec.conversion_rate - baselineRate) / baselineRate
            : null)
        if (
          rec.count >= Math.max(1, nbaDraft.min_next_support * 3) &&
          (uplift ?? 0) >= 0.2
        ) {
          return {
            label: 'High confidence',
            style: {
              ...confidenceBadgeBase,
              color: t.color.success,
              border: `1px solid ${t.color.success}`,
            },
          }
        }
        if (
          rec.count >= Math.max(1, nbaDraft.min_next_support * 2) &&
          (uplift ?? 0) >= 0.1
        ) {
          return {
            label: 'Medium confidence',
            style: {
              ...confidenceBadgeBase,
              color: t.color.warning,
              border: `1px solid ${t.color.warning}`,
            },
          }
        }
        return {
          label: 'Exploratory',
          style: {
            ...confidenceBadgeBase,
            color: t.color.textSecondary,
            border: `1px solid ${t.color.border}`,
          },
        }
      }

      return (
        <div style={{ display: 'grid', gap: t.space.xl }}>
          <div style={cardStyle}>
            <div style={{ display: 'grid', gap: t.space.xs }}>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Recommendation strictness
              </h3>
              <p style={helperTextStyle}>
                Pick a preset to match the governance level you want. Advanced
                controls let you fine-tune thresholds for demanding enterprise
                teams.
              </p>
            </div>

            <label style={{ display: 'grid', gap: 6, maxWidth: 360 }}>
              <span style={labelStyle}>Preset</span>
              <select
                value={nbaActivePreset}
                onChange={(e) =>
                  handlePresetChange(e.target.value as NbaPresetKey | 'custom')
                }
                style={{
                  ...inputStyle,
                  cursor: 'pointer',
                  background: t.color.surface,
                }}
              >
                <option value="conservative">
                  Conservative · Highest thresholds
                </option>
                <option value="balanced">Balanced · Recommended default</option>
                <option value="aggressive">
                  Aggressive · Explore more ideas
                </option>
                <option value="custom" disabled>
                  Custom (from advanced controls)
                </option>
              </select>
              <span style={helperTextStyle}>
                Selecting a preset updates journeys per prefix, minimum
                conversion rate, and prefix depth.
              </span>
            </label>

            <ul
              style={{
                margin: 0,
                paddingLeft: t.space.lg,
                color: t.color.textSecondary,
                fontSize: t.font.sizeSm,
                display: 'grid',
                gap: t.space.xs,
              }}
            >
              <li>
                Journeys per prefix ≥ {nbaDraft.min_prefix_support.toLocaleString()}
              </li>
              <li>
                Conversion rate ≥ {formatPercentDisplay(nbaDraft.min_conversion_rate)}
              </li>
              <li>Prefix depth ≤ {nbaDraft.max_prefix_depth}</li>
            </ul>

            <button
              type="button"
              onClick={() => setShowNbaAdvanced((prev) => !prev)}
              style={advancedToggleStyle}
            >
              {showNbaAdvanced ? 'Hide advanced controls' : 'Show advanced controls'}
            </button>

            {showNbaAdvanced && (
              <div
                style={{
                  display: 'grid',
                  gap: t.space.md,
                }}
              >
                <div
                  style={{
                    display: 'grid',
                    gap: t.space.md,
                    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                  }}
                >
                  <label style={{ display: 'grid', gap: 6 }}>
                    <span style={labelStyle}>Minimum journeys per prefix</span>
                    <input
                      type="number"
                      min={1}
                      value={nbaDraft.min_prefix_support}
                      onChange={(e) => {
                        const nextValue = Math.max(
                          1,
                          Math.round(Number(e.target.value) || 0),
                        )
                        setNbaDraft((prev) => ({
                          ...prev,
                          min_prefix_support: nextValue,
                        }))
                      }}
                      style={{
                        ...inputStyle,
                        border: `1px solid ${
                          nbaErrors.min_prefix_support
                            ? t.color.danger
                            : t.color.border
                        }`,
                      }}
                    />
                    <span style={helperTextStyle}>
                      Higher values favour well-travelled prefixes and reduce noise.
                    </span>
                    {nbaErrors.min_prefix_support && (
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {nbaErrors.min_prefix_support}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: 6 }}>
                    <span style={labelStyle}>Minimum conversion rate (%)</span>
                    <input
                      type="text"
                      inputMode="decimal"
                      value={nbaConversionRateInput}
                      onChange={(e) => {
                        const nextValue = e.target.value
                        setNbaConversionRateInput(nextValue)
                        const normalized = normalizeNumericString(nextValue)
                        const parsed = Number(normalized)
                        if (Number.isFinite(parsed)) {
                          setNbaDraft((prev) => ({
                            ...prev,
                            min_conversion_rate: parsed / 100,
                          }))
                        }
                      }}
                      onBlur={() =>
                        setNbaConversionRateInput(
                          formatPercentInput(nbaDraft.min_conversion_rate),
                        )
                      }
                      placeholder="e.g. 1.5"
                      style={{
                        ...inputStyle,
                        border: `1px solid ${
                          nbaErrors.min_conversion_rate
                            ? t.color.danger
                            : t.color.border
                        }`,
                      }}
                    />
                    <span style={helperTextStyle}>
                      Guard against low-quality suggestions. Uses dot decimals (1.5).
                    </span>
                    {nbaErrors.min_conversion_rate && (
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {nbaErrors.min_conversion_rate}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: 6 }}>
                    <span style={labelStyle}>Maximum prefix depth</span>
                    <input
                      type="number"
                      min={0}
                      max={10}
                      value={nbaDraft.max_prefix_depth}
                      onChange={(e) => {
                        const nextValue = Math.max(
                          0,
                          Math.round(Number(e.target.value) || 0),
                        )
                        setNbaDraft((prev) => ({
                          ...prev,
                          max_prefix_depth: nextValue,
                        }))
                      }}
                      style={{
                        ...inputStyle,
                        border: `1px solid ${
                          nbaErrors.max_prefix_depth
                            ? t.color.danger
                            : t.color.border
                        }`,
                      }}
                    />
                    <span style={helperTextStyle}>
                      Limits how far into a journey we look when proposing next steps.
                    </span>
                    {nbaErrors.max_prefix_depth && (
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {nbaErrors.max_prefix_depth}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: 6 }}>
                    <span style={labelStyle}>
                      Minimum support for suggested next (N)
                    </span>
                    <input
                      type="number"
                      min={1}
                      value={nbaDraft.min_next_support}
                      onChange={(e) => {
                        const nextValue = Math.max(
                          1,
                          Math.round(Number(e.target.value) || 0),
                        )
                        setNbaDraft((prev) => ({
                          ...prev,
                          min_next_support: nextValue,
                        }))
                      }}
                      style={{
                        ...inputStyle,
                        border: `1px solid ${
                          nbaErrors.min_next_support
                            ? t.color.danger
                            : t.color.border
                        }`,
                      }}
                    />
                    <span style={helperTextStyle}>
                      Ensures the recommended action itself has enough observations.
                    </span>
                    {nbaErrors.min_next_support && (
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {nbaErrors.min_next_support}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: 6 }}>
                    <span style={labelStyle}>Max suggestions per prefix</span>
                    <input
                      type="number"
                      min={1}
                      max={10}
                      value={nbaDraft.max_suggestions_per_prefix}
                      onChange={(e) => {
                        const nextValue = Math.max(
                          1,
                          Math.round(Number(e.target.value) || 0),
                        )
                        setNbaDraft((prev) => ({
                          ...prev,
                          max_suggestions_per_prefix: Math.min(nextValue, 10),
                        }))
                      }}
                      style={{
                        ...inputStyle,
                        border: `1px solid ${
                          nbaErrors.max_suggestions_per_prefix
                            ? t.color.danger
                            : t.color.border
                        }`,
                      }}
                    />
                    <span style={helperTextStyle}>
                      Keeps the list of actions focused for sales and marketing teams.
                    </span>
                    {nbaErrors.max_suggestions_per_prefix && (
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {nbaErrors.max_suggestions_per_prefix}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: 6 }}>
                    <span style={labelStyle}>Minimum uplift over baseline (%)</span>
                    <input
                      type="text"
                      inputMode="decimal"
                      value={nbaUpliftInput}
                      onChange={(e) => {
                        const nextValue = e.target.value
                        setNbaUpliftInput(nextValue)
                        const normalized = normalizeNumericString(nextValue)
                        if (!normalized) {
                          setNbaDraft((prev) => ({
                            ...prev,
                            min_uplift_pct: null,
                          }))
                          return
                        }
                        const parsed = Number(normalized)
                        if (Number.isFinite(parsed)) {
                          setNbaDraft((prev) => ({
                            ...prev,
                            min_uplift_pct: parsed / 100,
                          }))
                        }
                      }}
                      onBlur={() =>
                        setNbaUpliftInput(
                          formatPercentInput(nbaDraft.min_uplift_pct),
                        )
                      }
                      placeholder="optional"
                      style={{
                        ...inputStyle,
                        border: `1px solid ${
                          nbaErrors.min_uplift_pct
                            ? t.color.danger
                            : t.color.border
                        }`,
                      }}
                    />
                    <span style={helperTextStyle}>
                      Require the next step to outperform the baseline by a percentage.
                    </span>
                    {nbaErrors.min_uplift_pct && (
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {nbaErrors.min_uplift_pct}
                      </span>
                    )}
                  </label>
                </div>

                <div style={{ display: 'grid', gap: t.space.sm }}>
                  <span style={labelStyle}>Excluded channels</span>
                  <span style={helperTextStyle}>
                    Channels in this list never surface as default suggestions.
                  </span>
                  <div
                    style={{
                      display: 'flex',
                      flexWrap: 'wrap',
                      gap: t.space.sm,
                    }}
                  >
                    {excludedOptions.map((channel) => {
                      const isChecked = normalizedExcluded.includes(channel)
                      return (
                        <label
                          key={channel}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: t.space.xs,
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${
                              isChecked ? t.color.accent : t.color.borderLight
                            }`,
                            background: isChecked
                              ? t.color.accentMuted
                              : t.color.bgSubtle,
                            cursor: 'pointer',
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          <input
                            type="checkbox"
                            checked={isChecked}
                            onChange={() => toggleExcludedChannel(channel)}
                          />
                          <span>{prettyChannel(channel)}</span>
                        </label>
                      )
                    })}
                  </div>
                </div>
              </div>
            )}
          </div>

          <div style={cardStyle}>
            <div style={{ display: 'grid', gap: t.space.xs }}>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Preview impact
              </h3>
              <p style={helperTextStyle}>
                Estimate how many prefixes and recommendations remain after your
                thresholds. Preview is based on loaded journeys.
              </p>
            </div>

            {nbaPreviewStatus === 'idle' && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Adjust the defaults to see projected reach for next-best-action.
              </span>
            )}

            {nbaPreviewStatus === 'loading' && (
              <span style={helperTextStyle}>Calculating preview…</span>
            )}

            {nbaPreviewStatus === 'blocked' && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Resolve validation errors to refresh the preview.
              </span>
            )}

            {previewUnavailableMessage && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                {previewUnavailableMessage.toLowerCase().includes('no journeys')
                  ? 'Load sample data or journeys to preview NBA impact.'
                  : previewUnavailableMessage}
              </span>
            )}

            {previewErrorMessage && (
              <span style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>
                {previewErrorMessage}
              </span>
            )}

            {nbaPreviewStatus === 'ready' && nbaPreview && (
              <div
                style={{
                  display: 'grid',
                  gap: t.space.sm,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                <ul
                  style={{
                    margin: 0,
                    paddingLeft: t.space.lg,
                    display: 'grid',
                    gap: t.space.xs,
                  }}
                >
                  <li>
                    Prefixes eligible:{' '}
                    <strong>
                      {nbaPreview.prefixesEligible.toLocaleString()} /{' '}
                      {nbaPreview.totalPrefixes.toLocaleString()}
                    </strong>
                  </li>
                  <li>
                    Recommendations generated:{' '}
                    <strong>
                      {nbaPreview.totalRecommendations.toLocaleString()}
                    </strong>{' '}
                    (avg {nbaPreview.averageRecommendationsPerPrefix.toFixed(2)} per
                    prefix)
                  </li>
                  <li>
                    Filtered for low support:{' '}
                    {nbaPreview.filteredBySupportPct.toFixed(1)}%
                  </li>
                  <li>
                    Filtered for conversion rate:{' '}
                    {nbaPreview.filteredByConversionPct.toFixed(1)}%
                  </li>
                </ul>
                <span style={{ fontSize: t.font.sizeXs }}>
                  Journeys analysed: {nbaPreview.datasetJourneys.toLocaleString()}
                </span>
              </div>
            )}
          </div>

          <div style={cardStyle}>
            <div style={{ display: 'grid', gap: t.space.xs }}>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Try it: recommendations console
              </h3>
              <p style={helperTextStyle}>
                Test a prefix to see the top recommendations with support,
                conversion rate, and confidence.
              </p>
            </div>

            <div
              style={{
                display: 'grid',
                gap: t.space.md,
                gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                alignItems: 'end',
              }}
            >
              <label style={{ display: 'grid', gap: 6 }}>
                <span style={labelStyle}>Path prefix</span>
                <input
                  type="text"
                  value={nbaTestPrefix}
                  onChange={(e) => setNbaTestPrefix(e.target.value)}
                  placeholder="google_ads > email"
                  style={inputStyle}
                />
                <span style={helperTextStyle}>
                  Use arrows ( &gt; ) to separate steps. Leave blank to test the start.
                </span>
              </label>

              <div style={{ display: 'grid', gap: 6 }}>
                <span style={labelStyle}>Suggest by</span>
                <div style={{ display: 'flex', gap: t.space.sm }}>
                  {(['channel', 'campaign'] as const).map((level) => {
                    const isActive = nbaTestLevel === level
                    return (
                      <button
                        key={level}
                        type="button"
                        onClick={() => setNbaTestLevel(level)}
                        style={{
                          padding: `${t.space.sm}px ${t.space.lg}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${
                            isActive ? t.color.accent : t.color.border
                          }`,
                          background: isActive
                            ? t.color.accentMuted
                            : t.color.bgSubtle,
                          color: isActive ? t.color.accent : t.color.textSecondary,
                          cursor: 'pointer',
                          fontSize: t.font.sizeSm,
                          fontWeight: t.font.weightMedium,
                        }}
                      >
                        {level === 'channel' ? 'Channel' : 'Campaign'}
                      </button>
                    )
                  })}
                </div>
              </div>

              <button
                type="button"
                onClick={() => void handleRunNbaTest()}
                disabled={isTestingNba}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background: isTestingNba ? t.color.borderLight : t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor: isTestingNba ? 'not-allowed' : 'pointer',
                  opacity: isTestingNba ? 0.7 : 1,
                  justifySelf: 'flex-start',
                }}
              >
                {isTestingNba ? 'Testing…' : 'Test recommendations'}
              </button>
            </div>

            {nbaTestError && (
              <span style={{ color: t.color.danger, fontSize: t.font.sizeSm }}>
                {nbaTestError}
              </span>
            )}

            {nbaTestResult?.reason && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                {nbaTestResult.reason.toLowerCase().includes('no journeys')
                  ? 'Load sample data or journeys to preview NBA impact.'
                  : nbaTestResult.reason}
              </span>
            )}

            {nbaTestResult?.previewAvailable === false && !nbaTestResult.reason && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Load sample data or journeys to power the test console.
              </span>
            )}

            {nbaTestResult?.previewAvailable && testRecommendations.length === 0 && (
              <span style={{ color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                No recommendations meet the current thresholds for this prefix.
              </span>
            )}

            {testRecommendations.length > 0 && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                {baselineRate > 0 && (
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                    Baseline conversion rate for this prefix:{' '}
                    {formatPercentDisplay(baselineRate)}
                  </span>
                )}
                <table
                  style={{
                    width: '100%',
                    borderCollapse: 'collapse',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  <thead>
                    <tr
                      style={{
                        textAlign: 'left',
                        color: t.color.textSecondary,
                        textTransform: 'uppercase',
                        letterSpacing: '0.05em',
                        fontSize: t.font.sizeXs,
                      }}
                    >
                      <th style={{ padding: `${t.space.sm}px ${t.space.sm}px` }}>
                        Suggestion
                      </th>
                      <th style={{ padding: `${t.space.sm}px ${t.space.sm}px` }}>
                        Support
                      </th>
                      <th style={{ padding: `${t.space.sm}px ${t.space.sm}px` }}>
                        Conversion rate
                      </th>
                      <th style={{ padding: `${t.space.sm}px ${t.space.sm}px` }}>
                        Uplift
                      </th>
                      <th style={{ padding: `${t.space.sm}px ${t.space.sm}px` }}>
                        Confidence
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {testRecommendations.map((rec) => {
                      const confidence = confidenceFor(rec)
                      return (
                        <tr key={`${rec.step}-${rec.channel}`}>
                          <td
                            style={{
                              padding: `${t.space.sm}px ${t.space.sm}px`,
                              borderTop: `1px solid ${t.color.borderLight}`,
                            }}
                          >
                            <div style={{ display: 'grid', gap: 2 }}>
                              <span style={{ color: t.color.text }}>
                                {prettyChannel(rec.channel)}
                                {rec.campaign ? ` · ${rec.campaign}` : ''}
                              </span>
                              <span
                                style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}
                              >
                                Next step: {rec.step}
                              </span>
                            </div>
                          </td>
                          <td
                            style={{
                              padding: `${t.space.sm}px ${t.space.sm}px`,
                              borderTop: `1px solid ${t.color.borderLight}`,
                            }}
                          >
                            {rec.count.toLocaleString()}
                          </td>
                          <td
                            style={{
                              padding: `${t.space.sm}px ${t.space.sm}px`,
                              borderTop: `1px solid ${t.color.borderLight}`,
                            }}
                          >
                            {formatPercentDisplay(rec.conversion_rate)}
                          </td>
                          <td
                            style={{
                              padding: `${t.space.sm}px ${t.space.sm}px`,
                              borderTop: `1px solid ${t.color.borderLight}`,
                            }}
                          >
                            {rec.uplift_pct !== null && rec.uplift_pct !== undefined
                              ? `${(rec.uplift_pct * 100).toFixed(1)}%`
                              : '—'}
                          </td>
                          <td
                            style={{
                              padding: `${t.space.sm}px ${t.space.sm}px`,
                              borderTop: `1px solid ${t.color.borderLight}`,
                            }}
                          >
                            <span style={confidence.style}>{confidence.label}</span>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              background: t.color.bgSubtle,
              padding: t.space.md,
              display: 'flex',
              flexWrap: 'wrap',
              justifyContent: 'space-between',
              gap: t.space.sm,
              alignItems: 'center',
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              {nbaLastSaved ? `Last saved at ${nbaLastSaved}` : 'Not saved yet'}
            </div>
            <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center' }}>
              <button
                type="button"
                onClick={() => handleReset('nba')}
                disabled={!nbaDirty || nbaSaving}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: 'transparent',
                  color:
                    !nbaDirty || nbaSaving
                      ? t.color.textMuted
                      : t.color.text,
                  cursor:
                    !nbaDirty || nbaSaving ? 'not-allowed' : 'pointer',
                  fontSize: t.font.sizeSm,
                }}
              >
                Reset
              </button>
              <button
                type="button"
                onClick={() => void handleSave('nba')}
                disabled={!nbaDirty || nbaSaving || nbaHasErrors}
                style={{
                  padding: `${t.space.sm}px ${t.space.lg}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background:
                    !nbaDirty || nbaSaving || nbaHasErrors
                      ? t.color.borderLight
                      : t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor:
                    !nbaDirty || nbaSaving || nbaHasErrors
                      ? 'not-allowed'
                      : 'pointer',
                  opacity: nbaSaving ? 0.7 : 1,
                }}
              >
                {nbaSaving ? 'Saving…' : 'Save defaults'}
              </button>
            </div>
          </div>

          {nbaHasErrors && (
            <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
              Resolve validation issues above before saving.
            </span>
          )}
        </div>
      )
    }

    const renderMmm = () => (
      <div style={{ maxWidth: 360 }}>
        <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span
            style={{
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Time aggregation
          </span>
          <select
            value={mmmDraft.frequency}
            onChange={(e) =>
              setMmmDraft((prev) => ({ ...prev, frequency: e.target.value }))
            }
            style={{
              padding: `${t.space.sm}px ${t.space.md}px`,
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              background: t.color.surface,
              fontSize: t.font.sizeSm,
              color: t.color.text,
            }}
          >
            <option value="W">Weekly</option>
            <option value="M">Monthly</option>
          </select>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Applied when scheduling new MMM runs. Analysts can override per run.
          </span>
        </label>
      </div>
    )

    const renderKpi = () => {
      const sortedDefinitions = kpiDraft.definitions
        .map((definition, index) => ({ definition, index }))
        .sort((a, b) => {
          if (a.definition.id === kpiDraft.primary_kpi_id) return -1
          if (b.definition.id === kpiDraft.primary_kpi_id) return 1
          return a.definition.label.localeCompare(b.definition.label)
        })

      const renderTypeBadge = (definition: KpiDefinition) => {
        const isPrimary = kpiDraft.primary_kpi_id === definition.id
        const baseStyle = {
          display: 'inline-flex',
          alignItems: 'center',
          gap: 4,
          padding: '2px 8px',
          borderRadius: 999,
          fontSize: t.font.sizeXs,
          fontWeight: t.font.weightMedium,
        }
        if (isPrimary) {
          return (
            <span
              style={{
                ...baseStyle,
                background: t.color.accentMuted,
                color: t.color.accent,
              }}
            >
              Primary KPI
            </span>
          )
        }
        return (
          <span
            style={{
              ...baseStyle,
              background: t.color.borderLight,
              color: t.color.textSecondary,
            }}
          >
            Micro conversion
          </span>
        )
      }

      return (
        <div style={{ display: 'grid', gap: t.space.lg }}>
          {kpiQuery.isError && !kpiBaseline && (
            <div
              style={{
                padding: t.space.md,
                borderRadius: t.radius.md,
                border: `1px solid ${t.color.danger}`,
                background: t.color.dangerSubtle,
                color: t.color.danger,
              }}
            >
              Failed to load KPI config: {(kpiQuery.error as Error)?.message}
            </div>
          )}

          <div
            style={{
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: t.color.bgSubtle,
              borderRadius: t.radius.md,
              border: `1px solid ${t.color.borderLight}`,
              padding: t.space.md,
            }}
          >
            Manage KPIs through guided dialogs to avoid accidental edits. The primary
            KPI drives attribution defaults and ROI calculations, while micro
            conversions enrich reporting and next-best-action models.
          </div>

          <RevenueKpiDefinitionCard />

          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: t.space.sm,
            }}
          >
            <div
              style={{
                fontSize: t.font.sizeXs,
                color: t.color.textMuted,
              }}
            >
              {kpiDraft.primary_kpi_id
                ? `Primary KPI: ${
                    kpiDraft.definitions.find(
                      (def) => def.id === kpiDraft.primary_kpi_id,
                    )?.label ?? kpiDraft.primary_kpi_id
                  }`
                : 'No primary KPI selected'}
            </div>
            <button
              type="button"
              onClick={handleOpenCreateKpi}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: 'none',
                background: t.color.accent,
                color: t.color.surface,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                cursor: 'pointer',
              }}
            >
              Add KPI
            </button>
          </div>

          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.lg,
              overflow: 'hidden',
            }}
          >
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead
                style={{
                  background: t.color.bgSubtle,
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeXs,
                  textTransform: 'uppercase',
                  letterSpacing: '0.05em',
                }}
              >
                <tr>
                  <th
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      textAlign: 'left',
                    }}
                  >
                    KPI
                  </th>
                  <th
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      textAlign: 'left',
                    }}
                  >
                    Event
                  </th>
                  <th
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      textAlign: 'left',
                    }}
                  >
                    Value field
                  </th>
                  <th
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      textAlign: 'right',
                    }}
                  >
                    Weight
                  </th>
                  <th
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      textAlign: 'right',
                    }}
                  >
                    Lookback
                  </th>
                  <th
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      textAlign: 'center',
                    }}
                  >
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedDefinitions.length === 0 ? (
                  <tr>
                    <td
                      colSpan={6}
                      style={{
                        padding: `${t.space.lg}px`,
                        textAlign: 'center',
                        color: t.color.textSecondary,
                        fontSize: t.font.sizeSm,
                      }}
                    >
                      No KPIs configured yet. Add your primary KPI to get started.
                    </td>
                  </tr>
                ) : (
                  sortedDefinitions.map(({ definition: def, index }) => {
                    const rowError = kpiValidation.rowErrors[index]
                    const hasRowErrors =
                      rowError &&
                      Object.keys(rowError).some(
                        (key) => !!rowError[key as keyof typeof rowError],
                      )
                    const isPrimary = kpiDraft.primary_kpi_id === def.id
                    return (
                      <tr
                        key={def.id}
                        onClick={() => handleOpenEditKpi(index)}
                        style={{
                          cursor: 'pointer',
                          background: hasRowErrors
                            ? t.color.dangerSubtle
                            : isPrimary
                            ? t.color.accentMuted
                            : 'transparent',
                          borderBottom: `1px solid ${t.color.borderLight}`,
                          transition: 'background-color 120ms ease',
                        }}
                      >
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            fontSize: t.font.sizeSm,
                            fontWeight: t.font.weightMedium,
                            color: t.color.text,
                          }}
                        >
                          <div
                            style={{
                              display: 'flex',
                              flexDirection: 'column',
                              gap: 4,
                            }}
                          >
                            <span>{def.label}</span>
                            <span
                              style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}
                            >
                              {def.id}
                            </span>
                          </div>
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            fontSize: t.font.sizeSm,
                            color: t.color.text,
                          }}
                        >
                          <div
                            style={{
                              display: 'flex',
                              flexDirection: 'column',
                              gap: 4,
                            }}
                          >
                            <span>{def.event_name || '—'}</span>
                            {renderTypeBadge(def)}
                          </div>
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            fontSize: t.font.sizeSm,
                            color: t.color.text,
                          }}
                        >
                          {def.value_field || '—'}
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            textAlign: 'right',
                            fontSize: t.font.sizeSm,
                            color: t.color.text,
                          }}
                        >
                          {def.weight}
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            textAlign: 'right',
                            fontSize: t.font.sizeSm,
                            color: t.color.text,
                          }}
                        >
                          {def.lookback_days ?? '—'}
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            textAlign: 'center',
                          }}
                        >
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation()
                              handleDeleteKpi(index)
                            }}
                            style={{
                              border: 'none',
                              background: 'transparent',
                              color: t.color.danger,
                              cursor: 'pointer',
                              fontSize: t.font.sizeSm,
                            }}
                          >
                            Remove
                          </button>
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>

          {kpiValidation.primaryError && (
            <div
              style={{
                border: `1px solid ${t.color.warning}`,
                background: t.color.warningSubtle,
                color: t.color.warning,
                borderRadius: t.radius.md,
                padding: t.space.sm,
                fontSize: t.font.sizeXs,
              }}
            >
              {kpiValidation.primaryError}
            </div>
          )}
          {!kpiValidation.primaryError && kpiValidation.hasErrors && (
            <div
              style={{
                border: `1px solid ${t.color.danger}`,
                background: t.color.dangerSubtle,
                color: t.color.danger,
                borderRadius: t.radius.md,
                padding: t.space.sm,
                fontSize: t.font.sizeXs,
              }}
            >
              Some KPI definitions need attention. Open a KPI to resolve field-level
              validation errors.
            </div>
          )}
        </div>
      )
    }

    const renderTaxonomy = () => {
      const cardStyle = {
        background: t.color.surface,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.borderLight}`,
        boxShadow: t.shadowSm,
        padding: t.space.lg,
        display: 'grid',
        gap: t.space.md,
      }

      const ruleEntries = taxonomyDraft.channel_rules
        .map((rule, index) => ({ rule, index }))
        .sort(
          (a, b) =>
            a.rule.priority - b.rule.priority ||
            a.rule.name.localeCompare(b.rule.name),
        )

      const summarizeRule = (rule: ChannelRule) => {
        const parts = [
          describeMatchExpression('Source', rule.source),
          describeMatchExpression('Medium', rule.medium),
        ]
        if (
          rule.campaign.operator !== 'any' &&
          rule.campaign.value &&
          rule.campaign.value.trim().length > 0
        ) {
          parts.push(describeMatchExpression('Campaign', rule.campaign))
        }
        return parts.join(' • ')
      }

      const renderStatusBadge = (rule: ChannelRule) => (
        <span
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 4,
            padding: '2px 8px',
            borderRadius: 999,
            fontSize: t.font.sizeXs,
            fontWeight: t.font.weightMedium,
            background: rule.enabled ? t.color.successMuted : t.color.borderLight,
            color: rule.enabled ? t.color.success : t.color.textSecondary,
          }}
        >
          {rule.enabled ? 'Enabled' : 'Disabled'}
        </span>
      )

      const renderAliasSection = (
        title: string,
        description: string,
        rows: AliasRow[],
        handleAdd: () => void,
        handleChange: (
          id: string,
          field: 'alias' | 'canonical',
          value: string,
        ) => void,
        handleRemove: (id: string) => void,
        issues: ReturnType<typeof analyzeAliasRows>,
        validationMessage?: string,
        importJson?: string,
        setImportJson?: (value: string) => void,
        importError?: string,
        handleImport?: () => void,
      ) => (
        <div
          style={{
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            padding: t.space.md,
            display: 'grid',
            gap: t.space.md,
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              gap: t.space.sm,
              flexWrap: 'wrap',
            }}
          >
            <div>
              <h4
                style={{
                  margin: 0,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                {title}
              </h4>
              <p
                style={{
                  margin: `${t.space.xs}px 0 0`,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                }}
              >
                {description}
              </p>
            </div>
            <button
              type="button"
              onClick={handleAdd}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px dashed ${t.color.accent}`,
                background: t.color.accentMuted,
                color: t.color.accent,
                fontSize: t.font.sizeXs,
                cursor: 'pointer',
              }}
            >
              Add alias
            </button>
          </div>

          <div
            style={{
              display: 'grid',
              gap: t.space.xs,
            }}
          >
            {rows.length === 0 ? (
              <div
                style={{
                  padding: t.space.sm,
                  borderRadius: t.radius.sm,
                  border: `1px dashed ${t.color.borderLight}`,
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeXs,
                  textAlign: 'center',
                }}
              >
                No aliases yet. Add rows to normalize raw values.
              </div>
            ) : (
              rows.map((row) => (
                <div
                  key={row.id}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr auto',
                    gap: t.space.xs,
                    alignItems: 'center',
                  }}
                >
                  <input
                    type="text"
                    value={row.alias}
                    onChange={(e) => handleChange(row.id, 'alias', e.target.value)}
                    placeholder="Alias (raw value)"
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.border}`,
                      fontSize: t.font.sizeSm,
                    }}
                  />
                  <input
                    type="text"
                    value={row.canonical}
                    onChange={(e) =>
                      handleChange(row.id, 'canonical', e.target.value)
                    }
                    placeholder="Canonical value"
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.border}`,
                      fontSize: t.font.sizeSm,
                    }}
                  />
                  <button
                    type="button"
                    onClick={() => handleRemove(row.id)}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: 'none',
                      background: 'transparent',
                      color: t.color.danger,
                      fontSize: t.font.sizeXs,
                      cursor: 'pointer',
                    }}
                  >
                    Remove
                  </button>
                </div>
              ))
            )}
          </div>

          {(issues.duplicates.length > 0 ||
            issues.hasEmptyAlias ||
            issues.hasEmptyCanonical ||
            validationMessage) && (
            <div
              style={{
                border: `1px solid ${t.color.warning}`,
                background: t.color.warningSubtle,
                color: t.color.warning,
                borderRadius: t.radius.sm,
                padding: t.space.xs,
                fontSize: t.font.sizeXs,
                display: 'grid',
                gap: 4,
              }}
            >
              {issues.duplicates.length > 0 && (
                <span>
                  Duplicate aliases: {issues.duplicates.join(', ')}. Aliases are
                  case-insensitive.
                </span>
              )}
              {issues.hasEmptyAlias && (
                <span>Some rows are missing an alias value.</span>
              )}
              {issues.hasEmptyCanonical && (
                <span>Some rows are missing a canonical value.</span>
              )}
              {validationMessage && <span>{validationMessage}</span>}
            </div>
          )}

          {importJson !== undefined &&
            setImportJson &&
            handleImport &&
            (
              <details>
                <summary
                  style={{
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                    cursor: 'pointer',
                  }}
                >
                  Advanced JSON import/export
                </summary>
                <div
                  style={{
                    display: 'grid',
                    gap: t.space.sm,
                    marginTop: t.space.sm,
                  }}
                >
                  <textarea
                    value={importJson}
                    onChange={(e) => setImportJson(e.target.value)}
                    rows={6}
                    style={{
                      width: '100%',
                      fontFamily: 'monospace',
                      fontSize: t.font.sizeXs,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.borderLight}`,
                      padding: t.space.sm,
                      background: t.color.bgSubtle,
                      whiteSpace: 'pre',
                    }}
                  />
                  {importError && (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {importError}
                    </span>
                  )}
                  <div
                    style={{
                      display: 'flex',
                      gap: t.space.sm,
                      flexWrap: 'wrap',
                    }}
                  >
                    <button
                      type="button"
                      onClick={handleImport}
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
                      Import JSON
                    </button>
                  </div>
                </div>
              </details>
            )}
        </div>
      )

      return (
        <div style={{ display: 'grid', gap: t.space.xl }}>
          {taxonomyQuery.isError && !taxonomyBaseline && (
            <div
              style={{
                padding: t.space.md,
                borderRadius: t.radius.md,
                border: `1px solid ${t.color.danger}`,
                background: t.color.dangerSubtle,
                color: t.color.danger,
              }}
            >
              Failed to load taxonomy: {(taxonomyQuery.error as Error)?.message}
            </div>
          )}

          <div style={cardStyle}>
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: t.space.sm,
                flexWrap: 'wrap',
              }}
            >
              <div>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeMd,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Channel mapping rules
                </h3>
                <p
                  style={{
                    margin: `${t.space.xs}px 0 0`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                    maxWidth: 520,
                  }}
                >
                  Rules map raw source and medium values to a standardized channel.
                  Rules execute in priority order; the first enabled rule that matches wins.
                </p>
              </div>
              <button
                type="button"
                onClick={handleOpenCreateRule}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background: t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor: 'pointer',
                }}
              >
                Add rule
              </button>
            </div>

            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr
                    style={{
                      borderBottom: `2px solid ${t.color.border}`,
                      color: t.color.textSecondary,
                      fontSize: t.font.sizeXs,
                      textTransform: 'uppercase',
                      letterSpacing: '0.06em',
                    }}
                  >
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>
                      Name
                    </th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>
                      Channel
                    </th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>
                      Match criteria
                    </th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                      Priority
                    </th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                      Status
                    </th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {ruleEntries.length === 0 ? (
                    <tr>
                      <td
                        colSpan={6}
                        style={{
                          padding: `${t.space.lg}px`,
                          textAlign: 'center',
                          color: t.color.textSecondary,
                          fontSize: t.font.sizeSm,
                        }}
                      >
                        No rules defined yet. Add a rule to start mapping traffic.
                      </td>
                    </tr>
                  ) : (
                    ruleEntries.map(({ rule, index }) => (
                      <tr
                        key={`${rule.name}-${index}`}
                        style={{
                          borderBottom: `1px solid ${t.color.borderLight}`,
                          background: rule.enabled ? 'transparent' : t.color.bgSubtle,
                        }}
                      >
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                          <div
                            style={{
                              display: 'flex',
                              flexDirection: 'column',
                              gap: 4,
                            }}
                          >
                            <span
                              style={{
                                fontSize: t.font.sizeSm,
                                fontWeight: t.font.weightSemibold,
                                color: t.color.text,
                              }}
                            >
                              {rule.name || 'Untitled rule'}
                            </span>
                            <span
                              style={{
                                fontSize: t.font.sizeXs,
                                color: t.color.textSecondary,
                              }}
                            >
                              Priority {rule.priority}
                            </span>
                          </div>
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, fontSize: t.font.sizeSm }}>
                          {rule.channel || '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, fontSize: t.font.sizeSm }}>
                          {summarizeRule(rule)}
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            textAlign: 'right',
                            fontSize: t.font.sizeSm,
                          }}
                        >
                          {rule.priority}
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            textAlign: 'center',
                          }}
                        >
                          {renderStatusBadge(rule)}
                        </td>
                        <td
                          style={{
                            padding: `${t.space.sm}px ${t.space.md}px`,
                            textAlign: 'right',
                          }}
                        >
                          <div
                            style={{
                              display: 'inline-flex',
                              gap: t.space.xs,
                              flexWrap: 'wrap',
                              justifyContent: 'flex-end',
                            }}
                          >
                            <button
                              type="button"
                              onClick={() => handleOpenEditRule(index)}
                              style={{
                                padding: `${t.space.xs}px ${t.space.sm}px`,
                                borderRadius: t.radius.sm,
                                border: `1px solid ${t.color.borderLight}`,
                                background: 'transparent',
                                color: t.color.text,
                                fontSize: t.font.sizeXs,
                                cursor: 'pointer',
                              }}
                            >
                              Edit
                            </button>
                            <button
                              type="button"
                              onClick={() => handleDuplicateRule(index)}
                              style={{
                                padding: `${t.space.xs}px ${t.space.sm}px`,
                                borderRadius: t.radius.sm,
                                border: `1px solid ${t.color.borderLight}`,
                                background: 'transparent',
                                color: t.color.text,
                                fontSize: t.font.sizeXs,
                                cursor: 'pointer',
                              }}
                            >
                              Duplicate
                            </button>
                            <button
                              type="button"
                              onClick={() => handleToggleRuleEnabled(index)}
                              style={{
                                padding: `${t.space.xs}px ${t.space.sm}px`,
                                borderRadius: t.radius.sm,
                                border: `1px solid ${t.color.borderLight}`,
                                background: 'transparent',
                                color: rule.enabled ? t.color.textSecondary : t.color.success,
                                fontSize: t.font.sizeXs,
                                cursor: 'pointer',
                              }}
                            >
                              {rule.enabled ? 'Disable' : 'Enable'}
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div style={cardStyle}>
            <div>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Taxonomy test console
              </h3>
              <p
                style={{
                  margin: `${t.space.xs}px 0 0`,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                  maxWidth: 620,
                }}
              >
                Test how a touchpoint would be normalized. Provide raw source, medium,
                and campaign values (optional). Dataset analysis highlights common
                unmapped combinations.
              </p>
            </div>

            <div
              style={{
                display: 'grid',
                gap: t.space.sm,
                gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
              }}
            >
              {[
                { label: 'Source', field: 'source' as const },
                { label: 'Medium', field: 'medium' as const },
                { label: 'Campaign', field: 'campaign' as const },
                { label: 'utm_source', field: 'utm_source' as const },
                { label: 'utm_medium', field: 'utm_medium' as const },
                { label: 'utm_campaign', field: 'utm_campaign' as const },
              ].map(({ label, field }) => (
                <label key={field} style={{ display: 'grid', gap: 4 }}>
                  <span
                    style={{
                      fontSize: t.font.sizeXs,
                      color: t.color.textSecondary,
                    }}
                  >
                    {label}
                  </span>
                  <input
                    type="text"
                    value={taxonomyTestInput[field]}
                    onChange={(e) =>
                      handleTaxonomyTestInputChange(field, e.target.value)
                    }
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.borderLight}`,
                      fontSize: t.font.sizeSm,
                    }}
                  />
                </label>
              ))}
            </div>

            <div
              style={{
                display: 'flex',
                gap: t.space.sm,
                flexWrap: 'wrap',
              }}
            >
              <button
                type="button"
                onClick={() => void handleRunTaxonomyTest()}
                disabled={isTestingTaxonomy}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background: t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor: isTestingTaxonomy ? 'wait' : 'pointer',
                }}
              >
                {isTestingTaxonomy ? 'Testing…' : 'Run test'}
              </button>
              <button
                type="button"
                onClick={() => void handleTaxonomyDatasetTest()}
                disabled={isTestingDataset}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  background: 'transparent',
                  color: t.color.text,
                  fontSize: t.font.sizeSm,
                  cursor: isTestingDataset ? 'wait' : 'pointer',
                }}
              >
                {isTestingDataset ? 'Scanning…' : 'Test against dataset'}
              </button>
            </div>

            {taxonomyTestError && (
              <div
                style={{
                  border: `1px solid ${t.color.danger}`,
                  background: t.color.dangerSubtle,
                  color: t.color.danger,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  fontSize: t.font.sizeXs,
                }}
              >
                {taxonomyTestError}
              </div>
            )}

            {taxonomyTestResult && (
              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  background: t.color.bgSubtle,
                  fontSize: t.font.sizeXs,
                  display: 'grid',
                  gap: 4,
                }}
              >
                <span>
                  Channel:{' '}
                  <strong style={{ color: t.color.text }}>
                    {taxonomyTestResult.channel}
                  </strong>
                </span>
                <span>
                  Matched rule:{' '}
                  {taxonomyTestResult.matched_rule ?? 'No rule matched'}
                </span>
                <span>
                  Confidence: {(taxonomyTestResult.confidence * 100).toFixed(1)}%
                </span>
                {taxonomyTestResult.fallback_reason && (
                  <span>Fallback: {taxonomyTestResult.fallback_reason}</span>
                )}
              </div>
            )}

            {datasetTestError && (
              <div
                style={{
                  border: `1px solid ${t.color.danger}`,
                  background: t.color.dangerSubtle,
                  color: t.color.danger,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  fontSize: t.font.sizeXs,
                }}
              >
                {datasetTestError}
              </div>
            )}

            {datasetTestRan && !datasetTestError && unknownPatterns.length === 0 && (
              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  background: t.color.bgSubtle,
                }}
              >
                All sampled touchpoints were matched by existing rules.
              </div>
            )}

            {unknownPatterns.length > 0 && (
              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  display: 'grid',
                  gap: t.space.xs,
                }}
              >
                <strong style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                  Top unmapped patterns
                </strong>
                <ul
                  style={{
                    margin: 0,
                    paddingLeft: t.space.lg,
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                  }}
                >
                  {unknownPatterns.map((pattern, idx) => (
                    <li key={`${pattern.source}-${pattern.medium}-${idx}`}>
                      {pattern.source || '—'} / {pattern.medium || '—'}
                      {pattern.campaign ? ` / ${pattern.campaign}` : ''} —{' '}
                      {pattern.count} touchpoints
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          <div style={cardStyle}>
            <div>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Aliases
              </h3>
              <p
                style={{
                  margin: `${t.space.xs}px 0 0`,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                  maxWidth: 620,
                }}
              >
                Map noisy values to normalized labels. Aliases are matched
                case-insensitively after trimming whitespace.
              </p>
            </div>

            <div
              style={{
                display: 'grid',
                gap: t.space.md,
                gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
              }}
            >
              {renderAliasSection(
                'Source aliases',
                'Example: fb → facebook',
                sourceAliasRows,
                handleAddSourceAlias,
                handleSourceAliasChange,
                handleRemoveSourceAlias,
                sourceAliasIssues,
                aliasValidationErrors.source,
                sourceAliasImportJson,
                setSourceAliasImportJson,
                aliasImportErrors.source,
                handleImportSourceAliases,
              )}
              {renderAliasSection(
                'Medium aliases',
                'Example: paid → cpc',
                mediumAliasRows,
                handleAddMediumAlias,
                handleMediumAliasChange,
                handleRemoveMediumAlias,
                mediumAliasIssues,
                aliasValidationErrors.medium,
                mediumAliasImportJson,
                setMediumAliasImportJson,
                aliasImportErrors.medium,
                handleImportMediumAliases,
              )}
            </div>
          </div>
        </div>
      )
    }

    const renderMeasurementConfigs = () => {
      const statusStyles: Record<
        string,
        { bg: string; color: string; border: string }
      > = {
        active: {
          bg: 'rgba(34, 197, 94, 0.16)',
          color: t.color.success,
          border: `1px solid ${t.color.success}`,
        },
        draft: {
          bg: t.color.warningSubtle,
          color: t.color.warning,
          border: `1px solid ${t.color.warning}`,
        },
        archived: {
          bg: t.color.borderLight,
          color: t.color.textSecondary,
          border: `1px solid ${t.color.border}`,
        },
      }

      const statusLabel = (status: string) => {
        switch (status) {
          case 'active':
            return 'Active'
          case 'archived':
            return 'Archived'
          default:
            return 'Draft'
        }
      }

      const renderStatusBadge = (status: string) => {
        const normalized = status.toLowerCase()
        const styles = statusStyles[normalized] ?? statusStyles.draft
        return (
          <span
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 4,
              padding: '2px 8px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              backgroundColor: styles.bg,
              color: styles.color,
              border: styles.border,
            }}
          >
            {statusLabel(normalized)}
          </span>
        )
      }

      const renderConfigCard = (cfg: ModelConfigSummary) => {
        const isSelected = cfg.id === selectedModelConfigId
        return (
          <div
            key={cfg.id}
            style={{
              borderRadius: t.radius.md,
              border: `1px solid ${
                isSelected ? t.color.accent : t.color.borderLight
              }`,
              background: isSelected ? t.color.accentMuted : t.color.surface,
              padding: t.space.sm,
              display: 'grid',
              gap: 6,
              transition: 'border 120ms ease, background 120ms ease',
            }}
          >
            <button
              type="button"
              onClick={() => setSelectedModelConfigId(cfg.id)}
              style={{
                textAlign: 'left',
                border: 'none',
                background: 'transparent',
                cursor: 'pointer',
                display: 'grid',
                gap: 6,
              }}
            >
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  gap: t.space.sm,
                }}
              >
                <div
                  style={{
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  {cfg.name}{' '}
                  <span style={{ color: t.color.textMuted }}>v{cfg.version}</span>
                </div>
                {renderStatusBadge(cfg.status)}
              </div>
              <div
                style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}
              >
                Created {formatDateTime(cfg.created_at)} •{' '}
                {cfg.created_by ?? 'unknown'}
              </div>
              {cfg.activated_at && (
                <div
                  style={{
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                  }}
                >
                  Activated {formatDateTime(cfg.activated_at)}
                </div>
              )}
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                {cfg.change_note?.trim() || 'No description'}
              </div>
            </button>
            {cfg.status === 'draft' && (
              <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation()
                    void handleArchiveDraft(cfg)
                  }}
                  disabled={archiveModelConfigMutation.isPending}
                  style={{
                    border: 'none',
                    background: 'transparent',
                    color: t.color.danger,
                    fontSize: t.font.sizeXs,
                    cursor: archiveModelConfigMutation.isPending ? 'wait' : 'pointer',
                  }}
                >
                  Archive
                </button>
              </div>
            )}
          </div>
        )
      }

      const noConfigs = modelConfigs.length === 0 && !modelConfigsQuery.isLoading
      const noMatches =
        filteredModelConfigs.length === 0 && !noConfigs && !modelConfigsQuery.isLoading

      const previewAvailable =
        modelConfigPreview?.preview_available &&
        modelConfigPreview.baseline &&
        modelConfigPreview.draft

      const previewRows = previewAvailable
        ? (['journeys', 'touchpoints', 'conversions'] as const).map((metric) => {
            const baselineValue =
              modelConfigPreview!.baseline![metric] ?? 0
            const draftValue = modelConfigPreview!.draft![metric] ?? 0
            const delta =
              modelConfigPreview!.deltas?.[metric] ?? draftValue - baselineValue
            const deltaPct =
              modelConfigPreview!.deltas_pct?.[metric] ?? null
            return {
              metric,
              baselineValue,
              draftValue,
              delta,
              deltaPct,
            }
          })
        : []

      const renderTokenField = (
        label: string,
        description: string,
        field:
          | 'include_channels'
          | 'exclude_channels'
          | 'include_event_types'
          | 'exclude_event_types',
        value: string,
        setValue: (next: string) => void,
        values: string[],
        suggestions: string[],
      ) => (
        <div
          key={field}
          style={{
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            padding: t.space.md,
            display: 'grid',
            gap: t.space.sm,
            background: t.color.surface,
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <span
              style={{
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              {label}
            </span>
            <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              {values.length} selected
            </span>
          </div>
          <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            {description}
          </p>
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 6,
              minHeight: 32,
              alignItems: 'center',
            }}
          >
            {values.length === 0 ? (
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                None
              </span>
            ) : (
              values.map((token) => (
                <span
                  key={token}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '2px 8px',
                    borderRadius: 999,
                    background: t.color.accentMuted,
                    color: t.color.accent,
                    fontSize: t.font.sizeXs,
                  }}
                >
                  {token}
                  <button
                    type="button"
                    onClick={() => handleRemoveTouchpointValue(field, token)}
                    style={{
                      border: 'none',
                      background: 'transparent',
                      color: t.color.accent,
                      cursor: 'pointer',
                      fontSize: t.font.sizeXs,
                    }}
                    title={`Remove ${token}`}
                  >
                    ×
                  </button>
                </span>
              ))
            )}
          </div>
          <div
            style={{ display: 'flex', alignItems: 'center', gap: t.space.xs }}
          >
            <input
              type="text"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault()
                  handleAddTouchpointValue(field, value)
                  setValue('')
                }
              }}
              placeholder="Add value"
              style={{
                flex: 1,
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
              }}
            />
            <button
              type="button"
              onClick={() => {
                handleAddTouchpointValue(field, value)
                setValue('')
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
              Add
            </button>
          </div>
          {suggestions.length > 0 && (
            <div
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: 6,
                fontSize: t.font.sizeXs,
                color: t.color.textMuted,
              }}
            >
              Suggestions:
              {suggestions.map((suggestion) => (
                <button
                  type="button"
                  key={suggestion}
                  onClick={() => handleAddTouchpointValue(field, suggestion)}
                  disabled={values.includes(suggestion)}
                  style={{
                    border: 'none',
                    background: 'transparent',
                    color: values.includes(suggestion)
                      ? t.color.textMuted
                      : t.color.accent,
                    cursor: values.includes(suggestion) ? 'default' : 'pointer',
                  }}
                >
                  {suggestion}
                </button>
              ))}
            </div>
          )}
        </div>
      )

      return (
        <div style={{ display: 'grid', gap: t.space.xl }}>
          {modelConfigsQuery.isError && (
            <div
              style={{
                padding: t.space.md,
                borderRadius: t.radius.md,
                border: `1px solid ${t.color.danger}`,
                background: t.color.dangerSubtle,
                color: t.color.danger,
              }}
            >
              Failed to load model configs:{' '}
              {(modelConfigsQuery.error as Error)?.message}
            </div>
          )}

          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: t.space.md,
            }}
          >
            <div
              style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}
            >
              <input
                type="search"
                value={modelConfigSearch}
                onChange={(e) => setModelConfigSearch(e.target.value)}
                placeholder="Search configs…"
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  fontSize: t.font.sizeSm,
                }}
              />
              <select
                value={modelConfigStatusFilter}
                onChange={(e) =>
                  setModelConfigStatusFilter(e.target.value as typeof modelConfigStatusFilter)
                }
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  fontSize: t.font.sizeSm,
                  background: t.color.surface,
                }}
              >
                <option value="all">All statuses</option>
                <option value="draft">Draft</option>
                <option value="active">Active</option>
                <option value="archived">Archived</option>
              </select>
            </div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: t.space.sm,
                flexWrap: 'wrap',
              }}
            >
              <button
                type="button"
                onClick={() => handleCreateDraft()}
                disabled={createModelConfigMutation.isPending}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.accent}`,
                  background: t.color.accentMuted,
                  color: t.color.accent,
                  fontSize: t.font.sizeXs,
                  cursor: createModelConfigMutation.isPending ? 'wait' : 'pointer',
                }}
              >
                {createModelConfigMutation.isPending ? 'Creating…' : 'New draft'}
              </button>
              <button
                type="button"
                onClick={() => createDefaultConfigMutation.mutate()}
                disabled={createDefaultConfigMutation.isPending}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background: t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeXs,
                  cursor: createDefaultConfigMutation.isPending ? 'wait' : 'pointer',
                }}
              >
                {createDefaultConfigMutation.isPending
                  ? 'Generating…'
                  : 'From template'}
              </button>
              <button
                type="button"
                onClick={() => handleDuplicateFromActive()}
                disabled={!activeConfig || cloneModelConfigMutation.isPending}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  background: 'transparent',
                  color: activeConfig
                    ? t.color.text
                    : t.color.textMuted,
                  fontSize: t.font.sizeXs,
                  cursor:
                    !activeConfig || cloneModelConfigMutation.isPending
                      ? 'not-allowed'
                      : 'pointer',
                  opacity:
                    !activeConfig || cloneModelConfigMutation.isPending ? 0.6 : 1,
                }}
              >
                {cloneModelConfigMutation.isPending
                  ? 'Duplicating…'
                  : 'Duplicate active'}
              </button>
            </div>
          </div>

          {noConfigs ? (
            <div
              style={{
                border: `1px dashed ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: t.space.lg,
                background: t.color.bgSubtle,
              }}
            >
              <h3
                style={{
                  margin: `0 0 ${t.space.sm}px`,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                }}
              >
                No measurement configs yet
              </h3>
              <p
                style={{
                  margin: 0,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                Start from the recommended template or create a fresh draft for your
                workspace.
              </p>
            </div>
          ) : (
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: '280px 1fr',
                gap: t.space.lg,
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  borderRight: `1px solid ${t.color.borderLight}`,
                  paddingRight: t.space.lg,
                  display: 'grid',
                  gap: t.space.sm,
                  maxHeight: 420,
                  overflowY: 'auto',
                }}
              >
                {noMatches ? (
                  <p
                    style={{
                      fontSize: t.font.sizeXs,
                      color: t.color.textSecondary,
                      margin: 0,
                    }}
                  >
                    No configs match the current filters.
                  </p>
                ) : (
                  filteredModelConfigs.map(renderConfigCard)
                )}
              </div>

              <div style={{ display: 'grid', gap: t.space.lg }}>
                {!selectedConfigSummary ? (
                  <p
                    style={{
                      margin: 0,
                      fontSize: t.font.sizeSm,
                      color: t.color.textSecondary,
                    }}
                  >
                    Select a config to view details, run validation, and adjust fields.
                  </p>
                ) : (
                  <>
                    <div
                      style={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        gap: t.space.sm,
                        alignItems: 'center',
                        color: t.color.textSecondary,
                        fontSize: t.font.sizeXs,
                      }}
                    >
                      {renderStatusBadge(selectedConfigSummary.status)}
                      <span>
                        Version v{selectedConfigSummary.version} • Created{' '}
                        {formatDateTime(selectedConfigSummary.created_at)}
                      </span>
                      {selectedConfigSummary.activated_at && (
                        <span>
                          Last activated {formatDateTime(selectedConfigSummary.activated_at)}
                        </span>
                      )}
                    </div>

                    <label
                      style={{
                        display: 'flex',
                        flexDirection: 'column',
                        gap: t.space.xs,
                      }}
                    >
                      <span
                        style={{
                          fontSize: t.font.sizeSm,
                          fontWeight: t.font.weightMedium,
                          color: t.color.text,
                        }}
                      >
                        Description
                      </span>
                      <textarea
                        value={modelConfigChangeNote}
                        onChange={(e) => setModelConfigChangeNote(e.target.value)}
                        rows={3}
                        placeholder="Summarise what this draft adjusts…"
                        style={{
                          width: '100%',
                          padding: `${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.borderLight}`,
                          fontSize: t.font.sizeSm,
                        }}
                      />
                    </label>

                    {modelConfigParseError && (
                      <div
                        style={{
                          border: `1px solid ${t.color.danger}`,
                          background: t.color.dangerSubtle,
                          color: t.color.danger,
                          borderRadius: t.radius.md,
                          padding: t.space.sm,
                          fontSize: t.font.sizeXs,
                        }}
                      >
                        JSON parse error: {modelConfigParseError}
                      </div>
                    )}
                    {modelConfigError && !modelConfigParseError && (
                      <div
                        style={{
                          border: `1px solid ${t.color.danger}`,
                          background: t.color.dangerSubtle,
                          color: t.color.danger,
                          borderRadius: t.radius.md,
                          padding: t.space.sm,
                          fontSize: t.font.sizeXs,
                        }}
                      >
                        {modelConfigError}
                      </div>
                    )}

                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        flexWrap: 'wrap',
                        gap: t.space.sm,
                      }}
                    >
                      <div
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: t.space.sm,
                          flexWrap: 'wrap',
                        }}
                      >
                        <button
                          type="button"
                          onClick={() => handleValidateDraft()}
                          disabled={
                            !selectedModelConfigId || isValidatingConfig || !currentConfigObject
                          }
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: 'transparent',
                            color: t.color.text,
                            fontSize: t.font.sizeXs,
                            cursor: isValidatingConfig ? 'wait' : 'pointer',
                          }}
                        >
                          {isValidatingConfig ? 'Validating…' : 'Validate draft'}
                        </button>
                        <button
                          type="button"
                          onClick={() => handlePreviewDraft()}
                          disabled={
                            !selectedModelConfigId || isPreviewingConfig || !currentConfigObject
                          }
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.borderLight}`,
                            background: 'transparent',
                            color: t.color.text,
                            fontSize: t.font.sizeXs,
                            cursor: isPreviewingConfig ? 'wait' : 'pointer',
                          }}
                        >
                          {isPreviewingConfig ? 'Calculating…' : 'Impact preview'}
                        </button>
                      </div>

                      <div
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: t.space.xs,
                          flexWrap: 'wrap',
                        }}
                      >
                        <button
                          type="button"
                          onClick={() => handleEditorModeChange('basic')}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${
                              measurementEditorMode === 'basic'
                                ? t.color.accent
                                : t.color.borderLight
                            }`,
                            background:
                              measurementEditorMode === 'basic'
                                ? t.color.accentMuted
                                : 'transparent',
                            color:
                              measurementEditorMode === 'basic'
                                ? t.color.accent
                                : t.color.text,
                            fontSize: t.font.sizeXs,
                            cursor: 'pointer',
                          }}
                        >
                          Basic
                        </button>
                        <button
                          type="button"
                          onClick={() => handleEditorModeChange('advanced')}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${
                              measurementEditorMode === 'advanced'
                                ? t.color.accent
                                : t.color.borderLight
                            }`,
                            background:
                              measurementEditorMode === 'advanced'
                                ? t.color.accentMuted
                                : 'transparent',
                            color:
                              measurementEditorMode === 'advanced'
                                ? t.color.accent
                                : t.color.text,
                            fontSize: t.font.sizeXs,
                            cursor: 'pointer',
                          }}
                        >
                          Advanced
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleSave('measurement-models')}
                          disabled={
                            !measurementDirty ||
                            !!modelConfigParseError ||
                            saveModelConfigMutation.isPending
                          }
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: 'none',
                            background: measurementDirty
                              ? t.color.accent
                              : t.color.borderLight,
                            color: t.color.surface,
                            fontSize: t.font.sizeXs,
                            fontWeight: t.font.weightSemibold,
                            cursor:
                              measurementDirty && !modelConfigParseError
                                ? 'pointer'
                                : 'not-allowed',
                          }}
                        >
                          {saveModelConfigMutation.isPending ? 'Saving…' : 'Save draft'}
                        </button>
                        <button
                          type="button"
                          onClick={() => setShowActivationModal(true)}
                          disabled={
                            !selectedModelConfigId ||
                            activateModelConfigMutation.isPending ||
                            !!modelConfigParseError
                          }
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: 'none',
                            background: t.color.success,
                            color: t.color.surface,
                            fontSize: t.font.sizeXs,
                            fontWeight: t.font.weightSemibold,
                            cursor: activateModelConfigMutation.isPending
                              ? 'wait'
                              : 'pointer',
                            opacity: activateModelConfigMutation.isPending ? 0.85 : 1,
                          }}
                        >
                          Activate…
                        </button>
                      </div>
                    </div>

                    {modelConfigValidation && (
                      <div
                        style={{
                          border: `1px solid ${
                            modelConfigValidation.valid
                              ? t.color.success
                              : t.color.warning
                          }`,
                          background: modelConfigValidation.valid
                            ? 'rgba(34,197,94,0.12)'
                            : t.color.warningSubtle,
                          borderRadius: t.radius.md,
                          padding: t.space.md,
                          display: 'grid',
                          gap: 6,
                        }}
                      >
                        <div
                          style={{
                            fontSize: t.font.sizeSm,
                            fontWeight: t.font.weightSemibold,
                            color: modelConfigValidation.valid
                              ? t.color.success
                              : t.color.warning,
                          }}
                        >
                          {modelConfigValidation.valid
                            ? 'Validation passed'
                            : 'Validation issues found'}
                        </div>
                        {modelConfigValidation.errors.length > 0 && (
                          <div style={{ fontSize: t.font.sizeXs }}>
                            <strong>Errors:</strong>
                            <ul style={{ margin: '4px 0 0 16px' }}>
                              {modelConfigValidation.errors.map((err) => (
                                <li key={err}>{err}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                        {modelConfigValidation.warnings.length > 0 && (
                          <div style={{ fontSize: t.font.sizeXs }}>
                            <strong>Warnings:</strong>
                            <ul style={{ margin: '4px 0 0 16px' }}>
                              {modelConfigValidation.warnings.map((warn) => (
                                <li key={warn}>{warn}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}

                    {previewAvailable && (
                      <div
                        style={{
                          border: `1px solid ${t.color.borderLight}`,
                          borderRadius: t.radius.md,
                          padding: t.space.md,
                          display: 'grid',
                          gap: t.space.sm,
                          background: t.color.surface,
                        }}
                      >
                        <div
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                          }}
                        >
                          <span
                            style={{
                              fontSize: t.font.sizeSm,
                              fontWeight: t.font.weightSemibold,
                              color: t.color.text,
                            }}
                          >
                            Impact preview vs active config
                          </span>
                          {modelConfigPreview?.coverage_warning && (
                            <span
                              style={{
                                fontSize: t.font.sizeXs,
                                color: t.color.warning,
                                fontWeight: t.font.weightSemibold,
                              }}
                            >
                              Coverage drop warning
                            </span>
                          )}
                        </div>
                        <table
                          style={{
                            width: '100%',
                            borderCollapse: 'collapse',
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          <thead>
                            <tr style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                              <th style={{ textAlign: 'left', padding: '6px 8px' }}>Metric</th>
                              <th style={{ textAlign: 'right', padding: '6px 8px' }}>Active</th>
                              <th style={{ textAlign: 'right', padding: '6px 8px' }}>Draft</th>
                              <th style={{ textAlign: 'right', padding: '6px 8px' }}>Δ</th>
                              <th style={{ textAlign: 'right', padding: '6px 8px' }}>Δ%</th>
                            </tr>
                          </thead>
                          <tbody>
                            {previewRows.map((row) => (
                              <tr key={row.metric}>
                                <td style={{ padding: '6px 8px' }}>{row.metric}</td>
                                <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                  {row.baselineValue}
                                </td>
                                <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                  {row.draftValue}
                                </td>
                                <td
                                  style={{
                                    padding: '6px 8px',
                                    textAlign: 'right',
                                    color:
                                      row.delta > 0
                                        ? t.color.success
                                        : row.delta < 0
                                        ? t.color.danger
                                        : t.color.text,
                                  }}
                                >
                                  {row.delta > 0 ? '+' : ''}
                                  {row.delta}
                                </td>
                                <td
                                  style={{
                                    padding: '6px 8px',
                                    textAlign: 'right',
                                    color:
                                      (row.deltaPct ?? 0) > 0
                                        ? t.color.success
                                        : (row.deltaPct ?? 0) < 0
                                        ? t.color.danger
                                        : t.color.text,
                                  }}
                                >
                                  {row.deltaPct == null
                                    ? '—'
                                    : `${row.deltaPct > 0 ? '+' : ''}${row.deltaPct.toFixed(
                                        2,
                                      )}%`}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {modelConfigPreview?.warnings?.length ? (
                          <ul
                            style={{
                              margin: 0,
                              paddingLeft: 16,
                              color: t.color.warning,
                              fontSize: t.font.sizeXs,
                            }}
                          >
                            {modelConfigPreview.warnings.map((warn) => (
                              <li key={warn}>{warn}</li>
                            ))}
                          </ul>
                        ) : null}
                      </div>
                    )}

                    {!previewAvailable && impactUnavailableReason && (
                      <div
                        style={{
                          border: `1px dashed ${t.color.borderLight}`,
                          borderRadius: t.radius.md,
                          padding: t.space.md,
                          fontSize: t.font.sizeXs,
                          color: t.color.textSecondary,
                        }}
                      >
                        Impact preview unavailable: {impactUnavailableReason}
                      </div>
                    )}

                    {activationWarning && (
                      <div
                        style={{
                          border: `1px solid ${t.color.warning}`,
                          background: t.color.warningSubtle,
                          color: t.color.warning,
                          borderRadius: t.radius.md,
                          padding: t.space.sm,
                          fontSize: t.font.sizeXs,
                        }}
                      >
                        {activationWarning}
                      </div>
                    )}

                    {measurementEditorMode === 'basic' ? (
                      currentConfigObject ? (
                        <div style={{ display: 'grid', gap: t.space.lg }}>
                          <div
                            style={{
                              display: 'grid',
                              gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
                              gap: t.space.sm,
                            }}
                          >
                            {renderTokenField(
                              'Channels to include',
                              'Only journeys touching these channels are eligible.',
                              'include_channels',
                              includeChannelInput,
                              setIncludeChannelInput,
                              touchpointConfig.include_channels,
                              SUGGESTED_CHANNELS,
                            )}
                            {renderTokenField(
                              'Channels to exclude',
                              'Journeys hitting these channels are excluded.',
                              'exclude_channels',
                              excludeChannelInput,
                              setExcludeChannelInput,
                              touchpointConfig.exclude_channels,
                              SUGGESTED_CHANNELS,
                            )}
                            {renderTokenField(
                              'Event types to include',
                              'Journeys must contain at least one of these event types.',
                              'include_event_types',
                              includeEventInput,
                              setIncludeEventInput,
                              touchpointConfig.include_event_types,
                              SUGGESTED_EVENT_TYPES,
                            )}
                            {renderTokenField(
                              'Event types to exclude',
                              'Journeys are dropped when these event types occur.',
                              'exclude_event_types',
                              excludeEventInput,
                              setExcludeEventInput,
                              touchpointConfig.exclude_event_types,
                              SUGGESTED_EVENT_TYPES,
                            )}
                          </div>

                          <div
                            style={{
                              border: `1px solid ${t.color.borderLight}`,
                              borderRadius: t.radius.md,
                              padding: t.space.md,
                              display: 'grid',
                              gap: t.space.sm,
                              background: t.color.surface,
                            }}
                          >
                            <span
                              style={{
                                fontSize: t.font.sizeSm,
                                fontWeight: t.font.weightSemibold,
                              }}
                            >
                              Time windows
                            </span>
                            <div
                              style={{
                                display: 'grid',
                                gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
                                gap: t.space.sm,
                              }}
                            >
                              <label style={{ display: 'grid', gap: 4 }}>
                                <span style={{ fontSize: t.font.sizeXs }}>
                                  Click lookback (days)
                                </span>
                                <input
                                  type="number"
                                  min={0}
                                  max={120}
                                  value={windowsConfig.click_lookback_days}
                                  onChange={(e) =>
                                    handleWindowFieldChange(
                                      'click_lookback_days',
                                      Number(e.target.value),
                                    )
                                  }
                                  style={{
                                    padding: `${t.space.xs}px ${t.space.sm}px`,
                                    borderRadius: t.radius.sm,
                                    border: `1px solid ${t.color.borderLight}`,
                                    fontSize: t.font.sizeSm,
                                  }}
                                />
                              </label>
                              <label style={{ display: 'grid', gap: 4 }}>
                                <span style={{ fontSize: t.font.sizeXs }}>
                                  Impression lookback (days)
                                </span>
                                <input
                                  type="number"
                                  min={0}
                                  max={120}
                                  value={windowsConfig.impression_lookback_days}
                                  onChange={(e) =>
                                    handleWindowFieldChange(
                                      'impression_lookback_days',
                                      Number(e.target.value),
                                    )
                                  }
                                  style={{
                                    padding: `${t.space.xs}px ${t.space.sm}px`,
                                    borderRadius: t.radius.sm,
                                    border: `1px solid ${t.color.borderLight}`,
                                    fontSize: t.font.sizeSm,
                                  }}
                                />
                              </label>
                              <label style={{ display: 'grid', gap: 4 }}>
                                <span style={{ fontSize: t.font.sizeXs }}>
                                  Session timeout (minutes)
                                </span>
                                <input
                                  type="number"
                                  min={1}
                                  max={1440}
                                  value={windowsConfig.session_timeout_minutes}
                                  onChange={(e) =>
                                    handleWindowFieldChange(
                                      'session_timeout_minutes',
                                      Number(e.target.value),
                                    )
                                  }
                                  style={{
                                    padding: `${t.space.xs}px ${t.space.sm}px`,
                                    borderRadius: t.radius.sm,
                                    border: `1px solid ${t.color.borderLight}`,
                                    fontSize: t.font.sizeSm,
                                  }}
                                />
                              </label>
                              <label style={{ display: 'grid', gap: 4 }}>
                                <span style={{ fontSize: t.font.sizeXs }}>
                                  Conversion latency (days)
                                </span>
                                <input
                                  type="number"
                                  min={0}
                                  max={120}
                                  value={windowsConfig.conversion_latency_days}
                                  onChange={(e) =>
                                    handleWindowFieldChange(
                                      'conversion_latency_days',
                                      Number(e.target.value),
                                    )
                                  }
                                  style={{
                                    padding: `${t.space.xs}px ${t.space.sm}px`,
                                    borderRadius: t.radius.sm,
                                    border: `1px solid ${t.color.borderLight}`,
                                    fontSize: t.font.sizeSm,
                                  }}
                                />
                              </label>
                            </div>
                          </div>

                          <div
                            style={{
                              border: `1px solid ${t.color.borderLight}`,
                              borderRadius: t.radius.md,
                              padding: t.space.md,
                              background: t.color.surface,
                              display: 'grid',
                              gap: t.space.xs,
                            }}
                          >
                            <div
                              style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center',
                              }}
                            >
                              <span
                                style={{
                                  fontSize: t.font.sizeSm,
                                  fontWeight: t.font.weightSemibold,
                                }}
                              >
                                Conversion definitions
                              </span>
                              <span
                                style={{
                                  fontSize: t.font.sizeXs,
                                  color: t.color.textMuted,
                                }}
                              >
                                {conversionsConfig.selectedKeys.length} selected
                              </span>
                            </div>
                            <div
                              style={{
                                display: 'grid',
                                gap: t.space.xs,
                                maxHeight: 220,
                                overflowY: 'auto',
                                paddingRight: 4,
                              }}
                            >
                              {availableKpis.map((kpi) => {
                                const isSelected =
                                  conversionsConfig.selectedKeys.includes(kpi.id)
                                const isPrimary =
                                  conversionsConfig.primaryKey === kpi.id
                                return (
                                  <label
                                    key={kpi.id}
                                    style={{
                                      display: 'grid',
                                      gap: 2,
                                      padding: `${t.space.xs}px`,
                                      borderRadius: t.radius.sm,
                                      border: `1px solid ${
                                        isSelected ? t.color.accent : t.color.borderLight
                                      }`,
                                      background: isSelected
                                        ? t.color.accentMuted
                                        : 'transparent',
                                    }}
                                  >
                                    <div
                                      style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: t.space.sm,
                                      }}
                                    >
                                      <input
                                        type="checkbox"
                                        checked={isSelected}
                                        onChange={(e) =>
                                          handleToggleConversion(kpi.id, e.target.checked)
                                        }
                                      />
                                      <div>
                                        <div
                                          style={{
                                            fontSize: t.font.sizeSm,
                                            fontWeight: t.font.weightMedium,
                                            color: t.color.text,
                                          }}
                                        >
                                          {kpi.label}
                                        </div>
                                        <div
                                          style={{
                                            fontSize: t.font.sizeXs,
                                            color: t.color.textSecondary,
                                          }}
                                        >
                                          Event: {kpi.event_name}
                                        </div>
                                      </div>
                                      <div style={{ marginLeft: 'auto' }}>
                                        <input
                                          type="radio"
                                          name="primary-conversion"
                                          checked={isPrimary}
                                          disabled={!isSelected}
                                          onChange={() => handleSetPrimaryConversion(kpi.id)}
                                        />{' '}
                                        <span style={{ fontSize: t.font.sizeXs }}>
                                          Primary
                                        </span>
                                      </div>
                                    </div>
                                  </label>
                                )
                              })}
                            </div>
                          </div>
                        </div>
                      ) : (
                        <div
                          style={{
                            border: `1px solid ${t.color.borderLight}`,
                            borderRadius: t.radius.md,
                            padding: t.space.md,
                            color: t.color.textSecondary,
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          Invalid JSON – switch to Advanced to resolve errors before using
                          the basic editor.
                        </div>
                      )
                    ) : (
                      <div style={{ display: 'grid', gap: t.space.sm }}>
                        <textarea
                          value={modelConfigJson}
                          onChange={(e) => handleAdvancedJsonChange(e.target.value)}
                          rows={24}
                          style={{
                            width: '100%',
                            minHeight: 360,
                            fontFamily: 'monospace',
                            fontSize: t.font.sizeXs,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${
                              modelConfigParseError ? t.color.danger : t.color.border
                            }`,
                            padding: t.space.sm,
                            background: t.color.bgSubtle,
                            whiteSpace: 'pre',
                          }}
                        />
                        <div
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                          }}
                        >
                          <button
                            type="button"
                            onClick={() => handleFormatAdvancedJson()}
                            style={{
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              borderRadius: t.radius.sm,
                              border: `1px solid ${t.color.borderLight}`,
                              background: 'transparent',
                              color: t.color.text,
                              fontSize: t.font.sizeXs,
                              cursor: 'pointer',
                            }}
                          >
                            Format JSON
                          </button>
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                            Advanced edits update the basic form automatically.
                          </span>
                        </div>
                        <details>
                          <summary
                            style={{
                              fontSize: t.font.sizeXs,
                              color: t.color.textSecondary,
                              cursor: 'pointer',
                            }}
                          >
                            Reference template
                          </summary>
                          <pre
                            style={{
                              marginTop: t.space.xs,
                              background: t.color.bgSubtle,
                              border: `1px solid ${t.color.borderLight}`,
                              borderRadius: t.radius.sm,
                              padding: t.space.sm,
                              fontSize: t.font.sizeXs,
                              whiteSpace: 'pre-wrap',
                              maxHeight: 240,
                              overflow: 'auto',
                            }}
                          >
                            {TEMPLATE_REFERENCE_SNIPPET}
                          </pre>
                        </details>
                      </div>
                    )}
                  </>
                )}
              </div>
            </div>
          )}
        </div>
      )
    }

    const SEVERITY_OPTIONS = [
      { value: 'info', label: 'Info' },
      { value: 'warn', label: 'Warning' },
      { value: 'critical', label: 'Critical' },
    ]
    const TIMEZONES = [
      'UTC',
      'Europe/London',
      'Europe/Prague',
      'Europe/Paris',
      'America/New_York',
      'America/Los_Angeles',
      'Asia/Tokyo',
    ]

    const renderNotifications = () => {
      const inputStyle: CSSProperties = {
        padding: `${t.space.sm}px ${t.space.md}px`,
        borderRadius: t.radius.sm,
        border: `1px solid ${t.color.border}`,
        fontSize: t.font.sizeSm,
        background: t.color.surface,
        color: t.color.text,
      }
      const emailCh = notificationsChannelsDraft.find((c) => c.type === 'email')
      const slackCh = notificationsChannelsDraft.find(
        (c) => c.type === 'slack_webhook',
      )
      const getPrefForChannel = (channelId: number) =>
        notificationsPrefsDraft.find((p) => p.channel_id === channelId)

      const addEmailChannel = () => {
        if (emailCh) return
        setNotificationsChannelsDraft((prev) => [
          ...prev,
          {
            id: -1,
            type: 'email' as const,
            config: { emails: [] },
            is_verified: false,
            created_at: null,
          },
        ])
        setNotificationsPrefsDraft((prev) => [
          ...prev,
          {
            id: -1,
            user_id: 'default',
            channel_id: -1,
            severities: [],
            digest_mode: 'realtime',
            quiet_hours: null,
            is_enabled: false,
            created_at: null,
            updated_at: null,
          },
        ])
      }
      const addSlackChannel = () => {
        if (slackCh) return
        setNotificationsChannelsDraft((prev) => [
          ...prev,
          {
            id: -2,
            type: 'slack_webhook' as const,
            config: { configured: false },
            is_verified: false,
            created_at: null,
          },
        ])
        setNotificationsPrefsDraft((prev) => [
          ...prev,
          {
            id: -1,
            user_id: 'default',
            channel_id: -2,
            severities: [],
            digest_mode: 'realtime',
            quiet_hours: null,
            is_enabled: false,
            created_at: null,
            updated_at: null,
          },
        ])
      }
      const removeChannel = (id: number) => {
        setNotificationsChannelsDraft((prev) => prev.filter((c) => c.id !== id))
        setNotificationsPrefsDraft((prev) => prev.filter((p) => p.channel_id !== id))
        if (id === slackCh?.id) setNotificationsSlackWebhookInput('')
      }
      const setChannelEmails = (emails: string[]) => {
        setNotificationsChannelsDraft((prev) =>
          prev.map((c) =>
            c.type === 'email'
              ? { ...c, config: { ...c.config, emails } }
              : c,
          ),
        )
      }
      const setPref = (
        channelId: number,
        patch: Partial<NotificationPrefRow>,
      ) => {
        setNotificationsPrefsDraft((prev) =>
          prev.map((p) =>
            p.channel_id === channelId ? { ...p, ...patch } : p,
          ),
        )
      }

      return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.xl }}>
          <div>
            <h3
              style={{
                margin: `0 0 ${t.space.sm}px`,
                fontSize: t.font.sizeMd,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Notification channels
            </h3>
            <p
              style={{
                margin: `0 0 ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              Email (workspace) or user-specific; Slack webhook optional. Notifications are off by default until you enable them.
            </p>
            <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.md }}>
              {!emailCh && (
                <button
                  type="button"
                  onClick={addEmailChannel}
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
                  + Add email channel (workspace)
                </button>
              )}
              {emailCh && (
                <div
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.md,
                    display: 'flex',
                    flexDirection: 'column',
                    gap: t.space.sm,
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                    }}
                  >
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Email (workspace)
                    </span>
                    <button
                      type="button"
                      onClick={() => removeChannel(emailCh.id)}
                      style={{
                        padding: '2px 8px',
                        fontSize: t.font.sizeXs,
                        border: `1px solid ${t.color.border}`,
                        background: 'transparent',
                        color: t.color.textMuted,
                        cursor: 'pointer',
                        borderRadius: t.radius.sm,
                      }}
                    >
                      Remove
                    </button>
                  </div>
                  <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Recipients (one per line or comma-separated)
                    </span>
                    <textarea
                      value={(emailCh.config?.emails ?? []).join('\n')}
                      onChange={(e) => {
                        const raw = e.target.value
                        const emails = raw
                          .split(/[\n,]+/)
                          .map((s) => s.trim())
                          .filter(Boolean)
                        setChannelEmails(emails)
                      }}
                      placeholder="user@example.com"
                      rows={3}
                      style={{ ...inputStyle, resize: 'vertical' }}
                    />
                  </label>
                </div>
              )}
              {!slackCh && (
                <button
                  type="button"
                  onClick={addSlackChannel}
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
                  + Add Slack webhook
                </button>
              )}
              {slackCh && (
                <div
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.md,
                    display: 'flex',
                    flexDirection: 'column',
                    gap: t.space.sm,
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                    }}
                  >
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Slack webhook
                    </span>
                    <button
                      type="button"
                      onClick={() => removeChannel(slackCh.id)}
                      style={{
                        padding: '2px 8px',
                        fontSize: t.font.sizeXs,
                        border: `1px solid ${t.color.border}`,
                        background: 'transparent',
                        color: t.color.textMuted,
                        cursor: 'pointer',
                        borderRadius: t.radius.sm,
                      }}
                    >
                      Remove
                    </button>
                  </div>
                  {slackCh.config?.configured ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Webhook configured (URL is stored securely and not shown).
                      </span>
                      <input
                        type="password"
                        placeholder="Enter new URL to replace"
                        value={notificationsSlackWebhookInput}
                        onChange={(e) => setNotificationsSlackWebhookInput(e.target.value)}
                        style={{ ...inputStyle, maxWidth: 400 }}
                      />
                    </div>
                  ) : (
                    <input
                      type="password"
                      placeholder="https://hooks.slack.com/services/..."
                      value={notificationsSlackWebhookInput}
                      onChange={(e) => setNotificationsSlackWebhookInput(e.target.value)}
                      style={{ ...inputStyle, maxWidth: 400 }}
                    />
                  )}
                </div>
              )}
            </div>
          </div>

          <div>
            <h3
              style={{
                margin: `0 0 ${t.space.sm}px`,
                fontSize: t.font.sizeMd,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Preferences
            </h3>
            <p
              style={{
                margin: `0 0 ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              Severities to notify, delivery mode, and quiet hours (timezone-aware). Default: notifications off.
            </p>
            <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.lg }}>
              {notificationsChannelsDraft.map((ch) => {
                const pref = getPrefForChannel(ch.id)
                return (
                  <div
                    key={ch.id}
                    style={{
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      padding: t.space.md,
                      display: 'flex',
                      flexDirection: 'column',
                      gap: t.space.md,
                    }}
                  >
                    <div
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      {ch.type === 'email' ? 'Email' : 'Slack'} – preferences
                    </div>
                    <label
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: t.space.sm,
                        cursor: 'pointer',
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={pref?.is_enabled ?? false}
                        onChange={(e) =>
                          setPref(ch.id, { is_enabled: e.target.checked })
                        }
                      />
                      <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                        Enable notifications for this channel
                      </span>
                    </label>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Severities to notify
                      </span>
                      <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap' }}>
                        {SEVERITY_OPTIONS.map((opt) => (
                          <label
                            key={opt.value}
                            style={{
                              display: 'flex',
                              alignItems: 'center',
                              gap: 6,
                              cursor: 'pointer',
                            }}
                          >
                            <input
                              type="checkbox"
                              checked={(pref?.severities ?? []).includes(opt.value)}
                              onChange={(e) => {
                                const next = e.target.checked
                                  ? [...(pref?.severities ?? []), opt.value]
                                  : (pref?.severities ?? []).filter((s) => s !== opt.value)
                                setPref(ch.id, { severities: next })
                              }}
                            />
                            <span style={{ fontSize: t.font.sizeSm }}>{opt.label}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                    <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Delivery
                      </span>
                      <select
                        value={pref?.digest_mode ?? 'realtime'}
                        onChange={(e) =>
                          setPref(ch.id, {
                            digest_mode: e.target.value as 'realtime' | 'daily',
                          })
                        }
                        style={{ ...inputStyle, maxWidth: 200 }}
                      >
                        <option value="realtime">Realtime</option>
                        <option value="daily">Daily digest</option>
                      </select>
                    </label>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Quiet hours (optional, timezone-aware)
                      </span>
                      <div
                        style={{
                          display: 'grid',
                          gridTemplateColumns: '1fr 1fr auto',
                          gap: t.space.sm,
                          alignItems: 'end',
                        }}
                      >
                        <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Start</span>
                          <input
                            type="time"
                            value={pref?.quiet_hours?.start ?? ''}
                            onChange={(e) =>
                              setPref(ch.id, {
                                quiet_hours: {
                                  ...(pref?.quiet_hours ?? {}),
                                  start: e.target.value || undefined,
                                  timezone:
                                    pref?.quiet_hours?.timezone ?? TIMEZONES[0],
                                },
                              })
                            }
                            style={inputStyle}
                          />
                        </label>
                        <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>End</span>
                          <input
                            type="time"
                            value={pref?.quiet_hours?.end ?? ''}
                            onChange={(e) =>
                              setPref(ch.id, {
                                quiet_hours: {
                                  ...(pref?.quiet_hours ?? {}),
                                  end: e.target.value || undefined,
                                  timezone:
                                    pref?.quiet_hours?.timezone ?? TIMEZONES[0],
                                },
                              })
                            }
                            style={inputStyle}
                          />
                        </label>
                        <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Timezone</span>
                          <select
                            value={pref?.quiet_hours?.timezone ?? TIMEZONES[0]}
                            onChange={(e) =>
                              setPref(ch.id, {
                                quiet_hours: {
                                  ...(pref?.quiet_hours ?? {}),
                                  timezone: e.target.value,
                                },
                              })
                            }
                            style={{ ...inputStyle, minWidth: 140 }}
                          >
                            {TIMEZONES.map((tz) => (
                              <option key={tz} value={tz}>
                                {tz}
                              </option>
                            ))}
                          </select>
                        </label>
                      </div>
                    </div>
                  </div>
                )
              })}
              {notificationsChannelsDraft.length === 0 && (
                <p
                  style={{
                    fontSize: t.font.sizeSm,
                    color: t.color.textMuted,
                  }}
                >
                  Add a channel above to configure preferences.
                </p>
              )}
            </div>
          </div>
        </div>
      )
    }

    const renderSectionBody = () => {
      switch (activeSection) {
        case 'attribution':
          return renderAttribution()
        case 'nba':
          return renderNba()
        case 'mmm':
          return renderMmm()
        case 'kpi':
          return renderKpi()
        case 'taxonomy':
          return renderTaxonomy()
        case 'measurement-models':
          return renderMeasurementConfigs()
        case 'journeys':
          return (
            <JourneysSettingsSection
              featureFlags={settingsBaseline?.feature_flags ?? DEFAULT_SETTINGS.feature_flags}
            />
          )
        case 'access-control-users':
          return (
            <AccessControlUsersSection
              featureFlags={settingsBaseline?.feature_flags ?? DEFAULT_SETTINGS.feature_flags}
            />
          )
        case 'access-control-roles':
          return (
            <AccessControlRolesSection
              featureFlags={settingsBaseline?.feature_flags ?? DEFAULT_SETTINGS.feature_flags}
            />
          )
        case 'access-control-audit-log':
          return (
            <AccessControlAuditLogSection
              featureFlags={settingsBaseline?.feature_flags ?? DEFAULT_SETTINGS.feature_flags}
            />
          )
        case 'notifications':
          return renderNotifications()
        default:
          return null
      }
    }

    return (
      <>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '260px 1fr',
            gap: t.space.xl,
            alignItems: 'start',
          }}
        >
          <nav
            style={{
              position: 'sticky',
              top: 20,
              alignSelf: 'start',
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.borderLight}`,
              boxShadow: t.shadowXs,
              padding: t.space.md,
            }}
          >
            <h2
              style={{
                margin: `0 0 ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                color: t.color.textSecondary,
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
              }}
            >
              Settings sections
            </h2>
            <ul
              style={{
                listStyle: 'none',
                margin: 0,
                padding: 0,
                display: 'grid',
                gap: 6,
              }}
            >
              {visibleSectionOrder.map((sectionKey) => {
                const meta = SECTION_META[sectionKey]
                const isActive = sectionKey === activeSection
                const isDirty = dirtySections.includes(sectionKey)
                return (
                  <li key={sectionKey}>
                    <button
                      type="button"
                      onClick={() => handleSectionNav(sectionKey)}
                      style={{
                        width: '100%',
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.md,
                        border: `1px solid ${
                          isActive ? t.color.accent : t.color.borderLight
                        }`,
                        background: isActive ? t.color.accentMuted : 'transparent',
                        color: isActive ? t.color.accent : t.color.text,
                        cursor: 'pointer',
                        fontSize: t.font.sizeSm,
                        fontWeight: isActive
                          ? t.font.weightSemibold
                          : t.font.weightMedium,
                        transition: 'background-color 120ms ease',
                      }}
                    >
                      <span style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
                        <span
                          style={{
                            width: 24,
                            height: 24,
                            borderRadius: 999,
                            background: isActive
                              ? t.color.accent
                              : t.color.borderLight,
                            color: isActive ? t.color.surface : t.color.textSecondary,
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            fontSize: t.font.sizeXs,
                            fontWeight: t.font.weightSemibold,
                          }}
                        >
                          {meta.icon}
                        </span>
                        <span>{meta.title}</span>
                      </span>
                      {isDirty && <span style={badgeStyle}>Unsaved</span>}
                    </button>
                  </li>
                )
              })}
            </ul>
          </nav>

          <div style={{ display: 'flex', flexDirection: 'column', gap: t.space.xl }}>
            <header
              style={{
                background: t.color.surface,
                borderRadius: t.radius.lg,
                border: `1px solid ${t.color.borderLight}`,
                boxShadow: t.shadowXs,
                padding: t.space.lg,
                display: 'flex',
                flexDirection: 'column',
                gap: t.space.sm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                Admin / Settings
              </div>
              <div
                style={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  gap: t.space.md,
                }}
              >
                <div>
                  <h1
                    style={{
                      margin: 0,
                      fontSize: t.font.sizeXl,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.text,
                      letterSpacing: '-0.01em',
                    }}
                  >
                    {activeMeta.title}
                  </h1>
                  <p
                    style={{
                      margin: `${t.space.xs}px 0 0`,
                      fontSize: t.font.sizeSm,
                      color: t.color.textSecondary,
                      maxWidth: 620,
                    }}
                  >
                    {activeMeta.description}
                  </p>
                </div>
                <div
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: t.space.sm,
                    flexWrap: 'wrap',
                  }}
                >
                  {!['attribution', 'nba', 'journeys', 'access-control-users', 'access-control-roles', 'access-control-audit-log'].includes(activeSection) && (
                    <>
                      {lastSavedAt[activeSection] &&
                        !dirtySections.includes(activeSection) && (
                          <span
                            style={{
                              fontSize: t.font.sizeXs,
                              color: t.color.textMuted,
                            }}
                          >
                            Saved at {lastSavedAt[activeSection]}
                          </span>
                        )}
                      <button
                        type="button"
                        onClick={() => handleReset(activeSection)}
                        disabled={!dirtySections.includes(activeSection)}
                        style={{
                          padding: `${t.space.sm}px ${t.space.md}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.border}`,
                          background: 'transparent',
                          color: dirtySections.includes(activeSection)
                            ? t.color.text
                            : t.color.textMuted,
                          cursor: dirtySections.includes(activeSection)
                            ? 'pointer'
                            : 'not-allowed',
                          fontSize: t.font.sizeSm,
                        }}
                      >
                        Reset
                      </button>
                      <button
                        type="button"
                        onClick={() => void handleSave(activeSection)}
                        disabled={
                          !dirtySections.includes(activeSection) ||
                          sectionIsSaving(activeSection) ||
                          (activeSection === 'kpi' && kpiValidation.hasErrors) ||
                          (activeSection === 'measurement-models' &&
                            (!!modelConfigError || modelConfigJson.trim().length === 0))
                        }
                        style={{
                          padding: `${t.space.sm}px ${t.space.lg}px`,
                          borderRadius: t.radius.sm,
                          border: 'none',
                          background: dirtySections.includes(activeSection)
                            ? t.color.accent
                            : t.color.borderLight,
                          color: t.color.surface,
                          fontSize: t.font.sizeSm,
                          fontWeight: t.font.weightSemibold,
                          cursor: dirtySections.includes(activeSection)
                            ? 'pointer'
                            : 'not-allowed',
                          opacity: sectionIsSaving(activeSection) ? 0.7 : 1,
                        }}
                      >
                        {sectionIsSaving(activeSection) ? 'Saving…' : 'Save changes'}
                      </button>
                    </>
                  )}
                </div>
              </div>
            </header>

            <section
              style={{
                background: t.color.surface,
                borderRadius: t.radius.lg,
                border: `1px solid ${t.color.borderLight}`,
                boxShadow: t.shadowSm,
                padding: t.space.xl,
              }}
            >
              {renderSectionBody()}
            </section>
          </div>
      </div>

      {showRuleModal && ruleModalDraft && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(12, 14, 18, 0.6)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: t.space.lg,
            zIndex: 20,
          }}
        >
          <div
            style={{
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.borderLight}`,
              width: 'min(540px, 90vw)',
              maxHeight: '90vh',
              overflowY: 'auto',
              boxShadow: t.shadowLg,
              display: 'grid',
              gap: t.space.md,
              padding: t.space.lg,
            }}
          >
            <div style={{ display: 'grid', gap: t.space.xs }}>
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeLg,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                {ruleModalMode === 'create' ? 'Add channel rule' : 'Edit channel rule'}
              </h3>
              <p
                style={{
                  margin: 0,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                Define how source, medium, and campaign values map into a channel.
                Rules run in priority order; the first enabled match wins.
              </p>
            </div>

            <div
              style={{
                display: 'grid',
                gap: t.space.md,
              }}
            >
              <label style={{ display: 'grid', gap: 6 }}>
                <span
                  style={{
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    color: t.color.textSecondary,
                  }}
                >
                  Rule name
                </span>
                <input
                  type="text"
                  value={ruleModalDraft.name}
                  onChange={(e) => handleRuleModalFieldChange('name', e.target.value)}
                  placeholder="e.g. Paid Social - Meta"
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${
                      ruleModalErrors.name ? t.color.danger : t.color.border
                    }`,
                    fontSize: t.font.sizeSm,
                  }}
                />
                {ruleModalErrors.name && (
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                    {ruleModalErrors.name}
                  </span>
                )}
              </label>

              <label style={{ display: 'grid', gap: 6 }}>
                <span
                  style={{
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    color: t.color.textSecondary,
                  }}
                >
                  Output channel
                </span>
                <input
                  type="text"
                  list="channel-options"
                  value={ruleModalDraft.channel}
                  onChange={(e) => handleRuleModalFieldChange('channel', e.target.value)}
                  placeholder="Select or enter a channel"
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${
                      ruleModalErrors.channel ? t.color.danger : t.color.border
                    }`,
                    fontSize: t.font.sizeSm,
                  }}
                />
                <datalist id="channel-options">
                  {KNOWN_CHANNELS.map((channel) => (
                    <option key={channel} value={channel} />
                  ))}
                </datalist>
                {ruleModalErrors.channel && (
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                    {ruleModalErrors.channel}
                  </span>
                )}
              </label>

              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
                  gap: t.space.sm,
                }}
              >
                <label style={{ display: 'grid', gap: 6 }}>
                  <span
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                    }}
                  >
                    Priority
                  </span>
                  <input
                    type="number"
                    value={ruleModalDraft.priority}
                    onChange={(e) =>
                      handleRuleModalFieldChange('priority', Number(e.target.value))
                    }
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${
                        ruleModalErrors.priority ? t.color.danger : t.color.border
                      }`,
                      fontSize: t.font.sizeSm,
                    }}
                  />
                  {ruleModalErrors.priority && (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {ruleModalErrors.priority}
                    </span>
                  )}
                </label>

                <label
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: t.space.sm,
                    paddingTop: t.space.lg,
                  }}
                >
                  <input
                    type="checkbox"
                    checked={ruleModalDraft.enabled}
                    onChange={(e) =>
                      handleRuleModalFieldChange('enabled', e.target.checked)
                    }
                  />
                  <span
                    style={{
                      fontSize: t.font.sizeSm,
                      color: t.color.textSecondary,
                    }}
                  >
                    Enabled
                  </span>
                </label>
              </div>

              {(['source', 'medium', 'campaign'] as const).map((fieldKey) => {
                const label =
                  fieldKey === 'source'
                    ? 'Source condition'
                    : fieldKey === 'medium'
                    ? 'Medium condition'
                    : 'Campaign condition'
                const fieldValue = ensureMatchExpression(ruleModalDraft[fieldKey])
                const error = ruleModalErrors[fieldKey]
                return (
                  <div
                    key={fieldKey}
                    style={{
                      display: 'grid',
                      gap: t.space.xs,
                    }}
                  >
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.textSecondary,
                      }}
                    >
                      {label}
                    </span>
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: '160px 1fr',
                        gap: t.space.sm,
                      }}
                    >
                      <select
                        value={fieldValue.operator}
                        onChange={(e) =>
                          handleRuleExpressionChange(fieldKey, {
                            operator: e.target.value as MatchOperator,
                          })
                        }
                        style={{
                          padding: `${t.space.sm}px ${t.space.md}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.border}`,
                          fontSize: t.font.sizeSm,
                          background: t.color.surface,
                        }}
                      >
                        <option value="any">Any (no condition)</option>
                        <option value="contains">Contains</option>
                        <option value="equals">Equals</option>
                        <option value="regex">Regex (advanced)</option>
                      </select>
                      <input
                        type="text"
                        value={fieldValue.value}
                        onChange={(e) =>
                          handleRuleExpressionChange(fieldKey, {
                            value: e.target.value,
                          })
                        }
                        placeholder={
                          fieldValue.operator === 'regex'
                            ? 'Regular expression, e.g. facebook|instagram'
                            : 'Match value'
                        }
                        disabled={fieldValue.operator === 'any'}
                        style={{
                          padding: `${t.space.sm}px ${t.space.md}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${error ? t.color.danger : t.color.border}`,
                          fontSize: t.font.sizeSm,
                          background:
                            fieldValue.operator === 'any'
                              ? t.color.bgSubtle
                              : t.color.surface,
                        }}
                      />
                    </div>
                    <span
                      style={{
                        fontSize: t.font.sizeXs,
                        color: error ? t.color.danger : t.color.textSecondary,
                      }}
                    >
                      {error
                        ? error
                        : fieldValue.operator === 'regex'
                        ? 'Uses case-insensitive regular expressions.'
                        : 'Leave blank to match any value.'}
                    </span>
                  </div>
                )
              })}
            </div>

            <div
              style={{
                display: 'flex',
                justifyContent: 'flex-end',
                gap: t.space.sm,
                marginTop: t.space.sm,
              }}
            >
              <button
                type="button"
                onClick={handleCloseRuleModal}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                  background: 'transparent',
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeSm,
                  cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => handleSubmitRuleModal()}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: 'none',
                  background: t.color.accent,
                  color: t.color.surface,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor: 'pointer',
                }}
              >
                Save rule
              </button>
            </div>
          </div>
        </div>
      )}

      {showKpiModal && kpiModalDraft && (
          <div
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(15, 23, 42, 0.55)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: t.space.lg,
              zIndex: 30,
            }}
          >
            <div
              style={{
                width: 'min(560px, 100%)',
                background: t.color.surface,
                borderRadius: t.radius.lg,
                border: `1px solid ${t.color.border}`,
                boxShadow: t.shadowLg,
                padding: t.space.lg,
                display: 'grid',
                gap: t.space.md,
                maxHeight: '90vh',
                overflowY: 'auto',
              }}
            >
              <div>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeLg,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  {kpiModalMode === 'create' ? 'Add KPI' : 'Edit KPI'}
                </h3>
                <p
                  style={{
                    margin: `${t.space.xs}px 0 0`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  Define how this KPI is tracked. Save changes to update the settings
                  draft; nothing is published until you save the Settings section.
                </p>
              </div>

              <div
                style={{
                  display: 'grid',
                  gap: t.space.md,
                }}
              >
                <label style={{ display: 'grid', gap: t.space.xs }}>
                  <span
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.text,
                    }}
                  >
                    KPI ID
                  </span>
                  <input
                    type="text"
                    value={kpiModalDraft.id}
                    onChange={(e) => handleKpiModalFieldChange('id', e.target.value)}
                    disabled={kpiModalMode === 'edit'}
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${
                        kpiModalErrors.id ? t.color.danger : t.color.border
                      }`,
                      fontSize: t.font.sizeSm,
                      background:
                        kpiModalMode === 'edit' ? t.color.bgSubtle : t.color.surface,
                      color:
                        kpiModalMode === 'edit'
                          ? t.color.textSecondary
                          : t.color.text,
                    }}
                  />
                  {kpiModalErrors.id ? (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {kpiModalErrors.id}
                    </span>
                  ) : (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Unique identifier used in reporting and APIs.{' '}
                      {kpiModalMode === 'edit'
                        ? 'Existing KPIs cannot change IDs to preserve history.'
                        : 'Avoid spaces; use snake_case or kebab-case.'}
                    </span>
                  )}
                </label>

                <div
                  style={{
                    display: 'grid',
                    gap: t.space.md,
                    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                  }}
                >
                  <label style={{ display: 'grid', gap: t.space.xs }}>
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Label
                    </span>
                    <input
                      type="text"
                      value={kpiModalDraft.label}
                      onChange={(e) =>
                        handleKpiModalFieldChange('label', e.target.value)
                      }
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${
                          kpiModalErrors.label ? t.color.danger : t.color.border
                        }`,
                        fontSize: t.font.sizeSm,
                      }}
                    />
                    {kpiModalErrors.label && (
                      <span
                        style={{ fontSize: t.font.sizeXs, color: t.color.danger }}
                      >
                        {kpiModalErrors.label}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: t.space.xs }}>
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      KPI type
                    </span>
                    <select
                      value={kpiModalDraft.type}
                      onChange={(e) =>
                        handleKpiModalFieldChange(
                          'type',
                          e.target.value as 'primary' | 'micro',
                        )
                      }
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        fontSize: t.font.sizeSm,
                        background: t.color.surface,
                      }}
                    >
                      <option value="primary">Primary</option>
                      <option value="micro">Micro</option>
                    </select>
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Primary KPIs drive default attribution outputs. Micro conversions
                      provide supporting funnel signals.
                    </span>
                  </label>
                </div>

                <div
                  style={{
                    display: 'grid',
                    gap: t.space.md,
                    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                  }}
                >
                  <label style={{ display: 'grid', gap: t.space.xs }}>
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Event name
                    </span>
                    <input
                      type="text"
                      value={kpiModalDraft.event_name}
                      onChange={(e) =>
                        handleKpiModalFieldChange('event_name', e.target.value)
                      }
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${
                          kpiModalErrors.event_name
                            ? t.color.danger
                            : t.color.border
                        }`,
                        fontSize: t.font.sizeSm,
                      }}
                    />
                    {kpiModalErrors.event_name && (
                      <span
                        style={{ fontSize: t.font.sizeXs, color: t.color.danger }}
                      >
                        {kpiModalErrors.event_name}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: t.space.xs }}>
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Value field (optional)
                    </span>
                    <input
                      type="text"
                      value={kpiModalDraft.value_field ?? ''}
                      onChange={(e) =>
                        handleKpiModalFieldChange('value_field', e.target.value)
                      }
                      placeholder="e.g. revenue"
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        fontSize: t.font.sizeSm,
                      }}
                    />
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Provide when the KPI carries a monetary or custom value attribute.
                    </span>
                  </label>
                </div>

                <div
                  style={{
                    display: 'grid',
                    gap: t.space.md,
                    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                  }}
                >
                  <label style={{ display: 'grid', gap: t.space.xs }}>
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Weight
                    </span>
                    <input
                      type="number"
                      step={0.1}
                      min={0}
                      value={
                        Number.isFinite(kpiModalDraft.weight)
                          ? kpiModalDraft.weight
                          : ''
                      }
                      onChange={(e) =>
                        handleKpiModalFieldChange(
                          'weight',
                          e.target.value === ''
                            ? Number.NaN
                            : Number(e.target.value),
                        )
                      }
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${
                          kpiModalErrors.weight ? t.color.danger : t.color.border
                        }`,
                        fontSize: t.font.sizeSm,
                      }}
                    />
                    {kpiModalErrors.weight && (
                      <span
                        style={{ fontSize: t.font.sizeXs, color: t.color.danger }}
                      >
                        {kpiModalErrors.weight}
                      </span>
                    )}
                  </label>

                  <label style={{ display: 'grid', gap: t.space.xs }}>
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color: t.color.text,
                      }}
                    >
                      Lookback window (days)
                    </span>
                    <input
                      type="number"
                      min={1}
                      value={
                        kpiModalDraft.lookback_days != null
                          ? kpiModalDraft.lookback_days
                          : ''
                      }
                      onChange={(e) =>
                        handleKpiModalFieldChange(
                          'lookback_days',
                          e.target.value === ''
                            ? undefined
                            : Number(e.target.value),
                        )
                      }
                      placeholder="Optional"
                      style={{
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${
                          kpiModalErrors.lookback_days
                            ? t.color.danger
                            : t.color.border
                        }`,
                        fontSize: t.font.sizeSm,
                      }}
                    />
                    {kpiModalErrors.lookback_days && (
                      <span
                        style={{ fontSize: t.font.sizeXs, color: t.color.danger }}
                      >
                        {kpiModalErrors.lookback_days}
                      </span>
                    )}
                  </label>
                </div>

                <label
                  style={{
                    display: 'grid',
                    gap: t.space.xs,
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.md,
                    background: t.color.bgSubtle,
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: t.space.sm,
                      flexWrap: 'wrap',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={kpiModalPrimarySelected}
                      onChange={(e) => handleTogglePrimary(e.target.checked)}
                      disabled={kpiModalDraft.type !== 'primary'}
                    />
                    <span
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        color:
                          kpiModalDraft.type === 'primary'
                            ? t.color.text
                            : t.color.textSecondary,
                      }}
                    >
                      Set as primary KPI
                    </span>
                  </div>
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                    Only one KPI can be primary. Switch type to Primary to enable this
                    toggle.
                  </span>
                  {kpiModalErrors.primary && (
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                      {kpiModalErrors.primary}
                    </span>
                  )}
                </label>
              </div>

              {kpiTestError && (
                <div
                  style={{
                    border: `1px solid ${t.color.danger}`,
                    background: t.color.dangerSubtle,
                    color: t.color.danger,
                    borderRadius: t.radius.md,
                    padding: t.space.sm,
                    fontSize: t.font.sizeXs,
                  }}
                >
                  {kpiTestError}
                </div>
              )}

              {kpiTestResult && (
                <div
                  style={{
                    border: `1px solid ${
                      kpiTestResult.testAvailable ? t.color.borderLight : t.color.border
                    }`,
                    borderRadius: t.radius.md,
                    padding: t.space.md,
                    display: 'grid',
                    gap: t.space.xs,
                    background: t.color.surface,
                  }}
                >
                  <strong
                    style={{
                      fontSize: t.font.sizeSm,
                      color: t.color.text,
                    }}
                  >
                    Test results
                  </strong>
                  {kpiTestResult.testAvailable ? (
                    <>
                      <span
                        style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}
                      >
                        Events matched: {kpiTestResult.eventsMatched}
                      </span>
                      <span
                        style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}
                      >
                        Journeys impacted: {kpiTestResult.journeysMatched} of{' '}
                        {kpiTestResult.journeysTotal} (
                        {kpiTestResult.journeysPct.toFixed(1)}%)
                      </span>
                      {kpiTestResult.missingValueChecks ? (
                        <span
                          style={{
                            fontSize: t.font.sizeXs,
                            color: t.color.textSecondary,
                          }}
                        >
                          Missing value rate:{' '}
                          {kpiTestResult.missingValuePct == null
                            ? '—'
                            : `${kpiTestResult.missingValuePct.toFixed(1)}%`}(
                          {kpiTestResult.missingValueCount}/
                          {kpiTestResult.missingValueChecks})
                        </span>
                      ) : null}
                      {kpiTestResult.message && (
                        <span
                          style={{
                            fontSize: t.font.sizeXs,
                            color: t.color.textSecondary,
                          }}
                        >
                          {kpiTestResult.message}
                        </span>
                      )}
                    </>
                  ) : (
                    <span
                      style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}
                    >
                      {kpiTestResult.reason ?? 'Test unavailable. Load sample data first.'}
                    </span>
                  )}
                </div>
              )}

              <div
                style={{
                  display: 'flex',
                  justifyContent: 'flex-end',
                  gap: t.space.sm,
                  flexWrap: 'wrap',
                }}
              >
                <button
                  type="button"
                  onClick={handleCloseKpiModal}
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
                  onClick={() => void handleTestKpiDefinition()}
                  disabled={testKpiMutation.isPending}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                    background: 'transparent',
                    color: t.color.text,
                    fontSize: t.font.sizeSm,
                    cursor: testKpiMutation.isPending ? 'wait' : 'pointer',
                  }}
                >
                  {testKpiMutation.isPending ? 'Testing…' : 'Test definition'}
                </button>
                <button
                  type="button"
                  onClick={handleSubmitKpiModal}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.accent,
                    color: t.color.surface,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  Save KPI
                </button>
              </div>
            </div>
          </div>
        )}

        {kpiDeleteConfirm && (
          <div
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(15, 23, 42, 0.45)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: t.space.lg,
              zIndex: 25,
            }}
          >
            <div
              style={{
                width: 'min(420px, 100%)',
                background: t.color.surface,
                borderRadius: t.radius.lg,
                border: `1px solid ${t.color.border}`,
                boxShadow: t.shadowLg,
                padding: t.space.lg,
                display: 'grid',
                gap: t.space.md,
              }}
            >
              <div>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeLg,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Remove KPI “{kpiDeleteConfirm.definition.label}”?
                </h3>
                <p
                  style={{
                    margin: `${t.space.xs}px 0 0`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  This removes the KPI from the current draft. Historical reporting
                  remains intact.{' '}
                  {kpiDeleteConfirm.definition.id === kpiDraft.primary_kpi_id
                    ? 'Because this KPI is primary, you will need to select a new primary before saving.'
                    : ''}
                </p>
              </div>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'flex-end',
                  gap: t.space.sm,
                  flexWrap: 'wrap',
                }}
              >
                <button
                  type="button"
                  onClick={handleCancelDeleteKpi}
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
                  onClick={handleConfirmDeleteKpi}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.danger,
                    color: t.color.surface,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  Remove
                </button>
              </div>
            </div>
          </div>
        )}

      {toast && (
        <div
          style={{
            position: 'fixed',
            bottom: 24,
            right: 24,
            minWidth: 240,
            maxWidth: 360,
            padding: `${t.space.sm}px ${t.space.md}px`,
            borderRadius: t.radius.md,
            background:
              toast.type === 'success' ? t.color.success : t.color.danger,
            color: t.color.surface,
            fontSize: t.font.sizeSm,
            boxShadow: t.shadowSm,
            zIndex: 40,
          }}
        >
          {toast.message}
        </div>
      )}

        {pendingSectionChange && (
          <div
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(15, 23, 42, 0.45)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 20,
              padding: t.space.lg,
            }}
          >
            <div
              style={{
                width: 'min(460px, 100%)',
                background: t.color.surface,
                borderRadius: t.radius.lg,
                border: `1px solid ${t.color.border}`,
                boxShadow: t.shadowLg,
                padding: t.space.lg,
                display: 'grid',
                gap: t.space.md,
              }}
            >
              <div>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeLg,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Unsaved changes in {SECTION_META[activeSection].title}
                </h3>
                <p
                  style={{
                    margin: `${t.space.xs}px 0 0`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  You have pending edits. Save them before switching sections or discard them to continue.
                </p>
              </div>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'flex-end',
                  gap: t.space.sm,
                  flexWrap: 'wrap',
                }}
              >
                <button
                  type="button"
                  onClick={() => handleConfirmSectionChange('stay')}
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
                  Stay
                </button>
                <button
                  type="button"
                  onClick={() => handleConfirmSectionChange('discard')}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.borderLight,
                    color: t.color.text,
                    fontSize: t.font.sizeSm,
                    cursor: 'pointer',
                  }}
                >
                  Discard
                </button>
                <button
                  type="button"
                  onClick={() => void handleConfirmSectionChange('save')}
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.accent,
                    color: t.color.surface,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  Save & continue
                </button>
              </div>
            </div>
          </div>
        )}

        {showActivationModal && selectedConfigSummary && (
          <div
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(15, 23, 42, 0.55)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: 24,
              zIndex: 40,
            }}
          >
            <div
              style={{
                width: 'min(560px, 100%)',
                background: t.color.surface,
                borderRadius: t.radius.lg,
                border: `1px solid ${t.color.border}`,
                boxShadow: t.shadowLg,
                padding: t.space.lg,
                display: 'grid',
                gap: t.space.md,
                maxHeight: '90vh',
                overflowY: 'auto',
              }}
            >
              <div>
                <h3
                  style={{
                    margin: 0,
                    fontSize: t.font.sizeLg,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.text,
                  }}
                >
                  Activate {selectedConfigSummary.name} v{selectedConfigSummary.version}
                </h3>
                <p
                  style={{
                    margin: `${t.space.xs}px 0 0`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  Review validation and projected impact before promoting this draft to
                  active.
                </p>
              </div>

              {modelConfigValidation ? (
                <div
                  style={{
                    border: `1px solid ${
                      modelConfigValidation.valid ? t.color.success : t.color.warning
                    }`,
                    background: modelConfigValidation.valid
                      ? 'rgba(34,197,94,0.12)'
                      : t.color.warningSubtle,
                    borderRadius: t.radius.md,
                    padding: t.space.sm,
                    fontSize: t.font.sizeXs,
                  }}
                >
                  {modelConfigValidation.valid
                    ? 'Validation passed. Required fields look good.'
                    : 'Validation issues found. Resolve them before activating.'}
                </div>
              ) : (
                <div
                  style={{
                    border: `1px dashed ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.sm,
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                  }}
                >
                  Validation pending…
                </div>
              )}

              {activationSummary && activationSummary.preview_available && activationSummary.baseline && activationSummary.draft ? (
                <div
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.md,
                    display: 'grid',
                    gap: t.space.xs,
                  }}
                >
                  <span
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.text,
                    }}
                  >
                    Impact preview vs active config
                  </span>
                  <table
                    style={{
                      width: '100%',
                      borderCollapse: 'collapse',
                      fontSize: t.font.sizeXs,
                    }}
                  >
                    <thead>
                      <tr
                        style={{ borderBottom: `1px solid ${t.color.borderLight}` }}
                      >
                        <th style={{ textAlign: 'left', padding: '6px 8px' }}>Metric</th>
                        <th style={{ textAlign: 'right', padding: '6px 8px' }}>
                          Active
                        </th>
                        <th style={{ textAlign: 'right', padding: '6px 8px' }}>Draft</th>
                        <th style={{ textAlign: 'right', padding: '6px 8px' }}>Δ</th>
                        <th style={{ textAlign: 'right', padding: '6px 8px' }}>Δ%</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(['journeys', 'touchpoints', 'conversions'] as const).map(
                        (metric) => {
                          const baselineValue =
                            activationSummary.baseline?.[metric] ?? 0
                          const draftValue = activationSummary.draft?.[metric] ?? 0
                          const delta =
                            activationSummary.deltas?.[metric] ??
                            draftValue - baselineValue
                          const deltaPct =
                            activationSummary.deltas_pct?.[metric] ?? null
                          return (
                            <tr key={metric}>
                              <td style={{ padding: '6px 8px' }}>{metric}</td>
                              <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                {baselineValue}
                              </td>
                              <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                {draftValue}
                              </td>
                              <td
                                style={{
                                  padding: '6px 8px',
                                  textAlign: 'right',
                                  color:
                                    delta > 0
                                      ? t.color.success
                                      : delta < 0
                                      ? t.color.danger
                                      : t.color.text,
                                }}
                              >
                                {delta > 0 ? '+' : ''}
                                {delta}
                              </td>
                              <td
                                style={{
                                  padding: '6px 8px',
                                  textAlign: 'right',
                                  color:
                                    (deltaPct ?? 0) > 0
                                      ? t.color.success
                                      : (deltaPct ?? 0) < 0
                                      ? t.color.danger
                                      : t.color.text,
                                }}
                              >
                                {deltaPct == null
                                  ? '—'
                                  : `${deltaPct > 0 ? '+' : ''}${deltaPct.toFixed(2)}%`}
                              </td>
                            </tr>
                          )
                        },
                      )}
                    </tbody>
                  </table>
                  {activationSummary.warnings?.length ? (
                    <ul
                      style={{
                        margin: 0,
                        paddingLeft: 16,
                        fontSize: t.font.sizeXs,
                        color: t.color.warning,
                      }}
                    >
                      {activationSummary.warnings.map((warn) => (
                        <li key={warn}>{warn}</li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              ) : (
                <div
                  style={{
                    border: `1px dashed ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.sm,
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                  }}
                >
                  Impact preview unavailable. Activate with caution.
                </div>
              )}

              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.md,
                  padding: t.space.sm,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                }}
              >
                <strong>Changed sections:</strong>{' '}
                {activationSummary?.changed_keys?.length
                  ? activationSummary.changed_keys.join(', ')
                  : 'No material differences detected'}
              </div>

              <label
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: t.space.xs,
                }}
              >
                <span
                  style={{
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                  }}
                >
                  Activation note{' '}
                  <span style={{ color: t.color.textMuted }}>(optional)</span>
                </span>
                <textarea
                  value={activationNote}
                  onChange={(e) => setActivationNote(e.target.value)}
                  rows={3}
                  placeholder="Summarise why this configuration is being activated…"
                  style={{
                    width: '100%',
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                    padding: t.space.sm,
                    fontSize: t.font.sizeSm,
                  }}
                />
              </label>

              <div
                style={{
                  display: 'flex',
                  justifyContent: 'flex-end',
                  gap: t.space.sm,
                  flexWrap: 'wrap',
                }}
              >
                <button
                  type="button"
                  onClick={() => {
                    setShowActivationModal(false)
                    setActivationNote('')
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
                  onClick={async () => {
                    if (!selectedModelConfigId) return
                    try {
                      await activateModelConfigMutation.mutateAsync({
                        id: selectedModelConfigId,
                        activationNote: activationNote.trim() || undefined,
                      })
                    } catch (error) {
                      setModelConfigError(
                        (error as Error)?.message ?? 'Activation failed',
                      )
                    }
                  }}
                  disabled={
                    !selectedModelConfigId ||
                    !modelConfigValidation?.valid ||
                    activateModelConfigMutation.isPending ||
                    !!modelConfigParseError
                  }
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.success,
                    color: t.color.surface,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    cursor: activateModelConfigMutation.isPending ? 'wait' : 'pointer',
                    opacity:
                      !modelConfigValidation?.valid ||
                      activateModelConfigMutation.isPending
                        ? 0.8
                        : 1,
                  }}
                >
                  {activateModelConfigMutation.isPending ? 'Activating…' : 'Activate'}
                </button>
              </div>
            </div>
          </div>
        )}
      </>
    )
  },
)

export default SettingsPage
