import { useMemo, useState } from 'react'
import type { CSSProperties } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import DecisionStatusCard from '../components/DecisionStatusCard'
import RecommendedActionsList, { type RecommendedActionItem } from '../components/RecommendedActionsList'
import { navigateForRecommendedAction } from '../lib/recommendedActions'
import { apiGetJson, apiSendJson, getUserContext, withQuery } from '../lib/apiClient'

type FeatureFlags = {
  journeys_enabled: boolean
  journey_examples_enabled: boolean
  funnel_builder_enabled: boolean
  funnel_diagnostics_enabled: boolean
}

type Status = 'draft' | 'active' | 'archived'

interface JourneyReadiness {
  status: string
  summary: {
    journeys_loaded: number
    converted_journeys: number
    primary_kpi_coverage: number
    freshness_hours?: number | null
    taxonomy_unknown_share: number
    journey_validation_errors: number
    journey_validation_warnings: number
    active_settings_version?: string | null
  }
  blockers: string[]
  warnings: string[]
  recommended_actions: Array<{
    id: string
    label: string
    benefit?: string
    domain?: string
  }>
}

interface JourneySettingsVersion {
  id: string
  status: Status
  version_label: string
  description?: string | null
  created_at?: string | null
  updated_at?: string | null
  created_by?: string | null
  activated_at?: string | null
  activated_by?: string | null
  settings_json: Record<string, any>
  validation_json?: {
    valid?: boolean
    errors?: Array<{ path: string; message: string }>
    warnings?: Array<{ path: string; message: string }>
  } | null
  diff_json?: Record<string, any> | null
}

interface JourneySettingsContext {
  active_version_id?: string | null
  workspace_summary: {
    journeys_loaded: number
    observed_channels: number
    observed_event_names: number
    observed_steps: number
    observed_conversion_keys: number
  }
  observed_channels: Array<{ value: string; count: number }>
  observed_event_names: Array<{ value: string; count: number }>
  observed_steps: Array<{ value: string; count: number }>
  observed_conversion_keys: Array<{ value: string; count: number }>
  observed_kpis: Array<{ id: string; label: string; observed_count: number; is_primary?: boolean }>
  recommendations: {
    flow_defaults?: { min_volume_threshold?: number; max_nodes?: number }
    paths_explorer_defaults?: { top_paths_limit?: number }
    notes?: string[]
  }
  scaffold_settings_json: Record<string, any>
}

type FieldBadgeKind = 'derived' | 'recommended' | 'advanced' | 'inactive'

function formatDateTime(value?: string | null): string {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleString()
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

function setIn(obj: any, path: Array<string | number>, value: any): any {
  const next = deepClone(obj)
  let cursor = next
  for (let i = 0; i < path.length - 1; i += 1) {
    const key = path[i]
    if (cursor[key] == null) {
      cursor[key] = typeof path[i + 1] === 'number' ? [] : {}
    }
    cursor = cursor[key]
  }
  cursor[path[path.length - 1]] = value
  return next
}

function asInt(value: string, fallback: number): number {
  const n = Number(value)
  return Number.isFinite(n) ? Math.trunc(n) : fallback
}

function defaultJourneysTemplate() {
  return {
    schema_version: '1.0',
    sessionization: {
      session_timeout_minutes: 30,
      start_new_session_on_events: [],
      lookback_window_days: 30,
      conversion_journeys_only: true,
      allow_all_journeys: false,
      max_steps_per_journey: 20,
      dedup_consecutive_identical_steps: true,
      timezone_handling: 'platform_default',
    },
    step_canonicalization: {
      first_match_wins: true,
      fallback_step: 'Other',
      collapse_rare_steps_into_other_default: true,
      rules: [
        { step_name: 'Paid Landing', priority: 10, enabled: true, channel_group_equals: ['paid'] },
        { step_name: 'Organic Landing', priority: 20, enabled: true, channel_group_equals: ['organic'] },
        { step_name: 'Product View / Content View', priority: 30, enabled: true, event_name_equals: ['product_view', 'content_view'] },
        { step_name: 'Add to Cart / Form Start', priority: 40, enabled: true, event_name_equals: ['add_to_cart', 'form_start'] },
        { step_name: 'Checkout / Form Submit', priority: 50, enabled: true, event_name_equals: ['checkout', 'form_submit'] },
        { step_name: 'Purchase / Lead Won', priority: 60, enabled: true, event_name_equals: ['purchase', 'lead_won'] },
      ],
    },
    paths_explorer_defaults: {
      default_sort: 'conversions_desc',
      top_paths_limit: 50,
      group_low_frequency_paths_into_other: true,
      trend_metric: 'conversion_rate',
      comparison_window: 'previous_period',
    },
    flow_defaults: {
      max_depth: 4,
      min_volume_threshold: 20,
      rare_event_threshold: 10,
      collapse_rare_into_other: true,
      max_nodes: 30,
      always_show_conversion_terminal_node: true,
    },
    funnels_defaults: {
      default_counting_method: 'uniques',
      default_conversion_window_seconds: 604800,
      step_to_step_max_time_enabled: false,
      step_to_step_max_time_seconds: null,
      attribution_model_default: 'data_driven',
      breakdown_top_n: 5,
    },
    diagnostics_defaults: {
      enabled: false,
      baseline_mode: 'previous_period',
      sensitivity: 'medium',
      signals: {
        time_to_next_step_spike: true,
        device_skew_change: true,
        geo_skew_change: true,
        consent_opt_out_spike: false,
        error_event_rate_spike: false,
        landing_page_group_change: false,
      },
      output_policy: 'hypotheses_only',
      require_evidence_for_every_claim: true,
      confidence_thresholds: { low_max: 39, medium_max: 69, high_min: 70 },
    },
    performance_guardrails: {
      aggregation_reprocess_window_days: 3,
      journey_instance_sampling_retention_days: null,
      sampling_rate_example_journeys: 0,
      max_flow_query_lookback_window_days: 90,
      max_flow_date_range_days_before_weekly: 45,
    },
  }
}

function valuesEqual(left: any, right: any): boolean {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null)
}

function summarizeStepRule(rule: Record<string, any>): string[] {
  const parts: string[] = []
  const channels = Array.isArray(rule.channel_group_equals) ? rule.channel_group_equals.filter(Boolean) : []
  const events = Array.isArray(rule.event_name_equals) ? rule.event_name_equals.filter(Boolean) : []
  const urls = Array.isArray(rule.url_contains) ? rule.url_contains.filter(Boolean) : []
  const referrers = Array.isArray(rule.referrer_contains) ? rule.referrer_contains.filter(Boolean) : []
  if (channels.length) parts.push(`channels: ${channels.join(', ')}`)
  if (events.length) parts.push(`events: ${events.join(', ')}`)
  if (urls.length) parts.push(`url contains: ${urls.join(', ')}`)
  if (referrers.length) parts.push(`referrer: ${referrers.join(', ')}`)
  return parts
}

function normalizeRuleListInput(value: string): string[] {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function createEmptyStepRule(priority: number): Record<string, any> {
  return {
    step_name: '',
    priority,
    enabled: true,
    event_name_equals: [],
    channel_group_equals: [],
    url_contains: [],
    referrer_contains: [],
  }
}

function normalizedStringSet(values: unknown): Set<string> {
  if (!Array.isArray(values)) return new Set()
  return new Set(
    values
      .map((value) => String(value ?? '').trim().toLowerCase())
      .filter(Boolean),
  )
}

function analyzeStepRule(
  rule: Record<string, any>,
  {
    observedChannels,
    observedEvents,
    allRules,
    index,
  }: {
    observedChannels: Set<string>
    observedEvents: Set<string>
    allRules: Array<Record<string, any>>
    index: number
  },
): string[] {
  const warnings: string[] = []
  const channels = normalizedStringSet(rule.channel_group_equals)
  const events = normalizedStringSet(rule.event_name_equals)
  const urls = normalizedStringSet(rule.url_contains)
  const referrers = normalizedStringSet(rule.referrer_contains)
  const hasConditions = channels.size > 0 || events.size > 0 || urls.size > 0 || referrers.size > 0

  if (!hasConditions) {
    warnings.push('No conditions set. This rule will not match anything useful until you add at least one channel, event, URL, or referrer condition.')
  }
  if (!(rule.step_name || '').trim()) {
    warnings.push('Step name is empty.')
  }

  const unknownChannels = [...channels].filter((value) => !observedChannels.has(value))
  if (unknownChannels.length) {
    warnings.push(`Unknown channels for this workspace: ${unknownChannels.join(', ')}.`)
  }

  const unknownEvents = [...events].filter((value) => !observedEvents.has(value))
  if (unknownEvents.length) {
    warnings.push(`Unknown event names for this workspace: ${unknownEvents.join(', ')}.`)
  }

  const currentPriority = Number(rule.priority) || 0
  const overlaps = allRules.some((candidate, candidateIndex) => {
    if (candidateIndex === index) return false
    const candidatePriority = Number(candidate?.priority) || 0
    if (candidatePriority !== currentPriority) return false
    const candidateChannels = normalizedStringSet(candidate?.channel_group_equals)
    const candidateEvents = normalizedStringSet(candidate?.event_name_equals)
    const sharesChannels = [...channels].some((value) => candidateChannels.has(value))
    const sharesEvents = [...events].some((value) => candidateEvents.has(value))
    return sharesChannels || sharesEvents
  })
  if (overlaps) {
    warnings.push('Another rule shares this priority and some of the same channel/event conditions. Review ordering or specificity to avoid ambiguous matching.')
  }

  return warnings
}

function badgeStyle(kind: FieldBadgeKind): CSSProperties {
  if (kind === 'derived') {
    return {
      fontSize: t.font.sizeXs,
      padding: '2px 8px',
      borderRadius: 999,
      background: t.color.accentMuted,
      color: t.color.accent,
      border: `1px solid ${t.color.accent}`,
    }
  }
  if (kind === 'recommended') {
    return {
      fontSize: t.font.sizeXs,
      padding: '2px 8px',
      borderRadius: 999,
      background: t.color.successMuted,
      color: t.color.success,
      border: `1px solid ${t.color.success}`,
    }
  }
  if (kind === 'inactive') {
    return {
      fontSize: t.font.sizeXs,
      padding: '2px 8px',
      borderRadius: 999,
      background: t.color.bgSubtle,
      color: t.color.textMuted,
      border: `1px solid ${t.color.borderLight}`,
    }
  }
  return {
    fontSize: t.font.sizeXs,
    padding: '2px 8px',
    borderRadius: 999,
    background: t.color.warningSubtle,
    color: t.color.warning,
    border: `1px solid ${t.color.warning}`,
  }
}

function FieldBadge({ kind, children }: { kind: FieldBadgeKind; children: string }) {
  return <span style={badgeStyle(kind)}>{children}</span>
}

function LabeledField({
  label,
  kind,
}: {
  label: string
  kind?: FieldBadgeKind
}) {
  return (
    <span style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', fontSize: t.font.sizeXs }}>
      <span>{label}</span>
      {kind ? <FieldBadge kind={kind}>{kind}</FieldBadge> : null}
    </span>
  )
}

export default function JourneysSettingsSection({
  featureFlags,
}: {
  featureFlags: FeatureFlags
}) {
  const queryClient = useQueryClient()
  const role = useMemo(() => getUserContext().role.trim().toLowerCase(), [])
  const canActivate = role === 'admin' || role === 'power_user' || role === 'editor'

  const [statusFilter, setStatusFilter] = useState<'all' | Status>('all')
  const [search, setSearch] = useState('')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [editorMode, setEditorMode] = useState<'basic' | 'advanced'>('basic')
  const [draftJson, setDraftJson] = useState('')
  const [draftObj, setDraftObj] = useState<Record<string, any> | null>(null)
  const [draftDescription, setDraftDescription] = useState('')
  const [parseError, setParseError] = useState<string | null>(null)
  const [validationResult, setValidationResult] = useState<any | null>(null)
  const [previewResult, setPreviewResult] = useState<any | null>(null)

  const versionsQuery = useQuery<JourneySettingsVersion[]>({
    queryKey: ['journeys-settings-versions'],
    queryFn: async () => apiGetJson<JourneySettingsVersion[]>('/api/settings/journeys/versions', {
      fallbackMessage: 'Failed to load journey settings versions',
    }),
  })

  const readinessQuery = useQuery<JourneyReadiness>({
    queryKey: ['journeys-settings-readiness'],
    queryFn: async () => apiGetJson<JourneyReadiness>('/api/settings/journeys/readiness', {
      fallbackMessage: 'Failed to load journeys readiness',
    }),
  })

  const contextQuery = useQuery<JourneySettingsContext>({
    queryKey: ['journeys-settings-context'],
    queryFn: async () => apiGetJson<JourneySettingsContext>('/api/settings/journeys/context', {
      fallbackMessage: 'Failed to load journey settings context',
    }),
  })

  const createDraftMutation = useMutation({
    mutationFn: async (payload: { description?: string; settings_json?: Record<string, any> }) => {
      return apiSendJson<JourneySettingsVersion>('/api/settings/journeys/versions', 'POST', {
        created_by: 'ui',
        ...payload,
      }, {
        fallbackMessage: 'Failed to create journey settings draft',
      })
    },
    onSuccess: (next) => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
      setSelectedId(next.id)
      setDraftObj(next.settings_json)
      setDraftJson(JSON.stringify(next.settings_json, null, 2))
      setDraftDescription(next.description ?? '')
      setParseError(null)
    },
  })

  const updateDraftMutation = useMutation({
    mutationFn: async (payload: { id: string; settings_json: Record<string, any>; description?: string }) => {
      return apiSendJson<JourneySettingsVersion>(`/api/settings/journeys/versions/${payload.id}`, 'PATCH', {
        actor: 'ui',
        settings_json: payload.settings_json,
        description: payload.description,
      }, {
        fallbackMessage: 'Failed to save draft',
      })
    },
    onSuccess: (next) => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
      setDraftObj(next.settings_json)
      setDraftJson(JSON.stringify(next.settings_json, null, 2))
      setDraftDescription(next.description ?? '')
    },
  })

  const archiveMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiSendJson<JourneySettingsVersion>(withQuery(`/api/settings/journeys/versions/${id}/archive`, { actor: 'ui' }), 'POST', undefined, {
        fallbackMessage: 'Failed to archive draft',
      })
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
    },
  })

  const validateMutation = useMutation({
    mutationFn: async (settings: Record<string, any>) => {
      return apiSendJson<any>('/api/settings/journeys/validate', 'POST', {
        settings_json: settings,
      }, {
        fallbackMessage: 'Failed to validate draft',
      })
    },
    onSuccess: (data) => setValidationResult(data),
  })

  const previewMutation = useMutation({
    mutationFn: async (settings: Record<string, any>) => {
      return apiSendJson<any>('/api/settings/journeys/preview', 'POST', {
        settings_json: settings,
      }, {
        fallbackMessage: 'Failed to preview draft impact',
      })
    },
    onSuccess: (data) => setPreviewResult(data),
  })

  const activateMutation = useMutation({
    mutationFn: async (payload: { version_id: string; activation_note?: string }) => {
      return apiSendJson<any>('/api/settings/journeys/activate', 'POST', {
        ...payload,
        actor: 'ui',
        confirm: true,
      }, {
        fallbackMessage: 'Failed to activate settings',
      })
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
    },
  })

  const versions = versionsQuery.data ?? []
  const activeVersion = useMemo(() => versions.find((v) => v.status === 'active') ?? null, [versions])
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    return versions.filter((v) => {
      if (statusFilter !== 'all' && v.status !== statusFilter) return false
      if (!q) return true
      return (
        v.version_label.toLowerCase().includes(q) ||
        (v.description || '').toLowerCase().includes(q)
      )
    })
  }, [search, statusFilter, versions])

  const selected = useMemo(
    () => versions.find((v) => v.id === selectedId) ?? filtered[0] ?? null,
    [filtered, selectedId, versions],
  )

  const isDraft = selected?.status === 'draft'
  const hasDraft = !!draftObj && isDraft

  const loadSelected = (item: JourneySettingsVersion) => {
    setSelectedId(item.id)
    setDraftObj(item.settings_json)
    setDraftJson(JSON.stringify(item.settings_json, null, 2))
    setDraftDescription(item.description ?? '')
    setValidationResult(item.validation_json ?? null)
    setPreviewResult(null)
    setParseError(null)
  }

  const updateDraft = (next: Record<string, any>) => {
    setDraftObj(next)
    setDraftJson(JSON.stringify(next, null, 2))
  }

  const updateDraftWith = (updater: (next: Record<string, any>) => void) => {
    if (!draftObj) return
    const next = deepClone(draftObj)
    updater(next)
    updateDraft(next)
  }

  const cardStyle: CSSProperties = {
    border: `1px solid ${t.color.borderLight}`,
    borderRadius: t.radius.md,
    background: t.color.surface,
    padding: t.space.md,
    display: 'grid',
    gap: t.space.sm,
  }

  const handleRecommendedAction = (action: RecommendedActionItem) => {
    navigateForRecommendedAction(action, { defaultPage: 'settings' })
  }

  const scaffoldSettingsJson = contextQuery.data?.scaffold_settings_json ?? activeVersion?.settings_json ?? defaultJourneysTemplate()
  const activeSettingsJson = activeVersion?.settings_json ?? null
  const suggestedStepRules = contextQuery.data?.scaffold_settings_json?.step_canonicalization?.rules ?? []
  const activeStepRules = activeSettingsJson?.step_canonicalization?.rules ?? []
  const draftStepRules = draftObj?.step_canonicalization?.rules ?? []
  const recommendedTopPathsLimit = contextQuery.data?.recommendations?.paths_explorer_defaults?.top_paths_limit ?? 50
  const recommendedMinVolumeThreshold = contextQuery.data?.recommendations?.flow_defaults?.min_volume_threshold ?? 20
  const recommendedMaxNodes = contextQuery.data?.recommendations?.flow_defaults?.max_nodes ?? 30
  const draftMatchesSuggestedRules = valuesEqual(draftStepRules, suggestedStepRules)
  const draftMatchesActiveRules = valuesEqual(draftStepRules, activeStepRules)
  const hasObservedSignals = (contextQuery.data?.workspace_summary.observed_channels ?? 0) > 0 || (contextQuery.data?.workspace_summary.observed_event_names ?? 0) > 0
  const hasObservedStepSignals = (contextQuery.data?.workspace_summary.observed_event_names ?? 0) > 0 || (contextQuery.data?.workspace_summary.observed_steps ?? 0) > 0
  const showFunnelsDiagnosticsSection = featureFlags.funnel_builder_enabled || featureFlags.funnel_diagnostics_enabled
  const diagnosticsInactive = !featureFlags.funnel_diagnostics_enabled
  const funnelsInactive = !featureFlags.funnel_builder_enabled
  const nextStepRulePriority = Math.max(
    10,
    ...draftStepRules.map((rule: Record<string, any>) => Number(rule?.priority) || 0),
  ) + 10
  const observedChannelSet = useMemo(
    () => new Set((contextQuery.data?.observed_channels ?? []).map((item) => String(item.value ?? '').trim().toLowerCase()).filter(Boolean)),
    [contextQuery.data?.observed_channels],
  )
  const observedEventSet = useMemo(
    () => new Set((contextQuery.data?.observed_event_names ?? []).map((item) => String(item.value ?? '').trim().toLowerCase()).filter(Boolean)),
    [contextQuery.data?.observed_event_names],
  )

  return (
    <div style={{ display: 'grid', gap: t.space.xl }}>
      <div style={cardStyle}>
        <div>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Journeys readiness</h3>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Combined readiness from journeys health, taxonomy, KPI coverage, and active journey settings.
          </p>
        </div>

        {readinessQuery.isError ? (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
            {(readinessQuery.error as Error)?.message}
          </div>
        ) : null}

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Status</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: readinessQuery.data?.status === 'blocked' ? t.color.danger : readinessQuery.data?.status === 'warning' ? t.color.warning : t.color.success }}>
              {readinessQuery.isLoading ? '…' : (readinessQuery.data?.status || 'unknown')}
            </div>
          </div>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Journeys loaded</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{readinessQuery.data?.summary.journeys_loaded ?? 0}</div>
          </div>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Primary KPI coverage</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{`${(((readinessQuery.data?.summary.primary_kpi_coverage ?? 0) * 100)).toFixed(1)}%`}</div>
          </div>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Taxonomy unknown share</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{`${(((readinessQuery.data?.summary.taxonomy_unknown_share ?? 0) * 100)).toFixed(1)}%`}</div>
          </div>
        </div>

      {(readinessQuery.data?.blockers?.length || readinessQuery.data?.warnings?.length || readinessQuery.data?.recommended_actions?.length) ? (
          <DecisionStatusCard
            title="Journeys Readiness Guidance"
            status={readinessQuery.data?.status}
            blockers={readinessQuery.data?.blockers}
            warnings={readinessQuery.data?.warnings}
            actions={readinessQuery.data?.recommended_actions}
            onActionClick={handleRecommendedAction}
            compact
          />
        ) : null}
      </div>

      <div style={cardStyle}>
        <div>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Workspace-derived context</h3>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            New drafts are now seeded from active settings plus observed workspace signals instead of static frontend defaults.
          </p>
        </div>

        {contextQuery.isError ? (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
            {(contextQuery.error as Error)?.message}
          </div>
        ) : null}

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed channels</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{contextQuery.data?.workspace_summary.observed_channels ?? 0}</div>
          </div>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed event names</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{contextQuery.data?.workspace_summary.observed_event_names ?? 0}</div>
          </div>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed steps</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{contextQuery.data?.workspace_summary.observed_steps ?? 0}</div>
          </div>
          <div style={cardStyle}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed KPIs</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{contextQuery.data?.workspace_summary.observed_conversion_keys ?? 0}</div>
          </div>
        </div>

        {contextQuery.data ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: t.space.sm }}>
            <div style={cardStyle}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Observed channels</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {(contextQuery.data.observed_channels ?? []).slice(0, 8).map((item) => (
                  <span key={item.value} style={{ fontSize: t.font.sizeXs, padding: '2px 8px', borderRadius: 999, background: t.color.accentMuted, color: t.color.accent }}>
                    {item.value} · {item.count}
                  </span>
                ))}
              </div>
            </div>
            <div style={cardStyle}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Observed event names</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {(contextQuery.data.observed_event_names ?? []).slice(0, 8).map((item) => (
                  <span key={item.value} style={{ fontSize: t.font.sizeXs, padding: '2px 8px', borderRadius: 999, background: t.color.surfaceMuted, color: t.color.text }}>
                    {item.value}
                  </span>
                ))}
              </div>
            </div>
            <div style={cardStyle}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Observed KPI usage</div>
              <div style={{ display: 'grid', gap: 4 }}>
                {(contextQuery.data.observed_kpis ?? []).filter((item) => item.observed_count > 0).slice(0, 6).map((item) => (
                  <div key={item.id} style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    {item.label}{item.is_primary ? ' (primary)' : ''} · {item.observed_count}
                  </div>
                ))}
              </div>
            </div>
            <div style={cardStyle}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Recommended defaults</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                Flow min volume: {contextQuery.data.recommendations?.flow_defaults?.min_volume_threshold ?? '—'}
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                Flow max nodes: {contextQuery.data.recommendations?.flow_defaults?.max_nodes ?? '—'}
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                Top paths limit: {contextQuery.data.recommendations?.paths_explorer_defaults?.top_paths_limit ?? '—'}
              </div>
            </div>
          </div>
        ) : null}

        {!!contextQuery.data?.recommendations?.notes?.length && (
          <div style={{ display: 'grid', gap: 4 }}>
            {contextQuery.data.recommendations.notes.map((note) => (
              <div key={note} style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                {note}
              </div>
            ))}
          </div>
        )}

        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          <FieldBadge kind="derived">derived</FieldBadge>
          <FieldBadge kind="recommended">recommended</FieldBadge>
          <FieldBadge kind="advanced">advanced</FieldBadge>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            Derived values come from observed workspace data. Recommended values are planner defaults. Advanced values stay available in JSON but are not needed for most workspaces.
          </div>
        </div>
      </div>

      <div style={cardStyle}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Journeys settings versions</h3>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Draft/Active/Archived lifecycle for paths, flow, funnels, and diagnostics defaults.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={() => createDraftMutation.mutate({ settings_json: scaffoldSettingsJson, description: 'New journeys draft' })}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.accent}`,
                background: t.color.accentMuted,
                color: t.color.accent,
                cursor: 'pointer',
                fontSize: t.font.sizeXs,
              }}
            >
              {createDraftMutation.isPending ? 'Creating…' : 'New draft'}
            </button>
            <button
              type="button"
              disabled={!activeVersion}
              onClick={() =>
                createDraftMutation.mutate({
                  settings_json: activeVersion?.settings_json ?? scaffoldSettingsJson,
                  description: activeVersion ? `Cloned from ${activeVersion.version_label}` : 'Cloned draft',
                })
              }
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: 'transparent',
                color: t.color.text,
                cursor: activeVersion ? 'pointer' : 'not-allowed',
                opacity: activeVersion ? 1 : 0.6,
                fontSize: t.font.sizeXs,
              }}
            >
              Duplicate active
            </button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(260px, 340px) 1fr', gap: t.space.md }}>
          <div style={{ display: 'grid', gap: t.space.xs, alignContent: 'start' }}>
            <div style={{ display: 'flex', gap: t.space.xs }}>
              <input
                type="search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search versions"
                style={{ flex: 1, padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}` }}
              />
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value as any)}
                style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}` }}
              >
                <option value="all">All</option>
                <option value="draft">Draft</option>
                <option value="active">Active</option>
                <option value="archived">Archived</option>
              </select>
            </div>

            {filtered.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => loadSelected(item)}
                style={{
                  textAlign: 'left',
                  borderRadius: t.radius.sm,
                  border: `1px solid ${selected?.id === item.id ? t.color.accent : t.color.borderLight}`,
                  background: selected?.id === item.id ? t.color.accentMuted : t.color.surface,
                  padding: t.space.sm,
                  cursor: 'pointer',
                  display: 'grid',
                  gap: 4,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm }}>
                  <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {item.version_label}
                  </span>
                  <span
                    style={{
                      fontSize: t.font.sizeXs,
                      padding: '2px 8px',
                      borderRadius: 999,
                      border: `1px solid ${
                        item.status === 'active' ? t.color.success : item.status === 'draft' ? t.color.warning : t.color.border
                      }`,
                      color: item.status === 'active' ? t.color.success : item.status === 'draft' ? t.color.warning : t.color.textSecondary,
                    }}
                  >
                    {item.status}
                  </span>
                </div>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  {item.description || 'No description'}
                </span>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  Created {formatDateTime(item.created_at)}
                </span>
              </button>
            ))}

            {filtered.length === 0 && (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, padding: t.space.sm }}>
                No versions found.
              </div>
            )}
          </div>

          <div style={{ display: 'grid', gap: t.space.sm }}>
            {!selected ? (
              <div style={{ ...cardStyle, color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Select a version to edit or review.
              </div>
            ) : (
              <>
                <div style={cardStyle}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
                    <div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        {selected.version_label} • {selected.status}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Created by {selected.created_by || 'unknown'} · {formatDateTime(selected.created_at)}
                      </div>
                      {selected.activated_at && (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Activated {formatDateTime(selected.activated_at)}
                        </div>
                      )}
                    </div>
                    <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                      <button
                        type="button"
                        onClick={() => setEditorMode('basic')}
                        style={{
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${editorMode === 'basic' ? t.color.accent : t.color.borderLight}`,
                          background: editorMode === 'basic' ? t.color.accentMuted : 'transparent',
                          color: editorMode === 'basic' ? t.color.accent : t.color.text,
                        }}
                      >
                        Basic
                      </button>
                      <button
                        type="button"
                        onClick={() => setEditorMode('advanced')}
                        style={{
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${editorMode === 'advanced' ? t.color.accent : t.color.borderLight}`,
                          background: editorMode === 'advanced' ? t.color.accentMuted : 'transparent',
                          color: editorMode === 'advanced' ? t.color.accent : t.color.text,
                        }}
                      >
                        Advanced JSON
                      </button>
                    </div>
                  </div>

                  <label style={{ display: 'grid', gap: 4 }}>
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Description</span>
                    <input
                      value={draftDescription}
                      onChange={(e) => setDraftDescription(e.target.value)}
                      disabled={!isDraft}
                      style={{
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.borderLight}`,
                        fontSize: t.font.sizeSm,
                        background: isDraft ? t.color.surface : t.color.bgSubtle,
                      }}
                    />
                  </label>

                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                    <button
                      type="button"
                      disabled={!hasDraft || updateDraftMutation.isPending}
                      onClick={() => {
                        if (!draftObj || !selected) return
                        updateDraftMutation.mutate({
                          id: selected.id,
                          settings_json: draftObj,
                          description: draftDescription.trim() || undefined,
                        })
                      }}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: 'none', background: t.color.accent, color: t.color.surface, fontSize: t.font.sizeXs, cursor: hasDraft ? 'pointer' : 'not-allowed', opacity: hasDraft ? 1 : 0.6 }}
                    >
                      {updateDraftMutation.isPending ? 'Saving…' : 'Save draft'}
                    </button>
                    <button
                      type="button"
                      disabled={!hasDraft || validateMutation.isPending}
                      onClick={() => draftObj && validateMutation.mutate(draftObj)}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: 'transparent', fontSize: t.font.sizeXs }}
                    >
                      {validateMutation.isPending ? 'Validating…' : 'Validate draft'}
                    </button>
                    <button
                      type="button"
                      disabled={!hasDraft || previewMutation.isPending}
                      onClick={() => draftObj && previewMutation.mutate(draftObj)}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: 'transparent', fontSize: t.font.sizeXs }}
                    >
                      {previewMutation.isPending ? 'Calculating…' : 'Impact preview'}
                    </button>
                    <button
                      type="button"
                      disabled={!selected || !canActivate || activateMutation.isPending}
                      onClick={() => {
                        if (!selected) return
                        const ok = window.confirm(`Activate ${selected.version_label}? This archives the previously active version.`)
                        if (!ok) return
                        activateMutation.mutate({ version_id: selected.id })
                      }}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: 'none', background: t.color.success, color: t.color.surface, fontSize: t.font.sizeXs, opacity: canActivate ? 1 : 0.6 }}
                    >
                      {activateMutation.isPending ? 'Activating…' : 'Activate'}
                    </button>
                    {isDraft && (
                      <button
                        type="button"
                        onClick={() => selected && archiveMutation.mutate(selected.id)}
                        style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: 'none', background: t.color.dangerSubtle, color: t.color.danger, fontSize: t.font.sizeXs }}
                      >
                        Archive draft
                      </button>
                    )}
                  </div>

                  {!canActivate && (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Activation requires `admin` or `power_user` role.
                    </div>
                  )}
                </div>

                {editorMode === 'advanced' ? (
                  <div style={cardStyle}>
                    <textarea
                      value={draftJson}
                      onChange={(e) => {
                        const nextText = e.target.value
                        setDraftJson(nextText)
                        try {
                          const parsed = JSON.parse(nextText)
                          setDraftObj(parsed)
                          setParseError(null)
                        } catch (error) {
                          setParseError((error as Error).message)
                        }
                      }}
                      disabled={!isDraft}
                      rows={22}
                      style={{
                        width: '100%',
                        minHeight: 360,
                        fontFamily: 'monospace',
                        fontSize: t.font.sizeXs,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${parseError ? t.color.danger : t.color.borderLight}`,
                        background: isDraft ? t.color.bgSubtle : t.color.surface,
                        padding: t.space.sm,
                      }}
                    />
                    {parseError && (
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        JSON parse error: {parseError}
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    <details open style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                        Data bindings & Journey Construction
                        <FieldBadge kind="derived">derived</FieldBadge>
                        <FieldBadge kind="recommended">recommended</FieldBadge>
                      </summary>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        These controls shape how journeys are built from your workspace data. Channels, events, and suggested steps are derived from observed data above.
                      </div>
                      {!hasObservedSignals ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>
                          No strong workspace signals are available yet. New drafts will still work, but they are closer to generic defaults until more journeys and touchpoints are loaded.
                        </div>
                      ) : null}
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: t.space.sm }}>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Session timeout (minutes)" kind="recommended" />
                          <input type="number" value={draftObj?.sessionization?.session_timeout_minutes ?? 30} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'session_timeout_minutes'], asInt(e.target.value, 30)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Lookback window (days)" kind="recommended" />
                          <input type="number" value={draftObj?.sessionization?.lookback_window_days ?? 30} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'lookback_window_days'], asInt(e.target.value, 30)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Max steps per journey" kind="advanced" />
                          <input type="number" value={draftObj?.sessionization?.max_steps_per_journey ?? 20} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'max_steps_per_journey'], asInt(e.target.value, 20)))} disabled={!isDraft} />
                        </label>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Observed journey volume in this workspace: {contextQuery.data?.workspace_summary.journeys_loaded ?? 0} journeys.
                      </div>
                      <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: t.font.sizeXs }}>
                        <input type="checkbox" checked={!!draftObj?.sessionization?.conversion_journeys_only} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'conversion_journeys_only'], e.target.checked))} disabled={!isDraft} />
                        Conversion journeys only (default ON)
                      </label>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Full-session overrides like `allow_all_journeys` stay in Advanced JSON because they change the meaning of the analysis rather than the default workspace policy.
                      </div>
                    </details>

                    <details open style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                        Step Canonicalization
                        <FieldBadge kind="derived">derived</FieldBadge>
                        <FieldBadge kind={hasObservedStepSignals ? 'recommended' : 'advanced'}>
                          {hasObservedStepSignals ? 'recommended' : 'advanced'}
                        </FieldBadge>
                      </summary>
                      {!hasObservedStepSignals ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          This workspace has not produced enough event/step evidence yet, so step mapping remains mostly manual until more touchpoint data is available.
                        </div>
                      ) : null}
                      <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                        <button
                          type="button"
                          disabled={!isDraft || !suggestedStepRules.length || draftMatchesSuggestedRules}
                          onClick={() => updateDraft(setIn(draftObj, ['step_canonicalization', 'rules'], deepClone(suggestedStepRules)))}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.accent}`,
                            background: t.color.accentMuted,
                            color: t.color.accent,
                            fontSize: t.font.sizeXs,
                            cursor: !isDraft || !suggestedStepRules.length || draftMatchesSuggestedRules ? 'not-allowed' : 'pointer',
                            opacity: !isDraft || !suggestedStepRules.length || draftMatchesSuggestedRules ? 0.6 : 1,
                          }}
                        >
                          Apply workspace suggestions
                        </button>
                        <button
                          type="button"
                          disabled={!isDraft || !activeStepRules.length || draftMatchesActiveRules}
                          onClick={() => updateDraft(setIn(draftObj, ['step_canonicalization', 'rules'], deepClone(activeStepRules)))}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.border}`,
                            background: 'transparent',
                            color: t.color.text,
                            fontSize: t.font.sizeXs,
                            cursor: !isDraft || !activeStepRules.length || draftMatchesActiveRules ? 'not-allowed' : 'pointer',
                            opacity: !isDraft || !activeStepRules.length || draftMatchesActiveRules ? 0.6 : 1,
                          }}
                        >
                          Restore active rules
                        </button>
                      </div>
                      <label style={{ display: 'grid', gap: 4 }}>
                        <LabeledField label="Fallback step" kind="recommended" />
                        <input value={draftObj?.step_canonicalization?.fallback_step ?? 'Other'} onChange={(e) => updateDraft(setIn(draftObj, ['step_canonicalization', 'fallback_step'], e.target.value))} disabled={!isDraft} />
                      </label>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: t.space.sm }}>
                        <div style={{ ...cardStyle, padding: t.space.sm }}>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Workspace suggestions</div>
                          {(suggestedStepRules ?? []).length ? suggestedStepRules.map((rule: Record<string, any>, index: number) => (
                            <div key={`${rule.step_name}-${index}`} style={{ display: 'grid', gap: 4 }}>
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center' }}>
                                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>{rule.step_name}</div>
                                <button
                                  type="button"
                                  disabled={!isDraft}
                                  onClick={() =>
                                    updateDraftWith((next) => {
                                      next.step_canonicalization = next.step_canonicalization ?? {}
                                      next.step_canonicalization.rules = Array.isArray(next.step_canonicalization.rules)
                                        ? [...next.step_canonicalization.rules, deepClone(rule)]
                                        : [deepClone(rule)]
                                    })
                                  }
                                  style={{
                                    padding: `2px ${t.space.xs}px`,
                                    borderRadius: t.radius.sm,
                                    border: `1px solid ${t.color.border}`,
                                    background: 'transparent',
                                    color: t.color.text,
                                    fontSize: t.font.sizeXs,
                                    cursor: !isDraft ? 'not-allowed' : 'pointer',
                                    opacity: !isDraft ? 0.6 : 1,
                                  }}
                                >
                                  Add
                                </button>
                              </div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                                {summarizeStepRule(rule).join(' · ') || 'Generic fallback rule'}
                              </div>
                            </div>
                          )) : (
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                              No workspace-derived rule suggestions yet. Add more touchpoint history or use Advanced JSON for custom rules.
                            </div>
                          )}
                        </div>
                        <div style={{ ...cardStyle, padding: t.space.sm }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Draft rules in use</div>
                            <button
                              type="button"
                              disabled={!isDraft}
                              onClick={() =>
                                updateDraftWith((next) => {
                                  next.step_canonicalization = next.step_canonicalization ?? {}
                                  next.step_canonicalization.rules = Array.isArray(next.step_canonicalization.rules)
                                    ? [...next.step_canonicalization.rules, createEmptyStepRule(nextStepRulePriority)]
                                    : [createEmptyStepRule(nextStepRulePriority)]
                                })
                              }
                              style={{
                                padding: `2px ${t.space.xs}px`,
                                borderRadius: t.radius.sm,
                                border: `1px solid ${t.color.accent}`,
                                background: t.color.accentMuted,
                                color: t.color.accent,
                                fontSize: t.font.sizeXs,
                                cursor: !isDraft ? 'not-allowed' : 'pointer',
                                opacity: !isDraft ? 0.6 : 1,
                              }}
                            >
                              Add empty rule
                            </button>
                          </div>
                          {(draftStepRules ?? []).length ? draftStepRules.map((rule: Record<string, any>, index: number) => {
                            const ruleWarnings = analyzeStepRule(rule, {
                              observedChannels: observedChannelSet,
                              observedEvents: observedEventSet,
                              allRules: draftStepRules,
                              index,
                            })
                            return (
                              <div key={`${rule.step_name || 'rule'}-${index}`} style={{ display: 'grid', gap: t.space.xs, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                                      {rule.step_name || `Rule ${index + 1}`}
                                    </div>
                                    {rule.enabled === false ? <FieldBadge kind="inactive">disabled</FieldBadge> : null}
                                    {ruleWarnings.length ? <FieldBadge kind="advanced">needs review</FieldBadge> : null}
                                  </div>
                                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                                    <button
                                      type="button"
                                      disabled={!isDraft}
                                      onClick={() =>
                                        updateDraftWith((next) => {
                                          const rules = Array.isArray(next.step_canonicalization?.rules) ? [...next.step_canonicalization.rules] : []
                                          const target = { ...(rules[index] ?? {}) }
                                          target.enabled = !(target.enabled ?? true)
                                          rules[index] = target
                                          next.step_canonicalization = next.step_canonicalization ?? {}
                                          next.step_canonicalization.rules = rules
                                        })
                                      }
                                      style={{
                                        padding: `2px ${t.space.xs}px`,
                                        borderRadius: t.radius.sm,
                                        border: `1px solid ${t.color.border}`,
                                        background: 'transparent',
                                        color: t.color.text,
                                        fontSize: t.font.sizeXs,
                                        cursor: !isDraft ? 'not-allowed' : 'pointer',
                                        opacity: !isDraft ? 0.6 : 1,
                                      }}
                                    >
                                      {rule.enabled === false ? 'Enable' : 'Disable'}
                                    </button>
                                    <button
                                      type="button"
                                      disabled={!isDraft}
                                      onClick={() =>
                                        updateDraftWith((next) => {
                                          const rules = Array.isArray(next.step_canonicalization?.rules) ? [...next.step_canonicalization.rules] : []
                                          rules.splice(index, 1)
                                          next.step_canonicalization = next.step_canonicalization ?? {}
                                          next.step_canonicalization.rules = rules
                                        })
                                      }
                                      style={{
                                        padding: `2px ${t.space.xs}px`,
                                        borderRadius: t.radius.sm,
                                        border: `1px solid ${t.color.danger}`,
                                        background: t.color.dangerSubtle,
                                        color: t.color.danger,
                                        fontSize: t.font.sizeXs,
                                        cursor: !isDraft ? 'not-allowed' : 'pointer',
                                        opacity: !isDraft ? 0.6 : 1,
                                      }}
                                    >
                                      Remove
                                    </button>
                                  </div>
                                </div>
                                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.xs }}>
                                  <label style={{ display: 'grid', gap: 4 }}>
                                    <LabeledField label="Step name" kind="recommended" />
                                    <input
                                      value={rule.step_name ?? ''}
                                      disabled={!isDraft}
                                      onChange={(e) =>
                                        updateDraftWith((next) => {
                                          const rules = Array.isArray(next.step_canonicalization?.rules) ? [...next.step_canonicalization.rules] : []
                                          const target = { ...(rules[index] ?? {}) }
                                          target.step_name = e.target.value
                                          rules[index] = target
                                          next.step_canonicalization = next.step_canonicalization ?? {}
                                          next.step_canonicalization.rules = rules
                                        })
                                      }
                                    />
                                  </label>
                                  <label style={{ display: 'grid', gap: 4 }}>
                                    <LabeledField label="Priority" kind="advanced" />
                                    <input
                                      type="number"
                                      value={rule.priority ?? (index + 1) * 10}
                                      disabled={!isDraft}
                                      onChange={(e) =>
                                        updateDraftWith((next) => {
                                          const rules = Array.isArray(next.step_canonicalization?.rules) ? [...next.step_canonicalization.rules] : []
                                          const target = { ...(rules[index] ?? {}) }
                                          target.priority = asInt(e.target.value, (index + 1) * 10)
                                          rules[index] = target
                                          next.step_canonicalization = next.step_canonicalization ?? {}
                                          next.step_canonicalization.rules = rules
                                        })
                                      }
                                    />
                                  </label>
                                </div>
                                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.xs }}>
                                  <label style={{ display: 'grid', gap: 4 }}>
                                    <LabeledField label="Channels" kind="derived" />
                                    <input
                                      value={Array.isArray(rule.channel_group_equals) ? rule.channel_group_equals.join(', ') : ''}
                                      placeholder="paid_search, organic_search"
                                      disabled={!isDraft}
                                      onChange={(e) =>
                                        updateDraftWith((next) => {
                                          const rules = Array.isArray(next.step_canonicalization?.rules) ? [...next.step_canonicalization.rules] : []
                                          const target = { ...(rules[index] ?? {}) }
                                          target.channel_group_equals = normalizeRuleListInput(e.target.value)
                                          rules[index] = target
                                          next.step_canonicalization = next.step_canonicalization ?? {}
                                          next.step_canonicalization.rules = rules
                                        })
                                      }
                                    />
                                  </label>
                                  <label style={{ display: 'grid', gap: 4 }}>
                                    <LabeledField label="Event names" kind="derived" />
                                    <input
                                      value={Array.isArray(rule.event_name_equals) ? rule.event_name_equals.join(', ') : ''}
                                      placeholder="product_view, form_submit"
                                      disabled={!isDraft}
                                      onChange={(e) =>
                                        updateDraftWith((next) => {
                                          const rules = Array.isArray(next.step_canonicalization?.rules) ? [...next.step_canonicalization.rules] : []
                                          const target = { ...(rules[index] ?? {}) }
                                          target.event_name_equals = normalizeRuleListInput(e.target.value)
                                          rules[index] = target
                                          next.step_canonicalization = next.step_canonicalization ?? {}
                                          next.step_canonicalization.rules = rules
                                        })
                                      }
                                    />
                                  </label>
                                </div>
                                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                                  {summarizeStepRule(rule).join(' · ') || 'Add at least one channel or event condition to make this rule meaningful.'}
                                </div>
                                {ruleWarnings.length ? (
                                  <div style={{ display: 'grid', gap: 4 }}>
                                    {ruleWarnings.map((warning) => (
                                      <div
                                        key={warning}
                                        style={{
                                          fontSize: t.font.sizeXs,
                                          color: t.color.warning,
                                          background: t.color.warningSubtle,
                                          border: `1px solid ${t.color.warning}`,
                                          borderRadius: t.radius.sm,
                                          padding: `${t.space.xs}px ${t.space.sm}px`,
                                        }}
                                      >
                                        {warning}
                                      </div>
                                    ))}
                                  </div>
                                ) : null}
                              </div>
                            )
                          }) : (
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                              No step rules configured yet.
                            </div>
                          )}
                        </div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Basic mode keeps step mapping readable and workspace-backed. Use Advanced JSON only for custom predicates, regex rules, or exhaustive manual mapping.
                      </div>
                    </details>

                    <details open style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                        Paths / Flow defaults
                        <FieldBadge kind="recommended">recommended</FieldBadge>
                      </summary>
                      <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                        <button
                          type="button"
                          disabled={!isDraft}
                          onClick={() =>
                            updateDraftWith((next) => {
                              next.paths_explorer_defaults = next.paths_explorer_defaults ?? {}
                              next.flow_defaults = next.flow_defaults ?? {}
                              next.paths_explorer_defaults.top_paths_limit = recommendedTopPathsLimit
                              next.flow_defaults.min_volume_threshold = recommendedMinVolumeThreshold
                              next.flow_defaults.max_nodes = recommendedMaxNodes
                            })
                          }
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.accent}`,
                            background: t.color.accentMuted,
                            color: t.color.accent,
                            fontSize: t.font.sizeXs,
                            cursor: !isDraft ? 'not-allowed' : 'pointer',
                            opacity: !isDraft ? 0.6 : 1,
                          }}
                        >
                          Apply recommended defaults
                        </button>
                        <button
                          type="button"
                          disabled={!isDraft || !activeSettingsJson}
                          onClick={() =>
                            activeSettingsJson &&
                            updateDraftWith((next) => {
                              next.paths_explorer_defaults = deepClone(activeSettingsJson.paths_explorer_defaults ?? next.paths_explorer_defaults ?? {})
                              next.flow_defaults = deepClone(activeSettingsJson.flow_defaults ?? next.flow_defaults ?? {})
                            })
                          }
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.border}`,
                            background: 'transparent',
                            color: t.color.text,
                            fontSize: t.font.sizeXs,
                            cursor: !isDraft || !activeSettingsJson ? 'not-allowed' : 'pointer',
                            opacity: !isDraft || !activeSettingsJson ? 0.6 : 1,
                          }}
                        >
                          Restore active defaults
                        </button>
                      </div>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: t.space.sm }}>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Top paths limit" kind="recommended" />
                          <input type="number" value={draftObj?.paths_explorer_defaults?.top_paths_limit ?? 50} onChange={(e) => updateDraft(setIn(draftObj, ['paths_explorer_defaults', 'top_paths_limit'], asInt(e.target.value, 50)))} disabled={!isDraft} />
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                            Recommended: {contextQuery.data?.recommendations?.paths_explorer_defaults?.top_paths_limit ?? 50}
                          </span>
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Flow max depth" kind="advanced" />
                          <input type="number" value={draftObj?.flow_defaults?.max_depth ?? 4} onChange={(e) => updateDraft(setIn(draftObj, ['flow_defaults', 'max_depth'], asInt(e.target.value, 4)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Min volume threshold" kind="recommended" />
                          <input type="number" value={draftObj?.flow_defaults?.min_volume_threshold ?? 20} onChange={(e) => updateDraft(setIn(draftObj, ['flow_defaults', 'min_volume_threshold'], asInt(e.target.value, 20)))} disabled={!isDraft} />
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                            Recommended: {contextQuery.data?.recommendations?.flow_defaults?.min_volume_threshold ?? 20}
                          </span>
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <LabeledField label="Max nodes" kind="recommended" />
                          <input type="number" value={draftObj?.flow_defaults?.max_nodes ?? 30} onChange={(e) => updateDraft(setIn(draftObj, ['flow_defaults', 'max_nodes'], asInt(e.target.value, 30)))} disabled={!isDraft} />
                          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                            Recommended: {contextQuery.data?.recommendations?.flow_defaults?.max_nodes ?? 30}
                          </span>
                        </label>
                      </div>
                    </details>

                    {showFunnelsDiagnosticsSection ? (
                      <details open style={cardStyle}>
                        <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                          Funnels & Diagnostics
                          {funnelsInactive ? <FieldBadge kind="inactive">funnels inactive</FieldBadge> : <FieldBadge kind="recommended">recommended</FieldBadge>}
                          {diagnosticsInactive ? <FieldBadge kind="inactive">diagnostics inactive</FieldBadge> : <FieldBadge kind="advanced">advanced</FieldBadge>}
                        </summary>
                        {featureFlags.funnel_builder_enabled ? (
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
                            <label style={{ display: 'grid', gap: 4 }}>
                              <LabeledField label="Funnel counting method" kind="recommended" />
                              <select value={draftObj?.funnels_defaults?.default_counting_method ?? 'uniques'} onChange={(e) => updateDraft(setIn(draftObj, ['funnels_defaults', 'default_counting_method'], e.target.value))} disabled={!isDraft}>
                                <option value="uniques">Uniques</option>
                                <option value="totals">Totals</option>
                              </select>
                            </label>
                            <label style={{ display: 'grid', gap: 4 }}>
                              <LabeledField label="Conversion window (seconds)" kind="recommended" />
                              <input type="number" value={draftObj?.funnels_defaults?.default_conversion_window_seconds ?? 604800} onChange={(e) => updateDraft(setIn(draftObj, ['funnels_defaults', 'default_conversion_window_seconds'], asInt(e.target.value, 604800)))} disabled={!isDraft} />
                            </label>
                          </div>
                        ) : null}

                        {featureFlags.funnel_builder_enabled && featureFlags.funnel_diagnostics_enabled ? (
                          <div style={{ height: 1, background: t.color.borderLight }} />
                        ) : null}

                        {featureFlags.funnel_diagnostics_enabled ? (
                          <>
                            <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: t.font.sizeXs }}>
                              <input type="checkbox" checked={!!draftObj?.diagnostics_defaults?.enabled} onChange={(e) => updateDraft(setIn(draftObj, ['diagnostics_defaults', 'enabled'], e.target.checked))} disabled={!isDraft} />
                              <LabeledField label="Enable diagnostics" kind="advanced" />
                            </label>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                              Output policy is fixed to hypotheses with evidence-required claims.
                            </div>
                          </>
                        ) : null}
                      </details>
                    ) : (
                      <div style={{ ...cardStyle, background: t.color.bgSubtle }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Funnels & Diagnostics</div>
                          <FieldBadge kind="inactive">inactive in this workspace</FieldBadge>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          This workspace does not currently use funnel builder or diagnostics features, so their settings stay out of the main editing path.
                        </div>
                      </div>
                    )}

                    <div style={{ ...cardStyle, background: t.color.bgSubtle }}>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>Advanced controls moved out of the main path</div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Performance guardrails, full step-rule JSON, and rare override fields stay in Advanced JSON. They are still available, but they no longer compete with the workspace-backed defaults most users actually need.
                      </div>
                    </div>
                  </div>
                )}

                {validationResult && (
                  <DecisionStatusCard
                    title={validationResult.valid ? 'Draft validation passed' : 'Draft validation issues'}
                    status={validationResult.valid ? 'ready' : 'warning'}
                    blockers={(validationResult.errors ?? []).map((err: any) => `${err.path}: ${err.message}`)}
                    warnings={(validationResult.warnings ?? []).map((warn: any) => `${warn.path}: ${warn.message}`)}
                  />
                )}

                {previewResult?.preview_available && (
                  <div style={cardStyle}>
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                      Impact preview (last 7 days aggregates)
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Changed sections: {(previewResult.changed_keys || []).join(', ') || 'none'}
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Estimated paths returned: {previewResult.estimated_paths_returned ?? 0}
                    </div>
                    {!!previewResult.warnings?.length && (
                      <DecisionStatusCard
                        title="Preview warnings"
                        status="warning"
                        compact
                        warnings={previewResult.warnings}
                      />
                    )}
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
