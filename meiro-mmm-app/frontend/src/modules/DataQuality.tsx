import { useState, useCallback, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { BarChart, Bar, CartesianGrid, LabelList, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { tokens as t } from '../theme/tokens'
import RecommendedActionsList, { type RecommendedActionItem } from '../components/RecommendedActionsList'
import { AnalyticsTable, AnalyticsToolbar, type AnalyticsTableColumn } from '../components/dashboard'
import { navigateForRecommendedAction } from '../lib/recommendedActions'
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'
import ActivationMeasurementShortcuts from '../features/meiro/ActivationMeasurementShortcuts'
import MeiroTargetInstanceBadge from '../features/meiro/MeiroTargetInstanceBadge'
import MeiroMeasurementScopeNotice from '../features/meiro/MeiroMeasurementScopeNotice'
import { getMeiroConfig, type MeiroConfig } from '../connectors/meiroConnector'

// --- Types ---

interface DQSnapshot {
  id: number
  ts_bucket: string
  source: string
  metric_key: string
  metric_value: number
  meta?: Record<string, unknown>
}

interface DQAlert {
  id: number
  rule_id: number
  triggered_at: string
  ts_bucket: string
  metric_value: number
  baseline_value?: number | null
  status: string
  message: string
  note?: string | null
  rule?: {
    name?: string | null
    metric_key?: string | null
    source?: string | null
    severity?: string | null
  } | null
}

interface LastRun {
  last_bucket: string | null
  has_data: boolean
}

interface DQDrilldown {
  metric_key: string
  definition: string
  breakdown: { source: string; value: number }[]
  top_offenders: { key?: string; value?: unknown }[]
  recommended_actions: RecommendedActionItem[]
}

interface RunResult {
  snapshots_created: number
  alerts_created: number
  latest_bucket: string | null
  duration_ms?: number
}

interface MeiroWebhookEvent {
  received_at: string
  received_count: number
  stored_total: number
  replace: boolean
  parser_version?: string | null
  ingest_kind?: 'profiles' | 'events' | string | null
  ip?: string | null
  user_agent?: string | null
  payload_shape?: string | null
  payload_excerpt?: string | null
  payload_truncated?: boolean
  payload_bytes?: number
  payload_json_valid?: boolean
  conversion_event_names?: string[]
  channels_detected?: string[]
}

interface MeiroWebhookSuggestions {
  generated_at: string
  events_analyzed: number
  total_conversions_observed: number
  total_touchpoints_observed: number
  site_scope?: {
    strict?: boolean
    events_excluded?: number
  }
  event_stream_diagnostics?: {
    available: boolean
    batches_examined: number
    events_examined: number
    usable_event_name_share: number
    identity_share: number
    source_medium_share: number
    referrer_only_share: number
    touchpoint_like_events: number
    conversion_like_events: number
    conversion_linkage_share: number
    avg_reconstructed_profiles_per_event: number
    warnings?: string[]
  }
  dedup_key_suggestion: string
  sanitation_suggestions?: Array<{
    id: string
    type: string
    title: string
    description: string
    impact_count?: number
    confidence?: { score?: number; band?: string }
    recommended_action?: string
  }>
  kpi_suggestions: Array<{
    id: string
    label: string
    type: string
    event_name: string
    value_field?: string | null
    coverage_pct?: number
  }>
  taxonomy_suggestions: {
    channel_rules: Array<{ name: string; channel: string; observed_count?: number; match_source?: string; match_medium?: string }>
    source_aliases: Record<string, string>
    medium_aliases: Record<string, string>
    top_sources?: Array<{ source: string; count: number }>
    top_mediums?: Array<{ medium: string; count: number }>
    top_campaigns?: Array<{ campaign: string; count: number }>
    observed_pairs?: Array<{ source: string; medium: string; count: number }>
    unresolved_pairs?: Array<{ source: string; medium: string; count: number }>
  }
  mapping_suggestions: {
    touchpoint_attr_candidates: Array<{ path: string; count: number }>
    value_field_candidates: Array<{ path: string; count: number }>
    currency_field_candidates: Array<{ path: string; count: number }>
    channel_field_candidates: Array<{ path: string; count: number }>
    source_field_candidates: Array<{ path: string; count: number }>
    medium_field_candidates: Array<{ path: string; count: number }>
    campaign_field_candidates: Array<{ path: string; count: number }>
  }
  apply_payloads: {
    kpis: {
      definitions: Array<{
        id: string
        label: string
        type: string
        event_name: string
        value_field?: string | null
        weight: number
        lookback_days?: number | null
      }>
      primary_kpi_id?: string | null
    }
    taxonomy: {
      channel_rules: Array<Record<string, unknown>>
      source_aliases: Record<string, string>
      medium_aliases: Record<string, string>
    }
    mapping: Record<string, unknown>
    sanitation?: Record<string, unknown>
  }
}

interface MeiroMappingState {
  mapping: Record<string, unknown>
  approval: {
    status: string
    note?: string | null
    updated_at?: string | null
  }
  history: Array<Record<string, unknown>>
  version: number
  presets: Record<string, unknown>
}

interface MeiroWebhookArchiveStatus {
  available: boolean
  entries: number
  profiles_received?: number
  last_received_at?: string | null
  parser_versions: string[]
  source_scope?: ArchiveSourceScope
}

interface MeiroEventArchiveStatus {
  available: boolean
  entries: number
  events_received?: number
  last_received_at?: string | null
  parser_versions: string[]
  source_scope?: ArchiveSourceScope
  site_scope?: {
    strict?: boolean
    target_sites?: string[]
    target_site_events?: number
    out_of_scope_site_events?: number
    unknown_site_events?: number
    top_hosts?: Array<{ host: string; count: number }>
  }
}

interface ArchiveSourceScope {
  target_url?: string
  target_host?: string
  verified_entries?: number
  legacy_unverified_entries?: number
  out_of_scope_entries?: number
  status?: string
}

interface MeiroWebhookReprocessResult {
  reprocessed_profiles: number
  archive_entries_used: number
  archive_source?: 'events' | 'profiles' | string
  parser_version: string
  mapping_version: number
  mapping_approval_status: string
  persisted_to_attribution: boolean
  import_result?: { count?: number; message?: string }
}

// --- Threshold config per metric ---


const METRIC_CONFIG: Record<
  string,
  { okThreshold?: number; warnThreshold?: number; criticalThreshold?: number; unit: string; invert?: boolean }
> = {
  freshness_lag_minutes: {
    okThreshold: 60,
    warnThreshold: 180,
    criticalThreshold: 360,
    unit: 'min',
    invert: false,
  },
  missing_profile_pct: {
    okThreshold: 2,
    warnThreshold: 5,
    criticalThreshold: 15,
    unit: '%',
    invert: false,
  },
  missing_timestamp_pct: {
    okThreshold: 2,
    warnThreshold: 5,
    criticalThreshold: 15,
    unit: '%',
    invert: false,
  },
  duplicate_id_pct: {
    okThreshold: 1,
    warnThreshold: 3,
    criticalThreshold: 10,
    unit: '%',
    invert: false,
  },
  conversion_attributable_pct: {
    okThreshold: 80,
    warnThreshold: 60,
    criticalThreshold: 40,
    unit: '%',
    invert: true,
  },
  defaulted_conversion_value_pct: {
    okThreshold: 5,
    warnThreshold: 15,
    criticalThreshold: 30,
    unit: '%',
    invert: false,
  },
  raw_zero_conversion_value_pct: {
    okThreshold: 5,
    warnThreshold: 15,
    criticalThreshold: 30,
    unit: '%',
    invert: false,
  },
  unresolved_source_medium_touchpoint_pct: {
    okThreshold: 5,
    warnThreshold: 15,
    criticalThreshold: 30,
    unit: '%',
    invert: false,
  },
  inferred_mapping_journey_pct: {
    okThreshold: 10,
    warnThreshold: 25,
    criticalThreshold: 40,
    unit: '%',
    invert: false,
  },
}

const SCOPE_SOURCE_MAP: Record<string, string[] | null> = {
  overall: null,
  meiro_web: ['meiro_web', 'journeys', 'taxonomy'],
  meta_cost: ['meta_cost'],
  google_ads_cost: ['google_ads_cost'],
  linkedin_cost: ['linkedin_cost'],
}

function getStatus(
  value: number,
  cfg: { okThreshold?: number; warnThreshold?: number; criticalThreshold?: number; invert?: boolean }
): 'ok' | 'warning' | 'critical' {
  if (!cfg) return 'ok'
  const { okThreshold = 0, warnThreshold = 100, invert = false } = cfg
  const fn = invert
    ? (v: number, o: number, w: number) => (v >= o ? 'ok' : v >= w ? 'warning' : 'critical')
    : (v: number, o: number, w: number) => (v <= o ? 'ok' : v <= w ? 'warning' : 'critical')
  return fn(value, okThreshold, warnThreshold) as 'ok' | 'warning' | 'critical'
}

// --- Main component ---

export default function DataQuality() {
  const queryClient = useQueryClient()
  const tilesRef = useRef<HTMLDivElement>(null)

  const [period] = useState<string>(() => {
    const d = new Date()
    const start = new Date(d)
    start.setDate(start.getDate() - 7)
    return `${start.toISOString().slice(0, 10)} to ${d.toISOString().slice(0, 10)}`
  })
  const [scope, setScope] = useState<string>('overall')
  const [drilldownMetric, setDrilldownMetric] = useState<string | null>(null)
  const [alertStatusFilter, setAlertStatusFilter] = useState<string>('')
  const [alertSeverityFilter, setAlertSeverityFilter] = useState<string>('')
  const [alertSourceFilter, setAlertSourceFilter] = useState<string>('')
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null)
  const [lastRunMeta, setLastRunMeta] = useState<{ bucket: string | null; duration_ms?: number } | null>(null)
  const [expandedWebhookRow, setExpandedWebhookRow] = useState<number | null>(null)
  const [showApplyAllPreview, setShowApplyAllPreview] = useState(false)

  const showToast = useCallback((message: string, type: 'success' | 'error') => {
    setToast({ message, type })
    setTimeout(() => setToast(null), 4000)
  }, [])

  // Run DQ checks
  const runMutation = useMutation({
    mutationFn: async () => {
      return apiSendJson<RunResult>('/api/data-quality/run', 'POST', undefined, {
        fallbackMessage: 'Failed to run data quality job',
      })
    },
    onSuccess: (data) => {
      setLastRunMeta({
        bucket: data.latest_bucket,
        duration_ms: data.duration_ms,
      })
      queryClient.invalidateQueries({ queryKey: ['dq-snapshots'] })
      queryClient.invalidateQueries({ queryKey: ['dq-alerts'] })
      queryClient.invalidateQueries({ queryKey: ['dq-last-run'] })
      showToast(`DQ run complete at ${new Date().toLocaleTimeString()}`, 'success')
    },
    onError: (err: Error) => {
      showToast(err.message || 'DQ run failed', 'error')
    },
  })

  const lastRunQuery = useQuery<LastRun>({
    queryKey: ['dq-last-run'],
    queryFn: async () => apiGetJson<LastRun>('/api/data-quality/last-run', { fallbackMessage: 'Failed to load last run' }),
  })

  const snapshotsQuery = useQuery<DQSnapshot[]>({
    queryKey: ['dq-snapshots'],
    queryFn: async () =>
      apiGetJson<DQSnapshot[]>(withQuery('/api/data-quality/snapshots', { limit: 200 }), {
        fallbackMessage: 'Failed to load data quality snapshots',
      }),
  })

  const alertsQuery = useQuery<DQAlert[]>({
    queryKey: ['dq-alerts', alertStatusFilter, alertSeverityFilter, alertSourceFilter],
    queryFn: async () => {
      const params = new URLSearchParams({ limit: '100' })
      if (alertStatusFilter) params.set('status', alertStatusFilter)
      if (alertSeverityFilter) params.set('severity', alertSeverityFilter)
      if (alertSourceFilter) params.set('source', alertSourceFilter)
      return apiGetJson<DQAlert[]>(`/api/data-quality/alerts?${params}`, {
        fallbackMessage: 'Failed to load data quality alerts',
      })
    },
  })

  const meiroWebhookEventsQuery = useQuery<{ items: MeiroWebhookEvent[]; total: number }>({
    queryKey: ['meiro-webhook-events-dq'],
    queryFn: async () =>
      apiGetJson<{ items: MeiroWebhookEvent[]; total: number }>(
        withQuery('/api/connectors/meiro/webhook/events', { limit: 100, include_payload: 1 }),
        { fallbackMessage: 'Failed to load Meiro webhook debug log' },
      ),
  })

  const meiroConfigQuery = useQuery<MeiroConfig>({
    queryKey: ['meiro-config'],
    queryFn: getMeiroConfig,
  })

  const meiroWebhookSuggestionsQuery = useQuery<MeiroWebhookSuggestions>({
    queryKey: ['meiro-webhook-suggestions'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookSuggestions>(
        withQuery('/api/connectors/meiro/webhook/suggestions', { limit: 100 }),
        { fallbackMessage: 'Failed to build webhook suggestions' },
      ),
  })

  const meiroMappingStateQuery = useQuery<MeiroMappingState>({
    queryKey: ['meiro-mapping-state'],
    queryFn: async () =>
      apiGetJson<MeiroMappingState>('/api/connectors/meiro/mapping', {
        fallbackMessage: 'Failed to load Meiro mapping state',
      }),
  })

  const meiroWebhookArchiveStatusQuery = useQuery<MeiroWebhookArchiveStatus>({
    queryKey: ['meiro-webhook-archive-status'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookArchiveStatus>('/api/connectors/meiro/webhook/archive-status', {
        fallbackMessage: 'Failed to load Meiro webhook archive status',
      }),
  })

  const meiroEventArchiveStatusQuery = useQuery<MeiroEventArchiveStatus>({
    queryKey: ['meiro-event-archive-status-dq'],
    queryFn: async () =>
      apiGetJson<MeiroEventArchiveStatus>('/api/connectors/meiro/events/archive-status', {
        fallbackMessage: 'Failed to load Meiro event archive status',
      }),
  })

  const applyKpiSuggestionsMutation = useMutation({
    mutationFn: async (payload: MeiroWebhookSuggestions['apply_payloads']['kpis']) =>
      apiSendJson('/api/kpis', 'POST', payload, { fallbackMessage: 'Failed to apply KPI suggestions' }),
    onSuccess: () => showToast('KPI suggestions applied.', 'success'),
    onError: (err: Error) => showToast(err.message || 'Failed to apply KPI suggestions', 'error'),
  })

  const applyTaxonomySuggestionsMutation = useMutation({
    mutationFn: async (payload: MeiroWebhookSuggestions['apply_payloads']['taxonomy']) =>
      apiSendJson('/api/taxonomy', 'POST', payload, { fallbackMessage: 'Failed to apply taxonomy suggestions' }),
    onSuccess: () => showToast('Taxonomy suggestions applied.', 'success'),
    onError: (err: Error) => showToast(err.message || 'Failed to apply taxonomy suggestions', 'error'),
  })

  const applyMappingSuggestionsMutation = useMutation({
    mutationFn: async (payload: MeiroWebhookSuggestions['apply_payloads']['mapping']) =>
      apiSendJson('/api/connectors/meiro/mapping', 'POST', payload, { fallbackMessage: 'Failed to apply Meiro mapping suggestions' }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['meiro-mapping-state'] })
      showToast('Meiro mapping suggestions applied.', 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to apply Meiro mapping suggestions', 'error'),
  })

  const applySanitationSuggestionsMutation = useMutation({
    mutationFn: async (payload: Record<string, unknown> | undefined) =>
      apiSendJson('/api/connectors/meiro/pull-config', 'POST', payload || {}, { fallbackMessage: 'Failed to apply sanitation suggestions' }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['meiro-webhook-suggestions'] })
      showToast('Webhook sanitation suggestions applied.', 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to apply sanitation suggestions', 'error'),
  })

  const updateMappingApprovalMutation = useMutation({
    mutationFn: async (payload: { status: string; note?: string }) =>
      apiSendJson('/api/connectors/meiro/mapping/approval', 'POST', payload, {
        fallbackMessage: 'Failed to update Meiro mapping approval',
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['meiro-mapping-state'] })
      showToast('Meiro mapping approval updated.', 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to update Meiro mapping approval', 'error'),
  })

  const reprocessWebhookArchiveMutation = useMutation({
    mutationFn: async () =>
      apiSendJson<MeiroWebhookReprocessResult>('/api/connectors/meiro/webhook/reprocess', 'POST', {
        archive_source: meiroEventArchiveStatusQuery.data?.available ? 'events' : 'profiles',
        persist_to_attribution: true,
        import_note: meiroEventArchiveStatusQuery.data?.available
          ? 'Reprocessed from raw-event archive using current approved mapping'
          : 'Reprocessed from profile archive using current approved mapping',
      }, {
        fallbackMessage: 'Failed to reprocess Meiro webhook archive',
      }),
    onSuccess: async (data) => {
      await queryClient.invalidateQueries({ queryKey: ['journeys-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journeys-validation-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-webhook-events-dq'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-webhook-archive-status'] })
      await queryClient.invalidateQueries({ queryKey: ['meiro-event-archive-status-dq'] })
      showToast(
        `Reprocessed ${data.reprocessed_profiles} profiles from the ${data.archive_source === 'events' ? 'raw-event' : 'profile'} archive with mapping v${data.mapping_version}.`,
        'success',
      )
    },
    onError: (err: Error) => showToast(err.message || 'Failed to reprocess Meiro webhook archive', 'error'),
  })

  const applyAllSuggestionsMutation = useMutation({
    mutationFn: async (payloads: MeiroWebhookSuggestions['apply_payloads']) => {
      await apiSendJson('/api/kpis', 'POST', payloads.kpis, { fallbackMessage: 'Failed to apply KPI suggestions' })
      await apiSendJson('/api/taxonomy', 'POST', payloads.taxonomy, { fallbackMessage: 'Failed to apply taxonomy suggestions' })
      await apiSendJson('/api/connectors/meiro/mapping', 'POST', payloads.mapping, { fallbackMessage: 'Failed to apply Meiro mapping suggestions' })
    },
    onSuccess: () => {
      setShowApplyAllPreview(false)
      queryClient.invalidateQueries({ queryKey: ['meiro-mapping-state'] })
      showToast('Applied KPI, taxonomy, and mapping suggestions.', 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to apply all suggestions', 'error'),
  })

  const drilldownQuery = useQuery<DQDrilldown>({
    queryKey: ['dq-drilldown', drilldownMetric],
    queryFn: async () => {
      if (!drilldownMetric) throw new Error('No metric')
      return apiGetJson<DQDrilldown>(withQuery('/api/data-quality/drilldown', { metric_key: drilldownMetric }), {
        fallbackMessage: 'Failed to load drilldown',
      })
    },
    enabled: !!drilldownMetric,
  })

  const updateAlertStatus = useMutation({
    mutationFn: async (payload: { id: number; status: string }) => {
      return apiSendJson<any>(`/api/data-quality/alerts/${payload.id}/status`, 'POST', { status: payload.status }, {
        fallbackMessage: 'Failed to update alert status',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dq-alerts'] })
    },
  })

  const updateAlertNote = useMutation({
    mutationFn: async (payload: { id: number; note: string }) => {
      return apiSendJson<any>(`/api/data-quality/alerts/${payload.id}/note`, 'POST', { note: payload.note }, {
        fallbackMessage: 'Failed to update alert note',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dq-alerts'] })
    },
  })

  const lastBucket = lastRunMeta?.bucket ?? lastRunQuery.data?.last_bucket ?? null
  const lastRunDuration = lastRunMeta?.duration_ms

  // Derive tile data from snapshots (filter by scope)
  const snapshots = snapshotsQuery.data ?? []
  const activeSources =
    Object.prototype.hasOwnProperty.call(SCOPE_SOURCE_MAP, scope)
      ? SCOPE_SOURCE_MAP[scope]
      : [scope]
  const latest = snapshots.filter((s) => {
    if (activeSources == null) return true
    return activeSources.includes(s.source)
  })

  const freshness = latest.filter((s) => s.metric_key === 'freshness_lag_minutes')
  const completenessMissingProfile = latest.find((s) => s.metric_key === 'missing_profile_pct')
  const completenessMissingTs = latest.find((s) => s.metric_key === 'missing_timestamp_pct')
  const duplication = latest.find((s) => s.metric_key === 'duplicate_id_pct')
  const joinRate = latest.find((s) => s.metric_key === 'conversion_attributable_pct')
  const defaultedConversionValue = latest.find((s) => s.metric_key === 'defaulted_conversion_value_pct')
  const rawZeroConversionValue = latest.find((s) => s.metric_key === 'raw_zero_conversion_value_pct')
  const unresolvedSourceMedium = latest.find((s) => s.metric_key === 'unresolved_source_medium_touchpoint_pct')
  const inferredMappingJourneys = latest.find((s) => s.metric_key === 'inferred_mapping_journey_pct')

  // Trend: compare latest vs previous run (by ts_bucket)
  const buckets = [...new Set(snapshots.map((s) => s.ts_bucket))].sort().reverse()
  const latestBucket = buckets[0]
  const prevBucket = buckets[1]
  const getTrend = (metricKey: string, source?: string) => {
    if (!latestBucket || !prevBucket) return null
    const trendSources = source ? [source] : activeSources
    const latestSnap = snapshots.find(
      (s) => s.metric_key === metricKey && s.ts_bucket === latestBucket && (trendSources == null || trendSources.includes(s.source))
    )
    const prevSnap = snapshots.find(
      (s) => s.metric_key === metricKey && s.ts_bucket === prevBucket && (trendSources == null || trendSources.includes(s.source))
    )
    if (!latestSnap || !prevSnap) return null
    const delta = latestSnap.metric_value - prevSnap.metric_value
    return { delta, improved: metricKey === 'conversion_attributable_pct' ? delta > 0 : delta < 0 }
  }

  const exportAlerts = () => {
    const alerts = alertsQuery.data ?? []
    const headers = ['id', 'triggered_at', 'rule_name', 'source', 'metric_key', 'severity', 'status', 'metric_value', 'baseline_value', 'message', 'note']
    const rows = alerts.map((a) =>
      [
        a.id,
        a.triggered_at,
        a.rule?.name ?? '',
        a.rule?.source ?? '',
        a.rule?.metric_key ?? '',
        a.rule?.severity ?? '',
        a.status,
        a.metric_value,
        a.baseline_value ?? '',
        `"${(a.message ?? '').replace(/"/g, '""')}"`,
        `"${(a.note ?? '').replace(/"/g, '""')}"`,
      ].join(',')
    )
    const csv = [headers.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `dq-alerts-${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  // DQ Score: derive from metrics
  const scoreInputs = {
    missing_profile: completenessMissingProfile?.metric_value ?? 0,
    missing_ts: completenessMissingTs?.metric_value ?? 0,
    dup: duplication?.metric_value ?? 0,
    freshness_hours: freshness.length ? Math.max(...freshness.map((f) => f.metric_value)) / 60 : 0,
    attributable: joinRate?.metric_value ?? 0,
    defaulted_value: defaultedConversionValue?.metric_value ?? 0,
    unresolved_pairs: unresolvedSourceMedium?.metric_value ?? 0,
    inferred_journeys: inferredMappingJourneys?.metric_value ?? 0,
  }
  let dqScore = 100
  dqScore -= scoreInputs.missing_profile * 1.5
  dqScore -= scoreInputs.missing_ts * 1.5
  dqScore -= scoreInputs.dup * 0.5
  dqScore -= Math.min(20, scoreInputs.freshness_hours * 2)
  dqScore -= Math.max(0, 100 - scoreInputs.attributable) * 0.3
  dqScore -= scoreInputs.defaulted_value * 0.4
  dqScore -= scoreInputs.unresolved_pairs * 0.4
  dqScore -= scoreInputs.inferred_journeys * 0.3
  dqScore = Math.max(0, Math.min(100, Math.round(dqScore)))
  const dqLabel = dqScore >= 80 ? 'High' : dqScore >= 50 ? 'Medium' : 'Low'
  const topDrivers: string[] = []
  if (scoreInputs.missing_profile > 5) topDrivers.push(`${scoreInputs.missing_profile.toFixed(1)}% missing profile_id`)
  if (scoreInputs.missing_ts > 5) topDrivers.push(`${scoreInputs.missing_ts.toFixed(1)}% missing timestamps`)
  if (scoreInputs.dup > 3) topDrivers.push(`${scoreInputs.dup.toFixed(1)}% duplicate IDs`)
  if (scoreInputs.freshness_hours > 6) topDrivers.push(`${scoreInputs.freshness_hours.toFixed(1)}h freshness lag`)
  if (scoreInputs.attributable < 70) topDrivers.push(`${scoreInputs.attributable.toFixed(1)}% attributable conversions`)
  if (scoreInputs.defaulted_value > 10) topDrivers.push(`${scoreInputs.defaulted_value.toFixed(1)}% defaulted conversion values`)
  if (scoreInputs.unresolved_pairs > 10) topDrivers.push(`${scoreInputs.unresolved_pairs.toFixed(1)}% unresolved source/medium touchpoints`)
  if (scoreInputs.inferred_journeys > 15) topDrivers.push(`${scoreInputs.inferred_journeys.toFixed(1)}% journeys using inferred mappings`)
  const rawEventDiagnostics = meiroWebhookSuggestionsQuery.data?.event_stream_diagnostics
  const rawEventFunnelData = rawEventDiagnostics?.available
    ? [
        { label: 'Events', value: rawEventDiagnostics.events_examined, annotation: 'Examined' },
        {
          label: 'Named',
          value: Math.round(rawEventDiagnostics.events_examined * rawEventDiagnostics.usable_event_name_share),
          annotation: `${(rawEventDiagnostics.usable_event_name_share * 100).toFixed(0)}%`,
        },
        {
          label: 'Identified',
          value: Math.round(rawEventDiagnostics.events_examined * rawEventDiagnostics.identity_share),
          annotation: `${(rawEventDiagnostics.identity_share * 100).toFixed(0)}%`,
        },
        {
          label: 'Source/medium',
          value: Math.round(rawEventDiagnostics.events_examined * rawEventDiagnostics.source_medium_share),
          annotation: `${(rawEventDiagnostics.source_medium_share * 100).toFixed(0)}%`,
        },
        {
          label: 'Linked',
          value: Math.round(rawEventDiagnostics.events_examined * rawEventDiagnostics.conversion_linkage_share),
          annotation: `${(rawEventDiagnostics.conversion_linkage_share * 100).toFixed(0)}%`,
        },
      ]
    : []
  const rawEventQualityStatus = rawEventDiagnostics?.available
    ? rawEventDiagnostics.usable_event_name_share >= 0.9 &&
      rawEventDiagnostics.identity_share >= 0.8 &&
      rawEventDiagnostics.source_medium_share >= 0.7
      ? 'Ready'
      : rawEventDiagnostics.usable_event_name_share >= 0.7 &&
        rawEventDiagnostics.identity_share >= 0.5 &&
        rawEventDiagnostics.source_medium_share >= 0.4
        ? 'Needs mapping'
        : 'Needs attention'
    : 'No raw events'
  const rawEventQualityColor =
    rawEventQualityStatus === 'Ready'
      ? t.color.success
      : rawEventQualityStatus === 'Needs mapping'
        ? t.color.warning
        : t.color.danger
  const rawEventQualityBackground =
    rawEventQualityStatus === 'Ready'
      ? t.color.successMuted
      : rawEventQualityStatus === 'Needs mapping'
        ? t.color.warningMuted
        : t.color.dangerMuted
  const rawEventReplayHint = rawEventDiagnostics?.available
    ? rawEventDiagnostics.avg_reconstructed_profiles_per_event > 0
      ? 'Raw batches are replayed into MMM profiles; reconstruction is expected when identity, timestamps, and event names are present.'
      : 'Raw batches are available, but replay has not reconstructed measurable profile journeys yet.'
    : 'No usable raw-event diagnostics are available from the Meiro webhook archive.'
  const taxonomySuggestions = meiroWebhookSuggestionsQuery.data?.taxonomy_suggestions
  const topSourceRows = (taxonomySuggestions?.top_sources || []).slice(0, 8)
  const topMediumRows = (taxonomySuggestions?.top_mediums || []).slice(0, 8)
  const unresolvedPairRows = (taxonomySuggestions?.unresolved_pairs || []).slice(0, 16)
  const observedSources = (taxonomySuggestions?.top_sources || []).slice(0, 6).map((item) => item.source)
  const observedMediums = (taxonomySuggestions?.top_mediums || []).slice(0, 6).map((item) => item.medium)
  const observedPairLookup = new Map(
    (taxonomySuggestions?.observed_pairs || []).map((item) => [`${item.source}__${item.medium}`, item.count]),
  )
  const maxObservedPairCount = Math.max(
    1,
    ...(taxonomySuggestions?.observed_pairs || []).map((item) => Number(item.count || 0)),
  )
  const unresolvedPairColumns: AnalyticsTableColumn<{ source: string; medium: string; count: number }>[] = [
    {
      key: 'source',
      label: 'Source',
      render: (row) => row.source || '∅',
      cellStyle: { fontWeight: t.font.weightMedium },
    },
    {
      key: 'medium',
      label: 'Medium',
      render: (row) => row.medium || '∅',
      cellStyle: { color: t.color.textSecondary },
    },
    {
      key: 'count',
      label: 'Count',
      align: 'right',
      render: (row) => Number(row.count || 0).toLocaleString(),
      cellStyle: { fontWeight: t.font.weightMedium },
    },
  ]
  const webhookEventColumns: AnalyticsTableColumn<MeiroWebhookEvent>[] = [
    {
      key: 'received_at',
      label: 'Received',
      hideable: false,
      render: (event) => (event.received_at ? new Date(event.received_at).toLocaleString() : '—'),
    },
    {
      key: 'ingest_kind',
      label: 'Kind',
      render: (event) => {
        const ingestKind = String(event.ingest_kind || 'profiles')
        return (
          <span
            style={{
              padding: '2px 6px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              background: ingestKind === 'events' ? t.color.warningMuted : t.color.successMuted,
              color: ingestKind === 'events' ? t.color.warning : t.color.success,
            }}
          >
            {ingestKind === 'events' ? 'events' : 'profiles'}
          </span>
        )
      },
    },
    {
      key: 'received_count',
      label: 'Records',
      align: 'right',
      render: (event) => (event.received_count ?? 0).toLocaleString(),
    },
    {
      key: 'replace',
      label: 'Mode',
      render: (event) => (event.replace ? 'replace' : 'append'),
    },
    {
      key: 'payload_bytes',
      label: 'Payload size',
      align: 'right',
      render: (event) => (event.payload_bytes != null ? `${event.payload_bytes.toLocaleString()} B` : '—'),
    },
    {
      key: 'conversion_event_names',
      label: 'Detected events',
      render: (event) => (event.conversion_event_names?.length ? event.conversion_event_names.join(', ') : '—'),
    },
    {
      key: 'detected_summary',
      label: 'Detected channels / profiles',
      render: (event) => {
        const ingestKind = String(event.ingest_kind || 'profiles')
        return ingestKind === 'events'
          ? event.channels_detected?.length
            ? event.channels_detected.join(', ')
            : 'Reconstructed on replay/import'
          : event.channels_detected?.length
          ? event.channels_detected.join(', ')
          : '—'
      },
    },
    {
      key: 'source',
      label: 'Source',
      render: (event) => (
        <div>
          <div>{event.ip || '—'}</div>
          {event.user_agent ? (
            <div
              style={{
                fontSize: t.font.sizeXs,
                color: t.color.textMuted,
                maxWidth: 300,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {event.user_agent}
            </div>
          ) : null}
        </div>
      ),
    },
    {
      key: 'payload',
      label: 'Payload',
      align: 'center',
      render: (event, idx) => {
        const isExpanded = expandedWebhookRow === idx
        return (
          <div style={{ display: 'grid', gap: 8, justifyItems: 'center' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6, flexWrap: 'wrap' }}>
              <span
                style={{
                  padding: '2px 6px',
                  borderRadius: 999,
                  fontSize: t.font.sizeXs,
                  fontWeight: t.font.weightMedium,
                  background: event.payload_json_valid === false ? t.color.dangerMuted : t.color.successMuted,
                  color: event.payload_json_valid === false ? t.color.danger : t.color.success,
                }}
              >
                {event.payload_json_valid === false ? 'JSON invalid' : 'JSON valid'}
              </span>
              {event.payload_truncated ? (
                <span style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>Truncated</span>
              ) : null}
              <button
                type="button"
                onClick={(rowEvent) => {
                  rowEvent.stopPropagation()
                  setExpandedWebhookRow(isExpanded ? null : idx)
                }}
                style={{
                  padding: '2px 8px',
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                {isExpanded ? 'Hide' : 'View'}
              </button>
              <button
                type="button"
                onClick={async (rowEvent) => {
                  rowEvent.stopPropagation()
                  try {
                    await navigator.clipboard.writeText(event.payload_excerpt || '')
                    showToast('Payload copied to clipboard.', 'success')
                  } catch {
                    showToast('Failed to copy payload.', 'error')
                  }
                }}
                style={{
                  padding: '2px 8px',
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Copy
              </button>
            </div>
            {isExpanded ? (
              <pre
                style={{
                  margin: 0,
                  width: 'min(560px, 100%)',
                  fontSize: t.font.sizeXs,
                  background: t.color.bgSubtle,
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word',
                  maxHeight: 260,
                  overflow: 'auto',
                  textAlign: 'left',
                }}
              >
                {event.payload_excerpt || 'No payload captured'}
                {event.payload_truncated ? '\n\n[Payload truncated in log]' : ''}
              </pre>
            ) : null}
          </div>
        )
      },
    },
  ]
  const alertColumns: AnalyticsTableColumn<DQAlert>[] = [
    {
      key: 'triggered_at',
      label: 'First seen',
      hideable: false,
      render: (alert) => new Date(alert.triggered_at).toLocaleString(),
    },
    {
      key: 'rule',
      label: 'Rule',
      hideable: false,
      render: (alert) => alert.rule?.name ?? alert.message,
    },
    {
      key: 'source',
      label: 'Source',
      render: (alert) => alert.rule?.source ?? '—',
    },
    {
      key: 'metric_key',
      label: 'Metric',
      render: (alert) => alert.rule?.metric_key ?? '—',
    },
    {
      key: 'metric_value',
      label: 'Value',
      align: 'right',
      render: (alert) => alert.metric_value.toFixed(2),
    },
    {
      key: 'baseline_value',
      label: 'Threshold',
      align: 'right',
      render: (alert) => (alert.baseline_value != null ? alert.baseline_value.toFixed(2) : '—'),
    },
    {
      key: 'severity',
      label: 'Severity',
      align: 'center',
      render: (alert) => {
        const sev = (alert.rule?.severity ?? 'warn') as string
        const sevColor =
          sev === 'critical' ? t.color.danger : sev === 'info' ? t.color.textSecondary : t.color.warning
        return <span style={{ color: sevColor }}>{sev}</span>
      },
    },
    {
      key: 'status',
      label: 'Status',
      align: 'center',
      render: (alert) => (alert.status === 'acked' ? 'Acknowledged' : alert.status === 'resolved' ? 'Resolved' : 'Open'),
    },
    {
      key: 'actions',
      label: 'Actions',
      align: 'center',
      render: (alert) => (
        <AlertActions
          alert={alert}
          onStatus={(status) => updateAlertStatus.mutate({ id: alert.id, status })}
          onNote={(note) => updateAlertNote.mutate({ id: alert.id, note })}
        />
      ),
    },
  ]

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      {/* Toast */}
      {toast && (
        <div
          style={{
            position: 'fixed',
            top: 16,
            right: 16,
            padding: `${t.space.sm}px ${t.space.lg}px`,
            borderRadius: t.radius.sm,
            background: toast.type === 'success' ? t.color.successMuted : t.color.dangerMuted,
            color: toast.type === 'success' ? t.color.success : t.color.danger,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightMedium,
            zIndex: 9999,
            boxShadow: t.shadow,
          }}
        >
          {toast.message}
        </div>
      )}

      {/* Header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.md,
          gap: t.space.md,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <h1
            style={{
              margin: 0,
              fontSize: t.font.size2xl,
              fontWeight: t.font.weightBold,
              color: t.color.text,
              letterSpacing: '-0.02em',
            }}
          >
            Data quality
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Monitor freshness, completeness, and join rates. Alerts highlight issues that may impact attribution accuracy.
          </p>
        </div>
        <button
          type="button"
          onClick={() => runMutation.mutate()}
          disabled={runMutation.isPending}
          style={{
            padding: `${t.space.sm}px ${t.space.lg}px`,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightMedium,
            color: t.color.surface,
            backgroundColor: t.color.accent,
            border: 'none',
            borderRadius: t.radius.sm,
            cursor: runMutation.isPending ? 'wait' : 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: t.space.sm,
          }}
        >
          {runMutation.isPending && (
            <span
              style={{
                width: 14,
                height: 14,
                border: `2px solid ${t.color.surface}`,
                borderTopColor: 'transparent',
                borderRadius: '50%',
                animation: 'spin 0.8s linear infinite',
              }}
            />
          )}
          {runMutation.isPending ? 'Running checks…' : 'Run data quality checks'}
        </button>
      </div>

      <div style={{ marginBottom: t.space.md }}>
        <MeiroMeasurementScopeNotice compact />
      </div>

      {/* Measurement context bar */}
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: t.space.lg,
          alignItems: 'center',
          padding: `${t.space.sm}px ${t.space.md}px`,
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.sm,
          marginBottom: t.space.xl,
          fontSize: t.font.sizeSm,
          color: t.color.textSecondary,
        }}
      >
        <span title="Evaluation period">
          <strong style={{ color: t.color.text }}>Period:</strong> {period}
        </span>
        <span title="Conversion type">
          <strong style={{ color: t.color.text }}>Conversion:</strong> purchase (primary)
        </span>
        <span>
          <strong style={{ color: t.color.text }}>Scope:</strong>{' '}
          <select
            value={scope}
            onChange={(e) => setScope(e.target.value)}
            style={{
              marginLeft: t.space.xs,
              padding: '2px 6px',
              fontSize: t.font.sizeSm,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              background: t.color.surface,
              color: t.color.text,
            }}
          >
            <option value="overall">Overall</option>
            <option value="meiro_web">Meiro</option>
            <option value="google_ads_cost">Google</option>
            <option value="meta_cost">Meta</option>
            <option value="linkedin_cost">LinkedIn</option>
            <option value="journeys">Journeys</option>
          </select>
        </span>
        <span>
          <strong style={{ color: t.color.text }}>Last run:</strong>{' '}
          {lastBucket ? new Date(lastBucket).toLocaleString() : '—'}
          {lastRunDuration != null && ` (${(lastRunDuration / 1000).toFixed(1)}s)`}
        </span>
      </div>

      {/* DQ Score card */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.lg,
          marginBottom: t.space.xl,
          boxShadow: t.shadowSm,
          display: 'flex',
          alignItems: 'center',
          gap: t.space.xl,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md }}>
          <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Data Confidence{' '}
            <span
              title="How is this computed? Combined score (0–100) from: missing profile_id, missing timestamps, duplicate IDs, freshness lag, attributable conversions, defaulted conversion values, and unresolved source/medium touchpoints. Penalties reduce the score; higher attributable % increases it."
              style={{ cursor: 'help', color: t.color.textMuted }}
            >
              ?
            </span>
          </span>
          <span
            title="Combined score from freshness, completeness, duplication, attributable conversions, defaulted revenue share, and unresolved taxonomy pairs. Higher is better."
            style={{
              fontSize: t.font.size2xl,
              fontWeight: t.font.weightBold,
              color:
                dqLabel === 'High'
                  ? t.color.success
                  : dqLabel === 'Medium'
                    ? t.color.warning
                    : t.color.danger,
            }}
          >
            {dqScore}
          </span>
          <span
            style={{
              padding: '2px 8px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              background:
                dqLabel === 'High'
                  ? t.color.successMuted
                  : dqLabel === 'Medium'
                    ? t.color.warningMuted
                    : t.color.dangerMuted,
              color:
                dqLabel === 'High'
                  ? t.color.success
                  : dqLabel === 'Medium'
                    ? t.color.warning
                    : t.color.danger,
            }}
          >
            {dqLabel}
          </span>
        </div>
        {topDrivers.length > 0 && (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Top drivers lowering score: {topDrivers.slice(0, 2).join('; ')}
          </div>
        )}
      </div>

      {/* Raw event readiness summary */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.lg,
          marginBottom: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'flex-start',
            justifyContent: 'space-between',
            gap: t.space.md,
            flexWrap: 'wrap',
            marginBottom: t.space.md,
          }}
        >
          <div>
            <h2 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Pipes raw-event input</h2>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Top-level readiness for events replayed from Meiro Pipes into MMM attribution and taxonomy logic.
            </p>
          </div>
          <div style={{ display: 'grid', gap: t.space.xs, justifyItems: 'end' }}>
            <span
              style={{
                padding: '4px 10px',
                borderRadius: 999,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                background: rawEventQualityBackground,
                color: rawEventQualityColor,
              }}
            >
              {rawEventQualityStatus}
            </span>
            <MeiroTargetInstanceBadge config={meiroConfigQuery.data} compact showWarning={false} />
          </div>
        </div>
        {Number(meiroEventArchiveStatusQuery.data?.source_scope?.legacy_unverified_entries || 0) > 0 ? (
          <div
            style={{
              marginBottom: t.space.md,
              border: `1px solid ${t.color.warning}`,
              borderRadius: t.radius.md,
              background: t.color.warningMuted,
              padding: t.space.md,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            <strong style={{ color: t.color.text }}>Legacy source note:</strong>{' '}
            {Number(meiroEventArchiveStatusQuery.data?.source_scope?.legacy_unverified_entries || 0).toLocaleString()} archived raw-event batches predate instance tagging.
            New batches are tagged against the target Pipes instance.
          </div>
        ) : null}
        {Number(meiroEventArchiveStatusQuery.data?.site_scope?.out_of_scope_site_events || 0) > 0 ? (
          <div
            style={{
              marginBottom: t.space.md,
              border: `1px solid ${t.color.warning}`,
              borderRadius: t.radius.md,
              background: t.color.warningMuted,
              padding: t.space.md,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            <strong style={{ color: t.color.text }}>Site scope guard:</strong>{' '}
            {Number(meiroEventArchiveStatusQuery.data?.site_scope?.out_of_scope_site_events || 0).toLocaleString()} archived raw events are outside{' '}
            {(meiroEventArchiveStatusQuery.data?.site_scope?.target_sites || ['meiro.io', 'meir.store']).join(', ')} and are excluded from strict replay/reporting.
            {meiroEventArchiveStatusQuery.data?.site_scope?.top_hosts?.length ? (
              <> Top hosts: {meiroEventArchiveStatusQuery.data.site_scope.top_hosts.slice(0, 4).map((item) => `${item.host} (${Number(item.count || 0).toLocaleString()})`).join(' · ')}.</>
            ) : null}
          </div>
        ) : null}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
            gap: t.space.md,
            marginBottom: t.space.md,
          }}
        >
          {[
            { label: 'Events examined', value: rawEventDiagnostics?.available ? rawEventDiagnostics.events_examined.toLocaleString() : '—' },
            { label: 'Usable names', value: rawEventDiagnostics?.available ? `${(rawEventDiagnostics.usable_event_name_share * 100).toFixed(1)}%` : '—' },
            { label: 'Identity coverage', value: rawEventDiagnostics?.available ? `${(rawEventDiagnostics.identity_share * 100).toFixed(1)}%` : '—' },
            { label: 'Source / medium', value: rawEventDiagnostics?.available ? `${(rawEventDiagnostics.source_medium_share * 100).toFixed(1)}%` : '—' },
            { label: 'Conversion linkage', value: rawEventDiagnostics?.available ? `${(rawEventDiagnostics.conversion_linkage_share * 100).toFixed(1)}%` : '—' },
          ].map((item) => (
            <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', fontWeight: t.font.weightMedium }}>
                {item.label}
              </div>
              <div style={{ marginTop: 4, fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightBold }}>
                {item.value}
              </div>
            </div>
          ))}
        </div>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          {rawEventReplayHint}
          {rawEventDiagnostics?.warnings?.length ? ` ${rawEventDiagnostics.warnings.slice(0, 2).join(' ')}` : ''}
        </p>
      </div>

      {/* KPI tiles */}
      <div
        ref={tilesRef}
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        <Tile
          label="Max freshness lag (hours)"
          metricKey="freshness_lag_minutes"
          value={
            freshness.length
              ? (Math.max(...freshness.map((f) => f.metric_value)) / 60).toFixed(1)
              : null
          }
          naReason={
            !freshness.length
              ? 'No freshness data: no sources with timestamps, or no DQ run yet.'
              : undefined
          }
          description="Across Meiro and cost sources"
          trend={getTrend('freshness_lag_minutes')}
          onClick={() => setDrilldownMetric('freshness_lag_minutes')}
        />
        <Tile
          label="Journeys missing profile ID"
          metricKey="missing_profile_pct"
          value={completenessMissingProfile ? completenessMissingProfile.metric_value.toFixed(1) : null}
          description="Higher values mean poor joinability"
          trend={getTrend('missing_profile_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('missing_profile_pct')}
        />
        <Tile
          label="Journeys missing timestamps"
          metricKey="missing_timestamp_pct"
          value={completenessMissingTs ? completenessMissingTs.metric_value.toFixed(1) : null}
          description="Without timestamps, windowing & paths break"
          trend={getTrend('missing_timestamp_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('missing_timestamp_pct')}
        />
        <Tile
          label="Duplicate IDs"
          metricKey="duplicate_id_pct"
          value={duplication ? duplication.metric_value.toFixed(1) : null}
          description="Potential double counting"
          trend={getTrend('duplicate_id_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('duplicate_id_pct')}
        />
        <Tile
          label="Attributable conversions"
          metricKey="conversion_attributable_pct"
          value={joinRate ? joinRate.metric_value.toFixed(1) : null}
          description="Conversions with at least one eligible touchpoint"
          trend={getTrend('conversion_attributable_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('conversion_attributable_pct')}
        />
        <Tile
          label="Defaulted conversion values"
          metricKey="defaulted_conversion_value_pct"
          value={defaultedConversionValue ? defaultedConversionValue.metric_value.toFixed(1) : null}
          description="Conversions using fallback value rules"
          trend={getTrend('defaulted_conversion_value_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('defaulted_conversion_value_pct')}
        />
        <Tile
          label="Raw zero-value conversions"
          metricKey="raw_zero_conversion_value_pct"
          value={rawZeroConversionValue ? rawZeroConversionValue.metric_value.toFixed(1) : null}
          description="Conversions arriving with raw value = 0"
          trend={getTrend('raw_zero_conversion_value_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('raw_zero_conversion_value_pct')}
        />
        <Tile
          label="Unresolved source / medium"
          metricKey="unresolved_source_medium_touchpoint_pct"
          value={unresolvedSourceMedium ? unresolvedSourceMedium.metric_value.toFixed(1) : null}
          description="Touchpoints not matched by taxonomy rules"
          trend={getTrend('unresolved_source_medium_touchpoint_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('unresolved_source_medium_touchpoint_pct')}
        />
        <Tile
          label="Journeys using inferred mappings"
          metricKey="inferred_mapping_journey_pct"
          value={inferredMappingJourneys ? inferredMappingJourneys.metric_value.toFixed(1) : null}
          description="Journeys normalized via parser fallback logic"
          trend={getTrend('inferred_mapping_journey_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('inferred_mapping_journey_pct')}
        />
      </div>

      {/* Meiro webhook debug log */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: t.space.md,
            marginBottom: t.space.md,
          }}
        >
          <div>
            <h2 style={{ margin: '0 0 4px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Meiro Pipes diagnostics
            </h2>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Advanced payload debugging for Meiro Pipes. Day-to-day source setup now lives in the Meiro Measurement Pipeline.
            </p>
          </div>
          <button
            type="button"
            onClick={() => meiroWebhookEventsQuery.refetch()}
            style={{
              padding: `${t.space.xs}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Refresh
          </button>
        </div>

        {meiroWebhookEventsQuery.isLoading ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading webhook events…</p>
        ) : meiroWebhookEventsQuery.isError ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
            {(meiroWebhookEventsQuery.error as Error)?.message || 'Failed to load webhook events'}
          </p>
        ) : (meiroWebhookEventsQuery.data?.items?.length || 0) > 0 ? (
          <AnalyticsTable
            columns={webhookEventColumns}
            rows={meiroWebhookEventsQuery.data?.items || []}
            rowKey={(event, idx) => `evt-${idx}-${event.received_at || 'na'}`}
            tableLabel="Meiro pipes diagnostics"
            stickyFirstColumn
            minWidth={1200}
            virtualized
            virtualizationThreshold={20}
            virtualizationHeight={440}
            virtualRowHeight={54}
            allowColumnHiding
            allowDensityToggle
            persistKey="data-quality-webhook-events"
            defaultHiddenColumnKeys={['source', 'payload_bytes']}
            presets={[
              {
                key: 'overview',
                label: 'Overview',
                visibleColumnKeys: ['received_at', 'ingest_kind', 'received_count', 'replace', 'conversion_event_names', 'detected_summary'],
              },
              {
                key: 'payloads',
                label: 'Payloads',
                visibleColumnKeys: ['received_at', 'ingest_kind', 'payload_bytes', 'source', 'payload'],
              },
            ]}
            defaultPresetKey="overview"
          />
        ) : (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            No Meiro webhook traffic captured yet. Send data to either `/api/connectors/meiro/profiles` or `/api/connectors/meiro/events`.
          </p>
        )}
      </div>

      {/* Webhook-based setup suggestions */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ margin: '0 0 4px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Advanced setup suggestions from Meiro payloads
            </h2>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Suggested KPI/conversion and taxonomy defaults from recent Meiro Pipes payloads. Use the Meiro Measurement Pipeline for the primary integration workflow.
            </p>
          </div>
          <button
            type="button"
            onClick={() => meiroWebhookSuggestionsQuery.refetch()}
            style={{
              padding: `${t.space.xs}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Refresh suggestions
          </button>
        </div>

        {meiroWebhookSuggestionsQuery.isLoading ? (
          <p style={{ marginTop: t.space.md, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Analyzing webhook payloads…</p>
        ) : meiroWebhookSuggestionsQuery.isError ? (
          <p style={{ marginTop: t.space.md, fontSize: t.font.sizeSm, color: t.color.danger }}>
            {(meiroWebhookSuggestionsQuery.error as Error)?.message || 'Failed to generate suggestions'}
          </p>
        ) : meiroWebhookSuggestionsQuery.data ? (
          <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.md }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Analyzed <strong>{meiroWebhookSuggestionsQuery.data.events_analyzed}</strong> events,
              {' '}<strong>{meiroWebhookSuggestionsQuery.data.total_conversions_observed.toLocaleString()}</strong> conversions,
              {' '}<strong>{meiroWebhookSuggestionsQuery.data.total_touchpoints_observed.toLocaleString()}</strong> touchpoints.
              {' '}Suggested dedup key: <strong>{meiroWebhookSuggestionsQuery.data.dedup_key_suggestion}</strong>.
              {Number(meiroWebhookSuggestionsQuery.data.site_scope?.events_excluded || 0) > 0 ? (
                <>
                  {' '}Strict Meiro site scope excluded <strong>{Number(meiroWebhookSuggestionsQuery.data.site_scope?.events_excluded || 0).toLocaleString()}</strong> archive event{Number(meiroWebhookSuggestionsQuery.data.site_scope?.events_excluded || 0) === 1 ? '' : 's'} before generating these suggestions.
                </>
              ) : null}
            </div>

            {meiroWebhookSuggestionsQuery.data.event_stream_diagnostics?.available ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  Raw event stream diagnostics
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.35fr) minmax(280px, 1fr)', gap: t.space.md, alignItems: 'stretch' }}>
                  <div style={{ minWidth: 0 }}>
                    <ResponsiveContainer width="100%" height={260}>
                      <BarChart data={rawEventFunnelData} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                        <XAxis dataKey="label" tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }} />
                        <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                        <Tooltip
                          contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                          formatter={(value: number, _name: string, item: { payload?: { annotation?: string } }) => [
                            Number(value).toLocaleString(),
                            item.payload?.annotation || 'count',
                          ]}
                        />
                        <Bar dataKey="value" fill={t.color.chart[2]} radius={[4, 4, 0, 0]}>
                          <LabelList dataKey="annotation" position="top" fill={t.color.textMuted} fontSize={11} />
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: t.space.sm }}>
                    {[
                      { label: 'Usable names', value: `${(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.usable_event_name_share * 100).toFixed(1)}%` },
                      { label: 'Identity coverage', value: `${(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.identity_share * 100).toFixed(1)}%` },
                      { label: 'Source/medium', value: `${(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.source_medium_share * 100).toFixed(1)}%` },
                      { label: 'Referrer-only', value: `${(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.referrer_only_share * 100).toFixed(1)}%` },
                      { label: 'Conversion linkage', value: `${(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.conversion_linkage_share * 100).toFixed(1)}%` },
                      { label: 'Touchpoint-like', value: Number(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.touchpoint_like_events || 0).toLocaleString() },
                    ].map((item) => (
                      <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                        <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{item.value}</div>
                      </div>
                    ))}
                  </div>
                </div>
                {(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.warnings || []).length ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                    {(meiroWebhookSuggestionsQuery.data.event_stream_diagnostics.warnings || []).join(' · ')}
                  </div>
                ) : (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Recent raw events look structurally usable for reconstruction.
                  </div>
                )}
              </div>
            ) : null}

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                KPI / conversion suggestions
              </div>
              {(meiroWebhookSuggestionsQuery.data.kpi_suggestions || []).length ? (
                <div style={{ display: 'grid', gap: 6 }}>
                  {meiroWebhookSuggestionsQuery.data.kpi_suggestions.slice(0, 6).map((kpi) => (
                    <div key={kpi.id} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      <strong>{kpi.label}</strong> ({kpi.type}) · event: <code>{kpi.event_name}</code>
                      {kpi.value_field ? <> · value field: <code>{kpi.value_field}</code></> : null}
                      {kpi.coverage_pct != null ? <> · coverage: {kpi.coverage_pct.toFixed(1)}%</> : null}
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No KPI suggestions yet.</div>
              )}
              <div style={{ marginTop: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => applyKpiSuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads.kpis)}
                  disabled={applyKpiSuggestionsMutation.isPending || !(meiroWebhookSuggestionsQuery.data.apply_payloads.kpis.definitions || []).length}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: '#fff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: applyKpiSuggestionsMutation.isPending ? 'wait' : 'pointer',
                    opacity: applyKpiSuggestionsMutation.isPending ? 0.75 : 1,
                  }}
                >
                  {applyKpiSuggestionsMutation.isPending ? 'Applying…' : 'Apply KPI suggestions'}
                </button>
              </div>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                Taxonomy suggestions
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                {(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.channel_rules || []).length} channel rules,
                {' '}{Object.keys(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.source_aliases || {}).length} source aliases,
                {' '}{Object.keys(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.medium_aliases || {}).length} medium aliases,
                {' '}{(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.unresolved_pairs || []).length} unresolved source/medium pairs.
              </div>
              <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))' }}>
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.xs }}>Top sources</div>
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={topSourceRows} margin={{ top: 8, right: 16, left: 8, bottom: 24 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                      <XAxis dataKey="source" tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }} interval={0} angle={-18} textAnchor="end" height={56} />
                      <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                      <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                      <Bar dataKey="count" fill={t.color.chart[4]} radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.xs }}>Top mediums</div>
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={topMediumRows} margin={{ top: 8, right: 16, left: 8, bottom: 24 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                      <XAxis dataKey="medium" tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }} interval={0} angle={-18} textAnchor="end" height={56} />
                      <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                      <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                      <Bar dataKey="count" fill={t.color.chart[1]} radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
              {!!observedSources.length && !!observedMediums.length && (
                <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.xs }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed source / medium matrix</div>
                  <div style={{ overflowX: 'auto' }}>
                    <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: 0, fontSize: t.font.sizeXs }}>
                      <thead>
                        <tr>
                          <th style={{ textAlign: 'left', padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.textMuted, background: t.color.bg, position: 'sticky', left: 0 }}>Source \ Medium</th>
                          {observedMediums.map((medium) => (
                            <th key={medium} style={{ textAlign: 'center', padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.textMuted, background: t.color.bg }}>
                              {medium || '∅'}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {observedSources.map((source, rowIdx) => (
                          <tr key={source}>
                            <td
                              style={{
                                padding: `${t.space.sm}px ${t.space.md}px`,
                                fontWeight: t.font.weightMedium,
                                background: rowIdx % 2 === 0 ? t.color.surface : t.color.bg,
                                position: 'sticky',
                                left: 0,
                              }}
                            >
                              {source || '∅'}
                            </td>
                            {observedMediums.map((medium) => {
                              const count = Number(observedPairLookup.get(`${source}__${medium}`) || 0)
                              const intensity = count > 0 ? count / maxObservedPairCount : 0
                              return (
                                <td
                                  key={`${source}-${medium}`}
                                  style={{
                                    padding: `${t.space.sm}px ${t.space.md}px`,
                                    textAlign: 'center',
                                    fontVariantNumeric: 'tabular-nums',
                                    background: intensity > 0 ? `color-mix(in srgb, ${t.color.accentMuted} ${Math.round(intensity * 85)}%, ${t.color.surface})` : t.color.surface,
                                    color: count > 0 ? t.color.text : t.color.textMuted,
                                    borderBottom: `1px solid ${t.color.borderLight}`,
                                  }}
                                >
                                  {count > 0 ? count.toLocaleString() : '—'}
                                </td>
                              )
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
              {Object.keys(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.source_aliases || {}).slice(0, 4).map((raw) => (
                <div key={`src-${raw}`} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 4 }}>
                  Source alias: <code>{raw}</code> → <code>{meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.source_aliases[raw]}</code>
                </div>
              ))}
              {Object.keys(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.medium_aliases || {}).slice(0, 4).map((raw) => (
                <div key={`med-${raw}`} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 4 }}>
                  Medium alias: <code>{raw}</code> → <code>{meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.medium_aliases[raw]}</code>
                </div>
              ))}
              {(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.channel_rules || []).slice(0, 5).map((rule) => (
                <div key={rule.name} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 4 }}>
                  <strong>{rule.name}</strong> → <code>{rule.channel}</code>
                  {rule.match_source || rule.match_medium ? (
                    <> · match: <code>{rule.match_source || 'any'}</code> / <code>{rule.match_medium || 'any'}</code></>
                  ) : null}
                  {rule.observed_count != null ? <> · observed: {rule.observed_count}</> : null}
                </div>
              ))}
              {!!unresolvedPairRows.length && (
                <div style={{ marginTop: t.space.md }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.xs }}>Top unresolved pairs</div>
                  <AnalyticsTable
                    columns={unresolvedPairColumns}
                    rows={unresolvedPairRows}
                    rowKey={(row, index) => `${row.source}-${row.medium}-${index}`}
                    tableLabel="Top unresolved taxonomy pairs"
                    density="compact"
                    minWidth={460}
                    stickyFirstColumn
                  />
                </div>
              )}
              <div style={{ marginTop: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => applyTaxonomySuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy)}
                  disabled={applyTaxonomySuggestionsMutation.isPending}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: '#fff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: applyTaxonomySuggestionsMutation.isPending ? 'wait' : 'pointer',
                    opacity: applyTaxonomySuggestionsMutation.isPending ? 0.75 : 1,
                  }}
                >
                  {applyTaxonomySuggestionsMutation.isPending ? 'Applying…' : 'Apply taxonomy suggestions'}
                </button>
              </div>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                Meiro mapping suggestions
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Suggested field paths for importing flat webhook payloads through the Meiro CDP import flow.
              </div>
              <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Current mapping version: <strong>{meiroMappingStateQuery.data?.version ?? 0}</strong>
                {' '}· approval: <strong>{meiroMappingStateQuery.data?.approval?.status || 'unreviewed'}</strong>
                {meiroMappingStateQuery.data?.approval?.updated_at ? (
                  <> · updated: {new Date(meiroMappingStateQuery.data.approval.updated_at).toLocaleString()}</>
                ) : null}
              </div>
              {meiroMappingStateQuery.data?.approval?.note ? (
                <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Note: {meiroMappingStateQuery.data.approval.note}
                </div>
              ) : null}
              <div style={{ display: 'grid', gap: 4, marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <div>Touchpoints: <code>{String(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping.touchpoint_attr || 'touchpoints')}</code></div>
                <div>Value: <code>{String(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping.value_attr || 'conversion_value')}</code></div>
                <div>Source: <code>{String(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping.source_field || 'source')}</code> · Medium: <code>{String(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping.medium_field || 'medium')}</code></div>
                <div>Campaign: <code>{String(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping.campaign_field || 'campaign')}</code> · Channel: <code>{String(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping.channel_field || 'channel')}</code></div>
              </div>
              <div style={{ marginTop: t.space.sm, display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <button
                  type="button"
                  onClick={() => applyMappingSuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping)}
                  disabled={applyMappingSuggestionsMutation.isPending}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: '#fff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: applyMappingSuggestionsMutation.isPending ? 'wait' : 'pointer',
                    opacity: applyMappingSuggestionsMutation.isPending ? 0.75 : 1,
                  }}
                >
                  {applyMappingSuggestionsMutation.isPending ? 'Applying…' : 'Apply mapping suggestions'}
                </button>
                <button
                  type="button"
                  onClick={() => updateMappingApprovalMutation.mutate({ status: 'approved', note: 'Approved from webhook suggestions' })}
                  disabled={updateMappingApprovalMutation.isPending}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                    background: 'transparent',
                    border: `1px solid ${t.color.border}`,
                    borderRadius: t.radius.sm,
                    cursor: updateMappingApprovalMutation.isPending ? 'wait' : 'pointer',
                    opacity: updateMappingApprovalMutation.isPending ? 0.75 : 1,
                  }}
                >
                  Approve mapping
                </button>
                <button
                  type="button"
                  onClick={() => updateMappingApprovalMutation.mutate({ status: 'rejected', note: 'Rejected from webhook suggestions review' })}
                  disabled={updateMappingApprovalMutation.isPending}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                    background: 'transparent',
                    border: `1px solid ${t.color.border}`,
                    borderRadius: t.radius.sm,
                    cursor: updateMappingApprovalMutation.isPending ? 'wait' : 'pointer',
                    opacity: updateMappingApprovalMutation.isPending ? 0.75 : 1,
                  }}
                >
                  Reject mapping
                </button>
              </div>
              <div style={{ marginTop: t.space.md, paddingTop: t.space.md, borderTop: `1px solid ${t.color.borderLight}` }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                  Sanitation policy suggestions
                </div>
                {(meiroWebhookSuggestionsQuery.data.sanitation_suggestions || []).length ? (
                  <div style={{ display: 'grid', gap: 6 }}>
                    {(meiroWebhookSuggestionsQuery.data.sanitation_suggestions || []).slice(0, 6).map((item) => (
                      <div key={item.id} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        <strong>{item.title}</strong>
                        {item.impact_count != null ? <> · observed: {item.impact_count}</> : null}
                        {item.confidence?.band ? <> · confidence: {item.confidence.band}</> : null}
                        <div style={{ marginTop: 2 }}>{item.description}</div>
                        {item.recommended_action ? <div style={{ marginTop: 2 }}>Action: {item.recommended_action}</div> : null}
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No sanitation policy updates are suggested from recent webhook drift.</div>
                )}
                <div style={{ marginTop: t.space.sm }}>
                  <button
                    type="button"
                    onClick={() => applySanitationSuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads.sanitation)}
                    disabled={applySanitationSuggestionsMutation.isPending || !(meiroWebhookSuggestionsQuery.data.sanitation_suggestions || []).length}
                    style={{
                      padding: `${t.space.xs}px ${t.space.md}px`,
                      fontSize: t.font.sizeSm,
                      color: '#fff',
                      background: t.color.accent,
                      border: 'none',
                      borderRadius: t.radius.sm,
                      cursor: applySanitationSuggestionsMutation.isPending ? 'wait' : 'pointer',
                      opacity: applySanitationSuggestionsMutation.isPending ? 0.75 : 1,
                    }}
                  >
                    {applySanitationSuggestionsMutation.isPending ? 'Applying…' : 'Apply sanitation suggestions'}
                  </button>
                </div>
              </div>
              {(meiroMappingStateQuery.data?.history || []).length ? (
                <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Recent mapping history:
                  {(meiroMappingStateQuery.data?.history || []).slice(-3).reverse().map((item, index) => (
                    <div key={index} style={{ marginTop: 4 }}>
                      <code>{String(item.action || 'event')}</code> · {item.at ? new Date(String(item.at)).toLocaleString() : '—'}
                      {item.status ? <> · status: <strong>{String(item.status)}</strong></> : null}
                    </div>
                  ))}
                </div>
              ) : null}
              <div style={{ marginTop: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => setShowApplyAllPreview(true)}
                  disabled={applyAllSuggestionsMutation.isPending}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: '#fff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: applyAllSuggestionsMutation.isPending ? 'wait' : 'pointer',
                    opacity: applyAllSuggestionsMutation.isPending ? 0.75 : 1,
                  }}
                >
                  Apply all (preview first)
                </button>
              </div>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                Webhook replay
              </div>
              {meiroWebhookArchiveStatusQuery.isLoading ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading webhook archive status…</div>
              ) : meiroWebhookArchiveStatusQuery.isError ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                  {(meiroWebhookArchiveStatusQuery.error as Error)?.message || 'Failed to load webhook archive status'}
                </div>
              ) : (meiroWebhookArchiveStatusQuery.data?.available || meiroEventArchiveStatusQuery.data?.available) ? (
                <>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Profile archive: <strong>{Number(meiroWebhookArchiveStatusQuery.data?.entries || 0).toLocaleString()}</strong> batches / <strong>{Number(meiroWebhookArchiveStatusQuery.data?.profiles_received || 0).toLocaleString()}</strong> payloads
                    {' '}· Event archive: <strong>{Number(meiroEventArchiveStatusQuery.data?.entries || 0).toLocaleString()}</strong> batches / <strong>{Number(meiroEventArchiveStatusQuery.data?.events_received || 0).toLocaleString()}</strong> events
                  </div>
                  <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Latest profile archive: <strong>{meiroWebhookArchiveStatusQuery.data?.last_received_at ? new Date(meiroWebhookArchiveStatusQuery.data.last_received_at).toLocaleString() : '—'}</strong>
                    {' '}· latest event archive: <strong>{meiroEventArchiveStatusQuery.data?.last_received_at ? new Date(meiroEventArchiveStatusQuery.data.last_received_at).toLocaleString() : '—'}</strong>
                  </div>
                  <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Parser versions: profiles <strong>{(meiroWebhookArchiveStatusQuery.data?.parser_versions || []).join(', ') || '—'}</strong>
                    {' '}· events <strong>{(meiroEventArchiveStatusQuery.data?.parser_versions || []).join(', ') || '—'}</strong>
                    {' '}· current parser: <strong>{meiroWebhookEventsQuery.data?.items?.[0]?.parser_version || 'current'}</strong>
                  </div>
                  <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {meiroEventArchiveStatusQuery.data?.available
                      ? 'Rebuilds journeys from archived raw webhook events and re-imports them using the current approved mapping.'
                      : 'Rebuilds stored Meiro profiles from archived webhook payloads and re-imports them using the current approved mapping.'}
                  </div>
                  <div style={{ marginTop: t.space.sm }}>
                    <button
                      type="button"
                      onClick={() => reprocessWebhookArchiveMutation.mutate()}
                      disabled={
                        reprocessWebhookArchiveMutation.isPending ||
                        meiroMappingStateQuery.data?.approval?.status !== 'approved'
                      }
                      style={{
                        padding: `${t.space.xs}px ${t.space.md}px`,
                        fontSize: t.font.sizeSm,
                        color: '#fff',
                        background: t.color.accent,
                        border: 'none',
                        borderRadius: t.radius.sm,
                        cursor: reprocessWebhookArchiveMutation.isPending ? 'wait' : 'pointer',
                        opacity:
                          reprocessWebhookArchiveMutation.isPending ||
                          meiroMappingStateQuery.data?.approval?.status !== 'approved'
                            ? 0.75
                            : 1,
                      }}
                    >
                      {reprocessWebhookArchiveMutation.isPending ? 'Reprocessing…' : 'Reprocess archive into attribution'}
                    </button>
                  </div>
                  {meiroMappingStateQuery.data?.approval?.status !== 'approved' ? (
                    <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.warning }}>
                      Approve the current mapping before replaying archived payloads.
                    </div>
                  ) : null}
                </>
              ) : (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  No archived webhook payloads available yet.
                </div>
              )}
            </div>
          </div>
        ) : null}
      </div>

      <ActivationMeasurementShortcuts
        title="Measured activation evidence"
        subtitle="Use this after Meiro replay or deciEngine imports to inspect the campaigns, decisions, assets, and offers now backed by journey evidence."
      />

      {/* Alerts */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: t.space.md,
            marginBottom: t.space.md,
          }}
        >
          <div>
            <h2 style={{ margin: '0 0 4px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Alerts
            </h2>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Issues detected by data quality rules. Acknowledge or resolve once investigated.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center' }}>
            <button
              type="button"
              onClick={exportAlerts}
              style={{
                padding: `${t.space.xs}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
                background: 'transparent',
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Export DQ report
            </button>
            <a
              href="#"
              onClick={(e) => {
                e.preventDefault()
                tilesRef.current?.scrollIntoView({ behavior: 'smooth' })
              }}
              style={{ fontSize: t.font.sizeSm, color: t.color.accent }}
            >
              View metric trends
            </a>
          </div>
        </div>

        <div style={{ marginBottom: t.space.md }}>
          <AnalyticsToolbar
            filters={
              <>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.xs }}>
                  {['', 'open', 'acked', 'resolved'].map((s) => (
                    <button
                      key={s || 'all'}
                      type="button"
                      onClick={() => setAlertStatusFilter(s)}
                      style={{
                        padding: '4px 10px',
                        fontSize: t.font.sizeXs,
                        borderRadius: 999,
                        border: `1px solid ${alertStatusFilter === s ? t.color.accent : t.color.border}`,
                        background: alertStatusFilter === s ? t.color.accentMuted : 'transparent',
                        color: alertStatusFilter === s ? t.color.accent : t.color.textSecondary,
                        cursor: 'pointer',
                      }}
                    >
                      {s || 'All'}
                    </button>
                  ))}
                </div>
                <select
                  value={alertSeverityFilter}
                  onChange={(e) => setAlertSeverityFilter(e.target.value)}
                  style={{
                    padding: '4px 8px',
                    fontSize: t.font.sizeXs,
                    border: `1px solid ${t.color.border}`,
                    borderRadius: t.radius.sm,
                    background: t.color.surface,
                    color: t.color.text,
                  }}
                >
                  <option value="">All severity</option>
                  <option value="info">Info</option>
                  <option value="warn">Warning</option>
                  <option value="critical">Critical</option>
                </select>
                <select
                  value={alertSourceFilter}
                  onChange={(e) => setAlertSourceFilter(e.target.value)}
                  style={{
                    padding: '4px 8px',
                    fontSize: t.font.sizeXs,
                    border: `1px solid ${t.color.border}`,
                    borderRadius: t.radius.sm,
                    background: t.color.surface,
                    color: t.color.text,
                  }}
                >
                  <option value="">All sources</option>
                  <option value="journeys">Journeys</option>
                  <option value="meiro_web">Meiro</option>
                  <option value="google_ads_cost">Google</option>
                  <option value="meta_cost">Meta</option>
                  <option value="linkedin_cost">LinkedIn</option>
                  <option value="taxonomy">Taxonomy</option>
                </select>
              </>
            }
            summary="Filter data-quality alerts by status, severity, and source without leaving the current table."
          />
        </div>

        {alertsQuery.isLoading ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading alerts…</p>
        ) : alertsQuery.data && alertsQuery.data.length > 0 ? (
          <AnalyticsTable
            columns={alertColumns}
            rows={alertsQuery.data}
            rowKey={(alert) => String(alert.id)}
            tableLabel="Data quality alerts"
            stickyFirstColumn
            minWidth={1100}
            virtualized
            virtualizationThreshold={20}
            virtualizationHeight={520}
            virtualRowHeight={54}
            allowColumnHiding
            allowDensityToggle
            persistKey="data-quality-alerts"
            defaultHiddenColumnKeys={['source', 'baseline_value']}
            presets={[
              {
                key: 'overview',
                label: 'Overview',
                visibleColumnKeys: ['triggered_at', 'rule', 'metric_key', 'metric_value', 'severity', 'status', 'actions'],
              },
              {
                key: 'thresholds',
                label: 'Thresholds',
                visibleColumnKeys: ['triggered_at', 'rule', 'source', 'metric_key', 'metric_value', 'baseline_value', 'severity', 'status'],
              },
            ]}
            defaultPresetKey="overview"
          />
        ) : (
          <div
            style={{
              padding: t.space.xl,
              textAlign: 'center',
              color: t.color.textSecondary,
              fontSize: t.font.sizeSm,
            }}
          >
            <p style={{ margin: '0 0 8px' }}>No alerts detected in selected period.</p>
            <a
              href="#"
              onClick={(e) => {
                e.preventDefault()
                tilesRef.current?.scrollIntoView({ behavior: 'smooth' })
              }}
              style={{ color: t.color.accent }}
            >
              View metric trends
            </a>
          </div>
        )}
      </div>

      {/* Drilldown drawer + backdrop */}
      {drilldownMetric && (
        <>
          <div
            role="presentation"
            onClick={() => setDrilldownMetric(null)}
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(0,0,0,0.3)',
              zIndex: 999,
            }}
          />
          <DrilldownDrawer
            metricKey={drilldownMetric}
            data={drilldownQuery.data}
            loading={drilldownQuery.isLoading}
            onClose={() => setDrilldownMetric(null)}
          />
        </>
      )}

      {/* Apply-all dry-run preview */}
      {showApplyAllPreview && meiroWebhookSuggestionsQuery.data && (
        <>
          <div
            role="presentation"
            onClick={() => setShowApplyAllPreview(false)}
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(0,0,0,0.3)',
              zIndex: 999,
            }}
          />
          <div
            role="dialog"
            aria-modal="true"
            aria-label="Apply all suggestions preview"
            style={{
              position: 'fixed',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: 'min(860px, calc(100vw - 32px))',
              maxHeight: '80vh',
              overflow: 'auto',
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.lg,
              boxShadow: t.shadowSm,
              zIndex: 1000,
              padding: t.space.lg,
              display: 'grid',
              gap: t.space.md,
            }}
          >
            <div>
              <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Apply all suggestions (dry-run preview)</h3>
              <p style={{ margin: '6px 0 0', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                This preview shows what will be saved to KPI and taxonomy settings.
              </p>
            </div>

            <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <div>
                KPI definitions to save: <strong>{meiroWebhookSuggestionsQuery.data.apply_payloads.kpis.definitions.length}</strong>
                {' '}· primary KPI: <strong>{meiroWebhookSuggestionsQuery.data.apply_payloads.kpis.primary_kpi_id || '—'}</strong>
              </div>
              <div>
                Taxonomy rules to save: <strong>{meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy.channel_rules.length}</strong>
                {' '}· source aliases: <strong>{Object.keys(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy.source_aliases || {}).length}</strong>
                {' '}· medium aliases: <strong>{Object.keys(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy.medium_aliases || {}).length}</strong>
              </div>
              <div>
                Mapping fields to save: <strong>{Object.keys(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping || {}).length}</strong>
              </div>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 6 }}>KPI payload preview</div>
              <pre style={{ margin: 0, fontSize: t.font.sizeXs, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {JSON.stringify(meiroWebhookSuggestionsQuery.data.apply_payloads.kpis, null, 2)}
              </pre>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 6 }}>Taxonomy payload preview</div>
              <pre style={{ margin: 0, fontSize: t.font.sizeXs, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {JSON.stringify(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy, null, 2)}
              </pre>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 6 }}>Meiro mapping payload preview</div>
              <pre style={{ margin: 0, fontSize: t.font.sizeXs, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {JSON.stringify(meiroWebhookSuggestionsQuery.data.apply_payloads.mapping, null, 2)}
              </pre>
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
              <button
                type="button"
                onClick={() => setShowApplyAllPreview(false)}
                style={{
                  padding: `${t.space.xs}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => applyAllSuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads)}
                disabled={applyAllSuggestionsMutation.isPending}
                style={{
                  padding: `${t.space.xs}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  color: '#fff',
                  background: t.color.accent,
                  border: 'none',
                  borderRadius: t.radius.sm,
                  cursor: applyAllSuggestionsMutation.isPending ? 'wait' : 'pointer',
                  opacity: applyAllSuggestionsMutation.isPending ? 0.75 : 1,
                }}
              >
                {applyAllSuggestionsMutation.isPending ? 'Applying…' : 'Confirm apply all'}
              </button>
            </div>
          </div>
        </>
      )}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}

// --- Tile ---

function Tile(props: {
  label: string
  metricKey: string
  value: string | null
  naReason?: string
  description: string
  trend: { delta: number; improved: boolean } | null
  onClick: () => void
}) {
  const cfg = METRIC_CONFIG[props.metricKey]
  const numVal = props.value != null ? parseFloat(props.value) : null
  const status = cfg && numVal != null ? getStatus(numVal, cfg) : 'ok'
  const statusColor = status === 'ok' ? t.color.success : status === 'warning' ? t.color.warning : t.color.danger
  const thresholdHint =
    cfg && props.metricKey !== 'freshness_lag_minutes'
      ? `Warning > ${cfg.warnThreshold}${cfg.unit}; Critical > ${cfg.criticalThreshold}${cfg.unit}`
      : cfg && props.metricKey === 'conversion_attributable_pct'
        ? `Warning < ${cfg.warnThreshold}%; Critical < ${cfg.criticalThreshold}%`
        : cfg
          ? `Warning > ${cfg.warnThreshold}h; Critical > ${cfg.criticalThreshold}h`
          : ''
  const displayValue = props.value != null ? (props.metricKey === 'freshness_lag_minutes' ? props.value : `${props.value}%`) : 'N/A'

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={props.onClick}
      onKeyDown={(e) => e.key === 'Enter' && props.onClick()}
      title={`${props.description}${thresholdHint ? ` Thresholds: ${thresholdHint}` : ''}${props.naReason ? ` ${props.naReason}` : ''}`}
      style={{
        background: t.color.surface,
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.lg,
        padding: t.space.lg,
        boxShadow: t.shadowSm,
        cursor: 'pointer',
        transition: 'box-shadow 0.15s',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.08)'
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.boxShadow = t.shadowSm
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 4 }}>
        <p
          style={{
            margin: 0,
            fontSize: t.font.sizeXs,
            fontWeight: t.font.weightMedium,
            textTransform: 'uppercase',
            letterSpacing: '0.06em',
            color: t.color.textSecondary,
          }}
        >
          {props.label}
        </p>
        {numVal != null && cfg && (
          <span
            style={{
              padding: '2px 6px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              background: status === 'ok' ? t.color.successMuted : status === 'warning' ? t.color.warningMuted : t.color.dangerMuted,
              color: statusColor,
            }}
          >
            {status === 'ok' ? 'OK' : status === 'warning' ? 'Warning' : 'Critical'}
          </span>
        )}
        {props.value == null && props.naReason && (
          <span
            title={props.naReason}
            style={{
              padding: '2px 6px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              color: t.color.textMuted,
            }}
          >
            N/A
          </span>
        )}
      </div>
      <p
        style={{
          margin: '0 0 4px',
          fontSize: t.font.sizeXl,
          fontWeight: t.font.weightBold,
          color: t.color.text,
        }}
      >
        {displayValue}
      </p>
      <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
        {props.trend != null && (
          <span
            style={{
              fontSize: t.font.sizeXs,
              color: props.trend.improved ? t.color.success : t.color.danger,
            }}
          >
            {props.trend.improved ? '↓' : '↑'} {Math.abs(props.trend.delta).toFixed(1)}
            {props.metricKey === 'freshness_lag_minutes' ? 'min' : '%'}
          </span>
        )}
        <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textMuted }}>{props.description}</p>
      </div>
    </div>
  )
}

// --- AlertActions ---

function AlertActions(props: {
  alert: DQAlert
  onStatus: (status: string) => void
  onNote: (note: string) => void
}) {
  const [showNote, setShowNote] = useState(false)
  const [noteText, setNoteText] = useState(props.alert.note ?? '')
  const { alert, onStatus } = props
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.xs, alignItems: 'center' }}>
      {alert.status !== 'acked' && alert.status !== 'resolved' && (
        <button
          type="button"
          onClick={() => onStatus('acked')}
          style={{
            padding: '2px 6px',
            fontSize: t.font.sizeXs,
            color: t.color.textSecondary,
            background: 'transparent',
            border: `1px solid ${t.color.border}`,
            borderRadius: t.radius.sm,
            cursor: 'pointer',
          }}
        >
          Acknowledge
        </button>
      )}
      {alert.status !== 'resolved' && (
        <button
          type="button"
          onClick={() => onStatus('resolved')}
          style={{
            padding: '2px 6px',
            fontSize: t.font.sizeXs,
            color: t.color.success,
            background: 'transparent',
            border: `1px solid ${t.color.success}`,
            borderRadius: t.radius.sm,
            cursor: 'pointer',
          }}
        >
          Resolve
        </button>
      )}
      <button
        type="button"
        onClick={() => setShowNote(!showNote)}
        style={{
          padding: '2px 6px',
          fontSize: t.font.sizeXs,
          color: t.color.textSecondary,
          background: 'transparent',
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.sm,
          cursor: 'pointer',
        }}
      >
        Add note
      </button>
      {showNote && (
        <div style={{ display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            type="text"
            value={noteText}
            onChange={(e) => setNoteText(e.target.value)}
            placeholder="Note..."
            style={{
              padding: '2px 6px',
              fontSize: t.font.sizeXs,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              minWidth: 120,
            }}
          />
          <button
            type="button"
            onClick={() => {
              props.onNote(noteText)
              setShowNote(false)
            }}
            style={{
              padding: '2px 6px',
              fontSize: t.font.sizeXs,
              color: t.color.accent,
              background: 'transparent',
              border: 'none',
              cursor: 'pointer',
            }}
          >
            Save
          </button>
        </div>
      )}
    </div>
  )
}

// --- DrilldownDrawer ---

function DrilldownDrawer(props: {
  metricKey: string
  data: DQDrilldown | undefined
  loading: boolean
  onClose: () => void
}) {
  const { metricKey, data, loading, onClose } = props
  const labelMap: Record<string, string> = {
    freshness_lag_minutes: 'Max freshness lag',
    missing_profile_pct: 'Journeys missing profile ID',
    missing_timestamp_pct: 'Journeys missing timestamps',
    duplicate_id_pct: 'Duplicate IDs',
    conversion_attributable_pct: 'Attributable conversions',
    defaulted_conversion_value_pct: 'Defaulted conversion values',
    raw_zero_conversion_value_pct: 'Raw zero-value conversions',
    unresolved_source_medium_touchpoint_pct: 'Unresolved source / medium touchpoints',
    inferred_mapping_journey_pct: 'Journeys using inferred mappings',
  }
  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        right: 0,
        width: 420,
        maxWidth: '100%',
        height: '100%',
        background: t.color.surface,
        borderLeft: `1px solid ${t.color.border}`,
        boxShadow: '-4px 0 20px rgba(0,0,0,0.1)',
        zIndex: 1000,
        overflow: 'auto',
        padding: t.space.xl,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.lg }}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>
          {labelMap[metricKey] ?? metricKey}
        </h3>
        <button
          type="button"
          onClick={onClose}
          style={{
            padding: t.space.xs,
            background: 'transparent',
            border: 'none',
            cursor: 'pointer',
            fontSize: t.font.sizeLg,
            color: t.color.textSecondary,
          }}
        >
          ×
        </button>
      </div>
      {loading ? (
        <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading…</p>
      ) : data ? (
        <>
          <section style={{ marginBottom: t.space.xl }}>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Definition</h4>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.text, lineHeight: 1.5 }}>
              {data.definition}
            </p>
          </section>
          <section style={{ marginBottom: t.space.xl }}>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Breakdown by source</h4>
            {data.breakdown.length > 0 ? (
              <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeSm, color: t.color.text }}>
                {data.breakdown.map((b) => (
                  <li key={b.source}>
                    {b.source}: {typeof b.value === 'number' ? (metricKey === 'freshness_lag_minutes' ? `${(b.value / 60).toFixed(1)}h` : `${b.value.toFixed(1)}%`) : b.value}
                  </li>
                ))}
              </ul>
            ) : (
              <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>N/A</p>
            )}
          </section>
          <section style={{ marginBottom: t.space.xl }}>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Top offenders</h4>
            {data.top_offenders.length > 0 ? (
              <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeSm, color: t.color.text }}>
                {data.top_offenders.map((o, i) => (
                  <li key={i}>{o.key ?? JSON.stringify(o)}</li>
                ))}
              </ul>
            ) : (
              <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>N/A</p>
            )}
          </section>
          <section>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Recommended actions</h4>
            <RecommendedActionsList
              actions={data.recommended_actions}
              onActionClick={(action) => navigateForRecommendedAction(action, { defaultPage: 'datasources' })}
            />
          </section>
        </>
      ) : (
        <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>No data</p>
      )}
    </div>
  )
}
