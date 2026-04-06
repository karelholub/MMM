import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import { AnalyticsTable, AnalyticsToolbar, type AnalyticsTableColumn } from '../components/dashboard'
import GlobalFilterBar, { GlobalFiltersState } from '../components/dashboard/GlobalFilterBar'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import { tokens as t } from '../theme/tokens'
import { apiGetJson, apiSendJson, getUserContext, withQuery } from '../lib/apiClient'
import { buildListQuery, type PaginatedResponse } from '../lib/apiSchemas'
import { defaultRecentDateRange } from '../lib/dateRange'
import CreateJourneyModal from './journeys/CreateJourneyModal'
import {
  createJourneyAlert,
  previewJourneyAlert,
  type JourneyAlertCreatePayload,
  type JourneyAlertPreviewResponse,
} from './alerts/api'

interface JourneysProps {
  featureEnabled: boolean
  hasPermission: boolean
  canManageDefinitions: boolean
  journeyExamplesEnabled: boolean
  funnelBuilderEnabled: boolean
}

interface JourneyDefinition {
  id: string
  name: string
  description?: string | null
  conversion_kpi_id?: string | null
  lookback_window_days: number
  mode_default: 'conversion_only' | 'all_journeys'
  is_archived?: boolean
  lifecycle_status?: 'active' | 'archived' | string
  created_by?: string | null
  updated_by?: string | null
  archived_by?: string | null
  created_at?: string | null
  archived_at?: string | null
  updated_at?: string | null
}

interface JourneyDefinitionLifecycle {
  definition: JourneyDefinition
  dependency_counts: {
    saved_views: number
    funnels: number
    hypotheses: number
    experiments: number
    alerts: number
  }
  output_counts: {
    journey_instances: number
    path_days: number
    transition_days: number
    example_days?: number
  }
  allowed_actions: {
    can_archive: boolean
    can_restore: boolean
    can_duplicate: boolean
    can_delete: boolean
    can_rebuild?: boolean
  }
  rebuild_state?: {
    status: 'active' | 'stale' | 'archived' | string
    stale_reason?: string | null
    last_rebuilt_at?: string | null
  }
  warnings: string[]
}

interface JourneyDefinitionAuditItem {
  id: number
  journey_definition_id: string
  actor: string
  action: string
  diff_json?: Record<string, unknown> | null
  created_at?: string | null
}

interface JourneyPathRow {
  path_hash: string
  path_steps: string[] | string
  path_length: number
  count_journeys: number
  count_conversions: number
  conversion_rate: number
  avg_time_to_convert_sec: number | null
  p50_time_to_convert_sec: number | null
  p90_time_to_convert_sec: number | null
  channel_group?: string | null
  campaign_id?: string | null
  device?: string | null
  country?: string | null
}

interface JourneyPathsResponse {
  items: JourneyPathRow[]
  total: number
  page: number
  limit: number
}

interface ChannelCredit {
  channel: string
  attributed_value: number
  attributed_share: number
}

interface AttributionSummaryResponse {
  by_channel: ChannelCredit[]
  by_channel_group: Array<{ channel_group: string; attributed_value: number; attributed_share: number }>
  totals: { journeys: number; total_value_observed: number; total_value_attributed: number }
}

interface FunnelDefinition {
  id: string
  name: string
  description?: string | null
  steps: string[]
  counting_method: string
  window_days: number
}

interface FunnelListResponse {
  items: FunnelDefinition[]
}

interface FunnelResultsResponse {
  steps: Array<{ step: string; position: number; count: number; dropoff_pct: number | null }>
  time_between_steps: Array<{ from_step: string; to_step: string; count: number; avg_sec: number; p50_sec: number; p90_sec: number }>
  breakdown: {
    device: Array<{ key: string; count: number }>
    channel_group: Array<{ key: string; count: number }>
  }
  meta: {
    source: string
    used_raw_fallback: boolean
    warning?: string | null
  }
}

interface FunnelDiagnosticItem {
  title: string
  evidence: string[]
  impact_estimate: {
    direction?: string
    magnitude?: string
    estimated_users_affected?: number
  }
  confidence: 'low' | 'medium' | 'high' | string
  next_action: string
}

interface JourneyTransitionsResponse {
  nodes: Array<{ id: string; label: string; depth: number | null; count_in: number; count_out: number; count_total: number }>
  edges: Array<{ source: string; target: string; value: number; count_transitions: number; count_profiles: number }>
  meta: {
    date_from: string
    date_to: string
    mode: string
    min_count: number
    max_nodes: number
    max_depth: number
    group_other?: boolean
    grouped_to_other?: boolean
    dropped_edges?: number
  }
}

interface JourneyExampleItem {
  conversion_id: string
  profile_id: string
  conversion_key?: string | null
  conversion_ts?: string | null
  path_hash: string
  steps: string[]
  touchpoints_count: number
  conversion_value: number
  dimensions: {
    channel_group?: string | null
    campaign_id?: string | null
    device?: string | null
    country?: string | null
  }
  touchpoints_preview: Array<{
    ts?: string | null
    channel?: string | null
    event?: string | null
    campaign?: string | null
  }>
}

interface JourneyExamplesResponse {
  items: JourneyExampleItem[]
  total: number
  date_from: string
  date_to: string
  path_hash?: string | null
  contains_step?: string | null
}

interface KpiDefinition {
  id: string
  label: string
}

interface KpiResponse {
  definitions: KpiDefinition[]
  primary_kpi_id?: string | null
}

interface CreateJourneyDraft {
  name: string
  description: string
  conversion_kpi_id: string
  lookback_window_days: number
  mode_default: 'conversion_only' | 'all_journeys'
}

interface CreateFunnelDraft {
  name: string
  description: string
  stepsText: string
  counting_method: 'ordered'
  window_days: number
}

interface AlertDraft {
  name: string
  type: JourneyAlertCreatePayload['type']
  domain: JourneyAlertCreatePayload['domain']
  metric: string
  comparison_mode: 'previous_period' | 'rolling_baseline'
  threshold_pct: number
  severity: 'info' | 'warn' | 'critical'
  cooldown_days: number
}

interface JourneyInsightItem {
  id: string
  kind: string
  title: string
  summary: string
  severity: string
  confidence: string
  support_count: number
  baseline_rate: number
  observed_rate: number
  impact_estimate: {
    direction?: string
    magnitude?: string
    estimated_users_affected?: number
  }
  evidence: string[]
  suggested_hypothesis: {
    title: string
    hypothesis_text: string
    trigger: Record<string, unknown>
    segment: Record<string, unknown>
    current_action: Record<string, unknown>
    proposed_action: Record<string, unknown>
    target_kpi?: string | null
    support_count?: number
    baseline_rate?: number
    sample_size_target?: number
  }
}

interface JourneyInsightsResponse {
  items: JourneyInsightItem[]
  summary: {
    paths_considered: number
    journeys: number
    conversions: number
    baseline_conversion_rate: number
  }
}

interface JourneyHypothesisRecord {
  id: string
  journey_definition_id: string
  owner_user_id: string
  title: string
  target_kpi?: string | null
  hypothesis_text: string
  trigger: Record<string, unknown>
  segment: Record<string, unknown>
  current_action: Record<string, unknown>
  proposed_action: Record<string, unknown>
  support_count: number
  baseline_rate?: number | null
  sample_size_target?: number | null
  status: string
  linked_experiment_id?: number | null
  result: Record<string, unknown>
  created_at?: string | null
  updated_at?: string | null
}

interface JourneyHypothesesResponse {
  items: JourneyHypothesisRecord[]
  total: number
}

interface LinkedExperimentSummary {
  id: number
  name: string
  channel: string
  start_at: string
  end_at: string
  status: string
  conversion_key?: string | null
  experiment_type: string
  source_type?: string | null
  source_id?: string | null
  source_name?: string | null
}

interface LinkedExperimentDetail extends LinkedExperimentSummary {
  notes?: string | null
  segment?: Record<string, unknown>
  policy?: Record<string, unknown>
  guardrails?: Record<string, unknown>
}

interface LinkedExperimentResults {
  experiment_id: number
  status: string
  treatment?: {
    n: number
    conversions: number
    conversion_rate: number
    total_value: number
  }
  control?: {
    n: number
    conversions: number
    conversion_rate: number
    total_value: number
  }
  uplift_abs?: number | null
  uplift_rel?: number | null
  ci_low?: number | null
  ci_high?: number | null
  p_value?: number | null
  insufficient_data?: boolean
}

interface LinkedExperimentHealth {
  experiment_id: number
  sample: { treatment: number; control: number }
  exposures: { treatment: number; control: number }
  outcomes: { treatment: number; control: number }
  balance: { status: 'ok' | 'warn'; expected_share: number; observed_share: number }
  data_completeness: {
    assignments: { status: 'ok' | 'fail' }
    outcomes: { status: 'ok' | 'fail' }
    exposures: { status: 'ok' | 'warn' }
  }
  overlap_risk: { status: 'ok' | 'warn'; overlapping_profiles: number }
  ready_state: { label: 'not_ready' | 'early' | 'ready'; reasons: string[] }
}

interface JourneyHypothesisExperimentLinkResponse {
  experiment: LinkedExperimentSummary
  hypothesis: JourneyHypothesisRecord
}

interface JourneyPolicyCandidate {
  rank: number
  step: string
  support_count: number
  conversion_rate: number
  avg_value: number
  uplift_abs: number
  uplift_rel?: number | null
  estimated_incremental_conversions: number
  confidence: string
  is_current_step: boolean
}

interface JourneyPolicySimulationResponse {
  previewAvailable: boolean
  reason?: string | null
  source_window?: { date_from?: string | null; date_to?: string | null }
  prefix?: {
    steps: string[]
    label: string
    current_step?: string | null
  }
  summary?: {
    eligible_journeys: number
    baseline_conversion_rate: number
    current_path_support: number
    current_path_conversion_rate: number
    candidate_count: number
    sample_size_target?: number | null
    observational_only: boolean
  }
  top_candidates: JourneyPolicyCandidate[]
  selected_policy?: (JourneyPolicyCandidate & { rationale?: string | null }) | null
  current_path?: {
    step?: string | null
    support_count: number
    conversion_rate: number
  } | null
  decision?: {
    status: string
    warnings: string[]
    recommended_action?: string | null
  } | null
}

interface JourneyPolicyRecommendationRecord {
  hypothesis_id: string
  title: string
  journey_definition_id: string
  status: string
  learning_stage: string
  linked_experiment_id?: number | null
  prefix: {
    steps: string[]
    label: string
  }
  segment: Record<string, unknown>
  segment_label: string
  policy: {
    step: string
    action: Record<string, unknown>
  }
  support_count: number
  sample_size_target?: number | null
  uplift_abs?: number | null
  uplift_rel?: number | null
  p_value?: number | null
  summary?: string | null
  recommendation: string
  recommendation_reason: string
  score: number
  promotion: Record<string, unknown>
  updated_at?: string | null
}

interface JourneyPolicyRecommendationsResponse {
  items: JourneyPolicyRecommendationRecord[]
  summary: {
    total: number
    promoted: number
    ready_to_promote: number
    validated: number
    in_flight: number
    rejected: number
  }
}

interface JourneyPolicyPromotionResponse {
  hypothesis: JourneyHypothesisRecord
  policy_candidate: JourneyPolicyRecommendationRecord
}

interface JourneyFilterDimensionValue {
  value: string
  count: number
}

interface JourneyFilterDimensionsResponse {
  summary: {
    journey_rows: number
    date_from: string
    date_to: string
    segment_supported: boolean
  }
  channels: JourneyFilterDimensionValue[]
  campaigns: JourneyFilterDimensionValue[]
  devices: JourneyFilterDimensionValue[]
  countries: JourneyFilterDimensionValue[]
  segments: JourneyFilterDimensionValue[]
}

interface JourneyHypothesisRequest {
  journey_definition_id: string
  title: string
  target_kpi?: string | null
  hypothesis_text: string
  trigger: Record<string, unknown>
  segment: Record<string, unknown>
  current_action: Record<string, unknown>
  proposed_action: Record<string, unknown>
  support_count: number
  baseline_rate?: number | null
  sample_size_target?: number | null
  status: string
  linked_experiment_id?: number | null
  result: Record<string, unknown>
}

interface HypothesisDraft {
  title: string
  target_kpi: string
  hypothesis_text: string
  trigger: Record<string, unknown>
  segment: Record<string, unknown>
  current_action: Record<string, unknown>
  proposed_action: Record<string, unknown>
  support_count: number
  baseline_rate: number | null
  sample_size_target: number | null
  status: string
  result: Record<string, unknown>
}

interface HypothesisExperimentDraft {
  start_at: string
  end_at: string
  notes: string
}

type JourneysTab = 'insights' | 'hypotheses' | 'policy' | 'experiments' | 'paths' | 'flow' | 'examples' | 'funnels'
type PathSortBy = 'journeys' | 'conversion_rate' | 'avg_time'

interface SavedJourneyView {
  id: string
  name: string
  selectedJourneyId: string
  activeTab: JourneysTab
  filters: GlobalFiltersState
  pathSortBy: PathSortBy
  pathSortDir: 'asc' | 'desc'
  pathsLimit: number
  examplesPathHash: string
  examplesStepFilter: string
  createdAt: string
}

interface SavedJourneyViewRecord {
  id: string
  name: string
  journey_definition_id?: string | null
  state?: Partial<{
    selectedJourneyId: string
    activeTab: JourneysTab
    filters: GlobalFiltersState
    pathSortBy: PathSortBy
    pathSortDir: 'asc' | 'desc'
    pathsLimit: number
    examplesPathHash: string
    examplesStepFilter: string
  }>
  created_at?: string | null
}

interface SavedJourneyViewsResponse {
  items: SavedJourneyViewRecord[]
  total: number
}

type SavedJourneyViewStatePayload = {
  selectedJourneyId: string
  activeTab: JourneysTab
  filters: GlobalFiltersState
  pathSortBy: PathSortBy
  pathSortDir: 'asc' | 'desc'
  pathsLimit: number
  examplesPathHash: string
  examplesStepFilter: string
}

const ATTR_MODELS = [
  { id: 'last_touch', label: 'Last Touch' },
  { id: 'first_touch', label: 'First Touch' },
  { id: 'linear', label: 'Linear' },
  { id: 'time_decay', label: 'Time Decay' },
  { id: 'position_based', label: 'Position Based' },
  { id: 'markov', label: 'Data-Driven (Markov)' },
]

function buildInitialFilters(dateMin?: string | null, dateMax?: string | null): GlobalFiltersState {
  const fallback = defaultRecentDateRange(30)
  return {
    dateFrom: dateMin?.slice(0, 10) ?? fallback.dateFrom,
    dateTo: dateMax?.slice(0, 10) ?? fallback.dateTo,
    channel: 'all',
    campaign: 'all',
    device: 'all',
    geo: 'all',
    segment: 'all',
  }
}

function readParams(): URLSearchParams {
  if (typeof window === 'undefined') return new URLSearchParams()
  return new URLSearchParams(window.location.search)
}

function clampLookback(v: number): number {
  if (!Number.isFinite(v)) return 30
  return Math.min(365, Math.max(1, Math.round(v)))
}

function normalizeSteps(steps: JourneyPathRow['path_steps']): string[] {
  if (Array.isArray(steps)) return steps.map((s) => String(s))
  if (typeof steps === 'string') {
    return steps
      .split('>')
      .map((s) => s.trim())
      .filter(Boolean)
  }
  return []
}

function parseStepsText(v: string): string[] {
  return v
    .split(/\n|>/g)
    .map((s) => s.trim())
    .filter(Boolean)
}

function formatPercent(v: number): string {
  if (!Number.isFinite(v)) return '0.0%'
  return `${(v * 100).toFixed(1)}%`
}

function experimentStatusLabel(status: string): string {
  const normalized = String(status || '').trim().toLowerCase()
  if (!normalized) return 'Unknown'
  return normalized.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
}

function formatSeconds(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—'
  if (v < 60) return `${v.toFixed(0)}s`
  const mins = v / 60
  if (mins < 60) return `${mins.toFixed(1)}m`
  return `${(mins / 60).toFixed(1)}h`
}

function dimensionLabel(value: string, count: number): string {
  return `${value} (${count.toLocaleString()})`
}

function formatLifecycleActor(value?: string | null): string {
  const normalized = String(value || '').trim()
  return normalized || 'Unknown'
}

function formatLifecycleTimestamp(value?: string | null): string {
  if (!value) return '—'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString()
}

function formatLifecycleAction(value?: string | null): string {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return 'Unknown'
  return normalized.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
}

function buildIncrementalityHref(experimentId: number): string {
  const params = new URLSearchParams()
  params.set('page', 'incrementality')
  params.set('experiment_id', String(experimentId))
  return `/?${params.toString()}`
}

function readNumericUrlParam(name: string): number | null {
  const raw = readParams().get(name)
  if (!raw) return null
  const parsed = Number(raw)
  return Number.isFinite(parsed) ? parsed : null
}

function channelFitsGroup(channel: string, group?: string | null): boolean {
  if (!group) return true
  const ch = channel.toLowerCase()
  if (group === 'paid') return ['paid', 'search', 'social', 'display', 'affiliate', 'ads'].some((tkn) => ch.includes(tkn))
  if (group === 'organic') return !['paid', 'search', 'social', 'display', 'affiliate', 'ads'].some((tkn) => ch.includes(tkn))
  return true
}

function pathChip(label: string) {
  return (
    <span
      key={label}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        padding: '4px 8px',
        borderRadius: t.radius.full,
        border: `1px solid ${t.color.borderLight}`,
        background: t.color.bg,
        fontSize: t.font.sizeXs,
        color: t.color.text,
      }}
    >
      {label}
    </span>
  )
}

function creditOverlay(channels: ChannelCredit[], group?: string | null) {
  const filtered = channels.filter((c) => channelFitsGroup(c.channel, group))
  const top = (filtered.length ? filtered : channels).slice(0, 3)
  const palette = ['#2563eb', '#0ea5e9', '#14b8a6']
  if (!top.length) return <span style={{ color: t.color.textMuted }}>—</span>
  return (
    <div style={{ display: 'grid', gap: 4 }}>
      <div style={{ display: 'flex', width: 120, height: 6, borderRadius: 999, overflow: 'hidden', background: t.color.bgSubtle }}>
        {top.map((c, idx) => (
          <span
            key={`${c.channel}-${idx}`}
            style={{
              width: `${Math.max(6, c.attributed_share * 100)}%`,
              background: palette[idx % palette.length],
            }}
          />
        ))}
      </div>
      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
        {top.map((c) => `${c.channel} ${formatPercent(c.attributed_share)}`).join(' · ')}
      </div>
    </div>
  )
}

function formatDateTime(value?: string | null): string {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleString()
}

function defaultHypothesisDraft(targetKpi = ''): HypothesisDraft {
  return {
    title: '',
    target_kpi: targetKpi,
    hypothesis_text: '',
    trigger: {},
    segment: {},
    current_action: {},
    proposed_action: {},
    support_count: 0,
    baseline_rate: null,
    sample_size_target: null,
    status: 'draft',
    result: {},
  }
}

function objectPreview(value: Record<string, unknown> | null | undefined): string {
  if (!value || typeof value !== 'object') return '—'
  const pairs = Object.entries(value).filter(([, item]) => item != null && item !== '')
  if (!pairs.length) return '—'
  return pairs
    .map(([key, item]) => {
      if (Array.isArray(item)) return `${key}: ${item.join(' → ')}`
      if (typeof item === 'object') return `${key}: ${Object.entries(item as Record<string, unknown>).filter(([, nested]) => nested != null && nested !== '').map(([nestedKey, nested]) => `${nestedKey}=${String(nested)}`).join(', ')}`
      return `${key}: ${String(item)}`
    })
    .join(' · ')
}

function readResultString(result: Record<string, unknown>, key: string): string | null {
  const value = result?.[key]
  return typeof value === 'string' && value ? value : null
}

function readResultNumber(result: Record<string, unknown>, key: string): number | null {
  const value = result?.[key]
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function readResultObject(result: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = result?.[key]
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
}

function readResultBool(result: Record<string, unknown>, key: string): boolean | null {
  const value = result?.[key]
  return typeof value === 'boolean' ? value : null
}

function getHypothesisLearningStage(item: JourneyHypothesisRecord): string {
  return readResultString(item.result || {}, 'learning_stage') || item.status || 'draft'
}

function formatStageLabel(stage: string): string {
  return stage.replace(/_/g, ' ')
}

function getHypothesisEvidenceSummary(item: JourneyHypothesisRecord): string | null {
  return readResultString(item.result || {}, 'summary') || readResultString(item.result || {}, 'note')
}

function getHypothesisPromotion(item: JourneyHypothesisRecord): Record<string, unknown> {
  return readResultObject(item.result || {}, 'policy_promotion')
}

function parseDelimitedSteps(value: string): string[] {
  return value
    .split(/\n|>/g)
    .map((part) => part.trim())
    .filter(Boolean)
}

function toDateInputValue(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function aggregateBreakdown(
  rows: JourneyPathRow[],
  key: 'device' | 'channel_group',
): Array<{ key: string; journeys: number; conversions: number; conversionRate: number }> {
  const buckets = new Map<string, { journeys: number; conversions: number }>()
  rows.forEach((row) => {
    const bucketKey = String(row[key] || 'unknown')
    const current = buckets.get(bucketKey) || { journeys: 0, conversions: 0 }
    current.journeys += row.count_journeys || 0
    current.conversions += row.count_conversions || 0
    buckets.set(bucketKey, current)
  })
  return [...buckets.entries()]
    .map(([bucketKey, value]) => ({
      key: bucketKey,
      journeys: value.journeys,
      conversions: value.conversions,
      conversionRate: value.journeys > 0 ? value.conversions / value.journeys : 0,
    }))
    .sort((a, b) => b.journeys - a.journeys)
}

function normalizeSavedView(record: SavedJourneyViewRecord): SavedJourneyView {
  return {
    id: record.id,
    name: record.name,
    selectedJourneyId: record.state?.selectedJourneyId || record.journey_definition_id || '',
    activeTab: record.state?.activeTab || 'paths',
    filters: {
      ...buildInitialFilters(null, null),
      ...(record.state?.filters ?? {}),
    },
    pathSortBy: record.state?.pathSortBy || 'journeys',
    pathSortDir: record.state?.pathSortDir || 'desc',
    pathsLimit: record.state?.pathsLimit || 50,
    examplesPathHash: record.state?.examplesPathHash || '',
    examplesStepFilter: record.state?.examplesStepFilter || '',
    createdAt: record.created_at || new Date(0).toISOString(),
  }
}

export default function Journeys({
  featureEnabled,
  hasPermission,
  canManageDefinitions,
  journeyExamplesEnabled,
  funnelBuilderEnabled,
}: JourneysProps) {
  const queryClient = useQueryClient()
  const { journeysSummary, attributionModel, setAttributionModel } = useWorkspaceContext()
  const user = useMemo(() => getUserContext(), [])
  const featureDisabled = !featureEnabled || !hasPermission

  const [filters, setFilters] = useState<GlobalFiltersState>(() => buildInitialFilters(null, null))
  const [activeTab, setActiveTab] = useState<JourneysTab>('paths')
  const [selectedJourneyId, setSelectedJourneyId] = useState('')
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [showEditModal, setShowEditModal] = useState(false)
  const [showCreateFunnelModal, setShowCreateFunnelModal] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)
  const [createFunnelError, setCreateFunnelError] = useState<string | null>(null)
  const [pathSearch, setPathSearch] = useState('')
  const [pathSortBy, setPathSortBy] = useState<PathSortBy>('journeys')
  const [pathSortDir, setPathSortDir] = useState<'asc' | 'desc'>('desc')
  const [pathsPage, setPathsPage] = useState(1)
  const [pathsLimit, setPathsLimit] = useState(50)
  const [selectedPath, setSelectedPath] = useState<JourneyPathRow | null>(null)
  const [comparePathHash, setComparePathHash] = useState('')
  const [creditExpanded, setCreditExpanded] = useState(false)
  const [selectedFunnelId, setSelectedFunnelId] = useState('')
  const [selectedFunnelStep, setSelectedFunnelStep] = useState<string | null>(null)
  const [flowMinCount, setFlowMinCount] = useState(5)
  const [flowMaxNodes, setFlowMaxNodes] = useState(20)
  const [flowMaxDepth, setFlowMaxDepth] = useState(5)
  const [examplesPathHash, setExamplesPathHash] = useState('')
  const [examplesStepFilter, setExamplesStepFilter] = useState('')
  const [savedViewName, setSavedViewName] = useState('')
  const [showCreateAlertModal, setShowCreateAlertModal] = useState(false)
  const [createAlertError, setCreateAlertError] = useState<string | null>(null)
  const [alertScope, setAlertScope] = useState<Record<string, unknown>>({})
  const [alertPreview, setAlertPreview] = useState<JourneyAlertPreviewResponse | null>(null)
  const [editingHypothesisId, setEditingHypothesisId] = useState<string | null>(null)
  const [hypothesisError, setHypothesisError] = useState<string | null>(null)
  const [hypothesisDraft, setHypothesisDraft] = useState<HypothesisDraft>(() => defaultHypothesisDraft())
  const [sandboxHypothesisId, setSandboxHypothesisId] = useState<string | null>(() => readParams().get('hypothesis_id') || null)
  const [sandboxCandidateStep, setSandboxCandidateStep] = useState('')
  const [selectedJourneyExperimentId, setSelectedJourneyExperimentId] = useState<number | null>(() => readNumericUrlParam('experiment_id'))
  const savedViewsSectionRef = useRef<HTMLDivElement | null>(null)
  const [hypothesisExperimentDraft, setHypothesisExperimentDraft] = useState<HypothesisExperimentDraft>(() => {
    const start = new Date()
    const end = new Date()
    end.setDate(end.getDate() + 14)
    return {
      start_at: toDateInputValue(start),
      end_at: toDateInputValue(end),
      notes: '',
    }
  })
  const [alertDraft, setAlertDraft] = useState<AlertDraft>({
    name: '',
    type: 'path_volume_change',
    domain: 'journeys',
    metric: 'count_journeys',
    comparison_mode: 'previous_period',
    threshold_pct: 25,
    severity: 'warn',
    cooldown_days: 2,
  })

  const [draft, setDraft] = useState<CreateJourneyDraft>({
    name: '',
    description: '',
    conversion_kpi_id: '',
    lookback_window_days: 30,
    mode_default: 'conversion_only',
  })
  const [funnelDraft, setFunnelDraft] = useState<CreateFunnelDraft>({
    name: '',
    description: '',
    stepsText: '',
    counting_method: 'ordered',
    window_days: 30,
  })
  const [showArchivedDefinitions, setShowArchivedDefinitions] = useState(false)

  const definitionsQuery = useQuery<PaginatedResponse<JourneyDefinition>>({
    queryKey: ['journey-definitions', 'journeys-page', showArchivedDefinitions ? 'with-archived' : 'active-only'],
    queryFn: async () => {
      const query = buildListQuery({
        page: 1,
        perPage: 100,
        order: 'desc',
      })
      if (showArchivedDefinitions) query.include_archived = 'true'
      return apiGetJson<PaginatedResponse<JourneyDefinition>>(withQuery('/api/journeys/definitions', query), {
        fallbackMessage: 'Failed to load journey definitions',
      })
    },
    enabled: !featureDisabled,
  })

  const kpisQuery = useQuery<KpiResponse>({
    queryKey: ['kpis', 'journeys-create-modal'],
    queryFn: async () => apiGetJson<KpiResponse>('/api/kpis', { fallbackMessage: 'Failed to load KPI definitions' }),
  })

  const selectedDefinition = useMemo(
    () => (definitionsQuery.data?.items ?? []).find((item) => item.id === selectedJourneyId) ?? null,
    [definitionsQuery.data?.items, selectedJourneyId],
  )
  const selectedDefinitionArchived = Boolean(selectedDefinition?.is_archived)
  const definitionWorkspaceReadOnly = featureDisabled || selectedDefinitionArchived

  const mode = selectedDefinition?.mode_default ?? 'conversion_only'

  const definitionLifecycleQuery = useQuery<JourneyDefinitionLifecycle>({
    queryKey: ['journey-definition-lifecycle', selectedJourneyId],
    queryFn: async () => {
      return apiGetJson<JourneyDefinitionLifecycle>(`/api/journeys/definitions/${selectedJourneyId}/lifecycle`, {
        fallbackMessage: 'Failed to load journey definition lifecycle',
      })
    },
    enabled: !!selectedJourneyId && !featureDisabled,
  })
  const definitionAuditQuery = useQuery<JourneyDefinitionAuditItem[]>({
    queryKey: ['journey-definition-audit', selectedJourneyId],
    queryFn: async () => {
      return apiGetJson<JourneyDefinitionAuditItem[]>(`/api/journeys/definitions/${selectedJourneyId}/audit?limit=20`, {
        fallbackMessage: 'Failed to load journey definition audit',
      })
    },
    enabled: !!selectedJourneyId && !featureDisabled,
  })
  const selectedDefinitionLifecycleStatus =
    definitionLifecycleQuery.data?.definition.lifecycle_status ?? (selectedDefinitionArchived ? 'archived' : 'active')
  const dimensionsQuery = useQuery<JourneyFilterDimensionsResponse>({
    queryKey: [
      'journey-dimensions',
      selectedJourneyId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<JourneyFilterDimensionsResponse>(`/api/journeys/${selectedJourneyId}/dimensions?${params.toString()}`, {
        fallbackMessage: 'Failed to load journey filter dimensions',
      })
    },
    enabled: !!selectedJourneyId && !featureDisabled,
  })
  const journeyFilterOptions = useMemo(() => {
    const dims = dimensionsQuery.data
    return {
      channels: (dims?.channels ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      campaigns: (dims?.campaigns ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      devices: (dims?.devices ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      geos: (dims?.countries ?? []).map((item) => ({ value: item.value, label: dimensionLabel(String(item.value).toUpperCase(), item.count) })),
    }
  }, [dimensionsQuery.data])

  function openAlertsForDefinition(definitionId: string) {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    params.set('page', 'alerts')
    params.set('alerts_tab', 'journey_alerts')
    params.set('journey_alert_domain', 'journeys')
    params.set('journey_definition_id', definitionId)
    window.history.pushState({}, '', `/?${params.toString()}`)
    window.dispatchEvent(new PopStateEvent('popstate'))
  }

  function openLifecycleDependency(target: 'saved_views' | 'funnels' | 'hypotheses' | 'experiments' | 'alerts' | 'journey_rows') {
    switch (target) {
      case 'saved_views':
        savedViewsSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
        break
      case 'funnels':
        setActiveTab('funnels')
        break
      case 'hypotheses':
        setActiveTab('hypotheses')
        break
      case 'experiments':
        setActiveTab('experiments')
        break
      case 'alerts':
        if (selectedDefinition?.id) openAlertsForDefinition(selectedDefinition.id)
        break
      case 'journey_rows':
        setActiveTab('paths')
        break
    }
  }

  const pathsQuery = useQuery<JourneyPathsResponse>({
    queryKey: [
      'journey-paths',
      selectedJourneyId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
      mode,
      pathsPage,
      pathsLimit,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        mode,
        page: String(pathsPage),
        limit: String(pathsLimit),
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<JourneyPathsResponse>(`/api/journeys/${selectedJourneyId}/paths?${params.toString()}`, {
        fallbackMessage: 'Failed to load path aggregates',
      })
    },
    enabled: !!selectedJourneyId && activeTab === 'paths' && !definitionWorkspaceReadOnly,
  })

  const attributionSummaryQuery = useQuery<AttributionSummaryResponse>({
    queryKey: [
      'journey-attribution-summary',
      selectedJourneyId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
      attributionModel,
      mode,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        model: attributionModel,
        mode,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<AttributionSummaryResponse>(`/api/journeys/${selectedJourneyId}/attribution-summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load attribution summary',
      })
    },
    enabled: !!selectedJourneyId && !definitionWorkspaceReadOnly,
  })

  const pathAttributionQuery = useQuery<AttributionSummaryResponse>({
    queryKey: [
      'journey-attribution-path',
      selectedJourneyId,
      selectedPath?.path_hash ?? '',
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
      attributionModel,
      mode,
    ],
    queryFn: async () => {
      if (!selectedPath) throw new Error('No path selected')
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        model: attributionModel,
        mode,
        path_hash: selectedPath.path_hash,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<AttributionSummaryResponse>(`/api/journeys/${selectedJourneyId}/attribution-summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load path credit split',
      })
    },
    enabled: !!selectedJourneyId && !!selectedPath && !definitionWorkspaceReadOnly,
  })

  const funnelListQuery = useQuery<FunnelListResponse>({
    queryKey: ['funnels-list', selectedJourneyId],
    queryFn: async () => {
      const params = new URLSearchParams({
        workspace_id: 'default',
      })
      if (selectedJourneyId) params.set('journey_definition_id', selectedJourneyId)
      return apiGetJson<FunnelListResponse>(`/api/funnels?${params.toString()}`, {
        fallbackMessage: 'Failed to load funnels',
      })
    },
    enabled: !!selectedJourneyId && funnelBuilderEnabled && !definitionWorkspaceReadOnly,
  })

  const funnelResultsQuery = useQuery<FunnelResultsResponse>({
    queryKey: [
      'funnel-results',
      selectedFunnelId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({ date_from: filters.dateFrom, date_to: filters.dateTo })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<FunnelResultsResponse>(`/api/funnels/${selectedFunnelId}/results?${params.toString()}`, {
        fallbackMessage: 'Failed to load funnel results',
      })
    },
    enabled: !!selectedFunnelId && activeTab === 'funnels' && funnelBuilderEnabled,
  })

  const funnelDiagnosticsQuery = useQuery<FunnelDiagnosticItem[]>({
    queryKey: [
      'funnel-diagnostics',
      selectedFunnelId,
      selectedFunnelStep,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
    ],
    queryFn: async () => {
      if (!selectedFunnelId || !selectedFunnelStep) return []
      const params = new URLSearchParams({
        step: selectedFunnelStep,
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<FunnelDiagnosticItem[]>(`/api/funnels/${selectedFunnelId}/diagnostics?${params.toString()}`, {
        fallbackMessage: 'Failed to load diagnostics',
      })
    },
    enabled: !!selectedFunnelId && !!selectedFunnelStep && activeTab === 'funnels' && funnelBuilderEnabled,
  })

  const transitionsQuery = useQuery<JourneyTransitionsResponse>({
    queryKey: [
      'journey-transitions',
      selectedJourneyId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
      mode,
      flowMinCount,
      flowMaxNodes,
      flowMaxDepth,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        mode,
        min_count: String(flowMinCount),
        max_nodes: String(flowMaxNodes),
        max_depth: String(flowMaxDepth),
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<JourneyTransitionsResponse>(`/api/journeys/${selectedJourneyId}/transitions?${params.toString()}`, {
        fallbackMessage: 'Failed to load flow transitions',
      })
    },
    enabled: !!selectedJourneyId && activeTab === 'flow' && !definitionWorkspaceReadOnly,
  })

  const examplesQuery = useQuery<JourneyExamplesResponse>({
    queryKey: [
      'journey-examples',
      selectedJourneyId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
      examplesPathHash,
      examplesStepFilter,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        limit: '12',
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      if (examplesPathHash) params.set('path_hash', examplesPathHash)
      if (examplesStepFilter.trim()) params.set('contains_step', examplesStepFilter.trim())
      return apiGetJson<JourneyExamplesResponse>(`/api/journeys/${selectedJourneyId}/examples?${params.toString()}`, {
        fallbackMessage: 'Failed to load journey examples',
      })
    },
    enabled: !!selectedJourneyId && activeTab === 'examples' && !definitionWorkspaceReadOnly,
  })

  const insightsQuery = useQuery<JourneyInsightsResponse>({
    queryKey: [
      'journey-insights',
      selectedJourneyId,
      filters.dateFrom,
      filters.dateTo,
      filters.channel,
      filters.campaign,
      filters.device,
      filters.geo,
      mode,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        mode,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<JourneyInsightsResponse>(`/api/journeys/${selectedJourneyId}/insights?${params.toString()}`, {
        fallbackMessage: 'Failed to load journey insights',
      })
    },
    enabled: !!selectedJourneyId && activeTab === 'insights' && !definitionWorkspaceReadOnly,
  })

  const hypothesesQuery = useQuery<JourneyHypothesesResponse>({
    queryKey: ['journey-hypotheses', selectedJourneyId],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (selectedJourneyId) params.set('journey_definition_id', selectedJourneyId)
      return apiGetJson<JourneyHypothesesResponse>(`/api/journeys/hypotheses?${params.toString()}`, {
        fallbackMessage: 'Failed to load journey hypotheses',
      })
    },
    enabled: !!selectedJourneyId && (activeTab === 'hypotheses' || activeTab === 'insights' || activeTab === 'policy' || activeTab === 'experiments') && !definitionWorkspaceReadOnly,
  })

  const linkedExperimentSourceIds = useMemo(
    () => Array.from(new Set((hypothesesQuery.data?.items ?? []).filter((item) => item.linked_experiment_id).map((item) => item.id))),
    [hypothesesQuery.data?.items],
  )

  const linkedExperimentsQuery = useQuery<LinkedExperimentSummary[]>({
    queryKey: ['journey-linked-experiments', selectedJourneyId, linkedExperimentSourceIds.join(',')],
    queryFn: async () => {
      const params = new URLSearchParams({ source_type: 'journey_hypothesis' })
      linkedExperimentSourceIds.forEach((id) => params.append('source_id', id))
      return apiGetJson<LinkedExperimentSummary[]>(`/api/experiments?${params.toString()}`, {
        fallbackMessage: 'Failed to load linked journey experiments',
      })
    },
    enabled: !!selectedJourneyId && activeTab === 'experiments' && linkedExperimentSourceIds.length > 0 && !definitionWorkspaceReadOnly,
  })

  const linkedExperimentDetailQuery = useQuery<LinkedExperimentDetail>({
    queryKey: ['journey-linked-experiment-detail', selectedJourneyExperimentId],
    queryFn: async () => {
      return apiGetJson<LinkedExperimentDetail>(`/api/experiments/${selectedJourneyExperimentId}`, {
        fallbackMessage: 'Failed to load journey experiment detail',
      })
    },
    enabled: activeTab === 'experiments' && selectedJourneyExperimentId != null && !definitionWorkspaceReadOnly,
  })

  const linkedExperimentResultsQuery = useQuery<LinkedExperimentResults>({
    queryKey: ['journey-linked-experiment-results', selectedJourneyExperimentId],
    queryFn: async () => {
      return apiGetJson<LinkedExperimentResults>(`/api/experiments/${selectedJourneyExperimentId}/results`, {
        fallbackMessage: 'Failed to load journey experiment results',
      })
    },
    enabled: activeTab === 'experiments' && selectedJourneyExperimentId != null && !definitionWorkspaceReadOnly,
  })

  const linkedExperimentHealthQuery = useQuery<LinkedExperimentHealth>({
    queryKey: ['journey-linked-experiment-health', selectedJourneyExperimentId],
    queryFn: async () => {
      return apiGetJson<LinkedExperimentHealth>(`/api/experiments/${selectedJourneyExperimentId}/health`, {
        fallbackMessage: 'Failed to load journey experiment health',
      })
    },
    enabled: activeTab === 'experiments' && selectedJourneyExperimentId != null && !definitionWorkspaceReadOnly,
  })

  const policySimulationQuery = useQuery<JourneyPolicySimulationResponse>({
    queryKey: ['journey-policy-simulation', sandboxHypothesisId, sandboxCandidateStep],
    queryFn: async () => {
      if (!sandboxHypothesisId) throw new Error('No hypothesis selected')
      return apiSendJson<JourneyPolicySimulationResponse>(
        `/api/journeys/hypotheses/${sandboxHypothesisId}/simulate`,
        'POST',
        { proposed_step: sandboxCandidateStep || null },
        { fallbackMessage: 'Failed to simulate journey policy' },
      )
    },
    enabled: !!sandboxHypothesisId && activeTab === 'policy' && !definitionWorkspaceReadOnly,
  })

  const policyRecommendationsQuery = useQuery<JourneyPolicyRecommendationsResponse>({
    queryKey: ['journey-policy-recommendations', selectedJourneyId],
    queryFn: async () => {
      return apiGetJson<JourneyPolicyRecommendationsResponse>(`/api/journeys/${selectedJourneyId}/policies?limit=10`, {
        fallbackMessage: 'Failed to load learned journey policies',
      })
    },
    enabled: !!selectedJourneyId && (activeTab === 'insights' || activeTab === 'policy' || activeTab === 'hypotheses' || activeTab === 'experiments') && !definitionWorkspaceReadOnly,
  })

  const savedViewsQuery = useQuery<SavedJourneyViewsResponse>({
    queryKey: ['journey-saved-views', user.userId],
    queryFn: async () => {
      return apiGetJson<SavedJourneyViewsResponse>('/api/journeys/views', {
        fallbackMessage: 'Failed to load saved journey views',
      })
    },
    enabled: !featureDisabled,
  })

  const createMutation = useMutation({
    mutationFn: async (payload: CreateJourneyDraft) => {
      return apiSendJson<JourneyDefinition>('/api/journeys/definitions', 'POST', payload, {
        fallbackMessage: 'Failed to create journey definition',
      })
    },
    onSuccess: async (created) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definitions', 'journeys-page'] })
      setSelectedJourneyId(created.id)
      setShowCreateModal(false)
      setCreateError(null)
      setDraft((prev) => ({ ...prev, name: '', description: '' }))
    },
    onError: (err) => setCreateError((err as Error).message || 'Failed to create journey'),
  })

  const updateMutation = useMutation({
    mutationFn: async (payload: { definitionId: string; body: CreateJourneyDraft }) => {
      return apiSendJson<JourneyDefinition>(`/api/journeys/definitions/${payload.definitionId}`, 'PUT', payload.body, {
        fallbackMessage: 'Failed to update journey definition',
      })
    },
    onSuccess: async (updated) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definitions', 'journeys-page'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-lifecycle'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-audit'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-dimensions'] })
      setSelectedJourneyId(updated.id)
      setShowEditModal(false)
      setCreateError(null)
    },
    onError: (err) => setCreateError((err as Error).message || 'Failed to update journey'),
  })

  const archiveDefinitionMutation = useMutation({
    mutationFn: async (definitionId: string) => {
      return apiSendJson<{ id: string; status: string }>(`/api/journeys/definitions/${definitionId}/archive`, 'POST', undefined, {
        fallbackMessage: 'Failed to archive journey definition',
      })
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definitions', 'journeys-page'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-lifecycle'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-audit'] })
    },
  })

  const restoreDefinitionMutation = useMutation({
    mutationFn: async (definitionId: string) => {
      return apiSendJson<JourneyDefinition>(`/api/journeys/definitions/${definitionId}/restore`, 'POST', undefined, {
        fallbackMessage: 'Failed to restore journey definition',
      })
    },
    onSuccess: async (restored) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definitions', 'journeys-page'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-lifecycle'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-audit'] })
      setSelectedJourneyId(restored.id)
    },
  })

  const duplicateDefinitionMutation = useMutation({
    mutationFn: async (payload: { definitionId: string; name?: string }) => {
      return apiSendJson<JourneyDefinition>(`/api/journeys/definitions/${payload.definitionId}/duplicate`, 'POST', {
        name: payload.name,
      }, {
        fallbackMessage: 'Failed to duplicate journey definition',
      })
    },
    onSuccess: async (created) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definitions', 'journeys-page'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-lifecycle'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-audit'] })
      setSelectedJourneyId(created.id)
    },
  })

  const hardDeleteDefinitionMutation = useMutation({
    mutationFn: async (definitionId: string) => {
      return apiSendJson<{ id: string; status: string }>(`/api/journeys/definitions/${definitionId}/hard-delete`, 'POST', undefined, {
        fallbackMessage: 'Failed to delete journey definition permanently',
      })
    },
    onSuccess: async (_, definitionId) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definitions', 'journeys-page'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-lifecycle'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-audit'] })
      if (selectedJourneyId === definitionId) setSelectedJourneyId('')
    },
  })

  const rebuildDefinitionMutation = useMutation({
    mutationFn: async (payload: { definitionId: string; reprocessDays?: number }) => {
      const query = new URLSearchParams()
      if (payload.reprocessDays) query.set('reprocess_days', String(payload.reprocessDays))
      return apiSendJson<{ definition_id: string; metrics: Record<string, unknown> }>(
        `/api/journeys/definitions/${payload.definitionId}/rebuild${query.toString() ? `?${query.toString()}` : ''}`,
        'POST',
        undefined,
        { fallbackMessage: 'Failed to rebuild journey definition' },
      )
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-lifecycle'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-definition-audit'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-paths'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-attribution-summary'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-transitions'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-examples'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-insights'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-hypotheses'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-policy-recommendations'] })
    },
  })

  const createFunnelMutation = useMutation({
    mutationFn: async (payload: { name: string; description: string; steps: string[]; window_days: number }) => {
      return apiSendJson<FunnelDefinition>('/api/funnels', 'POST', {
        journey_definition_id: selectedJourneyId,
        workspace_id: 'default',
        name: payload.name,
        description: payload.description,
        steps: payload.steps,
        counting_method: 'ordered',
        window_days: payload.window_days,
      }, {
        fallbackMessage: 'Failed to create funnel',
      })
    },
    onSuccess: async (created) => {
      await queryClient.invalidateQueries({ queryKey: ['funnels-list'] })
      setSelectedFunnelId(created.id)
      setShowCreateFunnelModal(false)
      setCreateFunnelError(null)
      setActiveTab('funnels')
    },
    onError: (err) => setCreateFunnelError((err as Error).message || 'Failed to create funnel'),
  })

  const createAlertMutation = useMutation({
    mutationFn: async (payload: JourneyAlertCreatePayload) => {
      return createJourneyAlert(payload)
    },
    onSuccess: async () => {
      setShowCreateAlertModal(false)
      setCreateAlertError(null)
      setAlertPreview(null)
      await queryClient.invalidateQueries({ queryKey: ['alerts-list'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-alert-definitions'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-alert-events'] })
      await queryClient.invalidateQueries({ queryKey: ['journey-alerts-main-rows'] })
    },
    onError: (err) => setCreateAlertError((err as Error).message || 'Failed to create alert'),
  })

  const previewAlertMutation = useMutation({
    mutationFn: async (payload: { type: AlertDraft['type']; scope: Record<string, unknown>; metric: string; condition: Record<string, unknown> }) => {
      return previewJourneyAlert(payload)
    },
    onSuccess: (data) => setAlertPreview(data),
    onError: (err) => setCreateAlertError((err as Error).message || 'Failed to preview alert'),
  })

  const createSavedViewMutation = useMutation({
    mutationFn: async (payload: { name: string; journey_definition_id: string; state: Record<string, unknown> }) => {
      return apiSendJson<SavedJourneyViewRecord>('/api/journeys/views', 'POST', payload, {
        fallbackMessage: 'Failed to save journey view',
      })
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['journey-saved-views', user.userId] })
      setSavedViewName('')
    },
  })

  const createHypothesisMutation = useMutation({
    mutationFn: async (payload: JourneyHypothesisRequest) => {
      return apiSendJson<JourneyHypothesisRecord>('/api/journeys/hypotheses', 'POST', payload, {
        fallbackMessage: 'Failed to create journey hypothesis',
      })
    },
    onSuccess: async (created) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-hypotheses', selectedJourneyId] })
      await queryClient.invalidateQueries({ queryKey: ['journey-policy-recommendations', selectedJourneyId] })
      setEditingHypothesisId(created.id)
      setHypothesisDraft({
        title: created.title,
        target_kpi: created.target_kpi || '',
        hypothesis_text: created.hypothesis_text,
        trigger: created.trigger || {},
        segment: created.segment || {},
        current_action: created.current_action || {},
        proposed_action: created.proposed_action || {},
        support_count: created.support_count || 0,
        baseline_rate: created.baseline_rate ?? null,
        sample_size_target: created.sample_size_target ?? null,
        status: created.status || 'draft',
        result: created.result || {},
      })
      setHypothesisError(null)
    },
    onError: (err) => setHypothesisError((err as Error).message || 'Failed to create journey hypothesis'),
  })

  const updateHypothesisMutation = useMutation({
    mutationFn: async (payload: { id: string; body: JourneyHypothesisRequest }) => {
      return apiSendJson<JourneyHypothesisRecord>(`/api/journeys/hypotheses/${payload.id}`, 'PUT', payload.body, {
        fallbackMessage: 'Failed to update journey hypothesis',
      })
    },
    onSuccess: async (updated) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-hypotheses', selectedJourneyId] })
      await queryClient.invalidateQueries({ queryKey: ['journey-policy-recommendations', selectedJourneyId] })
      setEditingHypothesisId(updated.id)
      setHypothesisDraft({
        title: updated.title,
        target_kpi: updated.target_kpi || '',
        hypothesis_text: updated.hypothesis_text,
        trigger: updated.trigger || {},
        segment: updated.segment || {},
        current_action: updated.current_action || {},
        proposed_action: updated.proposed_action || {},
        support_count: updated.support_count || 0,
        baseline_rate: updated.baseline_rate ?? null,
        sample_size_target: updated.sample_size_target ?? null,
        status: updated.status || 'draft',
        result: updated.result || {},
      })
      setHypothesisError(null)
    },
    onError: (err) => setHypothesisError((err as Error).message || 'Failed to update journey hypothesis'),
  })

  const createExperimentFromHypothesisMutation = useMutation({
    mutationFn: async (payload: { hypothesisId: string; start_at: string; end_at: string; notes: string; proposed_step: string }) => {
      return apiSendJson<JourneyHypothesisExperimentLinkResponse>(
        `/api/journeys/hypotheses/${payload.hypothesisId}/create-experiment`,
        'POST',
        {
          start_at: new Date(`${payload.start_at}T00:00:00`).toISOString(),
          end_at: new Date(`${payload.end_at}T23:59:59`).toISOString(),
          notes: payload.notes || null,
          proposed_step: payload.proposed_step || null,
        },
        { fallbackMessage: 'Failed to create experiment from hypothesis' },
      )
    },
    onSuccess: async (payload) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-hypotheses', selectedJourneyId] })
      await queryClient.invalidateQueries({ queryKey: ['journey-policy-recommendations', selectedJourneyId] })
      setEditingHypothesisId(payload.hypothesis.id)
      setHypothesisDraft({
        title: payload.hypothesis.title,
        target_kpi: payload.hypothesis.target_kpi || '',
        hypothesis_text: payload.hypothesis.hypothesis_text,
        trigger: payload.hypothesis.trigger || {},
        segment: payload.hypothesis.segment || {},
        current_action: payload.hypothesis.current_action || {},
        proposed_action: payload.hypothesis.proposed_action || {},
        support_count: payload.hypothesis.support_count || 0,
        baseline_rate: payload.hypothesis.baseline_rate ?? null,
        sample_size_target: payload.hypothesis.sample_size_target ?? null,
        status: payload.hypothesis.status || 'in_experiment',
        result: payload.hypothesis.result || {},
      })
      setHypothesisError(null)
    },
    onError: (err) => setHypothesisError((err as Error).message || 'Failed to create experiment from hypothesis'),
  })

  const promoteHypothesisPolicyMutation = useMutation({
    mutationFn: async (payload: { hypothesisId: string; active: boolean }) => {
      return apiSendJson<JourneyPolicyPromotionResponse>(
        `/api/journeys/hypotheses/${payload.hypothesisId}/policy-promotion`,
        'POST',
        {
          active: payload.active,
          notes: payload.active ? 'Promoted from the Journey Lab learned policy workflow.' : 'Removed from the Journey Lab learned policy workflow.',
        },
        { fallbackMessage: payload.active ? 'Failed to promote journey policy' : 'Failed to remove journey policy promotion' },
      )
    },
    onSuccess: async (payload) => {
      await queryClient.invalidateQueries({ queryKey: ['journey-hypotheses', selectedJourneyId] })
      await queryClient.invalidateQueries({ queryKey: ['journey-policy-recommendations', selectedJourneyId] })
      if (editingHypothesisId === payload.hypothesis.id) {
        setHypothesisDraft({
          title: payload.hypothesis.title,
          target_kpi: payload.hypothesis.target_kpi || '',
          hypothesis_text: payload.hypothesis.hypothesis_text,
          trigger: payload.hypothesis.trigger || {},
          segment: payload.hypothesis.segment || {},
          current_action: payload.hypothesis.current_action || {},
          proposed_action: payload.hypothesis.proposed_action || {},
          support_count: payload.hypothesis.support_count || 0,
          baseline_rate: payload.hypothesis.baseline_rate ?? null,
          sample_size_target: payload.hypothesis.sample_size_target ?? null,
          status: payload.hypothesis.status || 'draft',
          result: payload.hypothesis.result || {},
        })
      }
      setHypothesisError(null)
    },
    onError: (err) => setHypothesisError((err as Error).message || 'Failed to update journey policy promotion'),
  })

  const deleteSavedViewMutation = useMutation({
    mutationFn: async (viewId: string) => {
      return apiSendJson<{ id: string; status: string }>(`/api/journeys/views/${viewId}`, 'DELETE', undefined, {
        fallbackMessage: 'Failed to delete journey view',
      })
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['journey-saved-views', user.userId] })
    },
  })

  useEffect(() => {
    const params = readParams()
    setSelectedJourneyId(params.get('journey_id') || '')
    const tabParam = params.get('tab')
    if (tabParam === 'insights' || tabParam === 'hypotheses' || tabParam === 'policy' || tabParam === 'experiments' || tabParam === 'paths' || tabParam === 'flow' || tabParam === 'examples' || tabParam === 'funnels') {
      setActiveTab(tabParam)
    }
    setExamplesPathHash(params.get('examples_path_hash') || '')
    setExamplesStepFilter(params.get('examples_step') || '')
    setSandboxHypothesisId(params.get('hypothesis_id') || null)
    setSelectedJourneyExperimentId(readNumericUrlParam('experiment_id'))
    setFilters((prev) => ({
      ...prev,
      dateFrom: params.get('date_from') || prev.dateFrom,
      dateTo: params.get('date_to') || prev.dateTo,
      channel: params.get('channel') || prev.channel,
      campaign: params.get('campaign') || prev.campaign,
      device: params.get('device') || prev.device,
      geo: params.get('geo') || prev.geo,
      segment: 'all',
    }))
  }, [])

  useEffect(() => {
    const primary = kpisQuery.data?.primary_kpi_id || kpisQuery.data?.definitions?.[0]?.id || ''
    if (primary && !draft.conversion_kpi_id) {
      setDraft((prev) => ({ ...prev, conversion_kpi_id: primary }))
    }
  }, [draft.conversion_kpi_id, kpisQuery.data?.definitions, kpisQuery.data?.primary_kpi_id])

  useEffect(() => {
    if (editingHypothesisId) return
    setHypothesisDraft((prev) => {
      if (prev.title || prev.hypothesis_text || prev.support_count > 0) return prev
      return defaultHypothesisDraft(selectedDefinition?.conversion_kpi_id || '')
    })
  }, [editingHypothesisId, selectedDefinition?.conversion_kpi_id])

  useEffect(() => {
    setFilters((prev) => ({
      ...prev,
      dateFrom: journeysSummary?.date_min?.slice(0, 10) ?? prev.dateFrom,
      dateTo: journeysSummary?.date_max?.slice(0, 10) ?? prev.dateTo,
    }))
  }, [journeysSummary?.date_min, journeysSummary?.date_max])

  useEffect(() => {
    const defs = definitionsQuery.data?.items ?? []
    if (!defs.length) return
    if (!selectedJourneyId || !defs.some((d) => d.id === selectedJourneyId)) {
      const fromUrl = readParams().get('journey_id')
      const fallback = fromUrl && defs.some((d) => d.id === fromUrl) ? fromUrl : defs[0].id
      setSelectedJourneyId(fallback)
    }
  }, [definitionsQuery.data?.items, selectedJourneyId])

  useEffect(() => {
    const dims = dimensionsQuery.data
    if (!dims) return
    setFilters((prev) => {
      const next = { ...prev }
      const validChannels = new Set((dims.channels ?? []).map((item) => item.value))
      const validCampaigns = new Set((dims.campaigns ?? []).map((item) => item.value))
      const validDevices = new Set((dims.devices ?? []).map((item) => item.value))
      const validGeos = new Set((dims.countries ?? []).map((item) => item.value))
      if (next.channel !== 'all' && !validChannels.has(next.channel)) next.channel = 'all'
      if (next.campaign !== 'all' && !validCampaigns.has(next.campaign)) next.campaign = 'all'
      if (next.device !== 'all' && !validDevices.has(next.device)) next.device = 'all'
      if (next.geo !== 'all' && !validGeos.has(next.geo.toLowerCase())) next.geo = 'all'
      next.segment = 'all'
      if (
        next.channel === prev.channel &&
        next.campaign === prev.campaign &&
        next.device === prev.device &&
        next.geo === prev.geo &&
        next.segment === prev.segment
      ) {
        return prev
      }
      return next
    })
  }, [dimensionsQuery.data])

  useEffect(() => {
    setEditingHypothesisId(null)
    setHypothesisError(null)
    setHypothesisDraft(defaultHypothesisDraft(selectedDefinition?.conversion_kpi_id || ''))
  }, [selectedJourneyId, selectedDefinition?.conversion_kpi_id])

  useEffect(() => {
    const items = hypothesesQuery.data?.items ?? []
    if (!items.length) {
      setSandboxHypothesisId(null)
      setSandboxCandidateStep('')
      return
    }
    if (!sandboxHypothesisId || !items.some((item) => item.id === sandboxHypothesisId)) {
      setSandboxHypothesisId(items[0].id)
      setSandboxCandidateStep('')
    }
  }, [hypothesesQuery.data?.items, sandboxHypothesisId])

  useEffect(() => {
    const items = linkedExperimentsQuery.data ?? []
    if (!items.length) {
      setSelectedJourneyExperimentId(null)
      return
    }
    if (!selectedJourneyExperimentId || !items.some((item) => item.id === selectedJourneyExperimentId)) {
      setSelectedJourneyExperimentId(items[0].id)
    }
  }, [linkedExperimentsQuery.data, selectedJourneyExperimentId])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = readParams()
    if (selectedJourneyId) params.set('journey_id', selectedJourneyId)
    else params.delete('journey_id')
    params.set('tab', activeTab)
    params.set('date_from', filters.dateFrom)
    params.set('date_to', filters.dateTo)
    params.set('channel', filters.channel)
    params.set('campaign', filters.campaign)
    params.set('device', filters.device)
    params.set('geo', filters.geo)
    params.delete('segment')
    if (examplesPathHash) params.set('examples_path_hash', examplesPathHash)
    else params.delete('examples_path_hash')
    if (examplesStepFilter.trim()) params.set('examples_step', examplesStepFilter.trim())
    else params.delete('examples_step')
    if (activeTab === 'policy' && sandboxHypothesisId) params.set('hypothesis_id', sandboxHypothesisId)
    else if (activeTab !== 'experiments') params.delete('hypothesis_id')
    if (activeTab === 'experiments' && selectedJourneyExperimentId != null) params.set('experiment_id', String(selectedJourneyExperimentId))
    else params.delete('experiment_id')
    window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`)
  }, [activeTab, examplesPathHash, examplesStepFilter, filters, sandboxHypothesisId, selectedJourneyExperimentId, selectedJourneyId])

  useEffect(() => {
    setPathsPage(1)
  }, [selectedJourneyId, filters.dateFrom, filters.dateTo, filters.channel, filters.campaign, filters.device, filters.geo])

  useEffect(() => {
    const items = funnelListQuery.data?.items ?? []
    if (!items.length) {
      setSelectedFunnelId('')
      setSelectedFunnelStep(null)
      return
    }
    if (!selectedFunnelId || !items.some((f) => f.id === selectedFunnelId)) {
      setSelectedFunnelId(items[0].id)
      setSelectedFunnelStep(null)
    }
  }, [funnelListQuery.data?.items, selectedFunnelId])

  useEffect(() => {
    setComparePathHash('')
    setCreditExpanded(false)
  }, [selectedPath?.path_hash])

  const definitions = definitionsQuery.data?.items ?? []
  const kpiOptions = kpisQuery.data?.definitions ?? []
  const journeyOptions = definitions.map((item) => ({
    value: item.id,
    label: item.is_archived ? `${item.name} (archived)` : item.name,
  }))

  const tabs = useMemo(
    () =>
      [
        { key: 'insights' as JourneysTab, label: 'Insights', visible: true, disabled: false },
        { key: 'hypotheses' as JourneysTab, label: 'Hypotheses', visible: true, disabled: false },
        { key: 'policy' as JourneysTab, label: 'Policy Sandbox', visible: true, disabled: false },
        { key: 'experiments' as JourneysTab, label: 'Experiments', visible: true, disabled: false },
        { key: 'paths' as JourneysTab, label: 'Paths', visible: true, disabled: false },
        { key: 'flow' as JourneysTab, label: 'Flow', visible: true, disabled: false },
        { key: 'examples' as JourneysTab, label: 'Examples', visible: journeyExamplesEnabled, disabled: false },
        { key: 'funnels' as JourneysTab, label: 'Funnels', visible: funnelBuilderEnabled, disabled: false },
      ].filter((tab) => tab.visible),
    [journeyExamplesEnabled, funnelBuilderEnabled],
  )

  const filteredPaths = useMemo(() => {
    const items = pathsQuery.data?.items ?? []
    const q = pathSearch.trim().toLowerCase()
    const base = q ? items.filter((row) => normalizeSteps(row.path_steps).join(' > ').toLowerCase().includes(q)) : items
    return [...base].sort((a, b) => {
      const av = pathSortBy === 'journeys' ? a.count_journeys : pathSortBy === 'conversion_rate' ? a.conversion_rate : a.avg_time_to_convert_sec ?? 0
      const bv = pathSortBy === 'journeys' ? b.count_journeys : pathSortBy === 'conversion_rate' ? b.conversion_rate : b.avg_time_to_convert_sec ?? 0
      return pathSortDir === 'asc' ? av - bv : bv - av
    })
  }, [pathSearch, pathSortBy, pathSortDir, pathsQuery.data?.items])

  const submitCreate = () => {
    if (!draft.name.trim()) {
      setCreateError('Journey name is required')
      return
    }
    createMutation.mutate({
      ...draft,
      name: draft.name.trim(),
      description: draft.description.trim(),
      lookback_window_days: clampLookback(draft.lookback_window_days),
      conversion_kpi_id: draft.conversion_kpi_id || '',
    })
  }

  const submitEdit = () => {
    if (!selectedDefinition) return
    if (!draft.name.trim()) {
      setCreateError('Journey name is required')
      return
    }
    updateMutation.mutate({
      definitionId: selectedDefinition.id,
      body: {
        ...draft,
        name: draft.name.trim(),
        description: draft.description.trim(),
        lookback_window_days: clampLookback(draft.lookback_window_days),
        conversion_kpi_id: draft.conversion_kpi_id || '',
      },
    })
  }

  const submitCreateFunnel = () => {
    const steps = parseStepsText(funnelDraft.stepsText)
    if (!funnelDraft.name.trim()) {
      setCreateFunnelError('Funnel name is required')
      return
    }
    if (steps.length < 2) {
      setCreateFunnelError('At least 2 steps are required')
      return
    }
    createFunnelMutation.mutate({
      name: funnelDraft.name.trim(),
      description: funnelDraft.description.trim(),
      steps,
      window_days: clampLookback(funnelDraft.window_days),
    })
  }

  const totalPaths = pathsQuery.data?.total ?? 0
  const maxPage = Math.max(1, Math.ceil(totalPaths / pathsLimit))
  const globalCredits = attributionSummaryQuery.data?.by_channel ?? []
  const drawerCredits = (pathAttributionQuery.data?.by_channel ?? globalCredits).sort((a, b) => b.attributed_share - a.attributed_share)
  const drawerTop = creditExpanded ? drawerCredits : drawerCredits.slice(0, 3)
  const samePathRows = useMemo(
    () => (selectedPath ? (pathsQuery.data?.items ?? []).filter((row) => row.path_hash === selectedPath.path_hash) : []),
    [pathsQuery.data?.items, selectedPath],
  )
  const pathBreakdownByDevice = useMemo(() => aggregateBreakdown(samePathRows, 'device'), [samePathRows])
  const pathBreakdownByChannelGroup = useMemo(() => aggregateBreakdown(samePathRows, 'channel_group'), [samePathRows])
  const compareCandidateOptions = useMemo(
    () => {
      const seen = new Set<string>()
      return filteredPaths.filter((row) => {
        if (row.path_hash === selectedPath?.path_hash) return false
        if (seen.has(row.path_hash)) return false
        seen.add(row.path_hash)
        return true
      })
    },
    [filteredPaths, selectedPath?.path_hash],
  )
  const comparedPath = useMemo(
    () => compareCandidateOptions.find((row) => row.path_hash === comparePathHash) || null,
    [compareCandidateOptions, comparePathHash],
  )
  const savedViews = useMemo(
    () => (savedViewsQuery.data?.items ?? []).map((item) => normalizeSavedView(item)),
    [savedViewsQuery.data?.items],
  )
  const flowTopEdges = useMemo(
    () => (transitionsQuery.data?.edges ?? []).slice(0, 12),
    [transitionsQuery.data?.edges],
  )
  const pathTableColumns: AnalyticsTableColumn<JourneyPathRow>[] = [
    {
      key: 'path_steps',
      label: 'Path steps',
      hideable: false,
      render: (row) => (
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {normalizeSteps(row.path_steps).map((step) => pathChip(step))}
        </div>
      ),
      cellStyle: { fontWeight: t.font.weightMedium },
    },
    {
      key: 'count_journeys',
      label: 'Journeys',
      align: 'right',
      render: (row) => row.count_journeys.toLocaleString(),
    },
    {
      key: 'conversion_rate',
      label: 'Conv. rate',
      align: 'right',
      render: (row) => formatPercent(row.conversion_rate),
    },
    {
      key: 'avg_time_to_convert_sec',
      label: 'Avg',
      align: 'right',
      render: (row) => formatSeconds(row.avg_time_to_convert_sec),
    },
    {
      key: 'p50_time_to_convert_sec',
      label: 'P50',
      align: 'right',
      render: (row) => formatSeconds(row.p50_time_to_convert_sec),
    },
    {
      key: 'p90_time_to_convert_sec',
      label: 'P90',
      align: 'right',
      render: (row) => formatSeconds(row.p90_time_to_convert_sec),
    },
    {
      key: 'credit_overlay',
      label: 'Credit overlay',
      render: (row) => creditOverlay(globalCredits, row.channel_group),
    },
  ]

  const hypotheses = hypothesesQuery.data?.items ?? []
  const linkedExperiments = linkedExperimentsQuery.data ?? []
  const insightItems = insightsQuery.data?.items ?? []
  const policyRecommendations = policyRecommendationsQuery.data?.items ?? []
  const hypothesisResultNote = typeof hypothesisDraft.result?.['note'] === 'string' ? String(hypothesisDraft.result['note']) : ''
  const activeHypothesis = editingHypothesisId ? hypotheses.find((item) => item.id === editingHypothesisId) || null : null
  const sandboxHypothesis = sandboxHypothesisId ? hypotheses.find((item) => item.id === sandboxHypothesisId) || null : null
  const experimentHypothesisById = useMemo(
    () =>
      new Map(
        hypotheses
          .filter((item) => item.linked_experiment_id != null)
          .map((item) => [Number(item.linked_experiment_id), item] as const),
      ),
    [hypotheses],
  )
  const selectedJourneyExperiment = selectedJourneyExperimentId != null ? linkedExperiments.find((item) => item.id === selectedJourneyExperimentId) || null : null
  const selectedJourneyExperimentHypothesis = selectedJourneyExperimentId != null ? experimentHypothesisById.get(selectedJourneyExperimentId) || null : null
  const linkedExperimentCounts = useMemo(
    () =>
      linkedExperiments.reduce(
        (acc, item) => {
          acc.total += 1
          acc[item.status] = (acc[item.status] || 0) + 1
          return acc
        },
        { total: 0, draft: 0, running: 0, completed: 0 } as Record<string, number>,
      ),
    [linkedExperiments],
  )
  const hypothesisLearningCounts = useMemo(() => {
    return hypotheses.reduce<Record<string, number>>((acc, item) => {
      const key = getHypothesisLearningStage(item)
      acc[key] = (acc[key] || 0) + 1
      return acc
    }, {})
  }, [hypotheses])
  const latestLearnedHypotheses = useMemo(
    () =>
      hypotheses.filter((item) => ['validated', 'rejected', 'inconclusive'].includes(getHypothesisLearningStage(item))).slice(0, 3),
    [hypotheses],
  )
  const topPromotablePolicies = useMemo(
    () => policyRecommendations.filter((item) => item.recommendation === 'promote' || item.recommendation === 'promoted').slice(0, 4),
    [policyRecommendations],
  )

  const renderHypothesisLearningCard = (item: JourneyHypothesisRecord, compact = false) => {
    const stage = getHypothesisLearningStage(item)
    const summary = getHypothesisEvidenceSummary(item)
    const experimentStatus = readResultString(item.result || {}, 'experiment_status')
    const experimentName = readResultString(item.result || {}, 'experiment_name')
    const upliftAbs = readResultNumber(item.result || {}, 'uplift_abs')
    const pValue = readResultNumber(item.result || {}, 'p_value')
    const treatment = readResultObject(item.result || {}, 'treatment')
    const control = readResultObject(item.result || {}, 'control')
    const promotion = getHypothesisPromotion(item)
    const stageColor =
      stage === 'validated'
        ? t.color.success
        : stage === 'rejected'
        ? t.color.danger
        : stage === 'inconclusive'
        ? t.color.warning
        : t.color.textMuted

    return (
      <div
        style={{
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.sm,
          padding: compact ? '10px 12px' : t.space.sm,
          background: compact ? t.color.surface : t.color.bg,
          display: 'grid',
          gap: 6,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
          <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
            <span
              style={{
                border: `1px solid ${stageColor}`,
                color: stageColor,
                borderRadius: t.radius.full,
                padding: '4px 8px',
                fontSize: t.font.sizeXs,
                textTransform: 'capitalize',
              }}
            >
              {formatStageLabel(stage)}
            </span>
            {experimentStatus ? (
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'capitalize' }}>
                experiment {formatStageLabel(experimentStatus)}
              </span>
            ) : null}
            {readResultBool(promotion, 'active') ? (
              <span
                style={{
                  border: `1px solid ${t.color.accent}`,
                  color: t.color.accent,
                  borderRadius: t.radius.full,
                  padding: '4px 8px',
                  fontSize: t.font.sizeXs,
                }}
              >
                Promoted
              </span>
            ) : null}
          </div>
          {item.linked_experiment_id ? (
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                #{item.linked_experiment_id}
                {experimentName ? ` · ${experimentName}` : ''}
              </div>
              <a
                href={buildIncrementalityHref(item.linked_experiment_id)}
                style={{ fontSize: t.font.sizeXs, color: t.color.accent, textDecoration: 'none' }}
              >
                Open in Incrementality
              </a>
            </div>
          ) : null}
        </div>
        {summary ? <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{summary}</div> : null}
        {upliftAbs != null || pValue != null ? (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            {upliftAbs != null ? `Observed uplift ${formatPercent(upliftAbs)}` : 'Observed uplift —'}
            {pValue != null ? ` · p=${pValue.toFixed(3)}` : ''}
            {typeof treatment.n === 'number' && typeof control.n === 'number'
              ? ` · ${Number(treatment.n).toLocaleString()} treatment / ${Number(control.n).toLocaleString()} control`
              : ''}
          </div>
        ) : null}
      </div>
    )
  }

  const renderPolicyRecommendationCard = (item: JourneyPolicyRecommendationRecord) => {
    const recommendationColor =
      item.recommendation === 'promote' || item.recommendation === 'promoted'
        ? t.color.success
        : item.recommendation === 'avoid'
        ? t.color.danger
        : t.color.warning
    return (
      <div
        key={item.hypothesis_id}
        style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface, display: 'grid', gap: 8 }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'start', flexWrap: 'wrap' }}>
          <div style={{ display: 'grid', gap: 4 }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightSemibold }}>{item.title}</div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              {item.prefix.label} · {item.policy.step} · score {item.score.toFixed(1)}
            </div>
          </div>
          <span
            style={{
              border: `1px solid ${recommendationColor}`,
              color: recommendationColor,
              borderRadius: t.radius.full,
              padding: '4px 8px',
              fontSize: t.font.sizeXs,
              textTransform: 'capitalize',
            }}
          >
            {formatStageLabel(item.recommendation)}
          </span>
        </div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          {item.summary || item.recommendation_reason}
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          {item.segment_label} · support {item.support_count.toLocaleString()}
          {typeof item.uplift_abs === 'number' ? ` · uplift ${formatPercent(item.uplift_abs)}` : ''}
          {typeof item.p_value === 'number' ? ` · p=${item.p_value.toFixed(3)}` : ''}
        </div>
        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => {
              const linked = hypotheses.find((hypothesis) => hypothesis.id === item.hypothesis_id)
              if (linked) {
                setSandboxHypothesisId(linked.id)
                setSandboxCandidateStep(item.policy.step || '')
              }
              setActiveTab('policy')
            }}
            style={{
              border: `1px solid ${t.color.border}`,
              background: t.color.surface,
              color: t.color.text,
              borderRadius: t.radius.sm,
              padding: '6px 10px',
              fontSize: t.font.sizeSm,
              cursor: 'pointer',
            }}
          >
            Open in sandbox
          </button>
          {item.recommendation === 'promote' || item.recommendation === 'promoted' ? (
            <button
              type="button"
              onClick={() => promoteHypothesisPolicyMutation.mutate({ hypothesisId: item.hypothesis_id, active: item.recommendation !== 'promoted' })}
              disabled={promoteHypothesisPolicyMutation.isPending}
              style={{
                border: `1px solid ${item.recommendation === 'promoted' ? t.color.border : t.color.accent}`,
                background: item.recommendation === 'promoted' ? t.color.surface : t.color.accent,
                color: item.recommendation === 'promoted' ? t.color.text : '#fff',
                borderRadius: t.radius.sm,
                padding: '6px 10px',
                fontSize: t.font.sizeSm,
                cursor: promoteHypothesisPolicyMutation.isPending ? 'wait' : 'pointer',
                opacity: promoteHypothesisPolicyMutation.isPending ? 0.8 : 1,
              }}
            >
              {item.recommendation === 'promoted' ? 'Remove promotion' : 'Promote policy'}
            </button>
          ) : null}
        </div>
      </div>
    )
  }

  const resetHypothesisDraft = () => {
    setEditingHypothesisId(null)
    setHypothesisError(null)
    setHypothesisDraft(defaultHypothesisDraft(selectedDefinition?.conversion_kpi_id || ''))
  }

  const loadHypothesisIntoDraft = (item: JourneyHypothesisRecord) => {
    setEditingHypothesisId(item.id)
    setHypothesisError(null)
    setActiveTab('hypotheses')
    setHypothesisDraft({
      title: item.title,
      target_kpi: item.target_kpi || selectedDefinition?.conversion_kpi_id || '',
      hypothesis_text: item.hypothesis_text,
      trigger: item.trigger || {},
      segment: item.segment || {},
      current_action: item.current_action || {},
      proposed_action: item.proposed_action || {},
      support_count: item.support_count || 0,
      baseline_rate: item.baseline_rate ?? null,
      sample_size_target: item.sample_size_target ?? null,
      status: item.status || 'draft',
      result: item.result || {},
    })
  }

  const startHypothesisFromInsight = (item: JourneyInsightItem) => {
    setEditingHypothesisId(null)
    setHypothesisError(null)
    setActiveTab('hypotheses')
    setHypothesisDraft({
      title: item.suggested_hypothesis.title || item.title,
      target_kpi: item.suggested_hypothesis.target_kpi || selectedDefinition?.conversion_kpi_id || '',
      hypothesis_text: item.suggested_hypothesis.hypothesis_text || item.summary,
      trigger: item.suggested_hypothesis.trigger || {},
      segment: item.suggested_hypothesis.segment || {},
      current_action: item.suggested_hypothesis.current_action || {},
      proposed_action: item.suggested_hypothesis.proposed_action || {},
      support_count: item.suggested_hypothesis.support_count || item.support_count || 0,
      baseline_rate: item.suggested_hypothesis.baseline_rate ?? item.baseline_rate ?? null,
      sample_size_target: item.suggested_hypothesis.sample_size_target ?? null,
      status: 'draft',
      result: {},
    })
  }

  const openPolicySandbox = (item: JourneyHypothesisRecord) => {
    setSandboxHypothesisId(item.id)
    setSandboxCandidateStep('')
    setActiveTab('policy')
  }

  const applySandboxCandidateToDraft = () => {
    const selected = policySimulationQuery.data?.selected_policy
    if (!selected || !sandboxHypothesis) return
    loadHypothesisIntoDraft({
      ...sandboxHypothesis,
      proposed_action: {
        ...sandboxHypothesis.proposed_action,
        step: selected.step,
        type: 'nba_intervention',
        idea: `Test ${selected.step} for prefix ${policySimulationQuery.data?.prefix?.label || ''}`.trim(),
      },
    })
  }

  const submitHypothesis = () => {
    if (!selectedJourneyId) {
      setHypothesisError('Select a journey definition first')
      return
    }
    if (!hypothesisDraft.title.trim()) {
      setHypothesisError('Hypothesis title is required')
      return
    }
    if (!hypothesisDraft.hypothesis_text.trim()) {
      setHypothesisError('Hypothesis description is required')
      return
    }
    const body: JourneyHypothesisRequest = {
      journey_definition_id: selectedJourneyId,
      title: hypothesisDraft.title.trim(),
      target_kpi: hypothesisDraft.target_kpi || selectedDefinition?.conversion_kpi_id || null,
      hypothesis_text: hypothesisDraft.hypothesis_text.trim(),
      trigger: hypothesisDraft.trigger,
      segment: hypothesisDraft.segment,
      current_action: hypothesisDraft.current_action,
      proposed_action: hypothesisDraft.proposed_action,
      support_count: Math.max(0, Math.round(hypothesisDraft.support_count || 0)),
      baseline_rate: hypothesisDraft.baseline_rate,
      sample_size_target: hypothesisDraft.sample_size_target,
      status: hypothesisDraft.status || 'draft',
      linked_experiment_id: null,
      result: hypothesisDraft.result || {},
    }
    setHypothesisError(null)
    if (editingHypothesisId) {
      updateHypothesisMutation.mutate({ id: editingHypothesisId, body })
      return
    }
    createHypothesisMutation.mutate(body)
  }

  const createExperimentFromHypothesis = (item: JourneyHypothesisRecord, proposedStep?: string) => {
    if (!hypothesisExperimentDraft.start_at || !hypothesisExperimentDraft.end_at) {
      setHypothesisError('Experiment start and end dates are required')
      return
    }
    createExperimentFromHypothesisMutation.mutate({
      hypothesisId: item.id,
      start_at: hypothesisExperimentDraft.start_at,
      end_at: hypothesisExperimentDraft.end_at,
      notes: hypothesisExperimentDraft.notes.trim(),
      proposed_step: proposedStep || '',
    })
  }

  const buildCurrentSavedViewState = (): SavedJourneyViewStatePayload => ({
    selectedJourneyId,
    activeTab,
    filters,
    pathSortBy,
    pathSortDir,
    pathsLimit,
    examplesPathHash,
    examplesStepFilter,
  })

  const saveCurrentView = () => {
    const trimmed = savedViewName.trim()
    if (!trimmed) return
    createSavedViewMutation.mutate({
      name: trimmed,
      journey_definition_id: selectedJourneyId,
      state: buildCurrentSavedViewState(),
    })
  }

  const applySavedView = (view: SavedJourneyView) => {
    setSelectedJourneyId(view.selectedJourneyId)
    setActiveTab(view.activeTab)
    setFilters(view.filters)
    setPathSortBy(view.pathSortBy)
    setPathSortDir(view.pathSortDir)
    setPathsLimit(view.pathsLimit)
    setExamplesPathHash(view.examplesPathHash || '')
    setExamplesStepFilter(view.examplesStepFilter || '')
    setSelectedPath(null)
    setComparePathHash('')
  }

  const deleteSavedView = (viewId: string) => {
    deleteSavedViewMutation.mutate(viewId)
  }

  const openCreateAlertModal = (
    draft: Pick<AlertDraft, 'name' | 'type' | 'domain' | 'metric'>,
    scope: Record<string, unknown>,
  ) => {
    setAlertDraft((prev) => ({
      ...prev,
      name: draft.name,
      type: draft.type,
      domain: draft.domain,
      metric: draft.metric,
    }))
    setAlertScope(scope)
    setCreateAlertError(null)
    setAlertPreview(null)
    setShowCreateAlertModal(true)
  }

  const submitAlertPreview = () => {
    setCreateAlertError(null)
    previewAlertMutation.mutate({
      type: alertDraft.type,
      scope: alertScope,
      metric: alertDraft.metric,
      condition: {
        comparison_mode: alertDraft.comparison_mode,
        threshold_pct: alertDraft.threshold_pct,
        severity: alertDraft.severity,
        cooldown_days: alertDraft.cooldown_days,
      },
    })
  }

  const submitCreateAlert = () => {
    if (!alertDraft.name.trim()) {
      setCreateAlertError('Alert name is required')
      return
    }
    setCreateAlertError(null)
    createAlertMutation.mutate({
      name: alertDraft.name.trim(),
      type: alertDraft.type,
      domain: alertDraft.domain,
      scope: alertScope,
      metric: alertDraft.metric,
      condition: {
        comparison_mode: alertDraft.comparison_mode,
        threshold_pct: alertDraft.threshold_pct,
        severity: alertDraft.severity,
        cooldown_days: alertDraft.cooldown_days,
      },
      schedule: { cadence: 'daily' },
      is_enabled: true,
    })
  }

  return (
    <>
      <DashboardPage
        title="Journeys"
        description="Customer journey paths + attribution overlay"
        filters={
          <GlobalFilterBar
            value={filters}
            onChange={setFilters}
            channels={journeyFilterOptions.channels}
            campaigns={journeyFilterOptions.campaigns}
            devices={journeyFilterOptions.devices}
            geos={journeyFilterOptions.geos}
            showSegment={false}
          />
        }
        isEmpty={featureDisabled}
        emptyState={
          <SectionCard
            title="Feature not enabled"
            subtitle={
              hasPermission
                ? 'Enable journeys_enabled in feature flags to access this module.'
                : 'Your role does not include access to Journeys.'
            }
          >
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              This page is available only when the Journeys feature flag is enabled.
            </div>
          </SectionCard>
        }
      >
        <style>{`
          @media (max-width: 1100px) {
            .journeys-split-layout {
              grid-template-columns: minmax(0, 1fr) !important;
            }
            .journeys-two-col-layout {
              grid-template-columns: minmax(0, 1fr) !important;
            }
            .journeys-detail-grid {
              grid-template-columns: minmax(0, 1fr) !important;
            }
            .journeys-metric-row {
              grid-template-columns: minmax(0, 1fr) !important;
            }
            .journeys-card-grid {
              grid-template-columns: minmax(0, 1fr) !important;
            }
          }
        `}</style>
        <SectionCard
          title="Journey definition"
          subtitle="Select an existing journey or create a new one for this workspace."
          actions={
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
              <label
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  padding: '8px 10px',
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  background: t.color.surface,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                <input
                  type="checkbox"
                  checked={showArchivedDefinitions}
                  onChange={(e) => setShowArchivedDefinitions(e.target.checked)}
                />
                Show archived
              </label>
              <button
                type="button"
                onClick={() => {
                  if (!canManageDefinitions) return
                  setCreateError(null)
                  setDraft({
                    name: '',
                    description: '',
                    conversion_kpi_id: kpisQuery.data?.primary_kpi_id || kpiOptions[0]?.id || '',
                    lookback_window_days: 30,
                    mode_default: 'conversion_only',
                  })
                  setShowCreateModal(true)
                }}
                disabled={!canManageDefinitions}
                style={{
                  border: `1px solid ${t.color.accent}`,
                  background: t.color.accent,
                  color: '#fff',
                  borderRadius: t.radius.sm,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  padding: '8px 14px',
                  cursor: canManageDefinitions ? 'pointer' : 'not-allowed',
                  opacity: canManageDefinitions ? 1 : 0.5,
                }}
                title={canManageDefinitions ? undefined : 'Only admin or editor can create journey definitions'}
              >
                Create journey
              </button>
              {!!selectedJourneyId && !selectedDefinitionArchived && (
                <button
                  type="button"
                  onClick={() =>
                    openCreateAlertModal(
                      {
                        name: `Journey volume change: ${selectedDefinition?.name || 'journey'}`,
                        type: 'path_volume_change',
                        domain: 'journeys',
                        metric: 'count_journeys',
                      },
                      {
                        journey_definition_id: selectedJourneyId,
                        filters: {
                          channel_group: filters.channel !== 'all' ? filters.channel : null,
                          campaign_id: filters.campaign !== 'all' ? filters.campaign : null,
                          device: filters.device !== 'all' ? filters.device : null,
                          country: filters.geo !== 'all' ? filters.geo : null,
                        },
                      },
                    )
                  }
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    padding: '8px 14px',
                    cursor: 'pointer',
                  }}
                >
                  Create alert
                </button>
              )}
              {canManageDefinitions && selectedDefinition && definitionLifecycleQuery.data ? (
                <>
                  <button
                    type="button"
                    onClick={() => {
                      setCreateError(null)
                      setDraft({
                        name: selectedDefinition.name || '',
                        description: selectedDefinition.description || '',
                        conversion_kpi_id: selectedDefinition.conversion_kpi_id || kpisQuery.data?.primary_kpi_id || kpiOptions[0]?.id || '',
                        lookback_window_days: clampLookback(selectedDefinition.lookback_window_days || 30),
                        mode_default: selectedDefinition.mode_default || 'conversion_only',
                      })
                      setShowEditModal(true)
                    }}
                    disabled={selectedDefinitionArchived || updateMutation.isPending}
                    style={{
                      border: `1px solid ${t.color.border}`,
                      background: t.color.surface,
                      color: t.color.text,
                      borderRadius: t.radius.sm,
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      padding: '8px 14px',
                      cursor: selectedDefinitionArchived || updateMutation.isPending ? 'not-allowed' : 'pointer',
                      opacity: selectedDefinitionArchived ? 0.5 : 1,
                    }}
                    title={selectedDefinitionArchived ? 'Restore the journey definition before editing it' : undefined}
                  >
                    {updateMutation.isPending ? 'Saving…' : 'Edit'}
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      const defaultName = `${selectedDefinition.name} copy`
                      const nextName = window.prompt('Name for duplicated journey definition', defaultName)?.trim()
                      if (!nextName) return
                      duplicateDefinitionMutation.mutate({ definitionId: selectedDefinition.id, name: nextName })
                    }}
                    disabled={!definitionLifecycleQuery.data.allowed_actions.can_duplicate || duplicateDefinitionMutation.isPending}
                    style={{
                      border: `1px solid ${t.color.border}`,
                      background: t.color.surface,
                      color: t.color.text,
                      borderRadius: t.radius.sm,
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      padding: '8px 14px',
                      cursor: 'pointer',
                    }}
                  >
                    {duplicateDefinitionMutation.isPending ? 'Duplicating…' : 'Duplicate'}
                  </button>
                  {!selectedDefinitionArchived ? (
                    <button
                      type="button"
                      onClick={() => rebuildDefinitionMutation.mutate({ definitionId: selectedDefinition.id })}
                      disabled={!definitionLifecycleQuery.data.allowed_actions.can_rebuild || rebuildDefinitionMutation.isPending}
                      style={{
                        border: `1px solid ${t.color.accent}`,
                        background: t.color.surface,
                        color: t.color.accent,
                        borderRadius: t.radius.sm,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        padding: '8px 14px',
                        cursor: 'pointer',
                      }}
                    >
                      {rebuildDefinitionMutation.isPending ? 'Rebuilding…' : 'Rebuild outputs'}
                    </button>
                  ) : null}
                  {!selectedDefinitionArchived ? (
                    <button
                      type="button"
                      onClick={() => {
                        const confirmed = window.confirm(
                          `Archive "${selectedDefinition.name}"? Downstream views, funnels, hypotheses, alerts, and outputs will be preserved, but the definition becomes read-only until restored.`,
                        )
                        if (!confirmed) return
                        archiveDefinitionMutation.mutate(selectedDefinition.id)
                      }}
                      disabled={!definitionLifecycleQuery.data.allowed_actions.can_archive || archiveDefinitionMutation.isPending}
                      style={{
                        border: `1px solid ${t.color.warning}`,
                        background: t.color.surface,
                        color: t.color.warning,
                        borderRadius: t.radius.sm,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        padding: '8px 14px',
                        cursor: 'pointer',
                      }}
                    >
                      {archiveDefinitionMutation.isPending ? 'Archiving…' : 'Archive'}
                    </button>
                  ) : (
                    <button
                      type="button"
                      onClick={() => restoreDefinitionMutation.mutate(selectedDefinition.id)}
                      disabled={!definitionLifecycleQuery.data.allowed_actions.can_restore || restoreDefinitionMutation.isPending}
                      style={{
                        border: `1px solid ${t.color.accent}`,
                        background: t.color.accent,
                        color: '#fff',
                        borderRadius: t.radius.sm,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        padding: '8px 14px',
                        cursor: 'pointer',
                      }}
                    >
                      {restoreDefinitionMutation.isPending ? 'Restoring…' : 'Restore'}
                    </button>
                  )}
                  {definitionLifecycleQuery.data.allowed_actions.can_delete ? (
                    <button
                      type="button"
                      onClick={() => {
                        const confirmed = window.confirm(`Delete "${selectedDefinition.name}" permanently? This cannot be undone.`)
                        if (!confirmed) return
                        hardDeleteDefinitionMutation.mutate(selectedDefinition.id)
                      }}
                      disabled={hardDeleteDefinitionMutation.isPending}
                      style={{
                        border: `1px solid ${t.color.danger}`,
                        background: t.color.dangerMuted,
                        color: t.color.danger,
                        borderRadius: t.radius.sm,
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightMedium,
                        padding: '8px 14px',
                        cursor: hardDeleteDefinitionMutation.isPending ? 'wait' : 'pointer',
                      }}
                    >
                      {hardDeleteDefinitionMutation.isPending ? 'Deleting…' : 'Delete permanently'}
                    </button>
                  ) : null}
                </>
              ) : null}
            </div>
          }
        >
          <div style={{ display: 'grid', gap: t.space.md, minWidth: 0 }}>
            <select
              value={selectedJourneyId}
              onChange={(e) => setSelectedJourneyId(e.target.value)}
              style={{ width: 'min(520px, 100%)', padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeSm }}
            >
              {!journeyOptions.length && <option value="">No journey definitions</option>}
              {journeyOptions.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
            <div
              style={{
                display: 'grid',
                gap: 4,
                padding: '10px 12px',
                borderRadius: t.radius.md,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              <div>Filters are derived from observed journey rows for the selected definition and date range.</div>
              <div>
                {dimensionsQuery.data
                  ? `${dimensionsQuery.data.summary.journey_rows.toLocaleString()} rows observed from ${dimensionsQuery.data.summary.date_from} to ${dimensionsQuery.data.summary.date_to}.`
                  : 'Filter values load from current workspace activity once the selected definition resolves.'}
              </div>
              <div>Segment is intentionally hidden here until it becomes a real modeled journey dimension.</div>
            </div>
            {selectedDefinition ? (
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
                  <div
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 6,
                      borderRadius: t.radius.full,
                      padding: '4px 10px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                      background:
                        selectedDefinitionLifecycleStatus === 'archived'
                          ? t.color.warningMuted
                          : selectedDefinitionLifecycleStatus === 'stale'
                            ? t.color.accentMuted
                            : t.color.successMuted,
                      color:
                        selectedDefinitionLifecycleStatus === 'archived'
                          ? t.color.warning
                          : selectedDefinitionLifecycleStatus === 'stale'
                            ? t.color.accent
                            : t.color.success,
                    }}
                  >
                    {selectedDefinitionLifecycleStatus === 'archived'
                      ? 'Archived'
                      : selectedDefinitionLifecycleStatus === 'stale'
                        ? 'Needs rebuild'
                        : 'Active'}
                  </div>
                  {selectedDefinition.updated_at ? (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Updated {new Date(selectedDefinition.updated_at).toLocaleString()}
                    </div>
                  ) : null}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{selectedDefinition.description?.trim() || 'No description'}</div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  Lookback: {selectedDefinition.lookback_window_days} days • Mode:{' '}
                  {selectedDefinition.mode_default === 'all_journeys' ? 'All journeys' : 'Conversion only'}
                </div>
                {definitionLifecycleQuery.data ? (
                  <div
                    style={{
                      marginTop: t.space.xs,
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      background: t.color.bg,
                      padding: t.space.md,
                      display: 'grid',
                      gap: t.space.sm,
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                      Definition lifecycle
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: t.space.sm }}>
                      {[
                        { label: 'Saved views', value: definitionLifecycleQuery.data.dependency_counts.saved_views, target: 'saved_views' as const, actionLabel: 'Open saved views' },
                        { label: 'Funnels', value: definitionLifecycleQuery.data.dependency_counts.funnels, target: 'funnels' as const, actionLabel: 'Open funnels' },
                        { label: 'Hypotheses', value: definitionLifecycleQuery.data.dependency_counts.hypotheses, target: 'hypotheses' as const, actionLabel: 'Open hypotheses' },
                        { label: 'Experiments', value: definitionLifecycleQuery.data.dependency_counts.experiments, target: 'experiments' as const, actionLabel: 'Open experiments' },
                        { label: 'Alerts', value: definitionLifecycleQuery.data.dependency_counts.alerts, target: 'alerts' as const, actionLabel: 'Open alerts' },
                        { label: 'Journey rows', value: definitionLifecycleQuery.data.output_counts.journey_instances, target: 'journey_rows' as const, actionLabel: 'Open paths' },
                      ].map((item) => (
                        <button
                          key={item.label}
                          type="button"
                          onClick={() => openLifecycleDependency(item.target)}
                          style={{
                            border: `1px solid ${t.color.borderLight}`,
                            borderRadius: t.radius.sm,
                            padding: t.space.sm,
                            background: t.color.surface,
                            textAlign: 'left',
                            cursor: 'pointer',
                          }}
                        >
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                          <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.value.toLocaleString()}</div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.accent, marginTop: 4 }}>{item.actionLabel}</div>
                        </button>
                      ))}
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Last rebuilt: {definitionLifecycleQuery.data.rebuild_state?.last_rebuilt_at ? new Date(definitionLifecycleQuery.data.rebuild_state.last_rebuilt_at).toLocaleString() : 'Never'}
                    </div>
                    <div
                      style={{
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.sm,
                        background: t.color.surface,
                        padding: t.space.sm,
                        display: 'grid',
                        gap: 6,
                      }}
                    >
                      <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                        Governance
                      </div>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
                        <div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Created by</div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{formatLifecycleActor(definitionLifecycleQuery.data.definition.created_by)}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Created at</div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{formatLifecycleTimestamp(definitionLifecycleQuery.data.definition.created_at)}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Last changed by</div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{formatLifecycleActor(definitionLifecycleQuery.data.definition.updated_by)}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Last changed at</div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{formatLifecycleTimestamp(definitionLifecycleQuery.data.definition.updated_at)}</div>
                        </div>
                        {definitionLifecycleQuery.data.definition.archived_at || definitionLifecycleQuery.data.definition.archived_by ? (
                          <>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Archived by</div>
                              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{formatLifecycleActor(definitionLifecycleQuery.data.definition.archived_by)}</div>
                            </div>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Archived at</div>
                              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{formatLifecycleTimestamp(definitionLifecycleQuery.data.definition.archived_at)}</div>
                            </div>
                          </>
                        ) : null}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        This shows the recent lifecycle events for the selected journey definition.
                      </div>
                    </div>
                    <div
                      style={{
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.sm,
                        background: t.color.surface,
                        padding: t.space.sm,
                        display: 'grid',
                        gap: 6,
                      }}
                    >
                      <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                        Lifecycle history
                      </div>
                      {definitionAuditQuery.isLoading ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Loading audit history…</div>
                      ) : definitionAuditQuery.isError ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                          {(definitionAuditQuery.error as Error).message}
                        </div>
                      ) : !(definitionAuditQuery.data ?? []).length ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          No lifecycle events recorded yet.
                        </div>
                      ) : (
                        <div style={{ display: 'grid', gap: 6 }}>
                          {(definitionAuditQuery.data ?? []).map((entry) => (
                            <div
                              key={entry.id}
                              style={{
                                borderLeft: `2px solid ${t.color.border}`,
                                paddingLeft: t.space.sm,
                                display: 'grid',
                                gap: 2,
                              }}
                            >
                              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                                {formatLifecycleAction(entry.action)} by {formatLifecycleActor(entry.actor)}
                              </div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                                {formatLifecycleTimestamp(entry.created_at)}
                              </div>
                              {entry.diff_json ? (
                                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                                  {JSON.stringify(entry.diff_json)}
                                </div>
                              ) : null}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                    {(definitionLifecycleQuery.data.warnings || []).length ? (
                      <div style={{ display: 'grid', gap: 4 }}>
                        {definitionLifecycleQuery.data.warnings.map((warning) => (
                          <div key={warning} style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>
                            {warning}
                          </div>
                        ))}
                      </div>
                    ) : null}
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Permanent delete unlocks only after archive, and only for definitions with no downstream dependencies or generated journey outputs.
                    </div>
                    {canManageDefinitions ? (
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Lifecycle actions are available in the Journey Definition header above so archive, restore, rebuild, and delete are visible without opening this panel.
                      </div>
                    ) : null}
                    {hardDeleteDefinitionMutation.isError ? (
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        {(hardDeleteDefinitionMutation.error as Error).message}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                {definitionsQuery.isLoading ? 'Loading journey definitions…' : 'Create a journey definition to get started.'}
              </div>
            )}
          </div>
        </SectionCard>

        <SectionCard title="Journey workspace" subtitle={selectedDefinitionArchived ? 'Archived definitions are visible but read-only until restored.' : 'Credit + paths in one workspace.'}>
          {selectedDefinitionArchived ? (
            <div
              style={{
                marginBottom: t.space.md,
                border: `1px solid ${t.color.warning}`,
                borderRadius: t.radius.md,
                background: t.color.warningMuted,
                color: t.color.warning,
                padding: t.space.md,
                fontSize: t.font.sizeSm,
              }}
            >
              This journey definition is archived. Restore it to re-enable paths, flow, examples, experiments, and other workspace analysis.
            </div>
          ) : null}
          <div style={{ display: 'grid', gap: t.space.md, minWidth: 0 }}>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', marginBottom: t.space.md, minWidth: 0 }}>
            {tabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                disabled={tab.disabled || selectedDefinitionArchived}
                onClick={() => setActiveTab(tab.key)}
                style={{
                  border: `1px solid ${tab.key === activeTab ? t.color.accent : t.color.borderLight}`,
                  borderRadius: t.radius.full,
                  background: tab.key === activeTab ? t.color.accentMuted : t.color.surface,
                  color: tab.key === activeTab ? t.color.accent : t.color.textMuted,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  padding: '6px 12px',
                  cursor: tab.disabled || selectedDefinitionArchived ? 'not-allowed' : 'pointer',
                  opacity: tab.disabled || selectedDefinitionArchived ? 0.75 : 1,
                }}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {!selectedDefinitionArchived && activeTab === 'insights' && (
            <div style={{ display: 'grid', gap: t.space.md }}>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Paths considered</div>
                  <div style={{ fontSize: t.font.sizeXl, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                    {(insightsQuery.data?.summary.paths_considered ?? 0).toLocaleString()}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Journeys</div>
                  <div style={{ fontSize: t.font.sizeXl, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                    {(insightsQuery.data?.summary.journeys ?? 0).toLocaleString()}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Conversions</div>
                  <div style={{ fontSize: t.font.sizeXl, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                    {(insightsQuery.data?.summary.conversions ?? 0).toLocaleString()}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Baseline CVR</div>
                  <div style={{ fontSize: t.font.sizeXl, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                    {formatPercent(insightsQuery.data?.summary.baseline_conversion_rate ?? 0)}
                  </div>
                </div>
              </div>

              {insightsQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading journey insights…</div>}
              {insightsQuery.isError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(insightsQuery.error as Error).message}</div>}
              {!insightsQuery.isLoading && !insightsQuery.isError && !insightItems.length && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  No insight candidates for the selected journey and filter set.
                </div>
              )}

              {!!hypotheses.length && (
                <SectionCard title="Learning loop" subtitle="Completed journey experiments feed back into discovery and policy selection.">
                  <div style={{ display: 'grid', gap: t.space.md }}>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: t.space.sm }}>
                      {[
                        { label: 'Validated', value: hypothesisLearningCounts.validated || 0 },
                        { label: 'Rejected', value: hypothesisLearningCounts.rejected || 0 },
                        { label: 'Inconclusive', value: hypothesisLearningCounts.inconclusive || 0 },
                        { label: 'In flight', value: (hypothesisLearningCounts.in_experiment || 0) + (hypothesisLearningCounts.experiment_draft || 0) },
                      ].map((item) => (
                        <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                          <div style={{ fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                            {item.value.toLocaleString()}
                          </div>
                        </div>
                      ))}
                    </div>
                    {!!latestLearnedHypotheses.length && (
                      <div style={{ display: 'grid', gap: t.space.sm }}>
                        {latestLearnedHypotheses.map((item) => (
                          <div key={item.id} style={{ display: 'grid', gap: 6 }}>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{item.title}</div>
                            {renderHypothesisLearningCard(item, true)}
                          </div>
                        ))}
                      </div>
                    )}
                    {policyRecommendationsQuery.isLoading ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading learned policy recommendations…</div>
                    ) : null}
                    {!policyRecommendationsQuery.isLoading && !!topPromotablePolicies.length ? (
                      <div style={{ display: 'grid', gap: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                          Top learned policies
                        </div>
                        {topPromotablePolicies.map((item) => renderPolicyRecommendationCard(item))}
                      </div>
                    ) : null}
                  </div>
                </SectionCard>
              )}

              {!!insightItems.length && (
                <div style={{ display: 'grid', gap: t.space.md }}>
                  {insightItems.map((item) => {
                    const confidenceColor =
                      item.confidence === 'high' ? t.color.success : item.confidence === 'medium' ? t.color.warning : t.color.textMuted
                    const severityColor =
                      item.severity === 'high' ? t.color.danger : item.severity === 'medium' ? t.color.warning : t.color.textMuted
                    return (
                      <SectionCard
                        key={item.id}
                        title={item.title}
                        subtitle={item.summary}
                        actions={
                          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
                            <span
                              style={{
                                border: `1px solid ${severityColor}`,
                                color: severityColor,
                                borderRadius: t.radius.full,
                                padding: '4px 8px',
                                fontSize: t.font.sizeXs,
                                textTransform: 'capitalize',
                              }}
                            >
                              {item.severity} priority
                            </span>
                            <span
                              style={{
                                border: `1px solid ${confidenceColor}`,
                                color: confidenceColor,
                                borderRadius: t.radius.full,
                                padding: '4px 8px',
                                fontSize: t.font.sizeXs,
                                textTransform: 'capitalize',
                              }}
                            >
                              {item.confidence} confidence
                            </span>
                          </div>
                        }
                      >
                        <div style={{ display: 'grid', gap: t.space.md }}>
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
                            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Support</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>{item.support_count.toLocaleString()} journeys</div>
                            </div>
                            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed CVR</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>{formatPercent(item.observed_rate)}</div>
                            </div>
                            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Impact estimate</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                                ~{(item.impact_estimate?.estimated_users_affected ?? 0).toLocaleString()} users
                              </div>
                            </div>
                          </div>

                          <div style={{ display: 'grid', gap: 6 }}>
                            {(item.evidence || []).map((entry, idx) => (
                              <div key={`${item.id}-${idx}`} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                                {entry}
                              </div>
                            ))}
                          </div>

                          <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', marginBottom: 4 }}>
                              Suggested hypothesis
                            </div>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                              {item.suggested_hypothesis.title}
                            </div>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 4 }}>
                              {item.suggested_hypothesis.hypothesis_text}
                            </div>
                          </div>

                          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                            <button
                              type="button"
                              onClick={() => startHypothesisFromInsight(item)}
                              style={{
                                border: `1px solid ${t.color.accent}`,
                                background: t.color.accent,
                                color: '#fff',
                                borderRadius: t.radius.sm,
                                padding: '8px 12px',
                                fontSize: t.font.sizeSm,
                                cursor: 'pointer',
                              }}
                            >
                              Create hypothesis
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                const trigger = item.suggested_hypothesis.trigger || {}
                                const pathHash = typeof trigger.path_hash === 'string' ? trigger.path_hash : ''
                                const steps = Array.isArray(trigger.steps) ? trigger.steps.join(' ') : ''
                                setExamplesPathHash(pathHash)
                                setExamplesStepFilter('')
                                if (!pathHash && steps) setExamplesStepFilter(String(steps).split(' ')[0] || '')
                                setActiveTab('examples')
                              }}
                              style={{
                                border: `1px solid ${t.color.border}`,
                                background: t.color.surface,
                                color: t.color.text,
                                borderRadius: t.radius.sm,
                                padding: '8px 12px',
                                fontSize: t.font.sizeSm,
                                cursor: 'pointer',
                              }}
                            >
                              Open evidence
                            </button>
                          </div>
                        </div>
                      </SectionCard>
                    )
                  })}
                </div>
              )}
            </div>
          )}

          {!selectedDefinitionArchived && activeTab === 'hypotheses' && (
            <div className="journeys-card-grid" style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(min(320px, 100%), 1fr))', minWidth: 0 }}>
              <SectionCard
                title={editingHypothesisId ? 'Edit hypothesis' : 'New hypothesis'}
                subtitle="Turn observed journey behavior into a structured experiment candidate."
                actions={
                  <button
                    type="button"
                    onClick={resetHypothesisDraft}
                    style={{
                      border: `1px solid ${t.color.border}`,
                      background: t.color.surface,
                      color: t.color.text,
                      borderRadius: t.radius.sm,
                      padding: '6px 10px',
                      fontSize: t.font.sizeSm,
                      cursor: 'pointer',
                    }}
                  >
                    New draft
                  </button>
                }
              >
                <div style={{ display: 'grid', gap: t.space.md }}>
                  <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                    Title
                    <input
                      value={hypothesisDraft.title}
                      onChange={(e) => setHypothesisDraft((prev) => ({ ...prev, title: e.target.value }))}
                      placeholder="Recover mobile retargeting exits"
                      style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                    />
                  </label>

                  <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                    Hypothesis
                    <textarea
                      value={hypothesisDraft.hypothesis_text}
                      onChange={(e) => setHypothesisDraft((prev) => ({ ...prev, hypothesis_text: e.target.value }))}
                      rows={3}
                      placeholder="If we intervene earlier for this path, conversion rate should improve."
                      style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                    />
                  </label>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.md }}>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Status
                      <select
                        value={hypothesisDraft.status}
                        onChange={(e) => setHypothesisDraft((prev) => ({ ...prev, status: e.target.value }))}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      >
                        <option value="draft">Draft</option>
                        <option value="ready_to_test">Ready to test</option>
                        <option value="in_experiment">In experiment</option>
                        <option value="validated">Validated</option>
                        <option value="rejected">Rejected</option>
                        <option value="inconclusive">Inconclusive</option>
                      </select>
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Target KPI
                      <select
                        value={hypothesisDraft.target_kpi}
                        onChange={(e) => setHypothesisDraft((prev) => ({ ...prev, target_kpi: e.target.value }))}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      >
                        <option value="">Default journey KPI</option>
                        {kpiOptions.map((kpi) => (
                          <option key={kpi.id} value={kpi.id}>
                            {kpi.label}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Support
                      <input
                        type="number"
                        min={0}
                        value={hypothesisDraft.support_count}
                        onChange={(e) => setHypothesisDraft((prev) => ({ ...prev, support_count: Math.max(0, Number(e.target.value) || 0) }))}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Sample size target
                      <input
                        type="number"
                        min={0}
                        value={hypothesisDraft.sample_size_target ?? ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            sample_size_target: e.target.value ? Math.max(0, Number(e.target.value) || 0) : null,
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Baseline conversion rate
                      <input
                        type="number"
                        min={0}
                        max={1}
                        step="0.0001"
                        value={hypothesisDraft.baseline_rate ?? ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            baseline_rate: e.target.value === '' ? null : Math.max(0, Number(e.target.value) || 0),
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))', gap: t.space.md, minWidth: 0 }}>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Trigger path hash
                      <input
                        value={typeof hypothesisDraft.trigger.path_hash === 'string' ? hypothesisDraft.trigger.path_hash : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            trigger: { ...prev.trigger, path_hash: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Trigger steps
                      <textarea
                        value={Array.isArray(hypothesisDraft.trigger.steps) ? (hypothesisDraft.trigger.steps as unknown[]).map(String).join('\n') : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            trigger: { ...prev.trigger, steps: parseDelimitedSteps(e.target.value) },
                          }))
                        }
                        rows={3}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))', gap: t.space.md, minWidth: 0 }}>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Channel group
                      <input
                        value={typeof hypothesisDraft.segment.channel_group === 'string' ? hypothesisDraft.segment.channel_group : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            segment: { ...prev.segment, channel_group: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Campaign
                      <input
                        value={typeof hypothesisDraft.segment.campaign_id === 'string' ? hypothesisDraft.segment.campaign_id : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            segment: { ...prev.segment, campaign_id: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Device
                      <input
                        value={typeof hypothesisDraft.segment.device === 'string' ? hypothesisDraft.segment.device : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            segment: { ...prev.segment, device: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Country
                      <input
                        value={typeof hypothesisDraft.segment.country === 'string' ? hypothesisDraft.segment.country : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            segment: { ...prev.segment, country: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.md }}>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Current action type
                      <input
                        value={typeof hypothesisDraft.current_action.type === 'string' ? hypothesisDraft.current_action.type : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            current_action: { ...prev.current_action, type: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Proposed action type
                      <input
                        value={typeof hypothesisDraft.proposed_action.type === 'string' ? hypothesisDraft.proposed_action.type : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            proposed_action: { ...prev.proposed_action, type: e.target.value },
                          }))
                        }
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, gridColumn: '1 / -1' }}>
                      Proposed action idea
                      <textarea
                        value={typeof hypothesisDraft.proposed_action.idea === 'string' ? hypothesisDraft.proposed_action.idea : ''}
                        onChange={(e) =>
                          setHypothesisDraft((prev) => ({
                            ...prev,
                            proposed_action: { ...prev.proposed_action, idea: e.target.value },
                          }))
                        }
                        rows={2}
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                  </div>

                  <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                    Result notes
                    <textarea
                      value={hypothesisResultNote}
                      onChange={(e) =>
                        setHypothesisDraft((prev) => ({
                          ...prev,
                          result: e.target.value ? { ...prev.result, note: e.target.value } : {},
                        }))
                      }
                      rows={2}
                      placeholder="What still needs validation, or what did we learn?"
                      style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                    />
                  </label>

                  <div style={{ borderTop: `1px solid ${t.color.borderLight}`, paddingTop: t.space.md, display: 'grid', gap: t.space.md }}>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                      Experiment linking
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))', gap: t.space.md, minWidth: 0 }}>
                      <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                        Experiment start
                        <input
                          type="date"
                          value={hypothesisExperimentDraft.start_at}
                          onChange={(e) => setHypothesisExperimentDraft((prev) => ({ ...prev, start_at: e.target.value }))}
                          style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                        />
                      </label>
                      <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                        Experiment end
                        <input
                          type="date"
                          value={hypothesisExperimentDraft.end_at}
                          onChange={(e) => setHypothesisExperimentDraft((prev) => ({ ...prev, end_at: e.target.value }))}
                          style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                        />
                      </label>
                    </div>
                    <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                      Experiment notes
                      <textarea
                        value={hypothesisExperimentDraft.notes}
                        onChange={(e) => setHypothesisExperimentDraft((prev) => ({ ...prev, notes: e.target.value }))}
                        rows={2}
                        placeholder="Operator notes, rollout assumptions, or holdout design details."
                        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    </label>
                    {activeHypothesis?.linked_experiment_id ? (
                      <div style={{ display: 'grid', gap: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                          Linked experiment #{activeHypothesis.linked_experiment_id} is active for this hypothesis.
                        </div>
                        {renderHypothesisLearningCard(activeHypothesis)}
                      </div>
                    ) : (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        Save the hypothesis first, then create an experiment that will appear in Incrementality with journey provenance.
                      </div>
                    )}
                  </div>

                  {hypothesisError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{hypothesisError}</div>}

                  <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                    {activeHypothesis && !activeHypothesis.linked_experiment_id ? (
                      <button
                        type="button"
                        onClick={() => createExperimentFromHypothesis(activeHypothesis)}
                        disabled={createExperimentFromHypothesisMutation.isPending}
                        style={{
                          border: `1px solid ${t.color.border}`,
                          background: t.color.surface,
                          color: t.color.text,
                          borderRadius: t.radius.sm,
                          padding: '8px 12px',
                          fontSize: t.font.sizeSm,
                          cursor: createExperimentFromHypothesisMutation.isPending ? 'wait' : 'pointer',
                          opacity: createExperimentFromHypothesisMutation.isPending ? 0.8 : 1,
                        }}
                      >
                        {createExperimentFromHypothesisMutation.isPending ? 'Creating experiment…' : 'Create experiment'}
                      </button>
                    ) : null}
                    <button
                      type="button"
                      onClick={submitHypothesis}
                      disabled={createHypothesisMutation.isPending || updateHypothesisMutation.isPending || createExperimentFromHypothesisMutation.isPending}
                      style={{
                        border: `1px solid ${t.color.accent}`,
                        background: t.color.accent,
                        color: '#fff',
                        borderRadius: t.radius.sm,
                        padding: '8px 12px',
                        fontSize: t.font.sizeSm,
                        cursor: createHypothesisMutation.isPending || updateHypothesisMutation.isPending || createExperimentFromHypothesisMutation.isPending ? 'wait' : 'pointer',
                        opacity: createHypothesisMutation.isPending || updateHypothesisMutation.isPending || createExperimentFromHypothesisMutation.isPending ? 0.8 : 1,
                      }}
                    >
                      {createHypothesisMutation.isPending || updateHypothesisMutation.isPending
                        ? 'Saving…'
                        : editingHypothesisId
                        ? 'Update hypothesis'
                        : 'Save hypothesis'}
                    </button>
                  </div>
                </div>
              </SectionCard>

              <SectionCard title="Saved hypotheses" subtitle="Reusable journey interventions with context, support, and readiness.">
                <div style={{ display: 'grid', gap: t.space.sm }}>
                  {hypothesesQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading journey hypotheses…</div>}
                  {hypothesesQuery.isError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(hypothesesQuery.error as Error).message}</div>}
                  {!hypothesesQuery.isLoading && !hypothesesQuery.isError && !hypotheses.length && (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      No hypotheses yet. Start from an insight or create one manually.
                    </div>
                  )}
                  {hypotheses.map((item) => (
                    <div key={item.id} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: editingHypothesisId === item.id ? t.color.accentMuted : t.color.surface, display: 'grid', gap: 8 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'start' }}>
                        <div style={{ display: 'grid', gap: 4 }}>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightSemibold }}>{item.title}</div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                            {formatStageLabel(getHypothesisLearningStage(item))} · {item.support_count.toLocaleString()} journeys · {item.sample_size_target?.toLocaleString() || '—'} sample target
                            {item.linked_experiment_id ? ` · experiment #${item.linked_experiment_id}` : ''}
                          </div>
                        </div>
                        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                          <button
                            type="button"
                            onClick={() => openPolicySandbox(item)}
                            style={{
                              border: `1px solid ${t.color.border}`,
                              background: t.color.surface,
                              color: t.color.text,
                              borderRadius: t.radius.sm,
                              padding: '6px 10px',
                              fontSize: t.font.sizeSm,
                              cursor: 'pointer',
                            }}
                          >
                            Open sandbox
                          </button>
                          {!item.linked_experiment_id ? (
                            <button
                              type="button"
                              onClick={() => {
                                loadHypothesisIntoDraft(item)
                                createExperimentFromHypothesis(item)
                              }}
                              style={{
                                border: `1px solid ${t.color.border}`,
                                background: t.color.surface,
                                color: t.color.text,
                                borderRadius: t.radius.sm,
                                padding: '6px 10px',
                                fontSize: t.font.sizeSm,
                                cursor: 'pointer',
                              }}
                            >
                              Create experiment
                            </button>
                          ) : null}
                          <button
                            type="button"
                            onClick={() => loadHypothesisIntoDraft(item)}
                            style={{
                              border: `1px solid ${t.color.border}`,
                              background: t.color.surface,
                              color: t.color.text,
                              borderRadius: t.radius.sm,
                              padding: '6px 10px',
                              fontSize: t.font.sizeSm,
                              cursor: 'pointer',
                            }}
                          >
                            Edit
                          </button>
                        </div>
                      </div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{item.hypothesis_text}</div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Trigger: {objectPreview(item.trigger)}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Segment: {objectPreview(item.segment)}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Proposed action: {objectPreview(item.proposed_action)}
                      </div>
                      {item.linked_experiment_id ? renderHypothesisLearningCard(item) : null}
                      {typeof item.result?.['note'] === 'string' && item.result['note'] ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Notes: {String(item.result['note'])}
                        </div>
                      ) : null}
                    </div>
                  ))}
                </div>
              </SectionCard>
            </div>
          )}

          {!selectedDefinitionArchived && activeTab === 'experiments' && (
            <div className="journeys-split-layout" style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'minmax(280px, 360px) minmax(0, 1fr)', minWidth: 0 }}>
              <div style={{ display: 'grid', gap: t.space.md, minWidth: 0 }}>
                <SectionCard title="Experiment queue" subtitle="Journey hypotheses already linked into Incrementality.">
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: t.space.xs }}>
                      {[
                        { label: 'Total', value: linkedExperimentCounts.total || 0 },
                        { label: 'Draft', value: linkedExperimentCounts.draft || 0 },
                        { label: 'Running', value: linkedExperimentCounts.running || 0 },
                        { label: 'Completed', value: linkedExperimentCounts.completed || 0 },
                      ].map((metric) => (
                        <div
                          key={metric.label}
                          style={{
                            border: `1px solid ${t.color.borderLight}`,
                            borderRadius: t.radius.sm,
                            padding: '10px 12px',
                            background: t.color.bg,
                          }}
                        >
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>{metric.label}</div>
                          <div style={{ fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                            {metric.value.toLocaleString()}
                          </div>
                        </div>
                      ))}
                    </div>
                    {linkedExperimentsQuery.isLoading ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading linked experiments…</div>
                    ) : null}
                    {linkedExperimentsQuery.isError ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(linkedExperimentsQuery.error as Error).message}</div>
                    ) : null}
                    {!linkedExperimentsQuery.isLoading && !linkedExperiments.length ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        No linked experiments yet. Create one from a hypothesis or the Policy Sandbox.
                      </div>
                    ) : null}
                    {linkedExperiments.map((item) => {
                      const linkedHypothesis = experimentHypothesisById.get(item.id)
                      const isSelected = selectedJourneyExperimentId === item.id
                      return (
                        <button
                          key={item.id}
                          type="button"
                          onClick={() => setSelectedJourneyExperimentId(item.id)}
                          style={{
                            textAlign: 'left',
                            border: `1px solid ${isSelected ? t.color.accent : t.color.borderLight}`,
                            background: isSelected ? t.color.accentMuted : t.color.surface,
                            borderRadius: t.radius.sm,
                            padding: '10px 12px',
                            cursor: 'pointer',
                            display: 'grid',
                            gap: 6,
                          }}
                        >
                          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center' }}>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                              {item.name}
                            </div>
                            <span
                              style={{
                                border: `1px solid ${item.status === 'completed' ? t.color.success : item.status === 'running' ? t.color.accent : t.color.border}`,
                                color: item.status === 'completed' ? t.color.success : item.status === 'running' ? t.color.accent : t.color.textSecondary,
                                borderRadius: t.radius.full,
                                padding: '2px 8px',
                                fontSize: t.font.sizeXs,
                              }}
                            >
                              {experimentStatusLabel(item.status)}
                            </span>
                          </div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                            {item.channel} · {new Date(item.start_at).toLocaleDateString()} → {new Date(item.end_at).toLocaleDateString()}
                          </div>
                          {linkedHypothesis ? (
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                              Hypothesis: {linkedHypothesis.title}
                            </div>
                          ) : null}
                        </button>
                      )
                    })}
                  </div>
                </SectionCard>
              </div>

              <SectionCard
                title="Experiment detail"
                subtitle="Inspect linked experiment status, measured results, and rollout guardrails without leaving Journey Lab."
                actions={
                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                    {selectedJourneyExperiment ? (
                      <a
                        href={buildIncrementalityHref(selectedJourneyExperiment.id)}
                        style={{
                          border: `1px solid ${t.color.border}`,
                          background: t.color.surface,
                          color: t.color.accent,
                          borderRadius: t.radius.sm,
                          padding: '6px 10px',
                          fontSize: t.font.sizeSm,
                          cursor: 'pointer',
                          textDecoration: 'none',
                        }}
                      >
                        Open in Incrementality
                      </a>
                    ) : null}
                    {selectedJourneyExperimentHypothesis ? (
                      <button
                        type="button"
                        onClick={() => loadHypothesisIntoDraft(selectedJourneyExperimentHypothesis)}
                        style={{
                          border: `1px solid ${t.color.border}`,
                          background: t.color.surface,
                          color: t.color.text,
                          borderRadius: t.radius.sm,
                          padding: '6px 10px',
                          fontSize: t.font.sizeSm,
                          cursor: 'pointer',
                        }}
                      >
                        Open hypothesis
                      </button>
                    ) : null}
                  </div>
                }
              >
                {!selectedJourneyExperiment ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Select a linked experiment to inspect its setup and measured readout.
                  </div>
                ) : (
                  <div style={{ display: 'grid', gap: t.space.md }}>
                    <div style={{ display: 'grid', gap: 6 }}>
                      <div style={{ fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                        {selectedJourneyExperiment.name}
                      </div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        {selectedJourneyExperiment.channel} · {experimentStatusLabel(selectedJourneyExperiment.status)} ·{' '}
                        {new Date(selectedJourneyExperiment.start_at).toLocaleDateString()} → {new Date(selectedJourneyExperiment.end_at).toLocaleDateString()}
                      </div>
                      {(selectedJourneyExperiment.source_type || selectedJourneyExperiment.source_name) && (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Source: {selectedJourneyExperiment.source_type || 'linked source'}
                          {selectedJourneyExperiment.source_name ? ` · ${selectedJourneyExperiment.source_name}` : ''}
                        </div>
                      )}
                    </div>

                    {linkedExperimentDetailQuery.isLoading ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading experiment detail…</div>
                    ) : null}
                    {linkedExperimentDetailQuery.isError ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(linkedExperimentDetailQuery.error as Error).message}</div>
                    ) : null}

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
                      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Ready to read</div>
                        <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                          {linkedExperimentHealthQuery.data?.ready_state.label
                            ? experimentStatusLabel(linkedExperimentHealthQuery.data.ready_state.label)
                            : '—'}
                        </div>
                      </div>
                      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Sample</div>
                        <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                          {linkedExperimentHealthQuery.data
                            ? `${linkedExperimentHealthQuery.data.sample.treatment.toLocaleString()} / ${linkedExperimentHealthQuery.data.sample.control.toLocaleString()}`
                            : '—'}
                        </div>
                      </div>
                      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed uplift</div>
                        <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                          {linkedExperimentResultsQuery.data?.uplift_abs != null
                            ? formatPercent(linkedExperimentResultsQuery.data.uplift_abs)
                            : '—'}
                        </div>
                      </div>
                      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>p-value</div>
                        <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                          {linkedExperimentResultsQuery.data?.p_value != null
                            ? linkedExperimentResultsQuery.data.p_value.toFixed(3)
                            : '—'}
                        </div>
                      </div>
                    </div>

                    {linkedExperimentResultsQuery.data?.insufficient_data ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                        Results are not ready yet. Keep assignments and outcomes flowing before making a decision.
                      </div>
                    ) : null}

                    {linkedExperimentHealthQuery.data?.ready_state.reasons?.length ? (
                      <div style={{ display: 'grid', gap: 6 }}>
                        {linkedExperimentHealthQuery.data.ready_state.reasons.map((reason, idx) => (
                          <div key={idx} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                            {reason}
                          </div>
                        ))}
                      </div>
                    ) : null}

                    {linkedExperimentDetailQuery.data ? (
                      <div style={{ display: 'grid', gap: t.space.sm }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>Experiment setup</div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Segment: {objectPreview(linkedExperimentDetailQuery.data.segment)}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Policy: {objectPreview(linkedExperimentDetailQuery.data.policy)}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Guardrails: {objectPreview(linkedExperimentDetailQuery.data.guardrails)}
                        </div>
                        {linkedExperimentDetailQuery.data.notes ? (
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                            Notes: {linkedExperimentDetailQuery.data.notes}
                          </div>
                        ) : null}
                      </div>
                    ) : null}

                    {selectedJourneyExperimentHypothesis ? (
                      <div style={{ display: 'grid', gap: 6 }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                          Linked hypothesis evidence
                        </div>
                        {renderHypothesisLearningCard(selectedJourneyExperimentHypothesis)}
                      </div>
                    ) : null}
                  </div>
                )}
              </SectionCard>
            </div>
          )}

          {!selectedDefinitionArchived && activeTab === 'policy' && (
            <div className="journeys-split-layout" style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'minmax(260px, 340px) minmax(0, 1fr)', minWidth: 0 }}>
              <div style={{ display: 'grid', gap: t.space.md, minWidth: 0 }}>
                <SectionCard title="Learned policies" subtitle="Promote validated journey policies or inspect what should be tested next.">
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    {policyRecommendationsQuery.isLoading ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading learned policies…</div>
                    ) : null}
                    {policyRecommendationsQuery.isError ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(policyRecommendationsQuery.error as Error).message}</div>
                    ) : null}
                    {!policyRecommendationsQuery.isLoading && !policyRecommendations.length ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        No policy evidence yet. Create hypotheses and run experiments first.
                      </div>
                    ) : null}
                    {policyRecommendations.slice(0, 3).map((item) => renderPolicyRecommendationCard(item))}
                  </div>
                </SectionCard>

                <SectionCard title="Hypotheses" subtitle="Pick a saved hypothesis to compare observed next-step candidates.">
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    {!hypotheses.length && (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        No hypotheses yet. Save one from Insights or the Hypotheses tab first.
                      </div>
                    )}
                    {hypotheses.map((item) => (
                      <button
                        key={item.id}
                        type="button"
                        onClick={() => {
                          setSandboxHypothesisId(item.id)
                          setSandboxCandidateStep('')
                        }}
                        style={{
                          textAlign: 'left',
                          border: `1px solid ${sandboxHypothesisId === item.id ? t.color.accent : t.color.borderLight}`,
                          background: sandboxHypothesisId === item.id ? t.color.accentMuted : t.color.surface,
                          borderRadius: t.radius.sm,
                          padding: '10px 12px',
                          cursor: 'pointer',
                        }}
                      >
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{item.title}</div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          {formatStageLabel(getHypothesisLearningStage(item))} · {item.support_count.toLocaleString()} journeys
                        </div>
                        {item.linked_experiment_id ? (
                          <div style={{ marginTop: 8 }}>{renderHypothesisLearningCard(item, true)}</div>
                        ) : null}
                      </button>
                    ))}
                  </div>
                </SectionCard>
              </div>

              <SectionCard title="Policy Sandbox" subtitle="Observational comparison of next-step candidates for the current hypothesis prefix.">
                {!sandboxHypothesis && (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Select a hypothesis to preview candidate next-step policies.
                  </div>
                )}
                {sandboxHypothesis && policySimulationQuery.isLoading && (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading policy preview…</div>
                )}
                {sandboxHypothesis && policySimulationQuery.isError && (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(policySimulationQuery.error as Error).message}</div>
                )}
                {sandboxHypothesis && policySimulationQuery.data && (
                  <div style={{ display: 'grid', gap: t.space.md }}>
                    {sandboxHypothesis.linked_experiment_id ? (
                      <div style={{ display: 'grid', gap: 6 }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                          Experiment evidence
                        </div>
                        {renderHypothesisLearningCard(sandboxHypothesis)}
                      </div>
                    ) : null}
                    {!policySimulationQuery.data.previewAvailable ? (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        {policySimulationQuery.data.reason || 'No observational preview available for this hypothesis.'}
                      </div>
                    ) : (
                      <>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
                          <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Prefix</div>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                              {policySimulationQuery.data.prefix?.label || '—'}
                            </div>
                          </div>
                          <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Eligible journeys</div>
                            <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                              {(policySimulationQuery.data.summary?.eligible_journeys ?? 0).toLocaleString()}
                            </div>
                          </div>
                          <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Baseline CVR</div>
                            <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                              {formatPercent(policySimulationQuery.data.summary?.baseline_conversion_rate ?? 0)}
                            </div>
                          </div>
                          <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed window</div>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                              {policySimulationQuery.data.source_window?.date_from || '—'} → {policySimulationQuery.data.source_window?.date_to || '—'}
                            </div>
                          </div>
                        </div>

                        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'start', flexWrap: 'wrap' }}>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Selected policy</div>
                              <div style={{ fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                                {policySimulationQuery.data.selected_policy?.step || '—'}
                              </div>
                              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 4 }}>
                                {policySimulationQuery.data.selected_policy?.rationale || 'No rationale available.'}
                              </div>
                            </div>
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                              Current step: <strong style={{ color: t.color.text }}>{policySimulationQuery.data.current_path?.step || '—'}</strong>
                            </div>
                          </div>
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: t.space.sm, marginTop: t.space.md }}>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Observed CVR</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                                {formatPercent(policySimulationQuery.data.selected_policy?.conversion_rate ?? 0)}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Delta vs current</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                                {formatPercent(policySimulationQuery.data.selected_policy?.uplift_abs ?? 0)}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Estimated incremental conversions</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                                {(policySimulationQuery.data.selected_policy?.estimated_incremental_conversions ?? 0).toLocaleString()}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Confidence</div>
                              <div style={{ fontSize: t.font.sizeBase, color: t.color.text, fontWeight: t.font.weightSemibold, textTransform: 'capitalize' }}>
                                {policySimulationQuery.data.selected_policy?.confidence || 'low'}
                              </div>
                            </div>
                          </div>
                        </div>

                        <div style={{ display: 'grid', gap: t.space.sm }}>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                            Candidate next steps
                          </div>
                          {(policySimulationQuery.data.top_candidates || []).map((candidate) => (
                            <button
                              key={candidate.step}
                              type="button"
                              onClick={() => setSandboxCandidateStep(candidate.step)}
                              style={{
                                textAlign: 'left',
                                border: `1px solid ${policySimulationQuery.data.selected_policy?.step === candidate.step ? t.color.accent : t.color.borderLight}`,
                                background: policySimulationQuery.data.selected_policy?.step === candidate.step ? t.color.accentMuted : t.color.surface,
                                borderRadius: t.radius.sm,
                                padding: '10px 12px',
                                cursor: 'pointer',
                              }}
                            >
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                                <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>
                                  #{candidate.rank} {candidate.step}
                                  {candidate.is_current_step ? ' · current' : ''}
                                </div>
                                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'capitalize' }}>
                                  {candidate.confidence} confidence
                                </div>
                              </div>
                              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: 4 }}>
                                {candidate.support_count.toLocaleString()} journeys · CVR {formatPercent(candidate.conversion_rate)} ·
                                uplift {formatPercent(candidate.uplift_abs)} · avg value {candidate.avg_value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                              </div>
                            </button>
                          ))}
                        </div>

                        {policySimulationQuery.data.decision?.warnings?.length ? (
                          <div style={{ display: 'grid', gap: 6 }}>
                            {policySimulationQuery.data.decision.warnings.map((warning, idx) => (
                              <div key={idx} style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                                {warning}
                              </div>
                            ))}
                          </div>
                        ) : null}

                        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                          {getHypothesisLearningStage(sandboxHypothesis) === 'validated' || readResultBool(getHypothesisPromotion(sandboxHypothesis), 'active') ? (
                            <button
                              type="button"
                              onClick={() =>
                                promoteHypothesisPolicyMutation.mutate({
                                  hypothesisId: sandboxHypothesis.id,
                                  active: !readResultBool(getHypothesisPromotion(sandboxHypothesis), 'active'),
                                })
                              }
                              disabled={promoteHypothesisPolicyMutation.isPending}
                              style={{
                                border: `1px solid ${readResultBool(getHypothesisPromotion(sandboxHypothesis), 'active') ? t.color.border : t.color.accent}`,
                                background: readResultBool(getHypothesisPromotion(sandboxHypothesis), 'active') ? t.color.surface : t.color.accent,
                                color: readResultBool(getHypothesisPromotion(sandboxHypothesis), 'active') ? t.color.text : '#fff',
                                borderRadius: t.radius.sm,
                                padding: '8px 12px',
                                fontSize: t.font.sizeSm,
                                cursor: promoteHypothesisPolicyMutation.isPending ? 'wait' : 'pointer',
                                opacity: promoteHypothesisPolicyMutation.isPending ? 0.8 : 1,
                              }}
                            >
                              {readResultBool(getHypothesisPromotion(sandboxHypothesis), 'active') ? 'Remove promotion' : 'Promote policy'}
                            </button>
                          ) : null}
                          <button
                            type="button"
                            onClick={applySandboxCandidateToDraft}
                            style={{
                              border: `1px solid ${t.color.border}`,
                              background: t.color.surface,
                              color: t.color.text,
                              borderRadius: t.radius.sm,
                              padding: '8px 12px',
                              fontSize: t.font.sizeSm,
                              cursor: 'pointer',
                            }}
                          >
                            Sync candidate to hypothesis
                          </button>
                          {!sandboxHypothesis.linked_experiment_id ? (
                            <button
                              type="button"
                              onClick={() => createExperimentFromHypothesis(sandboxHypothesis, policySimulationQuery.data.selected_policy?.step)}
                              disabled={createExperimentFromHypothesisMutation.isPending}
                              style={{
                                border: `1px solid ${t.color.accent}`,
                                background: t.color.accent,
                                color: '#fff',
                                borderRadius: t.radius.sm,
                                padding: '8px 12px',
                                fontSize: t.font.sizeSm,
                                cursor: createExperimentFromHypothesisMutation.isPending ? 'wait' : 'pointer',
                                opacity: createExperimentFromHypothesisMutation.isPending ? 0.8 : 1,
                              }}
                            >
                              {createExperimentFromHypothesisMutation.isPending ? 'Creating experiment…' : 'Create experiment'}
                            </button>
                          ) : (
                            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                              Linked experiment #{sandboxHypothesis.linked_experiment_id} already exists for this hypothesis. Use the experimental readout above as the primary source of truth; keep the observational sandbox for follow-up variants.
                            </div>
                          )}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </SectionCard>
            </div>
          )}

          {!selectedDefinitionArchived && activeTab === 'paths' && (
            <>
              <AnalyticsTable
                columns={pathTableColumns}
                rows={pathsQuery.isLoading || pathsQuery.isError ? [] : filteredPaths}
                rowKey={(row, idx) =>
                  `${row.path_hash}-${row.device || ''}-${row.country || ''}-${row.channel_group || ''}-${row.campaign_id || ''}-${idx}`
                }
                tableLabel="Journey paths"
                stickyFirstColumn
                allowColumnHiding
                allowDensityToggle
                persistKey="journey-paths-table"
                defaultHiddenColumnKeys={['p50_time_to_convert_sec', 'p90_time_to_convert_sec', 'credit_overlay']}
                presets={[
                  {
                    key: 'overview',
                    label: 'Overview',
                    visibleColumnKeys: ['path_steps', 'count_journeys', 'conversion_rate', 'avg_time_to_convert_sec'],
                  },
                  {
                    key: 'timing',
                    label: 'Timing',
                    visibleColumnKeys: ['path_steps', 'count_journeys', 'avg_time_to_convert_sec', 'p50_time_to_convert_sec', 'p90_time_to_convert_sec'],
                  },
                  {
                    key: 'credit',
                    label: 'Credit',
                    visibleColumnKeys: ['path_steps', 'count_journeys', 'conversion_rate', 'credit_overlay'],
                  },
                ]}
                defaultPresetKey="overview"
                onRowClick={(row) => setSelectedPath(row)}
                isRowActive={(row) =>
                  !!selectedPath &&
                  row.path_hash === selectedPath.path_hash &&
                  (row.channel_group || '') === (selectedPath.channel_group || '') &&
                  (row.campaign_id || '') === (selectedPath.campaign_id || '') &&
                  (row.device || '') === (selectedPath.device || '') &&
                  (row.country || '') === (selectedPath.country || '')
                }
                toolbar={
                  <AnalyticsToolbar
                    searchValue={pathSearch}
                    onSearchChange={setPathSearch}
                    searchPlaceholder="Search path steps…"
                    actions={
                      <>
                        <select
                          value={pathSortBy}
                          onChange={(e) => setPathSortBy(e.target.value as PathSortBy)}
                          style={{ padding: '7px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeSm }}
                        >
                          <option value="journeys">Sort: Journeys</option>
                          <option value="conversion_rate">Sort: Conversion rate</option>
                          <option value="avg_time">Sort: Avg time-to-convert</option>
                        </select>
                        <button
                          type="button"
                          onClick={() => setPathSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'))}
                          style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, fontSize: t.font.sizeSm, padding: '7px 10px', cursor: 'pointer' }}
                        >
                          {pathSortDir === 'asc' ? 'Ascending' : 'Descending'}
                        </button>
                        <select
                          value={pathsLimit}
                          onChange={(e) => {
                            setPathsLimit(Number(e.target.value))
                            setPathsPage(1)
                          }}
                          style={{ padding: '7px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeSm }}
                        >
                          {[25, 50, 100].map((v) => (
                            <option key={v} value={v}>
                              {v} rows
                            </option>
                          ))}
                        </select>
                      </>
                    }
                  />
                }
                pagination={
                  <>
                    <button
                      type="button"
                      onClick={() => setPathsPage((p) => Math.max(1, p - 1))}
                      disabled={pathsPage <= 1}
                      style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '6px 10px', cursor: pathsPage <= 1 ? 'not-allowed' : 'pointer' }}
                    >
                      Prev
                    </button>
                    <span>
                      Page {pathsPage} of {maxPage}
                    </span>
                    <button
                      type="button"
                      onClick={() => setPathsPage((p) => Math.min(maxPage, p + 1))}
                      disabled={pathsPage >= maxPage}
                      style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '6px 10px', cursor: pathsPage >= maxPage ? 'not-allowed' : 'pointer' }}
                    >
                      Next
                    </button>
                  </>
                }
                emptyState={
                  pathsQuery.isLoading
                    ? 'Loading path aggregates…'
                    : pathsQuery.isError
                    ? (pathsQuery.error as Error).message
                    : 'No paths for the selected journey and filters.'
                }
              />
            </>
          )}

          {!selectedDefinitionArchived && activeTab === 'funnels' && (
            <div style={{ display: 'grid', gap: t.space.md }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Build funnels from journey steps and inspect drop-offs.
                </div>
                <button
                  type="button"
                  onClick={() => {
                    setShowCreateFunnelModal(true)
                    setCreateFunnelError(null)
                    setFunnelDraft((prev) => ({ ...prev, name: prev.name || 'New funnel' }))
                  }}
                  style={{
                    border: `1px solid ${t.color.accent}`,
                    background: t.color.accent,
                    color: '#fff',
                    borderRadius: t.radius.sm,
                    fontSize: t.font.sizeSm,
                    padding: '8px 12px',
                    cursor: 'pointer',
                  }}
                >
                  Create funnel
                </button>
              </div>

              <div className="journeys-split-layout" style={{ display: 'grid', gridTemplateColumns: 'minmax(240px, 320px) 1fr', gap: t.space.md, minWidth: 0 }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, minWidth: 0 }}>
                  <div style={{ fontSize: t.font.sizeXs, textTransform: 'uppercase', color: t.color.textMuted, marginBottom: t.space.sm }}>
                    Funnels
                  </div>
                  <div style={{ display: 'grid', gap: 6 }}>
                    {(funnelListQuery.data?.items ?? []).map((f) => (
                      <button
                        key={f.id}
                        type="button"
                        onClick={() => {
                          setSelectedFunnelId(f.id)
                          setSelectedFunnelStep(null)
                        }}
                        style={{
                          textAlign: 'left',
                          border: `1px solid ${selectedFunnelId === f.id ? t.color.accent : t.color.borderLight}`,
                          background: selectedFunnelId === f.id ? t.color.accentMuted : t.color.surface,
                          borderRadius: t.radius.sm,
                          padding: '8px 10px',
                          cursor: 'pointer',
                        }}
                      >
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{f.name}</div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{f.steps.length} steps</div>
                      </button>
                    ))}
                    {!funnelListQuery.isLoading && !(funnelListQuery.data?.items ?? []).length && (
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No funnels yet.</div>
                    )}
                  </div>
                </div>

                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, minWidth: 0 }}>
                  {!selectedFunnelId && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Select a funnel to view results.</div>}
                  {selectedFunnelId && funnelResultsQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading funnel results…</div>}
                  {selectedFunnelId && funnelResultsQuery.isError && (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(funnelResultsQuery.error as Error).message}</div>
                  )}
                  {selectedFunnelId && funnelResultsQuery.data && (
                    <div style={{ display: 'grid', gap: t.space.md }}>
                      {funnelResultsQuery.data.meta.warning && (
                        <div style={{ border: `1px solid ${t.color.warning}`, background: t.color.warningSubtle, color: t.color.warning, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeSm }}>
                          {funnelResultsQuery.data.meta.warning}
                        </div>
                      )}

                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px,1fr))', gap: t.space.sm }}>
                        {funnelResultsQuery.data.steps.map((s) => (
                          <button
                            key={s.position}
                            type="button"
                            onClick={() => setSelectedFunnelStep(s.step)}
                            style={{
                              textAlign: 'left',
                              border: `1px solid ${selectedFunnelStep === s.step ? t.color.accent : t.color.borderLight}`,
                              borderRadius: t.radius.sm,
                              padding: t.space.sm,
                              background: selectedFunnelStep === s.step ? t.color.accentMuted : t.color.surface,
                              cursor: 'pointer',
                            }}
                          >
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{s.position}. {s.step}</div>
                            <div style={{ fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightSemibold }}>{s.count.toLocaleString()}</div>
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                              {s.dropoff_pct == null ? '—' : `Drop-off ${formatPercent(s.dropoff_pct)}`}
                            </div>
                          </button>
                        ))}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Click a step to open diagnostics and evidence.
                      </div>

                      <div>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium, marginBottom: 6 }}>Time between steps</div>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                          {funnelResultsQuery.data.time_between_steps.map((tb) => `${tb.from_step} → ${tb.to_step}: avg ${formatSeconds(tb.avg_sec)}, p50 ${formatSeconds(tb.p50_sec)}, p90 ${formatSeconds(tb.p90_sec)}`).join(' • ') || 'No timing data.'}
                        </div>
                      </div>

                      <div className="journeys-two-col-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md, minWidth: 0 }}>
                        <div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium, marginBottom: 6 }}>Device (top 5)</div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                            {(funnelResultsQuery.data.breakdown.device || []).map((d) => `${d.key}: ${d.count}`).join(' • ') || 'No data'}
                          </div>
                        </div>
                        <div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium, marginBottom: 6 }}>Channel group (top 5)</div>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                            {(funnelResultsQuery.data.breakdown.channel_group || []).map((d) => `${d.key}: ${d.count}`).join(' • ') || 'No data'}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {!selectedDefinitionArchived && activeTab === 'flow' && (
            <div style={{ display: 'grid', gap: t.space.md }}>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'end' }}>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Min transitions
                  <input type="number" min={1} max={1000} value={flowMinCount} onChange={(e) => setFlowMinCount(Math.max(1, Number(e.target.value) || 1))} style={{ width: 120, padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Max nodes
                  <input type="number" min={2} max={200} value={flowMaxNodes} onChange={(e) => setFlowMaxNodes(Math.max(2, Math.min(200, Number(e.target.value) || 2)))} style={{ width: 120, padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Max depth
                  <input type="number" min={1} max={20} value={flowMaxDepth} onChange={(e) => setFlowMaxDepth(Math.max(1, Math.min(20, Number(e.target.value) || 1)))} style={{ width: 120, padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
              </div>

              {transitionsQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading flow transitions…</div>}
              {transitionsQuery.isError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(transitionsQuery.error as Error).message}</div>}
              {!transitionsQuery.isLoading && !transitionsQuery.isError && !(transitionsQuery.data?.edges ?? []).length && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No transition flow available for the selected date range and filters.</div>
              )}
              {!!transitionsQuery.data?.edges?.length && (
                <div className="journeys-split-layout" style={{ display: 'grid', gridTemplateColumns: 'minmax(240px, 320px) 1fr', gap: t.space.md, minWidth: 0 }}>
                  <SectionCard title="Nodes" subtitle={`${transitionsQuery.data.nodes.length} visible nodes`}>
                    <div style={{ display: 'grid', gap: 8 }}>
                      {transitionsQuery.data.nodes.map((node) => (
                        <div key={node.id} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{node.label}</div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                            In {node.count_in.toLocaleString()} · Out {node.count_out.toLocaleString()} · Total {node.count_total.toLocaleString()}
                          </div>
                        </div>
                      ))}
                    </div>
                  </SectionCard>
                  <SectionCard title="Top transitions" subtitle={`Dropped ${transitionsQuery.data.meta.dropped_edges || 0} low-volume edges`}>
                    <div style={{ display: 'grid', gap: 10 }}>
                      {flowTopEdges.map((edge) => {
                        const maxValue = flowTopEdges[0]?.value || 1
                        return (
                          <button
                            key={`${edge.source}-${edge.target}`}
                            type="button"
                            onClick={() => {
                              setExamplesStepFilter(edge.target)
                              setExamplesPathHash('')
                              setActiveTab('examples')
                            }}
                            style={{ border: `1px solid ${t.color.borderLight}`, background: t.color.surface, borderRadius: t.radius.sm, padding: t.space.sm, textAlign: 'left', cursor: 'pointer' }}
                          >
                            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.text }}>
                              <span>{edge.source} → {edge.target}</span>
                              <span>{edge.value.toLocaleString()}</span>
                            </div>
                            <div style={{ marginTop: 6, height: 8, borderRadius: t.radius.full, background: t.color.bgSubtle, overflow: 'hidden' }}>
                              <div style={{ width: `${Math.max(8, (edge.value / maxValue) * 100)}%`, height: '100%', background: t.color.accent }} />
                            </div>
                          </button>
                        )
                      })}
                    </div>
                  </SectionCard>
                </div>
              )}
            </div>
          )}

          {!selectedDefinitionArchived && activeTab === 'examples' && (
            <div style={{ display: 'grid', gap: t.space.md }}>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'end' }}>
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, flex: '1 1 240px' }}>
                  Step contains
                  <input value={examplesStepFilter} onChange={(e) => setExamplesStepFilter(e.target.value)} placeholder="Checkout / Form Submit" style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                </label>
                <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                  <button type="button" onClick={() => { setExamplesPathHash(''); setExamplesStepFilter('') }} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>
                    Clear filters
                  </button>
                </div>
              </div>

              {(examplesPathHash || examplesStepFilter.trim()) && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Scoped to {examplesPathHash ? `path ${examplesPathHash.slice(0, 8)}` : 'all paths'}{examplesStepFilter.trim() ? ` · step match "${examplesStepFilter.trim()}"` : ''}.
                </div>
              )}

              {examplesQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading example journeys…</div>}
              {examplesQuery.isError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(examplesQuery.error as Error).message}</div>}
              {!examplesQuery.isLoading && !examplesQuery.isError && !(examplesQuery.data?.items ?? []).length && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No matching journey examples for the selected filters.</div>
              )}
              {!!examplesQuery.data?.items?.length && (
                <div style={{ display: 'grid', gap: t.space.md }}>
                  {examplesQuery.data.items.map((item) => (
                    <SectionCard
                      key={`${item.conversion_id}-${item.profile_id}`}
                      title={item.steps.join(' → ')}
                      subtitle={`Profile ${item.profile_id} · Converted ${formatDateTime(item.conversion_ts)}`}
                      actions={<span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.touchpoints_count} touchpoints</span>}
                    >
                      <div style={{ display: 'grid', gap: t.space.sm }}>
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          {item.steps.map((step) => pathChip(step))}
                        </div>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                          Value {item.conversion_value.toLocaleString(undefined, { maximumFractionDigits: 2 })} · {item.dimensions.channel_group || 'unknown'} · {item.dimensions.device || 'unknown'} · {item.dimensions.country || '—'}
                        </div>
                        <div style={{ display: 'grid', gap: 6 }}>
                          {item.touchpoints_preview.map((tp, idx) => (
                            <div key={`${item.conversion_id}-${idx}`} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                              {formatDateTime(tp.ts)} · {tp.channel || 'unknown'}{tp.campaign ? ` / ${tp.campaign}` : ''}{tp.event ? ` · ${tp.event}` : ''}
                            </div>
                          ))}
                        </div>
                      </div>
                    </SectionCard>
                  ))}
                </div>
              )}
            </div>
          )}
          </div>
        </SectionCard>

        <div ref={savedViewsSectionRef}>
          <SectionCard title="Saved views" subtitle="Save the current journey, tab, filters, and examples scope to your account.">
          <div style={{ display: 'grid', gap: t.space.md }}>
            <div style={{ display: 'flex', alignItems: 'end', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, flex: '1 1 260px' }}>
                View name
                <input value={savedViewName} onChange={(e) => setSavedViewName(e.target.value)} placeholder="Weekly checkout investigation" style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
              </label>
              <button type="button" onClick={saveCurrentView} disabled={!savedViewName.trim() || !selectedJourneyId || createSavedViewMutation.isPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, fontSize: t.font.sizeSm, padding: '8px 12px', cursor: !savedViewName.trim() || !selectedJourneyId || createSavedViewMutation.isPending ? 'not-allowed' : 'pointer', opacity: !savedViewName.trim() || !selectedJourneyId || createSavedViewMutation.isPending ? 0.7 : 1 }}>
                {createSavedViewMutation.isPending ? 'Saving…' : 'Save current view'}
              </button>
            </div>
            {savedViewsQuery.isError && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                {(savedViewsQuery.error as Error).message}
              </div>
            )}
            {createSavedViewMutation.isError && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                {(createSavedViewMutation.error as Error).message}
              </div>
            )}
            {deleteSavedViewMutation.isError && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
                {(deleteSavedViewMutation.error as Error).message}
              </div>
            )}
            {savedViewsQuery.isLoading ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Loading saved views…
              </div>
            ) : null}
            {!savedViewsQuery.isLoading && !savedViews.length ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                No saved views yet. Current state is still URL-shareable.
              </div>
            ) : (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                {savedViews.map((view) => (
                  <div key={view.id} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.md, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                    <div style={{ display: 'grid', gap: 2 }}>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{view.name}</div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        {view.activeTab} · {view.filters.dateFrom} → {view.filters.dateTo} · {formatDateTime(view.createdAt)}
                      </div>
                    </div>
                    <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                      <button type="button" onClick={() => applySavedView(view)} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '6px 10px', cursor: 'pointer' }}>
                        Apply
                      </button>
                      <button type="button" onClick={() => deleteSavedView(view.id)} disabled={deleteSavedViewMutation.isPending} style={{ border: `1px solid ${t.color.border}`, background: t.color.bgSubtle, color: t.color.textSecondary, borderRadius: t.radius.sm, padding: '6px 10px', cursor: deleteSavedViewMutation.isPending ? 'not-allowed' : 'pointer', opacity: deleteSavedViewMutation.isPending ? 0.7 : 1 }}>
                        {deleteSavedViewMutation.isPending ? 'Deleting…' : 'Delete'}
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
          </SectionCard>
        </div>
      </DashboardPage>

      {selectedPath && (
        <>
          <div role="presentation" onClick={() => setSelectedPath(null)} style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(15,23,42,0.4)', zIndex: 40 }} />
          <div role="dialog" aria-label="Path details" style={{ position: 'fixed', top: 0, right: 0, width: 'min(480px, 100vw)', height: '100vh', backgroundColor: t.color.surface, borderLeft: `1px solid ${t.color.borderLight}`, boxShadow: t.shadow, zIndex: 41, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <div style={{ padding: t.space.lg, borderBottom: `1px solid ${t.color.borderLight}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Path details</h3>
              <button type="button" onClick={() => setSelectedPath(null)} style={{ border: 'none', background: 'transparent', cursor: 'pointer', fontSize: t.font.sizeLg, color: t.color.textSecondary }}>×</button>
            </div>
            <div style={{ flex: 1, overflowY: 'auto', padding: t.space.lg, display: 'grid', gap: t.space.lg }}>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>{normalizeSteps(selectedPath.path_steps).map((s) => pathChip(s))}</div>
              <div className="journeys-two-col-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md, minWidth: 0 }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}><div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Journeys</div><div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold }}>{selectedPath.count_journeys.toLocaleString()}</div></div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}><div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Conversion rate</div><div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold }}>{formatPercent(selectedPath.conversion_rate)}</div></div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}><div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Avg time</div><div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold }}>{formatSeconds(selectedPath.avg_time_to_convert_sec)}</div></div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}><div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>P50 / P90</div><div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold }}>{formatSeconds(selectedPath.p50_time_to_convert_sec)} / {formatSeconds(selectedPath.p90_time_to_convert_sec)}</div></div>
              </div>

              <SectionCard
                title="Credit split"
                subtitle="Attribution overlay for this path scope."
                actions={
                  <select
                    value={attributionModel}
                    onChange={(e) => setAttributionModel(e.target.value)}
                    style={{ padding: '6px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeSm }}
                  >
                    {ATTR_MODELS.map((m) => (
                      <option key={m.id} value={m.id}>
                        {m.label}
                      </option>
                    ))}
                  </select>
                }
              >
                <div style={{ display: 'grid', gap: 8 }}>
                  {pathAttributionQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading credit split…</div>}
                  {!pathAttributionQuery.isLoading && !drawerTop.length && (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No credit data for this path.</div>
                  )}
                  {drawerTop.map((c) => (
                    <div key={c.channel} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: t.font.sizeSm }}>
                      <span>{c.channel}</span>
                      <span style={{ color: t.color.textSecondary }}>{formatPercent(c.attributed_share)}</span>
                    </div>
                  ))}
                  {drawerCredits.length > 3 && (
                    <button
                      type="button"
                      onClick={() => setCreditExpanded((prev) => !prev)}
                      style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '6px 10px', fontSize: t.font.sizeSm, cursor: 'pointer' }}
                    >
                      {creditExpanded ? 'Show less' : `Show more (${drawerCredits.length - 3})`}
                    </button>
                  )}
                </div>
              </SectionCard>

              <SectionCard title="Breakdown by device" subtitle="Conversion outcomes for this path across device slices in the current filter set.">
                <div style={{ display: 'grid', gap: 8 }}>
                  {!pathBreakdownByDevice.length && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No device breakdown available.</div>}
                  {pathBreakdownByDevice.map((row) => (
                    <div key={row.key} className="journeys-metric-row" style={{ display: 'grid', gridTemplateColumns: 'minmax(120px, 1fr) repeat(3, minmax(90px, auto))', gap: t.space.sm, fontSize: t.font.sizeSm, alignItems: 'center', minWidth: 0 }}>
                      <span style={{ color: t.color.text }}>{row.key}</span>
                      <span style={{ color: t.color.textSecondary }}>{row.journeys.toLocaleString()} journeys</span>
                      <span style={{ color: t.color.textSecondary }}>{row.conversions.toLocaleString()} conv.</span>
                      <span style={{ color: t.color.textSecondary }}>{formatPercent(row.conversionRate)}</span>
                    </div>
                  ))}
                </div>
              </SectionCard>
              <SectionCard title="Breakdown by channel group" subtitle="Path performance split by channel group across matching path rows.">
                <div style={{ display: 'grid', gap: 8 }}>
                  {!pathBreakdownByChannelGroup.length && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No channel-group breakdown available.</div>}
                  {pathBreakdownByChannelGroup.map((row) => (
                    <div key={row.key} className="journeys-metric-row" style={{ display: 'grid', gridTemplateColumns: 'minmax(120px, 1fr) repeat(3, minmax(90px, auto))', gap: t.space.sm, fontSize: t.font.sizeSm, alignItems: 'center', minWidth: 0 }}>
                      <span style={{ color: t.color.text }}>{row.key}</span>
                      <span style={{ color: t.color.textSecondary }}>{row.journeys.toLocaleString()} journeys</span>
                      <span style={{ color: t.color.textSecondary }}>{row.conversions.toLocaleString()} conv.</span>
                      <span style={{ color: t.color.textSecondary }}>{formatPercent(row.conversionRate)}</span>
                    </div>
                  ))}
                </div>
              </SectionCard>

              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
                  <select
                    value={comparePathHash}
                    onChange={(e) => setComparePathHash(e.target.value)}
                    style={{ minWidth: 0, width: 'min(100%, 360px)', padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeSm }}
                  >
                    <option value="">Compare against another path…</option>
                    {compareCandidateOptions.map((row) => (
                      <option key={row.path_hash} value={row.path_hash}>
                        {normalizeSteps(row.path_steps).join(' → ')}
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    onClick={() => {
                      setExamplesPathHash(selectedPath.path_hash)
                      setExamplesStepFilter('')
                      setActiveTab('examples')
                      setSelectedPath(null)
                    }}
                    style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '8px 12px', fontSize: t.font.sizeSm, cursor: 'pointer' }}
                  >
                    Open examples
                  </button>
                </div>
                {comparedPath && (
                  <SectionCard title="Path comparison" subtitle="Delta versus another path in the current result set.">
                    <div style={{ display: 'grid', gap: 8, fontSize: t.font.sizeSm }}>
                      <div style={{ color: t.color.textSecondary }}>{normalizeSteps(comparedPath.path_steps).join(' → ')}</div>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: t.space.sm }}>
                        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>Journeys Δ {((selectedPath.count_journeys || 0) - (comparedPath.count_journeys || 0)).toLocaleString()}</div>
                        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>CR Δ {formatPercent((selectedPath.conversion_rate || 0) - (comparedPath.conversion_rate || 0))}</div>
                        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>Avg time Δ {formatSeconds((selectedPath.avg_time_to_convert_sec || 0) - (comparedPath.avg_time_to_convert_sec || 0))}</div>
                      </div>
                    </div>
                  </SectionCard>
                )}
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <button
                  type="button"
                  onClick={() =>
                    openCreateAlertModal(
                      {
                        name: `Path conversion drop: ${normalizeSteps(selectedPath.path_steps).join(' → ')}`,
                        type: 'path_cr_drop',
                        domain: 'journeys',
                        metric: 'conversion_rate',
                      },
                      {
                        journey_definition_id: selectedJourneyId,
                        path_hash: selectedPath.path_hash,
                        filters: {
                          channel_group: filters.channel !== 'all' ? filters.channel : null,
                          campaign_id: filters.campaign !== 'all' ? filters.campaign : null,
                          device: filters.device !== 'all' ? filters.device : null,
                          country: filters.geo !== 'all' ? filters.geo : null,
                        },
                      },
                    )
                  }
                  style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '8px 12px', fontSize: t.font.sizeSm, cursor: 'pointer' }}
                >
                  Create alert
                </button>
                {funnelBuilderEnabled && (
                  <button
                    type="button"
                    onClick={() => {
                      const steps = normalizeSteps(selectedPath.path_steps)
                      setFunnelDraft({
                        name: `Funnel from ${steps[0] || 'path'}`,
                        description: `Derived from path ${selectedPath.path_hash.slice(0, 8)}`,
                        stepsText: steps.join('\n'),
                        counting_method: 'ordered',
                        window_days: selectedDefinition?.lookback_window_days ?? 30,
                      })
                      setShowCreateFunnelModal(true)
                    }}
                    style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 12px', fontSize: t.font.sizeSm, cursor: 'pointer' }}
                  >
                    Build funnel from this path
                  </button>
                )}
              </div>
            </div>
          </div>
        </>
      )}

      {selectedFunnelStep && (
        <>
          <div
            role="presentation"
            onClick={() => setSelectedFunnelStep(null)}
            style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(15,23,42,0.4)', zIndex: 40 }}
          />
          <div
            role="dialog"
            aria-label="Funnel step diagnostics"
            style={{
              position: 'fixed',
              top: 0,
              right: 0,
              width: 'min(560px, 100vw)',
              height: '100vh',
              backgroundColor: t.color.surface,
              borderLeft: `1px solid ${t.color.borderLight}`,
              boxShadow: t.shadow,
              zIndex: 41,
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
            }}
          >
            <div
              style={{
                padding: t.space.lg,
                borderBottom: `1px solid ${t.color.borderLight}`,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div>
                <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Step diagnostics</h3>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{selectedFunnelStep}</div>
              </div>
              <button
                type="button"
                onClick={() => setSelectedFunnelStep(null)}
                style={{ border: 'none', background: 'transparent', cursor: 'pointer', fontSize: t.font.sizeLg, color: t.color.textSecondary }}
              >
                ×
              </button>
            </div>
            <div style={{ flex: 1, overflowY: 'auto', padding: t.space.lg, display: 'grid', gap: t.space.md }}>
              {funnelDiagnosticsQuery.isLoading && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading diagnostics…</div>
              )}
              {funnelDiagnosticsQuery.isError && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(funnelDiagnosticsQuery.error as Error).message}</div>
              )}
              {!funnelDiagnosticsQuery.isLoading && !funnelDiagnosticsQuery.isError && !(funnelDiagnosticsQuery.data || []).length && (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No diagnostics available for this step and filter set.</div>
              )}
              {(funnelDiagnosticsQuery.data || []).map((d, idx) => {
                const conf = (d.confidence || 'low').toLowerCase()
                const confColor = conf === 'high' ? t.color.success : conf === 'medium' ? t.color.warning : t.color.textMuted
                const confBg = conf === 'high' ? t.color.successMuted : conf === 'medium' ? t.color.warningSubtle : t.color.bgSubtle
                return (
                  <SectionCard
                    key={`${d.title}-${idx}`}
                    title={d.title}
                    subtitle="Hypothesis based on observed period-over-period signals."
                    actions={
                      <span
                        style={{
                          border: `1px solid ${confColor}`,
                          color: confColor,
                          background: confBg,
                          borderRadius: t.radius.full,
                          padding: '4px 8px',
                          fontSize: t.font.sizeXs,
                          fontWeight: t.font.weightMedium,
                          textTransform: 'capitalize',
                        }}
                      >
                        {d.confidence} confidence
                      </span>
                    }
                  >
                    <div style={{ display: 'grid', gap: t.space.sm }}>
                      <div>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium, marginBottom: 4 }}>
                          Evidence
                        </div>
                        <ul style={{ margin: 0, paddingLeft: 18, display: 'grid', gap: 6 }}>
                          {(d.evidence || []).map((ev, i) => (
                            <li key={i} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                              {ev}
                            </li>
                          ))}
                        </ul>
                      </div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        <strong style={{ color: t.color.text }}>Impact estimate:</strong>{' '}
                        {(d.impact_estimate?.direction || 'unknown').toString()}
                        {typeof d.impact_estimate?.estimated_users_affected === 'number'
                          ? ` · ~${d.impact_estimate.estimated_users_affected.toLocaleString()} users affected`
                          : ''}
                      </div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        <strong style={{ color: t.color.text }}>Next action:</strong> {d.next_action}
                      </div>
                    </div>
                  </SectionCard>
                )
              })}
            </div>
            <div
              style={{
                borderTop: `1px solid ${t.color.borderLight}`,
                padding: t.space.md,
                display: 'flex',
                gap: t.space.sm,
                justifyContent: 'flex-end',
                flexWrap: 'wrap',
              }}
            >
              <button
                type="button"
                onClick={() => {
                  setExamplesPathHash('')
                  setExamplesStepFilter(selectedFunnelStep || '')
                  setActiveTab('examples')
                  setSelectedFunnelStep(null)
                }}
                style={{
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  color: t.color.text,
                  borderRadius: t.radius.sm,
                  padding: '8px 12px',
                  fontSize: t.font.sizeSm,
                  cursor: 'pointer',
                }}
              >
                Open related examples
              </button>
              <button
                type="button"
                onClick={() =>
                  openCreateAlertModal(
                    {
                      name: `Funnel dropoff spike: ${selectedFunnelStep || 'step'}`,
                      type: 'funnel_dropoff_spike',
                      domain: 'funnels',
                      metric: 'dropoff_rate',
                    },
                    {
                      journey_definition_id: selectedJourneyId,
                      funnel_id: selectedFunnelId,
                      step_index: (() => {
                        const idx = (funnelResultsQuery.data?.steps || []).findIndex((s) => s.step === selectedFunnelStep)
                        return idx < 0 ? 0 : idx
                      })(),
                      filters: {
                        channel_group: filters.channel !== 'all' ? filters.channel : null,
                        campaign_id: filters.campaign !== 'all' ? filters.campaign : null,
                        device: filters.device !== 'all' ? filters.device : null,
                        country: filters.geo !== 'all' ? filters.geo : null,
                      },
                    },
                  )
                }
                style={{
                  border: `1px solid ${t.color.accent}`,
                  background: t.color.accent,
                  color: '#fff',
                  borderRadius: t.radius.sm,
                  padding: '8px 12px',
                  fontSize: t.font.sizeSm,
                  cursor: 'pointer',
                }}
              >
                Create alert
              </button>
            </div>
          </div>
        </>
      )}

      {showCreateModal && canManageDefinitions && (
        <CreateJourneyModal
          draft={draft}
          kpiOptions={kpiOptions}
          errorMessage={createError}
          submitting={createMutation.isPending}
          title="Create journey"
          subtitle="Start a new journey definition for this workspace. You can refine lifecycle, settings, and outputs afterward."
          submitLabel="Create"
          onClose={() => {
            setShowCreateModal(false)
            setCreateError(null)
          }}
          onSubmit={submitCreate}
          onDraftChange={setDraft}
          onClampLookback={clampLookback}
        />
      )}

      {showEditModal && canManageDefinitions && selectedDefinition && (
        <CreateJourneyModal
          draft={draft}
          kpiOptions={kpiOptions}
          errorMessage={createError}
          submitting={updateMutation.isPending}
          title="Edit journey"
          subtitle="Update the active definition metadata and defaults. Saving will rebuild journey outputs in the background."
          submitLabel="Save changes"
          onClose={() => {
            setShowEditModal(false)
            setCreateError(null)
          }}
          onSubmit={submitEdit}
          onDraftChange={setDraft}
          onClampLookback={clampLookback}
        />
      )}

      {showCreateFunnelModal && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 55, background: 'rgba(15, 23, 42, 0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
          <div style={{ width: 'min(620px, 100%)', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
            <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Create funnel</h3>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Name
              <input value={funnelDraft.name} onChange={(e) => setFunnelDraft((p) => ({ ...p, name: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Description
              <textarea value={funnelDraft.description} onChange={(e) => setFunnelDraft((p) => ({ ...p, description: e.target.value }))} rows={2} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Steps (one per line)
              <textarea value={funnelDraft.stepsText} onChange={(e) => setFunnelDraft((p) => ({ ...p, stepsText: e.target.value }))} rows={6} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Window (days)
              <input type="number" min={1} max={365} value={funnelDraft.window_days} onChange={(e) => setFunnelDraft((p) => ({ ...p, window_days: clampLookback(Number(e.target.value)) }))} style={{ width: 180, padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            </label>
            {createFunnelError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{createFunnelError}</div>}
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
              <button type="button" onClick={() => setShowCreateFunnelModal(false)} style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>Cancel</button>
              <button type="button" onClick={submitCreateFunnel} disabled={createFunnelMutation.isPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 14px', cursor: createFunnelMutation.isPending ? 'wait' : 'pointer' }}>{createFunnelMutation.isPending ? 'Creating…' : 'Create funnel'}</button>
            </div>
          </div>
        </div>
      )}

      {showCreateAlertModal && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 56, background: 'rgba(15, 23, 42, 0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
          <div style={{ width: 'min(620px, 100%)', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
            <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Create alert</h3>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Name
              <input value={alertDraft.name} onChange={(e) => setAlertDraft((p) => ({ ...p, name: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Metric
              <select value={alertDraft.metric} onChange={(e) => setAlertDraft((p) => ({ ...p, metric: e.target.value }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
                {alertDraft.type === 'path_cr_drop' && <option value="conversion_rate">Path conversion rate</option>}
                {alertDraft.type === 'path_volume_change' && <option value="count_journeys">Path journeys volume</option>}
                {alertDraft.type === 'funnel_dropoff_spike' && <option value="dropoff_rate">Funnel drop-off rate</option>}
                {alertDraft.type === 'ttc_shift' && <option value="p50_time_to_convert_sec">P50 time-to-convert</option>}
              </select>
            </label>
            <div className="journeys-two-col-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md, minWidth: 0 }}>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Comparison mode
                <select value={alertDraft.comparison_mode} onChange={(e) => setAlertDraft((p) => ({ ...p, comparison_mode: e.target.value as AlertDraft['comparison_mode'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
                  <option value="previous_period">Previous period</option>
                  <option value="rolling_baseline">Rolling baseline (7 vs 28)</option>
                </select>
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Sensitivity (%)
                <input type="number" min={1} max={200} value={alertDraft.threshold_pct} onChange={(e) => setAlertDraft((p) => ({ ...p, threshold_pct: Math.max(1, Math.min(200, Number(e.target.value) || 1)) }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
              </label>
            </div>
            <div className="journeys-two-col-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md, minWidth: 0 }}>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Severity
                <select value={alertDraft.severity} onChange={(e) => setAlertDraft((p) => ({ ...p, severity: e.target.value as AlertDraft['severity'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
                  <option value="info">Info</option>
                  <option value="warn">Warn</option>
                  <option value="critical">Critical</option>
                </select>
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Cooldown (days)
                <input type="number" min={1} max={30} value={alertDraft.cooldown_days} onChange={(e) => setAlertDraft((p) => ({ ...p, cooldown_days: Math.max(1, Math.min(30, Number(e.target.value) || 1)) }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
              </label>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bgSubtle }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm, flexWrap: 'wrap' }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>Test alert on latest data</div>
                <button type="button" onClick={submitAlertPreview} disabled={previewAlertMutation.isPending} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, color: t.color.text, borderRadius: t.radius.sm, padding: '6px 10px', fontSize: t.font.sizeSm, cursor: previewAlertMutation.isPending ? 'wait' : 'pointer' }}>
                  {previewAlertMutation.isPending ? 'Testing…' : 'Test alert'}
                </button>
              </div>
              {alertPreview && (
                <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Current: {alertPreview.current_value ?? '—'} • Baseline: {alertPreview.baseline_value ?? '—'} • Delta: {alertPreview.delta_pct == null ? '—' : `${alertPreview.delta_pct.toFixed(1)}%`}<br />
                  Window: {alertPreview.window.current_from} → {alertPreview.window.current_to} vs {alertPreview.window.baseline_from} → {alertPreview.window.baseline_to}
                </div>
              )}
            </div>
            {createAlertError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{createAlertError}</div>}
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
              <button type="button" onClick={() => setShowCreateAlertModal(false)} style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>Cancel</button>
              <button type="button" onClick={submitCreateAlert} disabled={createAlertMutation.isPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 14px', cursor: createAlertMutation.isPending ? 'wait' : 'pointer' }}>{createAlertMutation.isPending ? 'Creating…' : 'Create alert'}</button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
