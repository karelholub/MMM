import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import DashboardTable from '../components/dashboard/DashboardTable'
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

type JourneysTab = 'paths' | 'flow' | 'examples' | 'funnels'
type PathSortBy = 'journeys' | 'conversion_rate' | 'avg_time'

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

function formatSeconds(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—'
  if (v < 60) return `${v.toFixed(0)}s`
  const mins = v / 60
  if (mins < 60) return `${mins.toFixed(1)}m`
  return `${(mins / 60).toFixed(1)}h`
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
  const [showCreateFunnelModal, setShowCreateFunnelModal] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)
  const [createFunnelError, setCreateFunnelError] = useState<string | null>(null)
  const [pathSearch, setPathSearch] = useState('')
  const [pathSortBy, setPathSortBy] = useState<PathSortBy>('journeys')
  const [pathSortDir, setPathSortDir] = useState<'asc' | 'desc'>('desc')
  const [pathsPage, setPathsPage] = useState(1)
  const [pathsLimit, setPathsLimit] = useState(50)
  const [selectedPath, setSelectedPath] = useState<JourneyPathRow | null>(null)
  const [creditExpanded, setCreditExpanded] = useState(false)
  const [selectedFunnelId, setSelectedFunnelId] = useState('')
  const [selectedFunnelStep, setSelectedFunnelStep] = useState<string | null>(null)
  const [showCreateAlertModal, setShowCreateAlertModal] = useState(false)
  const [createAlertError, setCreateAlertError] = useState<string | null>(null)
  const [alertScope, setAlertScope] = useState<Record<string, unknown>>({})
  const [alertPreview, setAlertPreview] = useState<JourneyAlertPreviewResponse | null>(null)
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

  const definitionsQuery = useQuery<PaginatedResponse<JourneyDefinition>>({
    queryKey: ['journey-definitions', 'journeys-page'],
    queryFn: async () => {
      return apiGetJson<PaginatedResponse<JourneyDefinition>>(withQuery('/api/journeys/definitions', buildListQuery({
        page: 1,
        perPage: 100,
        order: 'desc',
      })), {
        fallbackMessage: 'Failed to load journey definitions',
      })
    },
  })

  const kpisQuery = useQuery<KpiResponse>({
    queryKey: ['kpis', 'journeys-create-modal'],
    queryFn: async () => apiGetJson<KpiResponse>('/api/kpis', { fallbackMessage: 'Failed to load KPI definitions' }),
  })

  const selectedDefinition = useMemo(
    () => (definitionsQuery.data?.items ?? []).find((item) => item.id === selectedJourneyId) ?? null,
    [definitionsQuery.data?.items, selectedJourneyId],
  )

  const mode = selectedDefinition?.mode_default ?? 'conversion_only'

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
      if (filters.geo !== 'all') params.set('country', filters.geo.toUpperCase())
      return apiGetJson<JourneyPathsResponse>(`/api/journeys/${selectedJourneyId}/paths?${params.toString()}`, {
        fallbackMessage: 'Failed to load path aggregates',
      })
    },
    enabled: !!selectedJourneyId && activeTab === 'paths' && !featureDisabled,
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
      if (filters.geo !== 'all') params.set('country', filters.geo.toUpperCase())
      return apiGetJson<AttributionSummaryResponse>(`/api/journeys/${selectedJourneyId}/attribution-summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load attribution summary',
      })
    },
    enabled: !!selectedJourneyId && !featureDisabled,
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
      if (filters.geo !== 'all') params.set('country', filters.geo.toUpperCase())
      return apiGetJson<AttributionSummaryResponse>(`/api/journeys/${selectedJourneyId}/attribution-summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load path credit split',
      })
    },
    enabled: !!selectedJourneyId && !!selectedPath && !featureDisabled,
  })

  const funnelListQuery = useQuery<FunnelListResponse>({
    queryKey: ['funnels-list', selectedJourneyId, user.userId],
    queryFn: async () => {
      const params = new URLSearchParams({
        workspace_id: 'default',
        user_id: user.userId,
      })
      if (selectedJourneyId) params.set('journey_definition_id', selectedJourneyId)
      return apiGetJson<FunnelListResponse>(`/api/funnels?${params.toString()}`, {
        fallbackMessage: 'Failed to load funnels',
      })
    },
    enabled: !!selectedJourneyId && funnelBuilderEnabled && !featureDisabled,
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
      if (filters.geo !== 'all') params.set('country', filters.geo.toUpperCase())
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
      if (filters.geo !== 'all') params.set('country', filters.geo.toUpperCase())
      return apiGetJson<FunnelDiagnosticItem[]>(`/api/funnels/${selectedFunnelId}/diagnostics?${params.toString()}`, {
        fallbackMessage: 'Failed to load diagnostics',
      })
    },
    enabled: !!selectedFunnelId && !!selectedFunnelStep && activeTab === 'funnels' && funnelBuilderEnabled,
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

  useEffect(() => {
    const params = readParams()
    setSelectedJourneyId(params.get('journey_id') || '')
    setFilters((prev) => ({
      ...prev,
      dateFrom: params.get('date_from') || prev.dateFrom,
      dateTo: params.get('date_to') || prev.dateTo,
      channel: params.get('channel') || prev.channel,
      campaign: params.get('campaign') || prev.campaign,
      device: params.get('device') || prev.device,
      geo: params.get('geo') || prev.geo,
      segment: params.get('segment') || prev.segment,
    }))
  }, [])

  useEffect(() => {
    const primary = kpisQuery.data?.primary_kpi_id || kpisQuery.data?.definitions?.[0]?.id || ''
    if (primary && !draft.conversion_kpi_id) {
      setDraft((prev) => ({ ...prev, conversion_kpi_id: primary }))
    }
  }, [draft.conversion_kpi_id, kpisQuery.data?.definitions, kpisQuery.data?.primary_kpi_id])

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
    if (typeof window === 'undefined') return
    const params = readParams()
    if (selectedJourneyId) params.set('journey_id', selectedJourneyId)
    else params.delete('journey_id')
    params.set('date_from', filters.dateFrom)
    params.set('date_to', filters.dateTo)
    params.set('channel', filters.channel)
    params.set('campaign', filters.campaign)
    params.set('device', filters.device)
    params.set('geo', filters.geo)
    params.set('segment', filters.segment)
    window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`)
  }, [filters, selectedJourneyId])

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

  const definitions = definitionsQuery.data?.items ?? []
  const kpiOptions = kpisQuery.data?.definitions ?? []
  const journeyOptions = definitions.map((item) => ({ value: item.id, label: item.name }))

  const tabs = useMemo(
    () =>
      [
        { key: 'paths' as JourneysTab, label: 'Paths', visible: true, disabled: false },
        { key: 'flow' as JourneysTab, label: 'Flow', visible: true, disabled: true },
        { key: 'examples' as JourneysTab, label: 'Examples', visible: journeyExamplesEnabled, disabled: true },
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
        filters={<GlobalFilterBar value={filters} onChange={setFilters} channels={journeysSummary?.channels ?? []} />}
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
        <SectionCard
          title="Journey definition"
          subtitle="Select an existing journey or create a new one for this workspace."
          actions={
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
              <button
                type="button"
                onClick={() => {
                  if (!canManageDefinitions) return
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
              {!!selectedJourneyId && (
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
                          country: filters.geo !== 'all' ? filters.geo.toUpperCase() : null,
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
            </div>
          }
        >
          <div style={{ display: 'grid', gap: t.space.md }}>
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
            {selectedDefinition ? (
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{selectedDefinition.description?.trim() || 'No description'}</div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  Lookback: {selectedDefinition.lookback_window_days} days • Mode:{' '}
                  {selectedDefinition.mode_default === 'all_journeys' ? 'All journeys' : 'Conversion only'}
                </div>
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                {definitionsQuery.isLoading ? 'Loading journey definitions…' : 'Create a journey definition to get started.'}
              </div>
            )}
          </div>
        </SectionCard>

        <SectionCard title="Journey workspace" subtitle="Credit + paths in one workspace.">
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', marginBottom: t.space.md }}>
            {tabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                disabled={tab.disabled}
                onClick={() => setActiveTab(tab.key)}
                style={{
                  border: `1px solid ${tab.key === activeTab ? t.color.accent : t.color.borderLight}`,
                  borderRadius: t.radius.full,
                  background: tab.key === activeTab ? t.color.accentMuted : t.color.surface,
                  color: tab.key === activeTab ? t.color.accent : t.color.textMuted,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  padding: '6px 12px',
                  cursor: tab.disabled ? 'not-allowed' : 'pointer',
                  opacity: tab.disabled ? 0.75 : 1,
                }}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {activeTab === 'paths' && (
            <>
              <DashboardTable
                search={{ value: pathSearch, onChange: setPathSearch, placeholder: 'Search path steps…' }}
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
              >
                <thead>
                  <tr>
                    <th>Path steps</th>
                    <th>Journeys</th>
                    <th>Conv. rate</th>
                    <th>Avg</th>
                    <th>P50</th>
                    <th>P90</th>
                    <th>Credit overlay</th>
                  </tr>
                </thead>
                <tbody>
                  {pathsQuery.isLoading && (
                    <tr>
                      <td colSpan={7} style={{ color: t.color.textSecondary }}>
                        Loading path aggregates…
                      </td>
                    </tr>
                  )}
                  {pathsQuery.isError && (
                    <tr>
                      <td colSpan={7} style={{ color: t.color.danger }}>
                        {(pathsQuery.error as Error).message}
                      </td>
                    </tr>
                  )}
                  {!pathsQuery.isLoading && !pathsQuery.isError && filteredPaths.length === 0 && (
                    <tr>
                      <td colSpan={7} style={{ color: t.color.textSecondary }}>
                        No paths for the selected journey and filters.
                      </td>
                    </tr>
                  )}
                  {filteredPaths.map((row, idx) => (
                    <tr
                      key={`${row.path_hash}-${row.device || ''}-${row.country || ''}-${row.channel_group || ''}-${row.campaign_id || ''}-${idx}`}
                      onClick={() => setSelectedPath(row)}
                      style={{ cursor: 'pointer' }}
                    >
                      <td>
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          {normalizeSteps(row.path_steps).map((s) => pathChip(s))}
                        </div>
                      </td>
                      <td>{row.count_journeys.toLocaleString()}</td>
                      <td>{formatPercent(row.conversion_rate)}</td>
                      <td>{formatSeconds(row.avg_time_to_convert_sec)}</td>
                      <td>{formatSeconds(row.p50_time_to_convert_sec)}</td>
                      <td>{formatSeconds(row.p90_time_to_convert_sec)}</td>
                      <td>{creditOverlay(globalCredits, row.channel_group)}</td>
                    </tr>
                  ))}
                </tbody>
              </DashboardTable>
            </>
          )}

          {activeTab === 'funnels' && (
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

              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(240px, 320px) 1fr', gap: t.space.md }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
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

                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
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

                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md }}>
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

          {(activeTab === 'flow' || activeTab === 'examples') && (
            <div style={{ border: `1px dashed ${t.color.border}`, borderRadius: t.radius.md, padding: t.space.lg, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              This tab is scaffolded and will be enabled in a later increment.
            </div>
          )}
        </SectionCard>

        <SectionCard title="Saved views" subtitle="Scaffolding for named Journey views and shared links.">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              No saved views yet. Current journey and filters are URL-shareable.
            </div>
            <button type="button" disabled style={{ border: `1px solid ${t.color.border}`, background: t.color.bgSubtle, color: t.color.textMuted, borderRadius: t.radius.sm, fontSize: t.font.sizeSm, padding: '8px 12px', cursor: 'not-allowed' }}>
              Save current view (coming soon)
            </button>
          </div>
        </SectionCard>
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
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md }}>
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

              <SectionCard title="Breakdown by device" subtitle="Placeholder: dimension-level decomposition will be added later.">
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Current row device: {selectedPath.device || 'n/a'}
                </div>
              </SectionCard>
              <SectionCard title="Breakdown by channel_group" subtitle="Placeholder: detailed split will be available in a later release.">
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Current row channel group: {selectedPath.channel_group || 'n/a'}
                </div>
              </SectionCard>

              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <button type="button" disabled style={{ border: `1px solid ${t.color.border}`, background: t.color.bgSubtle, color: t.color.textMuted, borderRadius: t.radius.sm, padding: '8px 12px', fontSize: t.font.sizeSm, cursor: 'not-allowed' }}>
                  Compare (coming soon)
                </button>
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
                          country: filters.geo !== 'all' ? filters.geo.toUpperCase() : null,
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
                disabled
                style={{
                  border: `1px solid ${t.color.border}`,
                  background: t.color.bgSubtle,
                  color: t.color.textMuted,
                  borderRadius: t.radius.sm,
                  padding: '8px 12px',
                  fontSize: t.font.sizeSm,
                  cursor: 'not-allowed',
                }}
                title="Placeholder action"
              >
                Open related segments
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
                        country: filters.geo !== 'all' ? filters.geo.toUpperCase() : null,
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
          createError={createError}
          creating={createMutation.isPending}
          onClose={() => setShowCreateModal(false)}
          onSubmit={submitCreate}
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
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md }}>
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
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.md }}>
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
