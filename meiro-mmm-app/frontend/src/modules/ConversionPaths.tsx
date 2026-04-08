import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Sankey } from 'recharts'
import { tokens } from '../theme/tokens'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import ContextSummaryStrip from '../components/dashboard/ContextSummaryStrip'
import AnalysisNarrativePanel from '../components/dashboard/AnalysisNarrativePanel'
import GlobalFilterBar, { type GlobalFiltersState } from '../components/dashboard/GlobalFilterBar'
import SaveLocalSegmentDialog from '../components/segments/SaveLocalSegmentDialog'
import SegmentComparisonContextNote from '../components/segments/SegmentComparisonContextNote'
import SegmentOverlapNotice from '../components/segments/SegmentOverlapNotice'
import { AnalyticsTable, AnalyticsToolbar, type AnalyticsTableColumn } from '../components/dashboard'
import DecisionStatusCard from '../components/DecisionStatusCard'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import ConfidenceBadge, { type Confidence } from '../components/ConfidenceBadge'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import { buildListQuery, type PaginatedResponse } from '../lib/apiSchemas'
import { defaultRecentDateRange } from '../lib/dateRange'
import { buildIncrementalityPlannerHref } from '../lib/experimentLinks'
import { buildJourneyHypothesisHref, buildJourneyHypothesisSeedHref } from '../lib/journeyLinks'
import {
  activeLocalSegmentDefinitionFromFilters,
  applyLocalSegmentToFilterState,
  buildSegmentComparisonHref,
  buildLocalSegmentDefaultName,
  hasLocalSegmentCriteria,
  isLocalAnalyticalSegment,
  segmentOptionLabel,
  type SegmentComparisonResponse,
  type SegmentRegistryItem,
  type SegmentRegistryResponse,
} from '../lib/segments'
import { usePersistentToggle } from '../hooks/usePersistentToggle'

interface NextBestRec {
  channel: string
  campaign?: string
  step?: string
  count: number
  conversions: number
  conversion_rate: number
  avg_value: number
  is_promoted_policy?: boolean
  promoted_policy_title?: string | null
  promoted_policy_hypothesis_id?: string | null
  promoted_policy_journey_definition_id?: string | null
}

interface JourneyDefinition {
  id: string
  name: string
  description?: string | null
  is_archived?: boolean
  lifecycle_status?: string
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

interface JourneyDefinitionLifecycle {
  definition: JourneyDefinition
  rebuild_state?: {
    status: 'active' | 'stale' | 'archived' | string
    stale_reason?: string | null
    last_rebuilt_at?: string | null
  }
}

interface PathAnalysis {
  total_journeys: number
  avg_path_length: number
  avg_time_to_conversion_days: number | null
  common_paths: {
    path: string
    count: number
    share: number
    avg_time_to_convert_days?: number | null
    path_length?: number
  }[]
  channel_frequency: Record<string, number>
  path_length_distribution: { min: number; max: number; median: number; p90?: number }
  time_to_conversion_distribution?: { min: number; max: number; median: number; p90?: number } | null
  direct_unknown_diagnostics?: {
    touchpoint_share: number
    journeys_ending_direct_share: number
  }
  config?: {
    config_id?: string
    config_version?: number
    conversion_key?: string | null
    time_window?: {
      click_lookback_days?: number | null
      impression_lookback_days?: number | null
      session_timeout_minutes?: number | null
      conversion_latency_days?: number | null
    }
  } | null
  view_filters?: {
    direct_mode?: 'include' | 'exclude'
    path_scope?: 'converted' | 'all'
  } | null
  nba_config?: {
    min_prefix_support: number
    min_conversion_rate: number
  } | null
  next_best_by_prefix?: Record<string, NextBestRec[]>
  next_best_by_prefix_campaign?: Record<string, NextBestRec[]>
  source?: string
  journey_definition_id?: string | null
  date_from?: string | null
  date_to?: string | null
}

interface PathStepBreakdown {
  step: string
  position: number
  dropoff_share: number
  prefix_journeys: number
}

interface PathVariant {
  path: string
  count: number
  share: number
}

interface PathDetails {
  path: string
  summary: {
    count: number
    share: number
    avg_touchpoints: number
    avg_time_to_convert_days: number | null
  }
  step_breakdown: PathStepBreakdown[]
  variants: PathVariant[]
  data_health?: {
    direct_unknown_touch_share: number
    journeys_ending_direct_share: number
    confidence?: Confidence | null
  }
}

interface PathAnomaly {
  type: string
  severity: 'info' | 'warn' | 'critical' | string
  metric_key: string
  metric_value: number
  baseline_value?: number | null
  z_score?: number | null
  details?: Record<string, any> | null
  suggestion?: string | null
  message: string
}

const METRIC_DEFINITIONS: Record<string, string> = {
  'Total Journeys': 'Number of customer paths (converted or not) in the dataset.',
  'Avg Path Length': 'Average number of touchpoints per journey before conversion.',
  'Avg Time to Convert': 'Average days from first touch to conversion.',
  'Path Length Range': 'Min and max touchpoints observed in paths.',
}

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

function dimensionLabel(value: string, count: number): string {
  return `${value} (${count.toLocaleString()})`
}

function formatLifecycleTimestamp(value?: string | null): string {
  if (!value) return '—'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString()
}

function clampLabel(label: string, max = 24): string {
  if (label.length <= max) return label
  return `${label.slice(0, Math.max(0, max - 1))}…`
}

function medianOf(values: Array<number | null | undefined>): number | null {
  const valid = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  if (!valid.length) return null
  const sorted = [...valid].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle]
}

function describeLagPosition(selected: number | null | undefined, benchmark: number | null): string | null {
  if (selected == null || benchmark == null || benchmark <= 0) return null
  if (selected <= benchmark * 0.7) return 'This path family converts materially faster than the typical visible path.'
  if (selected >= benchmark * 1.5) return 'This path family is meaningfully long-lag versus the visible path set.'
  return 'This path family sits near the typical lag of the visible path set.'
}

function SankeyFlowTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: Array<{ payload?: { sourceLabel?: string; targetLabel?: string; value?: number } }>
}) {
  if (!active || !payload?.length) return null
  const raw = payload[0]?.payload
  if (!raw) return null
  return (
    <div
      style={{
        background: tokens.color.surface,
        border: `1px solid ${tokens.color.border}`,
        borderRadius: tokens.radius.sm,
        padding: tokens.space.sm,
        boxShadow: tokens.shadowSm,
        display: 'grid',
        gap: 4,
      }}
    >
      <div style={{ fontSize: tokens.font.sizeSm, color: tokens.color.text, fontWeight: tokens.font.weightMedium }}>
        {(raw.sourceLabel || 'Unknown')} → {(raw.targetLabel || 'Unknown')}
      </div>
      <div style={{ fontSize: tokens.font.sizeXs, color: tokens.color.textSecondary }}>
        {Number(raw.value || 0).toLocaleString()} journeys
      </div>
    </div>
  )
}

function buildJourneyPathsHref(journeyDefinitionId?: string | null): string | null {
  const journeyId = String(journeyDefinitionId || '').trim()
  if (!journeyId) return null
  const params = new URLSearchParams()
  params.set('page', 'analytics_journeys')
  params.set('journey_id', journeyId)
  params.set('tab', 'paths')
  return `/?${params.toString()}`
}

function normalizePathInput(raw: string): string {
  return raw
    .split(/>|,/g)
    .map((part) => part.trim())
    .filter(Boolean)
    .join(' > ')
}

function exportPathsCSV(
  paths: { path: string; count: number; share: number; avg_time_to_convert_days?: number | null; path_length?: number }[],
  nextBestByPrefix?: Record<string, NextBestRec[]>,
  meta?: {
    period?: string
    conversionKey?: string | null
    configVersion?: number | null
    directMode?: 'include' | 'exclude'
    pathScope?: 'converted' | 'all'
    filters?: {
      minCount?: number
      minPathLength?: number
      maxPathLength?: number | null
      containsChannels?: string[]
    }
  }
) {
  const headers = nextBestByPrefix ? ['Path', 'Count', 'Share (%)', 'Suggested next'] : ['Path', 'Count', 'Share (%)']
  const rows = paths.map((p) => {
    const base = [p.path, p.count.toString(), (p.share * 100).toFixed(1)]
    if (nextBestByPrefix) {
      const prefix = p.path.split(' > ').slice(0, -1).join(' > ')
      const recs = nextBestByPrefix[prefix]
      const top = recs?.[0]
      base.push(top ? `${top.channel} (${(top.conversion_rate * 100).toFixed(1)}%)` : '')
    }
    return base
  })
  const headerLines: string[] = []
  if (meta) {
    headerLines.push(
      `# period: ${meta.period || 'n/a'}`,
      `# conversion_key: ${meta.conversionKey ?? 'n/a'}`,
      `# config_version: ${meta.configVersion ?? 'n/a'}`,
      `# direct_mode: ${meta.directMode || 'include'}`,
      `# path_scope: ${meta.pathScope || 'converted'}`,
      `# filter_min_count: ${meta.filters?.minCount ?? 'n/a'}`,
      `# filter_path_length: ${meta.filters?.minPathLength ?? 'n/a'}–${meta.filters?.maxPathLength ?? 'n/a'}`,
      `# filter_contains_channels: ${(meta.filters?.containsChannels ?? []).join('|') || 'n/a'}`
    )
  }
  const csv = [
    ...headerLines,
    headers.join(','),
    ...rows.map((r) => (Array.isArray(r) ? r : [r]).join(',')),
  ].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `conversion-paths-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export default function ConversionPaths() {
  const queryClient = useQueryClient()
  const { journeysSummary: journeys, globalDateFrom, globalDateTo } = useWorkspaceContext()
  const [filters, setFilters] = useState<GlobalFiltersState>(() =>
    buildInitialFilters(globalDateFrom || journeys?.date_min, globalDateTo || journeys?.date_max),
  )
  const [selectedJourneyId, setSelectedJourneyId] = useState('')
  const [showSaveSegmentModal, setShowSaveSegmentModal] = useState(false)
  const [saveSegmentError, setSaveSegmentError] = useState<string | null>(null)
  const [pathSort, setPathSort] = useState<'count' | 'share' | 'avg_time' | 'length'>('count')
  const [pathSortDir, setPathSortDir] = useState<'asc' | 'desc'>('desc')
  const [freqSort, setFreqSort] = useState<'channel' | 'count' | 'pct'>('count')
  const [freqSortDir, setFreqSortDir] = useState<'asc' | 'desc'>('desc')
  const [tryPathInput, setTryPathInput] = useState('')
  const [tryPathLevel, setTryPathLevel] = useState<'channel' | 'campaign'>('channel')
  const [tryPathResult, setTryPathResult] = useState<{ path_so_far: string; level: string; recommendations: NextBestRec[] } | null>(null)
  const [tryPathError, setTryPathError] = useState<string | null>(null)
  const [showContext, setShowContext] = usePersistentToggle('conversion-paths:show-context', false)
  const [showDiagnostics, setShowDiagnostics] = usePersistentToggle('conversion-paths:show-diagnostics', false)
  const [compareSegmentId, setCompareSegmentId] = useState('')

  const [directMode, setDirectMode] = useState<'include' | 'exclude'>('include')
  const [pathScope, setPathScope] = useState<'converted' | 'all'>('converted')

  const [minPathCount, setMinPathCount] = useState<number>(1)
  const [minPathLength, setMinPathLength] = useState<number>(1)
  const [maxPathLength, setMaxPathLength] = useState<number | ''>('')
  const [channelFilter, setChannelFilter] = useState<string[]>([])

  const [selectedPath, setSelectedPath] = useState<string | null>(null)
  const [selectedPathDetails, setSelectedPathDetails] = useState<PathDetails | null>(null)
  const [selectedPathLoading, setSelectedPathLoading] = useState(false)
  const [selectedPathError, setSelectedPathError] = useState<string | null>(null)

  const definitionsQuery = useQuery<PaginatedResponse<JourneyDefinition>>({
    queryKey: ['journey-definitions', 'conversion-paths'],
    queryFn: async () => {
      const query = buildListQuery({ page: 1, perPage: 100, order: 'desc' })
      return apiGetJson<PaginatedResponse<JourneyDefinition>>(`/api/journeys/definitions?${new URLSearchParams(query as Record<string, string>).toString()}`, {
        fallbackMessage: 'Failed to load journey definitions',
      })
    },
  })

  const selectedDefinition = useMemo(
    () => (definitionsQuery.data?.items ?? []).find((item) => item.id === selectedJourneyId) ?? null,
    [definitionsQuery.data?.items, selectedJourneyId],
  )

  const definitionLifecycleQuery = useQuery<JourneyDefinitionLifecycle>({
    queryKey: ['journey-definition-lifecycle', 'conversion-paths', selectedJourneyId],
    queryFn: async () =>
      apiGetJson<JourneyDefinitionLifecycle>(`/api/journeys/definitions/${selectedJourneyId}/lifecycle`, {
        fallbackMessage: 'Failed to load journey definition lifecycle',
      }),
    enabled: !!selectedJourneyId,
  })

  const preferredDefinitionQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis-preferred-definition', directMode, pathScope, filters.dateFrom, filters.dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        direct_mode: directMode,
        path_scope: pathScope === 'all' ? 'all' : 'converted',
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
      })
      return apiGetJson<PathAnalysis>(`/api/conversion-paths/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to resolve preferred journey definition for conversion paths',
      })
    },
    enabled: !selectedJourneyId,
  })

  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['segment-registry'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load segment registry',
      }),
  })

  const dimensionsQuery = useQuery<JourneyFilterDimensionsResponse>({
    queryKey: ['journey-dimensions', 'conversion-paths', selectedJourneyId, filters.dateFrom, filters.dateTo, filters.channel, filters.campaign, filters.device, filters.geo],
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
        fallbackMessage: 'Failed to load conversion path filter dimensions',
      })
    },
    enabled: !!selectedJourneyId,
  })

  const allSegmentRegistryItems = segmentRegistryQuery.data?.items ?? []
  const localAnalyticalSegments = useMemo(
    () => allSegmentRegistryItems.filter(isLocalAnalyticalSegment),
    [allSegmentRegistryItems],
  )
  const selectedLocalSegment = useMemo(
    () => localAnalyticalSegments.find((item) => item.id === filters.segment) ?? null,
    [filters.segment, localAnalyticalSegments],
  )
  const compareLocalSegment = useMemo(
    () => localAnalyticalSegments.find((item) => item.id === compareSegmentId) ?? null,
    [compareSegmentId, localAnalyticalSegments],
  )
  const journeyFilterOptions = useMemo(
    () => ({
      channels: (dimensionsQuery.data?.channels ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      campaigns: (dimensionsQuery.data?.campaigns ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      devices: (dimensionsQuery.data?.devices ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      geos: (dimensionsQuery.data?.countries ?? []).map((item) => ({ value: item.value, label: dimensionLabel(String(item.value).toUpperCase(), item.count) })),
      segments: localAnalyticalSegments.map((item) => ({ value: item.id, label: segmentOptionLabel(item) })),
    }),
    [dimensionsQuery.data, localAnalyticalSegments],
  )

  const handleFiltersChange = (next: GlobalFiltersState) => {
    setFilters((prev) => {
      if (next.segment !== prev.segment) {
        if (next.segment === 'all') return { ...next, segment: 'all' }
        const selectedSegment = localAnalyticalSegments.find((item) => item.id === next.segment)
        if (selectedSegment) return applyLocalSegmentToFilterState(next, selectedSegment)
      }
      const dimensionsChanged =
        next.channel !== prev.channel ||
        next.campaign !== prev.campaign ||
        next.device !== prev.device ||
        next.geo !== prev.geo
      if (dimensionsChanged && prev.segment !== 'all') return { ...next, segment: 'all' }
      return next
    })
  }

  const saveSegmentMutation = useMutation({
    mutationFn: async ({ name, description }: { name: string; description: string }) => {
      const definition = activeLocalSegmentDefinitionFromFilters(filters)
      if (!hasLocalSegmentCriteria(definition)) {
        throw new Error('Select at least one channel, campaign, device, or geo filter to save a local segment')
      }
      return apiSendJson<SegmentRegistryItem>('/api/segments/local', 'POST', {
        name,
        description,
        definition,
      }, {
        fallbackMessage: 'Failed to save local segment',
      })
    },
    onSuccess: async (segment) => {
      setSaveSegmentError(null)
      setShowSaveSegmentModal(false)
      await queryClient.invalidateQueries({ queryKey: ['segment-registry'] })
      setFilters((prev) => ({ ...prev, segment: segment.id }))
    },
    onError: (err) => setSaveSegmentError((err as Error).message || 'Failed to save local segment'),
  })

  const pathsQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis', selectedJourneyId, directMode, pathScope, filters.dateFrom, filters.dateTo, filters.channel, filters.campaign, filters.device, filters.geo],
    queryFn: async () => {
      const params = new URLSearchParams({
        direct_mode: directMode,
        path_scope: pathScope === 'all' ? 'all' : 'converted',
        definition_id: selectedJourneyId,
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      return apiGetJson<PathAnalysis>(`/api/conversion-paths/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to fetch path analysis',
      })
    },
    enabled: !!selectedJourneyId,
  })
  const baselinePathsQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis-baseline', selectedJourneyId, directMode, pathScope, filters.dateFrom, filters.dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        direct_mode: directMode,
        path_scope: pathScope === 'all' ? 'all' : 'converted',
        definition_id: selectedJourneyId,
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
      })
      return apiGetJson<PathAnalysis>(`/api/conversion-paths/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to load conversion path workspace baseline',
      })
    },
    enabled: !!selectedJourneyId && filters.segment !== 'all',
  })
  const segmentCompareQuery = useQuery<SegmentComparisonResponse>({
    queryKey: ['conversion-paths-segment-compare', selectedLocalSegment?.id || 'none', compareLocalSegment?.id || 'none'],
    queryFn: async () =>
      apiGetJson<SegmentComparisonResponse>(`/api/segments/local/${selectedLocalSegment?.id}/compare?other_segment_id=${encodeURIComponent(compareLocalSegment?.id || '')}`, {
        fallbackMessage: 'Failed to compare saved analytical audiences',
      }),
    enabled: Boolean(selectedLocalSegment?.id && compareLocalSegment?.id && selectedLocalSegment?.id !== compareLocalSegment?.id),
  })

  const anomaliesQuery = useQuery<{ anomalies: PathAnomaly[] }>({
    queryKey: ['path-anomalies'],
    queryFn: async () => apiGetJson<{ anomalies: PathAnomaly[] }>('/api/paths/anomalies', {
      fallbackMessage: 'Failed to fetch path anomalies',
    }),
  })

  useEffect(() => {
    if ((globalDateFrom && globalDateTo) || (journeys?.date_min && journeys?.date_max)) {
      setFilters((prev) => ({
        ...prev,
        dateFrom: globalDateFrom || journeys?.date_min?.slice(0, 10) || prev.dateFrom,
        dateTo: globalDateTo || journeys?.date_max?.slice(0, 10) || prev.dateTo,
      }))
    }
  }, [globalDateFrom, globalDateTo, journeys?.date_min, journeys?.date_max])

  useEffect(() => {
    const defs = definitionsQuery.data?.items ?? []
    if (!defs.length) return
    if (!selectedJourneyId || !defs.some((item) => item.id === selectedJourneyId)) {
      const preferredId = preferredDefinitionQuery.data?.journey_definition_id
      if (preferredId && defs.some((item) => item.id === preferredId)) {
        setSelectedJourneyId(preferredId)
        return
      }
      const preferred = defs.find((item) => !item.is_archived) ?? defs[0]
      setSelectedJourneyId(preferred.id)
    }
  }, [definitionsQuery.data?.items, preferredDefinitionQuery.data?.journey_definition_id, selectedJourneyId])

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
      if (next.geo !== 'all' && !validGeos.has(next.geo)) next.geo = 'all'
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
    if (!localAnalyticalSegments.length) return
    setFilters((prev) => {
      if (prev.segment === 'all') return prev
      const selectedSegment = localAnalyticalSegments.find((item) => item.id === prev.segment)
      if (!selectedSegment) return { ...prev, segment: 'all' }
      const applied = applyLocalSegmentToFilterState(prev, selectedSegment)
      return JSON.stringify(applied) === JSON.stringify(prev) ? prev : applied
    })
  }, [localAnalyticalSegments])

  useEffect(() => {
    if (!compareSegmentId) return
    if (localAnalyticalSegments.some((item) => item.id === compareSegmentId && item.id !== filters.segment)) return
    setCompareSegmentId('')
  }, [compareSegmentId, filters.segment, localAnalyticalSegments])

  useEffect(() => {
    setSelectedPath(null)
    setSelectedPathDetails(null)
    setSelectedPathError(null)
  }, [selectedJourneyId, filters.dateFrom, filters.dateTo, filters.channel, filters.campaign, filters.device, filters.geo, directMode, pathScope])

  const data = pathsQuery.data
  const t = tokens

  const channelFreq = data?.channel_frequency ?? {}
  const totalTouchpoints = Object.values(channelFreq).reduce((s, n) => s + n, 0)
  const freqRows = Object.entries(channelFreq).map(([channel, count]) => ({
    channel,
    count,
    pct: totalTouchpoints > 0 ? (count / totalTouchpoints) * 100 : 0,
  }))
  const availableChannelFilters = useMemo(
    () =>
      Array.from(new Set(Object.keys(channelFreq).map((channel) => channel.split(':', 1)[0]))).sort((a, b) =>
        a.localeCompare(b),
      ),
    [channelFreq],
  )

  const sortedFreq = useMemo(() => {
    return [...freqRows].sort((a, b) => {
      let cmp = 0
      if (freqSort === 'channel') cmp = a.channel.localeCompare(b.channel)
      else if (freqSort === 'count') cmp = a.count - b.count
      else cmp = a.pct - b.pct
      return freqSortDir === 'asc' ? cmp : -cmp
    })
  }, [freqRows, freqSort, freqSortDir])

  const commonPaths = data?.common_paths ?? []
  const enrichedPaths = useMemo(
    () =>
      commonPaths.map((p) => ({
        ...p,
        path_length: p.path_length ?? (p.path ? p.path.split(' > ').length : 0),
      })),
    [commonPaths],
  )

  const filteredAndSortedPaths = useMemo(() => {
    const base = enrichedPaths.filter((p) => {
      if (p.count < (minPathCount || 1)) return false
      const len = p.path_length ?? (p.path ? p.path.split(' > ').length : 0)
      if (len < (minPathLength || 1)) return false
      if (typeof maxPathLength === 'number' && maxPathLength > 0 && len > maxPathLength) return false
      if (channelFilter.length) {
        const steps = p.path.split(' > ')
        const hasAny = steps.some((s) => channelFilter.includes(s.split(':', 1)[0]))
        if (!hasAny) return false
      }
      return true
    })

    return base.sort((a, b) => {
      let cmp = 0
      if (pathSort === 'count') cmp = a.count - b.count
      else if (pathSort === 'share') cmp = a.share - b.share
      else if (pathSort === 'length') cmp = (a.path_length ?? 0) - (b.path_length ?? 0)
      else if (pathSort === 'avg_time') {
        const av = a.avg_time_to_convert_days ?? -1
        const bv = b.avg_time_to_convert_days ?? -1
        cmp = av - bv
      }
      return pathSortDir === 'asc' ? cmp : -cmp
    })
  }, [enrichedPaths, pathSort, pathSortDir, minPathCount, minPathLength, maxPathLength, channelFilter])
  const selectedPathFlowData = useMemo(() => {
    if (!selectedPath || !selectedPathDetails) return null
    const family = [
      { path: selectedPath, count: selectedPathDetails.summary.count },
      ...(selectedPathDetails.variants ?? []).map((variant) => ({ path: variant.path, count: variant.count })),
    ]
      .filter((item) => item.count > 0)
      .slice(0, 10)
    if (!family.length) return null

    const nodeIndex = new Map<string, number>()
    const nodes: Array<{ name: string; fullName: string }> = []
    const linkMap = new Map<string, { source: number; target: number; value: number; sourceLabel: string; targetLabel: string }>()

    const ensureNode = (label: string) => {
      const existing = nodeIndex.get(label)
      if (existing != null) return existing
      const index = nodes.length
      nodes.push({ name: clampLabel(label, 22), fullName: label })
      nodeIndex.set(label, index)
      return index
    }

    family.forEach((item) => {
      const steps = item.path.split(' > ').map((step) => step.trim()).filter(Boolean)
      for (let index = 0; index < steps.length - 1; index += 1) {
        const sourceLabel = steps[index]
        const targetLabel = steps[index + 1]
        const source = ensureNode(sourceLabel)
        const target = ensureNode(targetLabel)
        const key = `${sourceLabel}__${targetLabel}`
        const current = linkMap.get(key) || { source, target, value: 0, sourceLabel, targetLabel }
        current.value += item.count
        linkMap.set(key, current)
      }
    })

    const links = [...linkMap.values()].sort((a, b) => b.value - a.value)
    if (!links.length) return null
    const totalJourneys = family.reduce((sum, item) => sum + item.count, 0)
    return { nodes, links, totalJourneys, variants: Math.max(0, family.length - 1) }
  }, [selectedPath, selectedPathDetails])
  const visiblePathLagMedian = useMemo(
    () => medianOf(filteredAndSortedPaths.map((path) => path.avg_time_to_convert_days)),
    [filteredAndSortedPaths],
  )
  const selectedPathLagNote = useMemo(
    () =>
      describeLagPosition(
        selectedPathDetails?.summary.avg_time_to_convert_days ?? null,
        visiblePathLagMedian,
      ),
    [selectedPathDetails?.summary.avg_time_to_convert_days, visiblePathLagMedian],
  )

  const kpis = data
    ? [
        { label: 'Total Journeys', value: data.total_journeys.toLocaleString(), def: METRIC_DEFINITIONS['Total Journeys'] },
        { label: 'Avg Path Length', value: `${data.avg_path_length} touchpoints`, def: METRIC_DEFINITIONS['Avg Path Length'] },
        {
          label: 'Avg Time to Convert',
          value: data.avg_time_to_conversion_days != null ? `${data.avg_time_to_conversion_days} days` : 'N/A',
          def: METRIC_DEFINITIONS['Avg Time to Convert'],
        },
        { label: 'Path Length Range', value: `${data.path_length_distribution.min} – ${data.path_length_distribution.max}`, def: METRIC_DEFINITIONS['Path Length Range'] },
      ]
    : []
  const segmentComparison = useMemo(() => {
    if (filters.segment === 'all' || !data || !baselinePathsQuery.data) return null
    const focusedTopShare = data.common_paths?.[0]?.share ?? null
    const baselineTopShare = baselinePathsQuery.data.common_paths?.[0]?.share ?? null
    return {
      journeySharePct:
        baselinePathsQuery.data.total_journeys > 0
          ? (data.total_journeys / baselinePathsQuery.data.total_journeys) * 100
          : null,
      focusedTopShare,
      baselineTopShare,
      focusedAvgLength: data.avg_path_length,
      baselineAvgLength: baselinePathsQuery.data.avg_path_length,
      focusedAvgLag: data.avg_time_to_conversion_days,
      baselineAvgLag: baselinePathsQuery.data.avg_time_to_conversion_days,
    }
  }, [baselinePathsQuery.data, data, filters.segment])

  const periodLabel =
    filters.dateFrom && filters.dateTo
      ? `${filters.dateFrom} – ${filters.dateTo}`
      : 'current dataset'

  const conversionLabel =
    data?.config?.conversion_key ||
    journeys?.primary_kpi_label ||
    journeys?.primary_kpi_id ||
    'All conversions'

  const pathLenDist = data?.path_length_distribution ?? { min: 0, max: 0, median: 0, p90: 0 }
  const timeDist = data?.time_to_conversion_distribution ?? null
  const directDiag = data?.direct_unknown_diagnostics ?? null
  const nbaConfig = data?.nba_config ?? null
  const hasAvgTimeColumn = filteredAndSortedPaths.some((path) => path.avg_time_to_convert_days != null)
  const pathsNarrative = useMemo(() => {
    const topPath = commonPaths[0] ?? null
    const headline = data
      ? `The current visible slice contains ${data.total_journeys.toLocaleString()} materialized journeys across ${commonPaths.length.toLocaleString()} ranked common paths.`
      : 'Conversion path analysis is loaded for the current slice.'
    const items = [
      topPath
        ? `The leading path contributes ${(topPath.share * 100).toFixed(1)}% of visible journeys: ${topPath.path}.`
        : null,
      timeDist?.median != null
        ? `Median time to convert is ${timeDist.median.toFixed(1)} days, with a P90 of ${timeDist.p90?.toFixed(1) ?? timeDist.max.toFixed(1)} days.`
        : null,
      directDiag
        ? `Direct or unknown touches account for ${(directDiag.touchpoint_share * 100).toFixed(1)}% of touchpoints, and ${(directDiag.journeys_ending_direct_share * 100).toFixed(1)}% of journeys end on Direct.`
        : null,
      segmentComparison
        ? `The focused audience contributes ${segmentComparison.journeySharePct?.toFixed(1) ?? '—'}% of workspace journeys and averages ${segmentComparison.focusedAvgLength.toFixed(1)} touches per path.`
        : null,
      selectedPath && selectedPathLagNote
        ? `Selected path family read: ${selectedPathLagNote}`
        : null,
    ].filter((item): item is string => Boolean(item))
    return { headline, items }
  }, [commonPaths, data, directDiag, segmentComparison, selectedPath, selectedPathLagNote, timeDist])

  const loadPathDetails = async (path: string) => {
    setSelectedPath(path)
    setSelectedPathDetails(null)
    setSelectedPathError(null)
    setSelectedPathLoading(true)
    try {
      const params = new URLSearchParams({
        path,
        direct_mode: directMode,
        path_scope: pathScope === 'all' ? 'all' : 'converted',
        definition_id: selectedJourneyId,
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
      })
      if (filters.channel !== 'all') params.set('channel_group', filters.channel)
      if (filters.campaign !== 'all') params.set('campaign_id', filters.campaign)
      if (filters.device !== 'all') params.set('device', filters.device)
      if (filters.geo !== 'all') params.set('country', filters.geo)
      const json = await apiGetJson<PathDetails>(`/api/conversion-paths/details?${params.toString()}`, {
        fallbackMessage: 'Failed to load path details',
      })
      setSelectedPathDetails(json)
    } catch (err) {
      setSelectedPathError((err as Error).message)
    } finally {
      setSelectedPathLoading(false)
    }
  }

  const frequencyColumns: AnalyticsTableColumn<(typeof sortedFreq)[number]>[] = [
    {
      key: 'channel',
      label: 'Channel',
      sortable: true,
      sortDirection: freqSort === 'channel' ? freqSortDir : null,
      onSort: () => {
        setFreqSort('channel')
        setFreqSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'))
      },
      render: (row) => row.channel,
      cellStyle: { fontWeight: t.font.weightMedium, color: t.color.text },
    },
    {
      key: 'count',
      label: 'Touchpoints',
      align: 'right',
      sortable: true,
      sortDirection: freqSort === 'count' ? freqSortDir : null,
      onSort: () => {
        setFreqSort('count')
        setFreqSortDir('desc')
      },
      render: (row) => row.count,
    },
    {
      key: 'pct',
      label: '% of Total',
      align: 'right',
      sortable: true,
      sortDirection: freqSort === 'pct' ? freqSortDir : null,
      onSort: () => {
        setFreqSort('pct')
        setFreqSortDir('desc')
      },
      render: (row) => `${row.pct.toFixed(1)}%`,
      cellStyle: { fontWeight: t.font.weightMedium, color: t.color.accent },
    },
  ]

  const pathColumns: AnalyticsTableColumn<(typeof filteredAndSortedPaths)[number]>[] = [
    {
      key: 'rank',
      label: '#',
      hideable: false,
      width: 40,
      render: (_row, idx) => idx + 1,
      cellStyle: { color: t.color.textMuted },
    },
    {
      key: 'path',
      label: 'Path',
      hideable: false,
      render: (path) => (
        <>
          {path.path.split(' > ').map((step, i, arr) => (
            <span key={`${path.path}-${i}`}>
              <span
                style={{
                  display: 'inline-block',
                  padding: '2px 8px',
                  backgroundColor: t.color.accentMuted,
                  color: t.color.accent,
                  borderRadius: t.radius.sm,
                  fontSize: t.font.sizeXs,
                  fontWeight: t.font.weightSemibold,
                }}
              >
                {step}
              </span>
              {i < arr.length - 1 && <span style={{ margin: '0 4px', color: t.color.textMuted }}>→</span>}
            </span>
          ))}
        </>
      ),
    },
    {
      key: 'count',
      label: 'Count',
      align: 'right',
      sortable: true,
      sortDirection: pathSort === 'count' ? pathSortDir : null,
      onSort: () => {
        setPathSort('count')
        setPathSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'))
      },
      render: (path) => path.count,
      cellStyle: { fontWeight: t.font.weightMedium },
    },
    {
      key: 'share',
      label: 'Share',
      align: 'right',
      sortable: true,
      sortDirection: pathSort === 'share' ? pathSortDir : null,
      onSort: () => {
        setPathSort('share')
        setPathSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'))
      },
      render: (path) => `${(path.share * 100).toFixed(1)}%`,
      cellStyle: { fontWeight: t.font.weightMedium, color: t.color.accent },
    },
    {
      key: 'length',
      label: 'Avg touchpoints',
      align: 'right',
      sortable: true,
      sortDirection: pathSort === 'length' ? pathSortDir : null,
      onSort: () => {
        setPathSort('length')
        setPathSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'))
      },
      render: (path) => path.path_length ?? path.path.split(' > ').length,
      cellStyle: { color: t.color.textSecondary },
    },
    ...(hasAvgTimeColumn
      ? [
          {
            key: 'avg_time',
            label: 'Avg time to convert',
            align: 'right' as const,
            sortable: true,
            sortDirection: pathSort === 'avg_time' ? pathSortDir : null,
            onSort: () => {
              setPathSort('avg_time')
              setPathSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'))
            },
            render: (path: (typeof filteredAndSortedPaths)[number]) =>
              path.avg_time_to_convert_days != null ? `${path.avg_time_to_convert_days.toFixed(1)}d` : '—',
            cellStyle: { color: t.color.textSecondary },
          } satisfies AnalyticsTableColumn<(typeof filteredAndSortedPaths)[number]>,
        ]
      : []),
    {
      key: 'suggested_next',
      label: 'Suggested next',
      render: (path) => {
        const prefix = path.path.split(' > ').slice(0, -1).join(' > ')
        const top = data?.next_best_by_prefix?.[prefix]?.[0]
        return top ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.xs, flexWrap: 'wrap' }}>
            <span
              title={`${top.count} journeys, ${(top.conversion_rate * 100).toFixed(1)}% conversion, avg value $${top.avg_value}`}
              style={{
                display: 'inline-block',
                padding: '2px 8px',
                backgroundColor: t.color.accentMuted,
                color: t.color.accent,
                borderRadius: t.radius.sm,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightSemibold,
              }}
            >
              {top.channel} ({(top.conversion_rate * 100).toFixed(0)}%)
            </span>
            {top.is_promoted_policy ? (
              <>
                <span
                  title={top.promoted_policy_title ?? 'Promoted Journey Lab policy'}
                  style={{
                    display: 'inline-block',
                    padding: '2px 8px',
                    border: `1px solid ${t.color.warning}`,
                    color: t.color.warning,
                    borderRadius: t.radius.full,
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                  }}
                >
                  Policy
                </span>
                {buildJourneyHypothesisHref({
                  journeyDefinitionId: top.promoted_policy_journey_definition_id,
                  hypothesisId: top.promoted_policy_hypothesis_id,
                }) ? (
                  <a
                    href={buildJourneyHypothesisHref({
                      journeyDefinitionId: top.promoted_policy_journey_definition_id,
                      hypothesisId: top.promoted_policy_hypothesis_id,
                    }) || '#'}
                    style={{ fontSize: t.font.sizeXs, color: t.color.accent, textDecoration: 'none' }}
                  >
                    Open policy
                  </a>
                ) : null}
              </>
            ) : null}
          </div>
        ) : (
          <span style={{ color: t.color.textMuted, fontSize: t.font.sizeXs }}>—</span>
        )
      },
    },
  ]

  const definitions = definitionsQuery.data?.items ?? []
  const journeyOptions = definitions
    .filter((item) => !item.is_archived)
    .map((item) => ({ value: item.id, label: item.name }))
  const selectedDefinitionName = selectedDefinition?.name || 'Journey definition'
  const lifecycleStatus = definitionLifecycleQuery.data?.rebuild_state?.status || selectedDefinition?.lifecycle_status || 'active'
  const liveJourneyCount = journeys?.count ?? null
  const countMismatch = liveJourneyCount != null && data ? liveJourneyCount !== data.total_journeys : false
  const rangeMismatch =
    Boolean(journeys?.date_max && data?.date_to) && journeys?.date_max?.slice(0, 10) !== data?.date_to
  const dashboardLoading =
    definitionsQuery.isLoading ||
    (!selectedJourneyId && preferredDefinitionQuery.isLoading) ||
    (definitions.length > 0 && !selectedJourneyId) ||
    (!!selectedJourneyId && pathsQuery.isLoading)
  const dashboardError =
    (definitionsQuery.error as Error | undefined)?.message ||
    (preferredDefinitionQuery.error as Error | undefined)?.message ||
    (pathsQuery.error as Error | undefined)?.message ||
    (dimensionsQuery.error as Error | undefined)?.message ||
    null
  const noDefinitions = !dashboardLoading && definitions.length === 0
  const noDataForSelection = !dashboardLoading && !!selectedJourneyId && !!data && data.total_journeys === 0
  const currentFilterSegmentDefinition = activeLocalSegmentDefinitionFromFilters(filters)
  const canSaveCurrentFilterSegment = hasLocalSegmentCriteria(currentFilterSegmentDefinition)
  const defaultFilterSegmentName = buildLocalSegmentDefaultName(currentFilterSegmentDefinition)

  return (
    <DashboardPage
      title="Conversion Paths"
      description="Top paths, next steps, and path quality for the selected journey definition."
      filters={
        <div style={{ display: 'grid', gap: t.space.sm }}>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
            <select
              value={selectedJourneyId}
              onChange={(e) => setSelectedJourneyId(e.target.value)}
              style={{ minWidth: 220, padding: '6px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}`, background: t.color.surface, color: t.color.text, fontSize: t.font.sizeSm }}
            >
              {!journeyOptions.length && <option value="">No journey definitions</option>}
              {journeyOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              Path analysis uses observed workspace values for the selected journey definition and date window. Saved local segments apply as analytical slices here.
            </div>
          </div>
          <GlobalFilterBar
            value={filters}
            onChange={handleFiltersChange}
            channels={journeyFilterOptions.channels}
            campaigns={journeyFilterOptions.campaigns}
            devices={journeyFilterOptions.devices}
            geos={journeyFilterOptions.geos}
            segments={journeyFilterOptions.segments}
            showSegment
          />
          <div style={{ display: 'flex', justifyContent: 'flex-end', flexWrap: 'wrap', gap: t.space.sm }}>
            {selectedLocalSegment ? (
              <label style={{ display: 'grid', gap: 6, minWidth: 240, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <span>Compare with</span>
                <select
                  value={compareSegmentId}
                  onChange={(event) => setCompareSegmentId(event.target.value)}
                  style={{
                    padding: '8px 10px',
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    fontSize: t.font.sizeSm,
                  }}
                >
                  <option value="">No paired comparison</option>
                  {localAnalyticalSegments
                    .filter((segment) => segment.id !== selectedLocalSegment.id)
                    .map((segment) => (
                      <option key={segment.id} value={segment.id}>
                        {segmentOptionLabel(segment)}
                      </option>
                    ))}
                </select>
              </label>
            ) : null}
            <a
              href="/?page=settings#settings/segments"
              style={{
                border: `1px solid ${t.color.border}`,
                background: 'transparent',
                borderRadius: t.radius.sm,
                padding: '8px 12px',
                textDecoration: 'none',
                color: t.color.text,
                fontSize: t.font.sizeSm,
              }}
            >
              Manage segments
            </a>
            <button
              type="button"
              disabled={!canSaveCurrentFilterSegment}
              onClick={() => {
                setSaveSegmentError(null)
                setShowSaveSegmentModal(true)
              }}
              style={{
                border: `1px solid ${t.color.border}`,
                background: 'transparent',
                borderRadius: t.radius.sm,
                padding: '8px 12px',
                cursor: canSaveCurrentFilterSegment ? 'pointer' : 'default',
                opacity: canSaveCurrentFilterSegment ? 1 : 0.6,
              }}
            >
              Save local segment
            </button>
          </div>
        </div>
      }
      actions={
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
          <button
            type="button"
            onClick={() => setDirectMode((m) => (m === 'include' ? 'exclude' : 'include'))}
            style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.full, padding: '6px 10px', fontSize: t.font.sizeXs, backgroundColor: t.color.bg, cursor: 'pointer' }}
            title="View filter only; underlying attribution models are unchanged."
          >
            {directMode === 'include' ? 'Include Direct' : 'Exclude Direct'}
          </button>
          {journeys && journeys.non_converted > 0 ? (
            <button
              type="button"
              onClick={() => setPathScope((s) => (s === 'converted' ? 'all' : 'converted'))}
              style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.full, padding: '6px 10px', fontSize: t.font.sizeXs, backgroundColor: t.color.bg, cursor: 'pointer' }}
              title="Include non-converted journeys in path statistics."
            >
              {pathScope === 'converted' ? 'Converted only' : 'Converted + non-converted'}
            </button>
          ) : null}
          <button
            type="button"
            onClick={() => setShowContext((v) => !v)}
            style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.full, padding: '6px 10px', fontSize: t.font.sizeXs, backgroundColor: showContext ? t.color.accentMuted : t.color.surface, color: t.color.accent, cursor: 'pointer' }}
          >
            {showContext ? 'Hide context' : 'Show context'}
          </button>
          <button
            type="button"
            onClick={() => setShowDiagnostics((v) => !v)}
            style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.full, padding: '6px 10px', fontSize: t.font.sizeXs, backgroundColor: showDiagnostics ? t.color.warningMuted : t.color.surface, color: t.color.warning, cursor: 'pointer' }}
          >
            {showDiagnostics ? 'Hide diagnostics' : 'Show diagnostics'}
          </button>
        </div>
      }
      isLoading={dashboardLoading}
      isError={Boolean(dashboardError)}
      errorMessage={dashboardError}
      isEmpty={noDefinitions || noDataForSelection}
      emptyState={
        noDefinitions ? (
          <SectionCard title="No journey definitions" subtitle="Create a journey definition first.">
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Conversion Paths now follows the selected journey definition and its workspace-derived filters.
            </div>
          </SectionCard>
        ) : (
          <SectionCard title="No conversion path data" subtitle="No path rows were found for the selected definition and filter window.">
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Try widening the date range, clearing campaign/device/geo filters, or rebuilding the journey definition outputs.
            </div>
          </SectionCard>
        )
      }
    >
      <div style={{ maxWidth: 1400, margin: '0 auto', display: 'grid', gap: t.space.xl }}>
        <SectionCard
          title="Analysis context"
          subtitle="Which definition, source, and time coverage this page is actually using."
        >
          <div style={{ display: 'grid', gap: t.space.md }}>
            <ContextSummaryStrip
              minItemWidth={220}
              items={[
                { label: 'Journey definition', value: <strong style={{ fontWeight: t.font.weightSemibold }}>{selectedDefinitionName}</strong> },
                { label: 'Data source', value: data?.source || '—' },
                { label: 'Covered period', value: data?.date_from && data?.date_to ? `${data.date_from} – ${data.date_to}` : periodLabel },
                { label: 'Last rebuilt', value: formatLifecycleTimestamp(definitionLifecycleQuery.data?.rebuild_state?.last_rebuilt_at) },
                {
                  label: 'Lifecycle status',
                  value: lifecycleStatus,
                  valueColor: lifecycleStatus === 'stale' ? t.color.warning : t.color.text,
                },
                { label: 'Conversion KPI', value: conversionLabel },
              ]}
            />
            {countMismatch || rangeMismatch ? (
              <div style={{ padding: t.space.md, borderRadius: t.radius.md, border: `1px solid ${t.color.warning}`, background: t.color.warningMuted, fontSize: t.font.sizeSm, color: t.color.text }}>
                Workspace attribution currently shows <strong>{liveJourneyCount?.toLocaleString() ?? '—'}</strong> live journeys through{' '}
                <strong>{journeys?.date_max?.slice(0, 10) ?? '—'}</strong>. Conversion Paths is using{' '}
                <strong>{data?.total_journeys.toLocaleString() ?? '—'}</strong> materialized journey outputs through{' '}
                <strong>{data?.date_to ?? '—'}</strong>.
              </div>
            ) : null}
            {buildJourneyPathsHref(selectedJourneyId) ? (
              <div>
                <a href={buildJourneyPathsHref(selectedJourneyId) || '#'} style={{ fontSize: t.font.sizeSm, color: t.color.accent, textDecoration: 'none' }}>
                  Open selected definition in Journey Lab
                </a>
              </div>
            ) : null}
          </div>
        </SectionCard>
        {segmentComparison ? (
          <ContextSummaryStrip
            minItemWidth={220}
            items={[
              {
                label: 'Focused journey share',
                value:
                  segmentComparison.journeySharePct != null
                    ? `${segmentComparison.journeySharePct.toFixed(1)}% of workspace`
                    : '—',
              },
              {
                label: 'Top-path concentration',
                value:
                  segmentComparison.focusedTopShare != null
                    ? `${(segmentComparison.focusedTopShare * 100).toFixed(1)}% vs ${segmentComparison.baselineTopShare != null ? `${(segmentComparison.baselineTopShare * 100).toFixed(1)}%` : '—'} baseline`
                    : '—',
              },
              {
                label: 'Avg path length',
                value: `${segmentComparison.focusedAvgLength.toFixed(1)} vs ${segmentComparison.baselineAvgLength.toFixed(1)} baseline`,
              },
              {
                label: 'Avg time to convert',
                value:
                  segmentComparison.focusedAvgLag != null
                    ? `${segmentComparison.focusedAvgLag.toFixed(1)}d vs ${segmentComparison.baselineAvgLag != null ? `${segmentComparison.baselineAvgLag.toFixed(1)}d` : '—'}`
                    : '—',
              },
            ]}
          />
        ) : null}
        {selectedLocalSegment ? <SegmentOverlapNotice selectedSegment={selectedLocalSegment} /> : null}
        {selectedLocalSegment && compareLocalSegment && segmentCompareQuery.data ? (
          <SectionCard
            title="Segment vs segment"
            subtitle={`How ${selectedLocalSegment.name} compares directly with ${compareLocalSegment.name} in path structure and lag.`}
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              <SegmentComparisonContextNote
                mode="exact_filter"
                pageLabel="conversion-path rows"
                basisLabel="matched journey-instance rows"
                primaryLabel={selectedLocalSegment.name}
                primaryRows={segmentCompareQuery.data.primary_summary.journey_rows}
                otherLabel={compareLocalSegment.name}
                otherRows={segmentCompareQuery.data.other_summary.journey_rows}
                baselineRows={segmentCompareQuery.data.baseline_summary.journey_rows}
                overlapRows={segmentCompareQuery.data.overlap.overlap_rows}
              />
              <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Relationship</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity · {segmentCompareQuery.data.overlap.overlap_rows.toLocaleString()} shared rows
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{selectedLocalSegment.name}</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {(segmentCompareQuery.data.primary_summary.journey_rows ?? 0).toLocaleString()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    rows · avg path {segmentCompareQuery.data.primary_summary.avg_path_length != null ? `${segmentCompareQuery.data.primary_summary.avg_path_length.toFixed(1)} steps` : '—'}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle, display: 'grid', gap: 4 }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{compareLocalSegment.name}</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {(segmentCompareQuery.data.other_summary.journey_rows ?? 0).toLocaleString()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    rows · avg path {segmentCompareQuery.data.other_summary.avg_path_length != null ? `${segmentCompareQuery.data.other_summary.avg_path_length.toFixed(1)} steps` : '—'}
                  </div>
                </div>
              </div>
              <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
                {[
                  {
                    label: 'Journey delta',
                    value:
                      segmentCompareQuery.data.deltas.journey_rows == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.journey_rows >= 0 ? '+' : '-'}${Math.abs(segmentCompareQuery.data.deltas.journey_rows).toLocaleString()}`,
                  },
                  {
                    label: 'Revenue delta',
                    value:
                      segmentCompareQuery.data.deltas.revenue == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : '-'}$${Math.abs(segmentCompareQuery.data.deltas.revenue).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
                  },
                  {
                    label: 'Median lag delta',
                    value:
                      segmentCompareQuery.data.deltas.median_lag_days == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.median_lag_days >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.median_lag_days}d`,
                  },
                  {
                    label: 'Avg. path length delta',
                    value:
                      segmentCompareQuery.data.deltas.avg_path_length == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.avg_path_length >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.avg_path_length.toFixed(1)} steps`,
                  },
                ].map((item) => (
                  <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface, display: 'grid', gap: 4 }}>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                    <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.value}</div>
                  </div>
                ))}
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <a
                  href={buildIncrementalityPlannerHref({
                    conversionKey: conversionLabel,
                    startAt: filters.dateFrom,
                    endAt: filters.dateTo,
                    segmentId: selectedLocalSegment.id,
                    name: `Audience path test: ${selectedLocalSegment.name} vs ${compareLocalSegment.name}`,
                    notes: `Compare ${selectedLocalSegment.name} against ${compareLocalSegment.name} in Conversion Paths. Relationship ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity.`,
                  })}
                  style={{
                    border: `1px solid ${t.color.accent}`,
                    background: t.color.accent,
                    color: '#fff',
                    borderRadius: t.radius.sm,
                    padding: '8px 12px',
                    fontSize: t.font.sizeSm,
                    textDecoration: 'none',
                  }}
                >
                  Draft experiment
                </a>
                {buildJourneyHypothesisSeedHref({
                  journeyDefinitionId: selectedJourneyId,
                  title: `Audience path hypothesis: ${selectedLocalSegment.name} vs ${compareLocalSegment.name}`,
                  hypothesisText: `${selectedLocalSegment.name} behaves differently from ${compareLocalSegment.name} in conversion paths and should be tested with a tailored journey intervention.`,
                  supportCount: segmentCompareQuery.data.primary_summary.journey_rows ?? null,
                }) ? (
                  <a
                    href={buildJourneyHypothesisSeedHref({
                      journeyDefinitionId: selectedJourneyId,
                      title: `Audience path hypothesis: ${selectedLocalSegment.name} vs ${compareLocalSegment.name}`,
                      hypothesisText: `${selectedLocalSegment.name} behaves differently from ${compareLocalSegment.name} in conversion paths and should be tested with a tailored journey intervention.`,
                      supportCount: segmentCompareQuery.data.primary_summary.journey_rows ?? null,
                    }) || '#'}
                    style={{
                      border: `1px solid ${t.color.border}`,
                      background: t.color.surface,
                      color: t.color.text,
                      borderRadius: t.radius.sm,
                      padding: '8px 12px',
                      fontSize: t.font.sizeSm,
                      textDecoration: 'none',
                    }}
                  >
                    Draft hypothesis
                  </a>
                ) : null}
                <a
                  href={buildSegmentComparisonHref(selectedLocalSegment.id, compareLocalSegment.id)}
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    padding: '8px 12px',
                    fontSize: t.font.sizeSm,
                    textDecoration: 'none',
                  }}
                >
                  Open segment compare
                </a>
              </div>
            </div>
          </SectionCard>
        ) : null}

        <AnalysisNarrativePanel
          title="What changed"
          subtitle="A short readout of the visible path family before you move into the detailed charts and tables."
          headline={pathsNarrative.headline}
          items={pathsNarrative.items}
        />

        <CollapsiblePanel
          title="Method & Context"
          subtitle="Why the counts look the way they do, and which attribution settings shape this view."
          open={showContext}
          onToggle={() => setShowContext((v) => !v)}
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
            {journeys?.readiness && (journeys.readiness.status === 'blocked' || journeys.readiness.warnings.length > 0) ? (
              <DecisionStatusCard
                title="Path Analysis Reliability Warning"
                status={journeys.readiness.status}
                blockers={journeys.readiness.blockers}
                warnings={journeys.readiness.warnings.slice(0, 3)}
              />
            ) : null}
            <ExplainabilityPanel scope="paths" />
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <strong style={{ color: t.color.text }}>Current settings:</strong>{' '}
              {data?.config?.time_window
                ? [
                    data?.config?.time_window?.click_lookback_days != null ? `Click ${data.config.time_window.click_lookback_days}d` : null,
                    data?.config?.time_window?.impression_lookback_days != null ? `Impr. ${data.config.time_window.impression_lookback_days}d` : null,
                    data?.config?.time_window?.session_timeout_minutes != null ? `Session ${data.config.time_window.session_timeout_minutes}m` : null,
                  ]
                    .filter(Boolean)
                    .join(' · ')
                : 'Read-only attribution defaults'}
            </div>
          </div>
        </CollapsiblePanel>

        <CollapsiblePanel
          title="Diagnostics"
          subtitle="Coverage gaps, Direct/Unknown reliance, and path anomalies."
          open={showDiagnostics}
          onToggle={() => setShowDiagnostics((v) => !v)}
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
            {directDiag ? (
              <div
                style={{
                  padding: t.space.md,
                  borderRadius: t.radius.lg,
                  border: `1px solid ${t.color.borderLight}`,
                  background: t.color.surface,
                  boxShadow: t.shadowSm,
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  gap: t.space.md,
                  flexWrap: 'wrap',
                }}
              >
                <div>
                  <h3 style={{ margin: '0 0 4px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    Direct / Unknown impact
                  </h3>
                  <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    Approximate share of all touchpoints and converted journeys dominated by direct or unknown channels.
                  </p>
                </div>
                <div style={{ display: 'flex', gap: t.space.lg, flexWrap: 'wrap', alignItems: 'center' }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                    <strong>{(directDiag.touchpoint_share * 100).toFixed(1)}%</strong>{' '}
                    <span style={{ color: t.color.textSecondary }}>Direct/Unknown touchpoints</span>
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                    <strong>{(directDiag.journeys_ending_direct_share * 100).toFixed(1)}%</strong>{' '}
                    <span style={{ color: t.color.textSecondary }}>journeys ending on Direct</span>
                  </div>
                </div>
              </div>
            ) : null}
            {anomaliesQuery.data && anomaliesQuery.data.anomalies.length > 0 ? (
              <div
                style={{
                  padding: t.space.md,
                  borderRadius: t.radius.lg,
                  border: `1px solid ${t.color.warning}`,
                  background: t.color.warningMuted,
                }}
              >
                <strong style={{ display: 'block', marginBottom: t.space.sm, fontSize: t.font.sizeSm, color: t.color.warning }}>
                  Path anomalies detected
                </strong>
                <ul style={{ margin: 0, paddingLeft: 20, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  {anomaliesQuery.data.anomalies.map((a, idx) => (
                    <li key={`${a.type}-${idx}`}>{a.message}</li>
                  ))}
                </ul>
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No path anomalies detected for the current view.</div>
            )}
          </div>
        </CollapsiblePanel>

      {/* KPI strip */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        {kpis.map((kpi) => (
          <div
            key={kpi.label}
            style={{
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              padding: `${t.space.lg}px ${t.space.xl}px`,
              boxShadow: t.shadowSm,
            }}
          >
            <div
              style={{
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                color: t.color.textMuted,
                textTransform: 'uppercase',
                letterSpacing: '0.04em',
                display: 'flex',
                alignItems: 'center',
                gap: 4,
              }}
              title={kpi.def}
            >
              {kpi.label}
              <span style={{ opacity: 0.7, cursor: 'help' }} aria-label="Definition">ⓘ</span>
            </div>
            <div
              style={{
                fontSize: t.font.sizeXl,
                fontWeight: t.font.weightBold,
                color: t.color.text,
                marginTop: t.space.xs,
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {kpi.value}
            </div>
            {kpi.label === 'Avg Path Length' && (
              <div
                style={{
                  marginTop: t.space.xs,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  fontVariantNumeric: 'tabular-nums',
                }}
              >
                Median {pathLenDist.median.toFixed(1)} · P90{' '}
                {(pathLenDist.p90 ?? pathLenDist.max).toFixed(1)}
              </div>
            )}
            {kpi.label === 'Avg Time to Convert' && timeDist && (
              <div
                style={{
                  marginTop: t.space.xs,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  fontVariantNumeric: 'tabular-nums',
                }}
              >
                Median {timeDist.median.toFixed(1)}d · P90 {timeDist.p90?.toFixed(1) ?? timeDist.max.toFixed(1)}d
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Channel frequency + table */}
      <style>{`
        @media (max-width: 900px) { .conv-charts { grid-template-columns: 1fr !important; } }
      `}</style>
      <div
        className="conv-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: t.space.xl,
          marginBottom: t.space.xl,
          minWidth: 0,
        }}
      >
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            padding: t.space.xl,
            boxShadow: t.shadowSm,
            minWidth: 0,
          }}
        >
          <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Channel Frequency in Paths
          </h3>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={sortedFreq} margin={{ top: 8, right: 16, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
              <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
              <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
              <Bar dataKey="count" fill={t.color.chart[4]} name="Touchpoints" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            padding: t.space.xl,
            boxShadow: t.shadowSm,
            minWidth: 0,
          }}
        >
          <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Channel Touchpoint Stats
          </h3>
          <AnalyticsTable
            columns={frequencyColumns}
            rows={sortedFreq}
            rowKey={(row) => row.channel}
            tableLabel="Channel touchpoint stats"
            minWidth={480}
            stickyFirstColumn
          />
        </div>
      </div>

      {/* Direct / Unknown diagnostics */}
      {directDiag && (
        <div
          style={{
            marginBottom: t.space.xl,
            padding: t.space.md,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.borderLight}`,
            background: t.color.surface,
            boxShadow: t.shadowSm,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: t.space.md,
            flexWrap: 'wrap',
          }}
        >
          <div>
            <h3 style={{ margin: '0 0 4px', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Direct / Unknown impact
            </h3>
            <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              Approximate share of all touchpoints and conversions that are dominated by <code>direct</code> or{' '}
              <code>unknown</code> channels. Use this as a quick trust indicator for path‑based insights.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.lg, flexWrap: 'wrap', alignItems: 'center' }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
              <strong>{(directDiag.touchpoint_share * 100).toFixed(1)}%</strong>{' '}
              <span style={{ color: t.color.textSecondary }}>of touchpoints are Direct/Unknown</span>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
              <strong>{(directDiag.journeys_ending_direct_share * 100).toFixed(1)}%</strong>{' '}
              <span style={{ color: t.color.textSecondary }}>of converted journeys end on Direct</span>
            </div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              For deeper breakdown, use the Why? panel and the Data quality dashboard.
            </div>
          </div>
        </div>
      )}

      {/* Try path – Next Best Action */}
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
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Try path – Next Best Action
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.lg }}>
          Enter the path so far (e.g. <code style={{ background: t.color.bg, padding: '2px 6px', borderRadius: t.radius.sm }}>google_ads</code> or <code style={{ background: t.color.bg, padding: '2px 6px', borderRadius: t.radius.sm }}>google_ads, email</code>) to see recommended next channels or campaigns.
        </p>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.md, alignItems: 'center', marginBottom: tryPathResult || tryPathError ? t.space.lg : 0 }}>
          <input
            type="text"
            value={tryPathInput}
            onChange={(e) => setTryPathInput(e.target.value)}
            placeholder="e.g. google_ads > email"
            style={{
              flex: '1',
              minWidth: 200,
              padding: `${t.space.sm}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              color: t.color.text,
            }}
            onKeyDown={(e) => e.key === 'Enter' && (document.querySelector('[data-try-path-btn]') as HTMLButtonElement)?.click()}
          />
          {data?.next_best_by_prefix_campaign && (
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <input
                type="radio"
                checked={tryPathLevel === 'channel'}
                onChange={() => setTryPathLevel('channel')}
              />
              Channel
            </label>
          )}
          {data?.next_best_by_prefix_campaign && (
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <input
                type="radio"
                checked={tryPathLevel === 'campaign'}
                onChange={() => setTryPathLevel('campaign')}
              />
              Campaign
            </label>
          )}
          <button
            data-try-path-btn
            type="button"
            disabled={!data}
            onClick={async () => {
              setTryPathError(null)
              setTryPathResult(null)
              const normalized = normalizePathInput(tryPathInput)
              if (!normalized) {
                setTryPathError('Enter at least one observed step.')
                return
              }
              const level = tryPathLevel === 'campaign' && data?.next_best_by_prefix_campaign ? 'campaign' : 'channel'
              const source = level === 'campaign' ? data?.next_best_by_prefix_campaign : data?.next_best_by_prefix
              setTryPathResult({
                path_so_far: normalized,
                level,
                recommendations: source?.[normalized] ?? [],
              })
            }}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.surface,
              backgroundColor: t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: data ? 'pointer' : 'not-allowed',
            }}
          >
            Get next best
          </button>
        </div>
        {tryPathError && (
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.danger }}>{tryPathError}</p>
        )}
        {tryPathResult && (
          <div>
            <p style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              After path <strong style={{ color: t.color.text }}>{tryPathResult.path_so_far}</strong>
              {tryPathResult.level === 'campaign' && ' (campaign-level)'}:
            </p>
            <ul style={{ margin: 0, paddingLeft: t.space.xl, listStyle: 'disc', display: 'flex', flexDirection: 'column', gap: t.space.xs }}>
              {tryPathResult.recommendations.length === 0 ? (
                <li style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>No recommendations for this prefix.</li>
              ) : (
                tryPathResult.recommendations.map((rec, i) => (
                  <li key={i} style={{ fontSize: t.font.sizeSm }}>
                    <span
                      style={{
                        display: 'inline-block',
                        padding: '2px 8px',
                        backgroundColor: t.color.accentMuted,
                        color: t.color.accent,
                        borderRadius: t.radius.sm,
                        fontWeight: t.font.weightSemibold,
                        marginRight: t.space.sm,
                      }}
                    >
                      {rec.campaign != null ? `${rec.channel} / ${rec.campaign}` : rec.channel}
                    </span>
                    {rec.is_promoted_policy ? (
                      <>
                        <span
                          title={rec.promoted_policy_title ?? 'Promoted Journey Lab policy'}
                          style={{
                            display: 'inline-block',
                            padding: '2px 8px',
                            border: `1px solid ${t.color.warning}`,
                            color: t.color.warning,
                            borderRadius: t.radius.full,
                            fontSize: t.font.sizeXs,
                            fontWeight: t.font.weightSemibold,
                            marginRight: t.space.sm,
                          }}
                        >
                          Deployed policy
                        </span>
                        {buildJourneyHypothesisHref({
                          journeyDefinitionId: rec.promoted_policy_journey_definition_id,
                          hypothesisId: rec.promoted_policy_hypothesis_id,
                        }) ? (
                          <a
                            href={buildJourneyHypothesisHref({
                              journeyDefinitionId: rec.promoted_policy_journey_definition_id,
                              hypothesisId: rec.promoted_policy_hypothesis_id,
                            }) || '#'}
                            style={{ marginRight: t.space.sm, fontSize: t.font.sizeXs, color: t.color.accent, textDecoration: 'none' }}
                          >
                            Open policy
                          </a>
                        ) : null}
                      </>
                    ) : null}
                    <span style={{ color: t.color.textSecondary }}>
                      Confidence {(rec.conversion_rate * 100).toFixed(1)}% · support {rec.count} journeys · avg ${rec.avg_value}
                    </span>
                    {((tryPathResult as any).nba_config ?? nbaConfig) &&
                      rec.count < (nbaConfig?.min_prefix_support || 0) * 2 && (
                        <span style={{ marginLeft: t.space.sm, fontSize: t.font.sizeXs, color: t.color.warning }}>
                          Low sample size: recommendation may be unreliable
                        </span>
                      )}
                  </li>
                ))
              )}
            </ul>
          </div>
        )}
      </div>

      {/* Common paths table + export */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.lg, flexWrap: 'wrap', gap: t.space.md }}>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Most Common Conversion Paths
          </h3>
        </div>
        <div style={{ marginBottom: t.space.lg }}>
          <AnalyticsToolbar
            filters={
              <>
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  <span>Min count</span>
                  <input
                    type="number"
                    min={1}
                    value={minPathCount}
                    onChange={(e) => {
                      const v = parseInt(e.target.value || '1', 10)
                      setMinPathCount(Number.isFinite(v) && v > 0 ? v : 1)
                    }}
                    style={{
                      width: 64,
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: `1px solid ${t.color.border}`,
                      borderRadius: t.radius.sm,
                    }}
                  />
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  <span>Path length</span>
                  <input
                    type="number"
                    min={1}
                    value={minPathLength}
                    onChange={(e) => {
                      const v = parseInt(e.target.value || '1', 10)
                      setMinPathLength(Number.isFinite(v) && v > 0 ? v : 1)
                    }}
                    style={{
                      width: 56,
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: `1px solid ${t.color.border}`,
                      borderRadius: t.radius.sm,
                    }}
                  />
                  <span>–</span>
                  <input
                    type="number"
                    min={1}
                    value={typeof maxPathLength === 'number' ? maxPathLength : ''}
                    onChange={(e) => {
                      const raw = e.target.value
                      if (!raw) {
                        setMaxPathLength('')
                        return
                      }
                      const v = parseInt(raw, 10)
                      setMaxPathLength(Number.isFinite(v) && v > 0 ? v : '')
                    }}
                    placeholder="Any"
                    style={{
                      width: 56,
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: `1px solid ${t.color.border}`,
                      borderRadius: t.radius.sm,
                    }}
                  />
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary, flexWrap: 'wrap' }}>
                  <span>Contains channel</span>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, maxWidth: 320 }}>
                    {availableChannelFilters.map((channel) => {
                      const active = channelFilter.includes(channel)
                      return (
                        <button
                          key={channel}
                          type="button"
                          onClick={() =>
                            setChannelFilter((prev) =>
                              prev.includes(channel) ? prev.filter((value) => value !== channel) : [...prev, channel],
                            )
                          }
                          style={{
                            borderRadius: t.radius.full,
                            border: `1px solid ${active ? t.color.accent : t.color.borderLight}`,
                            padding: '1px 8px',
                            fontSize: t.font.sizeXs,
                            backgroundColor: active ? t.color.accentMuted : 'transparent',
                            color: active ? t.color.accent : t.color.textSecondary,
                            cursor: 'pointer',
                          }}
                        >
                          {channel}
                        </button>
                      )
                    })}
                  </div>
                </div>
              </>
            }
            actions={
              <>
                {(minPathCount > 1 || minPathLength > 1 || typeof maxPathLength === 'number' || channelFilter.length > 0) ? (
                  <button
                    type="button"
                    onClick={() => {
                      setMinPathCount(1)
                      setMinPathLength(1)
                      setMaxPathLength('')
                      setChannelFilter([])
                    }}
                    style={{
                      padding: `${t.space.sm}px ${t.space.md}px`,
                      fontSize: t.font.sizeSm,
                      color: t.color.textSecondary,
                      background: 'transparent',
                      border: `1px solid ${t.color.border}`,
                      borderRadius: t.radius.sm,
                      cursor: 'pointer',
                    }}
                  >
                    Clear filters
                  </button>
                ) : null}
                <button
                  type="button"
                  onClick={() =>
                    exportPathsCSV(filteredAndSortedPaths, data?.next_best_by_prefix, {
                      period: periodLabel,
                      conversionKey: data?.config?.conversion_key ?? null,
                      configVersion: data?.config?.config_version ?? null,
                      directMode,
                      pathScope,
                      filters: {
                        minCount: minPathCount,
                        minPathLength,
                        maxPathLength: typeof maxPathLength === 'number' ? maxPathLength : null,
                        containsChannels: channelFilter,
                      },
                    })
                  }
                  style={{
                    padding: `${t.space.sm}px ${t.space.lg}px`,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    color: t.color.accent,
                    background: 'transparent',
                    border: `1px solid ${t.color.accent}`,
                    borderRadius: t.radius.sm,
                    cursor: 'pointer',
                  }}
                >
                  Export CSV
                </button>
              </>
            }
            summary={`Showing ${Math.min(filteredAndSortedPaths.length, 20)} of ${filteredAndSortedPaths.length} filtered paths · ${enrichedPaths.length} total`}
            padded
          />
        </div>
        <div style={{ overflowX: 'auto' }}>
          <AnalyticsTable
            columns={pathColumns}
            rows={filteredAndSortedPaths.slice(0, 20)}
            rowKey={(path, idx) => `${path.path}-${idx}`}
            tableLabel="Top conversion paths"
            minWidth={980}
            stickyFirstColumn
            allowColumnHiding
            allowDensityToggle
            persistKey="conversion-paths-top-table"
            defaultHiddenColumnKeys={hasAvgTimeColumn ? ['avg_time'] : []}
            presets={[
              {
                key: 'overview',
                label: 'Overview',
                visibleColumnKeys: hasAvgTimeColumn
                  ? ['rank', 'path', 'count', 'share', 'length', 'avg_time']
                  : ['rank', 'path', 'count', 'share', 'length'],
              },
              {
                key: 'volume',
                label: 'Volume',
                visibleColumnKeys: ['rank', 'path', 'count', 'share', 'length'],
              },
              {
                key: 'next',
                label: 'Next step',
                visibleColumnKeys: hasAvgTimeColumn
                  ? ['rank', 'path', 'count', 'share', 'avg_time', 'suggested_next']
                  : ['rank', 'path', 'count', 'share', 'suggested_next'],
              },
            ]}
            defaultPresetKey="overview"
            onRowClick={(path) => {
              void loadPathDetails(path.path)
            }}
            isRowActive={(path) => selectedPath === path.path}
          />
        </div>
        {filteredAndSortedPaths.length > 20 && (
          <p style={{ margin: `${t.space.md}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Showing top 20 of {filteredAndSortedPaths.length} paths (after filters).
          </p>
        )}
      </div>

      {/* Path drilldown drawer */}
      {selectedPath && (
        <div
          style={{
            position: 'fixed',
            top: 88,
            right: 24,
            bottom: 24,
            width: 360,
            maxWidth: '90vw',
            background: t.color.surface,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.borderLight}`,
            boxShadow: t.shadowLg,
            padding: t.space.lg,
            zIndex: 40,
            display: 'flex',
            flexDirection: 'column',
            gap: t.space.md,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm }}>
            <h3
              style={{
                margin: 0,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Path details
            </h3>
            <button
              type="button"
              onClick={() => {
                setSelectedPath(null)
                setSelectedPathDetails(null)
                setSelectedPathError(null)
              }}
              style={{
                border: 'none',
                background: 'transparent',
                color: t.color.textSecondary,
                cursor: 'pointer',
                fontSize: t.font.sizeSm,
              }}
            >
              Close
            </button>
          </div>
          <div
            style={{
              fontSize: t.font.sizeXs,
              color: t.color.textSecondary,
              maxHeight: 48,
              overflow: 'hidden',
            }}
          >
            {selectedPath.split(' > ').map((step, i, arr) => (
              <span key={i}>
                <span
                  style={{
                    display: 'inline-block',
                    padding: '1px 6px',
                    backgroundColor: t.color.accentMuted,
                    color: t.color.accent,
                    borderRadius: t.radius.sm,
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                  }}
                >
                  {step}
                </span>
                {i < arr.length - 1 && <span style={{ margin: '0 3px', color: t.color.textMuted }}>→</span>}
              </span>
            ))}
          </div>
          {selectedPathLoading && (
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading path details…</p>
          )}
          {selectedPathError && (
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.danger }}>{selectedPathError}</p>
          )}
          {selectedPathDetails && (
            <div
              style={{
                flex: 1,
                minHeight: 0,
                display: 'flex',
                flexDirection: 'column',
                gap: t.space.md,
                overflowY: 'auto',
              }}
            >
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                {buildJourneyPathsHref(selectedJourneyId) ? (
                  <a
                    href={buildJourneyPathsHref(selectedJourneyId) || '#'}
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      border: `1px solid ${t.color.accent}`,
                      color: t.color.accent,
                      textDecoration: 'none',
                      borderRadius: t.radius.sm,
                      padding: '6px 10px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                    }}
                  >
                    Open in Journey Lab
                  </a>
                ) : null}
                {buildJourneyHypothesisSeedHref({
                  journeyDefinitionId: selectedJourneyId,
                  title: `Lag reduction test: ${selectedPath}`,
                  hypothesisText:
                    'This path family converts more slowly than the visible path set. Test a faster follow-up action or shorter-delay intervention.',
                  path: selectedPath,
                  supportCount: selectedPathDetails?.summary.count ?? null,
                  channelGroup: filters.channel !== 'all' ? filters.channel : null,
                  campaignId: filters.campaign !== 'all' ? filters.campaign : null,
                  device: filters.device !== 'all' ? filters.device : null,
                  country: filters.geo !== 'all' ? filters.geo : null,
                }) ? (
                  <a
                    href={
                      buildJourneyHypothesisSeedHref({
                        journeyDefinitionId: selectedJourneyId,
                        title: `Lag reduction test: ${selectedPath}`,
                        hypothesisText:
                          'This path family converts more slowly than the visible path set. Test a faster follow-up action or shorter-delay intervention.',
                        path: selectedPath,
                        supportCount: selectedPathDetails?.summary.count ?? null,
                        baselineRate: selectedPathDetails?.summary.share ?? null,
                        channelGroup: filters.channel !== 'all' ? filters.channel : null,
                        campaignId: filters.campaign !== 'all' ? filters.campaign : null,
                        device: filters.device !== 'all' ? filters.device : null,
                        country: filters.geo !== 'all' ? filters.geo : null,
                      }) || '#'
                    }
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      border: `1px solid ${t.color.border}`,
                      color: t.color.text,
                      textDecoration: 'none',
                      borderRadius: t.radius.sm,
                      padding: '6px 10px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                    }}
                  >
                    Draft hypothesis
                  </a>
                ) : null}
              </div>
              {/* Summary */}
              <div>
                <h4
                  style={{
                    margin: '0 0 4px',
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                  }}
                >
                  Summary
                </h4>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.text }}>
                  <div>
                    <strong>{selectedPathDetails.summary.count}</strong>{' '}
                    <span style={{ color: t.color.textSecondary }}>journeys</span>
                  </div>
                  <div>
                    <strong>{(selectedPathDetails.summary.share * 100).toFixed(2)}%</strong>{' '}
                    <span style={{ color: t.color.textSecondary }}>of journeys in this view</span>
                  </div>
                  <div>
                    <strong>{selectedPathDetails.summary.avg_touchpoints.toFixed(1)}</strong>{' '}
                    <span style={{ color: t.color.textSecondary }}>avg touchpoints</span>
                  </div>
                  <div>
                    <strong>
                      {selectedPathDetails.summary.avg_time_to_convert_days != null
                        ? `${selectedPathDetails.summary.avg_time_to_convert_days.toFixed(1)}d`
                        : 'N/A'}
                    </strong>{' '}
                    <span style={{ color: t.color.textSecondary }}>avg time to convert</span>
                  </div>
                </div>
              </div>

              <div>
                <h4
                  style={{
                    margin: '0 0 4px',
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                  }}
                >
                  Lag diagnosis
                </h4>
                <div
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    padding: t.space.sm,
                    background: t.color.bg,
                    display: 'grid',
                    gap: t.space.sm,
                  }}
                >
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: t.space.sm }}>
                    <div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Visible path median</div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                        {visiblePathLagMedian != null ? `${visiblePathLagMedian.toFixed(1)}d` : '—'}
                      </div>
                    </div>
                    <div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Selected path avg</div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                        {selectedPathDetails.summary.avg_time_to_convert_days != null
                          ? `${selectedPathDetails.summary.avg_time_to_convert_days.toFixed(1)}d`
                          : '—'}
                      </div>
                    </div>
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {selectedPathLagNote || 'There is not enough comparable timing data in this view to classify the selected path family.'}
                  </div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                    Longer-lag path families are usually the first to move when attribution windows are tightened.
                  </div>
                </div>
              </div>

              {selectedPathFlowData ? (
                <div>
                  <h4
                    style={{
                      margin: '0 0 6px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      textTransform: 'uppercase',
                      letterSpacing: '0.04em',
                    }}
                  >
                    Path family flow
                  </h4>
                  <div
                    style={{
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      padding: t.space.sm,
                      background: t.color.bg,
                      display: 'grid',
                      gap: t.space.sm,
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Sankey view of this selected path plus {selectedPathFlowData.variants.toLocaleString()} similar variants, covering{' '}
                      <strong style={{ color: t.color.text }}>{selectedPathFlowData.totalJourneys.toLocaleString()}</strong> journeys.
                    </div>
                    <div style={{ width: '100%', height: 260, minWidth: 0 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <Sankey
                          data={{ nodes: selectedPathFlowData.nodes, links: selectedPathFlowData.links }}
                          nodePadding={22}
                          nodeWidth={12}
                          iterations={28}
                          margin={{ top: 8, right: 16, bottom: 8, left: 16 }}
                          link={{ stroke: t.color.accent, strokeOpacity: 0.28 }}
                          node={{ stroke: t.color.borderLight, fill: t.color.accent }}
                        >
                          <Tooltip content={<SankeyFlowTooltip />} />
                        </Sankey>
                      </ResponsiveContainer>
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Use this to spot dominant splits and where similar path variants diverge before conversion.
                    </div>
                  </div>
                </div>
              ) : null}

              {/* Step breakdown */}
              {selectedPathDetails.step_breakdown?.length > 0 && (
                <div>
                  <h4
                    style={{
                      margin: '0 0 4px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      textTransform: 'uppercase',
                      letterSpacing: '0.04em',
                    }}
                  >
                    Step breakdown
                  </h4>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeXs }}>
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                        <th style={{ textAlign: 'left', padding: '2px 4px', color: t.color.textSecondary }}>Pos</th>
                        <th style={{ textAlign: 'left', padding: '2px 4px', color: t.color.textSecondary }}>Step</th>
                        <th style={{ textAlign: 'right', padding: '2px 4px', color: t.color.textSecondary }}>Drop‑off</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedPathDetails.step_breakdown.map((s) => (
                        <tr key={s.position} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                          <td style={{ padding: '2px 4px', color: t.color.textSecondary }}>{s.position}</td>
                          <td style={{ padding: '2px 4px', color: t.color.text }}>{s.step}</td>
                          <td
                            style={{
                              padding: '2px 4px',
                              textAlign: 'right',
                              fontVariantNumeric: 'tabular-nums',
                              color: t.color.textSecondary,
                            }}
                          >
                            {(s.dropoff_share * 100).toFixed(1)}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Variants */}
              {selectedPathDetails.variants?.length > 0 && (
                <div>
                  <h4
                    style={{
                      margin: '0 0 4px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      textTransform: 'uppercase',
                      letterSpacing: '0.04em',
                    }}
                  >
                    Similar variants
                  </h4>
                  <ul style={{ margin: 0, paddingLeft: t.space.lg, listStyle: 'disc', fontSize: t.font.sizeXs }}>
                    {selectedPathDetails.variants.map((v) => (
                      <li key={v.path} style={{ marginBottom: 2 }}>
                        <span style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>{v.path}</span>{' '}
                        <span style={{ color: t.color.textSecondary }}>
                          · {v.count} journeys ({(v.share * 100).toFixed(1)}%)
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Data health */}
              {selectedPathDetails.data_health && (
                <div>
                  <h4
                    style={{
                      margin: '0 0 4px',
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      textTransform: 'uppercase',
                      letterSpacing: '0.04em',
                    }}
                  >
                    Data health
                  </h4>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.text }}>
                    <div>
                      <strong>
                        {(selectedPathDetails.data_health.direct_unknown_touch_share * 100).toFixed(1)}%
                      </strong>{' '}
                      <span style={{ color: t.color.textSecondary }}>of touches are Direct/Unknown on this path</span>
                    </div>
                    <div>
                      <strong>
                        {(selectedPathDetails.data_health.journeys_ending_direct_share * 100).toFixed(1)}%
                      </strong>{' '}
                      <span style={{ color: t.color.textSecondary }}>of journeys end on Direct</span>
                    </div>
                    <div style={{ marginTop: 4 }}>
                      <ConfidenceBadge confidence={selectedPathDetails.data_health.confidence} compact />
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
      <SaveLocalSegmentDialog
        open={showSaveSegmentModal}
        initialName={defaultFilterSegmentName}
        criteriaLabel={Object.entries(currentFilterSegmentDefinition)
          .map(([key, value]) => `${key}=${String(value)}`)
          .join(' · ')}
        error={saveSegmentError}
        saving={saveSegmentMutation.isPending}
        onClose={() => {
          setShowSaveSegmentModal(false)
          setSaveSegmentError(null)
        }}
        onSubmit={(payload) => saveSegmentMutation.mutate(payload)}
      />
      </div>
    </DashboardPage>
  )
}
