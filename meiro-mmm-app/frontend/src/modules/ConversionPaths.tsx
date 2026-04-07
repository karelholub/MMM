import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Sankey } from 'recharts'
import { tokens } from '../theme/tokens'
import DashboardPage from '../components/dashboard/DashboardPage'
import SectionCard from '../components/dashboard/SectionCard'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import ContextSummaryStrip from '../components/dashboard/ContextSummaryStrip'
import GlobalFilterBar, { type GlobalFiltersState } from '../components/dashboard/GlobalFilterBar'
import { AnalyticsTable, AnalyticsToolbar, type AnalyticsTableColumn } from '../components/dashboard'
import DecisionStatusCard from '../components/DecisionStatusCard'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import ConfidenceBadge, { type Confidence } from '../components/ConfidenceBadge'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import { apiGetJson } from '../lib/apiClient'
import { buildListQuery, type PaginatedResponse } from '../lib/apiSchemas'
import { defaultRecentDateRange } from '../lib/dateRange'
import { buildJourneyHypothesisHref } from '../lib/journeyLinks'
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
  const { journeysSummary: journeys } = useWorkspaceContext()
  const [filters, setFilters] = useState<GlobalFiltersState>(() => buildInitialFilters(journeys?.date_min, journeys?.date_max))
  const [selectedJourneyId, setSelectedJourneyId] = useState('')
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

  const journeyFilterOptions = useMemo(
    () => ({
      channels: (dimensionsQuery.data?.channels ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      campaigns: (dimensionsQuery.data?.campaigns ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      devices: (dimensionsQuery.data?.devices ?? []).map((item) => ({ value: item.value, label: dimensionLabel(item.value, item.count) })),
      geos: (dimensionsQuery.data?.countries ?? []).map((item) => ({ value: item.value, label: dimensionLabel(String(item.value).toUpperCase(), item.count) })),
    }),
    [dimensionsQuery.data],
  )

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

  const anomaliesQuery = useQuery<{ anomalies: PathAnomaly[] }>({
    queryKey: ['path-anomalies'],
    queryFn: async () => apiGetJson<{ anomalies: PathAnomaly[] }>('/api/paths/anomalies', {
      fallbackMessage: 'Failed to fetch path anomalies',
    }),
  })

  useEffect(() => {
    if (journeys?.date_min && journeys?.date_max) {
      setFilters((prev) => ({
        ...prev,
        dateFrom: journeys.date_min?.slice(0, 10) ?? prev.dateFrom,
        dateTo: journeys.date_max?.slice(0, 10) ?? prev.dateTo,
      }))
    }
  }, [journeys?.date_min, journeys?.date_max])

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
              Path analysis uses observed workspace values for the selected journey definition and date window.
            </div>
          </div>
          <GlobalFilterBar
            value={filters}
            onChange={setFilters}
            channels={journeyFilterOptions.channels}
            campaigns={journeyFilterOptions.campaigns}
            devices={journeyFilterOptions.devices}
            geos={journeyFilterOptions.geos}
            showSegment={false}
          />
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
      </div>
    </DashboardPage>
  )
}
