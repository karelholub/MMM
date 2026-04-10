import { useEffect, useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import ContextSummaryStrip from '../components/dashboard/ContextSummaryStrip'
import AnalysisShareActions from '../components/dashboard/AnalysisShareActions'
import AnalysisNarrativePanel from '../components/dashboard/AnalysisNarrativePanel'
import SurfaceBasisNotice from '../components/dashboard/SurfaceBasisNotice'
import DecisionStatusCard from '../components/DecisionStatusCard'
import { type LagInsightsResponse } from '../components/performance/LagInsightsPanel'
import SegmentComparisonContextNote from '../components/segments/SegmentComparisonContextNote'
import SegmentOverlapNotice from '../components/segments/SegmentOverlapNotice'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import { buildIncrementalityPlannerHref } from '../lib/experimentLinks'
import { buildSettingsHref } from '../lib/settingsLinks'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import {
  buildSegmentComparisonHref,
  isLocalAnalyticalSegment,
  localSegmentCompatibleWithDimensions,
  readLocalSegmentDefinition,
  segmentOptionLabel,
  type SegmentAnalysisResponse,
  type SegmentComparisonResponse,
  type SegmentRegistryResponse,
} from '../lib/segments'

interface AttributionComparisonProps {
  selectedModel: string
  onSelectModel: (model: string) => void
}

interface JourneyReadiness {
  status: string
  blockers: string[]
  warnings: string[]
  summary: {
    primary_kpi_coverage: number
    taxonomy_unknown_share: number
  }
  details?: {
    latest_event_replay?: {
      diagnostics?: {
        events_loaded?: number
        profiles_reconstructed?: number
        touchpoints_reconstructed?: number
        conversions_reconstructed?: number
        attributable_profiles?: number
        journeys_persisted?: number
        warnings?: string[]
      }
    } | null
  }
}

type Reliability = 'ok' | 'warning' | 'unreliable'

interface MarkovDiagnostics {
  journeys_used?: number
  converted_journeys?: number
  unique_states?: number
  unique_transitions?: number
  top_transitions?: { from: string; to: string; count: number; share?: number }[]
  warnings?: string[]
  reliability?: Reliability
  insufficient_data?: boolean
  what_to_do_next?: string[]
}

interface ModelResult {
  model: string
  channel_credit: Record<string, number>
  total_conversions: number
  total_value: number
  gross_total_conversions?: number
  net_total_conversions?: number
  gross_total_value?: number
  net_total_value?: number
  refunded_value?: number
  cancelled_value?: number
  invalid_leads?: number
  value_mode?: string
  interaction_summary?: {
    click_through_conversions?: number
    view_through_conversions?: number
    mixed_path_conversions?: number
  }
  channels: { channel: string; attributed_value: number; attributed_share: number; attributed_conversions: number }[]
  error?: string
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
    eligible_touchpoints?: {
      include_channels?: string[] | null
      exclude_channels?: string[] | null
      include_event_types?: string[] | null
      exclude_event_types?: string[] | null
    }
  } | null
  diagnostics?: MarkovDiagnostics
}

interface AttributionSettingsDraft {
  lookback_window_days: number
  use_converted_flag: boolean
  conversion_value_mode: string
  min_journey_quality_score: number
  min_conversion_value: number
  time_decay_half_life_days: number
  position_first_pct: number
  position_last_pct: number
  markov_min_paths: number
}

interface AttributionPreviewResult {
  previewAvailable: boolean
  totalJourneys: number
  eligibleJourneys: number
  windowImpactCount: number
  windowDirection: string
  qualityImpactCount: number
  qualityDirection: string
  useConvertedFlagImpact: number
  useConvertedFlagDirection: string
  reason?: string | null
}

interface SensitivityScenario {
  id: string
  label: string
  note: string
  settings: AttributionSettingsDraft
  preview: AttributionPreviewResult
}

interface SensitivityWorkspaceData {
  current: AttributionPreviewResult
  scenarios: SensitivityScenario[]
}

interface SavedSensitivityScenario {
  id: string
  label: string
  saved_at: string
  settings: AttributionSettingsDraft
}

const MODEL_LABELS: Record<string, string> = {
  last_touch: 'Last Touch',
  first_touch: 'First Touch',
  linear: 'Linear',
  time_decay: 'Time Decay',
  position_based: 'Position Based',
  markov: 'Data-Driven (Markov)',
}

const MODEL_COLORS: Record<string, string> = {
  last_touch: '#dc2626',
  first_touch: '#d97706',
  linear: '#3b82f6',
  time_decay: '#059669',
  position_based: '#7c3aed',
  markov: '#0ea5e9',
}

function formatCurrency(v: number): string {
  if (v >= 1000000) return `$${(v / 1000000).toFixed(1)}M`
  if (v >= 1000) return `$${(v / 1000).toFixed(0)}K`
  return `$${v.toFixed(0)}`
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

function exportComparisonCSV(
  comparisonData: Record<string, unknown>[],
  models: string[],
  opts: {
    mode: 'absolute' | 'delta'
    baselineModel?: string | null
    conversionKey?: string | null
    configVersion?: number | null
    directMode: 'include' | 'exclude_view'
  },
) {
  const headers = [
    'Channel',
    ...models.map((m) => `${MODEL_LABELS[m] || m} (%)`),
    'Mode',
    'Baseline model',
    'Conversion key',
    'Config version',
    'Direct handling',
  ]
  const rows = comparisonData.map((row) => [
    row.channel,
    ...models.map((m) => ((row[`${m}_share`] as number) || 0) * 100).map((v) => v.toFixed(1)),
    opts.mode,
    opts.mode === 'delta' && opts.baselineModel ? MODEL_LABELS[opts.baselineModel] || opts.baselineModel : '',
    opts.conversionKey || '',
    opts.configVersion != null ? String(opts.configVersion) : '',
    opts.directMode,
  ])
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `attribution-comparison-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function summarizeSensitivityRisk(preview?: AttributionPreviewResult | null): string {
  if (!preview || !preview.previewAvailable) return 'Draft not previewed'
  const totalImpact =
    Number(preview.windowImpactCount || 0) +
    Number(preview.qualityImpactCount || 0) +
    Number(preview.useConvertedFlagImpact || 0)
  if (totalImpact === 0) return 'Low eligibility change'
  if (totalImpact < 50) return `${totalImpact.toLocaleString()} journeys affected`
  return `High sensitivity · ${totalImpact.toLocaleString()} journeys affected`
}

export default function AttributionComparison({ selectedModel, onSelectModel }: AttributionComparisonProps) {
  const { journeysSummary, globalDateFrom, globalDateTo, selectedConfigId } = useWorkspaceContext()
  const [sortBy, setSortBy] = useState<'channel' | string>('channel')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [comparisonMode, setComparisonMode] = useState<'absolute' | 'delta'>('absolute')
  const [baselineModel, setBaselineModel] = useState<string>('linear')
  const [showDeltaRow, setShowDeltaRow] = useState(false)
  const [directMode, setDirectMode] = useState<'include' | 'exclude_view'>('include')
  const [showMarkovDiagnostics, setShowMarkovDiagnostics] = useState(false)
  const [showContextPanel, setShowContextPanel] = usePersistentToggle('attribution-comparison:show-context', false)
  const [showReplayPanel, setShowReplayPanel] = usePersistentToggle('attribution-comparison:show-replay', false)
  const [showSensitivityPanel, setShowSensitivityPanel] = usePersistentToggle('attribution-comparison:show-sensitivity', false)
  const [selectedSegmentId, setSelectedSegmentId] = useState('')
  const [compareSegmentId, setCompareSegmentId] = useState('')
  const [sensitivityDraft, setSensitivityDraft] = useState<AttributionSettingsDraft | null>(null)
  const [sharedScenarioSummary, setSharedScenarioSummary] = useState<string | null>(null)
  const [savedSensitivityScenarios, setSavedSensitivityScenarios] = useState<SavedSensitivityScenario[]>(() => {
    if (typeof window === 'undefined') return []
    try {
      const raw = window.localStorage.getItem('attribution-comparison:saved-scenarios')
      const parsed = raw ? JSON.parse(raw) : []
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  })

  const settingsQuery = useQuery<{ attribution: AttributionSettingsDraft }>({
    queryKey: ['attribution-comparison-settings'],
    queryFn: async () => apiGetJson<{ attribution: AttributionSettingsDraft }>('/api/settings', {
      fallbackMessage: 'Failed to load attribution settings',
    }),
    refetchInterval: false,
  })

  useEffect(() => {
    if (!sensitivityDraft && settingsQuery.data?.attribution) {
      setSensitivityDraft(settingsQuery.data.attribution)
    }
  }, [settingsQuery.data, sensitivityDraft])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    const segment = params.get('segment')
    if (segment) setSelectedSegmentId(segment)
    const compareSegment = params.get('compare_segment')
    if (compareSegment) setCompareSegmentId(compareSegment)
  }, [])

  useEffect(() => {
    if (!settingsQuery.data?.attribution) return
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    const lookback = params.get('sens_lookback')
    const quality = params.get('sens_quality')
    const converted = params.get('sens_converted')
    if (lookback == null && quality == null && converted == null) return

    setShowSensitivityPanel(true)
    setSensitivityDraft((prev) => {
      const base = prev ?? settingsQuery.data.attribution
      return {
        ...base,
        ...(lookback != null ? { lookback_window_days: Math.max(1, Number(lookback) || base.lookback_window_days) } : {}),
        ...(quality != null ? { min_journey_quality_score: Math.max(0, Math.min(100, Number(quality) || base.min_journey_quality_score)) } : {}),
        ...(converted != null ? { use_converted_flag: converted === '1' || converted === 'true' } : {}),
      }
    })
    const summaryParts = [
      lookback != null ? `lookback ${Math.max(1, Number(lookback) || 0)}d` : null,
      quality != null ? `quality ≥${Math.max(0, Math.min(100, Number(quality) || 0))}` : null,
      converted != null ? ((converted === '1' || converted === 'true') ? 'converted journeys only' : 'all journeys') : null,
    ].filter((value): value is string => Boolean(value))
    setSharedScenarioSummary(summaryParts.length ? `Shared scenario loaded: ${summaryParts.join(' · ')}.` : 'Shared scenario loaded.')
  }, [settingsQuery.data, setShowSensitivityPanel])

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      window.localStorage.setItem('attribution-comparison:saved-scenarios', JSON.stringify(savedSensitivityScenarios))
    } catch {
      // ignore persistence failures
    }
  }, [savedSensitivityScenarios])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    if (selectedSegmentId) params.set('segment', selectedSegmentId)
    else params.delete('segment')
    if (compareSegmentId) params.set('compare_segment', compareSegmentId)
    else params.delete('compare_segment')
    const next = `${window.location.pathname}?${params.toString()}${window.location.hash || ''}`
    window.history.replaceState({}, '', next)
  }, [compareSegmentId, selectedSegmentId])

  const sensitivityQuery = useQuery<SensitivityWorkspaceData>({
    queryKey: ['attribution-sensitivity-preview', sensitivityDraft],
    queryFn: async () => {
      if (!sensitivityDraft) {
        throw new Error('Attribution settings are unavailable')
      }
      const previewFor = async (settings: AttributionSettingsDraft) =>
        apiSendJson<AttributionPreviewResult>('/api/attribution/preview', 'POST', { settings }, {
          fallbackMessage: 'Failed to calculate attribution sensitivity preview',
        })

      const current = await previewFor(sensitivityDraft)
      const scenarioDefinitions = [
        {
          id: 'tighter_window',
          label: 'Tighter window',
          note: 'Reduce click lookback by 7 days.',
          settings: { ...sensitivityDraft, lookback_window_days: Math.max(1, sensitivityDraft.lookback_window_days - 7) },
        },
        {
          id: 'looser_window',
          label: 'Looser window',
          note: 'Extend click lookback by 7 days.',
          settings: { ...sensitivityDraft, lookback_window_days: sensitivityDraft.lookback_window_days + 7 },
        },
        {
          id: 'stricter_quality',
          label: 'Stricter quality floor',
          note: 'Raise minimum journey quality by 20 points.',
          settings: { ...sensitivityDraft, min_journey_quality_score: Math.min(100, sensitivityDraft.min_journey_quality_score + 20) },
        },
        {
          id: 'toggle_converted',
          label: sensitivityDraft.use_converted_flag ? 'Include non-converted journeys' : 'Restrict to converted journeys',
          note: sensitivityDraft.use_converted_flag ? 'Preview impact of removing the converted-only filter.' : 'Preview impact of requiring converted journeys.',
          settings: { ...sensitivityDraft, use_converted_flag: !sensitivityDraft.use_converted_flag },
        },
      ]

      const seen = new Set<string>([JSON.stringify(sensitivityDraft)])
      const scenarios: SensitivityScenario[] = []
      for (const definition of scenarioDefinitions) {
        const signature = JSON.stringify(definition.settings)
        if (seen.has(signature)) continue
        seen.add(signature)
        scenarios.push({
          id: definition.id,
          label: definition.label,
          note: definition.note,
          settings: definition.settings,
          preview: await previewFor(definition.settings),
        })
      }
      return { current, scenarios }
    },
    enabled: !!sensitivityDraft,
    refetchInterval: false,
  })
  const comparisonDateFrom = globalDateFrom || journeysSummary?.date_min?.slice(0, 10) || ''
  const comparisonDateTo = globalDateTo || journeysSummary?.date_max?.slice(0, 10) || ''

  const channelLagQuery = useQuery<LagInsightsResponse>({
    queryKey: ['attribution-comparison-channel-lag', comparisonDateFrom || 'none', comparisonDateTo || 'none'],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (comparisonDateFrom) params.set('date_from', comparisonDateFrom)
      if (comparisonDateTo) params.set('date_to', comparisonDateTo)
      return apiGetJson<LagInsightsResponse>(`/api/performance/channel/lag?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel lag analysis',
      })
    },
    enabled: Boolean(comparisonDateFrom && comparisonDateTo),
    refetchInterval: false,
  })
  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['segment-registry', 'attribution-comparison'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load segment registry',
      }),
    refetchInterval: false,
  })

  const localSegments = useMemo(
    () => (segmentRegistryQuery.data?.items ?? []).filter(isLocalAnalyticalSegment),
    [segmentRegistryQuery.data?.items],
  )
  const compatibleSegments = useMemo(
    () => localSegments.filter((item) => localSegmentCompatibleWithDimensions(item, ['channel_group'])),
    [localSegments],
  )
  const selectedSegment = useMemo(
    () => localSegments.find((item) => item.id === selectedSegmentId) ?? null,
    [localSegments, selectedSegmentId],
  )
  const selectedSegmentDefinition = useMemo(
    () => readLocalSegmentDefinition(selectedSegment),
    [selectedSegment],
  )
  const selectedSegmentAutoCompatible = useMemo(
    () => localSegmentCompatibleWithDimensions(selectedSegment, ['channel_group']),
    [selectedSegment],
  )
  const compareSegment = useMemo(
    () => localSegments.find((item) => item.id === compareSegmentId) ?? null,
    [localSegments, compareSegmentId],
  )
  const segmentAnalysisQuery = useQuery<SegmentAnalysisResponse>({
    queryKey: ['attribution-comparison', 'segment-analysis', selectedSegment?.id || 'none', comparisonDateFrom || 'none', comparisonDateTo || 'none'],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (comparisonDateFrom) params.set('date_from', comparisonDateFrom)
      if (comparisonDateTo) params.set('date_to', comparisonDateTo)
      return apiGetJson<SegmentAnalysisResponse>(`/api/segments/local/${selectedSegment?.id}/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to load segment audience analysis',
      })
    },
    enabled: Boolean(selectedSegment && comparisonDateFrom && comparisonDateTo),
    refetchInterval: false,
  })
  const segmentCompareQuery = useQuery<SegmentComparisonResponse>({
    queryKey: ['attribution-comparison', 'segment-compare', selectedSegment?.id || 'none', compareSegment?.id || 'none'],
    queryFn: async () =>
      apiGetJson<SegmentComparisonResponse>(`/api/segments/local/${selectedSegment?.id}/compare?other_segment_id=${encodeURIComponent(compareSegment?.id || '')}`, {
        fallbackMessage: 'Failed to compare saved analytical audiences',
      }),
    enabled: Boolean(selectedSegment?.id && compareSegment?.id && selectedSegment?.id !== compareSegment?.id),
    refetchInterval: false,
  })

  useEffect(() => {
    if (!selectedSegmentId) return
    if (!segmentRegistryQuery.data) return
    if (localSegments.some((item) => item.id === selectedSegmentId)) return
    setSelectedSegmentId('')
  }, [localSegments, selectedSegmentId, segmentRegistryQuery.data])
  useEffect(() => {
    if (!compareSegmentId) return
    if (localSegments.some((item) => item.id === compareSegmentId && item.id !== selectedSegmentId)) return
    setCompareSegmentId('')
  }, [compareSegmentId, localSegments, selectedSegmentId])

  const currentSensitivitySummary = useMemo(() => {
    if (!sensitivityDraft) return 'No draft loaded'
    return [
      `Lookback ${sensitivityDraft.lookback_window_days}d`,
      `Quality ≥${sensitivityDraft.min_journey_quality_score}`,
      sensitivityDraft.use_converted_flag ? 'Converted only' : 'All journeys',
    ].join(' · ')
  }, [sensitivityDraft])
  const topExposedChannels = useMemo(
    () =>
      [...(channelLagQuery.data?.items ?? [])]
        .filter((item) => {
          if (selectedSegmentAutoCompatible && selectedSegmentDefinition.channel_group && item.key !== selectedSegmentDefinition.channel_group) return false
          return true
        })
        .sort((a, b) => {
          const aShare = a.conversions > 0 ? a.lag_buckets.over_7d / a.conversions : 0
          const bShare = b.conversions > 0 ? b.lag_buckets.over_7d / b.conversions : 0
          return bShare - aShare
        })
        .slice(0, 4),
    [channelLagQuery.data?.items, selectedSegmentAutoCompatible, selectedSegmentDefinition.channel_group],
  )

  const buildSensitivityHref = (settings: AttributionSettingsDraft | null): string => {
    const params = new URLSearchParams(window.location.search)
    params.set('sens_lookback', String(settings?.lookback_window_days ?? settingsQuery.data?.attribution.lookback_window_days ?? 30))
    params.set('sens_quality', String(settings?.min_journey_quality_score ?? settingsQuery.data?.attribution.min_journey_quality_score ?? 0))
    params.set('sens_converted', settings?.use_converted_flag ? '1' : '0')
    params.set('page', 'analytics_attribution')
    if (selectedSegmentId) params.set('segment', selectedSegmentId)
    else params.delete('segment')
    return `/?${params.toString()}`
  }

  const resultsQuery = useQuery<Record<string, ModelResult>>({
    queryKey: ['attribution-results'],
    queryFn: async () => apiGetJson<Record<string, ModelResult>>('/api/attribution/results', {
      fallbackMessage: 'Failed to fetch results',
    }),
    refetchInterval: 3000,
  })

  const results = resultsQuery.data || {}
  const models = Object.keys(results).filter((k) => !results[k].error)
  const t = tokens

  const anyResult: ModelResult | undefined = models.length ? results[models[0]] : undefined
  const configMeta = anyResult?.config ?? null
  const resultsConfigId = configMeta?.config_id ?? null
  const configMismatch =
    Boolean(selectedConfigId) &&
    (!resultsConfigId || resultsConfigId !== selectedConfigId)
  const conversionKey = configMeta?.conversion_key ?? journeysSummary?.primary_kpi_id ?? null
  const configVersion = configMeta?.config_version ?? null
  const readiness = journeysSummary?.readiness ?? null
  const latestEventReplayDiagnostics = readiness?.details?.latest_event_replay?.diagnostics
  const periodLabel =
    comparisonDateFrom && comparisonDateTo
      ? `${new Date(comparisonDateFrom).toLocaleDateString()} – ${new Date(comparisonDateTo).toLocaleDateString()}`
      : 'Current dataset (range not configured)'
  const freshnessLabel =
    journeysSummary?.data_freshness_hours != null
      ? `${Math.round(Number(journeysSummary.data_freshness_hours || 0))}h lag`
      : 'Freshness unavailable'
  const coverageLabel =
    readiness?.summary?.primary_kpi_coverage != null
      ? `${(Number(readiness.summary.primary_kpi_coverage) * 100).toFixed(1)}% KPI coverage`
      : 'Coverage unavailable'

  const baselineKey = useMemo(() => {
    if (models.includes(baselineModel)) return baselineModel
    if (models.includes('linear')) return 'linear'
    return models[0] || ''
  }, [baselineModel, models])

  const allChannels = new Set<string>()
  for (const model of models) {
    const r = results[model]
    if (r?.channels) {
      for (const ch of r.channels) {
        if (directMode === 'exclude_view' && ch.channel.toLowerCase() === 'direct') continue
        if (selectedSegmentAutoCompatible && selectedSegmentDefinition.channel_group && ch.channel !== selectedSegmentDefinition.channel_group) continue
        allChannels.add(ch.channel)
      }
    }
  }
  const comparisonData = Array.from(allChannels).map((channel) => {
    const row: Record<string, unknown> = { channel }
    for (const model of models) {
      const r = results[model]
      const ch = r?.channels?.find((c: { channel: string }) => c.channel === channel)
      row[model] = ch ? ch.attributed_value : 0
      row[`${model}_share`] = ch ? ch.attributed_share : 0
    }
    return row
  })

  const deltaData = useMemo(() => {
    if (!baselineKey || comparisonMode === 'absolute') return comparisonData
    return comparisonData.map((row) => {
      const baseVal = Number(row[baselineKey] ?? 0)
      const next: Record<string, unknown> = { channel: row.channel }
      for (const m of models) {
        const v = Number(row[m] ?? 0)
        next[m] = v - baseVal
        next[`${m}_share`] = row[`${m}_share`]
      }
      return next
    })
  }, [baselineKey, comparisonMode, comparisonData, models])

  const chartData = comparisonMode === 'absolute' ? comparisonData : deltaData

  const sortedData = useMemo(() => {
    return [...comparisonMode === 'absolute' ? comparisonData : deltaData].sort((a, b) => {
      let va: number | string = a[sortBy] as number | string
      let vb: number | string = b[sortBy] as number | string
      if (sortBy === 'channel') {
        va = String(va)
        vb = String(vb)
        return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va)
      }
      va = Number(va ?? 0)
      vb = Number(vb ?? 0)
      return sortDir === 'asc' ? va - vb : vb - va
    })
  }, [comparisonData, deltaData, comparisonMode, sortBy, sortDir])

  const selectedResult = results[selectedModel]

  const markovResult: ModelResult | undefined = results['markov']
  const markovDiagnostics: MarkovDiagnostics | undefined = markovResult?.diagnostics

  const winnersLosers = useMemo(() => {
    if (comparisonMode !== 'delta' || !baselineKey || !models.length || !comparisonData.length) {
      return { winners: [] as { channel: string; delta: number }[], losers: [] as { channel: string; delta: number }[] }
    }
    if (!models.includes(selectedModel)) {
      return { winners: [], losers: [] }
    }
    const winners: { channel: string; delta: number }[] = []
    const losers: { channel: string; delta: number }[] = []
    for (const row of comparisonData) {
      const ch = String(row.channel)
      const baseVal = Number(row[baselineKey] ?? 0)
      const val = Number(row[selectedModel] ?? 0)
      const delta = val - baseVal
      if (delta > 0) winners.push({ channel: ch, delta })
      else if (delta < 0) losers.push({ channel: ch, delta })
    }
    winners.sort((a, b) => b.delta - a.delta)
    losers.sort((a, b) => a.delta - b.delta)
    return {
      winners: winners.slice(0, 3),
      losers: losers.slice(0, 3),
    }
  }, [baselineKey, comparisonData, comparisonMode, models, selectedModel])

  const focusedSegmentModelComparison = useMemo(() => {
    if (!selectedSegmentAutoCompatible || !selectedSegmentDefinition.channel_group || !models.length) return []
    return models.map((model) => {
      const channels = results[model]?.channels ?? []
      const totalValue = channels.reduce((sum: number, row: { attributed_value?: number }) => sum + Number(row.attributed_value || 0), 0)
      const focusedRow = channels.find((row: { channel: string }) => row.channel === selectedSegmentDefinition.channel_group)
      const focusedValue = Number(focusedRow?.attributed_value || 0)
      const focusedShare = totalValue > 0 ? focusedValue / totalValue : 0
      const baselineChannels = results[baselineKey]?.channels ?? []
      const baselineTotal = baselineChannels.reduce((sum: number, row: { attributed_value?: number }) => sum + Number(row.attributed_value || 0), 0)
      const baselineFocusedRow = baselineChannels.find((row: { channel: string }) => row.channel === selectedSegmentDefinition.channel_group)
      const baselineFocusedValue = Number(baselineFocusedRow?.attributed_value || 0)
      const baselineShare = baselineTotal > 0 ? baselineFocusedValue / baselineTotal : 0
      return {
        model,
        focusedValue,
        focusedShare,
        baselineShare,
        shareDelta: focusedShare - baselineShare,
      }
    })
  }, [baselineKey, models, results, selectedSegmentAutoCompatible, selectedSegmentDefinition.channel_group])
  const comparisonNarrative = useMemo(() => {
    const winner = winnersLosers.winners[0] ?? null
    const loser = winnersLosers.losers[0] ?? null
    const topLagRisk = topExposedChannels[0] ?? null
    const selectedModelLabel = MODEL_LABELS[selectedModel] || selectedModel
    const baselineModelLabel = MODEL_LABELS[baselineKey] || baselineKey
    const headline =
      comparisonMode === 'delta' && baselineKey
        ? `${selectedModelLabel} is currently being read against ${baselineModelLabel}.`
        : `${selectedModelLabel} is currently the primary comparison view.`
    const items = [
      winner
        ? `${winner.channel} gains the most value under ${selectedModelLabel} relative to ${baselineModelLabel}, up ${formatCurrency(winner.delta)} in attributed value.`
        : null,
      loser
        ? `${loser.channel} loses the most value under ${selectedModelLabel} relative to ${baselineModelLabel}, down ${formatCurrency(Math.abs(loser.delta))}.`
        : null,
      sensitivityQuery.data?.current?.previewAvailable
        ? `Sensitivity draft impact: ${summarizeSensitivityRisk(sensitivityQuery.data.current)}.`
        : 'Sensitivity preview is unavailable, so window and quality impact are not yet quantified.',
      topLagRisk
        ? `${topLagRisk.label} has the heaviest long-lag exposure, with ${formatPercent(topLagRisk.conversions > 0 ? topLagRisk.lag_buckets.over_7d / topLagRisk.conversions : null)} of conversions taking more than 7 days.`
        : null,
      selectedSegment
        ? selectedSegmentAutoCompatible
          ? `${selectedSegment.name} is a focused analytical slice. Attribution totals stay workspace-wide, but visible channel rows are filtered to that audience context.`
          : `${selectedSegment.name} is an advanced analytical audience. Attribution totals stay workspace-wide, and this page adds an audience lens instead of pretending the model output can be exactly sliced by one channel rule.`
        : null,
      selectedSegment && compareSegment && segmentCompareQuery.data
        ? `${selectedSegment.name} vs ${compareSegment.name}: ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity. Revenue delta is ${segmentCompareQuery.data.deltas.revenue == null ? 'unavailable' : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : ''}${formatCurrency(Math.abs(segmentCompareQuery.data.deltas.revenue))}`}.`
        : null,
      selectedSegment && !selectedSegmentAutoCompatible && segmentAnalysisQuery.data?.summary
        ? `The audience currently matches ${(segmentAnalysisQuery.data.summary.journey_rows ?? 0).toLocaleString()} journey rows with ${segmentAnalysisQuery.data.summary.median_lag_days != null ? `${segmentAnalysisQuery.data.summary.median_lag_days}d` : 'unavailable'} median lag.`
        : null,
    ].filter((item): item is string => Boolean(item))
    return { headline, items }
  }, [
    baselineKey,
    comparisonMode,
    compareSegment,
    segmentCompareQuery.data,
    selectedModel,
    selectedSegment,
    selectedSegmentAutoCompatible,
    segmentAnalysisQuery.data?.summary,
    sensitivityQuery.data?.current,
    topExposedChannels,
    winnersLosers.losers,
    winnersLosers.winners,
  ])

  if (resultsQuery.isError) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.danger}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl,
          boxShadow: t.shadowSm,
        }}
      >
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.danger }}>
          Could not load attribution results
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          {(resultsQuery.error as Error)?.message || 'Backend may be unreachable. Check that the API is running and CORS/proxy is correct.'}
        </p>
      </div>
    )
  }

  if (resultsQuery.isLoading) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl * 2,
          textAlign: 'center',
          boxShadow: t.shadowSm,
        }}
      >
        <p style={{ fontSize: t.font.sizeBase, color: t.color.textSecondary, margin: 0 }}>
          Loading attribution results…
        </p>
      </div>
    )
  }

  if (models.length === 0) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl,
          boxShadow: t.shadowSm,
        }}
      >
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          No attribution results
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          Load journeys, then use <strong>Re-run attribution models</strong> from the sticky workspace header to generate live comparison results for this page.
        </p>
        {latestEventReplayDiagnostics ? (
          <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Latest raw-event replay diagnosis</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Events {Number(latestEventReplayDiagnostics.events_loaded || 0).toLocaleString()} · reconstructed profiles {Number(latestEventReplayDiagnostics.profiles_reconstructed || 0).toLocaleString()} · touchpoints {Number(latestEventReplayDiagnostics.touchpoints_reconstructed || 0).toLocaleString()} · conversions {Number(latestEventReplayDiagnostics.conversions_reconstructed || 0).toLocaleString()} · attributable profiles {Number(latestEventReplayDiagnostics.attributable_profiles || 0).toLocaleString()} · persisted journeys {Number(latestEventReplayDiagnostics.journeys_persisted || 0).toLocaleString()}
            </div>
            {!!latestEventReplayDiagnostics.warnings?.length && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                {latestEventReplayDiagnostics.warnings.join(' · ')}
              </div>
            )}
          </div>
        ) : null}
      </div>
    )
  }

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <h1
        style={{
          margin: 0,
          fontSize: t.font.size2xl,
          fontWeight: t.font.weightBold,
          color: t.color.text,
          letterSpacing: '-0.02em',
        }}
      >
        Attribution Model Comparison
      </h1>
      <p style={{ margin: `${t.space.xs}px 0 ${t.space.xl}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        Compare how different attribution models distribute credit across channels.
      </p>
      <div style={{ display: 'flex', justifyContent: 'flex-end', margin: `-${t.space.lg} 0 ${t.space.lg}` }}>
        <AnalysisShareActions
          fileStem="attribution-model-comparison"
          summaryTitle="Attribution comparison brief"
          summaryLines={[
            `Period: ${periodLabel}`,
            `Selected model: ${MODEL_LABELS[selectedModel] || selectedModel}`,
            `Baseline model: ${MODEL_LABELS[baselineModel] || baselineModel}`,
            `Direct handling: ${directMode === 'include' ? 'Include Direct' : 'Exclude Direct'}`,
            `Focus segment: ${selectedSegment ? selectedSegment.name : 'All visible channels'}`,
            `Freshness: ${freshnessLabel}`,
            `Coverage: ${coverageLabel}`,
            `Sensitivity draft: ${currentSensitivitySummary}`,
            `Sensitivity risk: ${summarizeSensitivityRisk(sensitivityQuery.data?.current)}`,
          ]}
        />
      </div>

      {readiness && (readiness.status === 'blocked' || readiness.warnings.length > 0) ? (
        <DecisionStatusCard
          title="Attribution Reliability Warning"
          status={readiness.status}
          blockers={readiness.blockers}
          warnings={readiness.warnings.slice(0, 3)}
        />
      ) : null}

      <div style={{ marginBottom: t.space.lg }}>
        <ContextSummaryStrip
          items={[
            { label: 'Source', value: 'Live attribution results' },
            { label: 'Period', value: periodLabel },
            {
              label: 'Config basis',
              value: resultsConfigId
                ? `Live attribution · config ${resultsConfigId.slice(0, 8)}… applied`
                : selectedConfigId
                  ? `Live attribution · waiting for config ${selectedConfigId.slice(0, 8)}…`
                  : 'Live attribution · default active config',
            },
            { label: 'Conversion', value: conversionKey ? `Conversion: ${conversionKey}` : 'Conversion: N/A' },
            { label: 'Freshness', value: freshnessLabel },
            { label: 'Coverage', value: coverageLabel },
            { label: 'Focus segment', value: selectedSegment ? selectedSegment.name : 'All visible channels' },
            { label: 'Sensitivity draft', value: currentSensitivitySummary },
            { label: 'Sensitivity risk', value: summarizeSensitivityRisk(sensitivityQuery.data?.current) },
          ]}
        />
        <SurfaceBasisNotice marginTop={t.space.sm}>
          This page is a <strong>live config-aware</strong> view. Compare it directly with other live attribution pages such as Attribution Roles and Path Archetypes. Compare it only directionally with workspace-fact or materialized-output pages like Overview, Journeys, and Conversion Paths.
        </SurfaceBasisNotice>
        {configMismatch ? (
          <SurfaceBasisNotice marginTop={t.space.sm}>
            The workspace is currently set to config <strong>{selectedConfigId?.slice(0, 8)}…</strong>, but the visible attribution results still reflect config <strong>{resultsConfigId?.slice(0, 8) ?? '—'}…</strong>. A rerun is in progress or still needed before this page fully matches the selected config.
          </SurfaceBasisNotice>
        ) : null}
      </div>

      <div style={{ marginBottom: t.space.lg }}>
        <AnalysisNarrativePanel
          title="What changed"
          subtitle="A short interpretation of the current model-comparison view."
          headline={comparisonNarrative.headline}
          items={comparisonNarrative.items}
        />
      </div>

      {selectedSegment && selectedSegmentAutoCompatible ? (
        <div
          style={{
            display: 'grid',
            gap: t.space.md,
            marginBottom: t.space.lg,
            gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          }}
        >
          {focusedSegmentModelComparison.map((item) => {
            const positive = item.shareDelta >= 0
            return (
              <div
                key={item.model}
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.md,
                  background: t.color.surface,
                  padding: t.space.md,
                  boxShadow: t.shadowSm,
                  display: 'grid',
                  gap: 6,
                }}
              >
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  {item.model}
                </div>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {formatCurrency(item.focusedValue)}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  {selectedSegment.name} share of workspace value: <strong style={{ color: t.color.text }}>{(item.focusedShare * 100).toFixed(1)}%</strong>
                </div>
                <div
                  style={{
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    color: positive ? t.color.success : t.color.danger,
                  }}
                >
                  vs {baselineKey}: {positive ? '+' : ''}{(item.shareDelta * 100).toFixed(1)} pp
                </div>
              </div>
            )
          })}
        </div>
      ) : null}

      {selectedSegment && compareSegment && segmentCompareQuery.data ? (
        <div
          style={{
            display: 'grid',
            gap: t.space.md,
            marginBottom: t.space.lg,
            gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          }}
        >
          <div style={{ gridColumn: '1 / -1' }}>
            <SegmentComparisonContextNote
              mode={selectedSegmentAutoCompatible ? 'exact_filter' : 'analytical_lens'}
              pageLabel="model comparison rows"
              basisLabel="matched journey-instance rows"
              primaryLabel={selectedSegment.name}
              primaryRows={segmentCompareQuery.data.primary_summary.journey_rows}
              otherLabel={compareSegment.name}
              otherRows={segmentCompareQuery.data.other_summary.journey_rows}
              baselineRows={segmentCompareQuery.data.baseline_summary.journey_rows}
              overlapRows={segmentCompareQuery.data.overlap.overlap_rows}
            />
          </div>
          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              background: t.color.surface,
              padding: t.space.md,
              boxShadow: t.shadowSm,
              display: 'grid',
              gap: 6,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Audience comparison
            </div>
            <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              {selectedSegment.name} vs {compareSegment.name}
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} · {(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Revenue delta {segmentCompareQuery.data.deltas.revenue == null ? '—' : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : '-'}${formatCurrency(Math.abs(segmentCompareQuery.data.deltas.revenue))}`}
            </div>
          </div>
          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              background: t.color.surface,
              padding: t.space.md,
              boxShadow: t.shadowSm,
              display: 'grid',
              gap: 6,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Matched journeys
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {selectedSegment.name}: <strong style={{ color: t.color.text }}>{(segmentCompareQuery.data.primary_summary.journey_rows ?? 0).toLocaleString()}</strong>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {compareSegment.name}: <strong style={{ color: t.color.text }}>{(segmentCompareQuery.data.other_summary.journey_rows ?? 0).toLocaleString()}</strong>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Shared rows: <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.overlap.overlap_rows.toLocaleString()}</strong>
            </div>
          </div>
          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              background: t.color.surface,
              padding: t.space.md,
              boxShadow: t.shadowSm,
              display: 'grid',
              gap: 6,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Lag and path depth
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Median lag delta: <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.deltas.median_lag_days == null ? '—' : `${segmentCompareQuery.data.deltas.median_lag_days >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.median_lag_days}d`}</strong>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Path-length delta: <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.deltas.avg_path_length == null ? '—' : `${segmentCompareQuery.data.deltas.avg_path_length >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.avg_path_length.toFixed(1)} steps`}</strong>
            </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Top channels: <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.distributions.primary_channels.map((item) => item.value).slice(0, 2).join(', ') || '—'}</strong> vs <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.distributions.other_channels.map((item) => item.value).slice(0, 2).join(', ') || '—'}</strong>
              </div>
            </div>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', gridColumn: '1 / -1' }}>
            <a
              href={buildIncrementalityPlannerHref({
                conversionKey: conversionKey || null,
                startAt: comparisonDateFrom || null,
                endAt: comparisonDateTo || null,
                segmentId: selectedSegment.id,
                name: `Audience test: ${selectedSegment.name} vs ${compareSegment.name}`,
                notes: `Compare ${selectedSegment.name} against ${compareSegment.name} from Attribution Comparison. Relationship ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity.`,
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
            <a
              href={buildSegmentComparisonHref(selectedSegment.id, compareSegment.id)}
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
      ) : null}

      {selectedSegment && !selectedSegmentAutoCompatible && segmentAnalysisQuery.data ? (
        <div
          style={{
            display: 'grid',
            gap: t.space.md,
            marginBottom: t.space.lg,
            gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          }}
        >
          <div
            style={{
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              background: t.color.surface,
              padding: t.space.md,
              boxShadow: t.shadowSm,
              display: 'grid',
              gap: 6,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Advanced audience lens
            </div>
            <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              {(segmentAnalysisQuery.data.summary.journey_rows ?? 0).toLocaleString()} matched rows
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Median lag {segmentAnalysisQuery.data.summary.median_lag_days != null ? `${segmentAnalysisQuery.data.summary.median_lag_days}d` : '—'} · average path {segmentAnalysisQuery.data.summary.avg_path_length != null ? `${segmentAnalysisQuery.data.summary.avg_path_length.toFixed(1)} steps` : '—'}
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Top channels: {(segmentAnalysisQuery.data.distributions.channels ?? []).slice(0, 3).map((item) => item.value).join(', ') || '—'}
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Path types: {(segmentAnalysisQuery.data.distributions.path_types ?? []).slice(0, 3).map((item) => item.value).join(', ') || '—'}
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              This advanced audience stays as an <strong>analytical lens</strong>. Use it to interpret directional differences, not as a literal replacement for the visible channel table totals.
            </div>
          </div>
        </div>
      ) : null}

      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: t.space.md,
          alignItems: 'center',
          marginBottom: t.space.lg,
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.md,
          boxShadow: t.shadowSm,
        }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Conversion
          </span>
          <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
            {conversionKey ? `Conversion: ${conversionKey}` : 'Conversion: N/A'}
          </span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Date range
          </span>
          <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{periodLabel}</span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Direct handling (view filter)
          </span>
          <div
            style={{
              display: 'inline-flex',
              borderRadius: t.radius.full,
              border: `1px solid ${t.color.border}`,
              overflow: 'hidden',
            }}
          >
            <button
              type="button"
              onClick={() => setDirectMode('include')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                border: 'none',
                cursor: 'pointer',
                backgroundColor: directMode === 'include' ? t.color.accent : 'transparent',
                color: directMode === 'include' ? t.color.surface : t.color.textSecondary,
              }}
            >
              Include Direct
            </button>
            <button
              type="button"
              onClick={() => setDirectMode('exclude_view')}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                border: 'none',
                cursor: 'pointer',
                backgroundColor: directMode === 'exclude_view' ? t.color.accent : 'transparent',
                color: directMode === 'exclude_view' ? t.color.surface : t.color.textSecondary,
              }}
            >
              Exclude Direct
            </button>
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 220 }}>
          <span
            style={{
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
            }}
          >
            Focus segment
          </span>
          <select
            value={selectedSegmentId}
            onChange={(e) => setSelectedSegmentId(e.target.value)}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              fontSize: t.font.sizeXs,
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              background: t.color.surface,
              color: t.color.text,
            }}
          >
            <option value="">All visible channels / no saved segment</option>
            {localSegments.map((segment) => (
              <option key={segment.id} value={segment.id}>
                {segmentOptionLabel(segment)}
              </option>
            ))}
          </select>
        </div>

        {selectedSegment ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 220 }}>
            <span
              style={{
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                color: t.color.textSecondary,
              }}
            >
              Compare with
            </span>
            <select
              value={compareSegmentId}
              onChange={(e) => setCompareSegmentId(e.target.value)}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                fontSize: t.font.sizeXs,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.text,
              }}
            >
              <option value="">No paired comparison</option>
              {localSegments
                .filter((segment) => segment.id !== selectedSegment.id)
                .map((segment) => (
                  <option key={segment.id} value={segment.id}>
                    {segmentOptionLabel(segment)}
                  </option>
                ))}
            </select>
          </div>
        ) : null}

        <div
          style={{
            marginLeft: 'auto',
            fontSize: t.font.sizeXs,
            color: t.color.textMuted,
            maxWidth: 380,
          }}
        >
          {configMeta?.time_window ? (
            <>
              Click lookback: {configMeta.time_window.click_lookback_days ?? '—'}d · Impression lookback:{' '}
              {configMeta.time_window.impression_lookback_days ?? '—'}d · Session timeout:{' '}
              {configMeta.time_window.session_timeout_minutes ?? '—'}min
            </>
          ) : (
            <>Measurement windows not configured for this model.</>
          )}
          {configMeta?.eligible_touchpoints && (
            <div style={{ marginTop: 2 }}>
              Touchpoints:{' '}
              {[
                configMeta.eligible_touchpoints.include_channels?.length
                  ? `+${configMeta.eligible_touchpoints.include_channels.join(', ')}`
                  : null,
                configMeta.eligible_touchpoints.exclude_channels?.length
                  ? `excl. ${configMeta.eligible_touchpoints.exclude_channels.join(', ')}`
                  : null,
              ]
                .filter(Boolean)
                .join(' · ')}
            </div>
          )}
          <div style={{ marginTop: 6 }}>
            Focus:{' '}
            <strong style={{ color: t.color.text }}>
              {selectedSegment ? (selectedSegmentAutoCompatible ? `${selectedSegment.name} (channel-group compatible)` : `${selectedSegment.name} (advanced audience lens)`) : 'all visible channels'}
            </strong>
          </div>
          <div style={{ marginTop: 6 }}>
            <a href={buildSettingsHref('segments')} style={{ color: t.color.accent, textDecoration: 'none' }}>
              Manage segments
            </a>
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gap: t.space.lg, marginBottom: t.space.xl }}>
        <SegmentOverlapNotice selectedSegment={selectedSegment} />

        <CollapsiblePanel
          title="Method & Context"
          subtitle="Measurement windows, eligible touchpoints, and the current direct-traffic view filter."
          open={showContextPanel}
          onToggle={() => setShowContextPanel((value) => !value)}
        >
          <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <div>
              This page compares model outputs from the current attribution run. Direct handling is a view-only filter here and does not rerun attribution.
            </div>
            <div>
              Need a full reliability view?{' '}
              <a href="/?page=trust" style={{ color: t.color.accent, textDecoration: 'none' }}>
                Open Attribution Trust
              </a>
            </div>
            <div>
              Current view: <strong style={{ color: t.color.text }}>{directMode === 'include' ? 'Include Direct' : 'Exclude Direct'}</strong>
            </div>
            <div>
              Measurement windows:{' '}
              <strong style={{ color: t.color.text }}>
                {configMeta?.time_window
                  ? `click ${configMeta.time_window.click_lookback_days ?? '—'}d · impression ${configMeta.time_window.impression_lookback_days ?? '—'}d · session ${configMeta.time_window.session_timeout_minutes ?? '—'}min`
                  : 'not configured for this model'}
              </strong>
            </div>
            <div>
              Eligible touchpoints:{' '}
              <strong style={{ color: t.color.text }}>
                {configMeta?.eligible_touchpoints
                  ? [
                      configMeta.eligible_touchpoints.include_channels?.length
                        ? `+${configMeta.eligible_touchpoints.include_channels.join(', ')}`
                        : null,
                      configMeta.eligible_touchpoints.exclude_channels?.length
                        ? `excl. ${configMeta.eligible_touchpoints.exclude_channels.join(', ')}`
                        : null,
                    ]
                      .filter(Boolean)
                      .join(' · ') || 'all observed channels'
                  : 'not configured'}
              </strong>
            </div>
          </div>
        </CollapsiblePanel>

        {(latestEventReplayDiagnostics || readiness) && (
          <CollapsiblePanel
            title="Replay & Reliability"
            subtitle="Coverage, freshness, and latest raw-event replay diagnostics behind the current attribution outputs."
            open={showReplayPanel}
            onToggle={() => setShowReplayPanel((value) => !value)}
          >
            <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <div>
                Current readiness: <strong style={{ color: t.color.text }}>{readiness?.status || 'unavailable'}</strong>
                {readiness?.warnings?.length ? ` · ${readiness.warnings.slice(0, 2).join(' · ')}` : ''}
              </div>
              <div>
                Open the unified trust workspace when you need mapping coverage, taxonomy unknown share, direct-path diagnostics, and live-vs-materialized reconciliation in one place.
              </div>
              <div>
                KPI coverage <strong style={{ color: t.color.text }}>{coverageLabel}</strong> · Freshness{' '}
                <strong style={{ color: t.color.text }}>{freshnessLabel}</strong>
              </div>
              {latestEventReplayDiagnostics ? (
                <div>
                  Latest replay: events <strong style={{ color: t.color.text }}>{Number(latestEventReplayDiagnostics.events_loaded || 0).toLocaleString()}</strong>
                  {' · '}profiles <strong style={{ color: t.color.text }}>{Number(latestEventReplayDiagnostics.profiles_reconstructed || 0).toLocaleString()}</strong>
                  {' · '}touchpoints <strong style={{ color: t.color.text }}>{Number(latestEventReplayDiagnostics.touchpoints_reconstructed || 0).toLocaleString()}</strong>
                  {' · '}conversions <strong style={{ color: t.color.text }}>{Number(latestEventReplayDiagnostics.conversions_reconstructed || 0).toLocaleString()}</strong>
                </div>
              ) : null}
              {!!latestEventReplayDiagnostics?.warnings?.length && (
                <div style={{ color: t.color.warning }}>{latestEventReplayDiagnostics.warnings.join(' · ')}</div>
              )}
            </div>
          </CollapsiblePanel>
        )}

        <CollapsiblePanel
          title="Sensitivity & Window Impact"
          subtitle="Preview how attribution eligibility changes when you tighten or loosen key measurement settings."
          open={showSensitivityPanel}
          onToggle={() => setShowSensitivityPanel((value) => !value)}
        >
          <div style={{ display: 'grid', gap: t.space.md }}>
            {sharedScenarioSummary ? (
              <div
                style={{
                  border: `1px solid ${t.color.accent}`,
                  background: t.color.accentMuted,
                  color: t.color.text,
                  borderRadius: t.radius.md,
                  padding: t.space.md,
                  fontSize: t.font.sizeSm,
                }}
              >
                {sharedScenarioSummary}
              </div>
            ) : null}
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))',
                gap: t.space.md,
              }}
            >
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <span>Click lookback (days)</span>
                <input
                  type="number"
                  min={1}
                  value={sensitivityDraft?.lookback_window_days ?? 30}
                  onChange={(event) =>
                    setSensitivityDraft((current) =>
                      current
                        ? { ...current, lookback_window_days: Math.max(1, Number(event.target.value || 1)) }
                        : current,
                    )
                  }
                  style={{
                    fontSize: t.font.sizeSm,
                    padding: '8px 10px',
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                    background: t.color.surface,
                    color: t.color.text,
                  }}
                />
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <span>Minimum journey quality</span>
                <input
                  type="number"
                  min={0}
                  max={100}
                  value={sensitivityDraft?.min_journey_quality_score ?? 0}
                  onChange={(event) =>
                    setSensitivityDraft((current) =>
                      current
                        ? {
                            ...current,
                            min_journey_quality_score: Math.max(0, Math.min(100, Number(event.target.value || 0))),
                          }
                        : current,
                    )
                  }
                  style={{
                    fontSize: t.font.sizeSm,
                    padding: '8px 10px',
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                    background: t.color.surface,
                    color: t.color.text,
                  }}
                />
              </label>
              <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <input
                  type="checkbox"
                  checked={!!sensitivityDraft?.use_converted_flag}
                  onChange={(event) =>
                    setSensitivityDraft((current) =>
                      current ? { ...current, use_converted_flag: event.target.checked } : current,
                    )
                  }
                />
                Converted journeys only
              </label>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  This preview changes dataset eligibility and windowing only. It does not rerun model math or channel weights.
                </div>
                {selectedSegment ? (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Focus segment <strong style={{ color: t.color.text }}>{selectedSegment.name}</strong> limits the visible channel comparison and lag exposure only. Attribution totals remain workspace-wide.
                  </div>
                ) : null}
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <a
                  href="/?page=campaigns"
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    textDecoration: 'none',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  Open campaign lag view
                </a>
                <button
                  type="button"
                  onClick={() => {
                    if (!sensitivityDraft) return
                    const nextIndex = savedSensitivityScenarios.length + 1
                    const timestamp = new Date().toISOString()
                    setSavedSensitivityScenarios((current) => [
                      {
                        id: `scenario-${timestamp}`,
                        label: `Scenario ${nextIndex}`,
                        saved_at: timestamp,
                        settings: sensitivityDraft,
                      },
                      ...current,
                    ].slice(0, 6))
                  }}
                  style={{
                    border: `1px solid ${t.color.accent}`,
                    background: t.color.accentMuted,
                    color: t.color.accent,
                    borderRadius: t.radius.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    cursor: 'pointer',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  Save scenario
                </button>
                <button
                  type="button"
                  onClick={() => setSensitivityDraft(settingsQuery.data?.attribution ?? null)}
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    cursor: 'pointer',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  Reset to workspace defaults
                </button>
                <a
                    href={
                      sensitivityDraft
                        ? buildSettingsHref('attribution', {
                            searchParams: {
                              attr_lookback: String(sensitivityDraft.lookback_window_days),
                              attr_quality: String(sensitivityDraft.min_journey_quality_score),
                              attr_converted: sensitivityDraft.use_converted_flag ? '1' : '0',
                            },
                          })
                        : buildSettingsHref('attribution')
                    }
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    textDecoration: 'none',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  Open attribution settings
                </a>
                <button
                  type="button"
                  onClick={async () => {
                    if (typeof window === 'undefined') return
                    const href = buildSensitivityHref(sensitivityDraft)
                    try {
                      await window.navigator.clipboard.writeText(window.location.origin + href)
                    } catch {
                      window.prompt('Copy sensitivity scenario link', window.location.origin + href)
                    }
                  }}
                  style={{
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                    borderRadius: t.radius.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    cursor: 'pointer',
                    fontSize: t.font.sizeSm,
                  }}
                >
                  Copy scenario link
                </button>
              </div>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Current draft: <strong style={{ color: t.color.text }}>{currentSensitivitySummary}</strong>
            </div>
            {sensitivityQuery.isError ? (
              <DecisionStatusCard
                title="Sensitivity Preview Unavailable"
                status="warning"
                warnings={[(sensitivityQuery.error as Error)?.message || 'Failed to calculate attribution preview']}
              />
            ) : (
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(min(240px, 100%), 1fr))',
                  gap: t.space.md,
                }}
              >
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Current draft</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {Number(sensitivityQuery.data?.current.eligibleJourneys || 0).toLocaleString()} eligible journeys
                  </div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Window impact {Number(sensitivityQuery.data?.current.windowImpactCount || 0).toLocaleString()} · Quality impact {Number(sensitivityQuery.data?.current.qualityImpactCount || 0).toLocaleString()} · Converted-flag impact {Number(sensitivityQuery.data?.current.useConvertedFlagImpact || 0).toLocaleString()}
                  </div>
                </div>
                {(sensitivityQuery.data?.scenarios || []).map((scenario) => (
                  <div key={scenario.id} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>{scenario.label}</div>
                    <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                      {Number(scenario.preview.eligibleJourneys || 0).toLocaleString()} eligible journeys
                    </div>
                    <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{scenario.note}</div>
                    <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      Window {Number(scenario.preview.windowImpactCount || 0).toLocaleString()} · Quality {Number(scenario.preview.qualityImpactCount || 0).toLocaleString()} · Converted {Number(scenario.preview.useConvertedFlagImpact || 0).toLocaleString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ display: 'grid', gap: t.space.sm }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                Channels most exposed to tighter windows
              </div>
              {channelLagQuery.isLoading ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading channel lag exposure…</div>
              ) : channelLagQuery.isError ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                  {(channelLagQuery.error as Error)?.message || 'Failed to load channel lag exposure.'}
                </div>
              ) : !topExposedChannels.length ? (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  No channel lag evidence is available for the current period.
                </div>
              ) : (
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))',
                    gap: t.space.md,
                  }}
                >
                  {topExposedChannels.map((item) => {
                    const over7dShare = item.conversions > 0 ? item.lag_buckets.over_7d / item.conversions : 0
                    return (
                      <div
                        key={item.key}
                        style={{
                          border: `1px solid ${t.color.borderLight}`,
                          borderRadius: t.radius.md,
                          padding: t.space.md,
                          background: t.color.surface,
                          display: 'grid',
                          gap: t.space.xs,
                        }}
                      >
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                          {item.label}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          P50 first-touch lag {item.p50_days_from_first_touch != null ? `${item.p50_days_from_first_touch.toFixed(1)}d` : '—'} · Over 7d {(over7dShare * 100).toFixed(1)}%
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          {over7dShare >= 0.5
                            ? 'This channel has a heavy long-lag tail and should be reviewed before tightening lookback windows.'
                            : over7dShare >= 0.25
                              ? 'This channel has a moderate long-lag tail and may shift under shorter windows.'
                              : 'This channel is less exposed than the other visible channels.'}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
            {!!savedSensitivityScenarios.length && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  Saved scenarios
                </div>
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))',
                    gap: t.space.md,
                  }}
                >
                  {savedSensitivityScenarios.map((scenario) => (
                    <div
                      key={scenario.id}
                      style={{
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.md,
                        padding: t.space.md,
                        background: t.color.surface,
                        display: 'grid',
                        gap: t.space.xs,
                      }}
                    >
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                        {scenario.label}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {new Date(scenario.saved_at).toLocaleString()}
                      </div>
                      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                        Lookback {scenario.settings.lookback_window_days}d · Quality ≥{scenario.settings.min_journey_quality_score} · {scenario.settings.use_converted_flag ? 'Converted only' : 'All journeys'}
                      </div>
                      <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                        <button
                          type="button"
                          onClick={() => setSensitivityDraft(scenario.settings)}
                          style={{
                            border: `1px solid ${t.color.accent}`,
                            background: t.color.accentMuted,
                            color: t.color.accent,
                            borderRadius: t.radius.sm,
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            cursor: 'pointer',
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          Apply
                        </button>
                        <button
                          type="button"
                          onClick={async () => {
                            if (typeof window === 'undefined') return
                            const href = buildSensitivityHref(scenario.settings)
                            try {
                              await window.navigator.clipboard.writeText(window.location.origin + href)
                            } catch {
                              window.prompt('Copy sensitivity scenario link', window.location.origin + href)
                            }
                          }}
                          style={{
                            border: `1px solid ${t.color.border}`,
                            background: t.color.surface,
                            color: t.color.textSecondary,
                            borderRadius: t.radius.sm,
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            cursor: 'pointer',
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          Copy link
                        </button>
                        <button
                          type="button"
                          onClick={() =>
                            setSavedSensitivityScenarios((current) => current.filter((item) => item.id !== scenario.id))
                          }
                          style={{
                            border: `1px solid ${t.color.border}`,
                            background: t.color.surface,
                            color: t.color.textSecondary,
                            borderRadius: t.radius.sm,
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            cursor: 'pointer',
                            fontSize: t.font.sizeXs,
                          }}
                        >
                          Remove
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </CollapsiblePanel>
      </div>

      {/* Summary strip */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            padding: `${t.space.lg}px ${t.space.xl}px`,
            boxShadow: t.shadowSm,
          }}
        >
          <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            Models
          </div>
          <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
            {models.length}
          </div>
        </div>
        {selectedResult && (
          <>
            <div
              style={{
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: `${t.space.lg}px ${t.space.xl}px`,
                boxShadow: t.shadowSm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Selected: {MODEL_LABELS[selectedModel] || selectedModel}
              </div>
              <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
                {formatCurrency(selectedResult.total_value)}
              </div>
              {(selectedResult.gross_total_value != null || selectedResult.net_total_value != null) && (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: t.space.xs }}>
                  Gross {formatCurrency(selectedResult.gross_total_value || selectedResult.total_value)} · Net {formatCurrency(selectedResult.net_total_value || selectedResult.total_value)}
                </div>
              )}
            </div>
            <div
              style={{
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: `${t.space.lg}px ${t.space.xl}px`,
                boxShadow: t.shadowSm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Conversions
              </div>
              <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text, marginTop: t.space.xs, fontVariantNumeric: 'tabular-nums' }}>
                {selectedResult.total_conversions.toLocaleString()}
              </div>
              {(selectedResult.gross_total_conversions != null || selectedResult.net_total_conversions != null) && (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: t.space.xs }}>
                  Gross {Number(selectedResult.gross_total_conversions || selectedResult.total_conversions).toLocaleString()} · Net {Number(selectedResult.net_total_conversions || selectedResult.total_conversions).toLocaleString()}
                </div>
              )}
            </div>
            <div
              style={{
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: `${t.space.lg}px ${t.space.xl}px`,
                boxShadow: t.shadowSm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Click / View Split
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.text, marginTop: t.space.xs, lineHeight: 1.5 }}>
                Click {Number(selectedResult.interaction_summary?.click_through_conversions || 0).toLocaleString()} · View {Number(selectedResult.interaction_summary?.view_through_conversions || 0).toLocaleString()} · Mixed {Number(selectedResult.interaction_summary?.mixed_path_conversions || 0).toLocaleString()}
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: t.space.xs }}>
                {selectedResult.value_mode === 'net_only' ? 'Net-only attribution mode' : selectedResult.value_mode === 'gross_and_net' ? 'Gross and net tracked side by side' : 'Gross-only attribution mode'}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Comparison bar chart */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          marginBottom: t.space.xl,
          boxShadow: t.shadowSm,
        }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: t.space.md,
              gap: t.space.md,
              flexWrap: 'wrap',
            }}
          >
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              {comparisonMode === 'absolute' ? 'Attributed Revenue by Model & Channel' : 'Delta vs Baseline by Channel'}
            </h3>
            <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Comparison mode</span>
                <div
                  style={{
                    display: 'inline-flex',
                    borderRadius: t.radius.full,
                    border: `1px solid ${t.color.border}`,
                    overflow: 'hidden',
                  }}
                >
                  <button
                    type="button"
                    onClick={() => setComparisonMode('absolute')}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: 'none',
                      cursor: 'pointer',
                      backgroundColor: comparisonMode === 'absolute' ? t.color.accent : 'transparent',
                      color: comparisonMode === 'absolute' ? t.color.surface : t.color.textSecondary,
                    }}
                  >
                    Absolute
                  </button>
                  <button
                    type="button"
                    onClick={() => setComparisonMode('delta')}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      border: 'none',
                      cursor: 'pointer',
                      backgroundColor: comparisonMode === 'delta' ? t.color.accent : 'transparent',
                      color: comparisonMode === 'delta' ? t.color.surface : t.color.textSecondary,
                    }}
                  >
                    Delta vs baseline
                  </button>
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Baseline</span>
                <select
                  value={baselineKey}
                  onChange={(e) => setBaselineModel(e.target.value)}
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    fontSize: t.font.sizeXs,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: t.color.surface,
                    color: t.color.text,
                  }}
                >
                  {models.map((m) => (
                    <option key={m} value={m}>
                      {MODEL_LABELS[m] || m}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          {comparisonMode === 'delta' && winnersLosers.winners.length + winnersLosers.losers.length > 0 && (
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: '1fr 1fr',
                gap: t.space.md,
                marginBottom: t.space.md,
                fontSize: t.font.sizeXs,
              }}
            >
              <div>
                <div style={{ fontWeight: t.font.weightSemibold, color: t.color.success, marginBottom: 4 }}>
                  Winners vs baseline (selected model)
                </div>
                {winnersLosers.winners.length === 0 && (
                  <div style={{ color: t.color.textMuted }}>No channels gaining material credit.</div>
                )}
                {winnersLosers.winners.map((w) => (
                  <div key={w.channel} style={{ color: t.color.text }}>
                    {w.channel}: <span style={{ color: t.color.success }}>{formatCurrency(w.delta)}</span>
                  </div>
                ))}
              </div>
              <div>
                <div style={{ fontWeight: t.font.weightSemibold, color: t.color.danger, marginBottom: 4 }}>
                  Losers vs baseline (selected model)
                </div>
                {winnersLosers.losers.length === 0 && (
                  <div style={{ color: t.color.textMuted }}>No channels losing material credit.</div>
                )}
                {winnersLosers.losers.map((l) => (
                  <div key={l.channel} style={{ color: t.color.text }}>
                    {l.channel}: <span style={{ color: t.color.danger }}>{formatCurrency(l.delta)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          <ResponsiveContainer width="100%" height={360}>
            <BarChart data={chartData} margin={{ top: 8, right: 16, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
            <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
            <YAxis
              tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }}
              tickFormatter={(v) => formatCurrency(v)}
            />
            <Tooltip formatter={(value: number) => formatCurrency(value)} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
            {models.map((model) => (
              <Bar
                key={model}
                dataKey={model}
                name={MODEL_LABELS[model] || model}
                fill={MODEL_COLORS[model] || t.color.textMuted}
                radius={[4, 4, 0, 0]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Model cards */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
          gap: t.space.lg,
          marginBottom: t.space.xl,
        }}
      >
        {models.map((model) => {
          const r = results[model]
          const isSelected = model === selectedModel
          return (
            <div
              key={model}
              onClick={() => onSelectModel(model)}
              style={{
                background: t.color.surface,
                border: `2px solid ${isSelected ? MODEL_COLORS[model] : t.color.borderLight}`,
                borderRadius: t.radius.lg,
                padding: t.space.lg,
                boxShadow: t.shadowSm,
                cursor: 'pointer',
                transition: 'border-color 0.2s, box-shadow 0.2s',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.md }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <h4 style={{ margin: 0, fontSize: t.font.sizeBase, fontWeight: t.font.weightBold, color: MODEL_COLORS[model] }}>
                    {MODEL_LABELS[model] || model}
                  </h4>
                  {model === 'markov' && r.diagnostics && (
                    <span
                      style={{
                        alignSelf: 'flex-start',
                        padding: '2px 8px',
                        borderRadius: 999,
                        fontSize: t.font.sizeXs,
                        fontWeight: t.font.weightSemibold,
                        color:
                          r.diagnostics.reliability === 'unreliable'
                            ? t.color.danger
                            : r.diagnostics.reliability === 'warning'
                            ? t.color.warning
                            : t.color.success,
                        backgroundColor:
                          r.diagnostics.reliability === 'unreliable'
                            ? `${t.color.danger}18`
                            : r.diagnostics.reliability === 'warning'
                            ? `${t.color.warning}18`
                            : `${t.color.success}18`,
                      }}
                    >
                      Reliability:{' '}
                      {(r.diagnostics.reliability || 'ok').toUpperCase()}
                    </span>
                  )}
                </div>
                {isSelected && (
                  <span
                    style={{
                      fontSize: t.font.sizeXs,
                      fontWeight: t.font.weightBold,
                      color: t.color.surface,
                      backgroundColor: MODEL_COLORS[model],
                      padding: '2px 8px',
                      borderRadius: 10,
                    }}
                  >
                    ACTIVE
                  </span>
                )}
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <div style={{ marginBottom: 4 }}>
                  Conversions:{' '}
                  <strong style={{ color: t.color.text }}>{r.total_conversions}</strong>
                </div>
                <div style={{ marginBottom: 4 }}>
                  Total value:{' '}
                  <strong style={{ color: t.color.text }}>{formatCurrency(r.total_value)}</strong>
                </div>
                {model === 'markov' && r.diagnostics?.insufficient_data && (
                  <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.danger }}>
                    Insufficient data for stable Markov. Treat results as unreliable.
                  </div>
                )}
              </div>
              {model === 'markov' && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation()
                    setShowMarkovDiagnostics(true)
                  }}
                  style={{
                    marginTop: t.space.sm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    fontSize: t.font.sizeXs,
                    color: t.color.accent,
                    background: 'transparent',
                    border: 'none',
                    cursor: 'pointer',
                    textDecoration: 'underline',
                  }}
                >
                  View diagnostics
                </button>
              )}
              {model !== baselineKey && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation()
                    setBaselineModel(model)
                  }}
                  style={{
                    marginTop: t.space.xs,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    fontSize: t.font.sizeXs,
                    color: t.color.textSecondary,
                    background: 'transparent',
                    border: `1px dashed ${t.color.borderLight}`,
                    borderRadius: t.radius.full,
                    cursor: 'pointer',
                  }}
                >
                  Set as baseline
                </button>
              )}
              {r.channels && (
                <div style={{ fontSize: t.font.sizeSm }}>
                  {r.channels.slice(0, 4).map((ch) => (
                    <div key={ch.channel} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: `1px solid ${t.color.borderLight}` }}>
                      <span style={{ color: t.color.text }}>{ch.channel}</span>
                      <span style={{ fontWeight: t.font.weightSemibold, color: MODEL_COLORS[model] }}>{(ch.attributed_share * 100).toFixed(1)}%</span>
                    </div>
                  ))}
                  {r.channels.length > 4 && (
                    <div style={{ color: t.color.textMuted, fontSize: t.font.sizeXs, marginTop: 4 }}>+{r.channels.length - 4} more</div>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* Comparison table + export */}
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
            Attribution Share by Model (%)
          </h3>
          <label
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            <input
              type="checkbox"
              checked={showDeltaRow}
              onChange={(e) => setShowDeltaRow(e.target.checked)}
              disabled={comparisonMode !== 'delta' || !baselineKey}
            />
            Show delta vs baseline
          </label>
          <button
            type="button"
          onClick={() =>
              exportComparisonCSV(comparisonData, models, {
                mode: comparisonMode,
                baselineModel: comparisonMode === 'delta' ? baselineKey : null,
                conversionKey,
                configVersion,
                directMode,
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
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'left',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    cursor: 'pointer',
                    userSelect: 'none',
                  }}
                  onClick={() => {
                    setSortBy('channel')
                    setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                  }}
                >
                  Channel {sortBy === 'channel' && (sortDir === 'asc' ? '↑' : '↓')}
                </th>
                {models.map((model) => (
                  <th
                    key={model}
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'center',
                      fontWeight: t.font.weightSemibold,
                      color: MODEL_COLORS[model],
                      borderBottom: `2px solid ${t.color.border}`,
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => {
                      setSortBy(`${model}_share`)
                      setSortDir('desc')
                    }}
                  >
                    {MODEL_LABELS[model] || model}
                    {model === selectedModel && ' *'}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedData.map((row, idx) => (
                <tr
                  key={String(row.channel)}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                  }}
                >
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, fontWeight: t.font.weightMedium, color: t.color.text }}>
                    {String(row.channel)}
                  </td>
                  {models.map((model) => {
                    const share = ((row[`${model}_share`] as number) || 0) * 100
                    const maxShare = Math.max(...models.map((m) => ((row[`${m}_share`] as number) || 0) * 100))
                    const isMax = share > 0 && share === maxShare
                    const baseShare =
                      comparisonMode === 'delta' && baselineKey
                        ? (((row[`${baselineKey}_share`] as number) || 0) * 100)
                        : null
                    const deltaShare =
                      comparisonMode === 'delta' && baselineKey && baseShare != null
                        ? share - baseShare
                        : null
                    return (
                      <td
                        key={model}
                        style={{
                          padding: `${t.space.md}px ${t.space.lg}px`,
                          textAlign: 'center',
                          fontWeight: isMax ? t.font.weightBold : t.font.weightNormal,
                          color: isMax ? MODEL_COLORS[model] : t.color.text,
                          backgroundColor: share > 20 ? `${MODEL_COLORS[model]}18` : 'transparent',
                          fontVariantNumeric: 'tabular-nums',
                        }}
                      >
                        <div>{share.toFixed(1)}%</div>
                        {showDeltaRow && deltaShare != null && (
                          <div
                            style={{
                              fontSize: t.font.sizeXs,
                              color: deltaShare > 0 ? t.color.success : deltaShare < 0 ? t.color.danger : t.color.textMuted,
                            }}
                          >
                            {deltaShare > 0 ? '+' : ''}
                            {deltaShare.toFixed(1)}pp
                          </div>
                        )}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Markov diagnostics drawer */}
      {showMarkovDiagnostics && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            backgroundColor: '#00000055',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 40,
          }}
          onClick={() => setShowMarkovDiagnostics(false)}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              maxWidth: 520,
              width: '100%',
              maxHeight: '80vh',
              overflowY: 'auto',
              background: t.color.surface,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.border}`,
              padding: t.space.lg,
              boxShadow: t.shadowLg,
            }}
          >
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: t.space.sm,
              }}
            >
              <h3
                style={{
                  margin: 0,
                  fontSize: t.font.sizeMd,
                  fontWeight: t.font.weightSemibold,
                  color: t.color.text,
                }}
              >
                Data-Driven (Markov) diagnostics
              </h3>
              <button
                type="button"
                onClick={() => setShowMarkovDiagnostics(false)}
                style={{
                  border: 'none',
                  background: 'transparent',
                  cursor: 'pointer',
                  fontSize: t.font.sizeBase,
                  color: t.color.textSecondary,
                }}
              >
                ✕
              </button>
            </div>
            {!markovDiagnostics && (
              <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, margin: 0 }}>
                Diagnostics are not available for this Markov run.
              </p>
            )}
            {markovDiagnostics && (
              <>
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
                    gap: t.space.md,
                    marginBottom: t.space.md,
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Journeys used</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.journeys_used ?? 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Converted journeys</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.converted_journeys ?? 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Unique states</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.unique_states ?? 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: t.font.weightMedium }}>Unique transitions</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: t.color.text }}>
                      {markovDiagnostics.unique_transitions ?? 'N/A'}
                    </div>
                  </div>
                </div>

                <div style={{ marginBottom: t.space.md }}>
                  <div
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: 4,
                    }}
                  >
                    Top transitions
                  </div>
                  {!markovDiagnostics.top_transitions || markovDiagnostics.top_transitions.length === 0 ? (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>Not available.</div>
                  ) : (
                    <ul
                      style={{
                        listStyle: 'none',
                        padding: 0,
                        margin: 0,
                        fontSize: t.font.sizeSm,
                        color: t.color.text,
                      }}
                    >
                      {markovDiagnostics.top_transitions.map((tr, idx) => (
                        <li key={`${tr.from}-${tr.to}-${idx}`} style={{ marginBottom: 2 }}>
                          {tr.from} → {tr.to}{' '}
                          <span style={{ color: t.color.textSecondary }}>
                            ({tr.count} paths{tr.share != null ? ` • ${(tr.share * 100).toFixed(1)}%` : ''})
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div style={{ marginBottom: t.space.md }}>
                  <div
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: 4,
                    }}
                  >
                    Triggered warnings
                  </div>
                  {!markovDiagnostics.warnings || markovDiagnostics.warnings.length === 0 ? (
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>No warnings triggered.</div>
                  ) : (
                    <ul
                      style={{
                        margin: 0,
                        paddingLeft: t.space.lg,
                        fontSize: t.font.sizeSm,
                        color: t.color.text,
                      }}
                    >
                      {markovDiagnostics.warnings.map((w, idx) => (
                        <li key={idx}>{w}</li>
                      ))}
                    </ul>
                  )}
                </div>

                <div>
                  <div
                    style={{
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.textSecondary,
                      marginBottom: 4,
                    }}
                  >
                    What to do next
                  </div>
                  <ul
                    style={{
                      margin: 0,
                      paddingLeft: t.space.lg,
                      fontSize: t.font.sizeSm,
                      color: t.color.text,
                    }}
                  >
                    {(markovDiagnostics.what_to_do_next && markovDiagnostics.what_to_do_next.length
                      ? markovDiagnostics.what_to_do_next
                      : [
                          'Increase data window or volume to include more journeys.',
                          'Improve source and campaign mapping, especially for Direct-heavy traffic.',
                          'Compare Markov deltas against simpler models (Linear, Position-based).',
                          'Use simpler models until Markov reliability improves.',
                        ]
                    ).map((item, idx) => (
                      <li key={idx}>{item}</li>
                    ))}
                  </ul>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
