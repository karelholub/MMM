import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { DashboardPage, ContextSummaryStrip, SectionCard, AnalysisShareActions, AnalysisNarrativePanel } from '../components/dashboard'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import DecisionStatusCard from '../components/DecisionStatusCard'
import SegmentComparisonContextNote from '../components/segments/SegmentComparisonContextNote'
import SegmentOverlapNotice from '../components/segments/SegmentOverlapNotice'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import { apiGetJson } from '../lib/apiClient'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import {
  isLocalAnalyticalSegment,
  localSegmentCompatibleWithDimensions,
  readLocalSegmentDefinition,
  segmentOptionLabel,
  type SegmentAnalysisResponse,
  type SegmentRegistryItem,
  type SegmentRegistryResponse,
} from '../lib/segments'
import { tokens as t } from '../theme/tokens'

interface AttributionTrustProps {
  model: string
  configId?: string | null
}

interface JourneySourceState {
  active_source?: string | null
  last_success_source?: string | null
  updated_at?: string | null
}

interface TaxonomyUnknownShareResponse {
  total_touchpoints: number
  unknown_count: number
  unknown_share: number
  top_unmapped_patterns: Array<{
    source: string | null
    medium: string | null
    count: number
  }>
}

interface TaxonomyCoverageResponse {
  source_coverage: number
  medium_coverage: number
  channel_distribution?: Record<string, number>
  top_unmapped_patterns?: Array<{
    source?: string | null
    medium?: string | null
    count?: number
  }>
}

interface MappingCoverage {
  spend_mapped_pct: number
  value_mapped_pct: number
  spend_mapped: number
  spend_total: number
  value_mapped: number
  value_total: number
}

interface SummaryReadiness {
  status: string
  blockers: string[]
  warnings: string[]
  summary?: {
    primary_kpi_coverage?: number
    taxonomy_unknown_share?: number
    journeys_loaded?: number
    freshness_hours?: number | null
  }
  details?: {
    latest_event_replay?: {
      started_at?: string | null
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

interface ChannelSummaryResponse {
  mapping_coverage?: MappingCoverage | null
  readiness?: SummaryReadiness | null
  consistency_warnings?: string[]
  meta?: {
    conversion_key?: string | null
    conversion_key_resolution?: {
      configured_conversion_key?: string | null
      applied_conversion_key?: string | null
      reason?: string
    } | null
  }
}

interface CampaignSummaryResponse {
  mapping_coverage?: MappingCoverage | null
  spend_quality?: {
    status: string
    measured_spend: number
    allocated_spend: number
    allocated_share: number
  } | null
  readiness?: SummaryReadiness | null
  consistency_warnings?: string[]
}

interface ConversionPathsAnalysis {
  total_journeys: number
  source?: string | null
  journey_definition_id?: string | null
  date_from?: string | null
  date_to?: string | null
  direct_unknown_diagnostics?: {
    touchpoint_share: number
    journeys_ending_direct_share: number
  } | null
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

function formatNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return value.toLocaleString()
}

function formatSignedPercentPoints(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '—'
  const sign = value > 0 ? '+' : ''
  return `${sign}${(value * 100).toFixed(digits)} pp`
}

function formatTimestamp(value?: string | null): string {
  if (!value) return '—'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return parsed.toLocaleString()
}

function formatSourceLabel(value?: string | null): string {
  if (!value) return 'Unknown'
  if (value === 'meiro') return 'Meiro'
  if (value === 'upload') return 'Upload'
  if (value === 'sample') return 'Sample'
  return value
}

function mismatchLabel(liveCount: number | null, materializedCount: number | null): string {
  if (!liveCount || materializedCount == null) return 'Not enough data'
  const diff = materializedCount - liveCount
  if (diff === 0) return 'Aligned'
  const pct = Math.abs(diff) / Math.max(1, liveCount)
  if (pct < 0.05) return 'Minor mismatch'
  return diff > 0 ? 'Materialized exceeds live' : 'Materialized trails live'
}

export default function AttributionTrust({ model, configId }: AttributionTrustProps) {
  const { globalDateFrom, globalDateTo, journeysSummary } = useWorkspaceContext()
  const [selectedSegmentId, setSelectedSegmentId] = useState<string>(() => {
    if (typeof window === 'undefined') return ''
    try {
      const params = new URLSearchParams(window.location.search)
      return params.get('segment') || window.localStorage.getItem('attribution-trust:selected-segment') || ''
    } catch {
      return ''
    }
  })
  const [showMethodPanel, setShowMethodPanel] = usePersistentToggle('attribution-trust:show-method', false)
  const [showReplayPanel, setShowReplayPanel] = usePersistentToggle('attribution-trust:show-replay', false)
  const [showTaxonomyPanel, setShowTaxonomyPanel] = usePersistentToggle('attribution-trust:show-taxonomy', false)
  const [showReconcilePanel, setShowReconcilePanel] = usePersistentToggle('attribution-trust:show-reconcile', true)

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      if (selectedSegmentId) window.localStorage.setItem('attribution-trust:selected-segment', selectedSegmentId)
      else window.localStorage.removeItem('attribution-trust:selected-segment')
    } catch {
      // Ignore persistence failures and keep the selector usable.
    }
  }, [selectedSegmentId])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    if (selectedSegmentId) params.set('segment', selectedSegmentId)
    else params.delete('segment')
    const next = `${window.location.pathname}?${params.toString()}${window.location.hash || ''}`
    window.history.replaceState({}, '', next)
  }, [selectedSegmentId])

  const dateTo = globalDateTo || new Date().toISOString().slice(0, 10)
  const dateFrom =
    globalDateFrom ||
    new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)

  const sourceStateQuery = useQuery<JourneySourceState>({
    queryKey: ['attribution-trust', 'source-state'],
    queryFn: async () =>
      apiGetJson<JourneySourceState>('/api/attribution/journeys/source-state', {
        fallbackMessage: 'Failed to load journey source state',
      }),
  })

  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['attribution-trust', 'segment-registry'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load segment registry',
      }),
  })

  const channelSummaryQuery = useQuery<ChannelSummaryResponse>({
    queryKey: ['attribution-trust', 'channel-summary', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        timezone: 'UTC',
        compare: '0',
      })
      return apiGetJson<ChannelSummaryResponse>(`/api/performance/channel/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel trust diagnostics',
      })
    },
    enabled: !!dateFrom && !!dateTo,
  })

  const campaignSummaryQuery = useQuery<CampaignSummaryResponse>({
    queryKey: ['attribution-trust', 'campaign-summary', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        timezone: 'UTC',
        compare: '0',
      })
      return apiGetJson<CampaignSummaryResponse>(`/api/performance/campaign/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load campaign trust diagnostics',
      })
    },
    enabled: !!dateFrom && !!dateTo,
  })

  const taxonomyUnknownQuery = useQuery<TaxonomyUnknownShareResponse>({
    queryKey: ['attribution-trust', 'taxonomy-unknown'],
    queryFn: async () =>
      apiGetJson<TaxonomyUnknownShareResponse>('/api/taxonomy/unknown-share', {
        fallbackMessage: 'Failed to load taxonomy unknown share',
      }),
  })

  const taxonomyCoverageQuery = useQuery<TaxonomyCoverageResponse>({
    queryKey: ['attribution-trust', 'taxonomy-coverage'],
    queryFn: async () =>
      apiGetJson<TaxonomyCoverageResponse>('/api/taxonomy/coverage', {
        fallbackMessage: 'Failed to load taxonomy coverage',
      }),
  })

  const pathsAnalysisQuery = useQuery<ConversionPathsAnalysis>({
    queryKey: ['attribution-trust', 'paths-analysis', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        direct_mode: 'include',
        path_scope: 'converted',
      })
      return apiGetJson<ConversionPathsAnalysis>(`/api/conversion-paths/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to load conversion path diagnostics',
      })
    },
    enabled: !!dateFrom && !!dateTo,
  })

  const localSegments = useMemo(
    () => (segmentRegistryQuery.data?.items ?? []).filter(isLocalAnalyticalSegment),
    [segmentRegistryQuery.data?.items],
  )
  const compatibleSegments = useMemo(
    () => localSegments.filter((item) => localSegmentCompatibleWithDimensions(item, ['channel_group'])),
    [localSegments],
  )
  const selectedSegment = useMemo<SegmentRegistryItem | null>(
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
  const segmentAnalysisQuery = useQuery<SegmentAnalysisResponse>({
    queryKey: ['attribution-trust', 'segment-analysis', selectedSegment?.id || 'none', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({ date_from: dateFrom, date_to: dateTo })
      return apiGetJson<SegmentAnalysisResponse>(`/api/segments/local/${selectedSegment?.id}/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to load segment trust analysis',
      })
    },
    enabled: !!selectedSegment && !!dateFrom && !!dateTo,
  })

  const focusedChannelSummaryQuery = useQuery<ChannelSummaryResponse>({
    queryKey: ['attribution-trust', 'focused-channel-summary', dateFrom, dateTo, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        timezone: 'UTC',
        compare: '0',
      })
      if (selectedSegmentDefinition.channel_group) params.append('channels', selectedSegmentDefinition.channel_group)
      return apiGetJson<ChannelSummaryResponse>(`/api/performance/channel/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load focused channel trust diagnostics',
      })
    },
    enabled: !!selectedSegmentDefinition.channel_group && !!dateFrom && !!dateTo && selectedSegmentAutoCompatible,
  })

  const focusedCampaignSummaryQuery = useQuery<CampaignSummaryResponse>({
    queryKey: ['attribution-trust', 'focused-campaign-summary', dateFrom, dateTo, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        timezone: 'UTC',
        compare: '0',
      })
      if (selectedSegmentDefinition.channel_group) params.append('channels', selectedSegmentDefinition.channel_group)
      return apiGetJson<CampaignSummaryResponse>(`/api/performance/campaign/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load focused campaign trust diagnostics',
      })
    },
    enabled: !!selectedSegmentDefinition.channel_group && !!dateFrom && !!dateTo && selectedSegmentAutoCompatible,
  })

  const focusedPathsAnalysisQuery = useQuery<ConversionPathsAnalysis>({
    queryKey: ['attribution-trust', 'focused-paths-analysis', dateFrom, dateTo, selectedSegmentDefinition.channel_group || 'all'],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        direct_mode: 'include',
        path_scope: 'converted',
      })
      if (selectedSegmentDefinition.channel_group) params.set('channel_group', selectedSegmentDefinition.channel_group)
      return apiGetJson<ConversionPathsAnalysis>(`/api/conversion-paths/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to load focused path diagnostics',
      })
    },
    enabled: !!selectedSegmentDefinition.channel_group && !!dateFrom && !!dateTo && selectedSegmentAutoCompatible,
  })

  const readiness = journeysSummary?.readiness ?? channelSummaryQuery.data?.readiness ?? null
  const latestReplay = readiness?.details?.latest_event_replay ?? null
  const replayDiagnostics = latestReplay?.diagnostics ?? null
  const mappingCoverage = channelSummaryQuery.data?.mapping_coverage ?? campaignSummaryQuery.data?.mapping_coverage ?? null
  const spendQuality = campaignSummaryQuery.data?.spend_quality ?? null
  const conversionKeyResolution = channelSummaryQuery.data?.meta?.conversion_key_resolution ?? null
  const consistencyWarnings = useMemo(
    () =>
      [
        ...(journeysSummary?.consistency_warnings ?? []),
        ...(channelSummaryQuery.data?.consistency_warnings ?? []),
        ...(campaignSummaryQuery.data?.consistency_warnings ?? []),
      ].filter(Boolean),
    [journeysSummary?.consistency_warnings, channelSummaryQuery.data?.consistency_warnings, campaignSummaryQuery.data?.consistency_warnings],
  )

  const liveJourneys = journeysSummary?.count ?? null
  const materializedJourneys = pathsAnalysisQuery.data?.total_journeys ?? null
  const materializedGap =
    liveJourneys != null && materializedJourneys != null ? materializedJourneys - liveJourneys : null
  const pathDiagnostics = pathsAnalysisQuery.data?.direct_unknown_diagnostics ?? null
  const focusedMappingCoverage = focusedChannelSummaryQuery.data?.mapping_coverage ?? focusedCampaignSummaryQuery.data?.mapping_coverage ?? null
  const focusedSpendQuality = focusedCampaignSummaryQuery.data?.spend_quality ?? null
  const focusedPathDiagnostics = focusedPathsAnalysisQuery.data?.direct_unknown_diagnostics ?? null
  const focusedMaterializedJourneys = focusedPathsAnalysisQuery.data?.total_journeys ?? null
  const focusedTrustComparison = useMemo(() => {
    if (!selectedSegment) return null
    const segmentSummary = segmentAnalysisQuery.data?.summary
    const segmentRows = segmentSummary?.journey_rows ?? null
    return {
      shares: [
        {
          label: 'Path journey share',
          value: materializedJourneys && segmentRows != null ? segmentRows / materializedJourneys : null,
          note:
            segmentRows != null && materializedJourneys != null
              ? `${formatNumber(segmentRows)} matched rows vs ${formatNumber(materializedJourneys)} materialized path journeys`
              : 'Focused segment slice unavailable',
        },
        {
          label: 'Mapped value rate',
          value: focusedMappingCoverage?.value_mapped_pct != null ? focusedMappingCoverage.value_mapped_pct / 100 : null,
          note:
            mappingCoverage?.value_mapped_pct != null
              ? `${formatSignedPercentPoints(((focusedMappingCoverage?.value_mapped_pct ?? 0) - mappingCoverage.value_mapped_pct) / 100)} vs workspace`
              : segmentSummary?.revenue != null
              ? `Segment revenue ${formatNumber(segmentSummary.revenue)} in selected period`
              : 'Workspace mapping baseline unavailable',
        },
        {
          label: 'Mapped spend rate',
          value: focusedMappingCoverage?.spend_mapped_pct != null ? focusedMappingCoverage.spend_mapped_pct / 100 : null,
          note:
            mappingCoverage?.spend_mapped_pct != null
              ? `${formatSignedPercentPoints(((focusedMappingCoverage?.spend_mapped_pct ?? 0) - mappingCoverage.spend_mapped_pct) / 100)} vs workspace`
              : selectedSegmentAutoCompatible
              ? 'Workspace mapping baseline unavailable'
              : 'Spend mapping remains workspace-wide for advanced segments',
        },
        {
          label: 'Median lag',
          value: null,
          note:
            segmentSummary?.median_lag_days != null
              ? `${segmentSummary.median_lag_days}d median lag in segment`
              : 'Segment lag unavailable',
        },
      ],
      diagnostics: [
        {
          label: 'Direct / unknown touch share',
          focused: focusedPathDiagnostics?.touchpoint_share ?? null,
          baseline: pathDiagnostics?.touchpoint_share ?? null,
        },
        {
          label: 'Journeys ending direct',
          focused: focusedPathDiagnostics?.journeys_ending_direct_share ?? null,
          baseline: pathDiagnostics?.journeys_ending_direct_share ?? null,
        },
      ],
      spendQuality: focusedSpendQuality?.status || null,
    }
  }, [
    segmentAnalysisQuery.data?.summary,
    focusedMappingCoverage?.spend_mapped_pct,
    focusedMappingCoverage?.value_mapped_pct,
    focusedMaterializedJourneys,
    focusedPathDiagnostics?.journeys_ending_direct_share,
    focusedPathDiagnostics?.touchpoint_share,
    focusedSpendQuality?.status,
    mappingCoverage?.spend_mapped_pct,
    mappingCoverage?.value_mapped_pct,
    materializedJourneys,
    pathDiagnostics?.journeys_ending_direct_share,
    pathDiagnostics?.touchpoint_share,
    selectedSegment,
    selectedSegmentAutoCompatible,
  ])

  const summaryItems = [
    { label: 'Period', value: `${dateFrom} – ${dateTo}` },
    { label: 'Focus segment', value: selectedSegment?.name || 'Workspace baseline' },
    { label: 'Journey source', value: formatSourceLabel(sourceStateQuery.data?.active_source) },
    {
      label: 'Freshness',
      value:
        journeysSummary?.data_freshness_hours != null
          ? `${Math.round(Number(journeysSummary.data_freshness_hours))}h lag`
          : '—',
    },
    {
      label: 'KPI coverage',
      value: formatPercent(readiness?.summary?.primary_kpi_coverage),
      valueColor:
        (readiness?.summary?.primary_kpi_coverage ?? 0) < 0.7 ? t.color.warning : t.color.text,
    },
    {
      label: 'Unknown taxonomy',
      value:
        taxonomyUnknownQuery.data?.unknown_share != null
          ? formatPercent(taxonomyUnknownQuery.data.unknown_share)
          : '—',
      valueColor:
        (taxonomyUnknownQuery.data?.unknown_share ?? 0) > 0.2 ? t.color.warning : t.color.text,
    },
    {
      label: 'Live vs materialized',
      value:
        liveJourneys != null && materializedJourneys != null
          ? `${formatNumber(liveJourneys)} vs ${formatNumber(materializedJourneys)}`
          : '—',
      valueColor:
        materializedGap != null && Math.abs(materializedGap) > Math.max(25, (liveJourneys ?? 0) * 0.05)
          ? t.color.warning
          : t.color.text,
    },
  ]
  const trustNarrative = useMemo(() => {
    const gapPct =
      liveJourneys != null && materializedJourneys != null && liveJourneys > 0
        ? Math.abs(materializedJourneys - liveJourneys) / liveJourneys
        : null
    const headline =
      gapPct != null && gapPct >= 0.05
        ? `Live attribution and materialized path outputs are currently not fully aligned.`
        : 'Current attribution inputs look broadly aligned at the workspace level.'
    const items = [
      `Journey source is ${formatSourceLabel(sourceStateQuery.data?.active_source)} with ${
        journeysSummary?.data_freshness_hours != null
          ? `${Math.round(Number(journeysSummary.data_freshness_hours))}h lag`
          : 'freshness unavailable'
      }.`,
      mappingCoverage
        ? `Mapped value coverage is ${formatPercent(mappingCoverage.value_mapped_pct / 100)} and mapped spend coverage is ${formatPercent(mappingCoverage.spend_mapped_pct / 100)}.`
        : null,
      taxonomyUnknownQuery.data?.unknown_share != null
        ? `Unknown taxonomy share is ${formatPercent(taxonomyUnknownQuery.data.unknown_share)}${taxonomyUnknownQuery.data.unknown_share > 0.2 ? ', which is high enough to distort channel rollups.' : '.'}`
        : null,
      pathDiagnostics
        ? `Direct or unknown touchpoints make up ${formatPercent(pathDiagnostics.touchpoint_share)} of visible touches, and ${formatPercent(pathDiagnostics.journeys_ending_direct_share)} of journeys end direct.`
        : null,
      selectedSegment && focusedTrustComparison
        ? `${selectedSegment.name} represents ${formatPercent(focusedTrustComparison.shares[0]?.value)} of materialized path journeys in the visible trust slice.`
        : null,
    ].filter((item): item is string => Boolean(item))
    return { headline, items }
  }, [
    focusedTrustComparison,
    journeysSummary?.data_freshness_hours,
    mappingCoverage,
    materializedJourneys,
    pathDiagnostics,
    selectedSegment,
    sourceStateQuery.data?.active_source,
    taxonomyUnknownQuery.data?.unknown_share,
    liveJourneys,
  ])
  const comparisonHref = selectedSegmentId ? `/?page=comparison&segment=${encodeURIComponent(selectedSegmentId)}` : '/?page=comparison'
  const journeysHref = selectedSegmentId ? `/?page=journeys&segment=${encodeURIComponent(selectedSegmentId)}` : '/?page=journeys'

  const actionButtonStyle = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '8px 12px',
    borderRadius: t.radius.sm,
    border: `1px solid ${t.color.border}`,
    background: t.color.surface,
    color: t.color.text,
    fontSize: t.font.sizeSm,
    textDecoration: 'none',
  } satisfies React.CSSProperties

  const isLoading =
    sourceStateQuery.isLoading ||
    channelSummaryQuery.isLoading ||
    campaignSummaryQuery.isLoading ||
    taxonomyUnknownQuery.isLoading ||
    taxonomyCoverageQuery.isLoading ||
    pathsAnalysisQuery.isLoading

  const hasError =
    sourceStateQuery.isError ||
    channelSummaryQuery.isError ||
    campaignSummaryQuery.isError ||
    taxonomyUnknownQuery.isError ||
    taxonomyCoverageQuery.isError ||
    pathsAnalysisQuery.isError

  const errorMessage =
    (sourceStateQuery.error as Error | undefined)?.message ||
    (channelSummaryQuery.error as Error | undefined)?.message ||
    (campaignSummaryQuery.error as Error | undefined)?.message ||
    (taxonomyUnknownQuery.error as Error | undefined)?.message ||
    (taxonomyCoverageQuery.error as Error | undefined)?.message ||
    (pathsAnalysisQuery.error as Error | undefined)?.message ||
    null

  return (
    <DashboardPage
      title="Attribution Trust"
      description="One place to verify data source, freshness, exclusions, mapping quality, and count mismatches before acting on attribution outputs."
      actions={
        <>
          <AnalysisShareActions
            fileStem="attribution-trust"
            summaryTitle="Attribution trust brief"
            summaryLines={[
              `Period: ${dateFrom} – ${dateTo}`,
              `Focus segment: ${selectedSegment?.name || 'Workspace baseline'}`,
              `Journey source: ${formatSourceLabel(sourceStateQuery.data?.active_source)}`,
              `Freshness: ${journeysSummary?.data_freshness_hours != null ? `${Math.round(Number(journeysSummary.data_freshness_hours))}h lag` : 'unknown'}`,
              `KPI coverage: ${formatPercent(readiness?.summary?.primary_kpi_coverage)}`,
              `Unknown taxonomy share: ${formatPercent(taxonomyUnknownQuery.data?.unknown_share)}`,
              `Live vs materialized journeys: ${liveJourneys != null && materializedJourneys != null ? `${formatNumber(liveJourneys)} vs ${formatNumber(materializedJourneys)}` : 'unavailable'}`,
              `Direct / unknown touch share: ${formatPercent(pathDiagnostics?.touchpoint_share)}`,
            ]}
          />
          <a href={comparisonHref} style={actionButtonStyle}>
            Open model comparison
          </a>
          <a href={journeysHref} style={actionButtonStyle}>
            Open journey workspace
          </a>
          <a href="/?page=settings#settings/taxonomy" style={actionButtonStyle}>
            Review taxonomy
          </a>
        </>
      }
      isLoading={isLoading}
      isError={hasError}
      errorMessage={errorMessage}
      filters={
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Focus segment
            <select
              value={selectedSegmentId}
              onChange={(e) => setSelectedSegmentId(e.target.value)}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, minWidth: 240 }}
            >
              <option value="">Workspace baseline</option>
              {localSegments.map((segment) => (
                <option key={segment.id} value={segment.id}>
                  {segmentOptionLabel(segment)}
                </option>
              ))}
            </select>
          </label>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, maxWidth: 360 }}>
            Advanced saved segments now run as a real audience slice here. Source freshness and taxonomy unknown share remain workspace-wide diagnostics.
          </div>
        </div>
      }
    >
      <ContextSummaryStrip items={summaryItems} />

      <SegmentOverlapNotice selectedSegment={selectedSegment} />

      <AnalysisNarrativePanel
        title="What to trust"
        subtitle="A short readout of the current attribution data contract before you act on the diagnostics below."
        headline={trustNarrative.headline}
        items={trustNarrative.items}
      />

      {focusedTrustComparison ? (
        <SectionCard
          title={`Focused audience vs workspace: ${selectedSegment?.name}`}
          subtitle="This compares the selected audience's attributable and path diagnostics against the full workspace baseline. Source freshness and taxonomy coverage stay global."
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
            <SegmentComparisonContextNote
              mode={selectedSegmentAutoCompatible ? 'exact_filter' : 'analytical_lens'}
              pageLabel="trust diagnostics"
              basisLabel="matched journey-instance rows, with workspace-wide freshness and taxonomy diagnostics layered on top"
              primaryLabel={selectedSegment?.name || 'Selected audience'}
              primaryRows={segmentAnalysisQuery.data?.summary.journey_rows}
              baselineRows={segmentAnalysisQuery.data?.baseline_summary.journey_rows}
            />
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(min(190px, 100%), 1fr))',
                gap: t.space.md,
              }}
            >
              {focusedTrustComparison.shares.map((item) => (
                <div
                  key={item.label}
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.md,
                    background: t.color.bgSubtle,
                    padding: t.space.md,
                    display: 'grid',
                    gap: 4,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {formatPercent(item.value)}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{item.note}</div>
                </div>
              ))}
            </div>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))',
                gap: t.space.md,
              }}
            >
              {focusedTrustComparison.diagnostics.map((item) => {
                const delta =
                  item.focused != null && item.baseline != null ? item.focused - item.baseline : null
                const positive = (delta ?? 0) >= 0
                return (
                  <div
                    key={item.label}
                    style={{
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      background: t.color.surface,
                      padding: t.space.md,
                      display: 'grid',
                      gap: 4,
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{item.label}</div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      Segment <strong style={{ color: t.color.text }}>{formatPercent(item.focused)}</strong> · workspace{' '}
                      <strong style={{ color: t.color.text }}>{formatPercent(item.baseline)}</strong>
                    </div>
                    <div
                      style={{
                        fontSize: t.font.sizeSm,
                        fontWeight: t.font.weightSemibold,
                        color: delta == null ? t.color.textSecondary : positive ? t.color.warning : t.color.success,
                      }}
                    >
                      Δ {delta == null ? '—' : `${positive ? '+' : ''}${(delta * 100).toFixed(1)}pp`}
                    </div>
                  </div>
                )
              })}
            </div>
            {focusedTrustComparison.spendQuality ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Focused campaign spend quality: <strong style={{ color: t.color.text }}>{focusedTrustComparison.spendQuality.replace(/_/g, ' ')}</strong>
              </div>
            ) : null}
          </div>
        </SectionCard>
      ) : null}

      {readiness && (readiness.status === 'blocked' || readiness.warnings.length > 0 || consistencyWarnings.length > 0) ? (
        <DecisionStatusCard
          title="Attribution trust status"
          subtitle="This combines journeys readiness and current consistency warnings from the live attribution and performance layers."
          status={readiness.status}
          blockers={readiness.blockers}
          warnings={[...(readiness.warnings || []), ...consistencyWarnings].slice(0, 6)}
        />
      ) : null}

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(min(280px, 100%), 1fr))',
          gap: t.space.lg,
        }}
      >
        <SectionCard
          title="Input & freshness"
          subtitle="What source is active, how current it is, and what the last replay reconstructed."
        >
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Active source</span>
              <strong>{formatSourceLabel(sourceStateQuery.data?.active_source)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Last successful source</span>
              <strong>{formatSourceLabel(sourceStateQuery.data?.last_success_source)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Journey freshness</span>
              <strong>
                {journeysSummary?.data_freshness_hours != null
                  ? `${Math.round(Number(journeysSummary.data_freshness_hours))}h lag`
                  : '—'}
              </strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Last replay</span>
              <strong>{formatTimestamp(latestReplay?.started_at)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Events reconstructed</span>
              <strong>{formatNumber(replayDiagnostics?.events_loaded)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Journeys persisted</span>
              <strong>{formatNumber(replayDiagnostics?.journeys_persisted)}</strong>
            </div>
          </div>
        </SectionCard>

        <SectionCard
          title="Mapping & taxonomy"
          subtitle="How much traffic and value are mapped cleanly into channels and whether the taxonomy is keeping up."
        >
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Value mapped</span>
              <strong>{formatPercent((mappingCoverage?.value_mapped_pct ?? 0) / 100)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Spend mapped</span>
              <strong>{formatPercent((mappingCoverage?.spend_mapped_pct ?? 0) / 100)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Unknown source/medium share</span>
              <strong>{formatPercent(taxonomyUnknownQuery.data?.unknown_share)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Source coverage</span>
              <strong>{formatPercent(taxonomyCoverageQuery.data?.source_coverage)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Medium coverage</span>
              <strong>{formatPercent(taxonomyCoverageQuery.data?.medium_coverage)}</strong>
            </div>
            {spendQuality ? (
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
                <span style={{ color: t.color.textSecondary }}>Campaign spend quality</span>
                <strong>{spendQuality.status.replace(/_/g, ' ')}</strong>
              </div>
            ) : null}
          </div>
        </SectionCard>

        <SectionCard
          title="Reconciliation"
          subtitle="Where live journey counts and materialized path outputs line up, and where direct traffic still dominates."
        >
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Live journeys</span>
              <strong>{formatNumber(liveJourneys)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Materialized path journeys</span>
              <strong>{formatNumber(materializedJourneys)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Count relationship</span>
              <strong>{mismatchLabel(liveJourneys, materializedJourneys)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Direct/unknown touch share</span>
              <strong>{formatPercent(pathDiagnostics?.touchpoint_share)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Journeys ending direct</span>
              <strong>{formatPercent(pathDiagnostics?.journeys_ending_direct_share)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Path source</span>
              <strong>{pathsAnalysisQuery.data?.source || '—'}</strong>
            </div>
          </div>
        </SectionCard>

        <SectionCard
          title="Config & KPI application"
          subtitle="Which model/config context you are viewing and how the KPI resolved in the current summaries."
        >
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Selected model</span>
              <strong>{model}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Selected config</span>
              <strong>{configId || 'Default active'}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Workspace KPI</span>
              <strong>{journeysSummary?.primary_kpi_label || 'Primary KPI'}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
              <span style={{ color: t.color.textSecondary }}>Applied conversion key</span>
              <strong>{conversionKeyResolution?.applied_conversion_key || channelSummaryQuery.data?.meta?.conversion_key || '—'}</strong>
            </div>
            {conversionKeyResolution?.reason ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Resolution reason: <strong style={{ color: t.color.text }}>{conversionKeyResolution.reason}</strong>
              </div>
            ) : null}
          </div>
        </SectionCard>
      </div>

      <CollapsiblePanel
        title="Method & context"
        subtitle="How to interpret this workspace and why numbers can disagree across pages."
        open={showMethodPanel}
        onToggle={() => setShowMethodPanel((prev) => !prev)}
      >
        <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          <div>
            Live attribution counts come from the active journey source and current attribution ingestion pipeline. Conversion Paths uses materialized journey-definition outputs, so it can legitimately lag live counts when definitions have not been rebuilt for the latest day.
          </div>
          <div>
            Mapping coverage and taxonomy coverage answer different questions: mapping coverage is how much spend or value can be assigned cleanly into modeled entities, while taxonomy coverage is how much source/medium traffic can be classified into channels at all.
          </div>
          <div>
            Direct and unknown shares are not always errors, but high values reduce confidence in path interpretation and make model deltas harder to explain to stakeholders.
          </div>
        </div>
      </CollapsiblePanel>

      <CollapsiblePanel
        title="Latest replay diagnostics"
        subtitle="Details from the most recent event replay used by journeys readiness."
        open={showReplayPanel}
        onToggle={() => setShowReplayPanel((prev) => !prev)}
      >
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
            gap: t.space.md,
          }}
        >
          {[
            ['Events loaded', replayDiagnostics?.events_loaded],
            ['Profiles reconstructed', replayDiagnostics?.profiles_reconstructed],
            ['Touchpoints reconstructed', replayDiagnostics?.touchpoints_reconstructed],
            ['Conversions reconstructed', replayDiagnostics?.conversions_reconstructed],
            ['Attributable profiles', replayDiagnostics?.attributable_profiles],
            ['Journeys persisted', replayDiagnostics?.journeys_persisted],
          ].map(([label, value]) => (
            <div
              key={String(label)}
              style={{
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: t.space.md,
                background: t.color.surface,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>
                {label}
              </div>
              <div style={{ marginTop: 4, fontSize: t.font.sizeLg, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                {formatNumber(typeof value === 'number' ? value : null)}
              </div>
            </div>
          ))}
        </div>
        {replayDiagnostics?.warnings?.length ? (
          <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.xs }}>
            {replayDiagnostics.warnings.map((warning) => (
              <div key={warning} style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>
                {warning}
              </div>
            ))}
          </div>
        ) : null}
      </CollapsiblePanel>

      <CollapsiblePanel
        title="Taxonomy & mapping detail"
        subtitle="The most common unresolved source/medium combinations and what remains unmapped."
        open={showTaxonomyPanel}
        onToggle={() => setShowTaxonomyPanel((prev) => !prev)}
      >
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(min(280px, 100%), 1fr))',
            gap: t.space.lg,
          }}
        >
          <SectionCard title="Top unmapped source / medium pairs" subtitle="Patterns currently contributing to unknown channel share.">
            <div style={{ display: 'grid', gap: t.space.sm }}>
              {(taxonomyUnknownQuery.data?.top_unmapped_patterns ?? []).slice(0, 8).map((pattern, idx) => (
                <div
                  key={`${pattern.source || 'unknown'}-${pattern.medium || 'unknown'}-${idx}`}
                  style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}
                >
                  <span style={{ color: t.color.textSecondary }}>
                    {(pattern.source || 'unknown')} / {(pattern.medium || 'unknown')}
                  </span>
                  <strong>{formatNumber(pattern.count)}</strong>
                </div>
              ))}
              {!taxonomyUnknownQuery.data?.top_unmapped_patterns?.length ? (
                <div style={{ color: t.color.textMuted, fontSize: t.font.sizeSm }}>No unmapped patterns returned.</div>
              ) : null}
            </div>
          </SectionCard>

          <SectionCard title="Coverage notes" subtitle="How mapped classification differs from reporting completeness.">
            <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <div>
                Value mapped uses attribution outputs and campaign/channel joins. It can be high even when taxonomy unknown share is still elevated if revenue is concentrated in well-mapped traffic.
              </div>
              <div>
                Spend mapped is sensitive to campaign and channel naming quality. Mixed or allocated-only campaign spend means performance rankings can still be directionally useful, but campaign spend bars deserve caution.
              </div>
              {spendQuality ? (
                <div>
                  Current spend quality is <strong style={{ color: t.color.text }}>{spendQuality.status.replace(/_/g, ' ')}</strong>
                  {spendQuality.allocated_share ? ` with ${formatPercent(spendQuality.allocated_share)} allocated spend share.` : '.'}
                </div>
              ) : null}
            </div>
          </SectionCard>
        </div>
      </CollapsiblePanel>

      <CollapsiblePanel
        title="Reconciliation detail"
        subtitle="Where the live attribution layer and materialized journey outputs diverge, and what that means."
        open={showReconcilePanel}
        onToggle={() => setShowReconcilePanel((prev) => !prev)}
      >
        <div style={{ display: 'grid', gap: t.space.md, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          <div>
            Live journeys: <strong style={{ color: t.color.text }}>{formatNumber(liveJourneys)}</strong>. Materialized path journeys:{' '}
            <strong style={{ color: t.color.text }}>{formatNumber(materializedJourneys)}</strong>.
            {materializedGap != null ? ` Gap: ${materializedGap > 0 ? '+' : ''}${formatNumber(materializedGap)} journeys.` : ''}
          </div>
          <div>
            Materialized paths are sourced from <strong style={{ color: t.color.text }}>{pathsAnalysisQuery.data?.source || 'unknown source'}</strong>
            {pathsAnalysisQuery.data?.date_to ? ` and currently cover outputs through ${pathsAnalysisQuery.data.date_to.slice(0, 10)}.` : '.'}
          </div>
          <div>
            When materialized outputs trail live journeys, users should trust live totals for current ingestion status and trust Conversion Paths for structural analysis only within the rebuilt definition window.
          </div>
          {pathDiagnostics ? (
            <div>
              Direct / unknown touches are <strong style={{ color: t.color.text }}>{formatPercent(pathDiagnostics.touchpoint_share)}</strong> of visible touchpoints and{' '}
              <strong style={{ color: t.color.text }}>{formatPercent(pathDiagnostics.journeys_ending_direct_share)}</strong> of converted journeys end direct.
            </div>
          ) : null}
        </div>
      </CollapsiblePanel>
    </DashboardPage>
  )
}
