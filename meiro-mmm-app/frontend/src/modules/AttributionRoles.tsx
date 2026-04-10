import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { DashboardPage, ContextSummaryStrip, SectionCard, AnalysisShareActions, AnalysisNarrativePanel } from '../components/dashboard'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import SegmentComparisonContextNote from '../components/segments/SegmentComparisonContextNote'
import SegmentOverlapNotice from '../components/segments/SegmentOverlapNotice'
import { apiGetJson } from '../lib/apiClient'
import { buildIncrementalityPlannerHref } from '../lib/experimentLinks'
import { buildSettingsHref } from '../lib/settingsLinks'
import {
  buildSegmentComparisonHref,
  localSegmentCompatibleWithDimensions,
  readLocalSegmentDefinition,
  segmentOptionLabel,
  type SegmentAnalysisResponse,
  type SegmentComparisonResponse,
  type SegmentRegistryItem,
  type SegmentRegistryResponse,
} from '../lib/segments'
import { tokens as t } from '../theme/tokens'

interface AttributionRolesProps {
  model: string
  configId?: string | null
}

interface RoleDiagnostics {
  first_touch_conversions?: number
  last_touch_conversions?: number
  assist_conversions?: number
  first_touch_revenue?: number
  last_touch_revenue?: number
  assist_revenue?: number
}

interface ChannelSummaryItem {
  channel: string
  current: { spend: number; visits: number; conversions: number; revenue: number }
  diagnostics?: { roles?: RoleDiagnostics }
}

interface ChannelSummaryResponse {
  current_period: { date_from: string; date_to: string; grain?: string }
  items: ChannelSummaryItem[]
  config?: {
    conversion_key?: string | null
    config_version?: number | null
  } | null
}

interface CampaignSummaryItem {
  campaign_id: string
  campaign_name?: string | null
  channel: string
  current: { spend: number; visits: number; conversions: number; revenue: number }
  diagnostics?: { roles?: RoleDiagnostics }
}

interface CampaignSummaryResponse {
  current_period: { date_from: string; date_to: string; grain?: string }
  items: CampaignSummaryItem[]
  config?: {
    conversion_key?: string | null
    config_version?: number | null
  } | null
}

type ScopeKey = 'channels' | 'campaigns'
type MetricKey = 'conversions' | 'revenue'
type RoleKey = 'first' | 'assist' | 'last'

type RoleEntity = {
  id: string
  label: string
  secondaryLabel: string
  firstConversions: number
  assistConversions: number
  lastConversions: number
  firstRevenue: number
  assistRevenue: number
  lastRevenue: number
  visits: number
  conversions: number
  revenue: number
  spend: number
}

const ROLE_LABELS: Record<RoleKey, string> = {
  first: 'Introducer',
  assist: 'Assister',
  last: 'Closer',
}

const ROLE_COLORS: Record<RoleKey, string> = {
  first: '#2563eb',
  assist: '#7c3aed',
  last: '#059669',
}

function formatCurrency(value: number): string {
  if (!Number.isFinite(value)) return '$0'
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`
  if (Math.abs(value) >= 1_000) return `$${(value / 1_000).toFixed(1)}K`
  return `$${value.toFixed(0)}`
}

function formatNumber(value: number): string {
  if (!Number.isFinite(value)) return '0'
  return value.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

function readRoleValue(item: RoleEntity, role: RoleKey, metric: MetricKey): number {
  if (metric === 'conversions') {
    if (role === 'first') return item.firstConversions
    if (role === 'assist') return item.assistConversions
    return item.lastConversions
  }
  if (role === 'first') return item.firstRevenue
  if (role === 'assist') return item.assistRevenue
  return item.lastRevenue
}

function dominantRole(item: RoleEntity, metric: MetricKey): RoleKey {
  const values: Array<{ role: RoleKey; value: number }> = [
    { role: 'first', value: readRoleValue(item, 'first', metric) },
    { role: 'assist', value: readRoleValue(item, 'assist', metric) },
    { role: 'last', value: readRoleValue(item, 'last', metric) },
  ]
  values.sort((a, b) => b.value - a.value)
  return values[0]?.role ?? 'assist'
}

function roleDescriptor(role: RoleKey): string {
  if (role === 'first') return 'good at starting demand'
  if (role === 'assist') return 'good at keeping journeys alive'
  return 'good at converting demand'
}

export default function AttributionRoles({ model, configId }: AttributionRolesProps) {
  const { globalDateFrom, globalDateTo, journeysSummary } = useWorkspaceContext()
  const [scope, setScope] = useState<ScopeKey>('channels')
  const [metric, setMetric] = useState<MetricKey>('conversions')
  const [focusRole, setFocusRole] = useState<RoleKey>('assist')
  const [selectedSegmentId, setSelectedSegmentId] = useState(() => {
    if (typeof window === 'undefined') return ''
    try {
      return new URLSearchParams(window.location.search).get('segment') || ''
    } catch {
      return ''
    }
  })
  const [compareSegmentId, setCompareSegmentId] = useState(() => {
    if (typeof window === 'undefined') return ''
    try {
      return new URLSearchParams(window.location.search).get('compare_segment') || ''
    } catch {
      return ''
    }
  })
  const [showMethod, setShowMethod] = usePersistentToggle('attribution-roles:show-method', false)
  const [showTable, setShowTable] = usePersistentToggle('attribution-roles:show-table', true)

  const dateTo = globalDateTo || new Date().toISOString().slice(0, 10)
  const dateFrom =
    globalDateFrom ||
    new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)

  const channelSummaryQuery = useQuery<ChannelSummaryResponse>({
    queryKey: ['attribution-roles', 'channels', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        timezone: 'UTC',
        compare: '0',
      })
      return apiGetJson<ChannelSummaryResponse>(`/api/performance/channel/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load channel role summary',
      })
    },
    enabled: !!dateFrom && !!dateTo,
  })

  const campaignSummaryQuery = useQuery<CampaignSummaryResponse>({
    queryKey: ['attribution-roles', 'campaigns', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to: dateTo,
        timezone: 'UTC',
        compare: '0',
      })
      return apiGetJson<CampaignSummaryResponse>(`/api/performance/campaign/summary?${params.toString()}`, {
        fallbackMessage: 'Failed to load campaign role summary',
      })
    },
    enabled: !!dateFrom && !!dateTo,
  })

  const segmentRegistryQuery = useQuery<SegmentRegistryResponse>({
    queryKey: ['attribution-roles', 'segments'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load segment registry',
      }),
  })

  const channelEntities = useMemo<RoleEntity[]>(() => {
    return (channelSummaryQuery.data?.items ?? []).map((item) => {
      const roles = item.diagnostics?.roles || {}
      return {
        id: item.channel,
        label: item.channel,
        secondaryLabel: 'Channel',
        firstConversions: roles.first_touch_conversions || 0,
        assistConversions: roles.assist_conversions || 0,
        lastConversions: roles.last_touch_conversions || 0,
        firstRevenue: roles.first_touch_revenue || 0,
        assistRevenue: roles.assist_revenue || 0,
        lastRevenue: roles.last_touch_revenue || 0,
        visits: item.current.visits || 0,
        conversions: item.current.conversions || 0,
        revenue: item.current.revenue || 0,
        spend: item.current.spend || 0,
      }
    })
  }, [channelSummaryQuery.data?.items])

  const campaignEntities = useMemo<RoleEntity[]>(() => {
    return (campaignSummaryQuery.data?.items ?? []).map((item) => {
      const roles = item.diagnostics?.roles || {}
      const label = item.campaign_name || item.campaign_id || 'Unknown campaign'
      return {
        id: item.campaign_id,
        label,
        secondaryLabel: item.channel,
        firstConversions: roles.first_touch_conversions || 0,
        assistConversions: roles.assist_conversions || 0,
        lastConversions: roles.last_touch_conversions || 0,
        firstRevenue: roles.first_touch_revenue || 0,
        assistRevenue: roles.assist_revenue || 0,
        lastRevenue: roles.last_touch_revenue || 0,
        visits: item.current.visits || 0,
        conversions: item.current.conversions || 0,
        revenue: item.current.revenue || 0,
        spend: item.current.spend || 0,
      }
    })
  }, [campaignSummaryQuery.data?.items])

  const localSegments = useMemo(
    () => (segmentRegistryQuery.data?.items ?? []).filter((item) => item.source === 'local_analytical'),
    [segmentRegistryQuery.data?.items],
  )
  const compatibleSegments = useMemo(
    () =>
      localSegments.filter((item) =>
        localSegmentCompatibleWithDimensions(item, scope === 'channels' ? ['channel_group'] : ['channel_group', 'campaign_id']),
      ),
    [localSegments, scope],
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
    () => localSegmentCompatibleWithDimensions(selectedSegment, scope === 'channels' ? ['channel_group'] : ['channel_group', 'campaign_id']),
    [scope, selectedSegment],
  )
  const compareSegment = useMemo<SegmentRegistryItem | null>(
    () => localSegments.find((item) => item.id === compareSegmentId) ?? null,
    [compareSegmentId, localSegments],
  )
  const segmentAnalysisQuery = useQuery<SegmentAnalysisResponse>({
    queryKey: ['attribution-roles', 'segment-analysis', selectedSegment?.id || 'none', dateFrom, dateTo],
    queryFn: async () => {
      const params = new URLSearchParams({ date_from: dateFrom, date_to: dateTo })
      return apiGetJson<SegmentAnalysisResponse>(`/api/segments/local/${selectedSegment?.id}/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to load advanced segment analysis',
      })
    },
    enabled: !!selectedSegment && !!dateFrom && !!dateTo,
  })
  const segmentCompareQuery = useQuery<SegmentComparisonResponse>({
    queryKey: ['attribution-roles', 'segment-compare', selectedSegment?.id || 'none', compareSegment?.id || 'none'],
    queryFn: async () =>
      apiGetJson<SegmentComparisonResponse>(`/api/segments/local/${selectedSegment?.id}/compare?other_segment_id=${encodeURIComponent(compareSegment?.id || '')}`, {
        fallbackMessage: 'Failed to compare saved analytical audiences',
      }),
    enabled: Boolean(selectedSegment?.id && compareSegment?.id && selectedSegment?.id !== compareSegment?.id),
  })

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    params.set('scope', scope)
    params.set('metric', metric)
    params.set('role', focusRole)
    if (selectedSegmentId) params.set('segment', selectedSegmentId)
    else params.delete('segment')
    if (compareSegmentId) params.set('compare_segment', compareSegmentId)
    else params.delete('compare_segment')
    const next = `${window.location.pathname}?${params.toString()}${window.location.hash || ''}`
    window.history.replaceState({}, '', next)
  }, [compareSegmentId, focusRole, metric, scope, selectedSegmentId])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    const nextScope = params.get('scope')
    const nextMetric = params.get('metric')
    const nextRole = params.get('role')
    if (nextScope === 'channels' || nextScope === 'campaigns') setScope(nextScope)
    if (nextMetric === 'conversions' || nextMetric === 'revenue') setMetric(nextMetric)
    if (nextRole === 'first' || nextRole === 'assist' || nextRole === 'last') setFocusRole(nextRole)
    const nextCompareSegment = params.get('compare_segment')
    if (nextCompareSegment) setCompareSegmentId(nextCompareSegment)
  }, [])

  useEffect(() => {
    if (!compareSegmentId) return
    if (localSegments.some((item) => item.id === compareSegmentId && item.id !== selectedSegmentId)) return
    setCompareSegmentId('')
  }, [compareSegmentId, localSegments, selectedSegmentId])

  const entities = scope === 'channels' ? channelEntities : campaignEntities
  const advancedSegmentEntities = useMemo<RoleEntity[]>(() => {
    const items = (scope === 'channels'
      ? segmentAnalysisQuery.data?.role_entities.channels
      : segmentAnalysisQuery.data?.role_entities.campaigns) ?? []
    return items.map((item) => ({
      id: String(item.id || ''),
      label: String(item.label || item.id || 'Unknown'),
      secondaryLabel: String(item.secondaryLabel || ''),
      firstConversions: Number(item.firstConversions || 0),
      assistConversions: Number(item.assistConversions || 0),
      lastConversions: Number(item.lastConversions || 0),
      firstRevenue: Number(item.firstRevenue || 0),
      assistRevenue: Number(item.assistRevenue || 0),
      lastRevenue: Number(item.lastRevenue || 0),
      visits: 0,
      conversions:
        Number(item.firstConversions || 0) + Number(item.assistConversions || 0) + Number(item.lastConversions || 0),
      revenue:
        Number(item.firstRevenue || 0) + Number(item.assistRevenue || 0) + Number(item.lastRevenue || 0),
      spend: 0,
    }))
  }, [scope, segmentAnalysisQuery.data?.role_entities])
  const visibleEntities = useMemo(() => {
    if (!selectedSegment) return entities
    if (!selectedSegmentAutoCompatible) return advancedSegmentEntities
    return entities.filter((item) => {
      if (scope === 'channels') {
        if (selectedSegmentDefinition.channel_group && item.id !== selectedSegmentDefinition.channel_group) return false
        return true
      }
      if (selectedSegmentDefinition.channel_group && item.secondaryLabel !== selectedSegmentDefinition.channel_group) return false
      if (selectedSegmentDefinition.campaign_id && item.id !== selectedSegmentDefinition.campaign_id) return false
      return true
    })
  }, [advancedSegmentEntities, entities, scope, selectedSegment, selectedSegmentAutoCompatible, selectedSegmentDefinition])
  const activeQuery = scope === 'channels' ? channelSummaryQuery : campaignSummaryQuery
  const conversionKey =
    channelSummaryQuery.data?.config?.conversion_key ||
    campaignSummaryQuery.data?.config?.conversion_key ||
    journeysSummary?.primary_kpi_label ||
    'Primary KPI'

  const roleTotals = useMemo(() => {
    return {
      first: visibleEntities.reduce((sum, item) => sum + readRoleValue(item, 'first', metric), 0),
      assist: visibleEntities.reduce((sum, item) => sum + readRoleValue(item, 'assist', metric), 0),
      last: visibleEntities.reduce((sum, item) => sum + readRoleValue(item, 'last', metric), 0),
    }
  }, [visibleEntities, metric])

  const totalRoleValue = roleTotals.first + roleTotals.assist + roleTotals.last
  const totalObservedConversions = visibleEntities.reduce((sum, item) => sum + item.conversions, 0)
  const totalObservedRevenue = visibleEntities.reduce((sum, item) => sum + item.revenue, 0)

  const topByRole = useMemo(() => {
    const pick = (role: RoleKey) =>
      [...visibleEntities]
        .sort((a, b) => readRoleValue(b, role, metric) - readRoleValue(a, role, metric))
        .slice(0, 5)
    return {
      first: pick('first'),
      assist: pick('assist'),
      last: pick('last'),
    }
  }, [visibleEntities, metric])

  const rankedEntities = useMemo(() => {
    return [...visibleEntities]
      .sort((a, b) => readRoleValue(b, focusRole, metric) - readRoleValue(a, focusRole, metric))
      .slice(0, 12)
  }, [visibleEntities, focusRole, metric])

  const concentration = useMemo(() => {
    const build = (role: RoleKey) => {
      const top3 = topByRole[role].slice(0, 3).reduce((sum, item) => sum + readRoleValue(item, role, metric), 0)
      const total = roleTotals[role]
      return total > 0 ? top3 / total : null
    }
    return {
      first: build('first'),
      assist: build('assist'),
      last: build('last'),
    }
  }, [metric, roleTotals, topByRole])

  const topFocusedEntity = rankedEntities[0] ?? null
  const baselineRoleTotals = useMemo(
    () => ({
      first: entities.reduce((sum, item) => sum + readRoleValue(item, 'first', metric), 0),
      assist: entities.reduce((sum, item) => sum + readRoleValue(item, 'assist', metric), 0),
      last: entities.reduce((sum, item) => sum + readRoleValue(item, 'last', metric), 0),
    }),
    [entities, metric],
  )
  const focusedRoleTotal = roleTotals.first + roleTotals.assist + roleTotals.last
  const baselineRoleTotal = baselineRoleTotals.first + baselineRoleTotals.assist + baselineRoleTotals.last
  const segmentComparison = useMemo(() => {
    if (!selectedSegment || focusedRoleTotal <= 0 || baselineRoleTotal <= 0) return null
    const focusedMetricTotal = metric === 'conversions' ? totalObservedConversions : totalObservedRevenue
    const baselineMetricTotal =
      metric === 'conversions'
        ? entities.reduce((sum, item) => sum + item.conversions, 0)
        : entities.reduce((sum, item) => sum + item.revenue, 0)
    return {
      shares: [
        { label: 'Role-volume share', value: focusedRoleTotal / baselineRoleTotal },
        {
          label: metric === 'conversions' ? 'Conversion share' : 'Revenue share',
          value: baselineMetricTotal > 0 ? focusedMetricTotal / baselineMetricTotal : null,
        },
        { label: 'Entity coverage share', value: entities.length > 0 ? visibleEntities.length / entities.length : null },
      ],
      roles: (['first', 'assist', 'last'] as RoleKey[]).map((role) => {
        const focusedShare = focusedRoleTotal > 0 ? roleTotals[role] / focusedRoleTotal : null
        const baselineShare = baselineRoleTotal > 0 ? baselineRoleTotals[role] / baselineRoleTotal : null
        return {
          label: ROLE_LABELS[role],
          focused: focusedShare,
          baseline: baselineShare,
          delta: focusedShare != null && baselineShare != null ? focusedShare - baselineShare : null,
        }
      }),
    }
  }, [
    baselineRoleTotal,
    baselineRoleTotals,
    entities,
    focusedRoleTotal,
    metric,
    roleTotals,
    selectedSegment,
    totalObservedConversions,
    totalObservedRevenue,
    visibleEntities.length,
  ])
  const summaryItems = [
    { label: 'Period', value: `${dateFrom} – ${dateTo}` },
    { label: 'Scope', value: scope === 'channels' ? 'Channels' : 'Campaigns' },
    { label: 'Role metric', value: metric === 'conversions' ? 'Conversions' : 'Revenue' },
    { label: 'KPI', value: String(conversionKey || 'Primary KPI') },
    { label: 'Model context', value: model.replace(/_/g, ' ') },
    { label: 'Config context', value: configId ? `${configId.slice(0, 8)}…` : 'Default active' },
    { label: 'Focus segment', value: selectedSegment?.name || 'Workspace baseline' },
    { label: 'Journeys loaded', value: journeysSummary?.count?.toLocaleString() ?? '—' },
  ]
  const compareConversionsDelta = segmentCompareQuery.data?.deltas.conversions
  const roleVolumeShare =
    segmentComparison?.shares.find((item) => item.label === 'Role-volume share')?.value ?? null
  const rolesNarrative = useMemo(() => {
    const dominant = topFocusedEntity ? dominantRole(topFocusedEntity, metric) : null
    const largestDelta = segmentComparison
      ? [...segmentComparison.roles]
          .filter((item) => item.delta != null)
          .sort((a, b) => Math.abs(Number(b.delta || 0)) - Math.abs(Number(a.delta || 0)))[0] ?? null
      : null
    const focusConcentration = concentration[focusRole]
    const headline = topFocusedEntity
      ? `${topFocusedEntity.label} currently leads the ${ROLE_LABELS[focusRole].toLowerCase()} view.`
      : `No ${ROLE_LABELS[focusRole].toLowerCase()} leader is visible in the current slice.`
    const items = [
      topFocusedEntity && dominant
        ? `${topFocusedEntity.label} is primarily acting as a ${ROLE_LABELS[dominant].toLowerCase()} and is ${roleDescriptor(dominant)}.`
        : null,
      focusConcentration != null
        ? `The top 3 ${ROLE_LABELS[focusRole].toLowerCase()}s hold ${formatPercent(focusConcentration)} of visible ${metric}, so this role is ${focusConcentration > 0.6 ? 'highly concentrated' : 'fairly distributed'}.`
        : null,
      largestDelta && selectedSegment
        ? `${selectedSegment.name} over-indexes most on ${largestDelta.label.toLowerCase()} behavior, shifting role mix by ${largestDelta.delta == null ? '—' : `${largestDelta.delta >= 0 ? '+' : ''}${(largestDelta.delta * 100).toFixed(1)}pp`} vs workspace.`
        : null,
      selectedSegment && compareSegment && segmentCompareQuery.data
        ? `${selectedSegment.name} vs ${compareSegment.name}: ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity. ${metric === 'conversions' ? 'Conversion' : 'Revenue'} delta is ${metric === 'conversions' ? compareConversionsDelta == null ? '—' : `${compareConversionsDelta >= 0 ? '+' : ''}${formatNumber(Math.abs(compareConversionsDelta))}` : segmentCompareQuery.data.deltas.revenue == null ? '—' : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : ''}${formatCurrency(Math.abs(segmentCompareQuery.data.deltas.revenue))}`}.`
        : null,
      totalRoleValue > 0
        ? `Visible role-accounted ${metric} totals ${metric === 'conversions' ? formatNumber(totalRoleValue) : formatCurrency(totalRoleValue)} across ${visibleEntities.length.toLocaleString()} ${scope}.`
        : null,
    ].filter((item): item is string => Boolean(item))
    return { headline, items }
  }, [
    concentration,
    compareSegment,
    compareConversionsDelta,
    focusRole,
    metric,
    scope,
    segmentCompareQuery.data,
    segmentComparison,
    selectedSegment,
    topFocusedEntity,
    totalRoleValue,
    visibleEntities.length,
  ])
  const comparisonHref = selectedSegmentId ? `/?page=comparison&segment=${encodeURIComponent(selectedSegmentId)}` : '/?page=comparison'
  const trustHref = selectedSegmentId ? `/?page=trust&segment=${encodeURIComponent(selectedSegmentId)}` : '/?page=trust'
  const journeysHref = selectedSegmentId ? `/?page=journeys&segment=${encodeURIComponent(selectedSegmentId)}` : '/?page=journeys'
  const actionButtonStyle: React.CSSProperties = {
    padding: `${t.space.sm}px ${t.space.md}px`,
    borderRadius: t.radius.sm,
    border: `1px solid ${t.color.border}`,
    background: t.color.surface,
    color: t.color.text,
    fontSize: t.font.sizeSm,
    fontWeight: t.font.weightMedium,
    cursor: 'pointer',
    textDecoration: 'none',
  }

  const isLoading = activeQuery.isLoading
  const isError = activeQuery.isError
  const errorMessage = (activeQuery.error as Error | undefined)?.message || null

  return (
    <DashboardPage
      title="Attribution Roles"
      description="Who starts demand, who assists it, and who closes it."
      isLoading={isLoading}
      isError={isError}
      errorMessage={errorMessage}
      isEmpty={!isLoading && !isError && entities.length === 0}
    >
      <div style={{ display: 'grid', gap: t.space.xl }}>
        <SectionCard
          title="Analysis controls"
          subtitle="Choose the role view, audience slice, and handoff actions before reviewing introducers, assisters, and closers."
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
            <div
              style={{
                display: 'grid',
                gap: t.space.md,
                gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
                alignItems: 'end',
              }}
            >
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                Scope
                <select
                  value={scope}
                  onChange={(e) => setScope(e.target.value as ScopeKey)}
                  style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                >
                  <option value="channels">Channels</option>
                  <option value="campaigns">Campaigns</option>
                </select>
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                Metric
                <select
                  value={metric}
                  onChange={(e) => setMetric(e.target.value as MetricKey)}
                  style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                >
                  <option value="conversions">Conversions</option>
                  <option value="revenue">Revenue</option>
                </select>
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                Ranked by
                <select
                  value={focusRole}
                  onChange={(e) => setFocusRole(e.target.value as RoleKey)}
                  style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                >
                  <option value="first">Introducer</option>
                  <option value="assist">Assister</option>
                  <option value="last">Closer</option>
                </select>
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                Focus segment
                <select
                  value={selectedSegmentId}
                  onChange={(e) => setSelectedSegmentId(e.target.value)}
                  style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, minWidth: 0 }}
                >
                  <option value="">Workspace baseline</option>
                  {localSegments.map((segment) => (
                    <option key={segment.id} value={segment.id}>
                      {segmentOptionLabel(segment)}
                    </option>
                  ))}
                </select>
              </label>
              {selectedSegment ? (
                <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                  Compare with
                  <select
                    value={compareSegmentId}
                    onChange={(e) => setCompareSegmentId(e.target.value)}
                    style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, minWidth: 0 }}
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
                </label>
              ) : null}
            </div>

            <div
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: t.space.sm,
                alignItems: 'center',
                justifyContent: 'space-between',
              }}
            >
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
                <a href={buildSettingsHref('segments')} style={{ color: t.color.accent, textDecoration: 'none', fontSize: t.font.sizeSm }}>
                  Manage segments
                </a>
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center', justifyContent: 'flex-end' }}>
                <AnalysisShareActions
                  fileStem="attribution-roles"
                  summaryTitle="Attribution roles brief"
                  summaryLines={[
                    `Period: ${dateFrom} – ${dateTo}`,
                    `Scope: ${scope === 'channels' ? 'Channels' : 'Campaigns'}`,
                    `Metric: ${metric === 'conversions' ? 'Conversions' : 'Revenue'}`,
                    `Ranked by: ${ROLE_LABELS[focusRole]}`,
                    `Focus segment: ${selectedSegment?.name || 'Workspace baseline'}`,
                    `Top ${ROLE_LABELS[focusRole].toLowerCase()}: ${topFocusedEntity ? `${topFocusedEntity.label} (${metric === 'conversions' ? formatNumber(readRoleValue(topFocusedEntity, focusRole, metric)) : formatCurrency(readRoleValue(topFocusedEntity, focusRole, metric))})` : 'No ranked entity in the current slice'}`,
                  ]}
                />
                <a href={comparisonHref} style={actionButtonStyle}>
                  Open model comparison
                </a>
                <a href={trustHref} style={actionButtonStyle}>
                  Open attribution trust
                </a>
                <a href={journeysHref} style={actionButtonStyle}>
                  Open journeys
                </a>
              </div>
            </div>
          </div>
        </SectionCard>

        <ContextSummaryStrip items={summaryItems} minItemWidth={180} />
        <div style={{ marginTop: -t.space.md, marginBottom: t.space.lg, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Attribution Roles is a <strong>live config-aware</strong> view built from live attribution journeys and derived role entities. It is directly comparable to Attribution Comparison and Path Archetypes, but only directionally comparable to workspace-fact or materialized-output pages.
        </div>

        <SegmentOverlapNotice selectedSegment={selectedSegment} />

        <AnalysisNarrativePanel
          title="What changed"
          subtitle="A short readout of who is starting, assisting, and closing demand in the current slice."
          headline={rolesNarrative.headline}
          items={rolesNarrative.items}
        />

        {segmentComparison ? (
          <SectionCard
            title="Segment vs workspace baseline"
            subtitle="How the selected audience changes role mix and contribution concentration relative to the full visible workspace."
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              <SegmentComparisonContextNote
                mode={selectedSegmentAutoCompatible ? 'exact_filter' : 'analytical_lens'}
                pageLabel={scope === 'campaigns' ? 'campaign role rows' : 'channel role rows'}
                basisLabel="matched journey-instance rows and derived role entities"
                primaryLabel={selectedSegment?.name || 'Selected audience'}
                primaryRows={segmentAnalysisQuery.data?.summary.journey_rows}
                baselineRows={segmentAnalysisQuery.data?.baseline_summary.journey_rows}
              />
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
                  gap: t.space.md,
                }}
              >
                {segmentComparison.shares.map((item) => (
                  <div
                    key={item.label}
                    style={{
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.md,
                      padding: t.space.md,
                      background: t.color.bgSubtle,
                    }}
                  >
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.label}</div>
                    <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                      {formatPercent(item.value)}
                    </div>
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
                {segmentComparison.roles.map((item) => {
                  const positive = (item.delta ?? 0) >= 0
                  return (
                    <div
                      key={item.label}
                      style={{
                        border: `1px solid ${t.color.borderLight}`,
                        borderRadius: t.radius.md,
                        padding: t.space.md,
                        background: t.color.surface,
                        display: 'grid',
                        gap: t.space.xs,
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
                          color: positive ? t.color.success : t.color.danger,
                        }}
                      >
                        Δ {item.delta == null ? '—' : `${positive ? '+' : ''}${(item.delta * 100).toFixed(1)}pp`}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          </SectionCard>
        ) : null}

        {selectedSegment && compareSegment && segmentCompareQuery.data ? (
          <SectionCard
            title="Segment vs segment"
            subtitle="Direct audience-to-audience comparison for role behavior, lag, and overlap."
          >
            <div style={{ display: 'grid', gap: t.space.lg }}>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
                  gap: t.space.md,
                }}
              >
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Relationship</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity · {segmentCompareQuery.data.overlap.overlap_rows.toLocaleString()} shared rows
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{selectedSegment.name}</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {(segmentCompareQuery.data.primary_summary.journey_rows ?? 0).toLocaleString()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    rows · median lag {segmentCompareQuery.data.primary_summary.median_lag_days != null ? `${segmentCompareQuery.data.primary_summary.median_lag_days}d` : '—'}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{compareSegment.name}</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {(segmentCompareQuery.data.other_summary.journey_rows ?? 0).toLocaleString()}
                  </div>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    rows · median lag {segmentCompareQuery.data.other_summary.median_lag_days != null ? `${segmentCompareQuery.data.other_summary.median_lag_days}d` : '—'}
                  </div>
                </div>
              </div>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))',
                  gap: t.space.md,
                }}
              >
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Role-metric delta</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {metric === 'conversions'
                      ? segmentCompareQuery.data.deltas.conversions == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.conversions >= 0 ? '+' : '-'}${formatNumber(Math.abs(segmentCompareQuery.data.deltas.conversions))}`
                      : segmentCompareQuery.data.deltas.revenue == null
                        ? '—'
                        : `${segmentCompareQuery.data.deltas.revenue >= 0 ? '+' : '-'}${formatCurrency(Math.abs(segmentCompareQuery.data.deltas.revenue))}`}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Median lag delta</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {segmentCompareQuery.data.deltas.median_lag_days == null ? '—' : `${segmentCompareQuery.data.deltas.median_lag_days >= 0 ? '+' : ''}${segmentCompareQuery.data.deltas.median_lag_days}d`}
                  </div>
                </div>
                <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Top role entities</div>
                  <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.distributions.primary_channels.map((item) => item.value).slice(0, 2).join(', ') || '—'}</strong>
                    {' '}vs{' '}
                    <strong style={{ color: t.color.text }}>{segmentCompareQuery.data.distributions.other_channels.map((item) => item.value).slice(0, 2).join(', ') || '—'}</strong>
                  </div>
                </div>
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <a
                  href={buildIncrementalityPlannerHref({
                    conversionKey: String(conversionKey || ''),
                    startAt: dateFrom,
                    endAt: dateTo,
                    segmentId: selectedSegment.id,
                    name: `Audience role test: ${selectedSegment.name} vs ${compareSegment.name}`,
                    notes: `Compare ${selectedSegment.name} against ${compareSegment.name} in Attribution Roles. Relationship ${segmentCompareQuery.data.overlap.relationship.replace(/_/g, ' ')} with ${(segmentCompareQuery.data.overlap.jaccard * 100).toFixed(0)}% similarity.`,
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
          </SectionCard>
        ) : null}

        {selectedSegment && !selectedSegmentAutoCompatible && segmentAnalysisQuery.data ? (
          <SectionCard
            title={`Advanced audience lens: ${selectedSegment.name}`}
            subtitle="This segment is not a simple page filter, so the role view is computed from matched conversions directly instead of channel/campaign filter shortcuts."
          >
            <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Matched journey rows</div>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {(segmentAnalysisQuery.data.summary.journey_rows ?? 0).toLocaleString()}
                </div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Role-volume share</div>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {formatPercent(roleVolumeShare)}
                </div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bgSubtle }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Median lag</div>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {segmentAnalysisQuery.data.summary.median_lag_days != null ? `${segmentAnalysisQuery.data.summary.median_lag_days}d` : '—'}
                </div>
              </div>
            </div>
          </SectionCard>
        ) : null}

        <CollapsiblePanel
          title="How to read roles"
          subtitle="Role metrics describe where an entity appears in converting paths, not how much selected-model credit it gets."
          open={showMethod}
          onToggle={() => setShowMethod((v) => !v)}
        >
          <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <div><strong style={{ color: t.color.text }}>Introducer</strong>: earliest demand creator in the path.</div>
            <div><strong style={{ color: t.color.text }}>Assister</strong>: middle-path influence that keeps journeys moving.</div>
            <div><strong style={{ color: t.color.text }}>Closer</strong>: final demand capture before conversion.</div>
            <div>
              These metrics come from observed journey positions and can be read alongside, not instead of, your selected attribution model.
            </div>
          </div>
        </CollapsiblePanel>

        <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))' }}>
          {([
            { role: 'first', label: 'Introducer volume', total: roleTotals.first, concentration: concentration.first },
            { role: 'assist', label: 'Assister volume', total: roleTotals.assist, concentration: concentration.assist },
            { role: 'last', label: 'Closer volume', total: roleTotals.last, concentration: concentration.last },
          ] as Array<{ role: RoleKey; label: string; total: number; concentration: number | null }>).map((item) => (
            <SectionCard
              key={item.role}
              title={item.label}
              subtitle={item.concentration != null ? `Top 3 hold ${formatPercent(item.concentration)} of ${ROLE_LABELS[item.role].toLowerCase()} ${metric}.` : 'No observed role volume.'}
            >
              <div style={{ display: 'grid', gap: 6 }}>
                <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightSemibold, color: ROLE_COLORS[item.role] }}>
                  {metric === 'conversions' ? formatNumber(item.total) : formatCurrency(item.total)}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  {totalRoleValue > 0 ? `${formatPercent(item.total / totalRoleValue)} of role-accounted ${metric}` : 'No role-accounted volume in range'}
                </div>
              </div>
            </SectionCard>
          ))}
        <SectionCard
          title="Observed base"
          subtitle="Raw observed outcomes in the selected scope and period."
        >
          <div style={{ display: 'grid', gap: 6 }}>
              <div style={{ fontSize: t.font.sizeXl, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {metric === 'conversions' ? formatNumber(totalObservedConversions) : formatCurrency(totalObservedRevenue)}
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                {metric === 'conversions'
                  ? `${formatCurrency(totalObservedRevenue)} observed revenue across ${visibleEntities.length.toLocaleString()} visible ${scope}`
                  : `${formatNumber(totalObservedConversions)} observed conversions across ${visibleEntities.length.toLocaleString()} visible ${scope}`}
              </div>
            </div>
          </SectionCard>
        </div>

        <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))' }}>
          {(['first', 'assist', 'last'] as RoleKey[]).map((role) => (
            <SectionCard
              key={role}
              title={`Top ${ROLE_LABELS[role]}s`}
              subtitle={`Highest ${ROLE_LABELS[role].toLowerCase()} ${metric} in the current ${scope} view.`}
            >
              <div style={{ display: 'grid', gap: t.space.sm }}>
                {topByRole[role].map((item) => {
                  const value = readRoleValue(item, role, metric)
                  const total = roleTotals[role]
                  return (
                    <div key={`${role}-${item.id}`} style={{ display: 'grid', gap: 4 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm }}>
                        <div style={{ minWidth: 0 }}>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{item.label}</div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.secondaryLabel}</div>
                        </div>
                        <div style={{ fontSize: t.font.sizeSm, color: ROLE_COLORS[role], fontWeight: t.font.weightSemibold }}>
                          {metric === 'conversions' ? formatNumber(value) : formatCurrency(value)}
                        </div>
                      </div>
                      <div style={{ width: '100%', height: 6, background: t.color.bg, borderRadius: 999, overflow: 'hidden' }}>
                        <div
                          style={{
                            width: `${total > 0 ? Math.max(2, (value / total) * 100) : 0}%`,
                            height: '100%',
                            background: ROLE_COLORS[role],
                            borderRadius: 999,
                          }}
                        />
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {total > 0 ? `${formatPercent(value / total)} of visible ${ROLE_LABELS[role].toLowerCase()} ${metric}` : 'No visible role volume'}
                      </div>
                    </div>
                  )
                })}
              </div>
            </SectionCard>
          ))}
        </div>

        <SectionCard
          title={`${scope === 'channels' ? 'Channel' : 'Campaign'} role mix`}
          subtitle={`${topFocusedEntity ? `${topFocusedEntity.label} currently leads the ${ROLE_LABELS[focusRole].toLowerCase()} view and is ${roleDescriptor(dominantRole(topFocusedEntity, metric))}.` : 'Role mix is based on the top visible entities in the selected scope.'}`}
        >
          <div style={{ display: 'grid', gap: t.space.md }}>
            <div style={{ width: '100%', height: 420 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={rankedEntities.map((item) => ({
                    label: item.label,
                    introducer: metric === 'conversions' ? item.firstConversions : item.firstRevenue,
                    assister: metric === 'conversions' ? item.assistConversions : item.assistRevenue,
                    closer: metric === 'conversions' ? item.lastConversions : item.lastRevenue,
                  }))}
                  layout="vertical"
                  margin={{ top: 8, right: 24, bottom: 8, left: 24 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                  <XAxis
                    type="number"
                    stroke={t.color.textMuted}
                    tickFormatter={(value) => (metric === 'conversions' ? formatNumber(Number(value)) : formatCurrency(Number(value)))}
                  />
                  <YAxis type="category" dataKey="label" width={160} stroke={t.color.textMuted} tick={{ fontSize: 12 }} />
                  <Tooltip
                    formatter={(value: number, name: string) => [
                      metric === 'conversions' ? formatNumber(value) : formatCurrency(value),
                      name,
                    ]}
                  />
                  <Legend />
                  <Bar dataKey="introducer" stackId="roles" fill={ROLE_COLORS.first} name="Introducer" radius={[4, 0, 0, 4]} />
                  <Bar dataKey="assister" stackId="roles" fill={ROLE_COLORS.assist} name="Assister" />
                  <Bar dataKey="closer" stackId="roles" fill={ROLE_COLORS.last} name="Closer" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              Stacked bars show where each {scope === 'channels' ? 'channel' : 'campaign'} mostly contributes in converting journeys. A balanced entity spans roles; a skewed entity specializes in one role.
            </div>
          </div>
        </SectionCard>

        <CollapsiblePanel
          title={`All ${scope}`}
          subtitle="Detailed role mix for the visible entities."
          open={showTable}
          onToggle={() => setShowTable((v) => !v)}
        >
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.color.border}` }}>
                  <th style={{ textAlign: 'left', padding: `${t.space.sm}px 0` }}>{scope === 'channels' ? 'Channel' : 'Campaign'}</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Introducer</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Assister</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Closer</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Dominant role</th>
                  <th style={{ textAlign: 'right', padding: `${t.space.sm}px 0` }}>Observed total</th>
                </tr>
              </thead>
              <tbody>
                {rankedEntities.map((item) => {
                  const dominant = dominantRole(item, metric)
                  return (
                    <tr key={item.id} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                      <td style={{ padding: `${t.space.sm}px 0` }}>
                        <div style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>{item.label}</div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{item.secondaryLabel}</div>
                      </td>
                      <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                        {metric === 'conversions' ? formatNumber(item.firstConversions) : formatCurrency(item.firstRevenue)}
                      </td>
                      <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                        {metric === 'conversions' ? formatNumber(item.assistConversions) : formatCurrency(item.assistRevenue)}
                      </td>
                      <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                        {metric === 'conversions' ? formatNumber(item.lastConversions) : formatCurrency(item.lastRevenue)}
                      </td>
                      <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right', color: ROLE_COLORS[dominant], fontWeight: t.font.weightSemibold }}>
                        {ROLE_LABELS[dominant]}
                      </td>
                      <td style={{ padding: `${t.space.sm}px 0`, textAlign: 'right' }}>
                        {metric === 'conversions' ? formatNumber(item.conversions) : formatCurrency(item.revenue)}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </CollapsiblePanel>
      </div>
    </DashboardPage>
  )
}
