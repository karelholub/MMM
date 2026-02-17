import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import ConfidenceBadge, { type Confidence } from '../components/ConfidenceBadge'
import { apiGetJson } from '../lib/apiClient'

interface NextBestRec {
  channel: string
  campaign?: string
  step?: string
  count: number
  conversions: number
  conversion_rate: number
  avg_value: number
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
}

interface JourneysSummary {
  loaded: boolean
  count: number
  converted: number
  non_converted: number
  primary_kpi_id?: string | null
  primary_kpi_label?: string | null
  date_min?: string | null
  date_max?: string | null
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
  const [pathSort, setPathSort] = useState<'count' | 'share' | 'avg_time' | 'length'>('count')
  const [pathSortDir, setPathSortDir] = useState<'asc' | 'desc'>('desc')
  const [freqSort, setFreqSort] = useState<'channel' | 'count' | 'pct'>('count')
  const [freqSortDir, setFreqSortDir] = useState<'asc' | 'desc'>('desc')
  const [tryPathInput, setTryPathInput] = useState('')
  const [tryPathLevel, setTryPathLevel] = useState<'channel' | 'campaign'>('channel')
  const [tryPathResult, setTryPathResult] = useState<{ path_so_far: string; level: string; recommendations: NextBestRec[] } | null>(null)
  const [tryPathLoading, setTryPathLoading] = useState(false)
  const [tryPathError, setTryPathError] = useState<string | null>(null)
  const [tryPathWhyOpen, setTryPathWhyOpen] = useState(false)
  const [showWhy, setShowWhy] = useState(false)
  const [showAnomalies, setShowAnomalies] = useState(true)

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

  const journeysQuery = useQuery<JourneysSummary>({
    queryKey: ['journeys-summary-for-paths'],
    queryFn: async () => apiGetJson<JourneysSummary>('/api/attribution/journeys', {
      fallbackMessage: 'Failed to load journeys summary',
    }),
  })

  const pathsQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis', directMode, pathScope, journeysQuery.data?.date_min, journeysQuery.data?.date_max],
    queryFn: async () => {
      const params = new URLSearchParams({
        direct_mode: directMode,
        path_scope: pathScope === 'all' ? 'all' : 'converted',
      })
      if (journeysQuery.data?.date_min) params.set('date_from', journeysQuery.data.date_min.slice(0, 10))
      if (journeysQuery.data?.date_max) params.set('date_to', journeysQuery.data.date_max.slice(0, 10))
      return apiGetJson<PathAnalysis>(`/api/conversion-paths/analysis?${params.toString()}`, {
        fallbackMessage: 'Failed to fetch path analysis',
      })
    },
  })

  const anomaliesQuery = useQuery<{ anomalies: PathAnomaly[] }>({
    queryKey: ['path-anomalies'],
    queryFn: async () => apiGetJson<{ anomalies: PathAnomaly[] }>('/api/paths/anomalies', {
      fallbackMessage: 'Failed to fetch path anomalies',
    }),
  })

  const data = pathsQuery.data
  const t = tokens

  const channelFreq = data?.channel_frequency ?? {}
  const totalTouchpoints = Object.values(channelFreq).reduce((s, n) => s + n, 0)
  const freqRows = Object.entries(channelFreq).map(([channel, count]) => ({
    channel,
    count,
    pct: totalTouchpoints > 0 ? (count / totalTouchpoints) * 100 : 0,
  }))

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

  if (pathsQuery.isError) {
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
          Could not load path analysis
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          {(pathsQuery.error as Error)?.message || 'Backend may be unreachable. Check that the API is running and CORS/proxy is correct.'}
        </p>
      </div>
    )
  }

  if (pathsQuery.isLoading) {
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
          Analyzing conversion paths…
        </p>
      </div>
    )
  }

  if (!data || data.total_journeys === 0) {
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
          No conversion path data
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          Load journeys in Data Sources, then return here to analyze paths.
        </p>
      </div>
    )
  }

  const journeys = journeysQuery.data
  const periodLabel =
    journeys?.date_min && journeys?.date_max
      ? `${journeys.date_min.slice(0, 10)} – ${journeys.date_max.slice(0, 10)}`
      : 'current dataset'

  const conversionLabel =
    data.config?.conversion_key ||
    journeys?.primary_kpi_label ||
    journeys?.primary_kpi_id ||
    'All conversions'

  const pathLenDist = data.path_length_distribution
  const timeDist = data.time_to_conversion_distribution
  const directDiag = data.direct_unknown_diagnostics
  const nbaConfig = data.nba_config

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          gap: t.space.md,
          marginBottom: t.space.sm,
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
            Conversion Path Analysis
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            How customers interact with channels before converting.
          </p>
        </div>
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'flex-end',
            gap: t.space.xs,
          }}
        >
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: t.space.sm,
              alignItems: 'center',
              justifyContent: 'flex-end',
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              <strong>Period:</strong> {periodLabel}
            </div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              <strong>Conversion:</strong> {conversionLabel} (read‑only)
            </div>
            {data.config?.time_window && (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                <strong>Config:</strong>{' '}
                {[
                  data.config.time_window.click_lookback_days != null
                    ? `Click ${data.config.time_window.click_lookback_days}d`
                    : null,
                  data.config.time_window.impression_lookback_days != null
                    ? `Impr. ${data.config.time_window.impression_lookback_days}d`
                    : null,
                  data.config.time_window.session_timeout_minutes != null
                    ? `Session ${data.config.time_window.session_timeout_minutes}m`
                    : null,
                ]
                  .filter(Boolean)
                  .join(' · ')}
              </div>
            )}
          </div>
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: t.space.sm,
              alignItems: 'center',
              justifyContent: 'flex-end',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs }}>
              <span style={{ color: t.color.textSecondary }}>Direct handling:</span>
              <button
                type="button"
                onClick={() => setDirectMode((m) => (m === 'include' ? 'exclude' : 'include'))}
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.full,
                  padding: '2px 8px',
                  fontSize: t.font.sizeXs,
                  backgroundColor: t.color.bg,
                  cursor: 'pointer',
                }}
                title="View filter only; underlying attribution models are unchanged."
              >
                View filter: {directMode === 'include' ? 'Include Direct' : 'Exclude Direct'}
              </button>
            </div>
            {journeys && journeys.non_converted > 0 && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: t.font.sizeXs }}>
                <span style={{ color: t.color.textSecondary }}>Path scope:</span>
                <button
                  type="button"
                  onClick={() => setPathScope((s) => (s === 'converted' ? 'all' : 'converted'))}
                  style={{
                    border: `1px solid ${t.color.borderLight}`,
                    borderRadius: t.radius.full,
                    padding: '2px 8px',
                    fontSize: t.font.sizeXs,
                    backgroundColor: t.color.bg,
                    cursor: 'pointer',
                  }}
                  title="Include non‑converted journeys in path statistics."
                >
                  {pathScope === 'converted' ? 'Converted only' : 'Converted + non‑converted'}
                </button>
              </div>
            )}
            <button
              type="button"
              onClick={() => setShowWhy((v) => !v)}
              style={{
                border: 'none',
                backgroundColor: showWhy ? t.color.accentMuted : 'transparent',
                color: t.color.accent,
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.full,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightSemibold,
                cursor: 'pointer',
                alignSelf: 'center',
              }}
            >
              Why?
            </button>
          </div>
        </div>
      </div>

      {showWhy && (
        <div style={{ marginBottom: t.space.lg }}>
          <ExplainabilityPanel scope="paths" />
        </div>
      )}

      {anomaliesQuery.data && anomaliesQuery.data.anomalies.length > 0 && (
        <div
          style={{
            marginBottom: t.space.xl,
            padding: t.space.md,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.warning}`,
            background: t.color.warningMuted,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
            <strong
              style={{
                display: 'block',
                fontSize: t.font.sizeSm,
                color: t.color.warning,
              }}
            >
              Path anomalies detected
            </strong>
            <button
              type="button"
              onClick={() => setShowAnomalies((v) => !v)}
              style={{
                padding: `${t.space.xs}px ${t.space.md}px`,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightMedium,
                color: t.color.warning,
                backgroundColor: 'transparent',
                border: `1px solid ${t.color.warning}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              {showAnomalies ? 'Hide' : 'Show'} details
            </button>
          </div>

          {showAnomalies ? (
            <div style={{ marginTop: t.space.sm, overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Severity</th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Issue</th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Value</th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Baseline</th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>z</th>
                    <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Suggestion</th>
                  </tr>
                </thead>
                <tbody>
                  {anomaliesQuery.data.anomalies.map((a, idx) => {
                    const sevColor =
                      a.severity === 'critical' ? t.color.danger : a.severity === 'warn' ? t.color.warning : t.color.textSecondary
                    const formatValue = (v: number) => {
                      const isShare = /share|pct|rate/i.test(a.metric_key)
                      return isShare ? `${(v * 100).toFixed(1)}%` : v.toFixed(2)
                    }
                    return (
                      <tr key={`${a.type}-${idx}`} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, color: sevColor, fontWeight: t.font.weightSemibold }}>
                          {a.severity}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.text }}>
                          <div style={{ fontWeight: t.font.weightMedium }}>{a.type}</div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{a.message}</div>
                          {a.details && (
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginTop: 4 }}>
                              {JSON.stringify(a.details)}
                            </div>
                          )}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                          {formatValue(a.metric_value)}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: t.color.textSecondary }}>
                          {a.baseline_value != null ? formatValue(a.baseline_value) : '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: t.color.textSecondary }}>
                          {a.z_score != null ? a.z_score.toFixed(1) : '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.textSecondary }}>
                          {a.suggestion ?? '—'}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <ul
              style={{
                margin: `${t.space.sm}px 0 0`,
                paddingLeft: 20,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
              }}
            >
              {anomaliesQuery.data.anomalies.map((a, idx) => (
                <li key={`${a.type}-${idx}`}>{a.message}</li>
              ))}
            </ul>
          )}
        </div>
      )}

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
        .conv-table tbody tr:hover { background: ${t.color.accentMuted} !important; }
      `}</style>
      <div
        className="conv-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: t.space.xl,
          marginBottom: t.space.xl,
        }}
      >
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            padding: t.space.xl,
            boxShadow: t.shadowSm,
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
          }}
        >
          <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Channel Touchpoint Stats
          </h3>
          <div style={{ overflowX: 'auto' }}>
            <table className="conv-table" style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
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
                      setFreqSort('channel')
                      setFreqSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                    }}
                  >
                    Channel {freqSort === 'channel' && (freqSortDir === 'asc' ? '↑' : '↓')}
                  </th>
                  <th
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'right',
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => {
                      setFreqSort('count')
                      setFreqSortDir('desc')
                    }}
                  >
                    Touchpoints {freqSort === 'count' && (freqSortDir === 'asc' ? '↑' : '↓')}
                  </th>
                  <th
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'right',
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => {
                      setFreqSort('pct')
                      setFreqSortDir('desc')
                    }}
                  >
                    % of Total {freqSort === 'pct' && (freqSortDir === 'asc' ? '↑' : '↓')}
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedFreq.map((row, idx) => (
                  <tr
                    key={row.channel}
                    style={{
                      borderBottom: `1px solid ${t.color.borderLight}`,
                      backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                    }}
                  >
                    <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, fontWeight: t.font.weightMedium, color: t.color.text }}>{row.channel}</td>
                    <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{row.count}</td>
                    <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightMedium, color: t.color.accent, fontVariantNumeric: 'tabular-nums' }}>
                      {row.pct.toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
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
          {data.next_best_by_prefix_campaign && (
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <input
                type="radio"
                checked={tryPathLevel === 'channel'}
                onChange={() => setTryPathLevel('channel')}
              />
              Channel
            </label>
          )}
          {data.next_best_by_prefix_campaign && (
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
            disabled={tryPathLoading}
            onClick={async () => {
              setTryPathError(null)
              setTryPathResult(null)
              setTryPathWhyOpen(false)
              setTryPathLoading(true)
              try {
                const pathParam = encodeURIComponent(tryPathInput.trim())
                const levelParam = tryPathLevel === 'campaign' && data.next_best_by_prefix_campaign ? 'campaign' : 'channel'
                const json = await apiGetJson<any>(`/api/attribution/next_best_action?path_so_far=${pathParam}&level=${levelParam}`, {
                  fallbackMessage: 'Failed to fetch next best action',
                })
                setTryPathResult({
                  path_so_far: json.path_so_far,
                  level: json.level,
                  recommendations: json.recommendations || [],
                  why_samples: json.why_samples || [],
                  nba_config: json.nba_config || undefined,
                } as any)
              } catch (e) {
                setTryPathError((e as Error).message)
              } finally {
                setTryPathLoading(false)
              }
            }}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.surface,
              backgroundColor: t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: tryPathLoading ? 'wait' : 'pointer',
            }}
          >
            {tryPathLoading ? 'Loading…' : 'Get next best'}
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
                    <span style={{ color: t.color.textSecondary }}>
                      Confidence {(rec.conversion_rate * 100).toFixed(1)}% · support {rec.count} journeys · avg ${rec.avg_value}
                    </span>
                    {((tryPathResult as any).nba_config ?? nbaConfig) &&
                      rec.count < (((tryPathResult as any).nba_config ?? nbaConfig).min_prefix_support || 0) * 2 && (
                        <span style={{ marginLeft: t.space.sm, fontSize: t.font.sizeXs, color: t.color.warning }}>
                          Low sample size: recommendation may be unreliable
                        </span>
                      )}
                  </li>
                ))
              )}
            </ul>
            {(tryPathResult as any).why_samples && (tryPathResult as any).why_samples.length > 0 && (
              <div style={{ marginTop: t.space.md }}>
                <button
                  type="button"
                  onClick={() => setTryPathWhyOpen((v) => !v)}
                  style={{
                    border: 'none',
                    backgroundColor: tryPathWhyOpen ? t.color.accentMuted : t.color.bg,
                    color: t.color.accent,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderRadius: t.radius.full,
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  {tryPathWhyOpen ? 'Hide' : 'Why this recommendation?'}
                </button>
                {tryPathWhyOpen && (
                  <div
                    style={{
                      marginTop: t.space.sm,
                      borderRadius: t.radius.md,
                      border: `1px solid ${t.color.borderLight}`,
                      background: t.color.bg,
                      padding: t.space.sm,
                    }}
                  >
                    <p style={{ margin: '0 0 4px', fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Top historical continuation paths behind these suggestions (Direct/Unknown‑heavy paths are marked for caution).
                    </p>
                    <ul style={{ margin: 0, paddingLeft: t.space.lg, listStyle: 'disc' }}>
                      {(tryPathResult as any).why_samples.map((s: any, idx: number) => (
                        <li key={idx} style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          <span style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>{s.path}</span>{' '}
                          · {s.count} journeys ({(s.share * 100).toFixed(1)}% of prefix) · Direct/Unknown share{' '}
                          {(s.direct_unknown_share * 100).toFixed(1)}%
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
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
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
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
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, maxWidth: 260 }}>
                {Object.keys(channelFreq).map((ch) => {
                  const base = ch.split(':', 1)[0]
                  const active = channelFilter.includes(base)
                  return (
                    <button
                      key={ch}
                      type="button"
                      onClick={() =>
                        setChannelFilter((prev) =>
                          prev.includes(base) ? prev.filter((c) => c !== base) : [...prev, base],
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
                      {base}
                    </button>
                  )
                })}
              </div>
            </div>
          </div>
          <button
            type="button"
            onClick={() =>
              exportPathsCSV(filteredAndSortedPaths, data.next_best_by_prefix, {
                period: periodLabel,
                conversionKey: data.config?.conversion_key ?? null,
                configVersion: data.config?.config_version ?? null,
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
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table className="conv-table" style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary, width: 40 }}>#</th>
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Path</th>
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'right',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    cursor: 'pointer',
                    userSelect: 'none',
                  }}
                  onClick={() => {
                    setPathSort('count')
                    setPathSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                  }}
                >
                  Count {pathSort === 'count' && (pathSortDir === 'asc' ? '↑' : '↓')}
                </th>
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'right',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    cursor: 'pointer',
                    userSelect: 'none',
                  }}
                  onClick={() => {
                    setPathSort('share')
                    setPathSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                  }}
                >
                  Share {pathSort === 'share' && (pathSortDir === 'asc' ? '↑' : '↓')}
                </th>
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'right',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    cursor: 'pointer',
                    userSelect: 'none',
                  }}
                  onClick={() => {
                    setPathSort('length')
                    setPathSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                  }}
                >
                  Avg touchpoints {pathSort === 'length' && (pathSortDir === 'asc' ? '↑' : '↓')}
                </th>
                {filteredAndSortedPaths.some((p) => p.avg_time_to_convert_days != null) && (
                  <th
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'right',
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => {
                      setPathSort('avg_time')
                      setPathSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
                    }}
                  >
                    Avg time to convert {pathSort === 'avg_time' && (pathSortDir === 'asc' ? '↑' : '↓')}
                  </th>
                )}
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Suggested next</th>
              </tr>
            </thead>
            <tbody>
              {filteredAndSortedPaths.slice(0, 20).map((p, idx) => {
                const prefix = p.path.split(' > ').slice(0, -1).join(' > ')
                const recs = data.next_best_by_prefix?.[prefix]
                const top = recs?.[0]
                return (
                <tr
                  key={idx}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                    cursor: 'pointer',
                  }}
                  onClick={async () => {
                    setSelectedPath(p.path)
                    setSelectedPathDetails(null)
                    setSelectedPathError(null)
                    setSelectedPathLoading(true)
                    try {
                      const params = new URLSearchParams({
                        path: p.path,
                        direct_mode: directMode,
                        path_scope: pathScope === 'all' ? 'all' : 'converted',
                      })
                      if (journeysQuery.data?.date_min) params.set('date_from', journeysQuery.data.date_min.slice(0, 10))
                      if (journeysQuery.data?.date_max) params.set('date_to', journeysQuery.data.date_max.slice(0, 10))
                      const json = await apiGetJson<PathDetails>(`/api/conversion-paths/details?${params.toString()}`, {
                        fallbackMessage: 'Failed to load path details',
                      })
                      setSelectedPathDetails(json)
                    } catch (err) {
                      setSelectedPathError((err as Error).message)
                    } finally {
                      setSelectedPathLoading(false)
                    }
                  }}
                >
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, color: t.color.textMuted, fontVariantNumeric: 'tabular-nums' }}>{idx + 1}</td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px` }}>
                    {p.path.split(' > ').map((step, i, arr) => (
                      <span key={i}>
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
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightMedium, fontVariantNumeric: 'tabular-nums' }}>{p.count}</td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightMedium, color: t.color.accent, fontVariantNumeric: 'tabular-nums' }}>
                    {(p.share * 100).toFixed(1)}%
                  </td>
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: t.color.textSecondary }}>
                    {p.path_length ?? p.path.split(' > ').length}
                  </td>
                  {filteredAndSortedPaths.some((row) => row.avg_time_to_convert_days != null) && (
                    <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: t.color.textSecondary }}>
                      {p.avg_time_to_convert_days != null ? `${p.avg_time_to_convert_days.toFixed(1)}d` : '—'}
                    </td>
                  )}
                  <td style={{ padding: `${t.space.md}px ${t.space.lg}px` }}>
                    {top ? (
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
                    ) : (
                      <span style={{ color: t.color.textMuted, fontSize: t.font.sizeXs }}>—</span>
                    )}
                  </td>
                </tr>
                )
              })}
            </tbody>
          </table>
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
  )
}
