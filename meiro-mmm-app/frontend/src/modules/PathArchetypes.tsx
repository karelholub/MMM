import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { apiGetJson } from '../lib/apiClient'

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

interface PathVariant {
  path: string
  count: number
  share: number
  avg_time_to_conversion_days?: number | null
  avg_length?: number
}

interface PathTransition {
  from: string
  to: string
  count: number
  share: number
}

interface PathCluster {
  id: number
  name: string
  size: number
  share: number
  avg_length: number
  avg_time_to_conversion_days: number | null
  top_channels: string[]
  top_paths: PathVariant[]
  representative_path?: string
  cluster_label?: number
  human_label?: string
  defining_traits?: string[]
  distinctiveness_score?: number
  length_median?: number
  length_p90?: number
  time_to_conversion_median_days?: number | null
  time_to_conversion_p90_days?: number | null
  representativeness_score?: number
  top_transitions?: PathTransition[]
  variants?: PathVariant[]
  outlier_paths?: PathVariant[]
  actions?: {
    channel: string
    support: number
    support_share: number
    low_sample?: boolean
  }[]
  confidence?: 'high' | 'medium' | 'low' | string
  avg_conversion_value?: number | null
  total_conversion_value?: number | null
  compare?: {
    share_previous?: number
    share_current?: number
    share_delta?: number
    median_length_previous?: number
    median_length_current?: number
    median_length_delta?: number
    median_ttc_previous_days?: number | null
    median_ttc_current_days?: number | null
    median_ttc_delta_days?: number | null
  } | null
}

interface ArchetypeWarning {
  code: string
  severity: 'info' | 'warn' | 'critical' | string
  message: string
}

interface ArchetypesDiagnostics {
  algorithm?: string
  k_mode?: string
  k_selected?: number
  silhouette_cosine?: number | null
  n_unique_paths?: number
  total_converted?: number
  conversion_key?: string | null
  cluster_size_stats?: { min: number; median: number; max: number }
  direct_unknown_share?: number
  journeys_ending_direct_share?: number
  quality_badge?: 'ok' | 'warning' | 'weak' | string
  warnings?: ArchetypeWarning[]
  stability_score?: number | null
  stability_score_pct?: number | null
  stability_label?: string | null
  compare_available?: boolean
  emerging_cluster_id?: number | null
  declining_cluster_id?: number | null
  view_filters?: {
    direct_mode?: 'include' | 'exclude'
  } | null
}

interface ArchetypesResponse {
  clusters: PathCluster[]
  total_converted: number
  diagnostics?: ArchetypesDiagnostics
}

function formatDelta(delta: number | undefined | null, opts: { pct?: boolean; decimals?: number } = {}) {
  if (delta == null || !Number.isFinite(delta)) return '—'
  const decimals = opts.decimals ?? 1
  if (opts.pct) {
    return `${delta >= 0 ? '+' : ''}${(delta * 100).toFixed(decimals)}%`
  }
  return `${delta >= 0 ? '+' : ''}${delta.toFixed(decimals)}`
}

function exportArchetypesCSV(clusters: PathCluster[], meta: { period?: string; conversionKey?: string | null; diagnostics?: ArchetypesDiagnostics; directMode?: 'include' | 'exclude' }) {
  if (!clusters.length) return
  const lines: string[] = []
  lines.push('# Path archetypes export')
  if (meta.period) lines.push(`# Period: ${meta.period}`)
  if (meta.conversionKey) lines.push(`# Conversion: ${meta.conversionKey}`)
  if (meta.directMode) lines.push(`# Direct handling: ${meta.directMode}`)
  if (meta.diagnostics?.k_mode) lines.push(`# Clustering mode: ${meta.diagnostics.k_mode}`)
  if (meta.diagnostics?.k_selected != null) lines.push(`# K: ${meta.diagnostics.k_selected}`)
  if (meta.diagnostics?.silhouette_cosine != null)
    lines.push(`# Silhouette (cosine): ${meta.diagnostics.silhouette_cosine.toFixed(3)}`)
  if (meta.diagnostics?.stability_score_pct != null)
    lines.push(`# Stability score: ${meta.diagnostics.stability_score_pct}% (${meta.diagnostics.stability_label ?? 'n/a'})`)
  lines.push(
    [
      'Archetype ID',
      'Label',
      'Journeys',
      'Share (%)',
      'Avg length',
      'Median length',
      'P90 length',
      'Median TTC (days)',
      'P90 TTC (days)',
      'Distinctiveness (0-100)',
      'Confidence',
      'Top channels',
      'Representative path',
      'Representativeness',
      'Top paths (path|count|share%)',
    ].join(','),
  )

  clusters.forEach((c) => {
    const topPathsStr = (c.top_paths ?? [])
      .map((p) => `${p.path}|${p.count}|${(p.share * 100).toFixed(1)}`)
      .join('; ')
    lines.push(
      [
        c.id,
        `"${(c.human_label ?? c.name).replace(/"/g, '""')}"`,
        c.size,
        (c.share * 100).toFixed(2),
        c.avg_length.toFixed(2),
        c.length_median != null ? c.length_median.toFixed(2) : '',
        c.length_p90 != null ? c.length_p90.toFixed(2) : '',
        c.time_to_conversion_median_days != null ? c.time_to_conversion_median_days.toFixed(2) : '',
        c.time_to_conversion_p90_days != null ? c.time_to_conversion_p90_days.toFixed(2) : '',
        c.distinctiveness_score ?? '',
        c.confidence ?? '',
        `"${(c.top_channels || []).join(' | ').replace(/"/g, '""')}"`,
        `"${(c.representative_path ?? '').replace(/"/g, '""')}"`,
        c.representativeness_score != null ? c.representativeness_score.toFixed(3) : '',
        `"${topPathsStr.replace(/"/g, '""')}"`,
      ].join(','),
    )
  })

  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `path-archetypes-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export default function PathArchetypes() {
  const [kMode, setKMode] = useState<'auto' | 'fixed'>('auto')
  const [kFixed, setKFixed] = useState(6)
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [directMode, setDirectMode] = useState<'include' | 'exclude'>('include')
  const [comparePrevious, setComparePrevious] = useState(false)
  const [showQualityHelp, setShowQualityHelp] = useState(false)
  const [channelFilter, setChannelFilter] = useState<string[]>([])
  const [minAvgLength, setMinAvgLength] = useState<number | ''>('')
  const [maxAvgLength, setMaxAvgLength] = useState<number | ''>('')
  const [detailTab, setDetailTab] = useState<'overview' | 'composition' | 'transitions' | 'variants' | 'actions'>('overview')
  const [selectedPathForDrawer, setSelectedPathForDrawer] = useState<PathVariant | null>(null)

  const journeysQuery = useQuery<JourneysSummary>({
    queryKey: ['journeys-summary-for-archetypes'],
    queryFn: async () => apiGetJson<JourneysSummary>('/api/attribution/journeys', {
      fallbackMessage: 'Failed to load journeys summary',
    }),
  })

  const archetypesQuery = useQuery<ArchetypesResponse>({
    queryKey: ['path-archetypes', kMode, kFixed, directMode, comparePrevious],
    queryFn: async () => {
      const params = new URLSearchParams()
      params.set('k_mode', kMode)
      if (kMode === 'fixed') params.set('k', String(kFixed))
      params.set('direct_mode', directMode)
      if (comparePrevious) params.set('compare_previous', 'true')
      return apiGetJson<ArchetypesResponse>(`/api/paths/archetypes?${params.toString()}`, {
        fallbackMessage: 'Failed to load path archetypes',
      })
    },
  })

  const data = archetypesQuery.data
  const clustersRaw = data?.clusters ?? []

  const journeys = journeysQuery.data
  const periodLabel =
    journeys?.date_min && journeys?.date_max
      ? `${journeys.date_min.slice(0, 10)} – ${journeys.date_max.slice(0, 10)}`
      : 'current dataset'
  const conversionLabel =
    journeys?.primary_kpi_label ||
    journeys?.primary_kpi_id ||
    (journeys && journeys.converted ? 'Primary conversion' : '') ||
    'All conversions'

  // Derive available channels for the "channel contains" filter from current clusters.
  const allChannels = useMemo(() => {
    const s = new Set<string>()
    clustersRaw.forEach((c) => (c.top_channels || []).forEach((ch) => s.add(ch)))
    return Array.from(s).sort()
  }, [clustersRaw])

  const clusters = useMemo(() => {
    return clustersRaw.filter((c) => {
      if (channelFilter.length) {
        const hasAny = (c.top_channels || []).some((ch) => channelFilter.includes(ch))
        if (!hasAny) return false
      }
      if (minAvgLength !== '' && c.avg_length < minAvgLength) return false
      if (maxAvgLength !== '' && c.avg_length > maxAvgLength) return false
      return true
    })
  }, [clustersRaw, channelFilter, minAvgLength, maxAvgLength])

  const selected = useMemo(() => {
    if (!clusters.length) return null
    const id = selectedId ?? clusters[0].id
    return clusters.find((c) => c.id === id) ?? clusters[0]
  }, [clusters, selectedId])

  const diagnostics = data?.diagnostics
  const qualityBadge = diagnostics?.quality_badge ?? 'ok'
  const stabilityPct = diagnostics?.stability_score_pct ?? null
  const stabilityLabel = diagnostics?.stability_label ?? null

  const isCompareAvailable = Boolean(comparePrevious && diagnostics?.compare_available)

  const handleExport = () => {
    exportArchetypesCSV(clustersRaw, {
      period: periodLabel,
      conversionKey: diagnostics?.conversion_key ?? journeys?.primary_kpi_id ?? null,
      diagnostics,
      directMode,
    })
  }

  const tkn = t

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: tkn.space.xl,
          gap: tkn.space.md,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <h1
            style={{
              margin: 0,
              fontSize: tkn.font.size2xl,
              fontWeight: tkn.font.weightBold,
              color: tkn.color.text,
              letterSpacing: '-0.02em',
            }}
          >
            Path archetypes
          </h1>
          <p style={{ margin: `${tkn.space.xs}px 0 0`, fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
            The most common conversion paths, grouped as archetypes. Use these to understand typical journeys and where to
            focus optimization.
          </p>
        </div>
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: tkn.space.xs,
            alignItems: 'flex-end',
            minWidth: 260,
          }}
        >
          {/* Measurement context bar (read-only period + conversion) */}
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: tkn.space.sm,
              alignItems: 'center',
              justifyContent: 'flex-end',
              fontSize: tkn.font.sizeXs,
              color: tkn.color.textSecondary,
            }}
          >
            <div>
              <strong>Period:</strong> {periodLabel}
            </div>
            <div>
              <strong>Conversion:</strong> {conversionLabel} (read‑only)
            </div>
          </div>

          {/* Clustering controls + direct handling + compare */}
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: tkn.space.sm,
              alignItems: 'center',
              justifyContent: 'flex-end',
            }}
          >
            <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
              Clustering
              <select
                value={kMode}
                onChange={(e) => setKMode(e.target.value as 'auto' | 'fixed')}
                style={{
                  marginLeft: 8,
                  padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                  fontSize: tkn.font.sizeSm,
                  border: `1px solid ${tkn.color.border}`,
                  borderRadius: tkn.radius.sm,
                  background: '#ffffff',
                }}
              >
                <option value="auto">
                  Auto
                  {diagnostics?.k_selected != null ? ` (K≈${diagnostics.k_selected})` : ''}
                </option>
                <option value="fixed">Fixed K</option>
              </select>
            </label>
            {kMode === 'fixed' && (
              <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                K
                <input
                  type="number"
                  min={2}
                  max={20}
                  value={kFixed}
                  onChange={(e) =>
                    setKFixed(Math.max(2, Math.min(20, parseInt(e.target.value || '6', 10))))
                  }
                  style={{
                    width: 72,
                    marginLeft: 8,
                    padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                    fontSize: tkn.font.sizeSm,
                    border: `1px solid ${tkn.color.border}`,
                    borderRadius: tkn.radius.sm,
                  }}
                />
              </label>
            )}
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: tkn.font.sizeXs }}>
              <span style={{ color: tkn.color.textSecondary }}>Direct handling:</span>
              <button
                type="button"
                onClick={() => setDirectMode((m) => (m === 'include' ? 'exclude' : 'include'))}
                style={{
                  border: `1px solid ${tkn.color.borderLight}`,
                  borderRadius: tkn.radius.full,
                  padding: '2px 8px',
                  fontSize: tkn.font.sizeXs,
                  backgroundColor: tkn.color.bg,
                  cursor: 'pointer',
                }}
                title="View filter only; underlying clustering uses the filtered journeys."
              >
                View filter: {directMode === 'include' ? 'Include Direct' : 'Exclude Direct'}
              </button>
            </div>
            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                cursor: 'pointer',
                fontSize: tkn.font.sizeXs,
                color: tkn.color.textSecondary,
              }}
            >
              <input
                type="checkbox"
                checked={comparePrevious}
                onChange={(e) => setComparePrevious(e.target.checked)}
                style={{ margin: 0 }}
              />
              Compare to previous period
            </label>
            <button
              type="button"
              onClick={() => archetypesQuery.refetch()}
              disabled={archetypesQuery.isFetching}
              style={{
                borderRadius: tkn.radius.full,
                border: `1px solid ${tkn.color.borderLight}`,
                backgroundColor: tkn.color.bg,
                padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                fontSize: tkn.font.sizeXs,
                cursor: archetypesQuery.isFetching ? 'wait' : 'pointer',
              }}
            >
              {archetypesQuery.isFetching ? 'Recomputing…' : 'Recompute clustering'}
            </button>
            <button
              type="button"
              onClick={handleExport}
              disabled={!clustersRaw.length}
              style={{
                borderRadius: tkn.radius.full,
                border: `1px solid ${tkn.color.borderLight}`,
                backgroundColor: tkn.color.surface,
                padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                fontSize: tkn.font.sizeXs,
                cursor: clustersRaw.length ? 'pointer' : 'default',
              }}
            >
              Export CSV
            </button>
          </div>

          {/* Lightweight filters – view filters only */}
          {clustersRaw.length > 0 && (
            <div
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: tkn.space.xs,
                alignItems: 'center',
                justifyContent: 'flex-end',
                fontSize: tkn.font.sizeXs,
                color: tkn.color.textSecondary,
              }}
            >
              <span>Filters (view-only):</span>
              {allChannels.length > 0 && (
                <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', maxWidth: 360 }}>
                  {allChannels.slice(0, 10).map((ch) => {
                    const active = channelFilter.includes(ch)
                    return (
                      <button
                        key={ch}
                        type="button"
                        onClick={() =>
                          setChannelFilter((prev) =>
                            prev.includes(ch) ? prev.filter((c) => c !== ch) : [...prev, ch],
                          )
                        }
                        style={{
                          borderRadius: tkn.radius.full,
                          border: `1px solid ${active ? tkn.color.accent : tkn.color.borderLight}`,
                          padding: '2px 8px',
                          backgroundColor: active ? tkn.color.accentMuted : tkn.color.bg,
                          fontSize: tkn.font.sizeXs,
                          cursor: 'pointer',
                        }}
                      >
                        {ch}
                      </button>
                    )
                  })}
                </div>
              )}
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span>Avg path length</span>
                <input
                  type="number"
                  placeholder="min"
                  value={minAvgLength}
                  onChange={(e) =>
                    setMinAvgLength(e.target.value === '' ? '' : Number(e.target.value) || 0)
                  }
                  style={{
                    width: 56,
                    fontSize: tkn.font.sizeXs,
                    padding: `${tkn.space.xs}px`,
                    borderRadius: tkn.radius.sm,
                    border: `1px solid ${tkn.color.borderLight}`,
                  }}
                />
                <span>–</span>
                <input
                  type="number"
                  placeholder="max"
                  value={maxAvgLength}
                  onChange={(e) =>
                    setMaxAvgLength(e.target.value === '' ? '' : Number(e.target.value) || 0)
                  }
                  style={{
                    width: 56,
                    fontSize: tkn.font.sizeXs,
                    padding: `${tkn.space.xs}px`,
                    borderRadius: tkn.radius.sm,
                    border: `1px solid ${tkn.color.borderLight}`,
                  }}
                />
              </div>
            </div>
          )}
        </div>
      </div>

      <div
        style={{
          background: tkn.color.surface,
          border: `1px solid ${tkn.color.borderLight}`,
          borderRadius: tkn.radius.lg,
          padding: tkn.space.xl,
          boxShadow: tkn.shadowSm,
        }}
      >
        {archetypesQuery.isLoading && (
          <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Loading path archetypes…</p>
        )}
        {archetypesQuery.isError && (
          <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.danger }}>
            {(archetypesQuery.error as Error).message || 'Failed to load path archetypes.'}
          </p>
        )}
        {data && clustersRaw.length === 0 && !archetypesQuery.isLoading && !archetypesQuery.isError && (
          <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
            No converted journeys to build archetypes from yet. Load journeys and run attribution first.
          </p>
        )}

        {data && clustersRaw.length > 0 && (
          <>
            {/* Cluster quality & trust card */}
            <div
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: tkn.space.md,
                marginBottom: tkn.space.lg,
                alignItems: 'flex-start',
              }}
            >
              <div
                style={{
                  flex: '1 1 260px',
                  borderRadius: tkn.radius.lg,
                  border: `1px solid ${tkn.color.borderLight}`,
                  padding: tkn.space.md,
                  backgroundColor: tkn.color.bg,
                  fontSize: tkn.font.sizeXs,
                  color: tkn.color.textSecondary,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tkn.space.xs }}>
                  <strong style={{ fontSize: tkn.font.sizeSm, color: tkn.color.text }}>Cluster quality</strong>
                  <span
                    style={{
                      padding: '2px 8px',
                      borderRadius: tkn.radius.full,
                      border: `1px solid ${
                        qualityBadge === 'ok'
                          ? tkn.color.success
                          : qualityBadge === 'warning'
                          ? tkn.color.warning
                          : tkn.color.danger
                      }`,
                      color:
                        qualityBadge === 'ok'
                          ? tkn.color.success
                          : qualityBadge === 'warning'
                          ? tkn.color.warning
                          : tkn.color.danger,
                      backgroundColor: tkn.color.surface,
                      textTransform: 'capitalize',
                    }}
                  >
                    {qualityBadge}
                  </span>
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: tkn.space.sm, marginBottom: tkn.space.sm }}>
                  <div>
                    <div style={{ color: tkn.color.textMuted }}>Silhouette (cosine)</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: tkn.color.text }}>
                      {diagnostics?.silhouette_cosine != null
                        ? diagnostics.silhouette_cosine.toFixed(3)
                        : 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ color: tkn.color.textMuted }}>Unique paths</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: tkn.color.text }}>
                      {diagnostics?.n_unique_paths != null
                        ? diagnostics.n_unique_paths.toLocaleString()
                        : 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ color: tkn.color.textMuted }}>Cluster size (min / median / max)</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: tkn.color.text }}>
                      {diagnostics?.cluster_size_stats
                        ? `${diagnostics.cluster_size_stats.min.toLocaleString()} / ${diagnostics.cluster_size_stats.median.toLocaleString()} / ${diagnostics.cluster_size_stats.max.toLocaleString()}`
                        : 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div style={{ color: tkn.color.textMuted }}>Stability</div>
                    <div style={{ fontVariantNumeric: 'tabular-nums', color: tkn.color.text }}>
                      {stabilityPct != null
                        ? `${stabilityPct}%${stabilityLabel ? ` (${stabilityLabel})` : ''}`
                        : 'N/A'}
                    </div>
                  </div>
                </div>
                {diagnostics?.warnings && diagnostics.warnings.length > 0 && (
                  <ul style={{ margin: 0, paddingLeft: 16, color: tkn.color.textSecondary }}>
                    {diagnostics.warnings.slice(0, 4).map((w) => (
                      <li key={w.code} style={{ marginBottom: 2 }}>
                        {w.message}
                      </li>
                    ))}
                  </ul>
                )}
                <button
                  type="button"
                  onClick={() => setShowQualityHelp((v) => !v)}
                  style={{
                    marginTop: tkn.space.xs,
                    padding: 0,
                    border: 'none',
                    background: 'none',
                    color: tkn.color.accent,
                    fontSize: tkn.font.sizeXs,
                    cursor: 'pointer',
                  }}
                >
                  What does this mean?
                </button>
                {showQualityHelp && (
                  <div style={{ marginTop: tkn.space.xs, color: tkn.color.textMuted }}>
                    Silhouette measures how well paths are separated between clusters (higher is better). Stability
                    re-runs clustering with a different seed and compares assignments. Warnings highlight data issues
                    like Direct/Unknown dominance or low sample size that can make archetypes less reliable.
                  </div>
                )}
              </div>
            </div>

            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(260px, 1.2fr) minmax(0, 2fr)',
                gap: tkn.space.lg,
                alignItems: 'start',
              }}
            >
              {/* Archetype list */}
              <div
                style={{
                  border: `1px solid ${tkn.color.borderLight}`,
                  borderRadius: tkn.radius.lg,
                  overflow: 'hidden',
                }}
              >
                <div
                  style={{
                    padding: tkn.space.md,
                    background: tkn.color.bg,
                    borderBottom: `1px solid ${tkn.color.borderLight}`,
                  }}
                >
                  <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                    {data.total_converted.toLocaleString()} converted journeys • {clusters.length} archetypes
                  </div>
                  {isCompareAvailable && (
                    <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted, marginTop: 4 }}>
                      Emerging / declining flags are based on share change between two equal-sized time windows.
                    </div>
                  )}
                </div>
                <div style={{ maxHeight: 520, overflowY: 'auto' }}>
                  {clusters.map((c) => {
                    const isSel = selected?.id === c.id
                    const isEmerging =
                      isCompareAvailable && diagnostics?.emerging_cluster_id === c.id
                    const isDeclining =
                      isCompareAvailable && diagnostics?.declining_cluster_id === c.id
                    return (
                      <button
                        key={c.id}
                        type="button"
                        onClick={() => {
                          setSelectedId(c.id)
                          setSelectedPathForDrawer(null)
                        }}
                        style={{
                          width: '100%',
                          textAlign: 'left',
                          padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                          border: 'none',
                          borderBottom: `1px solid ${tkn.color.borderLight}`,
                          background: isSel ? tkn.color.accentMuted : tkn.color.surface,
                          cursor: 'pointer',
                        }}
                      >
                        <div
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 6,
                            marginBottom: 2,
                          }}
                        >
                          <div
                            style={{
                              fontSize: tkn.font.sizeSm,
                              fontWeight: tkn.font.weightSemibold,
                              color: tkn.color.text,
                            }}
                          >
                            Archetype #{c.id}
                          </div>
                          <span
                            style={{
                              fontSize: tkn.font.sizeXs,
                              color: tkn.color.textSecondary,
                            }}
                          >
                            {(c.human_label ?? c.name).slice(0, 80)}
                          </span>
                          {isEmerging && (
                            <span
                              style={{
                                padding: '0 6px',
                                borderRadius: tkn.radius.full,
                                border: `1px solid ${tkn.color.success}`,
                                color: tkn.color.success,
                                fontSize: tkn.font.sizeXs,
                              }}
                            >
                              Emerging
                            </span>
                          )}
                          {isDeclining && (
                            <span
                              style={{
                                padding: '0 6px',
                                borderRadius: tkn.radius.full,
                                border: `1px solid ${tkn.color.danger}`,
                                color: tkn.color.danger,
                                fontSize: tkn.font.sizeXs,
                              }}
                            >
                              Declining
                            </span>
                          )}
                        </div>
                        <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                          {c.size.toLocaleString()} journeys • {(c.share * 100).toFixed(1)}% •{' '}
                          {c.avg_length.toFixed(1)} steps
                          {c.time_to_conversion_median_days != null
                            ? ` • median ${(c.time_to_conversion_median_days).toFixed(1)}d`
                            : ''}
                        </div>
                        {c.defining_traits && c.defining_traits.length > 0 && (
                          <div
                            style={{
                              marginTop: 4,
                              display: 'flex',
                              flexWrap: 'wrap',
                              gap: 4,
                            }}
                          >
                            {c.defining_traits.slice(0, 4).map((trait) => (
                              <span
                                key={trait}
                                style={{
                                  padding: '2px 6px',
                                  borderRadius: tkn.radius.full,
                                  border: `1px solid ${tkn.color.borderLight}`,
                                  backgroundColor: tkn.color.bg,
                                  fontSize: tkn.font.sizeXs,
                                  color: tkn.color.textMuted,
                                }}
                              >
                                {trait}
                              </span>
                            ))}
                          </div>
                        )}
                      </button>
                    )
                  })}
                </div>
              </div>

              {/* Archetype detail with tabs */}
              {selected && (
                <div
                  style={{
                    background: tkn.color.surface,
                    border: `1px solid ${tkn.color.borderLight}`,
                    borderRadius: tkn.radius.lg,
                    padding: tkn.space.xl,
                    boxShadow: tkn.shadowSm,
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      gap: tkn.space.md,
                      flexWrap: 'wrap',
                      marginBottom: tkn.space.md,
                    }}
                  >
                    <div>
                      <div
                        style={{
                          fontSize: tkn.font.sizeSm,
                          fontWeight: tkn.font.weightSemibold,
                          color: tkn.color.text,
                        }}
                      >
                        Archetype #{selected.id} • {selected.human_label ?? selected.name}
                      </div>
                      <div
                        style={{
                          fontSize: tkn.font.sizeXs,
                          color: tkn.color.textSecondary,
                          marginTop: 4,
                        }}
                      >
                        {selected.size.toLocaleString()} journeys •{' '}
                        {(selected.share * 100).toFixed(1)}% of converted •{' '}
                        {selected.avg_length.toFixed(1)} steps
                        {selected.avg_time_to_conversion_days != null
                          ? ` • ${selected.avg_time_to_conversion_days.toFixed(1)} days to convert`
                          : ''}
                        {selected.distinctiveness_score != null && (
                          <> • Distinctiveness {selected.distinctiveness_score}/100</>
                        )}
                      </div>
                      {selected.defining_traits && selected.defining_traits.length > 0 && (
                        <div
                          style={{
                            display: 'flex',
                            flexWrap: 'wrap',
                            gap: 6,
                            marginTop: tkn.space.xs,
                          }}
                        >
                          {selected.defining_traits.map((trait) => (
                            <span
                              key={trait}
                              style={{
                                padding: '2px 8px',
                                borderRadius: tkn.radius.full,
                                background: tkn.color.bg,
                                border: `1px solid ${tkn.color.borderLight}`,
                                fontSize: tkn.font.sizeXs,
                                color: tkn.color.textSecondary,
                              }}
                            >
                              {trait}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                    <div
                      style={{
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'flex-end',
                        gap: 4,
                        fontSize: tkn.font.sizeXs,
                      }}
                    >
                      {selected.confidence && (
                        <div>
                          <strong>Confidence:</strong>{' '}
                          <span style={{ textTransform: 'capitalize' }}>{selected.confidence}</span>
                        </div>
                      )}
                      {selected.representativeness_score != null && (
                        <div>
                          <strong>Representativeness:</strong>{' '}
                          {(selected.representativeness_score * 100).toFixed(1)}% of journeys
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Tabs */}
                  <div
                    style={{
                      display: 'flex',
                      flexWrap: 'wrap',
                      gap: 8,
                      borderBottom: `1px solid ${tkn.color.borderLight}`,
                      marginBottom: tkn.space.md,
                    }}
                  >
                    {(['overview', 'composition', 'transitions', 'variants', 'actions'] as const).map(
                      (tab) => (
                        <button
                          key={tab}
                          type="button"
                          onClick={() => {
                            setDetailTab(tab)
                            setSelectedPathForDrawer(null)
                          }}
                          style={{
                            border: 'none',
                            borderBottom:
                              detailTab === tab
                                ? `2px solid ${tkn.color.accent}`
                                : '2px solid transparent',
                            padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                            background: 'transparent',
                            cursor: 'pointer',
                            fontSize: tkn.font.sizeXs,
                            fontWeight:
                              detailTab === tab ? tkn.font.weightSemibold : tkn.font.weightMedium,
                            color:
                              detailTab === tab ? tkn.color.text : tkn.color.textSecondary,
                            textTransform: 'capitalize',
                          }}
                        >
                          {tab}
                        </button>
                      ),
                    )}
                  </div>

                  {/* Tab content */}
                  {detailTab === 'overview' && (
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
                        gap: tkn.space.md,
                      }}
                    >
                      <div
                        style={{
                          borderRadius: tkn.radius.md,
                          border: `1px solid ${tkn.color.borderLight}`,
                          padding: tkn.space.md,
                        }}
                      >
                        <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                          Journeys
                        </div>
                        <div
                          style={{
                            fontSize: tkn.font.sizeLg,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.text,
                          }}
                        >
                          {selected.size.toLocaleString()}
                        </div>
                      </div>
                      <div
                        style={{
                          borderRadius: tkn.radius.md,
                          border: `1px solid ${tkn.color.borderLight}`,
                          padding: tkn.space.md,
                        }}
                      >
                        <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                          Share of converted
                        </div>
                        <div
                          style={{
                            fontSize: tkn.font.sizeLg,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.text,
                          }}
                        >
                          {(selected.share * 100).toFixed(1)}%
                        </div>
                      </div>
                      <div
                        style={{
                          borderRadius: tkn.radius.md,
                          border: `1px solid ${tkn.color.borderLight}`,
                          padding: tkn.space.md,
                        }}
                      >
                        <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                          Path length (avg / median / P90)
                        </div>
                        <div
                          style={{
                            fontSize: tkn.font.sizeLg,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.text,
                            fontVariantNumeric: 'tabular-nums',
                          }}
                        >
                          {selected.avg_length.toFixed(1)} /{' '}
                          {(selected.length_median ?? selected.avg_length).toFixed(1)} /{' '}
                          {(selected.length_p90 ?? selected.length_median ?? selected.avg_length).toFixed(1)}
                        </div>
                      </div>
                      <div
                        style={{
                          borderRadius: tkn.radius.md,
                          border: `1px solid ${tkn.color.borderLight}`,
                          padding: tkn.space.md,
                        }}
                      >
                        <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                          Time to convert (avg / median / P90)
                        </div>
                        <div
                          style={{
                            fontSize: tkn.font.sizeLg,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.text,
                            fontVariantNumeric: 'tabular-nums',
                          }}
                        >
                          {selected.avg_time_to_conversion_days != null
                            ? selected.avg_time_to_conversion_days.toFixed(1)
                            : 'N/A'}{' '}
                          /{' '}
                          {selected.time_to_conversion_median_days != null
                            ? selected.time_to_conversion_median_days.toFixed(1)
                            : 'N/A'}{' '}
                          /{' '}
                          {selected.time_to_conversion_p90_days != null
                            ? selected.time_to_conversion_p90_days.toFixed(1)
                            : 'N/A'}
                        </div>
                      </div>
                      {selected.total_conversion_value != null && (
                        <div
                          style={{
                            borderRadius: tkn.radius.md,
                            border: `1px solid ${tkn.color.borderLight}`,
                            padding: tkn.space.md,
                          }}
                        >
                          <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                            Conversion value (avg / total)
                          </div>
                          <div
                            style={{
                              fontSize: tkn.font.sizeLg,
                              fontWeight: tkn.font.weightSemibold,
                              color: tkn.color.text,
                              fontVariantNumeric: 'tabular-nums',
                            }}
                          >
                            {selected.avg_conversion_value != null
                              ? selected.avg_conversion_value.toFixed(2)
                              : 'N/A'}{' '}
                            / {selected.total_conversion_value.toFixed(2)}
                          </div>
                        </div>
                      )}
                      {isCompareAvailable && selected.compare && (
                        <div
                          style={{
                            borderRadius: tkn.radius.md,
                            border: `1px solid ${tkn.color.borderLight}`,
                            padding: tkn.space.md,
                          }}
                        >
                          <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                            Period‑over‑period deltas
                          </div>
                          <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                            Share: {formatDelta(selected.compare.share_delta, { pct: true })} • Median
                            length:{' '}
                            {formatDelta(selected.compare.median_length_delta, {
                              decimals: 1,
                            })}{' '}
                            steps • Median TTC:{' '}
                            {formatDelta(selected.compare.median_ttc_delta_days, {
                              decimals: 1,
                            })}{' '}
                            d
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {detailTab === 'composition' && (
                    <div>
                      <div
                        style={{
                          marginBottom: tkn.space.md,
                          fontSize: tkn.font.sizeXs,
                          color: tkn.color.textSecondary,
                        }}
                      >
                        Position distribution is approximated from representative and common paths. Use this to see which
                        channels typically start, assist, and close journeys.
                      </div>
                      <div
                        style={{
                          display: 'grid',
                          gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                          gap: tkn.space.md,
                        }}
                      >
                        <div
                          style={{
                            borderRadius: tkn.radius.md,
                            border: `1px solid ${tkn.color.borderLight}`,
                            padding: tkn.space.md,
                          }}
                        >
                          <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                            Top first‑touch (representative)
                          </div>
                          <div style={{ fontSize: tkn.font.sizeSm, color: tkn.color.text }}>
                            {selected.representative_path
                              ? selected.representative_path.split(' > ')[0]
                              : selected.top_paths?.[0]?.path.split(' > ')[0] ?? 'N/A'}
                          </div>
                        </div>
                        <div
                          style={{
                            borderRadius: tkn.radius.md,
                            border: `1px solid ${tkn.color.borderLight}`,
                            padding: tkn.space.md,
                          }}
                        >
                          <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                            Top last‑touch (representative)
                          </div>
                          <div style={{ fontSize: tkn.font.sizeSm, color: tkn.color.text }}>
                            {selected.representative_path
                              ? selected.representative_path.split(' > ').slice(-1)[0]
                              : selected.top_paths?.[0]?.path.split(' > ').slice(-1)[0] ?? 'N/A'}
                          </div>
                        </div>
                        <div
                          style={{
                            borderRadius: tkn.radius.md,
                            border: `1px solid ${tkn.color.borderLight}`,
                            padding: tkn.space.md,
                          }}
                        >
                          <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                            % paths ending Direct/Unknown (overall)
                          </div>
                          <div
                            style={{
                              fontSize: tkn.font.sizeSm,
                              color: tkn.color.text,
                              fontVariantNumeric: 'tabular-nums',
                            }}
                          >
                            {diagnostics?.journeys_ending_direct_share != null
                              ? `${(diagnostics.journeys_ending_direct_share * 100).toFixed(1)}%`
                              : 'N/A'}
                          </div>
                        </div>
                      </div>
                      <div style={{ marginTop: tkn.space.lg }}>
                        <div
                          style={{
                            fontSize: tkn.font.sizeXs,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.textSecondary,
                            marginBottom: tkn.space.xs,
                          }}
                        >
                          Representative path
                        </div>
                        <div
                          style={{
                            fontSize: tkn.font.sizeSm,
                            color: tkn.color.text,
                            background: tkn.color.bg,
                            borderRadius: tkn.radius.sm,
                            padding: tkn.space.sm,
                            border: `1px solid ${tkn.color.borderLight}`,
                          }}
                        >
                          {selected.representative_path ??
                            selected.top_paths?.[0]?.path ??
                            '—'}
                        </div>
                      </div>
                    </div>
                  )}

                  {detailTab === 'transitions' && (
                    <div>
                      <div
                        style={{
                          fontSize: tkn.font.sizeXs,
                          color: tkn.color.textSecondary,
                          marginBottom: tkn.space.sm,
                        }}
                      >
                        Top within‑archetype transitions (channel → channel).
                      </div>
                      <div style={{ overflowX: 'auto' }}>
                        <table
                          style={{
                            width: '100%',
                            borderCollapse: 'collapse',
                            fontSize: tkn.font.sizeSm,
                          }}
                        >
                          <thead>
                            <tr style={{ borderBottom: `2px solid ${tkn.color.border}` }}>
                              <th
                                style={{
                                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                  textAlign: 'left',
                                }}
                              >
                                From → To
                              </th>
                              <th
                                style={{
                                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                  textAlign: 'right',
                                }}
                              >
                                Count
                              </th>
                              <th
                                style={{
                                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                  textAlign: 'right',
                                }}
                              >
                                Share
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {(selected.top_transitions ?? []).map((tr) => (
                              <tr
                                key={`${tr.from}->${tr.to}`}
                                style={{
                                  borderBottom: `1px solid ${tkn.color.borderLight}`,
                                }}
                              >
                                <td
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    color: tkn.color.text,
                                  }}
                                >
                                  {tr.from} → {tr.to}
                                </td>
                                <td
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    textAlign: 'right',
                                    fontVariantNumeric: 'tabular-nums',
                                  }}
                                >
                                  {tr.count.toLocaleString()}
                                </td>
                                <td
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    textAlign: 'right',
                                    fontVariantNumeric: 'tabular-nums',
                                  }}
                                >
                                  {(tr.share * 100).toFixed(1)}%
                                </td>
                              </tr>
                            ))}
                            {(!selected.top_transitions ||
                              selected.top_transitions.length === 0) && (
                              <tr>
                                <td
                                  colSpan={3}
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    color: tkn.color.textSecondary,
                                  }}
                                >
                                  Not enough data to compute transitions.
                                </td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}

                  {detailTab === 'variants' && (
                    <div>
                      <div
                        style={{
                          fontSize: tkn.font.sizeXs,
                          color: tkn.color.textSecondary,
                          marginBottom: tkn.space.sm,
                        }}
                      >
                        Top path variants and outliers within this archetype. Click a row to see
                        path‑level stats.
                      </div>
                      <div style={{ display: 'grid', gap: tkn.space.lg }}>
                        <div>
                          <div
                            style={{
                              fontSize: tkn.font.sizeXs,
                              fontWeight: tkn.font.weightSemibold,
                              color: tkn.color.textSecondary,
                              marginBottom: tkn.space.xs,
                            }}
                          >
                            Top variants
                          </div>
                          <div style={{ overflowX: 'auto' }}>
                            <table
                              style={{
                                width: '100%',
                                borderCollapse: 'collapse',
                                fontSize: tkn.font.sizeSm,
                              }}
                            >
                              <thead>
                                <tr style={{ borderBottom: `2px solid ${tkn.color.border}` }}>
                                  <th
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'left',
                                    }}
                                  >
                                    Path
                                  </th>
                                  <th
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'right',
                                    }}
                                  >
                                    Count
                                  </th>
                                  <th
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'right',
                                    }}
                                  >
                                    Share
                                  </th>
                                </tr>
                              </thead>
                              <tbody>
                                {(selected.variants ?? selected.top_paths ?? []).map((p) => (
                                  <tr
                                    key={`var-${p.path}`}
                                    onClick={() => setSelectedPathForDrawer(p)}
                                    style={{
                                      borderBottom: `1px solid ${tkn.color.borderLight}`,
                                      cursor: 'pointer',
                                    }}
                                  >
                                    <td
                                      style={{
                                        padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                        color: tkn.color.text,
                                      }}
                                    >
                                      {p.path}
                                    </td>
                                    <td
                                      style={{
                                        padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                        textAlign: 'right',
                                        fontVariantNumeric: 'tabular-nums',
                                      }}
                                    >
                                      {p.count.toLocaleString()}
                                    </td>
                                    <td
                                      style={{
                                        padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                        textAlign: 'right',
                                        fontVariantNumeric: 'tabular-nums',
                                        color: tkn.color.accent,
                                      }}
                                    >
                                      {(p.share * 100).toFixed(1)}%
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                        <div>
                          <div
                            style={{
                              fontSize: tkn.font.sizeXs,
                              fontWeight: tkn.font.weightSemibold,
                              color: tkn.color.textSecondary,
                              marginBottom: tkn.space.xs,
                            }}
                          >
                            Outlier paths
                          </div>
                          <div style={{ overflowX: 'auto' }}>
                            <table
                              style={{
                                width: '100%',
                                borderCollapse: 'collapse',
                                fontSize: tkn.font.sizeSm,
                              }}
                            >
                              <thead>
                                <tr style={{ borderBottom: `2px solid ${tkn.color.border}` }}>
                                  <th
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'left',
                                    }}
                                  >
                                    Path
                                  </th>
                                  <th
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'right',
                                    }}
                                  >
                                    Count
                                  </th>
                                  <th
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'right',
                                    }}
                                  >
                                    Share
                                  </th>
                                </tr>
                              </thead>
                              <tbody>
                                {(selected.outlier_paths ?? []).map((p) => (
                                  <tr
                                    key={`out-${p.path}`}
                                    onClick={() => setSelectedPathForDrawer(p)}
                                    style={{
                                      borderBottom: `1px solid ${tkn.color.borderLight}`,
                                      cursor: 'pointer',
                                    }}
                                  >
                                    <td
                                      style={{
                                        padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                        color: tkn.color.text,
                                      }}
                                    >
                                      {p.path}
                                    </td>
                                    <td
                                      style={{
                                        padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                        textAlign: 'right',
                                        fontVariantNumeric: 'tabular-nums',
                                      }}
                                    >
                                      {p.count.toLocaleString()}
                                    </td>
                                    <td
                                      style={{
                                        padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                        textAlign: 'right',
                                        fontVariantNumeric: 'tabular-nums',
                                      }}
                                    >
                                      {(p.share * 100).toFixed(1)}%
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>

                      {selectedPathForDrawer && (
                        <div
                          style={{
                            marginTop: tkn.space.lg,
                            padding: tkn.space.md,
                            borderRadius: tkn.radius.md,
                            border: `1px solid ${tkn.color.borderLight}`,
                            backgroundColor: tkn.color.bg,
                          }}
                        >
                          <div
                            style={{
                              display: 'flex',
                              justifyContent: 'space-between',
                              alignItems: 'center',
                              marginBottom: tkn.space.xs,
                            }}
                          >
                            <strong style={{ fontSize: tkn.font.sizeSm, color: tkn.color.text }}>
                              Path details
                            </strong>
                            <button
                              type="button"
                              onClick={() => setSelectedPathForDrawer(null)}
                              style={{
                                border: 'none',
                                background: 'transparent',
                                color: tkn.color.textSecondary,
                                cursor: 'pointer',
                                fontSize: tkn.font.sizeXs,
                              }}
                            >
                              Close
                            </button>
                          </div>
                          <div
                            style={{
                              fontSize: tkn.font.sizeSm,
                              color: tkn.color.text,
                              marginBottom: tkn.space.xs,
                            }}
                          >
                            {selectedPathForDrawer.path}
                          </div>
                          <div
                            style={{
                              display: 'flex',
                              gap: tkn.space.md,
                              fontSize: tkn.font.sizeXs,
                              color: tkn.color.textSecondary,
                            }}
                          >
                            <div>
                              <strong>Journeys:</strong>{' '}
                              {selectedPathForDrawer.count.toLocaleString()}
                            </div>
                            <div>
                              <strong>Share:</strong>{' '}
                              {(selectedPathForDrawer.share * 100).toFixed(1)}%
                            </div>
                            {selectedPathForDrawer.avg_time_to_conversion_days != null && (
                              <div>
                                <strong>Avg time‑to‑convert:</strong>{' '}
                                {selectedPathForDrawer.avg_time_to_conversion_days.toFixed(1)}d
                              </div>
                            )}
                            {selectedPathForDrawer.avg_length != null && (
                              <div>
                                <strong>Avg length:</strong>{' '}
                                {selectedPathForDrawer.avg_length.toFixed(1)} steps
                              </div>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {detailTab === 'actions' && (
                    <div>
                      <div
                        style={{
                          marginBottom: tkn.space.md,
                          fontSize: tkn.font.sizeXs,
                          color: tkn.color.textSecondary,
                        }}
                      >
                        Next‑best actions are derived from how journeys within this archetype tend to continue from the
                        representative path prefix. Use these as directional hints, not prescriptive rules.
                      </div>
                      <div style={{ marginBottom: tkn.space.md }}>
                        <h4
                          style={{
                            margin: '0 0 4px',
                            fontSize: tkn.font.sizeSm,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.text,
                          }}
                        >
                          Next best channels
                        </h4>
                        <div style={{ overflowX: 'auto' }}>
                          <table
                            style={{
                              width: '100%',
                              borderCollapse: 'collapse',
                              fontSize: tkn.font.sizeSm,
                            }}
                          >
                            <thead>
                              <tr style={{ borderBottom: `2px solid ${tkn.color.border}` }}>
                                <th
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    textAlign: 'left',
                                  }}
                                >
                                  Channel
                                </th>
                                <th
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    textAlign: 'right',
                                  }}
                                >
                                  Support
                                </th>
                                <th
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    textAlign: 'right',
                                  }}
                                >
                                  Share
                                </th>
                                <th
                                  style={{
                                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                    textAlign: 'left',
                                  }}
                                >
                                  Notes
                                </th>
                              </tr>
                            </thead>
                            <tbody>
                              {(selected.actions ?? []).map((a) => (
                                <tr
                                  key={a.channel}
                                  style={{
                                    borderBottom: `1px solid ${tkn.color.borderLight}`,
                                  }}
                                >
                                  <td
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      color: tkn.color.text,
                                    }}
                                  >
                                    {a.channel}
                                  </td>
                                  <td
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'right',
                                      fontVariantNumeric: 'tabular-nums',
                                    }}
                                  >
                                    {a.support.toLocaleString()}
                                  </td>
                                  <td
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      textAlign: 'right',
                                      fontVariantNumeric: 'tabular-nums',
                                    }}
                                  >
                                    {(a.support_share * 100).toFixed(1)}%
                                  </td>
                                  <td
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      color: tkn.color.textSecondary,
                                    }}
                                  >
                                    {a.low_sample ? 'Low sample size – treat as hypothesis.' : 'Sufficient support.'}
                                  </td>
                                </tr>
                              ))}
                              {(!selected.actions || selected.actions.length === 0) && (
                                <tr>
                                  <td
                                    colSpan={4}
                                    style={{
                                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                                      color: tkn.color.textSecondary,
                                    }}
                                  >
                                    Not enough journeys for reliable next‑best‑action suggestions in this archetype.
                                  </td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>

                      {/* What to test suggestions */}
                      <div>
                        <h4
                          style={{
                            margin: '0 0 4px',
                            fontSize: tkn.font.sizeSm,
                            fontWeight: tkn.font.weightSemibold,
                            color: tkn.color.text,
                          }}
                        >
                          What to test
                        </h4>
                        <ul
                          style={{
                            margin: 0,
                            paddingLeft: 18,
                            fontSize: tkn.font.sizeSm,
                            color: tkn.color.textSecondary,
                          }}
                        >
                          {(() => {
                            const bullets: string[] = []
                            if (
                              selected.defining_traits?.some((tr) =>
                                tr.toLowerCase().includes('email'),
                              )
                            ) {
                              bullets.push(
                                'Email appears as a common assist. Test moving email touches earlier in the journey or strengthening follow‑ups.',
                              )
                            }
                            if (
                              selected.defining_traits?.some((tr) =>
                                tr.toLowerCase().includes('retarget'),
                              )
                            ) {
                              bullets.push(
                                'Retargeting is prominent. Experiment with frequency caps and creative sequencing to avoid fatigue.',
                              )
                            }
                            if (
                              selected.length_p90 != null &&
                              selected.length_p90 >= 6
                            ) {
                              bullets.push(
                                'Journeys are long. Test simplifying the funnel or adding stronger nudges at key mid‑funnel steps.',
                              )
                            }
                            if (
                              diagnostics?.journeys_ending_direct_share != null &&
                              diagnostics.journeys_ending_direct_share >= 0.4
                            ) {
                              bullets.push(
                                'Many journeys end in Direct/Unknown. Validate tracking coverage and referrer loss, especially around final touches and checkout.',
                              )
                            }
                            if (bullets.length === 0) {
                              bullets.push(
                                'Use this archetype to design a tailored nurture journey (ad → assist → close) and track uplift against a control group.',
                              )
                            }
                            return bullets.map((b, idx) => <li key={idx}>{b}</li>)
                          })()}
                        </ul>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
