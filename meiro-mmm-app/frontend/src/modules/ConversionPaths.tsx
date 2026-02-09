import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'

interface PathAnalysis {
  total_journeys: number
  avg_path_length: number
  avg_time_to_conversion_days: number | null
  common_paths: { path: string; count: number; share: number }[]
  channel_frequency: Record<string, number>
  path_length_distribution: { min: number; max: number; median: number }
}

const METRIC_DEFINITIONS: Record<string, string> = {
  'Total Journeys': 'Number of customer paths (converted or not) in the dataset.',
  'Avg Path Length': 'Average number of touchpoints per journey before conversion.',
  'Avg Time to Convert': 'Average days from first touch to conversion.',
  'Path Length Range': 'Min and max touchpoints observed in paths.',
}

function exportPathsCSV(paths: { path: string; count: number; share: number }[]) {
  const headers = ['Path', 'Count', 'Share (%)']
  const rows = paths.map((p) => [p.path, p.count.toString(), (p.share * 100).toFixed(1)])
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `conversion-paths-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export default function ConversionPaths() {
  const [pathSort, setPathSort] = useState<'count' | 'share'>('count')
  const [pathSortDir, setPathSortDir] = useState<'asc' | 'desc'>('desc')
  const [freqSort, setFreqSort] = useState<'channel' | 'count' | 'pct'>('count')
  const [freqSortDir, setFreqSortDir] = useState<'asc' | 'desc'>('desc')

  const pathsQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/paths')
      if (!res.ok) throw new Error('Failed to fetch path analysis')
      return res.json()
    },
  })

  const data = pathsQuery.data
  const t = tokens

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

  const totalTouchpoints = Object.values(data.channel_frequency).reduce((s, n) => s + n, 0)
  const freqRows = Object.entries(data.channel_frequency).map(([channel, count]) => ({
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

  const sortedPaths = useMemo(() => {
    return [...data.common_paths].sort((a, b) => {
      const cmp = pathSort === 'count' ? a.count - b.count : a.share - b.share
      return pathSortDir === 'asc' ? cmp : -cmp
    })
  }, [data.common_paths, pathSort, pathSortDir])

  const kpis = [
    { label: 'Total Journeys', value: data.total_journeys.toLocaleString(), def: METRIC_DEFINITIONS['Total Journeys'] },
    { label: 'Avg Path Length', value: `${data.avg_path_length} touchpoints`, def: METRIC_DEFINITIONS['Avg Path Length'] },
    { label: 'Avg Time to Convert', value: data.avg_time_to_conversion_days != null ? `${data.avg_time_to_conversion_days} days` : 'N/A', def: METRIC_DEFINITIONS['Avg Time to Convert'] },
    { label: 'Path Length Range', value: `${data.path_length_distribution.min} – ${data.path_length_distribution.max}`, def: METRIC_DEFINITIONS['Path Length Range'] },
  ]

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
        Conversion Path Analysis
      </h1>
      <p style={{ margin: `${t.space.xs}px 0 ${t.space.xl}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        How customers interact with channels before converting.
      </p>

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
          <button
            type="button"
            onClick={() => exportPathsCSV(data.common_paths)}
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
              </tr>
            </thead>
            <tbody>
              {sortedPaths.slice(0, 20).map((p, idx) => (
                <tr
                  key={idx}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
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
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {data.common_paths.length > 20 && (
          <p style={{ margin: `${t.space.md}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Showing top 20 of {data.common_paths.length} paths.
          </p>
        )}
      </div>
    </div>
  )
}
