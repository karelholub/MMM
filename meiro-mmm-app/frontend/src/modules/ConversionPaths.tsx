import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { tokens } from '../theme/tokens'
import ExplainabilityPanel from '../components/ExplainabilityPanel'

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
  common_paths: { path: string; count: number; share: number }[]
  channel_frequency: Record<string, number>
  path_length_distribution: { min: number; max: number; median: number }
  next_best_by_prefix?: Record<string, NextBestRec[]>
  next_best_by_prefix_campaign?: Record<string, NextBestRec[]>
}

interface PathAnomaly {
  type: string
  severity: 'info' | 'warn' | 'critical' | string
  metric_key: string
  metric_value: number
  message: string
}

const METRIC_DEFINITIONS: Record<string, string> = {
  'Total Journeys': 'Number of customer paths (converted or not) in the dataset.',
  'Avg Path Length': 'Average number of touchpoints per journey before conversion.',
  'Avg Time to Convert': 'Average days from first touch to conversion.',
  'Path Length Range': 'Min and max touchpoints observed in paths.',
}

function exportPathsCSV(
  paths: { path: string; count: number; share: number }[],
  nextBestByPrefix?: Record<string, NextBestRec[]>
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
  const csv = [headers.join(','), ...rows.map((r) => (Array.isArray(r) ? r : [r]).join(','))].join('\n')
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
  const [tryPathInput, setTryPathInput] = useState('')
  const [tryPathLevel, setTryPathLevel] = useState<'channel' | 'campaign'>('channel')
  const [tryPathResult, setTryPathResult] = useState<{ path_so_far: string; level: string; recommendations: NextBestRec[] } | null>(null)
  const [tryPathLoading, setTryPathLoading] = useState(false)
  const [tryPathError, setTryPathError] = useState<string | null>(null)
  const [showWhy, setShowWhy] = useState(false)

  const pathsQuery = useQuery<PathAnalysis>({
    queryKey: ['path-analysis'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/paths')
      if (!res.ok) throw new Error('Failed to fetch path analysis')
      return res.json()
    },
  })

  const anomaliesQuery = useQuery<{ anomalies: PathAnomaly[] }>({
    queryKey: ['path-anomalies'],
    queryFn: async () => {
      const res = await fetch('/api/paths/anomalies')
      if (!res.ok) throw new Error('Failed to fetch path anomalies')
      return res.json()
    },
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
  const sortedPaths = useMemo(() => {
    return [...commonPaths].sort((a, b) => {
      const cmp = pathSort === 'count' ? a.count - b.count : a.share - b.share
      return pathSortDir === 'asc' ? cmp : -cmp
    })
  }, [commonPaths, pathSort, pathSortDir])

  const kpis = data
    ? [
        { label: 'Total Journeys', value: data.total_journeys.toLocaleString(), def: METRIC_DEFINITIONS['Total Journeys'] },
        { label: 'Avg Path Length', value: `${data.avg_path_length} touchpoints`, def: METRIC_DEFINITIONS['Avg Path Length'] },
        { label: 'Avg Time to Convert', value: data.avg_time_to_conversion_days != null ? `${data.avg_time_to_conversion_days} days` : 'N/A', def: METRIC_DEFINITIONS['Avg Time to Convert'] },
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
          <strong
            style={{
              display: 'block',
              fontSize: t.font.sizeSm,
              color: t.color.warning,
              marginBottom: t.space.xs,
            }}
          >
            Path anomalies detected
          </strong>
          <ul
            style={{
              margin: 0,
              paddingLeft: 20,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
            }}
          >
            {anomaliesQuery.data.anomalies.map((a, idx) => (
              <li key={`${a.type}-${idx}`}>{a.message}</li>
            ))}
          </ul>
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
              setTryPathLoading(true)
              try {
                const pathParam = encodeURIComponent(tryPathInput.trim())
                const levelParam = tryPathLevel === 'campaign' && data.next_best_by_prefix_campaign ? 'campaign' : 'channel'
                const res = await fetch(`/api/attribution/next_best_action?path_so_far=${pathParam}&level=${levelParam}`)
                if (!res.ok) {
                  const err = await res.json().catch(() => ({}))
                  throw new Error(err.detail || res.statusText)
                }
                const json = await res.json()
                setTryPathResult({ path_so_far: json.path_so_far, level: json.level, recommendations: json.recommendations || [] })
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
                      {(rec.conversion_rate * 100).toFixed(0)}% conversion · {rec.count} journeys · avg ${rec.avg_value}
                    </span>
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
          <button
            type="button"
            onClick={() => exportPathsCSV(data.common_paths, data.next_best_by_prefix)}
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
                <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Suggested next</th>
              </tr>
            </thead>
            <tbody>
              {sortedPaths.slice(0, 20).map((p, idx) => {
                const prefix = p.path.split(' > ').slice(0, -1).join(' > ')
                const recs = data.next_best_by_prefix?.[prefix]
                const top = recs?.[0]
                return (
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
        {data.common_paths.length > 20 && (
          <p style={{ margin: `${t.space.md}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
            Showing top 20 of {data.common_paths.length} paths.
          </p>
        )}
      </div>
    </div>
  )
}
