import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import ConfidenceBadge, { Confidence } from '../components/ConfidenceBadge'
import ExplainabilityPanel from '../components/ExplainabilityPanel'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts'
import { tokens } from '../theme/tokens'

interface ChannelPerformanceProps {
  model: string
  channels: string[]
  modelsReady: boolean
  configId?: string | null
}

interface ChannelData {
  channel: string
  spend: number
  attributed_value: number
  attributed_conversions: number
  attributed_share: number
  roi: number
  roas: number
  cpa: number
  confidence?: Confidence
}

interface PerformanceResponse {
  model: string
  channels: ChannelData[]
  total_spend: number
  total_attributed_value: number
  total_conversions: number
  config?: {
    id: string
    name: string
    version: number
  } | null
}

const MODEL_LABELS: Record<string, string> = {
  last_touch: 'Last Touch',
  first_touch: 'First Touch',
  linear: 'Linear',
  time_decay: 'Time Decay',
  position_based: 'Position Based',
  markov: 'Data-Driven (Markov)',
}

const METRIC_DEFINITIONS: Record<string, string> = {
  'Total Spend': 'Sum of expenses mapped to channels in the selected period.',
  'Attributed Revenue': 'Revenue attributed to each channel by the selected model.',
  'Conversions': 'Attributed conversion count.',
  'ROAS': 'Return on ad spend: attributed revenue ÷ spend.',
  'ROI': 'Return on investment: (attributed value − spend) ÷ spend.',
  'CPA': 'Cost per acquisition: spend ÷ attributed conversions.',
  'Share': 'Share of total attributed revenue.',
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}K`
  return `$${val.toFixed(0)}`
}

function exportTableCSV(channels: ChannelData[]) {
  const headers = ['Channel', 'Spend', 'Attributed Revenue', 'Conversions', 'Share %', 'ROI %', 'ROAS', 'CPA']
  const rows = channels.map((ch) => [
    ch.channel,
    ch.spend.toFixed(2),
    ch.attributed_value.toFixed(2),
    ch.attributed_conversions.toFixed(1),
    (ch.attributed_share * 100).toFixed(1),
    (ch.roi * 100).toFixed(1),
    ch.roas.toFixed(2),
    ch.cpa > 0 ? ch.cpa.toFixed(2) : '',
  ])
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `channel-performance-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

type SortKey = keyof ChannelData | 'attributed_share'
type SortDir = 'asc' | 'desc'

export default function ChannelPerformance({ model, modelsReady, configId }: ChannelPerformanceProps) {
  const [sortKey, setSortKey] = useState<SortKey>('attributed_value')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [showWhy, setShowWhy] = useState(false)

  const perfQuery = useQuery<PerformanceResponse>({
    queryKey: ['channel-performance', model, configId ?? 'default'],
    queryFn: async () => {
      const params = new URLSearchParams({ model })
      if (configId) params.append('config_id', configId)
      const res = await fetch(`/api/attribution/performance?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to fetch performance')
      return res.json()
    },
    enabled: modelsReady,
    refetchInterval: false,
  })

  const data = perfQuery.data
  const loading = perfQuery.isLoading || !modelsReady

  const sortedChannels = useMemo(() => {
    if (!data?.channels?.length) return []
    const key = sortKey === 'attributed_share' ? 'attributed_share' : sortKey
    return [...data.channels].sort((a, b) => {
      const va = a[key as keyof ChannelData] as number
      const vb = b[key as keyof ChannelData] as number
      const cmp = typeof va === 'number' && typeof vb === 'number' ? va - vb : String(va).localeCompare(String(vb))
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [data?.channels, sortKey, sortDir])

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const t = tokens

  if (loading) {
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
          Loading attribution data…
        </p>
        <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted, marginTop: t.space.sm }}>
          Models are being computed. This may take a moment.
        </p>
      </div>
    )
  }

  if (!data || !data.channels?.length) {
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
          No performance data
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          Load conversion journeys and map expenses to channels, then run attribution models.
        </p>
      </div>
    )
  }

  const totalROI = data.total_spend > 0 ? (data.total_attributed_value - data.total_spend) / data.total_spend : 0
  const totalROAS = data.total_spend > 0 ? data.total_attributed_value / data.total_spend : 0
  const avgCPA = data.total_conversions > 0 ? data.total_spend / data.total_conversions : 0

  const kpis = [
    { label: 'Total Spend', value: formatCurrency(data.total_spend), def: METRIC_DEFINITIONS['Total Spend'] },
    { label: 'Attributed Revenue', value: formatCurrency(data.total_attributed_value), def: METRIC_DEFINITIONS['Attributed Revenue'] },
    { label: 'Conversions', value: data.total_conversions.toLocaleString(), def: METRIC_DEFINITIONS['Conversions'] },
    { label: 'ROAS', value: `${totalROAS.toFixed(2)}×`, def: METRIC_DEFINITIONS['ROAS'] },
    { label: 'Avg CPA', value: formatCurrency(avgCPA), def: METRIC_DEFINITIONS['CPA'] },
  ]

  const tableColumns: { key: SortKey; label: string; align: 'left' | 'right'; format: (ch: ChannelData) => string }[] = [
    { key: 'channel', label: 'Channel', align: 'left', format: (ch) => ch.channel },
    { key: 'spend', label: 'Spend', align: 'right', format: (ch) => formatCurrency(ch.spend) },
    { key: 'attributed_value', label: 'Attributed Revenue', align: 'right', format: (ch) => formatCurrency(ch.attributed_value) },
    { key: 'attributed_conversions', label: 'Conversions', align: 'right', format: (ch) => ch.attributed_conversions.toFixed(1) },
    { key: 'attributed_share', label: 'Share', align: 'right', format: (ch) => `${(ch.attributed_share * 100).toFixed(1)}%` },
    { key: 'roi', label: 'ROI', align: 'right', format: (ch) => `${(ch.roi * 100).toFixed(0)}%` },
    { key: 'roas', label: 'ROAS', align: 'right', format: (ch) => `${ch.roas.toFixed(2)}×` },
    { key: 'cpa', label: 'CPA', align: 'right', format: (ch) => (ch.cpa > 0 ? formatCurrency(ch.cpa) : '—') },
  ]

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.xl,
          flexWrap: 'wrap',
          gap: t.space.md,
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
            Channel Performance
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Attribution model: <strong style={{ color: t.color.accent }}>{MODEL_LABELS[model] || model}</strong>
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
          <ExplainabilityPanel scope="channel" configId={configId ?? undefined} />
        </div>
      )}

      {/* Metrics strip */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
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

      {/* Charts */}
      <div
        className="cp-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1.6fr 1fr',
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
          <h3
            style={{
              margin: `0 0 ${t.space.lg}px`,
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            Spend vs. Attributed Revenue by Channel
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={data.channels} layout="vertical" margin={{ left: 8, right: 16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis type="number" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => formatCurrency(v)} />
              <YAxis type="category" dataKey="channel" width={100} tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
              <Tooltip
                formatter={(value: number) => formatCurrency(value)}
                contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
              />
              <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
              <Bar dataKey="spend" fill={t.color.danger} name="Spend" radius={[0, 4, 4, 0]} />
              <Bar dataKey="attributed_value" fill={t.color.success} name="Attributed Revenue" radius={[0, 4, 4, 0]} />
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
          <h3
            style={{
              margin: `0 0 ${t.space.lg}px`,
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
            }}
          >
            Revenue Attribution Share
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={data.channels}
                dataKey="attributed_share"
                nameKey="channel"
                cx="50%"
                cy="50%"
                innerRadius={56}
                outerRadius={90}
                paddingAngle={1}
                label={({ channel, attributed_share }) => `${channel} ${(attributed_share * 100).toFixed(0)}%`}
              >
                {data.channels.map((_, i) => (
                  <Cell key={`cell-${i}`} fill={t.color.chart[i % t.color.chart.length]} />
                ))}
              </Pie>
              <Tooltip formatter={(value: number) => `${(value * 100).toFixed(1)}%`} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ROI / ROAS chart */}
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
        <h3 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          ROI & ROAS by Channel
        </h3>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={data.channels} margin={{ top: 8, right: 16, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
            <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
            <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
            <Tooltip formatter={(value: number) => value.toFixed(2)} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
            <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
            <Bar dataKey="roas" fill={t.color.chart[3]} name="ROAS" radius={[4, 4, 0, 0]} />
            <Bar dataKey="roi" fill={t.color.chart[4]} name="ROI" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Sortable detail table + export */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: t.space.lg,
            flexWrap: 'wrap',
            gap: t.space.md,
          }}
        >
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Channel Detail
          </h3>
          <button
            type="button"
            onClick={() => exportTableCSV(data.channels)}
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
        <style>{`
          .cp-table tbody tr:hover { background: ${t.color.accentMuted} !important; }
          @media (max-width: 900px) { .cp-charts { grid-template-columns: 1fr !important; } }
        `}</style>
        <div style={{ overflowX: 'auto' }}>
          <table className="cp-table" style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                {tableColumns.map((col) => (
                  <th
                    key={col.key}
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: col.align,
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      whiteSpace: 'nowrap',
                      cursor: 'pointer',
                      userSelect: 'none',
                    }}
                    onClick={() => handleSort(col.key)}
                    title={`Sort by ${col.label}`}
                  >
                    {col.label}
                    {sortKey === col.key && (sortDir === 'asc' ? ' ↑' : ' ↓')}
                  </th>
                ))}
                <th
                  style={{
                    padding: `${t.space.md}px ${t.space.lg}px`,
                    textAlign: 'right',
                    fontWeight: t.font.weightSemibold,
                    color: t.color.textSecondary,
                    whiteSpace: 'nowrap',
                  }}
                >
                  Confidence
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedChannels.map((ch, idx) => (
                <tr
                  key={ch.channel}
                  style={{
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg,
                  }}
                >
                  {tableColumns.map((col) => {
                    const isChannel = col.key === 'channel'
                    const isRoi = col.key === 'roi'
                    const isValue = col.key === 'attributed_value'
                    return (
                      <td
                        key={col.key}
                        style={{
                          padding: `${t.space.md}px ${t.space.lg}px`,
                          textAlign: col.align,
                          fontWeight: isChannel || isValue || isRoi ? t.font.weightMedium : t.font.weightNormal,
                          color: isValue ? t.color.success : isRoi ? (ch.roi >= 0 ? t.color.success : t.color.danger) : t.color.text,
                          fontVariantNumeric: col.align === 'right' ? 'tabular-nums' : undefined,
                        }}
                      >
                        {col.format(ch)}
                      </td>
                    )
                  })}
                  <td
                    style={{
                      padding: `${t.space.md}px ${t.space.lg}px`,
                      textAlign: 'right',
                    }}
                  >
                    <ConfidenceBadge confidence={ch.confidence} compact />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
