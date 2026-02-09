import { useEffect, useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { tokens } from '../theme/tokens'

interface MMMDashboardProps {
  runId: string
  datasetId: string
}

interface KPI {
  channel: string
  roi: number
}

interface Contrib {
  channel: string
  mean_share: number
}

const METRIC_DEFINITIONS: Record<string, string> = {
  'Total KPI': 'Sum of the target KPI (e.g. conversions, revenue) over the modeled period.',
  'Model Fit (R²)': 'Proportion of variance in the KPI explained by the model.',
  'Optimal Uplift': 'Potential % lift from reallocating spend to the optimal mix.',
  'Channels Modeled': 'Number of media channels in the model.',
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(0)}K`
  return `$${val.toFixed(0)}`
}

function exportRoiCSV(roi: KPI[], contrib: Contrib[]) {
  const headers = ['Channel', 'ROI', 'Contribution Share (%)']
  const rows = roi.map((r) => {
    const c = contrib.find((x) => x.channel === r.channel)
    return [r.channel, r.roi.toFixed(4), c ? (c.mean_share * 100).toFixed(2) : '']
  })
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `mmm-roi-contrib-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export default function MMMDashboard({ runId, datasetId }: MMMDashboardProps) {
  const [dataset, setDataset] = useState<Record<string, unknown>[]>([])
  const t = tokens

  const { data: run } = useQuery({
    queryKey: ['run', runId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${runId}`)
      if (!res.ok) throw new Error('Failed to fetch run')
      return res.json()
    },
  })

  const { data: contrib } = useQuery({
    queryKey: ['contrib', runId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${runId}/contrib`)
      if (!res.ok) throw new Error('Failed to fetch contributions')
      return res.json()
    },
  })

  const { data: roi } = useQuery({
    queryKey: ['roi', runId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${runId}/roi`)
      if (!res.ok) throw new Error('Failed to fetch ROI')
      return res.json()
    },
  })

  useEffect(() => {
    if (datasetId) {
      fetch(`/api/datasets/${datasetId}?preview_only=false`)
        .then((res) => res.json())
        .then((d) => setDataset(d.preview_rows || []))
        .catch(() => setDataset([]))
    }
  }, [datasetId])

  const kpiTotal = useMemo(() => {
    const kpiCol = run?.config?.kpi
    if (!kpiCol) return 0
    return dataset.reduce((sum, row) => sum + (Number(row[kpiCol]) || 0), 0)
  }, [dataset, run?.config?.kpi])

  const totalSpend = useMemo(() => {
    const chs = run?.config?.spend_channels || []
    return dataset.reduce((sum, row) => {
      let rowSpend = 0
      chs.forEach((ch: string) => { rowSpend += Number(row[ch]) || 0 })
      return sum + rowSpend
    }, 0)
  }, [dataset, run?.config?.spend_channels])

  const weightedROI = roi?.length ? roi.reduce((s: number, r: KPI) => s + r.roi, 0) / roi.length : 0
  const uplift = run?.uplift ?? 0

  const getKpiDisplayName = () => {
    const mode = run?.kpi_mode || run?.config?.kpi_mode || 'conversions'
    const map: Record<string, string> = {
      conversions: 'Conversions',
      aov: 'Average Order Value (AOV)',
      profit: 'Profitability',
    }
    return map[mode] || 'MMM'
  }

  const kpis = [
    { label: 'Total KPI', value: kpiTotal.toLocaleString(), def: METRIC_DEFINITIONS['Total KPI'] },
    { label: 'Model Fit (R²)', value: run?.r2 != null ? run.r2.toFixed(3) : '—', def: METRIC_DEFINITIONS['Model Fit (R²)'] },
    { label: 'Optimal Uplift', value: uplift ? `+${(uplift * 100).toFixed(1)}%` : '—', def: METRIC_DEFINITIONS['Optimal Uplift'] },
    { label: 'Channels Modeled', value: String(run?.config?.spend_channels?.length ?? 0), def: METRIC_DEFINITIONS['Channels Modeled'] },
  ]

  const datasetWithTotalSpend = useMemo(() => {
    const chs = run?.config?.spend_channels || []
    return dataset.map((row) => {
      const total_spend = chs.reduce((s: number, ch: string) => s + (Number(row[ch]) || 0), 0)
      return { ...row, total_spend }
    })
  }, [dataset, run?.config?.spend_channels])

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
        {getKpiDisplayName()} Dashboard
      </h1>
      <p style={{ margin: `${t.space.xs}px 0 ${t.space.xl}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        Bayesian MMM results: channel ROI, contribution share, and time series.
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
                color: kpi.label === 'Optimal Uplift' && uplift > 0 ? t.color.success : t.color.text,
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
      <style>{`@media (max-width: 900px) { .mmm-charts { grid-template-columns: 1fr !important; } }`}</style>
      <div
        className="mmm-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: t.space.xl,
          marginBottom: t.space.xl,
        }}
      >
        {dataset.length > 0 && (
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
              Spend vs KPI Over Time
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dataset} margin={{ top: 8, right: 16, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis dataKey="date" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <YAxis yAxisId="left" orientation="left" stroke={t.color.chart[0]} tick={{ fontSize: t.font.sizeSm }} />
                <YAxis yAxisId="right" orientation="right" stroke={t.color.chart[1]} tick={{ fontSize: t.font.sizeSm }} />
                <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
                {(run?.config?.spend_channels || []).slice(0, 2).map((channel: string, idx: number) => (
                  <Line
                    key={channel}
                    yAxisId="left"
                    type="monotone"
                    dataKey={channel}
                    stroke={t.color.chart[idx % t.color.chart.length]}
                    name={`${channel} Spend`}
                  />
                ))}
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey={run?.config?.kpi || 'sales'}
                  stroke={t.color.warning}
                  strokeWidth={2}
                  name="KPI"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {dataset.length > 0 && (
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
              Total Spend Trend
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={datasetWithTotalSpend} margin={{ top: 8, right: 16, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis dataKey="date" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
                <Area type="monotone" dataKey="total_spend" stroke={t.color.accent} fill={t.color.accent} fillOpacity={0.2} name="Total Spend" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* ROI by Channel */}
      {roi && roi.length > 0 && (
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
            ROI by Channel
          </h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={roi} margin={{ top: 8, right: 16, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
              <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
              <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
              <Bar dataKey="roi" name="ROI" radius={[4, 4, 0, 0]}>
                {roi.map((_, i) => (
                  <Cell key={i} fill={t.color.chart[i % t.color.chart.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Channel Contribution */}
      {contrib && contrib.length > 0 && (
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
            Channel Contribution Share
          </h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={contrib} margin={{ top: 8, right: 16, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis dataKey="channel" tick={{ fontSize: t.font.sizeSm, fill: t.color.text }} />
              <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
              <Tooltip formatter={(value: number) => `${(value * 100).toFixed(1)}%`} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
              <Bar dataKey="mean_share" name="Contribution" radius={[4, 4, 0, 0]}>
                {contrib.map((_, i) => (
                  <Cell key={i} fill={t.color.chart[(i + 2) % t.color.chart.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Summary + ROI/Contrib table + export */}
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
            Summary & Channel Metrics
          </h3>
          {roi?.length && contrib?.length ? (
            <button
              type="button"
              onClick={() => exportRoiCSV(roi, contrib)}
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
          ) : null}
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.xl, marginBottom: roi?.length ? t.space.xl : 0 }}>
          <div>
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted, marginBottom: 4 }}>Total Spend</p>
            <p style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightBold, color: t.color.text, margin: 0, fontVariantNumeric: 'tabular-nums' }}>
              {formatCurrency(totalSpend)}
            </p>
          </div>
          <div>
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted, marginBottom: 4 }}>Avg ROI per Channel</p>
            <p style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightBold, color: weightedROI > 1 ? t.color.success : t.color.danger, margin: 0, fontVariantNumeric: 'tabular-nums' }}>
              {weightedROI.toFixed(2)}×
            </p>
          </div>
          <div>
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted, marginBottom: 4 }}>Data Points</p>
            <p style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightBold, color: t.color.text, margin: 0, fontVariantNumeric: 'tabular-nums' }}>
              {dataset.length} weeks
            </p>
          </div>
        </div>
        {roi && roi.length > 0 && contrib && contrib.length > 0 && (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                  <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Channel</th>
                  <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>ROI</th>
                  <th style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>Contribution</th>
                </tr>
              </thead>
              <tbody>
                {roi.map((r, idx) => {
                  const c = contrib.find((x) => x.channel === r.channel)
                  return (
                    <tr key={r.channel} style={{ borderBottom: `1px solid ${t.color.borderLight}`, backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg }}>
                      <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, fontWeight: t.font.weightMedium, color: t.color.text }}>{r.channel}</td>
                      <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', color: r.roi >= 0 ? t.color.success : t.color.danger, fontVariantNumeric: 'tabular-nums' }}>
                        {r.roi.toFixed(4)}
                      </td>
                      <td style={{ padding: `${t.space.md}px ${t.space.lg}px`, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{c ? `${(c.mean_share * 100).toFixed(2)}%` : '—'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
