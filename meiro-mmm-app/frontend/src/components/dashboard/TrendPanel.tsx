import { type ReactNode, useMemo, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { tokens as t } from '../../theme/tokens'

export type TrendGrain = 'auto' | 'daily' | 'weekly'

export interface TrendPoint {
  ts: string
  value: number | null
}

export interface TrendMetric {
  key: string
  label: string
  current: TrendPoint[]
  previous?: TrendPoint[]
  summaryMode?: 'sum' | 'avg' | 'last'
  formatValue?: (value: number) => string
}

interface TrendPanelProps {
  title: string
  subtitle?: string
  metrics: TrendMetric[]
  defaultMetricKey?: string
  defaultGrain?: TrendGrain
  defaultCompare?: boolean
  metricKey?: string
  onMetricKeyChange?: (key: string) => void
  grain?: TrendGrain
  onGrainChange?: (grain: TrendGrain) => void
  compare?: boolean
  onCompareChange?: (compare: boolean) => void
  showMetricSelector?: boolean
  showGrainSelector?: boolean
  showCompareToggle?: boolean
  showTableToggle?: boolean
  compact?: boolean
  badge?: ReactNode
  footerNote?: string
  baselineLabel?: string
  infoTooltip?: string
  noDataMessage?: string
  onOpenDataSources?: () => void
  onOpenDataQuality?: () => void
}

function toDate(ts: string): Date | null {
  const d = new Date(ts)
  return Number.isNaN(d.getTime()) ? null : d
}

function periodDays(points: TrendPoint[]): number {
  const valid = points.map((p) => toDate(p.ts)).filter((d): d is Date => Boolean(d))
  if (!valid.length) return 0
  const min = valid.reduce((a, b) => (a < b ? a : b))
  const max = valid.reduce((a, b) => (a > b ? a : b))
  return Math.max(1, Math.floor((max.getTime() - min.getTime()) / (24 * 60 * 60 * 1000)) + 1)
}

function grainFor(metric: TrendMetric, selected: TrendGrain): 'daily' | 'weekly' {
  if (selected !== 'auto') return selected
  return periodDays(metric.current) <= 45 ? 'daily' : 'weekly'
}

function weekStartIso(date: Date): string {
  const copy = new Date(date)
  const day = copy.getDay()
  const diff = day === 0 ? -6 : 1 - day
  copy.setDate(copy.getDate() + diff)
  copy.setHours(0, 0, 0, 0)
  return copy.toISOString().slice(0, 10)
}

function bucketSeries(points: TrendPoint[], grain: 'daily' | 'weekly'): TrendPoint[] {
  if (!points.length) return []
  if (grain === 'daily') {
    return points.map((p) => ({ ts: p.ts.slice(0, 10), value: p.value }))
  }
  const buckets = new Map<string, { sum: number; count: number }>()
  points.forEach((p) => {
    const d = toDate(p.ts)
    if (!d) return
    const key = weekStartIso(d)
    const entry = buckets.get(key) || { sum: 0, count: 0 }
    if (typeof p.value === 'number' && Number.isFinite(p.value)) {
      entry.sum += p.value
      entry.count += 1
    }
    buckets.set(key, entry)
  })
  return Array.from(buckets.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([ts, v]) => ({ ts, value: v.count > 0 ? v.sum : null }))
}

function summarize(points: TrendPoint[], mode: 'sum' | 'avg' | 'last'): number | null {
  const nums = points.map((p) => p.value).filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
  if (!nums.length) return null
  if (mode === 'last') return nums[nums.length - 1]
  if (mode === 'avg') return nums.reduce((a, b) => a + b, 0) / nums.length
  return nums.reduce((a, b) => a + b, 0)
}

function formatAxisLabel(ts: string, grain: 'daily' | 'weekly'): string {
  const d = toDate(ts)
  if (!d) return ts
  if (grain === 'weekly') return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

export default function TrendPanel({
  title,
  subtitle,
  metrics,
  defaultMetricKey,
  defaultGrain = 'auto',
  defaultCompare = true,
  metricKey: controlledMetricKey,
  onMetricKeyChange,
  grain: controlledGrain,
  onGrainChange,
  compare: controlledCompare,
  onCompareChange,
  showMetricSelector = true,
  showGrainSelector = true,
  showCompareToggle = true,
  showTableToggle = true,
  compact = false,
  badge,
  footerNote,
  baselineLabel,
  infoTooltip,
  noDataMessage,
  onOpenDataSources,
  onOpenDataQuality,
}: TrendPanelProps) {
  const [metricKeyState, setMetricKeyState] = useState(defaultMetricKey || metrics[0]?.key || '')
  const [grainState, setGrainState] = useState<TrendGrain>(defaultGrain)
  const [compareState, setCompareState] = useState(defaultCompare)
  const [viewAsTable, setViewAsTable] = useState(false)
  const metricKey = controlledMetricKey ?? metricKeyState
  const grain = controlledGrain ?? grainState
  const compare = controlledCompare ?? compareState
  const setMetricKey = (key: string) => {
    if (onMetricKeyChange) onMetricKeyChange(key)
    else setMetricKeyState(key)
  }
  const setGrain = (next: TrendGrain) => {
    if (onGrainChange) onGrainChange(next)
    else setGrainState(next)
  }
  const setCompare = (next: boolean) => {
    if (onCompareChange) onCompareChange(next)
    else setCompareState(next)
  }

  const metric = useMemo(
    () => metrics.find((m) => m.key === metricKey) || metrics[0],
    [metrics, metricKey],
  )
  const resolvedGrain = metric ? grainFor(metric, grain) : 'daily'
  const current = metric ? bucketSeries(metric.current, resolvedGrain) : []
  const previous = metric ? bucketSeries(metric.previous || [], resolvedGrain) : []
  const hasPrevious = compare && previous.length > 0
  const mode = metric?.summaryMode || 'sum'
  const currentSummary = summarize(current, mode)
  const prevSummary = hasPrevious ? summarize(previous, mode) : null
  const deltaPct =
    currentSummary != null && prevSummary != null && Math.abs(prevSummary) > 1e-9
      ? ((currentSummary - prevSummary) / prevSummary) * 100
      : null
  const period = periodDays(metric?.current || [])
  const baseline = baselineLabel || `vs previous ${period || 0} ${period === 1 ? 'day' : 'days'}`
  const formatter = metric?.formatValue || ((v: number) => v.toLocaleString())

  const rows = useMemo(() => {
    if (!current.length) return []
    return current.map((p, idx) => ({
      ts: p.ts,
      current: p.value,
      previous: hasPrevious ? previous[idx]?.value ?? null : null,
    }))
  }, [current, previous, hasPrevious])

  const validCount = current.filter((p) => p.value != null).length
  const sparse = current.length > 0 && validCount < Math.max(2, Math.floor(current.length * 0.6))
  const empty = current.length === 0 || validCount === 0

  return (
    <div
      style={{
        display: 'grid',
        gap: compact ? t.space.sm : t.space.md,
        padding: compact ? t.space.lg : t.space.xl,
        background: t.color.surface,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.borderLight}`,
        boxShadow: t.shadowSm,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
        <div style={{ display: 'grid', gap: 2 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.xs }}>
            <h3 style={{ margin: 0, fontSize: compact ? t.font.sizeBase : t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              {title}
            </h3>
            {infoTooltip ? (
              <span
                title={infoTooltip}
                style={{
                  display: 'inline-flex',
                  width: 16,
                  height: 16,
                  alignItems: 'center',
                  justifyContent: 'center',
                  borderRadius: 999,
                  border: `1px solid ${t.color.border}`,
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeXs,
                  fontWeight: t.font.weightSemibold,
                }}
              >
                ?
              </span>
            ) : null}
          </div>
          {subtitle ? <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{subtitle}</span> : null}
        </div>
        {metric ? (
          <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
            {badge}
            {showMetricSelector && metrics.length > 1 ? (
              <select
                value={metric.key}
                onChange={(e) => setMetricKey(e.target.value)}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  background: t.color.surface,
                  color: t.color.text,
                }}
              >
                {metrics.map((m) => (
                  <option key={m.key} value={m.key}>
                    {m.label}
                  </option>
                ))}
              </select>
            ) : null}
            {showGrainSelector ? (
              <select
                value={grain}
                onChange={(e) => setGrain(e.target.value as TrendGrain)}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  background: t.color.surface,
                  color: t.color.text,
                }}
              >
                <option value="auto">Auto</option>
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
              </select>
            ) : null}
            {showCompareToggle ? (
              <label style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <input
                  type="checkbox"
                  checked={compare}
                  onChange={(e) => setCompare(e.target.checked)}
                />
                Compare to previous period
              </label>
            ) : null}
            {showTableToggle ? (
              <button
                type="button"
                onClick={() => setViewAsTable((v) => !v)}
                style={{
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  background: t.color.surface,
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeSm,
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  cursor: 'pointer',
                }}
              >
                {viewAsTable ? 'View chart' : 'View as table'}
              </button>
            ) : null}
          </div>
        ) : null}
      </div>

      {metric && !empty && (
        <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap', fontSize: t.font.sizeSm }}>
          <span style={{ color: t.color.textSecondary }}>
            {resolvedGrain === 'daily' ? 'Daily trend for selected period' : 'Weekly trend for selected period'}
          </span>
          <span style={{ color: t.color.text }}>
            Current: <strong>{currentSummary != null ? formatter(currentSummary) : '—'}</strong>
          </span>
          <span
            style={{
              color:
                deltaPct == null
                  ? t.color.textMuted
                  : deltaPct >= 0
                  ? t.color.success
                  : t.color.danger,
              fontWeight: t.font.weightMedium,
            }}
          >
            {deltaPct == null ? 'Δ N/A' : `${deltaPct >= 0 ? '+' : ''}${deltaPct.toFixed(1)}%`} {baseline}
          </span>
          <span style={{ color: t.color.textSecondary }}>Change vs previous period</span>
        </div>
      )}

      {empty ? (
        <div
          style={{
            border: `1px dashed ${t.color.border}`,
            borderRadius: t.radius.md,
            background: t.color.bg,
            color: t.color.textSecondary,
            fontSize: t.font.sizeSm,
            padding: t.space.lg,
            display: 'grid',
            gap: t.space.sm,
          }}
        >
          <span>{noDataMessage || 'No data for selected filters and date range.'}</span>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            {onOpenDataSources ? (
              <button
                type="button"
                onClick={onOpenDataSources}
                style={{
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  background: t.color.surface,
                  cursor: 'pointer',
                  fontSize: t.font.sizeXs,
                }}
              >
                Open Data Sources
              </button>
            ) : null}
            {onOpenDataQuality ? (
              <button
                type="button"
                onClick={onOpenDataQuality}
                style={{
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  background: t.color.surface,
                  cursor: 'pointer',
                  fontSize: t.font.sizeXs,
                }}
              >
                Open Data Quality
              </button>
            ) : null}
          </div>
        </div>
      ) : viewAsTable ? (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${t.color.border}` }}>
                <th style={{ textAlign: 'left', padding: `${t.space.xs}px ${t.space.sm}px` }}>Date</th>
                <th style={{ textAlign: 'right', padding: `${t.space.xs}px ${t.space.sm}px` }}>Current</th>
                {hasPrevious ? (
                  <th style={{ textAlign: 'right', padding: `${t.space.xs}px ${t.space.sm}px` }}>Previous</th>
                ) : null}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.ts} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                  <td style={{ padding: `${t.space.xs}px ${t.space.sm}px` }}>{formatAxisLabel(row.ts, resolvedGrain)}</td>
                  <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, textAlign: 'right' }}>
                    {row.current == null ? '—' : formatter(row.current)}
                  </td>
                  {hasPrevious ? (
                    <td style={{ padding: `${t.space.xs}px ${t.space.sm}px`, textAlign: 'right' }}>
                      {row.previous == null ? '—' : formatter(row.previous)}
                    </td>
                  ) : null}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={{ width: '100%', height: compact ? 140 : 280 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={rows} margin={{ top: 8, right: 12, left: 4, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis
                dataKey="ts"
                tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }}
                tickFormatter={(v) => formatAxisLabel(String(v), resolvedGrain)}
              />
              <YAxis
                tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }}
                tickFormatter={(v) => formatter(Number(v))}
                width={compact ? 52 : 64}
              />
              <Tooltip
                contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                labelFormatter={(label) => `Date: ${formatAxisLabel(String(label), resolvedGrain)}`}
                formatter={(value, name) => {
                  const numeric = typeof value === 'number' ? value : Number(value)
                  const label = name === 'previous' ? 'Previous' : 'Current'
                  if (value == null) return ['—', name === 'previous' ? 'Previous' : 'Current']
                  return [Number.isFinite(numeric) ? formatter(numeric) : String(value), label]
                }}
              />
              {hasPrevious ? (
                <Line
                  type="monotone"
                  dataKey="previous"
                  name="previous"
                  stroke={t.color.textMuted}
                  strokeDasharray="4 4"
                  strokeWidth={1.5}
                  dot={false}
                  connectNulls={false}
                />
              ) : null}
              <Line
                type="monotone"
                dataKey="current"
                name="current"
                stroke={t.color.accent}
                strokeWidth={2}
                dot={compact ? false : { r: 2 }}
                connectNulls={false}
              />
            </LineChart>
          </ResponsiveContainer>
          {sparse ? (
            <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              Sparse data detected: missing points are shown as gaps.
            </div>
          ) : null}
        </div>
      )}
      {footerNote ? (
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{footerNote}</div>
      ) : null}
    </div>
  )
}

export type { TrendPanelProps, TrendMetric as KpiSeriesMetric, TrendPoint as KpiSeriesPoint }
