import { useEffect, useMemo, useState } from 'react'
import { tokens as t } from '../../theme/tokens'

export interface LagInsightItem {
  key: string
  label: string
  channel?: string | null
  campaign?: string | null
  conversions: number
  share_of_conversions: number
  avg_days_from_first_touch: number | null
  p50_days_from_first_touch: number | null
  p90_days_from_first_touch: number | null
  avg_days_from_last_touch: number | null
  p50_days_from_last_touch: number | null
  p90_days_from_last_touch: number | null
  lag_buckets: {
    within_1d: number
    days_1_to_3: number
    days_3_to_7: number
    over_7d: number
  }
  role_mix: {
    first_touch_conversions: number
    assist_conversions: number
    last_touch_conversions: number
  }
}

export interface LagInsightsResponse {
  scope_type: string
  current_period: {
    date_from: string
    date_to: string
  }
  conversion_key?: string | null
  items: LagInsightItem[]
  summary: {
    conversions: number
    median_days_from_first_touch: number | null
    p90_days_from_first_touch: number | null
    median_days_from_last_touch: number | null
    p90_days_from_last_touch: number | null
    long_lag_share_over_7d: number
  }
}

function formatDays(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  return `${Number(value).toFixed(1)}d`
}

interface LagInsightsPanelProps {
  title: string
  subtitle: string
  data?: LagInsightsResponse | null
  loading?: boolean
  error?: string | null
  emptyLabel?: string
}

export default function LagInsightsPanel({
  title,
  subtitle,
  data,
  loading = false,
  error = null,
  emptyLabel = 'No lag data available for this period.',
}: LagInsightsPanelProps) {
  if (loading) {
    return <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading lag analysis…</div>
  }
  if (error) {
    return <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{error}</div>
  }
  if (!data || !data.items.length) {
    return <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{emptyLabel}</div>
  }

  const topSlow = [...data.items]
    .sort((a, b) => (b.p50_days_from_first_touch || 0) - (a.p50_days_from_first_touch || 0))
    .slice(0, 8)
  const [selectedKey, setSelectedKey] = useState<string>(topSlow[0]?.key || '')

  useEffect(() => {
    if (!topSlow.length) {
      setSelectedKey('')
      return
    }
    if (!topSlow.some((item) => item.key === selectedKey)) {
      setSelectedKey(topSlow[0].key)
    }
  }, [selectedKey, topSlow])

  const selectedItem = useMemo(
    () => topSlow.find((item) => item.key === selectedKey) || topSlow[0] || null,
    [selectedKey, topSlow],
  )
  const selectedTotal = selectedItem ? Math.max(1, selectedItem.conversions) : 1
  const roleTotal = selectedItem
    ? Math.max(
        1,
        selectedItem.role_mix.first_touch_conversions +
          selectedItem.role_mix.assist_conversions +
          selectedItem.role_mix.last_touch_conversions,
      )
    : 1

  function shareBar(value: number, total: number, color: string) {
    return {
      height: 10,
      width: `${Math.max(6, Math.round((value / Math.max(1, total)) * 100))}%`,
      borderRadius: 999,
      background: color,
    }
  }

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{subtitle}</div>
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
          gap: t.space.sm,
        }}
      >
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Median first-touch lag</div>
          <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold }}>{formatDays(data.summary.median_days_from_first_touch)}</div>
        </div>
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>P90 first-touch lag</div>
          <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold }}>{formatDays(data.summary.p90_days_from_first_touch)}</div>
        </div>
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Median last-touch gap</div>
          <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold }}>{formatDays(data.summary.median_days_from_last_touch)}</div>
        </div>
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Long-lag share (&gt;7d)</div>
          <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold }}>
            {(Number(data.summary.long_lag_share_over_7d || 0) * 100).toFixed(1)}%
          </div>
        </div>
      </div>
      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        {title} shows how long conversions take after the first observed touch and after the last touch. Longer-lag scopes are more sensitive to shorter attribution windows.
      </div>
      {selectedItem && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))',
            gap: t.space.md,
            padding: t.space.md,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            background: t.color.surface,
          }}
        >
          <div style={{ display: 'grid', gap: t.space.xs }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Selected scope</div>
            <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>{selectedItem.label}</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {selectedItem.conversions.toLocaleString()} conversions · {(selectedItem.share_of_conversions * 100).toFixed(1)}% of selected-period volume
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Median first-touch lag {formatDays(selectedItem.p50_days_from_first_touch)} · P90 {formatDays(selectedItem.p90_days_from_first_touch)}
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Median last-touch gap {formatDays(selectedItem.p50_days_from_last_touch)}
            </div>
          </div>
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Lag bucket mix</div>
            <div style={{ display: 'grid', gap: 6 }}>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>Within 1 day</span><span>{selectedItem.lag_buckets.within_1d.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.lag_buckets.within_1d, selectedTotal, t.color.success)} /></div>
              </div>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>1–3 days</span><span>{selectedItem.lag_buckets.days_1_to_3.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.lag_buckets.days_1_to_3, selectedTotal, t.color.accent)} /></div>
              </div>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>3–7 days</span><span>{selectedItem.lag_buckets.days_3_to_7.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.lag_buckets.days_3_to_7, selectedTotal, '#d97706')} /></div>
              </div>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>Over 7 days</span><span>{selectedItem.lag_buckets.over_7d.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.lag_buckets.over_7d, selectedTotal, t.color.warning)} /></div>
              </div>
            </div>
          </div>
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>Role mix</div>
            <div style={{ display: 'grid', gap: 6 }}>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>First touch</span><span>{selectedItem.role_mix.first_touch_conversions.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.role_mix.first_touch_conversions, roleTotal, '#2563eb')} /></div>
              </div>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>Assist</span><span>{selectedItem.role_mix.assist_conversions.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.role_mix.assist_conversions, roleTotal, '#7c3aed')} /></div>
              </div>
              <div style={{ display: 'grid', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: t.font.sizeXs, color: t.color.textSecondary }}><span>Last touch</span><span>{selectedItem.role_mix.last_touch_conversions.toLocaleString()}</span></div>
                <div style={{ background: t.color.bg, borderRadius: 999, padding: 2 }}><div style={shareBar(selectedItem.role_mix.last_touch_conversions, roleTotal, '#059669')} /></div>
              </div>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {selectedItem.lag_buckets.over_7d > selectedItem.lag_buckets.within_1d
                ? 'This scope has a heavier long-lag tail than short-lag conversions, so tighter windows will materially suppress it.'
                : 'This scope converts relatively quickly, so it is less sensitive to moderate lookback tightening.'}
            </div>
          </div>
        </div>
      )}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 760 }}>
          <thead>
            <tr>
              {['Scope', 'Conversions', 'P50 first-touch', 'P90 first-touch', 'P50 last-touch', 'Long-lag mix', 'Role mix'].map((header) => (
                <th
                  key={header}
                  style={{
                    textAlign: header === 'Scope' ? 'left' : 'right',
                    fontSize: t.font.sizeXs,
                    color: t.color.textMuted,
                    fontWeight: t.font.weightSemibold,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderBottom: `1px solid ${t.color.borderLight}`,
                    whiteSpace: 'nowrap',
                  }}
                >
                  {header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {topSlow.map((item) => (
              <tr
                key={item.key}
                onClick={() => setSelectedKey(item.key)}
                style={{
                  cursor: 'pointer',
                  background: item.key === selectedKey ? t.color.bg : 'transparent',
                }}
              >
                <td style={{ padding: `${t.space.sm}px`, borderBottom: `1px solid ${t.color.borderLight}` }}>
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightMedium }}>{item.label}</div>
                  {!!item.channel && item.channel !== item.label && (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{item.channel}</div>
                  )}
                </td>
                <td style={{ padding: `${t.space.sm}px`, textAlign: 'right', borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm }}>
                  {item.conversions.toLocaleString()}
                </td>
                <td style={{ padding: `${t.space.sm}px`, textAlign: 'right', borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm }}>
                  {formatDays(item.p50_days_from_first_touch)}
                </td>
                <td style={{ padding: `${t.space.sm}px`, textAlign: 'right', borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm }}>
                  {formatDays(item.p90_days_from_first_touch)}
                </td>
                <td style={{ padding: `${t.space.sm}px`, textAlign: 'right', borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm }}>
                  {formatDays(item.p50_days_from_last_touch)}
                </td>
                <td style={{ padding: `${t.space.sm}px`, textAlign: 'right', borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm }}>
                  {item.lag_buckets.over_7d.toLocaleString()} / {item.conversions.toLocaleString()}
                </td>
                <td style={{ padding: `${t.space.sm}px`, textAlign: 'right', borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeSm }}>
                  F {item.role_mix.first_touch_conversions.toLocaleString()} · A {item.role_mix.assist_conversions.toLocaleString()} · L {item.role_mix.last_touch_conversions.toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
