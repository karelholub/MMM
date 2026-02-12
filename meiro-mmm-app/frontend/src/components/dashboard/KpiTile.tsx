import { memo } from 'react'
import TrendPanel, { type KpiSeriesPoint } from './TrendPanel'
import { tokens as t } from '../../theme/tokens'

export interface KpiTileProps {
  label: string
  value: string
  deltaPct?: number | null
  deltaLabel: string
  trendLabel: string
  series?: KpiSeriesPoint[]
  seriesPrev?: KpiSeriesPoint[]
  confidence?: {
    score: number
    level: 'high' | 'medium' | 'low' | string
    reasons?: Array<{ key: string; label: string; score: number; detail: string; weight?: number }>
  } | null
  infoTooltip: string
  onClick?: () => void
  drilldownLabel?: string
  formatTooltipValue?: (value: number) => string
}

export const KpiTile = memo(function KpiTile({
  label,
  value,
  deltaPct,
  deltaLabel,
  trendLabel,
  series,
  seriesPrev,
  infoTooltip,
  onClick,
  drilldownLabel,
  formatTooltipValue,
  confidence,
}: KpiTileProps) {
  const subtitle = `${value} · ${deltaPct == null ? 'Δ N/A' : `${deltaPct >= 0 ? '+' : ''}${deltaPct.toFixed(1)}% ${deltaLabel}`}`
  return (
    <div
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
      onKeyDown={(e) => {
        if (onClick && (e.key === 'Enter' || e.key === ' ')) onClick()
      }}
      style={{ cursor: onClick ? 'pointer' : 'default' }}
    >
      <TrendPanel
        title={label}
        subtitle={trendLabel}
        metrics={[
          {
            key: label.toLowerCase(),
            label,
            current: series || [],
            previous: seriesPrev || [],
            summaryMode: 'sum',
            formatValue: formatTooltipValue,
          },
        ]}
        showMetricSelector={false}
        showGrainSelector={false}
        showCompareToggle={false}
        showTableToggle={false}
        compact
        badge={
          confidence ? (
            <span
              style={{
                fontSize: 11,
                color:
                  confidence.level === 'high'
                    ? t.color.success
                    : confidence.level === 'medium'
                    ? t.color.warning
                    : t.color.danger,
              }}
              title={confidence.reasons?.map((r) => `${r.label}: ${r.detail}`).join('\n') || 'Data quality score'}
            >
              Data quality {Math.round(confidence.score)}/100
            </span>
          ) : null
        }
        footerNote={drilldownLabel}
        baselineLabel={deltaLabel}
        infoTooltip={`${infoTooltip}\nSummary: ${subtitle}`}
      />
    </div>
  )
})

export default KpiTile
