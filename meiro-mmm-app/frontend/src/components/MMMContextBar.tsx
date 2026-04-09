import { tokens } from '../theme/tokens'
import ContextSummaryStrip from './dashboard/ContextSummaryStrip'
import { buildSettingsHref } from '../lib/settingsLinks'

export type KpiMode = 'sales' | 'attribution'

interface MMMContextBarProps {
  periodLabel?: string
  periodReadOnly?: boolean
  kpiMode: KpiMode
  onKpiModeChange?: (mode: KpiMode) => void
  currencyCode?: string
  currencyReadOnly?: boolean
  onOpenDataQuality?: () => void
  activeConfigLabel?: string
}

export function MMMContextBar({
  periodLabel = 'Weekly (model frequency: W)',
  periodReadOnly = true,
  kpiMode,
  onKpiModeChange,
  currencyCode = '—',
  currencyReadOnly = true,
  onOpenDataQuality,
  activeConfigLabel,
}: MMMContextBarProps) {
  const t = tokens
  const targetLabel = kpiMode === 'sales' ? 'Total sales' : 'Marketing-driven conversions'

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <ContextSummaryStrip
        minItemWidth={180}
        items={[
          {
            label: 'Period',
            value: `${periodLabel}${periodReadOnly ? ' (read-only)' : ''}`,
          },
          {
            label: 'Target',
            value: onKpiModeChange ? (
              <select
                value={kpiMode}
                onChange={(e) => onKpiModeChange(e.target.value as KpiMode)}
                style={{
                  padding: '2px 8px',
                  fontSize: t.font.sizeSm,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  color: t.color.text,
                  cursor: 'pointer',
                }}
              >
                <option value="sales">Total sales</option>
                <option value="attribution">Marketing-driven conversions</option>
              </select>
            ) : (
              targetLabel
            ),
          },
          {
            label: 'Currency',
            value: `${currencyCode}${currencyReadOnly ? ' (read-only)' : ''}`,
          },
          ...(activeConfigLabel
            ? [
                {
                  label: 'Active config',
                  value: activeConfigLabel,
                },
              ]
            : []),
        ]}
      />
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
        <button
          type="button"
          onClick={onOpenDataQuality}
          style={{
            padding: `${t.space.sm}px ${t.space.md}px`,
            borderRadius: t.radius.sm,
            border: `1px solid ${t.color.border}`,
            background: t.color.surface,
            color: t.color.text,
            fontSize: t.font.sizeSm,
            cursor: onOpenDataQuality ? 'pointer' : 'default',
          }}
        >
          Open data quality
        </button>
        <a
          href={buildSettingsHref('mmm')}
          style={{
            padding: `${t.space.sm}px ${t.space.md}px`,
            borderRadius: t.radius.sm,
            border: `1px solid ${t.color.border}`,
            background: t.color.surface,
            color: t.color.text,
            fontSize: t.font.sizeSm,
            textDecoration: 'none',
          }}
        >
          Open MMM settings
        </a>
      </div>
    </div>
  )
}
