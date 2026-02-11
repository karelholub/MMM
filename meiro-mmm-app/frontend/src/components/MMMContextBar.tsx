import { tokens } from '../theme/tokens'

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

  const pillStyle: React.CSSProperties = {
    padding: `${t.space.xs}px ${t.space.sm + 2}px`,
    borderRadius: 999,
    fontSize: t.font.sizeSm,
    backgroundColor: t.color.surface,
    border: `1px solid ${t.color.borderLight}`,
    color: t.color.textSecondary,
    display: 'inline-flex',
    alignItems: 'center',
    gap: t.space.xs,
  }

  return (
    <div
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: t.space.md,
        alignItems: 'center',
        padding: `${t.space.sm}px ${t.space.md}px`,
        background: t.color.surface,
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.sm,
        marginBottom: t.space.xl,
        fontSize: t.font.sizeSm,
        color: t.color.textSecondary,
      }}
    >
      {/* Period */}
      <div style={pillStyle} title="Measurement period for MMM runs">
        <span style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>Period</span>
        <span style={{ color: t.color.textSecondary }}>{periodLabel}</span>
        {periodReadOnly && (
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>(read-only)</span>
        )}
      </div>

      {/* KPI / Target */}
      <div style={pillStyle} title="Target KPI for MMM (sales vs marketing-driven conversions)">
        <span style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>Target</span>
        {onKpiModeChange ? (
          <select
            value={kpiMode}
            onChange={(e) => onKpiModeChange(e.target.value as KpiMode)}
            style={{
              marginLeft: t.space.xs,
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
          <span style={{ color: t.color.textSecondary }}>
            {kpiMode === 'sales' ? 'Total sales' : 'Marketing-driven conversions'}
          </span>
        )}
      </div>

      {/* Currency */}
      <div style={pillStyle} title="Currency used for spend and ROI">
        <span style={{ fontWeight: t.font.weightMedium, color: t.color.text }}>Currency</span>
        <span style={{ color: t.color.textSecondary }}>{currencyCode}</span>
        {currencyReadOnly && (
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>(read-only)</span>
        )}
      </div>

      {/* Data quality / sources link */}
      <button
        type="button"
        onClick={onOpenDataQuality}
        style={{
          ...pillStyle,
          borderStyle: 'dashed',
          borderColor: t.color.border,
          backgroundColor: 'transparent',
          cursor: onOpenDataQuality ? 'pointer' : 'default',
          color: t.color.accent,
        }}
      >
        <span>Data quality &amp; sources</span>
        <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>MMM inputs</span>
      </button>

      {/* Active config / version */}
      {activeConfigLabel && (
        <div
          style={{
            marginLeft: 'auto',
            display: 'inline-flex',
            alignItems: 'center',
            gap: t.space.xs,
            fontSize: t.font.sizeXs,
            color: t.color.textMuted,
          }}
        >
          <span style={{ textTransform: 'uppercase', letterSpacing: '0.06em' }}>Config</span>
          <span
            style={{
              padding: '2px 8px',
              borderRadius: 999,
              backgroundColor: t.color.bg,
              border: `1px solid ${t.color.borderLight}`,
              color: t.color.textSecondary,
              fontWeight: t.font.weightMedium,
            }}
          >
            {activeConfigLabel}
          </span>
        </div>
      )}
    </div>
  )
}

