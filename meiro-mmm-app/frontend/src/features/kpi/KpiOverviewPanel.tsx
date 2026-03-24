import { tokens as t } from '../../theme/tokens'

type KpiOverview = {
  status: string
  confidence: { score: number; band: string }
  summary: {
    definitions_count: number
    primary_kpi_id?: string | null
    primary_kpi_label?: string | null
    journeys_total: number
    journeys_with_any_kpi: number
    journeys_with_primary_kpi: number
    primary_coverage: number
  }
  warnings: string[]
  recommended_actions: Array<{ id: string; label: string; benefit?: string }>
}

interface KpiOverviewPanelProps {
  overview?: KpiOverview
  loading: boolean
  error?: string | null
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

export default function KpiOverviewPanel({ overview, loading, error }: KpiOverviewPanelProps) {
  const cards = [
    {
      label: 'Primary coverage',
      value: pct(overview?.summary.primary_coverage),
      detail: `${overview?.summary.journeys_with_primary_kpi ?? 0} journeys match the primary KPI`,
    },
    {
      label: 'Any KPI coverage',
      value: overview?.summary.journeys_total ? pct((overview?.summary.journeys_with_any_kpi || 0) / overview.summary.journeys_total) : '0.0%',
      detail: `${overview?.summary.journeys_with_any_kpi ?? 0} journeys carry any KPI tag`,
    },
    {
      label: 'Definitions',
      value: String(overview?.summary.definitions_count ?? 0),
      detail: overview?.summary.primary_kpi_label ? `Primary: ${overview.summary.primary_kpi_label}` : 'No primary KPI selected',
    },
    {
      label: 'Confidence',
      value: pct(overview?.confidence.score),
      detail: overview?.confidence.band || 'unknown',
    },
  ]

  return (
    <div style={{ display: 'grid', gap: t.space.lg }}>
      <div style={{ background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.borderLight}`, boxShadow: t.shadowSm, padding: t.space.lg, display: 'grid', gap: t.space.md }}>
        <div>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Overview</h3>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            KPI readiness, coverage, and the highest-value actions before editing definitions.
          </p>
        </div>

        {error ? (
          <div style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerSubtle, color: t.color.danger, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs }}>
            {error}
          </div>
        ) : null}

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
          {cards.map((card) => (
            <div key={card.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>{card.label}</div>
              <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>{loading ? '…' : card.value}</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{card.detail}</div>
            </div>
          ))}
        </div>

        <div style={{ display: 'grid', gap: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Recommended actions</div>
          {overview?.recommended_actions?.length ? overview.recommended_actions.map((action) => (
            <div key={action.id} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{action.label}</div>
              {action.benefit ? <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{action.benefit}</div> : null}
            </div>
          )) : <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No immediate KPI actions suggested.</div>}
        </div>

        {overview?.warnings?.length ? (
          <div style={{ border: `1px solid ${t.color.warning}`, background: t.color.warningSubtle, color: t.color.warning, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs, display: 'grid', gap: 4 }}>
            {overview.warnings.map((warning) => <div key={warning}>{warning}</div>)}
          </div>
        ) : null}
      </div>
    </div>
  )
}
