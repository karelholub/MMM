import { tokens as t } from '../../theme/tokens'
import DecisionStatusCard from '../../components/DecisionStatusCard'

type KpiPreview = {
  before: {
    definitions_count: number
    primary_kpi_id?: string | null
    primary_kpi_label?: string | null
    journeys_total: number
    journeys_with_any_kpi: number
    journeys_with_primary_kpi: number
    primary_coverage: number
  }
  after: {
    definitions_count: number
    primary_kpi_id?: string | null
    primary_kpi_label?: string | null
    journeys_total: number
    journeys_with_any_kpi: number
    journeys_with_primary_kpi: number
    primary_coverage: number
  }
  delta: {
    definitions_count: number
    journeys_with_primary_kpi: number
    journeys_with_any_kpi: number
    primary_coverage: number
  }
  primary_change: {
    before?: string | null
    after?: string | null
  }
  warnings: string[]
}

interface KpiPreviewPanelProps {
  preview?: KpiPreview
  loading: boolean
  error?: string | null
  dirty: boolean
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

function signedInt(value?: number) {
  return `${value && value > 0 ? '+' : ''}${Math.trunc(value || 0)}`
}

function signedPct(value?: number) {
  const next = ((value || 0) * 100).toFixed(1)
  return `${value && value > 0 ? '+' : ''}${next}%`
}

export default function KpiPreviewPanel({ preview, loading, error, dirty }: KpiPreviewPanelProps) {
  return (
    <div style={{ background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.borderLight}`, boxShadow: t.shadowSm, padding: t.space.lg, display: 'grid', gap: t.space.md }}>
      <div>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Preview</h3>
        <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Before and after impact for the current KPI draft.
        </p>
      </div>
      {error ? (
        <DecisionStatusCard
          title="Preview unavailable"
          status="blocked"
          compact
          blockers={[error]}
        />
      ) : null}
      {!dirty ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Edit KPI definitions or change the primary KPI to see the expected impact before saving.
        </div>
      ) : loading ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Computing KPI preview…</div>
      ) : !preview ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Preview is unavailable for the current draft.</div>
      ) : (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Primary coverage</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{pct(preview.before.primary_coverage)} → {pct(preview.after.primary_coverage)}</div>
              <div style={{ fontSize: t.font.sizeXs, color: preview.delta.primary_coverage >= 0 ? t.color.success : t.color.warning }}>{signedPct(preview.delta.primary_coverage)}</div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Definitions</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{preview.before.definitions_count} → {preview.after.definitions_count}</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{signedInt(preview.delta.definitions_count)} draft delta</div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Journeys with primary KPI</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{preview.before.journeys_with_primary_kpi} → {preview.after.journeys_with_primary_kpi}</div>
              <div style={{ fontSize: t.font.sizeXs, color: preview.delta.journeys_with_primary_kpi >= 0 ? t.color.success : t.color.warning }}>{signedInt(preview.delta.journeys_with_primary_kpi)}</div>
            </div>
          </div>

          <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Primary KPI change</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
              {(preview.primary_change.before || '—')} → {(preview.primary_change.after || '—')}
            </div>
          </div>

          {preview.warnings.length ? (
            <DecisionStatusCard
              title="Preview warnings"
              status="warning"
              compact
              warnings={preview.warnings}
            />
          ) : null}
        </>
      )}
    </div>
  )
}
