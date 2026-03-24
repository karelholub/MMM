import { tokens as t } from '../../theme/tokens'

type AttentionItem = {
  type: string
  title: string
  detail: string
  count: number
  sample?: { source?: string; medium?: string; campaign?: string | null }
}

type RecommendedAction = {
  id: string
  label: string
  benefit?: string
  requires_review?: boolean
}

type TaxonomyOverview = {
  status: 'ready' | 'warning' | 'blocked' | string
  confidence: { score: number; band: string }
  summary: {
    unknown_share: number
    unknown_count: number
    total_touchpoints: number
    source_coverage: number
    medium_coverage: number
    active_rules: number
    source_aliases: number
    medium_aliases: number
    low_confidence_share: number
    low_confidence_count: number
  }
  attention_queue: AttentionItem[]
  warnings: string[]
  recommended_actions: RecommendedAction[]
}

interface TaxonomyOverviewPanelProps {
  overview?: TaxonomyOverview
  loading: boolean
  error?: string | null
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

function capitalize(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1)
}

export default function TaxonomyOverviewPanel({
  overview,
  loading,
  error,
}: TaxonomyOverviewPanelProps) {
  const cards = [
    {
      label: 'Unknown share',
      value: pct(overview?.summary.unknown_share),
      detail: `${overview?.summary.unknown_count ?? 0} unresolved touchpoints`,
    },
    {
      label: 'Low confidence',
      value: pct(overview?.summary.low_confidence_share),
      detail: `${overview?.summary.low_confidence_count ?? 0} low-confidence touchpoints`,
    },
    {
      label: 'Source coverage',
      value: pct(overview?.summary.source_coverage),
      detail: 'Share of sources with confident mapping',
    },
    {
      label: 'Medium coverage',
      value: pct(overview?.summary.medium_coverage),
      detail: 'Share of mediums with confident mapping',
    },
  ]

  return (
    <div style={{ display: 'grid', gap: t.space.lg }}>
      <div
        style={{
          background: t.color.surface,
          borderRadius: t.radius.lg,
          border: `1px solid ${t.color.borderLight}`,
          boxShadow: t.shadowSm,
          padding: t.space.lg,
          display: 'grid',
          gap: t.space.md,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Overview</h3>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Taxonomy health, confidence, and the most valuable next actions.
            </p>
          </div>
          {overview && (
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
              <span style={{ padding: '4px 10px', borderRadius: 999, fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, background: overview.status === 'blocked' ? t.color.dangerSubtle : overview.status === 'warning' ? t.color.warningSubtle : t.color.successMuted, color: overview.status === 'blocked' ? t.color.danger : overview.status === 'warning' ? t.color.warning : t.color.success }}>
                {capitalize(overview.status)}
              </span>
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                Confidence {pct(overview.confidence.score)} · {overview.confidence.band}
              </span>
            </div>
          )}
        </div>

        {error && (
          <div style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerSubtle, color: t.color.danger, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs }}>
            {error}
          </div>
        )}

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
          )) : <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No immediate actions suggested.</div>}
        </div>

        <div style={{ display: 'grid', gap: t.space.sm }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Attention queue</div>
          {overview?.attention_queue?.length ? overview.attention_queue.map((item, idx) => (
            <div key={`${item.type}-${idx}`} style={{ display: 'grid', gap: 2, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{item.title}</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{item.detail}</div>
            </div>
          )) : <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No high-value issues found in the current sample.</div>}
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
