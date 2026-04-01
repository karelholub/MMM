import { tokens as t } from '../../theme/tokens'
import RecommendedActionsList, { type RecommendedActionItem } from '../../components/RecommendedActionsList'

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
  domain?: string
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
  top_unmapped_patterns: Array<{ source: string; medium: string; campaign?: string | null; count: number }>
  top_low_confidence_patterns: Array<{ source: string; medium: string; campaign?: string | null; count: number; confidence: number }>
  attention_queue: AttentionItem[]
  warnings: string[]
  recommended_actions: RecommendedAction[]
}

interface TaxonomyOverviewPanelProps {
  overview?: TaxonomyOverview
  loading: boolean
  error?: string | null
  onActionClick?: (action: RecommendedActionItem) => void
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

function capitalize(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1)
}

function toneForStatus(status?: string) {
  if (status === 'blocked') return { background: t.color.dangerSubtle, color: t.color.danger }
  if (status === 'warning') return { background: t.color.warningSubtle, color: t.color.warning }
  return { background: t.color.successMuted, color: t.color.success }
}

export default function TaxonomyOverviewPanel({
  overview,
  loading,
  error,
  onActionClick,
}: TaxonomyOverviewPanelProps) {
  const statusTone = toneForStatus(overview?.status)
  const topUnmappedPatterns = overview?.top_unmapped_patterns?.slice(0, 5) || []
  const topLowConfidencePatterns = overview?.top_low_confidence_patterns?.slice(0, 5) || []
  const totalUnmapped = topUnmappedPatterns.reduce((sum, item) => sum + Number(item.count || 0), 0)
  const totalLowConfidence = topLowConfidencePatterns.reduce((sum, item) => sum + Number(item.count || 0), 0)
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
              <span style={{ padding: '4px 10px', borderRadius: 999, fontSize: t.font.sizeXs, fontWeight: t.font.weightSemibold, background: statusTone.background, color: statusTone.color }}>
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
          <RecommendedActionsList
            actions={overview?.recommended_actions}
            emptyMessage="No immediate actions suggested."
            onActionClick={onActionClick}
          />
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

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: t.space.md }}>
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Top unmapped patterns</div>
            {topUnmappedPatterns.length ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, overflow: 'hidden', background: t.color.bg }}>
                {topUnmappedPatterns.map((item, idx) => {
                  const share = totalUnmapped > 0 ? Number(item.count || 0) / totalUnmapped : 0
                  return (
                    <div
                      key={`${item.source}-${item.medium}-${idx}`}
                      style={{
                        display: 'grid',
                        gap: 4,
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderTop: idx === 0 ? 'none' : `1px solid ${t.color.borderLight}`,
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                          <strong>{item.source || '—'}</strong> / <strong>{item.medium || '—'}</strong>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{Number(item.count || 0).toLocaleString()} · {pct(share)}</div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {item.campaign || 'No campaign sample'}
                      </div>
                      <div style={{ height: 6, borderRadius: 999, background: t.color.borderLight, overflow: 'hidden' }}>
                        <div style={{ width: `${Math.max(8, share * 100)}%`, height: '100%', borderRadius: 999, background: t.color.warning }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No repeated unmapped patterns are surfacing in the latest persisted sample.</div>
            )}
          </div>

          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Low-confidence hotspots</div>
            {topLowConfidencePatterns.length ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, overflow: 'hidden', background: t.color.bg }}>
                {topLowConfidencePatterns.map((item, idx) => {
                  const share = totalLowConfidence > 0 ? Number(item.count || 0) / totalLowConfidence : 0
                  return (
                    <div
                      key={`${item.source}-${item.medium}-${idx}`}
                      style={{
                        display: 'grid',
                        gap: 4,
                        padding: `${t.space.sm}px ${t.space.md}px`,
                        borderTop: idx === 0 ? 'none' : `1px solid ${t.color.borderLight}`,
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                          <strong>{item.source || '—'}</strong> / <strong>{item.medium || '—'}</strong>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{Number(item.count || 0).toLocaleString()} · {pct(share)}</div>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        <span>{item.campaign || 'No campaign sample'}</span>
                        <span>Confidence {pct(item.confidence)}</span>
                      </div>
                      <div style={{ height: 6, borderRadius: 999, background: t.color.borderLight, overflow: 'hidden' }}>
                        <div style={{ width: `${Math.max(8, (1 - Math.max(0, Math.min(1, Number(item.confidence || 0)))) * 100)}%`, height: '100%', borderRadius: 999, background: t.color.accent }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No concentrated low-confidence patterns were detected in the current taxonomy sample.</div>
            )}
          </div>
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
