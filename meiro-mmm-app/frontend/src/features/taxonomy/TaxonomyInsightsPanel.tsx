import { tokens as t } from '../../theme/tokens'

type TaxonomyUnknownShare = {
  total_touchpoints: number
  unknown_count: number
  unknown_share: number
  top_unmapped_patterns: Array<{ source: string; medium: string; campaign?: string | null; count: number }>
}

type TaxonomyCoverage = {
  source_coverage: number
  medium_coverage: number
  channel_distribution: Record<string, number>
  rule_usage: Record<string, number>
  top_unmapped_patterns: Array<{ source: string; medium: string; campaign?: string | null; count: number }>
}

type TaxonomySuggestion = {
  id: string
  type: 'source_alias' | 'medium_alias' | 'channel_rule'
  title: string
  description: string
  confidence: number
  impact_count: number
  estimated_unknown_share_delta?: number
  channel?: string | null
  sample?: { source?: string; medium?: string; campaign?: string | null }
}

type TaxonomySuggestionsResponse = {
  summary: {
    unknown_share: number
    unknown_count: number
    total_touchpoints: number
    source_coverage: number
    medium_coverage: number
    active_rules: number
    source_aliases: number
    medium_aliases: number
  }
  suggestions: TaxonomySuggestion[]
}

interface TaxonomyInsightsPanelProps {
  unknownShare?: TaxonomyUnknownShare
  coverage?: TaxonomyCoverage
  suggestions?: TaxonomySuggestionsResponse
  loading: boolean
  error?: string | null
  onApplySuggestion: (id: string) => void
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

export default function TaxonomyInsightsPanel({
  unknownShare,
  coverage,
  suggestions,
  loading,
  error,
  onApplySuggestion,
}: TaxonomyInsightsPanelProps) {
  const summary = suggestions?.summary
  const groupedSuggestions = (suggestions?.suggestions || []).reduce<Record<string, TaxonomySuggestion[]>>((acc, suggestion) => {
    const key = suggestion.channel || suggestion.type
    acc[key] = [...(acc[key] || []), suggestion]
    return acc
  }, {})
  const cards = [
    { label: 'Unknown share', value: pct(summary?.unknown_share ?? unknownShare?.unknown_share), detail: `${summary?.unknown_count ?? unknownShare?.unknown_count ?? 0} unmapped touchpoints` },
    { label: 'Source coverage', value: pct(summary?.source_coverage ?? coverage?.source_coverage), detail: 'Share of sources with confident mapping' },
    { label: 'Medium coverage', value: pct(summary?.medium_coverage ?? coverage?.medium_coverage), detail: 'Share of mediums with confident mapping' },
    { label: 'Active rules', value: String(summary?.active_rules ?? Object.keys(coverage?.rule_usage || {}).length), detail: `${summary?.source_aliases ?? 0} source aliases · ${summary?.medium_aliases ?? 0} medium aliases` },
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
        <div>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Overview</h3>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Review current taxonomy health before editing rules. The most important gaps are ranked below by impact.
          </p>
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
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Attention queue</div>
          {(coverage?.top_unmapped_patterns || unknownShare?.top_unmapped_patterns || []).slice(0, 5).map((pattern, idx) => (
            <div key={`${pattern.source}-${pattern.medium}-${idx}`} style={{ display: 'grid', gap: 2, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                <strong>{pattern.source || '—'}</strong> / <strong>{pattern.medium || '—'}</strong>
                {pattern.campaign ? ` / ${pattern.campaign}` : ''}
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{pattern.count} touchpoints currently unresolved</div>
            </div>
          ))}
          {!loading && (coverage?.top_unmapped_patterns || unknownShare?.top_unmapped_patterns || []).length === 0 && (
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No high-volume unmapped patterns detected in the sampled dataset.</div>
          )}
        </div>
      </div>

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
        <div>
          <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Suggestions</h3>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Conservative suggestions are generated from currently unresolved source and medium patterns. Applying a suggestion only updates the draft until you save.
          </p>
        </div>

        {loading ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Analyzing taxonomy patterns…</div>
        ) : (suggestions?.suggestions || []).length === 0 ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No safe suggestions are available right now.</div>
        ) : (
          <div style={{ display: 'grid', gap: t.space.sm }}>
            {Object.entries(groupedSuggestions).map(([group, items]) => (
              <div key={group} style={{ display: 'grid', gap: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>
                  {group.replace(/_/g, ' ')}
                </div>
                {items.map((suggestion) => (
                  <div key={suggestion.id} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, display: 'grid', gap: t.space.xs, background: t.color.bg }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{suggestion.title}</div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Confidence {pct(suggestion.confidence)} · {suggestion.impact_count.toLocaleString()} touchpoints
                      </div>
                    </div>
                    <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{suggestion.description}</div>
                    <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap', fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      <span>Estimated unknown-share reduction: {pct(suggestion.estimated_unknown_share_delta)}</span>
                      {suggestion.channel ? <span>Target channel: {suggestion.channel}</span> : null}
                    </div>
                    {suggestion.sample && (
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                        Sample: {suggestion.sample.source || '—'} / {suggestion.sample.medium || '—'}
                        {suggestion.sample.campaign ? ` / ${suggestion.sample.campaign}` : ''}
                      </div>
                    )}
                    <div>
                      <button
                        type="button"
                        onClick={() => onApplySuggestion(suggestion.id)}
                        style={{
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.accent}`,
                          background: t.color.accentMuted,
                          color: t.color.accent,
                          fontSize: t.font.sizeXs,
                          cursor: 'pointer',
                        }}
                      >
                        Apply to draft
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
