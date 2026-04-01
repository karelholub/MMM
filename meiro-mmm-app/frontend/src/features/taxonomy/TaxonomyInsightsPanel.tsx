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
  confidence: { score: number; band: string }
  impact_count: number
  estimated_unknown_share_delta?: number
  channel?: string | null
  sample?: { source?: string; medium?: string; campaign?: string | null }
}

type TaxonomySuggestionsResponse = {
  summary: Partial<{
    unknown_share: number
    unknown_count: number
    total_touchpoints: number
    source_coverage: number
    medium_coverage: number
    active_rules: number
    source_aliases: number
    medium_aliases: number
  }>
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

function pctNumber(value?: number) {
  return Math.max(0, Math.min(100, (value || 0) * 100))
}

function prettyLabel(value: string) {
  return value
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
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
  const unresolvedPatterns = (coverage?.top_unmapped_patterns || unknownShare?.top_unmapped_patterns || []).slice(0, 6)
  const unresolvedTotal = unresolvedPatterns.reduce((sum, pattern) => sum + Number(pattern.count || 0), 0)
  const channelDistribution = Object.entries(coverage?.channel_distribution || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 7)
  const channelDistributionTotal = channelDistribution.reduce((sum, [, count]) => sum + Number(count || 0), 0)
  const topSuggestions = [...(suggestions?.suggestions || [])]
    .sort((a, b) => {
      const deltaGap = Number(b.estimated_unknown_share_delta || 0) - Number(a.estimated_unknown_share_delta || 0)
      if (Math.abs(deltaGap) > 0.0001) return deltaGap
      return Number(b.impact_count || 0) - Number(a.impact_count || 0)
    })
    .slice(0, 5)
  const maxSuggestionImpact = topSuggestions.reduce((max, item) => Math.max(max, Number(item.impact_count || 0)), 0)
  const coverageRunway = [
    { label: 'Mapped sources', value: pctNumber(summary?.source_coverage ?? coverage?.source_coverage), color: t.color.success },
    { label: 'Mapped mediums', value: pctNumber(summary?.medium_coverage ?? coverage?.medium_coverage), color: t.color.accent },
    { label: 'Unknown share', value: pctNumber(summary?.unknown_share ?? unknownShare?.unknown_share), color: t.color.warning },
  ]
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

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.05fr) minmax(0, 1.25fr)', gap: t.space.md }}>
          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div
              style={{
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                background: t.color.bg,
                padding: t.space.md,
                display: 'grid',
                gap: t.space.sm,
              }}
            >
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Coverage runway</div>
              <div style={{ display: 'grid', gap: t.space.xs }}>
                {coverageRunway.map((item) => (
                  <div key={item.label} style={{ display: 'grid', gap: 4 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      <span>{item.label}</span>
                      <span>{item.value.toFixed(1)}%</span>
                    </div>
                    <div style={{ height: 8, borderRadius: 999, background: t.color.borderLight, overflow: 'hidden' }}>
                      <div style={{ width: `${Math.max(0, Math.min(100, item.value))}%`, height: '100%', borderRadius: 999, background: item.color }} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Channel distribution</div>
            {channelDistribution.length ? (
              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.md,
                  background: t.color.bg,
                  padding: t.space.md,
                  display: 'grid',
                  gap: t.space.sm,
                }}
              >
                <div style={{ height: 14, borderRadius: 999, background: t.color.borderLight, overflow: 'hidden', display: 'flex' }}>
                  {channelDistribution.map(([channel, count], idx) => {
                    const share = channelDistributionTotal > 0 ? count / channelDistributionTotal : 0
                    return (
                      <div
                        key={channel}
                        title={`${prettyLabel(channel)}: ${count.toLocaleString()} (${pct(share)})`}
                        style={{
                          width: `${share * 100}%`,
                          minWidth: share > 0 ? 8 : 0,
                          background: channel === 'unknown' ? t.color.warning : t.color.chart[idx % t.color.chart.length],
                        }}
                      />
                    )
                  })}
                </div>
                {channelDistribution.map(([channel, count], idx) => {
                  const share = channelDistributionTotal > 0 ? count / channelDistributionTotal : 0
                  return (
                    <div key={channel} style={{ display: 'grid', gap: 4 }}>
                      <div style={{ display: 'grid', gridTemplateColumns: 'auto minmax(0, 1fr) auto', gap: t.space.sm, alignItems: 'center', fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        <span style={{ width: 10, height: 10, borderRadius: 999, background: channel === 'unknown' ? t.color.warning : t.color.chart[idx % t.color.chart.length] }} />
                        <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{prettyLabel(channel)}</span>
                        <span>{count.toLocaleString()} · {pct(share)}</span>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Channel distribution becomes available once persisted taxonomy touchpoint facts are loaded.</div>
            )}
          </div>

          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Unresolved patterns</div>
            {unresolvedPatterns.length ? (
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, overflow: 'hidden', background: t.color.bg }}>
                <div style={{ display: 'grid', gridTemplateColumns: '0.4fr 1.2fr 1fr 0.8fr', gap: t.space.sm, padding: `${t.space.sm}px ${t.space.md}px`, fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4, background: t.color.surfaceMuted }}>
                  <span>Rank</span>
                  <span>Pattern</span>
                  <span>Campaign</span>
                  <span style={{ textAlign: 'right' }}>Volume</span>
                </div>
                {unresolvedPatterns.map((pattern, idx) => {
                  const share = unresolvedTotal > 0 ? Number(pattern.count || 0) / unresolvedTotal : 0
                  return (
                    <div key={`${pattern.source}-${pattern.medium}-${idx}`} style={{ display: 'grid', gridTemplateColumns: '0.4fr 1.2fr 1fr 0.8fr', gap: t.space.sm, padding: `${t.space.sm}px ${t.space.md}px`, borderTop: idx === 0 ? 'none' : `1px solid ${t.color.borderLight}`, alignItems: 'center' }}>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>#{idx + 1}</div>
                      <div style={{ display: 'grid', gap: 2 }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                          <strong>{pattern.source || '—'}</strong> / <strong>{pattern.medium || '—'}</strong>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{pct(share)} of unresolved sample</div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{pattern.campaign || '—'}</div>
                      <div style={{ display: 'grid', justifyItems: 'end', gap: 4 }}>
                        <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>{Number(pattern.count || 0).toLocaleString()}</div>
                        <div style={{ width: '100%', maxWidth: 84, height: 6, borderRadius: 999, background: t.color.borderLight, overflow: 'hidden' }}>
                          <div style={{ width: `${Math.max(8, share * 100)}%`, height: '100%', borderRadius: 999, background: t.color.warning }} />
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No high-volume unmapped patterns detected in the sampled dataset.</div>
            )}

            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Highest-impact suggestion queue</div>
            {topSuggestions.length ? (
              <div
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.md,
                  background: t.color.bg,
                  padding: t.space.md,
                  display: 'grid',
                  gap: t.space.sm,
                }}
              >
                {topSuggestions.map((suggestion, idx) => {
                  const width = maxSuggestionImpact > 0 ? (Number(suggestion.impact_count || 0) / maxSuggestionImpact) * 100 : 0
                  return (
                    <div key={suggestion.id} style={{ display: 'grid', gap: 4 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
                        <div style={{ minWidth: 0 }}>
                          <div style={{ fontSize: t.font.sizeSm, color: t.color.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {idx + 1}. {suggestion.title}
                          </div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                            {pct(suggestion.confidence.score)} confidence · {suggestion.confidence.band}
                          </div>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, textAlign: 'right' }}>
                          {suggestion.impact_count.toLocaleString()} touchpoints
                          <br />
                          {pct(suggestion.estimated_unknown_share_delta)} lift
                        </div>
                      </div>
                      <div style={{ height: 8, borderRadius: 999, background: t.color.borderLight, overflow: 'hidden' }}>
                        <div style={{ width: `${Math.max(8, width)}%`, height: '100%', borderRadius: 999, background: t.color.chart[idx % t.color.chart.length] }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Suggestion impact bars appear when the analyzer finds stable unresolved patterns worth promoting into draft rules or aliases.</div>
            )}
          </div>
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
                        Confidence {pct(suggestion.confidence.score)} · {suggestion.confidence.band} · {suggestion.impact_count.toLocaleString()} touchpoints
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
