import { tokens as t } from '../../theme/tokens'

type TaxonomySuggestion = {
  id: string
  type: 'source_alias' | 'medium_alias' | 'channel_rule'
  title: string
  description: string
  confidence: { score: number; band: string }
  impact_count: number
  estimated_unknown_share_delta?: number
  channel?: string | null
  reasons?: string[]
  recommended_action?: string
  sample?: { source?: string; medium?: string; campaign?: string | null }
}

type TaxonomySuggestionsResponse = {
  suggestions: TaxonomySuggestion[]
}

interface TaxonomySuggestionsPanelProps {
  suggestions?: TaxonomySuggestionsResponse
  loading: boolean
  error?: string | null
  suggestionStatusById?: Record<string, 'saved' | 'draft' | 'pending'>
  onApplySuggestion: (id: string) => void
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

export default function TaxonomySuggestionsPanel({
  suggestions,
  loading,
  error,
  suggestionStatusById,
  onApplySuggestion,
}: TaxonomySuggestionsPanelProps) {
  const groupedSuggestions = (suggestions?.suggestions || []).reduce<Record<string, TaxonomySuggestion[]>>((acc, suggestion) => {
    const key = suggestion.channel || suggestion.type
    acc[key] = [...(acc[key] || []), suggestion]
    return acc
  }, {})

  return (
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
          Conservative draft changes inferred from unresolved traffic and stable observed patterns.
        </p>
      </div>

      {error && (
        <div style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerSubtle, color: t.color.danger, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs }}>
          {error}
        </div>
      )}

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
                  {(() => {
                    const status = suggestionStatusById?.[suggestion.id] ?? 'pending'
                    const isSaved = status === 'saved'
                    const isDraft = status === 'draft'
                    const label = isSaved
                      ? 'Already in saved config'
                      : isDraft
                      ? 'Already in draft (unsaved)'
                      : 'Ready to apply'
                    const color = isSaved
                      ? t.color.success
                      : isDraft
                      ? t.color.warning
                      : t.color.accent
                    const background = isSaved
                      ? t.color.successMuted
                      : isDraft
                      ? t.color.warningSubtle
                      : t.color.accentMuted
                    return (
                      <div
                        style={{
                          justifySelf: 'start',
                          padding: '2px 8px',
                          borderRadius: 999,
                          fontSize: t.font.sizeXs,
                          fontWeight: t.font.weightMedium,
                          color,
                          background,
                          border: `1px solid ${color}`,
                        }}
                      >
                        {label}
                      </div>
                    )
                  })()}
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
                  {suggestion.reasons?.length ? (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Reasons: {suggestion.reasons.join(', ')}
                    </div>
                  ) : null}
                  {suggestion.sample && (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Sample: {suggestion.sample.source || '—'} / {suggestion.sample.medium || '—'}
                      {suggestion.sample.campaign ? ` / ${suggestion.sample.campaign}` : ''}
                    </div>
                  )}
                  {suggestion.recommended_action ? (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{suggestion.recommended_action}</div>
                  ) : null}
                  <div>
                    {(() => {
                      const status = suggestionStatusById?.[suggestion.id] ?? 'pending'
                      const disabled = status !== 'pending'
                      const buttonLabel =
                        status === 'saved'
                          ? 'Already saved'
                          : status === 'draft'
                          ? 'Already in draft'
                          : 'Apply to draft'
                      return (
                        <button
                          type="button"
                          onClick={() => onApplySuggestion(suggestion.id)}
                          disabled={disabled}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${disabled ? t.color.border : t.color.accent}`,
                            background: disabled ? t.color.borderLight : t.color.accentMuted,
                            color: disabled ? t.color.textMuted : t.color.accent,
                            fontSize: t.font.sizeXs,
                            cursor: disabled ? 'not-allowed' : 'pointer',
                          }}
                        >
                          {buttonLabel}
                        </button>
                      )
                    })()}
                  </div>
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
