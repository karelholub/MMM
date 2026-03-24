import { tokens as t } from '../../theme/tokens'

type KpiSuggestion = {
  id: string
  type: 'set_primary' | 'add_definition'
  title: string
  description: string
  confidence: { score: number; band: string }
  impact_count: number
  reasons?: string[]
  recommended_action?: string
}

type KpiSuggestionsResponse = {
  suggestions: KpiSuggestion[]
}

interface KpiSuggestionsPanelProps {
  suggestions?: KpiSuggestionsResponse
  loading: boolean
  error?: string | null
  onApplySuggestion: (id: string) => void
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

export default function KpiSuggestionsPanel({
  suggestions,
  loading,
  error,
  onApplySuggestion,
}: KpiSuggestionsPanelProps) {
  return (
    <div style={{ background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.borderLight}`, boxShadow: t.shadowSm, padding: t.space.lg, display: 'grid', gap: t.space.md }}>
      <div>
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Suggestions</h3>
        <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Suggested KPI changes derived from imported journey events and existing configuration gaps.
        </p>
      </div>
      {error ? (
        <div style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerSubtle, color: t.color.danger, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs }}>
          {error}
        </div>
      ) : null}
      {loading ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Analyzing KPI coverage…</div>
      ) : (suggestions?.suggestions || []).length === 0 ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No KPI suggestions are available right now.</div>
      ) : (
        <div style={{ display: 'grid', gap: t.space.sm }}>
          {(suggestions?.suggestions || []).map((suggestion) => (
            <div key={suggestion.id} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, display: 'grid', gap: t.space.xs, background: t.color.bg }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{suggestion.title}</div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  Confidence {pct(suggestion.confidence.score)} · {suggestion.confidence.band} · {suggestion.impact_count.toLocaleString()} journeys/events
                </div>
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{suggestion.description}</div>
              {suggestion.reasons?.length ? (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  Reasons: {suggestion.reasons.join(', ')}
                </div>
              ) : null}
              {suggestion.recommended_action ? (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{suggestion.recommended_action}</div>
              ) : null}
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
      )}
    </div>
  )
}
