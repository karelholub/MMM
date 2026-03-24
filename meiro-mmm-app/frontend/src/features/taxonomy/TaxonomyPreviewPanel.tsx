import { tokens as t } from '../../theme/tokens'

type TaxonomyPreviewResponse = {
  before: {
    unknown_share: number
    unknown_count: number
    low_confidence_share: number
    low_confidence_count: number
    active_rules: number
    source_aliases: number
    medium_aliases: number
  }
  after: {
    unknown_share: number
    unknown_count: number
    low_confidence_share: number
    low_confidence_count: number
    active_rules: number
    source_aliases: number
    medium_aliases: number
  }
  delta: {
    unknown_share: number
    unknown_count: number
    low_confidence_share: number
    active_rules: number
    source_aliases: number
    medium_aliases: number
  }
  top_new_matches: Array<{
    source: string
    medium: string
    campaign?: string | null
    count: number
    channel: string
    confidence: number
  }>
  warnings: string[]
}

interface TaxonomyPreviewPanelProps {
  preview?: TaxonomyPreviewResponse
  loading: boolean
  error?: string | null
  dirty: boolean
}

function pct(value?: number) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

function signedPct(value?: number) {
  const next = ((value || 0) * 100).toFixed(1)
  return `${value && value > 0 ? '+' : ''}${next}%`
}

function signedInt(value?: number) {
  return `${value && value > 0 ? '+' : ''}${Math.trunc(value || 0)}`
}

export default function TaxonomyPreviewPanel({
  preview,
  loading,
  error,
  dirty,
}: TaxonomyPreviewPanelProps) {
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
        <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Preview</h3>
        <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Before and after impact for the current taxonomy draft.
        </p>
      </div>

      {error && (
        <div style={{ border: `1px solid ${t.color.danger}`, background: t.color.dangerSubtle, color: t.color.danger, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs }}>
          {error}
        </div>
      )}

      {!dirty ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Edit rules or aliases to see a preview of the expected impact before saving.
        </div>
      ) : loading ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Computing preview…</div>
      ) : !preview ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Preview is unavailable for the current draft.</div>
      ) : (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Unknown share</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{pct(preview.before.unknown_share)} → {pct(preview.after.unknown_share)}</div>
              <div style={{ fontSize: t.font.sizeXs, color: preview.delta.unknown_share <= 0 ? t.color.success : t.color.warning }}>{signedPct(preview.delta.unknown_share)}</div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Low confidence share</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{pct(preview.before.low_confidence_share)} → {pct(preview.after.low_confidence_share)}</div>
              <div style={{ fontSize: t.font.sizeXs, color: preview.delta.low_confidence_share <= 0 ? t.color.success : t.color.warning }}>{signedPct(preview.delta.low_confidence_share)}</div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Rules</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{preview.before.active_rules} → {preview.after.active_rules}</div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{signedInt(preview.delta.active_rules)} draft delta</div>
            </div>
            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: 4 }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>Aliases</div>
              <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                {(preview.before.source_aliases + preview.before.medium_aliases)} → {(preview.after.source_aliases + preview.after.medium_aliases)}
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                {signedInt(preview.delta.source_aliases + preview.delta.medium_aliases)} draft delta
              </div>
            </div>
          </div>

          <div style={{ display: 'grid', gap: t.space.sm }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Top newly resolved patterns</div>
            {preview.top_new_matches.length ? preview.top_new_matches.map((item, index) => (
              <div key={`${item.source}-${item.medium}-${index}`} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.text }}>
                  <strong>{item.source || '—'}</strong> / <strong>{item.medium || '—'}</strong>
                  {item.campaign ? ` / ${item.campaign}` : ''} → <strong>{item.channel}</strong>
                </div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  {item.count.toLocaleString()} touchpoints · confidence {pct(item.confidence)}
                </div>
              </div>
            )) : <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>No new resolved patterns detected in the current sample.</div>}
          </div>

          {preview.warnings.length ? (
            <div style={{ border: `1px solid ${t.color.warning}`, background: t.color.warningSubtle, color: t.color.warning, borderRadius: t.radius.sm, padding: t.space.sm, fontSize: t.font.sizeXs, display: 'grid', gap: 4 }}>
              {preview.warnings.map((warning) => <div key={warning}>{warning}</div>)}
            </div>
          ) : null}
        </>
      )}
    </div>
  )
}
