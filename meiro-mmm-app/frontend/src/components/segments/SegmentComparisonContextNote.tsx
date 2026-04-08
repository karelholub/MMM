import { tokens as t } from '../../theme/tokens'

type ComparisonMode = 'exact_filter' | 'analytical_lens'

function formatRows(value: number | null | undefined): string {
  return value == null ? '—' : value.toLocaleString()
}

export default function SegmentComparisonContextNote({
  mode,
  pageLabel = 'analysis',
  basisLabel = 'matched journey rows',
  primaryLabel,
  primaryRows,
  otherLabel,
  otherRows,
  baselineRows,
  overlapRows,
}: {
  mode: ComparisonMode
  pageLabel?: string
  basisLabel?: string
  primaryLabel: string
  primaryRows?: number | null
  otherLabel?: string | null
  otherRows?: number | null
  baselineRows?: number | null
  overlapRows?: number | null
}) {
  const title = mode === 'exact_filter' ? 'Exact page filter + derived compare basis' : 'Derived audience lens'
  const body =
    mode === 'exact_filter'
      ? `This page can narrow visible ${pageLabel} directly because the selected audience resolves to compatible page dimensions. The audience comparison below still uses ${basisLabel} so the deltas stay comparable across pages.`
      : `This audience does not collapse cleanly into simple page filters. The page keeps workspace-level views where needed and computes the audience comparison from ${basisLabel}.`

  return (
    <div
      style={{
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.md,
        padding: t.space.md,
        background: t.color.bgSubtle,
        display: 'grid',
        gap: t.space.sm,
      }}
    >
      <div style={{ display: 'grid', gap: 4 }}>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
          Comparison basis
        </div>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{title}</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{body}</div>
      </div>
      <div
        style={{
          display: 'grid',
          gap: t.space.sm,
          gridTemplateColumns: 'repeat(auto-fit, minmax(min(160px, 100%), 1fr))',
        }}
      >
        <div style={{ display: 'grid', gap: 2 }}>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{primaryLabel}</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <strong style={{ color: t.color.text }}>{formatRows(primaryRows)}</strong> basis rows
          </div>
        </div>
        {otherLabel ? (
          <div style={{ display: 'grid', gap: 2 }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>{otherLabel}</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <strong style={{ color: t.color.text }}>{formatRows(otherRows)}</strong> basis rows
            </div>
          </div>
        ) : null}
        {baselineRows != null ? (
          <div style={{ display: 'grid', gap: 2 }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Workspace baseline</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <strong style={{ color: t.color.text }}>{formatRows(baselineRows)}</strong> basis rows
            </div>
          </div>
        ) : null}
        {overlapRows != null ? (
          <div style={{ display: 'grid', gap: 2 }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Shared audience</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <strong style={{ color: t.color.text }}>{formatRows(overlapRows)}</strong> shared rows
            </div>
          </div>
        ) : null}
      </div>
    </div>
  )
}
