import { tokens as t } from '../../theme/tokens'
import SectionCard from './SectionCard'

interface AnalysisNarrativePanelProps {
  title?: string
  subtitle?: string
  headline?: string | null
  items: string[]
}

export default function AnalysisNarrativePanel({
  title = 'What changed',
  subtitle,
  headline,
  items,
}: AnalysisNarrativePanelProps) {
  const visibleItems = items.filter((item) => item && item.trim().length > 0)
  if (!headline && !visibleItems.length) return null

  return (
    <SectionCard title={title} subtitle={subtitle}>
      <div style={{ display: 'grid', gap: t.space.sm }}>
        {headline ? (
          <div
            style={{
              fontSize: t.font.sizeMd,
              fontWeight: t.font.weightSemibold,
              color: t.color.text,
              lineHeight: 1.5,
            }}
          >
            {headline}
          </div>
        ) : null}
        {visibleItems.length ? (
          <div style={{ display: 'grid', gap: 8 }}>
            {visibleItems.map((item) => (
              <div
                key={item}
                style={{
                  display: 'grid',
                  gridTemplateColumns: '14px minmax(0, 1fr)',
                  gap: 8,
                  alignItems: 'start',
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeSm,
                  lineHeight: 1.55,
                }}
              >
                <span style={{ color: t.color.accent, fontWeight: t.font.weightBold }}>•</span>
                <span>{item}</span>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    </SectionCard>
  )
}
