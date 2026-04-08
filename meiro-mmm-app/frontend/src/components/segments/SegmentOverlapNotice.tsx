import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGetJson } from '../../lib/apiClient'
import { buildSegmentComparisonHref, type SegmentOverlapResponse, type SegmentRegistryItem } from '../../lib/segments'
import { tokens as t } from '../../theme/tokens'

type NoticeTone = 'warning' | 'accent'

const RELATIONSHIP_TONE: Record<string, NoticeTone> = {
  near_duplicate: 'warning',
  mostly_contained_in_other: 'warning',
  mostly_contains_other: 'warning',
  substantial_overlap: 'accent',
}

export default function SegmentOverlapNotice({
  selectedSegment,
}: {
  selectedSegment: SegmentRegistryItem | null
}) {
  const overlapQuery = useQuery<SegmentOverlapResponse>({
    queryKey: ['segment-overlap-notice', selectedSegment?.id || 'none'],
    queryFn: async () =>
      apiGetJson<SegmentOverlapResponse>(`/api/segments/local/${selectedSegment?.id}/overlap`, {
        fallbackMessage: 'Failed to load segment overlap analysis',
      }),
    enabled: Boolean(selectedSegment?.id),
  })

  const topOverlap = useMemo(() => {
    const items = overlapQuery.data?.items ?? []
    return items.find((item) => item.relationship in RELATIONSHIP_TONE) ?? null
  }, [overlapQuery.data?.items])

  if (!selectedSegment || !topOverlap) return null

  const tone = RELATIONSHIP_TONE[topOverlap.relationship]
  const compareHref = buildSegmentComparisonHref(selectedSegment.id, topOverlap.segment.id)
  const relationshipLabel = topOverlap.relationship.replace(/_/g, ' ')

  return (
    <div
      style={{
        border: `1px solid ${tone === 'warning' ? t.color.warning : t.color.accent}`,
        background: tone === 'warning' ? t.color.warningMuted : t.color.accentMuted,
        borderRadius: t.radius.md,
        padding: t.space.md,
        display: 'grid',
        gap: t.space.xs,
      }}
    >
      <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
        <strong style={{ color: t.color.text }}>Audience overlap detected</strong>
        <span
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            padding: '2px 8px',
            borderRadius: 999,
            fontSize: t.font.sizeXs,
            fontWeight: t.font.weightMedium,
            border: `1px solid ${tone === 'warning' ? t.color.warning : t.color.accent}`,
            color: tone === 'warning' ? t.color.warning : t.color.accent,
            background: t.color.surface,
          }}
        >
          {relationshipLabel}
        </span>
      </div>
      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
        <strong style={{ color: t.color.text }}>{selectedSegment.name}</strong> overlaps strongly with{' '}
        <strong style={{ color: t.color.text }}>{topOverlap.segment.name}</strong>:
        {' '}
        {Math.round(topOverlap.overlap_share_of_primary * 100)}% of the selected audience and{' '}
        {Math.round(topOverlap.overlap_share_of_other * 100)}% of the overlapping audience are shared.
      </div>
      <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap', fontSize: t.font.sizeSm }}>
        <a href={compareHref} style={{ color: t.color.accent, textDecoration: 'none', fontWeight: t.font.weightMedium }}>
          Compare audiences
        </a>
        <a href="/?page=settings#settings/segments" style={{ color: t.color.accent, textDecoration: 'none' }}>
          Manage segments
        </a>
      </div>
    </div>
  )
}
