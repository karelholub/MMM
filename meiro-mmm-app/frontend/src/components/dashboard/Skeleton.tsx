import { tokens as t } from '../../theme/tokens'

export interface SkeletonProps {
  width?: number | string
  height?: number | string
  borderRadius?: number
  style?: React.CSSProperties
}

export default function Skeleton({
  width = '100%',
  height = 16,
  borderRadius = t.radius.sm,
  style = {},
}: SkeletonProps) {
  return (
    <div
      aria-hidden
      style={{
        width,
        height,
        borderRadius,
        backgroundColor: t.color.borderLight,
        ...style,
      }}
    />
  )
}

export function KpiTileSkeleton() {
  return (
    <div
      style={{
        display: 'grid',
        gap: t.space.sm,
        padding: t.space.lg,
        background: t.color.surface,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.borderLight}`,
        boxShadow: t.shadowSm,
        minWidth: 180,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Skeleton width={100} height={14} />
        <Skeleton width={48} height={20} borderRadius={999} />
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: t.space.sm }}>
        <Skeleton width={80} height={28} />
        <Skeleton width={56} height={14} />
      </div>
      <Skeleton width="100%" height={32} />
    </div>
  )
}
