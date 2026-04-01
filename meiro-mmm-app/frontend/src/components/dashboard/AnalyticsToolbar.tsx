import type { ReactNode } from 'react'

import { tokens as t } from '../../theme/tokens'

interface AnalyticsToolbarProps {
  searchValue?: string
  onSearchChange?: (value: string) => void
  searchPlaceholder?: string
  searchLabel?: string
  searchMinWidth?: number
  beforeFilters?: ReactNode
  filters?: ReactNode
  actions?: ReactNode
  summary?: ReactNode
  padded?: boolean
}

export default function AnalyticsToolbar({
  searchValue,
  onSearchChange,
  searchPlaceholder = 'Search…',
  searchLabel,
  searchMinWidth = 220,
  beforeFilters,
  filters,
  actions,
  summary,
  padded = false,
}: AnalyticsToolbarProps) {
  return (
    <div
      style={{
        display: 'grid',
        gap: t.space.sm,
        ...(padded
          ? {
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.lg,
              padding: t.space.lg,
              boxShadow: t.shadowSm,
            }
          : {}),
      }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: t.space.md,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap', flex: '1 1 480px' }}>
          {searchLabel ? (
            <label style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary }}>
              {searchLabel}
            </label>
          ) : null}
          {onSearchChange ? (
            <input
              type="search"
              value={searchValue || ''}
              onChange={(event) => onSearchChange(event.target.value)}
              placeholder={searchPlaceholder}
              style={{
                minWidth: searchMinWidth,
                flex: '1 1 280px',
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
                color: t.color.text,
                background: t.color.surface,
              }}
            />
          ) : null}
          {beforeFilters}
          {filters}
        </div>
        {actions ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, flexWrap: 'wrap' }}>
            {actions}
          </div>
        ) : null}
      </div>
      {summary ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>
          {summary}
        </div>
      ) : null}
    </div>
  )
}
