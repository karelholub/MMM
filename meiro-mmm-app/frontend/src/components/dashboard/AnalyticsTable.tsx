import { CSSProperties, ReactNode, UIEvent, useEffect, useMemo, useState } from 'react'
import { tokens as t } from '../../theme/tokens'

type TableAlign = 'left' | 'center' | 'right'

export interface AnalyticsTableColumn<T> {
  key: string
  label: ReactNode
  render: (row: T, index: number) => ReactNode
  hideable?: boolean
  align?: TableAlign
  width?: number | string
  sortable?: boolean
  sortDirection?: 'asc' | 'desc' | null
  onSort?: () => void
  title?: string
  headerStyle?: CSSProperties
  cellStyle?: CSSProperties | ((row: T, index: number) => CSSProperties | undefined)
}

export interface AnalyticsTablePreset {
  key: string
  label: ReactNode
  hiddenColumnKeys?: string[]
  visibleColumnKeys?: string[]
}

export interface AnalyticsTableProps<T> {
  columns: AnalyticsTableColumn<T>[]
  rows: T[]
  rowKey: (row: T, index: number) => string
  toolbar?: ReactNode
  emptyState?: ReactNode
  pagination?: ReactNode
  density?: 'comfortable' | 'compact'
  minWidth?: number | string
  stickyFirstColumn?: boolean
  zebra?: boolean
  hoverHighlight?: boolean
  onRowClick?: (row: T, index: number) => void
  isRowActive?: (row: T, index: number) => boolean
  getRowStyle?: (row: T, index: number) => CSSProperties | undefined
  tableLabel?: string
  allowColumnHiding?: boolean
  defaultHiddenColumnKeys?: string[]
  allowDensityToggle?: boolean
  persistKey?: string
  virtualized?: boolean
  virtualizationThreshold?: number
  virtualRowHeight?: number
  virtualizationHeight?: number
  overscan?: number
  presets?: AnalyticsTablePreset[]
  defaultPresetKey?: string
}

function resolveCellStyle<T>(
  style: AnalyticsTableColumn<T>['cellStyle'],
  row: T,
  index: number,
): CSSProperties | undefined {
  if (!style) return undefined
  return typeof style === 'function' ? style(row, index) : style
}

function resolvePresetHiddenColumnKeys<T>(
  columns: AnalyticsTableColumn<T>[],
  preset: AnalyticsTablePreset | undefined,
  fallback: string[],
): string[] {
  if (!preset) return fallback
  if (preset.visibleColumnKeys?.length) {
    const visibleSet = new Set(preset.visibleColumnKeys)
    return columns
      .filter((column) => column.hideable !== false && !visibleSet.has(column.key))
      .map((column) => column.key)
  }
  return preset.hiddenColumnKeys ?? fallback
}

function sameStringArray(left: string[], right: string[]) {
  if (left.length !== right.length) return false
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return false
  }
  return true
}

export default function AnalyticsTable<T>({
  columns,
  rows,
  rowKey,
  toolbar,
  emptyState,
  pagination,
  density = 'comfortable',
  minWidth = '100%',
  stickyFirstColumn = false,
  zebra = true,
  hoverHighlight = true,
  onRowClick,
  isRowActive,
  getRowStyle,
  tableLabel = 'Analytics table',
  allowColumnHiding = false,
  defaultHiddenColumnKeys = [],
  allowDensityToggle = false,
  persistKey,
  virtualized = false,
  virtualizationThreshold = 100,
  virtualRowHeight,
  virtualizationHeight = 640,
  overscan = 6,
  presets = [],
  defaultPresetKey,
}: AnalyticsTableProps<T>) {
  const defaultPreset = useMemo(
    () => presets.find((preset) => preset.key === defaultPresetKey),
    [defaultPresetKey, presets],
  )
  const [hiddenColumnKeys, setHiddenColumnKeys] = useState<string[]>(
    resolvePresetHiddenColumnKeys(columns, defaultPreset, defaultHiddenColumnKeys),
  )
  const [currentDensity, setCurrentDensity] = useState<'comfortable' | 'compact'>(density)
  const [scrollTop, setScrollTop] = useState(0)
  const [activePresetKey, setActivePresetKey] = useState<string | null>(defaultPreset?.key ?? null)

  useEffect(() => {
    setCurrentDensity(density)
  }, [density])

  useEffect(() => {
    if (!persistKey || typeof window === 'undefined') return
    try {
      const savedHidden = window.localStorage.getItem(`${persistKey}:hidden-columns`)
      const savedDensity = window.localStorage.getItem(`${persistKey}:density`)
      const savedPreset = window.localStorage.getItem(`${persistKey}:preset`)
      const preset = presets.find((item) => item.key === savedPreset) ?? defaultPreset
      const nextHiddenColumnKeys = savedHidden
        ? (() => {
            const parsed = JSON.parse(savedHidden)
            return Array.isArray(parsed)
              ? parsed.filter((value): value is string => typeof value === 'string')
              : resolvePresetHiddenColumnKeys(columns, preset, defaultHiddenColumnKeys)
          })()
        : resolvePresetHiddenColumnKeys(columns, preset, defaultHiddenColumnKeys)
      const nextDensity =
        savedDensity === 'comfortable' || savedDensity === 'compact'
          ? savedDensity
          : density
      const nextPresetKey = preset?.key ?? null

      setHiddenColumnKeys((current) =>
        sameStringArray(current, nextHiddenColumnKeys) ? current : nextHiddenColumnKeys,
      )
      setCurrentDensity((current) =>
        current === nextDensity ? current : nextDensity,
      )
      setActivePresetKey((current) =>
        current === nextPresetKey ? current : nextPresetKey,
      )
    } catch {
      const fallbackHiddenColumnKeys = resolvePresetHiddenColumnKeys(columns, defaultPreset, defaultHiddenColumnKeys)
      setHiddenColumnKeys((current) =>
        sameStringArray(current, fallbackHiddenColumnKeys) ? current : fallbackHiddenColumnKeys,
      )
      setCurrentDensity((current) =>
        current === density ? current : density,
      )
      setActivePresetKey((current) =>
        current === (defaultPreset?.key ?? null) ? current : (defaultPreset?.key ?? null),
      )
    }
  }, [columns, defaultHiddenColumnKeys, defaultPreset, density, persistKey, presets])

  useEffect(() => {
    if (!persistKey || typeof window === 'undefined') return
    try {
      window.localStorage.setItem(`${persistKey}:hidden-columns`, JSON.stringify(hiddenColumnKeys))
      window.localStorage.setItem(`${persistKey}:density`, currentDensity)
      if (activePresetKey) {
        window.localStorage.setItem(`${persistKey}:preset`, activePresetKey)
      } else {
        window.localStorage.removeItem(`${persistKey}:preset`)
      }
    } catch {
      // Ignore localStorage persistence errors and keep the table usable.
    }
  }, [activePresetKey, currentDensity, hiddenColumnKeys, persistKey])

  const visibleColumns = useMemo(() => {
    const next = allowColumnHiding
      ? columns.filter((column) => !hiddenColumnKeys.includes(column.key))
      : columns
    return next.length > 0 ? next : [columns[0]].filter(Boolean)
  }, [allowColumnHiding, columns, hiddenColumnKeys])

  const paddingY = currentDensity === 'compact' ? t.space.sm : t.space.md
  const paddingX = currentDensity === 'compact' ? t.space.md : t.space.lg
  const estimatedRowHeight = virtualRowHeight ?? (currentDensity === 'compact' ? 40 : 52)
  const shouldVirtualize = virtualized && rows.length > virtualizationThreshold
  const visibleRowCount = shouldVirtualize
    ? Math.ceil(virtualizationHeight / estimatedRowHeight) + overscan * 2
    : rows.length
  const startIndex = shouldVirtualize
    ? Math.max(0, Math.floor(scrollTop / estimatedRowHeight) - overscan)
    : 0
  const endIndex = shouldVirtualize
    ? Math.min(rows.length, startIndex + visibleRowCount)
    : rows.length
  const visibleRows = shouldVirtualize ? rows.slice(startIndex, endIndex) : rows
  const topSpacerHeight = shouldVirtualize ? startIndex * estimatedRowHeight : 0
  const bottomSpacerHeight = shouldVirtualize ? Math.max(0, (rows.length - endIndex) * estimatedRowHeight) : 0
  const showControls = allowColumnHiding || allowDensityToggle || presets.length > 0
  const controls = showControls ? (
    <div
      style={{
        display: 'flex',
        gap: t.space.sm,
        alignItems: 'center',
        flexWrap: 'wrap',
        justifyContent: toolbar ? 'flex-end' : 'space-between',
      }}
    >
      {presets.length > 0 && (
        <div
          style={{
            display: 'inline-flex',
            borderRadius: t.radius.full,
            border: `1px solid ${t.color.borderLight}`,
            overflow: 'hidden',
            background: t.color.surface,
            flexWrap: 'wrap',
          }}
        >
          {presets.map((preset) => (
            <button
              key={preset.key}
              type="button"
              onClick={() => {
                setActivePresetKey(preset.key)
                setHiddenColumnKeys(resolvePresetHiddenColumnKeys(columns, preset, defaultHiddenColumnKeys))
              }}
              style={{
                border: 'none',
                padding: `${t.space.xs}px ${t.space.sm}px`,
                cursor: 'pointer',
                background: activePresetKey === preset.key ? t.color.accentMuted : 'transparent',
                color: activePresetKey === preset.key ? t.color.accent : t.color.textSecondary,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightSemibold,
              }}
            >
              {preset.label}
            </button>
          ))}
        </div>
      )}
      {allowDensityToggle && (
        <div
          style={{
            display: 'inline-flex',
            borderRadius: t.radius.full,
            border: `1px solid ${t.color.borderLight}`,
            overflow: 'hidden',
            background: t.color.surface,
          }}
        >
          {(['comfortable', 'compact'] as const).map((option) => (
            <button
              key={option}
              type="button"
              onClick={() => setCurrentDensity(option)}
              style={{
                border: 'none',
                padding: `${t.space.xs}px ${t.space.sm}px`,
                cursor: 'pointer',
                background: currentDensity === option ? t.color.accentMuted : 'transparent',
                color: currentDensity === option ? t.color.accent : t.color.textSecondary,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightSemibold,
              }}
            >
              {option === 'comfortable' ? 'Comfortable' : 'Compact'}
            </button>
          ))}
        </div>
      )}
      {allowColumnHiding && (
        <details style={{ position: 'relative' }}>
          <summary
            style={{
              listStyle: 'none',
              cursor: 'pointer',
              userSelect: 'none',
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.sm,
              padding: `${t.space.xs}px ${t.space.sm}px`,
              fontSize: t.font.sizeXs,
              color: t.color.textSecondary,
              background: t.color.surface,
            }}
          >
            Columns
          </summary>
          <div
            style={{
              position: 'absolute',
              right: 0,
              top: `calc(100% + ${t.space.xs}px)`,
              zIndex: 10,
              minWidth: 220,
              display: 'grid',
              gap: t.space.xs,
              padding: t.space.sm,
              borderRadius: t.radius.md,
              border: `1px solid ${t.color.borderLight}`,
              background: t.color.surface,
              boxShadow: t.shadowLg,
            }}
          >
            {columns.map((column) => {
              const checked = !hiddenColumnKeys.includes(column.key)
              const hideable = column.hideable !== false
              const onlyVisibleColumn = visibleColumns.length <= 1 && checked
              return (
                <label
                  key={column.key}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: t.space.xs,
                    fontSize: t.font.sizeSm,
                    color: hideable ? t.color.text : t.color.textMuted,
                  }}
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={!hideable || onlyVisibleColumn}
                    onChange={(event) => {
                      const nextChecked = event.target.checked
                      setActivePresetKey(null)
                      setHiddenColumnKeys((current) =>
                        nextChecked
                          ? current.filter((key) => key !== column.key)
                          : [...current, column.key]
                      )
                    }}
                  />
                  <span>{column.label}</span>
                </label>
              )
            })}
          </div>
        </details>
      )}
    </div>
  ) : null

  const handleScroll = (event: UIEvent<HTMLDivElement>) => {
    if (!shouldVirtualize) return
    setScrollTop(event.currentTarget.scrollTop)
  }

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      {(toolbar || controls) && (
        <div
          style={{
            display: 'flex',
            gap: t.space.sm,
            justifyContent: 'space-between',
            alignItems: 'flex-start',
            flexWrap: 'wrap',
          }}
        >
          {toolbar ? <div style={{ display: 'grid', gap: t.space.sm, flex: '1 1 320px' }}>{toolbar}</div> : <div />}
          {controls}
        </div>
      )}
      <div
        style={{
          overflowX: 'auto',
          overflowY: shouldVirtualize ? 'auto' : undefined,
          maxHeight: shouldVirtualize ? virtualizationHeight : undefined,
          borderRadius: t.radius.md,
          border: `1px solid ${t.color.borderLight}`,
          background: t.color.surface,
        }}
        onScroll={handleScroll}
      >
        <table
          className="analytics-table"
          aria-label={tableLabel}
          style={{
            minWidth,
            width: '100%',
            borderCollapse: 'separate',
            borderSpacing: 0,
            fontSize: t.font.sizeSm,
            color: t.color.text,
          }}
        >
          <thead>
            <tr>
              {visibleColumns.map((column, index) => {
                const stickyStyles: CSSProperties =
                  stickyFirstColumn && index === 0
                    ? {
                        position: 'sticky',
                        left: 0,
                        zIndex: 3,
                        boxShadow: `1px 0 0 ${t.color.borderLight}`,
                      }
                    : {}
                return (
                  <th
                    key={column.key}
                    style={{
                      padding: `${paddingY}px ${paddingX}px`,
                      textAlign: column.align ?? 'left',
                      fontWeight: t.font.weightSemibold,
                      color: t.color.textSecondary,
                      whiteSpace: 'nowrap',
                      textTransform: 'uppercase',
                      letterSpacing: '0.04em',
                      fontSize: t.font.sizeXs,
                      background: t.color.bg,
                      borderBottom: `1px solid ${t.color.borderLight}`,
                      ...(shouldVirtualize
                        ? {
                            position: 'sticky',
                            top: 0,
                            zIndex: stickyFirstColumn && index === 0 ? 5 : 4,
                          }
                        : {}),
                      userSelect: column.sortable ? 'none' : undefined,
                      cursor: column.sortable ? 'pointer' : undefined,
                      width: column.width,
                      ...stickyStyles,
                      ...column.headerStyle,
                    }}
                    onClick={column.sortable ? column.onSort : undefined}
                    title={column.title}
                  >
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                      <span>{column.label}</span>
                      {column.sortDirection && (
                        <span aria-hidden>{column.sortDirection === 'asc' ? '↑' : '↓'}</span>
                      )}
                    </span>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td
                  colSpan={visibleColumns.length}
                  style={{
                    padding: `${t.space.xl}px ${paddingX}px`,
                    textAlign: 'center',
                    color: t.color.textSecondary,
                    background: t.color.surface,
                  }}
                >
                  {emptyState ?? 'No rows match the current filters.'}
                </td>
              </tr>
            ) : (
              <>
                {shouldVirtualize && topSpacerHeight > 0 && (
                  <tr aria-hidden="true">
                    <td
                      colSpan={visibleColumns.length}
                      style={{
                        height: topSpacerHeight,
                        padding: 0,
                        border: 'none',
                        background: t.color.surface,
                      }}
                    />
                  </tr>
                )}
                {visibleRows.map((row, rowIndex) => {
                  const absoluteRowIndex = startIndex + rowIndex
                  const active = isRowActive?.(row, absoluteRowIndex) ?? false
                  const baseBackground = active
                    ? t.color.accentMuted
                    : zebra && absoluteRowIndex % 2 === 1
                    ? t.color.bg
                    : t.color.surface
                  return (
                    <tr
                      key={rowKey(row, absoluteRowIndex)}
                      style={{
                        background: baseBackground,
                        cursor: onRowClick ? 'pointer' : undefined,
                        ...getRowStyle?.(row, absoluteRowIndex),
                      }}
                      onClick={onRowClick ? () => onRowClick(row, absoluteRowIndex) : undefined}
                    >
                      {visibleColumns.map((column, columnIndex) => {
                        const stickyStyles: CSSProperties =
                          stickyFirstColumn && columnIndex === 0
                            ? {
                                position: 'sticky',
                                left: 0,
                                zIndex: 2,
                                boxShadow: `1px 0 0 ${t.color.borderLight}`,
                                background: baseBackground,
                              }
                            : {}
                        return (
                          <td
                            key={column.key}
                            style={{
                              padding: `${paddingY}px ${paddingX}px`,
                              textAlign: column.align ?? 'left',
                              borderBottom:
                                absoluteRowIndex === rows.length - 1 ? 'none' : `1px solid ${t.color.borderLight}`,
                              fontVariantNumeric: column.align === 'right' ? 'tabular-nums' : undefined,
                              verticalAlign: 'middle',
                              ...(hoverHighlight
                                ? {
                                    transition: 'background-color 120ms ease',
                                  }
                                : {}),
                              ...stickyStyles,
                              ...resolveCellStyle(column.cellStyle, row, absoluteRowIndex),
                            }}
                          >
                            {column.render(row, absoluteRowIndex)}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
                {shouldVirtualize && bottomSpacerHeight > 0 && (
                  <tr aria-hidden="true">
                    <td
                      colSpan={visibleColumns.length}
                      style={{
                        height: bottomSpacerHeight,
                        padding: 0,
                        border: 'none',
                        background: t.color.surface,
                      }}
                    />
                  </tr>
                )}
              </>
            )}
          </tbody>
        </table>
      </div>
      <style>{`
        .analytics-table tbody tr:hover td {
          background: ${hoverHighlight ? t.color.accentMuted : 'inherit'};
        }
      `}</style>
      {pagination && (
        <div
          style={{
            display: 'flex',
            justifyContent: 'flex-end',
            alignItems: 'center',
            gap: t.space.md,
            fontSize: t.font.sizeSm,
          }}
        >
          {pagination}
        </div>
      )}
    </div>
  )
}
