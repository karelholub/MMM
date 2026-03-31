import { CSSProperties, ReactNode } from 'react'
import { tokens as t } from '../../theme/tokens'

type TableAlign = 'left' | 'center' | 'right'

export interface AnalyticsTableColumn<T> {
  key: string
  label: ReactNode
  render: (row: T, index: number) => ReactNode
  align?: TableAlign
  width?: number | string
  sortable?: boolean
  sortDirection?: 'asc' | 'desc' | null
  onSort?: () => void
  title?: string
  headerStyle?: CSSProperties
  cellStyle?: CSSProperties | ((row: T, index: number) => CSSProperties | undefined)
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
}

function resolveCellStyle<T>(
  style: AnalyticsTableColumn<T>['cellStyle'],
  row: T,
  index: number,
): CSSProperties | undefined {
  if (!style) return undefined
  return typeof style === 'function' ? style(row, index) : style
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
}: AnalyticsTableProps<T>) {
  const paddingY = density === 'compact' ? t.space.sm : t.space.md
  const paddingX = density === 'compact' ? t.space.md : t.space.lg

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      {toolbar && <div style={{ display: 'grid', gap: t.space.sm }}>{toolbar}</div>}
      <div
        style={{
          overflowX: 'auto',
          borderRadius: t.radius.md,
          border: `1px solid ${t.color.borderLight}`,
          background: t.color.surface,
        }}
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
              {columns.map((column, index) => {
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
                  colSpan={columns.length}
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
              rows.map((row, rowIndex) => {
                const active = isRowActive?.(row, rowIndex) ?? false
                const baseBackground = active
                  ? t.color.accentMuted
                  : zebra && rowIndex % 2 === 1
                  ? t.color.bg
                  : t.color.surface
                return (
                  <tr
                    key={rowKey(row, rowIndex)}
                    style={{
                      background: baseBackground,
                      cursor: onRowClick ? 'pointer' : undefined,
                      ...getRowStyle?.(row, rowIndex),
                    }}
                    onClick={onRowClick ? () => onRowClick(row, rowIndex) : undefined}
                  >
                    {columns.map((column, columnIndex) => {
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
                              rowIndex === rows.length - 1 ? 'none' : `1px solid ${t.color.borderLight}`,
                            fontVariantNumeric: column.align === 'right' ? 'tabular-nums' : undefined,
                            verticalAlign: 'middle',
                            ...(hoverHighlight
                              ? {
                                  transition: 'background-color 120ms ease',
                                }
                              : {}),
                            ...stickyStyles,
                            ...resolveCellStyle(column.cellStyle, row, rowIndex),
                          }}
                        >
                          {column.render(row, rowIndex)}
                        </td>
                      )
                    })}
                  </tr>
                )
              })
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
