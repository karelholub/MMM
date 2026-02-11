import { ChangeEvent, ReactNode } from 'react'
import { tokens as t } from '../../theme/tokens'

export interface DashboardTableProps {
  children: ReactNode
  search?: {
    value: string
    onChange: (value: string) => void
    placeholder?: string
  }
  actions?: React.ReactNode
  pagination?: React.ReactNode
  density?: 'comfortable' | 'compact'
}

export default function DashboardTable({
  children,
  search,
  actions,
  pagination,
  density = 'comfortable',
}: DashboardTableProps) {
  const padding = density === 'compact' ? t.space.sm : t.space.md

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      {(search || actions) && (
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: t.space.md,
            flexWrap: 'wrap',
          }}
        >
          {search ? (
            <input
              type="search"
              value={search.value}
              onChange={(event: ChangeEvent<HTMLInputElement>) => search.onChange(event.target.value)}
              placeholder={search.placeholder ?? 'Search…'}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                fontSize: t.font.sizeSm,
                minWidth: 200,
              }}
            />
          ) : (
            <span />
          )}
          {actions && <div style={{ display: 'flex', gap: t.space.sm }}>{actions}</div>}
        </div>
      )}
      <div
        style={{
          overflowX: 'auto',
          borderRadius: t.radius.md,
          border: `1px solid ${t.color.borderLight}`,
        }}
      >
        <table
          style={{
            minWidth: '100%',
            borderCollapse: 'separate',
            borderSpacing: 0,
            fontSize: t.font.sizeSm,
            color: t.color.text,
          }}
        >
          {children}
        </table>
      </div>
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
      <style>{`
        table thead th {
          text-transform: uppercase;
          letter-spacing: 0.04em;
          font-size: ${t.font.sizeXs}px;
          color: ${t.color.textSecondary};
          background: ${t.color.bg};
          border-bottom: 1px solid ${t.color.borderLight};
          text-align: left;
          padding: ${padding}px;
        }
        table tbody tr:not(:last-of-type) td {
          border-bottom: 1px solid ${t.color.borderLight};
        }
        table tbody td {
          padding: ${padding}px;
        }
        table tbody tr:hover td {
          background: ${t.color.bg};
        }
      `}</style>
    </div>
  )
}
