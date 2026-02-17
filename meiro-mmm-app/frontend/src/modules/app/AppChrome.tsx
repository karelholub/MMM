import { useEffect, useState, type CSSProperties, type Dispatch, type SetStateAction } from 'react'
import { tokens } from '../../theme/tokens'

export type AppPage =
  | 'overview'
  | 'alerts'
  | 'dashboard'
  | 'comparison'
  | 'paths'
  | 'campaigns'
  | 'expenses'
  | 'datasources'
  | 'mmm'
  | 'settings'
  | 'dq'
  | 'incrementality'
  | 'path_archetypes'
  | 'analytics_journeys'
  | 'datasets'

export type NavSection =
  | 'Overview'
  | 'Analytics'
  | 'Attribution'
  | 'Journeys'
  | 'Causal / Planning'
  | 'Data & Ops'
  | 'Admin'

export interface NavItem {
  key: AppPage
  label: string
  section: NavSection
  icon: string
  breadcrumb: string
}

export const NAV_ITEMS: NavItem[] = [
  { key: 'overview', label: 'Overview', section: 'Overview', icon: 'OV', breadcrumb: 'Overview' },
  { key: 'analytics_journeys', label: 'Journeys', section: 'Analytics', icon: 'JR', breadcrumb: 'Analytics / Journeys' },
  { key: 'alerts', label: 'Alerts', section: 'Data & Ops', icon: 'AL', breadcrumb: 'Data & Ops / Alerts' },
  { key: 'dashboard', label: 'Channel performance', section: 'Attribution', icon: 'CH', breadcrumb: 'Attribution / Channel performance' },
  { key: 'campaigns', label: 'Campaign performance', section: 'Attribution', icon: 'CA', breadcrumb: 'Attribution / Campaign performance' },
  { key: 'comparison', label: 'Attribution models', section: 'Attribution', icon: 'AM', breadcrumb: 'Attribution / Attribution models' },
  { key: 'paths', label: 'Conversion paths', section: 'Journeys', icon: 'JP', breadcrumb: 'Journeys / Conversion paths' },
  { key: 'path_archetypes', label: 'Path archetypes', section: 'Journeys', icon: 'JA', breadcrumb: 'Journeys / Path archetypes' },
  { key: 'incrementality', label: 'Incrementality', section: 'Causal / Planning', icon: 'IC', breadcrumb: 'Causal / Incrementality' },
  { key: 'mmm', label: 'MMM (advanced)', section: 'Causal / Planning', icon: 'MM', breadcrumb: 'Causal / MMM (advanced)' },
  { key: 'dq', label: 'Data quality', section: 'Data & Ops', icon: 'DQ', breadcrumb: 'Data & Ops / Data quality' },
  { key: 'datasources', label: 'Data sources', section: 'Data & Ops', icon: 'DS', breadcrumb: 'Data & Ops / Data sources' },
  { key: 'expenses', label: 'Expenses', section: 'Data & Ops', icon: 'EX', breadcrumb: 'Data & Ops / Expenses' },
  { key: 'settings', label: 'Settings', section: 'Admin', icon: 'ST', breadcrumb: 'Admin / Settings' },
]

export const ATTRIBUTION_MODELS = [
  { id: 'last_touch', label: 'Last Touch' },
  { id: 'first_touch', label: 'First Touch' },
  { id: 'linear', label: 'Linear' },
  { id: 'time_decay', label: 'Time Decay' },
  { id: 'position_based', label: 'Position Based' },
  { id: 'markov', label: 'Data-Driven (Markov)' },
]

function renderNavIcon(page: AppPage, active: boolean) {
  const stroke = active ? tokens.color.accent : tokens.color.textMuted
  const common = {
    width: 14,
    height: 14,
    viewBox: '0 0 16 16',
    fill: 'none',
    stroke,
    strokeWidth: 1.5,
    strokeLinecap: 'round' as const,
    strokeLinejoin: 'round' as const,
    'aria-hidden': true,
  }

  if (page === 'overview') {
    return (
      <svg {...common}>
        <rect x="2" y="2" width="5" height="5" />
        <rect x="9" y="2" width="5" height="5" />
        <rect x="2" y="9" width="5" height="5" />
        <rect x="9" y="9" width="5" height="5" />
      </svg>
    )
  }
  if (page === 'alerts' || page === 'dq') {
    return (
      <svg {...common}>
        <path d="M8 2.5a3 3 0 0 0-3 3V8L3.5 10v1h9V10L11 8V5.5a3 3 0 0 0-3-3Z" />
        <path d="M6.5 12.5a1.5 1.5 0 0 0 3 0" />
      </svg>
    )
  }
  if (page === 'settings') {
    return (
      <svg {...common}>
        <circle cx="8" cy="8" r="2.2" />
        <path d="M8 2.2v1.6M8 12.2v1.6M2.2 8h1.6M12.2 8h1.6M3.7 3.7l1.1 1.1M11.2 11.2l1.1 1.1M12.3 3.7l-1.1 1.1M4.8 11.2l-1.1 1.1" />
      </svg>
    )
  }
  if (page === 'datasources' || page === 'datasets' || page === 'expenses') {
    return (
      <svg {...common}>
        <ellipse cx="8" cy="3.5" rx="5.5" ry="2" />
        <path d="M2.5 3.5v4c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2v-4" />
        <path d="M2.5 7.5v4c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2v-4" />
      </svg>
    )
  }
  if (page === 'incrementality' || page === 'mmm') {
    return (
      <svg {...common}>
        <path d="M2.5 12.5h11" />
        <path d="M3.5 10l3-3 2 2 4-4" />
        <path d="M10.8 5h1.7v1.7" />
      </svg>
    )
  }
  if (page === 'dashboard' || page === 'campaigns' || page === 'comparison') {
    return (
      <svg {...common}>
        <path d="M3 13V9M8 13V6M13 13V3" />
        <path d="M2 13.5h12" />
      </svg>
    )
  }
  return (
    <svg {...common}>
      <circle cx="3.5" cy="8" r="1.5" />
      <circle cx="8" cy="4" r="1.5" />
      <circle cx="12.5" cy="8" r="1.5" />
      <path d="M5 7l1.7-1.8M9.3 5.2 11 7" />
    </svg>
  )
}

interface AppSidebarProps {
  page: AppPage
  navSearch: string
  sidebarCollapsed: boolean
  pinnedPages: AppPage[]
  visibleNavItems: NavItem[]
  onToggleCollapse: () => void
  onNavSearchChange: (value: string) => void
  onSetPage: (page: AppPage) => void
  onSetPinnedPages: Dispatch<SetStateAction<AppPage[]>>
}

export function AppSidebar({
  page,
  navSearch,
  sidebarCollapsed,
  pinnedPages,
  visibleNavItems,
  onToggleCollapse,
  onNavSearchChange,
  onSetPage,
  onSetPinnedPages,
}: AppSidebarProps) {
  return (
    <aside
      style={{
        gridRow: '1 / span 2',
        gridColumn: 1,
        background: tokens.color.surface,
        borderRight: `1px solid ${tokens.color.borderLight}`,
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: sidebarCollapsed ? 'center' : 'space-between',
          padding: '12px 10px',
          borderBottom: `1px solid ${tokens.color.borderLight}`,
        }}
      >
        {!sidebarCollapsed && (
          <div>
            <div style={{ fontSize: tokens.font.sizeSm, fontWeight: tokens.font.weightSemibold, color: tokens.color.text, letterSpacing: '-0.02em' }}>
              Meiro measurement
            </div>
            <div style={{ fontSize: tokens.font.sizeXs, color: tokens.color.textSecondary }}>Workspace</div>
          </div>
        )}
        <button
          type="button"
          onClick={onToggleCollapse}
          style={{ border: 'none', background: 'transparent', cursor: 'pointer', padding: 6, borderRadius: tokens.radius.full, color: tokens.color.textSecondary }}
          title={sidebarCollapsed ? 'Expand navigation' : 'Collapse navigation'}
        >
          {sidebarCollapsed ? '›' : '‹'}
        </button>
      </div>

      {!sidebarCollapsed && (
        <div style={{ padding: '10px 12px 4px', borderBottom: `1px solid ${tokens.color.borderLight}` }}>
          <input
            type="text"
            value={navSearch}
            onChange={(e) => onNavSearchChange(e.target.value)}
            placeholder="Search pages…"
            style={{ width: '100%', padding: '6px 10px', fontSize: tokens.font.sizeSm, borderRadius: tokens.radius.sm, border: `1px solid ${tokens.color.borderLight}`, backgroundColor: tokens.color.bg, color: tokens.color.text }}
          />
        </div>
      )}

      <nav style={{ flex: 1, overflowY: 'auto', padding: sidebarCollapsed ? '8px 4px' : '8px 8px 16px' }}>
        {(() => {
          const filtered = visibleNavItems.filter((item) => item.label.toLowerCase().includes(navSearch.trim().toLowerCase()))
          const pinned = filtered.filter((item) => pinnedPages.includes(item.key))
          const bySection: Record<NavSection, NavItem[]> = {
            Overview: [],
            Analytics: [],
            Attribution: [],
            'Causal / Planning': [],
            Journeys: [],
            'Data & Ops': [],
            Admin: [],
          }
          filtered.forEach((item) => {
            bySection[item.section].push(item)
          })

          const renderItem = (item: NavItem) => {
            const active = page === item.key
            const isPinned = pinnedPages.includes(item.key)
            const baseStyle: CSSProperties = {
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              width: '100%',
              border: 'none',
              background: active ? tokens.color.accentMuted : 'transparent',
              color: active ? tokens.color.accent : tokens.color.textSecondary,
              padding: sidebarCollapsed ? '6px 6px' : '6px 10px',
              borderRadius: tokens.radius.md,
              cursor: 'pointer',
              fontSize: tokens.font.sizeSm,
              fontWeight: active ? tokens.font.weightSemibold : tokens.font.weightMedium,
            }
            return (
              <div key={item.key} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <button type="button" onClick={() => onSetPage(item.key)} style={baseStyle} title={sidebarCollapsed ? item.label : undefined}>
                  <div style={{ width: 24, height: 24, borderRadius: 999, border: `1px solid ${active ? tokens.color.accent : tokens.color.borderLight}`, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: tokens.font.sizeXs, color: active ? tokens.color.accent : tokens.color.textMuted, backgroundColor: tokens.color.surface, flexShrink: 0 }}>
                    {renderNavIcon(item.key, active)}
                  </div>
                  {!sidebarCollapsed && <span>{item.label}</span>}
                </button>
                {!sidebarCollapsed && (
                  <button
                    type="button"
                    onClick={() => {
                      onSetPinnedPages((prev) => {
                        const next = isPinned ? prev.filter((p) => p !== item.key) : [...prev, item.key]
                        try {
                          window.localStorage.setItem('mmm-pinned-pages-v1', JSON.stringify(next))
                        } catch {
                          // ignore
                        }
                        return next
                      })
                    }}
                    style={{ border: 'none', background: 'transparent', cursor: 'pointer', padding: 2, fontSize: tokens.font.sizeXs, color: isPinned ? tokens.color.accent : tokens.color.textMuted }}
                    title={isPinned ? 'Unpin from favorites' : 'Pin to favorites'}
                  >
                    {isPinned ? '★' : '☆'}
                  </button>
                )}
              </div>
            )
          }

          const sectionsInOrder: NavSection[] = ['Overview', 'Analytics', 'Attribution', 'Journeys', 'Causal / Planning', 'Data & Ops', 'Admin']
          return (
            <>
              {pinned.length > 0 && !sidebarCollapsed && (
                <div style={{ marginBottom: 12 }}>
                  <div style={{ fontSize: tokens.font.sizeXs, textTransform: 'uppercase', letterSpacing: '0.08em', color: tokens.color.textMuted, margin: '4px 8px' }}>
                    Pinned
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                    {pinned.map(renderItem)}
                  </div>
                </div>
              )}
              {sectionsInOrder.map((section) => {
                const items = bySection[section]
                if (!items.length) return null
                return (
                  <div key={section} style={{ marginBottom: 12 }}>
                    {!sidebarCollapsed && (
                      <div style={{ fontSize: tokens.font.sizeXs, textTransform: 'uppercase', letterSpacing: '0.08em', color: tokens.color.textMuted, margin: '6px 8px' }}>
                        {section}
                      </div>
                    )}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      {items.map(renderItem)}
                    </div>
                  </div>
                )
              })}
            </>
          )
        })()}
      </nav>
    </aside>
  )
}

interface ModelConfigOption {
  id: string
  name: string
  status: string
  version: number
}

interface AppTopBarProps {
  isMobileHeader: boolean
  periodLabel: string
  periodDateFrom: string
  periodDateTo: string
  conversionLabel: string
  selectedModel: string
  selectedConfigId: string | null
  modelConfigs: ModelConfigOption[]
  modelConfigsLoading: boolean
  journeysLoaded: boolean
  journeyCount: number
  primaryKpiLabel?: string
  primaryKpiCount?: number
  convertedCount: number
  loadingSample: boolean
  runningModels: boolean
  onModelChange: (value: string) => void
  onConfigChange: (value: string | null) => void
  onLoadSample: () => void
  onRunModels: () => void
  onPeriodChange: (next: { dateFrom: string; dateTo: string }) => void
}

export function AppTopBar({
  isMobileHeader,
  periodLabel,
  periodDateFrom,
  periodDateTo,
  conversionLabel,
  selectedModel,
  selectedConfigId,
  modelConfigs,
  modelConfigsLoading,
  journeysLoaded,
  journeyCount,
  primaryKpiLabel,
  primaryKpiCount,
  convertedCount,
  loadingSample,
  runningModels,
  onModelChange,
  onConfigChange,
  onLoadSample,
  onRunModels,
  onPeriodChange,
}: AppTopBarProps) {
  const [draftDateFrom, setDraftDateFrom] = useState(periodDateFrom)
  const [draftDateTo, setDraftDateTo] = useState(periodDateTo)
  useEffect(() => {
    setDraftDateFrom(periodDateFrom)
    setDraftDateTo(periodDateTo)
  }, [periodDateFrom, periodDateTo])
  const canApplyPeriod =
    !!draftDateFrom &&
    !!draftDateTo &&
    draftDateFrom <= draftDateTo &&
    (draftDateFrom !== periodDateFrom || draftDateTo !== periodDateTo)

  return (
    <header
      style={{
        gridRow: 1,
        gridColumn: 2,
        display: 'grid',
        gridTemplateAreas: isMobileHeader ? '"title" "period" "controls"' : '"title period" "controls controls"',
        gridTemplateColumns: isMobileHeader ? 'minmax(0,1fr)' : 'minmax(0,1fr) minmax(220px,340px)',
        alignItems: 'center',
        padding: isMobileHeader ? '10px 12px' : '8px 20px',
        borderBottom: `1px solid ${tokens.color.borderLight}`,
        background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #1d4ed8 100%)',
        color: '#e5e7eb',
        gap: 10,
        position: 'relative',
        zIndex: 20,
        minWidth: 0,
      }}
    >
      <div style={{ gridArea: 'title', display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
        <div style={{ fontSize: tokens.font.sizeSm, fontWeight: tokens.font.weightSemibold, letterSpacing: '-0.02em', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          Meiro measurement workspace
        </div>
        <div style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.85)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          Unified attribution, incrementality, MMM, and data quality.
        </div>
      </div>

      <div style={{ gridArea: 'period', display: 'inline-flex', alignItems: 'center', justifyContent: isMobileHeader ? 'flex-start' : 'flex-end', minWidth: 0 }}>
        <div style={{ padding: '6px 10px', borderRadius: 999, backgroundColor: 'rgba(15,23,42,0.65)', border: '1px solid rgba(148,163,184,0.5)', fontSize: tokens.font.sizeXs, display: 'inline-flex', alignItems: 'center', gap: 6, minHeight: 28, width: isMobileHeader ? '100%' : 'clamp(280px, 26vw, 420px)', maxWidth: '100%' }}>
          <span style={{ opacity: 0.85, flex: '0 0 auto' }}>Period</span>
          <input
            type="date"
            value={draftDateFrom}
            onChange={(e) => setDraftDateFrom(e.target.value)}
            style={{ height: 24, borderRadius: tokens.radius.sm, border: '1px solid rgba(148,163,184,0.6)', backgroundColor: '#0b1220', color: '#e5e7eb', padding: '0 6px', fontSize: tokens.font.sizeXs, minWidth: 0 }}
          />
          <span style={{ opacity: 0.8 }}>–</span>
          <input
            type="date"
            value={draftDateTo}
            onChange={(e) => setDraftDateTo(e.target.value)}
            style={{ height: 24, borderRadius: tokens.radius.sm, border: '1px solid rgba(148,163,184,0.6)', backgroundColor: '#0b1220', color: '#e5e7eb', padding: '0 6px', fontSize: tokens.font.sizeXs, minWidth: 0 }}
          />
          <button
            type="button"
            onClick={() => onPeriodChange({ dateFrom: draftDateFrom, dateTo: draftDateTo })}
            disabled={!canApplyPeriod}
            style={{ height: 24, borderRadius: tokens.radius.sm, border: '1px solid rgba(148,163,184,0.6)', backgroundColor: canApplyPeriod ? '#1d4ed8' : '#334155', color: '#e5e7eb', padding: '0 8px', fontSize: tokens.font.sizeXs, cursor: canApplyPeriod ? 'pointer' : 'not-allowed', opacity: canApplyPeriod ? 1 : 0.7 }}
          >
            Apply
          </button>
          <span style={{ fontWeight: tokens.font.weightMedium, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: isMobileHeader ? 'none' : 'inline-block', opacity: 0.75 }}>
            {periodLabel}
          </span>
        </div>
      </div>

      <div role="toolbar" aria-label="Workspace controls" style={{ gridArea: 'controls', display: 'flex', alignItems: 'center', gap: 8, rowGap: 8, flexWrap: 'wrap', justifyContent: isMobileHeader ? 'flex-start' : 'flex-end', minWidth: 0, maxWidth: '100%' }}>
        <div style={{ padding: '6px 10px', borderRadius: 999, backgroundColor: 'rgba(15,23,42,0.65)', border: '1px solid rgba(148,163,184,0.5)', fontSize: tokens.font.sizeXs, display: 'inline-flex', alignItems: 'center', gap: 6, minHeight: 28, width: isMobileHeader ? '100%' : 'clamp(180px, 18vw, 260px)', maxWidth: '100%' }}>
          <span style={{ opacity: 0.85, flex: '0 0 auto' }}>Conversion</span>
          <span style={{ fontWeight: tokens.font.weightMedium, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'inline-block' }}>
            {conversionLabel}
          </span>
          <span style={{ opacity: 0.7, flex: '0 0 auto' }}>(read-only)</span>
        </div>

        <label style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.9)', display: 'inline-flex', alignItems: isMobileHeader ? 'stretch' : 'center', flexDirection: isMobileHeader ? 'column' : 'row', gap: isMobileHeader ? 4 : 6, minWidth: isMobileHeader ? '100%' : 0 }}>
          <span>Model</span>
          <select value={selectedModel} onChange={(e) => onModelChange(e.target.value)} style={{ padding: '6px 8px', height: 30, fontSize: tokens.font.sizeXs, borderRadius: tokens.radius.sm, border: '1px solid rgba(148,163,184,0.6)', backgroundColor: '#0b1220', color: '#e5e7eb', cursor: 'pointer', width: isMobileHeader ? '100%' : 'clamp(180px, 18vw, 220px)', minWidth: isMobileHeader ? 0 : 180, maxWidth: isMobileHeader ? '100%' : 220 }}>
            {ATTRIBUTION_MODELS.map((m) => (
              <option key={m.id} value={m.id}>
                {m.label}
              </option>
            ))}
          </select>
        </label>

        <label style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.9)', display: 'inline-flex', alignItems: isMobileHeader ? 'stretch' : 'center', flexDirection: isMobileHeader ? 'column' : 'row', gap: isMobileHeader ? 4 : 6, minWidth: isMobileHeader ? '100%' : 0 }}>
          <span>Config</span>
          {modelConfigsLoading ? (
            <span aria-hidden="true" style={{ display: 'inline-block', height: 30, width: isMobileHeader ? '100%' : 'clamp(200px, 20vw, 240px)', minWidth: isMobileHeader ? 0 : 200, borderRadius: tokens.radius.sm, backgroundColor: 'rgba(100,116,139,0.45)', border: '1px solid rgba(148,163,184,0.4)' }} />
          ) : (
            <select value={selectedConfigId ?? ''} onChange={(e) => onConfigChange(e.target.value || null)} style={{ padding: '6px 8px', height: 30, fontSize: tokens.font.sizeXs, borderRadius: tokens.radius.sm, border: '1px solid rgba(148,163,184,0.6)', backgroundColor: '#0b1220', color: '#e5e7eb', cursor: 'pointer', width: isMobileHeader ? '100%' : 'clamp(200px, 20vw, 240px)', minWidth: isMobileHeader ? 0 : 200, maxWidth: isMobileHeader ? '100%' : 240 }}>
              <option value="">Default active</option>
              {modelConfigs.filter((c) => c.status === 'active' || c.status === 'draft').map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} v{c.version} {c.status === 'active' ? '• active' : '(draft)'}
                </option>
              ))}
            </select>
          )}
        </label>

        <div style={{ padding: '6px 12px', borderRadius: 999, fontSize: tokens.font.sizeXs, fontWeight: tokens.font.weightSemibold, backgroundColor: journeysLoaded ? 'rgba(22,163,74,0.26)' : 'rgba(250,204,21,0.18)', color: journeysLoaded ? '#bbf7d0' : '#facc15', border: `1px solid ${journeysLoaded ? 'rgba(34,197,94,0.7)' : 'rgba(234,179,8,0.6)'}`, minHeight: 30, display: 'inline-flex', alignItems: 'center', minWidth: 0, width: isMobileHeader ? '100%' : 'clamp(180px, 22vw, 360px)', maxWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {journeysLoaded
            ? primaryKpiLabel
              ? `${journeyCount} journeys · ${primaryKpiLabel}: ${primaryKpiCount ?? convertedCount}`
              : `${journeyCount} journeys · ${convertedCount} converted`
            : 'No journeys loaded'}
        </div>

        {!journeysLoaded && (
          <button
            type="button"
            onClick={onLoadSample}
            disabled={loadingSample}
            style={{ padding: '6px 14px', height: 30, borderRadius: 999, fontSize: tokens.font.sizeXs, fontWeight: tokens.font.weightSemibold, backgroundColor: tokens.color.accent, color: '#ffffff', border: 'none', cursor: loadingSample ? 'wait' : 'pointer', opacity: loadingSample ? 0.7 : 1, marginLeft: 0, width: isMobileHeader ? '100%' : 'auto' }}
          >
            {loadingSample ? 'Loading sample…' : 'Load sample data'}
          </button>
        )}
        {journeysLoaded && (
          <button
            type="button"
            onClick={onRunModels}
            disabled={runningModels}
            style={{ padding: '6px 16px', height: 30, borderRadius: 999, fontSize: tokens.font.sizeXs, fontWeight: tokens.font.weightSemibold, backgroundColor: '#f97316', color: '#111827', border: 'none', cursor: runningModels ? 'wait' : 'pointer', opacity: runningModels ? 0.85 : 1, boxShadow: '0 0 0 1px rgba(15,23,42,0.4)', marginLeft: 0, width: isMobileHeader ? '100%' : 'auto' }}
          >
            {runningModels ? 'Running models…' : 'Re-run attribution models'}
          </button>
        )}
      </div>
    </header>
  )
}
