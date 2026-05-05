import { useEffect, useState, type CSSProperties, type Dispatch, type SetStateAction } from 'react'
import { tokens } from '../../theme/tokens'

export type AppPage =
  | 'overview'
  | 'alerts'
  | 'dashboard'
  | 'roles'
  | 'trust'
  | 'comparison'
  | 'paths'
  | 'campaigns'
  | 'expenses'
  | 'datasources'
  | 'meiro'
  | 'mmm'
  | 'settings'
  | 'dq'
  | 'incrementality'
  | 'path_archetypes'
  | 'analytics_journeys'
  | 'datasets'
  | 'documentation'

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
  { key: 'roles', label: 'Attribution roles', section: 'Attribution', icon: 'AR', breadcrumb: 'Attribution / Attribution roles' },
  { key: 'trust', label: 'Attribution trust', section: 'Attribution', icon: 'AT', breadcrumb: 'Attribution / Trust & reconciliation' },
  { key: 'campaigns', label: 'Campaign performance', section: 'Attribution', icon: 'CA', breadcrumb: 'Attribution / Campaign performance' },
  { key: 'comparison', label: 'Attribution models', section: 'Attribution', icon: 'AM', breadcrumb: 'Attribution / Attribution models' },
  { key: 'paths', label: 'Conversion paths', section: 'Journeys', icon: 'JP', breadcrumb: 'Journeys / Conversion paths' },
  { key: 'path_archetypes', label: 'Path archetypes', section: 'Journeys', icon: 'JA', breadcrumb: 'Journeys / Path archetypes' },
  { key: 'incrementality', label: 'Incrementality', section: 'Causal / Planning', icon: 'IC', breadcrumb: 'Causal / Incrementality' },
  { key: 'mmm', label: 'MMM', section: 'Causal / Planning', icon: 'MM', breadcrumb: 'Causal / MMM' },
  { key: 'dq', label: 'Data quality', section: 'Data & Ops', icon: 'DQ', breadcrumb: 'Data & Ops / Data quality' },
  { key: 'datasources', label: 'Data sources', section: 'Data & Ops', icon: 'DS', breadcrumb: 'Data & Ops / Data sources' },
  { key: 'meiro', label: 'Meiro', section: 'Data & Ops', icon: 'ME', breadcrumb: 'Data & Ops / Meiro' },
  { key: 'expenses', label: 'Expenses', section: 'Data & Ops', icon: 'EX', breadcrumb: 'Data & Ops / Expenses' },
  { key: 'settings', label: 'Settings', section: 'Admin', icon: 'ST', breadcrumb: 'Admin / Settings' },
]

const ATTRIBUTION_MODELS = [
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
    width: 18,
    height: 18,
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
  if (page === 'datasources' || page === 'datasets' || page === 'expenses' || page === 'meiro') {
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
  if (page === 'dashboard' || page === 'campaigns' || page === 'comparison' || page === 'roles' || page === 'trust') {
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

function MeiroLogoMark({ compact = false }: { compact?: boolean }) {
  const size = compact ? 30 : 44
  return (
    <svg width={size} height={size} viewBox="0 0 44 44" aria-hidden="true" style={{ flexShrink: 0 }}>
      <defs>
        <linearGradient id="meiro-mark-gradient" x1="8" y1="36" x2="36" y2="8" gradientUnits="userSpaceOnUse">
          <stop stopColor="#ff7a2f" />
          <stop offset="0.48" stopColor="#ff4f80" />
          <stop offset="1" stopColor="#963cf2" />
        </linearGradient>
      </defs>
      <circle cx="22" cy="22" r="18" fill="none" stroke="url(#meiro-mark-gradient)" strokeWidth="3" strokeDasharray="82 18" strokeLinecap="round" transform="rotate(-20 22 22)" />
      <circle cx="22" cy="22" r="12" fill="none" stroke="url(#meiro-mark-gradient)" strokeWidth="3" strokeDasharray="48 16" strokeLinecap="round" transform="rotate(45 22 22)" />
      <circle cx="22" cy="22" r="6" fill="none" stroke="url(#meiro-mark-gradient)" strokeWidth="3" strokeDasharray="26 12" strokeLinecap="round" transform="rotate(150 22 22)" />
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
  const filtered = visibleNavItems.filter((item) => item.label.toLowerCase().includes(navSearch.trim().toLowerCase()))

  const renderItem = (item: NavItem) => {
    const active = page === item.key
    const baseStyle: CSSProperties = {
      display: 'flex',
      alignItems: 'center',
      gap: sidebarCollapsed ? 0 : 14,
      width: '100%',
      border: 'none',
      background: active ? tokens.color.accentMuted : 'transparent',
      color: active ? tokens.color.text : tokens.color.text,
      padding: sidebarCollapsed ? '10px 0' : '12px 16px',
      borderRadius: tokens.radius.sm,
      cursor: 'pointer',
      fontSize: tokens.font.sizeBase,
      fontWeight: tokens.font.weightMedium,
      lineHeight: 1.2,
      justifyContent: sidebarCollapsed ? 'center' : 'flex-start',
      minHeight: 48,
    }

    return (
      <button type="button" key={item.key} onClick={() => onSetPage(item.key)} style={baseStyle} title={sidebarCollapsed ? item.label : undefined}>
        <span
          style={{
            width: 22,
            height: 22,
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: active ? tokens.color.accent : tokens.color.textMuted,
            flexShrink: 0,
          }}
        >
          {renderNavIcon(item.key, active)}
        </span>
        {!sidebarCollapsed && <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{item.label}</span>}
      </button>
    )
  }

  return (
    <aside
      style={{
        gridRow: 1,
        gridColumn: 1,
        background: tokens.color.surface,
        borderRight: `1px solid ${tokens.color.borderLight}`,
        display: 'flex',
        flexDirection: 'column',
        minHeight: '100vh',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: sidebarCollapsed ? 'center' : 'flex-start',
          justifyContent: sidebarCollapsed ? 'center' : 'space-between',
          gap: 12,
          padding: sidebarCollapsed ? '18px 10px' : '16px 20px 18px',
          borderBottom: `1px solid ${tokens.color.borderLight}`,
        }}
      >
        {!sidebarCollapsed && (
          <div style={{ display: 'grid', gap: 20, minWidth: 0 }}>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10 }}>
              <MeiroLogoMark />
              <div style={{ display: 'grid', lineHeight: 0.92, paddingTop: 1 }}>
                <span style={{ fontSize: 28, fontWeight: 800, color: '#191d29', letterSpacing: 0 }}>meiro</span>
                <span style={{ fontSize: 28, fontWeight: 800, color: tokens.color.accent, letterSpacing: 0 }}>mmm</span>
              </div>
            </div>
            <div style={{ display: 'grid', gap: 5, minWidth: 0 }}>
              <div style={{ fontSize: tokens.font.sizeSm, color: tokens.color.textSecondary, fontWeight: tokens.font.weightMedium }}>Meiro-internal</div>
              <div style={{ fontSize: tokens.font.sizeSm, color: tokens.color.textSecondary, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>workspace@meiro.io</div>
            </div>
          </div>
        )}
        {sidebarCollapsed && <MeiroLogoMark compact />}
        {sidebarCollapsed && (
          <button
            type="button"
            onClick={onToggleCollapse}
            style={{
              border: `1px solid ${tokens.color.borderLight}`,
              background: tokens.color.surface,
              cursor: 'pointer',
              padding: 4,
              width: 28,
              height: 28,
              borderRadius: tokens.radius.sm,
              color: tokens.color.textMuted,
              boxShadow: tokens.shadowXs,
            }}
            title="Expand navigation"
          >
            ›
          </button>
        )}
      </div>

      <nav style={{ flex: 1, overflowY: 'auto', padding: sidebarCollapsed ? '12px 8px' : '18px 10px 16px', display: 'grid', alignContent: 'start', gap: 6 }}>
        {filtered.map(renderItem)}
      </nav>

      <div style={{ borderTop: `1px solid ${tokens.color.borderLight}`, padding: sidebarCollapsed ? '10px 8px' : '12px 10px' }}>
        <button
          type="button"
          onClick={() => onSetPage('documentation')}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: sidebarCollapsed ? 'center' : 'flex-start',
            gap: 14,
            width: '100%',
            border: 'none',
            background: 'transparent',
            padding: sidebarCollapsed ? '10px 0' : '10px 16px',
            borderRadius: tokens.radius.sm,
            cursor: 'pointer',
            color: tokens.color.text,
            fontSize: tokens.font.sizeBase,
            fontWeight: tokens.font.weightMedium,
          }}
          title="Documentation"
        >
          <svg width="22" height="22" viewBox="0 0 16 16" fill="none" stroke={tokens.color.textMuted} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M2.5 3.2c1.8-.8 3.5-.6 5.5.6v9c-2-1.2-3.7-1.4-5.5-.6v-9Z" />
            <path d="M13.5 3.2c-1.8-.8-3.5-.6-5.5.6v9c2-1.2 3.7-1.4 5.5-.6v-9Z" />
          </svg>
          {!sidebarCollapsed && <span>Documentation</span>}
        </button>
      </div>
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
  currentPage: AppPage
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
  activeJourneySource: 'sample' | 'upload' | 'meiro' | 'deciengine_inapp_events' | null
  journeySourceOptions: Array<{ key: 'sample' | 'upload' | 'meiro' | 'deciengine_inapp_events'; label: string; available: boolean }>
  sourceSwitching: boolean
  onJourneySourceChange: (value: 'sample' | 'upload' | 'meiro' | 'deciengine_inapp_events') => void
}

export function AppTopBar({
  currentPage,
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
  activeJourneySource,
  journeySourceOptions,
  sourceSwitching,
  onJourneySourceChange,
}: AppTopBarProps) {
  const [draftDateFrom, setDraftDateFrom] = useState(periodDateFrom)
  const [draftDateTo, setDraftDateTo] = useState(periodDateTo)
  const [expanded, setExpanded] = useState(false)
  useEffect(() => {
    setDraftDateFrom(periodDateFrom)
    setDraftDateTo(periodDateTo)
  }, [periodDateFrom, periodDateTo])
  const canApplyPeriod =
    !!draftDateFrom &&
    !!draftDateTo &&
    draftDateFrom <= draftDateTo &&
    (draftDateFrom !== periodDateFrom || draftDateTo !== periodDateTo)
  const canRunModels = ['dashboard', 'roles', 'trust', 'comparison', 'campaigns'].includes(currentPage)
  const selectedModelLabel =
    ATTRIBUTION_MODELS.find((model) => model.id === selectedModel)?.label ?? selectedModel
  const selectedConfigLabel = selectedConfigId
    ? modelConfigs.find((config) => config.id === selectedConfigId)?.name ?? 'Selected config'
    : 'Default active'
  const sourceLabel =
    journeySourceOptions.find((src) => src.key === activeJourneySource)?.label ?? 'Source'

  const panelStyle: CSSProperties = {
    display: 'grid',
    gap: 10,
    padding: isMobileHeader ? '10px 12px' : '12px 14px',
    borderRadius: 18,
    backgroundColor: 'rgba(15,23,42,0.58)',
    border: '1px solid rgba(148,163,184,0.32)',
    boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.04)',
    minWidth: 0,
  }

  const panelLabelStyle: CSSProperties = {
    fontSize: tokens.font.sizeXs,
    fontWeight: tokens.font.weightSemibold,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    color: 'rgba(191,219,254,0.78)',
  }

  const selectStyle: CSSProperties = {
    padding: '7px 9px',
    height: 34,
    fontSize: tokens.font.sizeXs,
    borderRadius: tokens.radius.sm,
    border: '1px solid rgba(148,163,184,0.45)',
    backgroundColor: '#0b1220',
    color: '#e5e7eb',
    cursor: 'pointer',
    width: '100%',
    minWidth: 0,
  }

  const dateInputStyle: CSSProperties = {
    height: 30,
    borderRadius: tokens.radius.sm,
    border: '1px solid rgba(148,163,184,0.45)',
    backgroundColor: '#0b1220',
    color: '#e5e7eb',
    padding: '0 8px',
    fontSize: tokens.font.sizeXs,
    minWidth: 0,
    width: '100%',
  }

  return (
    <header
      style={{
        gridRow: 1,
        gridColumn: 2,
        display: 'grid',
        gap: 12,
        padding: isMobileHeader ? '10px 12px' : '10px 20px',
        borderBottom: `1px solid ${tokens.color.borderLight}`,
        background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #1d4ed8 100%)',
        color: '#e5e7eb',
        minWidth: 0,
      }}
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: isMobileHeader ? 'minmax(0,1fr)' : 'minmax(0,1fr) auto',
          alignItems: 'center',
          gap: 10,
          minWidth: 0,
        }}
      >
        <div style={{ display: 'grid', gap: 6, minWidth: 0 }}>
          <div style={{ ...panelLabelStyle, color: 'rgba(191,219,254,0.72)' }}>Workspace</div>
          <div style={{ fontSize: isMobileHeader ? tokens.font.sizeBase : tokens.font.sizeLg, fontWeight: tokens.font.weightSemibold, letterSpacing: '-0.02em', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            Meiro measurement workspace
          </div>
          <div style={{ fontSize: tokens.font.sizeSm, color: 'rgba(226,232,240,0.82)', maxWidth: 720 }}>
            Unified attribution, incrementality, MMM, and data quality.
          </div>
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 8,
              marginTop: 2,
            }}
          >
            {[periodLabel, conversionLabel, sourceLabel, selectedModelLabel, selectedConfigLabel].map((item) => (
              <span
                key={item}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  padding: '4px 10px',
                  borderRadius: 999,
                  border: '1px solid rgba(148,163,184,0.35)',
                  backgroundColor: 'rgba(11,18,32,0.54)',
                  fontSize: tokens.font.sizeXs,
                  color: 'rgba(226,232,240,0.88)',
                  maxWidth: '100%',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {item}
              </span>
            ))}
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 8, justifySelf: isMobileHeader ? 'stretch' : 'end', flexWrap: 'wrap' }}>
          {!journeysLoaded && (
            <button
              type="button"
              onClick={onLoadSample}
              disabled={loadingSample}
              style={{ padding: '8px 14px', minHeight: 34, borderRadius: 999, fontSize: tokens.font.sizeXs, fontWeight: tokens.font.weightSemibold, backgroundColor: tokens.color.accent, color: '#ffffff', border: 'none', cursor: loadingSample ? 'wait' : 'pointer', opacity: loadingSample ? 0.7 : 1 }}
            >
              {loadingSample ? 'Loading sample…' : 'Load sample data'}
            </button>
          )}
          {journeysLoaded && canRunModels && (
            <button
              type="button"
              onClick={onRunModels}
              disabled={runningModels}
              style={{ padding: '8px 16px', minHeight: 34, borderRadius: 999, fontSize: tokens.font.sizeXs, fontWeight: tokens.font.weightSemibold, backgroundColor: '#f97316', color: '#111827', border: 'none', cursor: runningModels ? 'wait' : 'pointer', opacity: runningModels ? 0.85 : 1, boxShadow: '0 0 0 1px rgba(15,23,42,0.35)' }}
            >
              {runningModels ? 'Running models…' : 'Re-run attribution models'}
            </button>
          )}
          <button
            type="button"
            onClick={() => setExpanded((current) => !current)}
            style={{
              padding: '8px 14px',
              minHeight: 34,
              borderRadius: 999,
              fontSize: tokens.font.sizeXs,
              fontWeight: tokens.font.weightSemibold,
              backgroundColor: 'rgba(11,18,32,0.66)',
              color: '#e5e7eb',
              border: '1px solid rgba(148,163,184,0.45)',
              cursor: 'pointer',
            }}
          >
            {expanded ? 'Collapse context' : 'Edit context'}
          </button>
        </div>
      </div>

      {expanded && (
        <div
          role="toolbar"
          aria-label="Workspace controls"
          style={{
            display: 'grid',
            gridTemplateColumns: isMobileHeader ? 'minmax(0,1fr)' : 'minmax(0, 1.7fr) minmax(280px, 360px)',
            gap: 12,
            minWidth: 0,
          }}
        >
          <div style={panelStyle}>
            <div style={panelLabelStyle}>Analysis context</div>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
                gap: 10,
                minWidth: 0,
              }}
            >
              <div style={{ display: 'grid', gap: 4, minWidth: 0 }}>
                <span style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.72)' }}>Conversion</span>
                <div
                  style={{
                    minHeight: 34,
                    padding: '7px 10px',
                    borderRadius: tokens.radius.sm,
                    border: '1px solid rgba(148,163,184,0.45)',
                    backgroundColor: 'rgba(11,18,32,0.92)',
                    color: '#e5e7eb',
                    fontSize: tokens.font.sizeXs,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6,
                    minWidth: 0,
                  }}
                >
                  <span style={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontWeight: tokens.font.weightMedium }}>
                    {conversionLabel}
                  </span>
                  <span style={{ color: 'rgba(226,232,240,0.55)', flexShrink: 0 }}>read-only</span>
                </div>
              </div>

              <label style={{ display: 'grid', gap: 4, minWidth: 0 }}>
                <span style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.72)' }}>Source</span>
                <select
                  value={activeJourneySource || ''}
                  onChange={(e) => onJourneySourceChange(e.target.value as 'sample' | 'upload' | 'meiro' | 'deciengine_inapp_events')}
                  disabled={sourceSwitching || !activeJourneySource}
                  style={{ ...selectStyle, cursor: sourceSwitching ? 'wait' : 'pointer' }}
                >
                  {journeySourceOptions.map((src) => (
                    <option key={src.key} value={src.key} disabled={!src.available}>
                      {src.label}{src.available ? '' : ' (n/a)'}
                    </option>
                  ))}
                </select>
              </label>

              <label style={{ display: 'grid', gap: 4, minWidth: 0 }}>
                <span style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.72)' }}>Model</span>
                <select value={selectedModel} onChange={(e) => onModelChange(e.target.value)} style={selectStyle}>
                  {ATTRIBUTION_MODELS.map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.label}
                    </option>
                  ))}
                </select>
              </label>

              <label style={{ display: 'grid', gap: 4, minWidth: 0 }}>
                <span style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.72)' }}>Config</span>
                {modelConfigsLoading ? (
                  <span
                    aria-hidden="true"
                    style={{
                      display: 'inline-block',
                      height: 34,
                      borderRadius: tokens.radius.sm,
                      backgroundColor: 'rgba(100,116,139,0.45)',
                      border: '1px solid rgba(148,163,184,0.4)',
                    }}
                  />
                ) : (
                  <select value={selectedConfigId ?? ''} onChange={(e) => onConfigChange(e.target.value || null)} style={selectStyle}>
                    <option value="">Default active</option>
                    {modelConfigs.filter((c) => c.status === 'active' || c.status === 'draft').map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.name} v{c.version} {c.status === 'active' ? '• active' : '(draft)'}
                      </option>
                    ))}
                  </select>
                )}
              </label>
            </div>
          </div>

          <div style={panelStyle}>
            <div style={panelLabelStyle}>Analysis period</div>
            <div style={{ display: 'grid', gridTemplateColumns: isMobileHeader ? '1fr' : 'minmax(0,1fr) auto minmax(0,1fr) auto', gap: 8, alignItems: 'center' }}>
              <input
                type="date"
                value={draftDateFrom}
                onChange={(e) => setDraftDateFrom(e.target.value)}
                style={dateInputStyle}
              />
              {!isMobileHeader && <span style={{ opacity: 0.8 }}>–</span>}
              <input
                type="date"
                value={draftDateTo}
                onChange={(e) => setDraftDateTo(e.target.value)}
                style={dateInputStyle}
              />
              <button
                type="button"
                onClick={() => onPeriodChange({ dateFrom: draftDateFrom, dateTo: draftDateTo })}
                disabled={!canApplyPeriod}
                style={{
                  height: 30,
                  borderRadius: tokens.radius.sm,
                  border: '1px solid rgba(148,163,184,0.45)',
                  backgroundColor: canApplyPeriod ? '#2563eb' : '#334155',
                  color: '#e5e7eb',
                  padding: '0 10px',
                  fontSize: tokens.font.sizeXs,
                  fontWeight: tokens.font.weightSemibold,
                  cursor: canApplyPeriod ? 'pointer' : 'not-allowed',
                  opacity: canApplyPeriod ? 1 : 0.7,
                }}
              >
                Apply
              </button>
            </div>
            <div
              style={{
                display: 'grid',
                gap: 6,
                padding: '8px 10px',
                borderRadius: 14,
                backgroundColor: journeysLoaded ? 'rgba(22,163,74,0.16)' : 'rgba(250,204,21,0.12)',
                border: `1px solid ${journeysLoaded ? 'rgba(34,197,94,0.4)' : 'rgba(234,179,8,0.35)'}`,
              }}
            >
              <div style={{ fontSize: tokens.font.sizeSm, fontWeight: tokens.font.weightSemibold, color: journeysLoaded ? '#bbf7d0' : '#fde68a' }}>
                {journeysLoaded ? 'Journeys loaded and ready for analysis' : 'No journeys loaded yet'}
              </div>
              <div style={{ fontSize: tokens.font.sizeXs, color: 'rgba(226,232,240,0.8)' }}>
                {journeysLoaded
                  ? primaryKpiLabel
                    ? `${journeyCount} journeys observed · ${primaryKpiLabel}: ${primaryKpiCount ?? convertedCount}`
                    : `${journeyCount} journeys observed · ${convertedCount} converted`
                  : 'Load sample data or connect a source to populate attribution, journeys, and cover metrics.'}
              </div>
            </div>
          </div>
        </div>
      )}
    </header>
  )
}
