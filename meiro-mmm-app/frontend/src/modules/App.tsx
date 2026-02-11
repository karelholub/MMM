import {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
  lazy,
  Suspense,
} from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import { WorkspaceContext, JourneysSummary } from '../components/WorkspaceContext'
import MMMWizardShell from './MMMWizardShell'
import type { SettingsPageHandle, SectionKey } from './Settings'

const Overview = lazy(() => import('./Overview'))
const ChannelPerformance = lazy(() => import('./ChannelPerformance'))
const AttributionComparison = lazy(() => import('./AttributionComparison'))
const ConversionPaths = lazy(() => import('./ConversionPaths'))
const ExpenseManager = lazy(() => import('./ExpenseManager'))
const DataSources = lazy(() => import('./DataSources'))
const DatasetUploader = lazy(() => import('./DatasetUploader'))
const CampaignPerformance = lazy(() => import('./CampaignPerformance'))
const SettingsPage = lazy(() => import('./Settings'))
const DataQuality = lazy(() => import('./DataQuality'))
const IncrementalityPage = lazy(() => import('./Incrementality'))
const PathArchetypes = lazy(() => import('./PathArchetypes'))

type Page =
  | 'overview'
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
  | 'datasets'

type NavSection =
  | 'Overview'
  | 'Attribution'
  | 'Journeys'
  | 'Causal / Planning'
  | 'Data & Ops'
  | 'Admin'

interface NavItem {
  key: Page
  label: string
  section: NavSection
  icon: string
  breadcrumb: string
}

const NAV_ITEMS: NavItem[] = [
  {
    key: 'overview',
    label: 'Overview',
    section: 'Overview',
    icon: 'OV',
    breadcrumb: 'Overview',
  },
  {
    key: 'dashboard',
    label: 'Channel performance',
    section: 'Attribution',
    icon: 'CH',
    breadcrumb: 'Attribution / Channel performance',
  },
  {
    key: 'campaigns',
    label: 'Campaign performance',
    section: 'Attribution',
    icon: 'CA',
    breadcrumb: 'Attribution / Campaign performance',
  },
  {
    key: 'comparison',
    label: 'Attribution models',
    section: 'Attribution',
    icon: 'AM',
    breadcrumb: 'Attribution / Attribution models',
  },
  {
    key: 'paths',
    label: 'Conversion paths',
    section: 'Journeys',
    icon: 'JP',
    breadcrumb: 'Journeys / Conversion paths',
  },
  {
    key: 'path_archetypes',
    label: 'Path archetypes',
    section: 'Journeys',
    icon: 'JA',
    breadcrumb: 'Journeys / Path archetypes',
  },
  {
    key: 'incrementality',
    label: 'Incrementality',
    section: 'Causal / Planning',
    icon: 'IC',
    breadcrumb: 'Causal / Incrementality',
  },
  {
    key: 'mmm',
    label: 'MMM (advanced)',
    section: 'Causal / Planning',
    icon: 'MM',
    breadcrumb: 'Causal / MMM (advanced)',
  },
  {
    key: 'dq',
    label: 'Data quality',
    section: 'Data & Ops',
    icon: 'DQ',
    breadcrumb: 'Data & Ops / Data quality',
  },
  {
    key: 'datasources',
    label: 'Data sources',
    section: 'Data & Ops',
    icon: 'DS',
    breadcrumb: 'Data & Ops / Data sources',
  },
  {
    key: 'expenses',
    label: 'Expenses',
    section: 'Data & Ops',
    icon: 'EX',
    breadcrumb: 'Data & Ops / Expenses',
  },
  {
    key: 'settings',
    label: 'Settings',
    section: 'Admin',
    icon: 'ST',
    breadcrumb: 'Admin / Settings',
  },
]

const PAGE_FALLBACK = (
  <div style={{ padding: 48, textAlign: 'center', color: '#64748b', fontSize: 14 }}>
    Loading…
  </div>
)

const ATTRIBUTION_MODELS = [
  { id: 'last_touch', label: 'Last Touch' },
  { id: 'first_touch', label: 'First Touch' },
  { id: 'linear', label: 'Linear' },
  { id: 'time_decay', label: 'Time Decay' },
  { id: 'position_based', label: 'Position Based' },
  { id: 'markov', label: 'Data-Driven (Markov)' },
]

export default function App() {
  const [page, setPage] = useState<Page>('overview')
  const [lastPage, setLastPage] = useState<Page | null>(null)
  const settingsRef = useRef<SettingsPageHandle | null>(null)
  const [settingsDirtySections, setSettingsDirtySections] = useState<SectionKey[]>([])
  const [pendingSettingsNav, setPendingSettingsNav] = useState<Page | null>(null)
  const [showSettingsGuard, setShowSettingsGuard] = useState(false)
  const [selectedModel, setSelectedModel] = useState('linear')
  const [mmmRunId, setMmmRunId] = useState<string | null>(null)
  const [mmmDatasetId, setMmmDatasetId] = useState<string | null>(null)
  const [pendingMmmMapping, setPendingMmmMapping] = useState<{ dataset_id: string; columns: { kpi: string; spend_channels: string[]; covariates?: string[] } } | null>(null)
  const [selectedConfigId, setSelectedConfigId] = useState<string | null>(null)
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => {
    if (typeof window === 'undefined') return false
    try {
      const raw = window.localStorage.getItem('mmm-sidebar-collapsed-v1')
      return raw === 'true'
    } catch {
      return false
    }
  })
  const [pinnedPages, setPinnedPages] = useState<Page[]>(() => {
    if (typeof window === 'undefined') return []
    try {
      const raw = window.localStorage.getItem('mmm-pinned-pages-v1')
      if (!raw) return []
      const parsed = JSON.parse(raw) as Page[]
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  })
  const [navSearch, setNavSearch] = useState('')

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/journeys')
      if (!res.ok) return { loaded: false, count: 0 }
      return res.json()
    },
    refetchInterval: 15 * 1000,
  })

  const modelConfigsQuery = useQuery({
    queryKey: ['model-configs'],
    queryFn: async () => {
      const res = await fetch('/api/model-configs')
      if (!res.ok) throw new Error('Failed to load model configs')
      return res.json() as Promise<
        { id: string; name: string; status: string; version: number; activated_at?: string | null }[]
      >
    },
  })

  const loadSampleMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/attribution/journeys/load-sample', { method: 'POST' })
      if (!res.ok) throw new Error('Failed to load sample')
      return res.json()
    },
    onSuccess: () => {
      journeysQuery.refetch()
      runAllMutation.mutate()
    },
  })

  const runAllMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/attribution/run-all', { method: 'POST' })
      if (!res.ok) throw new Error('Failed to run models')
      return res.json()
    },
  })

  const createMmmRunMutation = useMutation({
    mutationFn: async (config: {
      dataset_id: string
      kpi: string
      spend_channels: string[]
      covariates: string[]
      kpi_mode?: string
      use_adstock?: boolean
      use_saturation?: boolean
      holdout_weeks?: number
      random_seed?: number | null
    }) => {
      const res = await fetch('/api/models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dataset_id: config.dataset_id,
          frequency: 'W',
          kpi_mode: config.kpi_mode ?? 'conversions',
          kpi: config.kpi,
          spend_channels: config.spend_channels,
          covariates: config.covariates || [],
          use_adstock: config.use_adstock ?? true,
          use_saturation: config.use_saturation ?? true,
          holdout_weeks: config.holdout_weeks ?? 8,
          random_seed: config.random_seed ?? null,
        }),
      })
      if (!res.ok) throw new Error('Failed to start MMM run')
      return res.json()
    },
    onSuccess: (data, variables) => {
      setMmmRunId(data.run_id)
      setMmmDatasetId(variables.dataset_id)
      setPendingMmmMapping(null)
    },
  })

  const mmmRunQuery = useQuery({
    queryKey: ['mmm-run', mmmRunId],
    queryFn: async () => {
      const res = await fetch(`/api/models/${mmmRunId}`)
      if (!res.ok) throw new Error('Failed to fetch run')
      return res.json()
    },
    enabled: !!mmmRunId,
    refetchInterval: (query) => {
      const data = query.state?.data as { status?: string } | undefined
      return data?.status === 'finished' || data?.status === 'error' ? false : 2000
    },
  })

  useEffect(() => {
    if (journeysQuery.data?.loaded && !runAllMutation.data && !runAllMutation.isPending) {
      runAllMutation.mutate()
    }
  }, [journeysQuery.data?.loaded])

  const journeysLoaded = journeysQuery.data?.loaded ?? false
  const journeyCount = journeysQuery.data?.count ?? 0
  const convertedCount = journeysQuery.data?.converted ?? 0
  const primaryKpiLabel: string | undefined = journeysQuery.data?.primary_kpi_label
  const primaryKpiId: string | undefined = journeysQuery.data?.primary_kpi_id ?? undefined
  const primaryKpiCount: number | undefined = journeysQuery.data?.primary_kpi_count
  const channels = useMemo(() => journeysQuery.data?.channels ?? [], [journeysQuery.data?.channels])

  const navigateToPage = useCallback((next: Page) => {
    setLastPage((prev) => (prev === next ? prev : prev ?? 'dashboard'))
    setPage(next)
  }, [])

  const handleSetPage = useCallback(
    (next: Page) => {
      if (
        page === 'settings' &&
        next !== 'settings' &&
        settingsDirtySections.length > 0
      ) {
        setPendingSettingsNav(next)
        setShowSettingsGuard(true)
        return
      }
      navigateToPage(next)
    },
    [navigateToPage, page, settingsDirtySections.length],
  )
  const handleSetDatasources = useCallback(
    () => handleSetPage('datasources'),
    [handleSetPage],
  )

  const handleConfirmLeaveSettings = useCallback(
    async (action: 'stay' | 'discard' | 'save') => {
      if (!pendingSettingsNav) {
        setShowSettingsGuard(false)
        return
      }
      if (action === 'stay') {
        setShowSettingsGuard(false)
        setPendingSettingsNav(null)
        return
      }
      if (action === 'discard') {
        settingsRef.current?.discardSections()
        setSettingsDirtySections([])
        setShowSettingsGuard(false)
        const next = pendingSettingsNav
        setPendingSettingsNav(null)
        navigateToPage(next)
        return
      }
      if (action === 'save') {
        const success = await settingsRef.current?.saveSections()
        if (success !== false) {
          setSettingsDirtySections([])
          setShowSettingsGuard(false)
          const next = pendingSettingsNav
          setPendingSettingsNav(null)
          navigateToPage(next)
        }
      }
    },
    [navigateToPage, pendingSettingsNav, settingsRef],
  )
  const handleMmmStartOver = useCallback(() => {
    setMmmRunId(null)
    setMmmDatasetId(null)
  }, [])
  const onMmmSelectRun = useCallback((runId: string, datasetId: string) => {
    setMmmRunId(runId)
    setMmmDatasetId(datasetId)
  }, [])
  const onJourneysImported = useCallback(() => {
    void journeysQuery.refetch()
    runAllMutation.mutate()
  }, [])
  const onMmmMappingComplete = useCallback(
    (mapping: { dataset_id: string; columns: { kpi: string; spend_channels: string[]; covariates?: string[] } }) => {
      setPendingMmmMapping(mapping)
      setMmmDatasetId(mapping.dataset_id)
    },
    [],
  )
  const onMmmStartRun = useCallback(
    (config: {
      dataset_id: string
      kpi: string
      spend_channels: string[]
      covariates: string[]
      kpi_mode: string
      use_adstock: boolean
      use_saturation: boolean
      holdout_weeks: number
      random_seed: number | null
    }) => {
      createMmmRunMutation.mutate(config)
    },
    [createMmmRunMutation],
  )

  return (
    <WorkspaceContext.Provider
      value={{
        attributionModel: selectedModel,
        setAttributionModel: setSelectedModel,
        selectedConfigId,
        setSelectedConfigId,
        journeysSummary: journeysQuery.data as JourneysSummary | undefined,
        journeysLoaded,
        isRunningAttribution: runAllMutation.isPending,
        isLoadingSampleJourneys: loadSampleMutation.isPending,
        runAllAttribution: () => runAllMutation.mutate(),
        loadSampleJourneys: () => loadSampleMutation.mutate(),
      }}
    >
      <div
        style={{
          fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
          minHeight: '100vh',
          backgroundColor: tokens.color.bg,
          display: 'grid',
          gridTemplateColumns: sidebarCollapsed ? '60px 1fr' : '240px 1fr',
          gridTemplateRows: '56px auto',
        }}
      >
        {/* Sidebar */}
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
                <div
                  style={{
                    fontSize: tokens.font.sizeSm,
                    fontWeight: tokens.font.weightSemibold,
                    color: tokens.color.text,
                    letterSpacing: '-0.02em',
                  }}
                >
                  Meiro measurement
                </div>
                <div
                  style={{
                    fontSize: tokens.font.sizeXs,
                    color: tokens.color.textSecondary,
                  }}
                >
                  Workspace
                </div>
              </div>
            )}
            <button
              type="button"
              onClick={() => {
                setSidebarCollapsed((prev) => {
                  const next = !prev
                  try {
                    window.localStorage.setItem('mmm-sidebar-collapsed-v1', next ? 'true' : 'false')
                  } catch {
                    // ignore
                  }
                  return next
                })
              }}
              style={{
                border: 'none',
                background: 'transparent',
                cursor: 'pointer',
                padding: 6,
                borderRadius: tokens.radius.full,
                color: tokens.color.textSecondary,
              }}
              title={sidebarCollapsed ? 'Expand navigation' : 'Collapse navigation'}
            >
              {sidebarCollapsed ? '›' : '‹'}
            </button>
          </div>

          {/* Nav search + pinned */}
          {!sidebarCollapsed && (
            <div
              style={{
                padding: '10px 12px 4px',
                borderBottom: `1px solid ${tokens.color.borderLight}`,
              }}
            >
              <input
                type="text"
                value={navSearch}
                onChange={(e) => setNavSearch(e.target.value)}
                placeholder="Search pages…"
                style={{
                  width: '100%',
                  padding: '6px 10px',
                  fontSize: tokens.font.sizeSm,
                  borderRadius: tokens.radius.sm,
                  border: `1px solid ${tokens.color.borderLight}`,
                  backgroundColor: tokens.color.bg,
                  color: tokens.color.text,
                }}
              />
            </div>
          )}

          <nav
            style={{
              flex: 1,
              overflowY: 'auto',
              padding: sidebarCollapsed ? '8px 4px' : '8px 8px 16px',
            }}
          >
            {(() => {
              const filtered = NAV_ITEMS.filter((item) =>
                item.label.toLowerCase().includes(navSearch.trim().toLowerCase()),
              )
              const pinned = filtered.filter((item) => pinnedPages.includes(item.key))
              const bySection: Record<NavSection, NavItem[]> = {
                Overview: [],
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
                const baseStyle: React.CSSProperties = {
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
                    <button
                      type="button"
                      onClick={() => handleSetPage(item.key)}
                      style={baseStyle}
                      title={sidebarCollapsed ? item.label : undefined}
                    >
                      <div
                        style={{
                          width: 24,
                          height: 24,
                          borderRadius: 999,
                          border: `1px solid ${active ? tokens.color.accent : tokens.color.borderLight}`,
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          fontSize: tokens.font.sizeXs,
                          color: active ? tokens.color.accent : tokens.color.textMuted,
                          backgroundColor: tokens.color.surface,
                          flexShrink: 0,
                        }}
                      >
                        {item.icon}
                      </div>
                      {!sidebarCollapsed && <span>{item.label}</span>}
                    </button>
                    {!sidebarCollapsed && (
                      <button
                        type="button"
                        onClick={() => {
                          setPinnedPages((prev) => {
                            const next = isPinned ? prev.filter((p) => p !== item.key) : [...prev, item.key]
                            try {
                              window.localStorage.setItem('mmm-pinned-pages-v1', JSON.stringify(next))
                            } catch {
                              // ignore
                            }
                            return next
                          })
                        }}
                        style={{
                          border: 'none',
                          background: 'transparent',
                          cursor: 'pointer',
                          padding: 2,
                          fontSize: tokens.font.sizeXs,
                          color: isPinned ? tokens.color.accent : tokens.color.textMuted,
                        }}
                        title={isPinned ? 'Unpin from favorites' : 'Pin to favorites'}
                      >
                        {isPinned ? '★' : '☆'}
                      </button>
                    )}
                  </div>
                )
              }

              const sectionsInOrder: NavSection[] = [
                'Overview',
                'Attribution',
                'Journeys',
                'Causal / Planning',
                'Data & Ops',
                'Admin',
              ]

              return (
                <>
                  {pinned.length > 0 && !sidebarCollapsed && (
                    <div style={{ marginBottom: 12 }}>
                      <div
                        style={{
                          fontSize: tokens.font.sizeXs,
                          textTransform: 'uppercase',
                          letterSpacing: '0.08em',
                          color: tokens.color.textMuted,
                          margin: '4px 8px',
                        }}
                      >
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
                          <div
                            style={{
                              fontSize: tokens.font.sizeXs,
                              textTransform: 'uppercase',
                              letterSpacing: '0.08em',
                              color: tokens.color.textMuted,
                              margin: '6px 8px',
                            }}
                          >
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

        {/* Top app bar */}
        <header
          style={{
            gridRow: 1,
            gridColumn: 2,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '8px 20px',
            borderBottom: `1px solid ${tokens.color.borderLight}`,
            background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #1d4ed8 100%)',
            color: '#e5e7eb',
            gap: 12,
          }}
        >
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
            }}
          >
            <div
              style={{
                fontSize: tokens.font.sizeSm,
                fontWeight: tokens.font.weightSemibold,
                letterSpacing: '-0.02em',
              }}
            >
              Meiro measurement workspace
            </div>
            <div
              style={{
                fontSize: tokens.font.sizeXs,
                color: 'rgba(226,232,240,0.85)',
              }}
            >
              Unified attribution, incrementality, MMM, and data quality.
            </div>
          </div>

          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 10,
              flexWrap: 'wrap',
              justifyContent: 'flex-end',
              minWidth: 0,
            }}
          >
            {/* Workspace context: period & conversion (read-only) */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                flexWrap: 'wrap',
              }}
            >
              <div
                style={{
                  padding: '4px 10px',
                  borderRadius: 999,
                  backgroundColor: 'rgba(15,23,42,0.65)',
                  border: '1px solid rgba(148,163,184,0.5)',
                  fontSize: tokens.font.sizeXs,
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  whiteSpace: 'nowrap',
                }}
              >
                <span style={{ opacity: 0.85 }}>Period</span>
                <span style={{ fontWeight: tokens.font.weightMedium }}>
                  {journeysLoaded && journeysQuery.data?.date_min && journeysQuery.data?.date_max
                    ? `${journeysQuery.data.date_min.slice(0, 10)} – ${journeysQuery.data.date_max.slice(0, 10)}`
                    : 'Derived from journeys'}
                </span>
              </div>
              <div
                style={{
                  padding: '4px 10px',
                  borderRadius: 999,
                  backgroundColor: 'rgba(15,23,42,0.65)',
                  border: '1px solid rgba(148,163,184,0.5)',
                  fontSize: tokens.font.sizeXs,
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  whiteSpace: 'nowrap',
                }}
              >
                <span style={{ opacity: 0.85 }}>Conversion</span>
                <span style={{ fontWeight: tokens.font.weightMedium }}>
                  {primaryKpiLabel || primaryKpiId || 'Primary KPI'}
                </span>
                <span style={{ opacity: 0.7 }}>(read‑only)</span>
              </div>
            </div>

            {/* Model + config selectors */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                flexWrap: 'wrap',
              }}
            >
              <label
                style={{
                  fontSize: tokens.font.sizeXs,
                  color: 'rgba(226,232,240,0.9)',
                }}
              >
                Model
              </label>
              <select
                value={selectedModel}
                onChange={(e) => setSelectedModel(e.target.value)}
                style={{
                  padding: '4px 8px',
                  fontSize: tokens.font.sizeXs,
                  borderRadius: tokens.radius.sm,
                  border: '1px solid rgba(148,163,184,0.6)',
                  backgroundColor: '#0b1220',
                  color: '#e5e7eb',
                  cursor: 'pointer',
                }}
              >
                {ATTRIBUTION_MODELS.map((m) => (
                  <option key={m.id} value={m.id}>
                    {m.label}
                  </option>
                ))}
              </select>
              <label
                style={{
                  fontSize: tokens.font.sizeXs,
                  color: 'rgba(226,232,240,0.9)',
                }}
              >
                Config
              </label>
              <select
                value={selectedConfigId ?? ''}
                onChange={(e) => setSelectedConfigId(e.target.value || null)}
                style={{
                  padding: '4px 8px',
                  fontSize: tokens.font.sizeXs,
                  borderRadius: tokens.radius.sm,
                  border: '1px solid rgba(148,163,184,0.6)',
                  backgroundColor: '#0b1220',
                  color: '#e5e7eb',
                  cursor: 'pointer',
                  maxWidth: 220,
                }}
              >
                <option value="">Default active</option>
                {(modelConfigsQuery.data ?? [])
                  .filter((c) => c.status === 'active' || c.status === 'draft')
                  .map((c) => (
                    <option key={c.id} value={c.id}>
                      {c.name} v{c.version} {c.status === 'active' ? '• active' : '(draft)'}
                    </option>
                  ))}
              </select>
            </div>

            {/* Journeys badge + CTA */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                flexWrap: 'wrap',
              }}
            >
              <div
                style={{
                  padding: '6px 12px',
                  borderRadius: 999,
                  fontSize: tokens.font.sizeXs,
                  fontWeight: tokens.font.weightSemibold,
                  backgroundColor: journeysLoaded ? 'rgba(22,163,74,0.26)' : 'rgba(250,204,21,0.18)',
                  color: journeysLoaded ? '#bbf7d0' : '#facc15',
                  border: `1px solid ${journeysLoaded ? 'rgba(34,197,94,0.7)' : 'rgba(234,179,8,0.6)'}`,
                  whiteSpace: 'nowrap',
                }}
              >
                {journeysLoaded
                  ? primaryKpiLabel
                    ? `${journeyCount} journeys · ${primaryKpiLabel}: ${primaryKpiCount ?? convertedCount}`
                    : `${journeyCount} journeys · ${convertedCount} converted`
                  : 'No journeys loaded'}
              </div>
              {!journeysLoaded && (
                <button
                  type="button"
                  onClick={() => loadSampleMutation.mutate()}
                  disabled={loadSampleMutation.isPending}
                  style={{
                    padding: '6px 14px',
                    borderRadius: 999,
                    fontSize: tokens.font.sizeXs,
                    fontWeight: tokens.font.weightSemibold,
                    backgroundColor: tokens.color.accent,
                    color: '#ffffff',
                    border: 'none',
                    cursor: loadSampleMutation.isPending ? 'wait' : 'pointer',
                    opacity: loadSampleMutation.isPending ? 0.7 : 1,
                  }}
                >
                  {loadSampleMutation.isPending ? 'Loading sample…' : 'Load sample data'}
                </button>
              )}
              {journeysLoaded && (
                <button
                  type="button"
                  onClick={() => runAllMutation.mutate()}
                  disabled={runAllMutation.isPending}
                  style={{
                    padding: '6px 16px',
                    borderRadius: 999,
                    fontSize: tokens.font.sizeXs,
                    fontWeight: tokens.font.weightSemibold,
                    backgroundColor: '#f97316',
                    color: '#111827',
                    border: 'none',
                    cursor: runAllMutation.isPending ? 'wait' : 'pointer',
                    opacity: runAllMutation.isPending ? 0.85 : 1,
                    boxShadow: '0 0 0 1px rgba(15,23,42,0.4)',
                  }}
                >
                  {runAllMutation.isPending ? 'Running models…' : 'Re-run attribution models'}
                </button>
              )}
            </div>
          </div>
        </header>

        {/* Main content with page-level header + breadcrumbs */}
        <main
          style={{
            gridRow: 2,
            gridColumn: 2,
            padding: '20px 24px 28px',
            overflow: 'auto',
          }}
        >
          <div
            style={{
              maxWidth: 1400,
              margin: '0 auto',
            }}
          >
            {/* Breadcrumb / page meta */}
            <div
              style={{
                marginBottom: 12,
                fontSize: tokens.font.sizeXs,
                color: tokens.color.textMuted,
              }}
            >
              {NAV_ITEMS.find((n) => n.key === page)?.breadcrumb}
            </div>

            <Suspense fallback={PAGE_FALLBACK}>
              {page === 'overview' && (
                <Overview
                  lastPage={lastPage}
                  onNavigate={(p) => handleSetPage(p)}
                  onConnectDataSources={handleSetDatasources}
                />
              )}
              {page === 'dashboard' && (
                <ChannelPerformance
                  model={selectedModel}
                  channels={channels}
                  modelsReady={!!runAllMutation.data}
                  configId={selectedConfigId}
                />
              )}
              {page === 'campaigns' && (
                <CampaignPerformance
                  model={selectedModel}
                  modelsReady={!!runAllMutation.data}
                  configId={selectedConfigId}
                />
              )}
              {page === 'comparison' && (
                <AttributionComparison selectedModel={selectedModel} onSelectModel={setSelectedModel} />
              )}
              {page === 'paths' && <ConversionPaths />}
              {page === 'path_archetypes' && <PathArchetypes />}
              {page === 'expenses' && <ExpenseManager />}
              {page === 'datasources' && <DataSources onJourneysImported={onJourneysImported} />}
              {page === 'settings' && (
                <SettingsPage
                  ref={settingsRef}
                  onDirtySectionsChange={setSettingsDirtySections}
                />
              )}
              {page === 'dq' && <DataQuality />}
              {page === 'incrementality' && <IncrementalityPage />}
              {page === 'mmm' && (
                <MMMWizardShell
                  primaryKpiLabel={primaryKpiLabel}
                  currencyCode={undefined}
                  onOpenDataQuality={() => handleSetPage('dq')}
                  mmmRunId={mmmRunId}
                  mmmDatasetId={mmmDatasetId}
                  pendingMmmMapping={pendingMmmMapping}
                  mmmRunQuery={mmmRunQuery}
                  createMmmRunMutation={createMmmRunMutation}
                  onMmmMappingComplete={onMmmMappingComplete}
                  onMmmStartRun={onMmmStartRun}
                  onMmmSelectRun={onMmmSelectRun}
                  onStartOver={handleMmmStartOver}
                />
              )}
            </Suspense>
          </div>
        </main>
      </div>

      {showSettingsGuard && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(15, 23, 42, 0.45)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 30,
            padding: 24,
          }}
        >
          <div
            style={{
              width: 'min(480px, 100%)',
              background: tokens.color.surface,
              borderRadius: tokens.radius.lg,
              border: `1px solid ${tokens.color.border}`,
              boxShadow: tokens.shadowLg,
              padding: 24,
              display: 'grid',
              gap: 16,
            }}
          >
            <div>
              <h3
                style={{
                  margin: 0,
                  fontSize: tokens.font.sizeLg,
                  fontWeight: tokens.font.weightSemibold,
                  color: tokens.color.text,
                }}
              >
                Unsaved changes in Settings
              </h3>
              <p
                style={{
                  margin: '8px 0 0',
                  fontSize: tokens.font.sizeSm,
                  color: tokens.color.textSecondary,
                }}
              >
                Save or discard your pending edits before leaving the Settings area.
              </p>
            </div>
            <div
              style={{
                display: 'flex',
                justifyContent: 'flex-end',
                gap: 12,
                flexWrap: 'wrap',
              }}
            >
              <button
                type="button"
                onClick={() => handleConfirmLeaveSettings('stay')}
                style={{
                  padding: '8px 14px',
                  borderRadius: tokens.radius.sm,
                  border: `1px solid ${tokens.color.border}`,
                  background: 'transparent',
                  color: tokens.color.text,
                  fontSize: tokens.font.sizeSm,
                  cursor: 'pointer',
                }}
              >
                Stay
              </button>
              <button
                type="button"
                onClick={() => handleConfirmLeaveSettings('discard')}
                style={{
                  padding: '8px 14px',
                  borderRadius: tokens.radius.sm,
                  border: 'none',
                  background: tokens.color.borderLight,
                  color: tokens.color.text,
                  fontSize: tokens.font.sizeSm,
                  cursor: 'pointer',
                }}
              >
                Discard
              </button>
              <button
                type="button"
                onClick={() => void handleConfirmLeaveSettings('save')}
                style={{
                  padding: '8px 18px',
                  borderRadius: tokens.radius.sm,
                  border: 'none',
                  background: tokens.color.accent,
                  color: tokens.color.surface,
                  fontSize: tokens.font.sizeSm,
                  fontWeight: tokens.font.weightSemibold,
                  cursor: 'pointer',
                }}
              >
                Save & leave
              </button>
            </div>
          </div>
        </div>
      )}
    </WorkspaceContext.Provider>
  )
}
