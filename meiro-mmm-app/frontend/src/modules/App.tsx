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
import { usePermissions } from '../hooks/usePermissions'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import ProtectedPage from '../components/ProtectedPage'
import {
  canAccessPage as canAccessAppPage,
  canManageJourneyDefinitions,
} from '../lib/accessControl'

const Overview = lazy(() => import('./Overview'))
const AlertsPage = lazy(() => import('./Alerts'))
const ChannelPerformance = lazy(() => import('./ChannelPerformance'))
const AttributionComparison = lazy(() => import('./AttributionComparison'))
const ConversionPaths = lazy(() => import('./ConversionPaths'))
const ExpenseManager = lazy(() => import('./ExpenseManager'))
const DataSources = lazy(() => import('./DataSources'))
const CampaignPerformance = lazy(() => import('./CampaignPerformance'))
const SettingsPage = lazy(() => import('./Settings'))
const DataQuality = lazy(() => import('./DataQuality'))
const IncrementalityPage = lazy(() => import('./Incrementality'))
const PathArchetypes = lazy(() => import('./PathArchetypes'))
const JourneysPage = lazy(() => import('./Journeys'))

type Page =
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

type NavSection =
  | 'Overview'
  | 'Analytics'
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
    key: 'analytics_journeys',
    label: 'Journeys',
    section: 'Analytics',
    icon: 'JR',
    breadcrumb: 'Analytics / Journeys',
  },
  {
    key: 'alerts',
    label: 'Alerts',
    section: 'Data & Ops',
    icon: 'AL',
    breadcrumb: 'Data & Ops / Alerts',
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

function renderNavIcon(page: Page, active: boolean) {
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

interface FeatureFlags {
  journeys_enabled: boolean
  journey_examples_enabled: boolean
  funnel_builder_enabled: boolean
  funnel_diagnostics_enabled: boolean
  access_control_enabled: boolean
  custom_roles_enabled: boolean
  audit_log_enabled: boolean
  scim_enabled: boolean
  sso_enabled: boolean
}

interface AppSettings {
  feature_flags?: Partial<FeatureFlags>
}

const DEFAULT_FEATURE_FLAGS: FeatureFlags = {
  journeys_enabled: false,
  journey_examples_enabled: false,
  funnel_builder_enabled: false,
  funnel_diagnostics_enabled: false,
  access_control_enabled: false,
  custom_roles_enabled: false,
  audit_log_enabled: false,
  scim_enabled: false,
  sso_enabled: false,
}

function parseInitialPage(pathname: string): Page {
  if (pathname === '/analytics/journeys') return 'analytics_journeys'
  return 'overview'
}

function pageToPathname(page: Page): string {
  if (page === 'analytics_journeys') return '/analytics/journeys'
  return '/'
}

export default function App() {
  const [page, setPage] = useState<Page>(() => {
    if (typeof window === 'undefined') return 'overview'
    return parseInitialPage(window.location.pathname)
  })
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
  const [viewportWidth, setViewportWidth] = useState<number>(() => {
    if (typeof window === 'undefined') return 1440
    return window.innerWidth
  })
  const [navSearch, setNavSearch] = useState('')
  const permissions = usePermissions()

  const settingsQuery = useQuery<AppSettings>({
    queryKey: ['app-settings-feature-flags'],
    queryFn: async () => {
      try {
        return await apiGetJson<AppSettings>('/api/settings', { fallbackMessage: 'Failed to load app settings' })
      } catch {
        return { feature_flags: DEFAULT_FEATURE_FLAGS }
      }
    },
    staleTime: 30 * 1000,
  })
  const featureFlags = useMemo<FeatureFlags>(() => {
    const incoming = settingsQuery.data?.feature_flags ?? {}
    return {
      journeys_enabled: incoming.journeys_enabled ?? DEFAULT_FEATURE_FLAGS.journeys_enabled,
      journey_examples_enabled:
        incoming.journey_examples_enabled ?? DEFAULT_FEATURE_FLAGS.journey_examples_enabled,
      funnel_builder_enabled:
        incoming.funnel_builder_enabled ?? DEFAULT_FEATURE_FLAGS.funnel_builder_enabled,
      funnel_diagnostics_enabled:
        incoming.funnel_diagnostics_enabled ?? DEFAULT_FEATURE_FLAGS.funnel_diagnostics_enabled,
      access_control_enabled:
        incoming.access_control_enabled ?? DEFAULT_FEATURE_FLAGS.access_control_enabled,
      custom_roles_enabled:
        incoming.custom_roles_enabled ?? DEFAULT_FEATURE_FLAGS.custom_roles_enabled,
      audit_log_enabled:
        incoming.audit_log_enabled ?? DEFAULT_FEATURE_FLAGS.audit_log_enabled,
      scim_enabled: incoming.scim_enabled ?? DEFAULT_FEATURE_FLAGS.scim_enabled,
      sso_enabled: incoming.sso_enabled ?? DEFAULT_FEATURE_FLAGS.sso_enabled,
    }
  }, [settingsQuery.data?.feature_flags])
  const rbacEnabled = featureFlags.access_control_enabled
  const hasPermission = useCallback(
    (key: string) => (!rbacEnabled ? true : permissions.hasPermission(key)),
    [permissions, rbacEnabled],
  )
  const hasAnyPermission = useCallback(
    (keys: string[]) => (!rbacEnabled ? true : permissions.hasAnyPermission(keys)),
    [permissions, rbacEnabled],
  )
  const hasJourneysPermission = hasAnyPermission(['journeys.view', 'journeys.manage'])
  const canManageJourneys = canManageJourneyDefinitions({
    rbacEnabled,
    hasPermission,
    hasAnyPermission,
  })
  const canAccessPage = useCallback(
    (nextPage: Page): boolean => {
      return canAccessAppPage(nextPage, {
        rbacEnabled,
        hasPermission,
        hasAnyPermission,
      })
    },
    [hasAnyPermission, hasPermission, rbacEnabled],
  )

  const journeysQuery = useQuery({
    queryKey: ['journeys-summary'],
    queryFn: async () => {
      try {
        return await apiGetJson<any>('/api/attribution/journeys', { fallbackMessage: 'Failed to load journeys summary' })
      } catch {
        return { loaded: false, count: 0 }
      }
    },
    refetchInterval: 15 * 1000,
  })

  const modelConfigsQuery = useQuery({
    queryKey: ['model-configs'],
    queryFn: async () => {
      return apiGetJson<
        { id: string; name: string; status: string; version: number; activated_at?: string | null }[]
      >('/api/model-configs', { fallbackMessage: 'Failed to load model configs' })
    },
  })

  const loadSampleMutation = useMutation({
    mutationFn: async () => apiSendJson<any>('/api/attribution/journeys/load-sample', 'POST', {}, {
      fallbackMessage: 'Failed to load sample',
    }),
    onSuccess: () => {
      journeysQuery.refetch()
      runAllMutation.mutate()
    },
  })

  const runAllMutation = useMutation({
    mutationFn: async () => apiSendJson<any>('/api/attribution/run-all', 'POST', undefined, {
      fallbackMessage: 'Failed to run models',
    }),
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
      return apiSendJson<{ run_id: string }>('/api/models', 'POST', {
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
      }, {
        fallbackMessage: 'Failed to start MMM run',
      })
    },
    onSuccess: (data, variables) => {
      setMmmRunId(data.run_id)
      setMmmDatasetId(variables.dataset_id)
      setPendingMmmMapping(null)
    },
  })

  const mmmRunQuery = useQuery({
    queryKey: ['mmm-run', mmmRunId],
    queryFn: async () => apiGetJson<any>(`/api/models/${mmmRunId}`, { fallbackMessage: 'Failed to fetch run' }),
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

  useEffect(() => {
    if (typeof window === 'undefined') return
    const onPopState = () => {
      setPage(parseInitialPage(window.location.pathname))
    }
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const targetPath = pageToPathname(page)
    if (page === 'analytics_journeys' && window.location.pathname !== targetPath) {
      window.history.pushState({}, '', targetPath)
      return
    }
    if (page !== 'analytics_journeys' && window.location.pathname === '/analytics/journeys') {
      const suffix = `${window.location.search}${window.location.hash}`
      window.history.pushState({}, '', `/${suffix}`)
    }
  }, [page])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const onResize = () => setViewportWidth(window.innerWidth)
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  const journeysLoaded = journeysQuery.data?.loaded ?? false
  const journeyCount = journeysQuery.data?.count ?? 0
  const convertedCount = journeysQuery.data?.converted ?? 0
  const primaryKpiLabel: string | undefined = journeysQuery.data?.primary_kpi_label
  const primaryKpiId: string | undefined = journeysQuery.data?.primary_kpi_id ?? undefined
  const primaryKpiCount: number | undefined = journeysQuery.data?.primary_kpi_count
  const channels = useMemo(() => journeysQuery.data?.channels ?? [], [journeysQuery.data?.channels])
  const isMobileHeader = viewportWidth < 768
  const periodReady = !!(journeysLoaded && journeysQuery.data?.date_min && journeysQuery.data?.date_max)
  const periodLabel = periodReady
    ? `${journeysQuery.data?.date_min?.slice(0, 10)} – ${journeysQuery.data?.date_max?.slice(0, 10)}`
    : 'Loading period…'
  const conversionLabel = primaryKpiLabel || primaryKpiId || (journeysQuery.isLoading ? 'Loading conversion…' : 'Primary KPI')
  const visibleNavItems = useMemo(
    () =>
      NAV_ITEMS.filter(
        (item) =>
          (item.key !== 'analytics_journeys' || featureFlags.journeys_enabled) &&
          canAccessPage(item.key),
      ),
    [canAccessPage, featureFlags.journeys_enabled],
  )

  const navigateToPage = useCallback((next: Page) => {
    setLastPage((prev) => (prev === next ? prev : prev ?? 'dashboard'))
    setPage(next)
  }, [])

  const handleSetPage = useCallback(
    (next: Page) => {
      if (!canAccessPage(next)) return
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
    [canAccessPage, navigateToPage, page, settingsDirtySections.length],
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
  const pageBlockedByFeature = page === 'analytics_journeys' && !featureFlags.journeys_enabled
  const pageBlockedByPermissions = !canAccessPage(page)
  const showNoAccess = !permissions.isLoading && (pageBlockedByFeature || pageBlockedByPermissions)
  const blockedReason = pageBlockedByFeature
    ? 'Journeys feature is disabled for this workspace.'
    : 'Your role does not have permission to view this page.'

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
              const filtered = visibleNavItems.filter((item) =>
                item.label.toLowerCase().includes(navSearch.trim().toLowerCase()),
              )
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
                        {renderNavIcon(item.key, active)}
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
                'Analytics',
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
            display: 'grid',
            gridTemplateAreas: isMobileHeader
              ? '"title" "period" "controls"'
              : '"title period" "controls controls"',
            gridTemplateColumns: isMobileHeader
              ? 'minmax(0,1fr)'
              : 'minmax(0,1fr) minmax(220px,340px)',
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
          <div
            style={{
              gridArea: 'title',
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
              minWidth: 0,
            }}
          >
            <div
              style={{
                fontSize: tokens.font.sizeSm,
                fontWeight: tokens.font.weightSemibold,
                letterSpacing: '-0.02em',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              Meiro measurement workspace
            </div>
            <div
              style={{
                fontSize: tokens.font.sizeXs,
                color: 'rgba(226,232,240,0.85)',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              Unified attribution, incrementality, MMM, and data quality.
            </div>
          </div>

          <div
            style={{
              gridArea: 'period',
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: isMobileHeader ? 'flex-start' : 'flex-end',
              minWidth: 0,
            }}
          >
            <div
              style={{
                padding: '6px 10px',
                borderRadius: 999,
                backgroundColor: 'rgba(15,23,42,0.65)',
                border: '1px solid rgba(148,163,184,0.5)',
                fontSize: tokens.font.sizeXs,
                display: 'inline-flex',
                alignItems: 'center',
                gap: 6,
                minHeight: 28,
                width: isMobileHeader ? '100%' : 'clamp(180px, 20vw, 320px)',
                maxWidth: '100%',
              }}
            >
              <span style={{ opacity: 0.85, flex: '0 0 auto' }}>Period</span>
              <span
                style={{
                  fontWeight: tokens.font.weightMedium,
                  minWidth: 0,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  display: 'inline-block',
                }}
              >
                {periodLabel}
              </span>
            </div>
          </div>

          <div
            role="toolbar"
            aria-label="Workspace controls"
            style={{
              gridArea: 'controls',
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              rowGap: 8,
              flexWrap: 'wrap',
              justifyContent: isMobileHeader ? 'flex-start' : 'flex-end',
              minWidth: 0,
              maxWidth: '100%',
            }}
          >
              <div
                style={{
                  padding: '6px 10px',
                  borderRadius: 999,
                  backgroundColor: 'rgba(15,23,42,0.65)',
                  border: '1px solid rgba(148,163,184,0.5)',
                  fontSize: tokens.font.sizeXs,
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  minHeight: 28,
                  width: isMobileHeader ? '100%' : 'clamp(180px, 18vw, 260px)',
                  maxWidth: '100%',
                }}
              >
                <span style={{ opacity: 0.85, flex: '0 0 auto' }}>Conversion</span>
                <span
                  style={{
                    fontWeight: tokens.font.weightMedium,
                    minWidth: 0,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    display: 'inline-block',
                  }}
                >
                  {conversionLabel}
                </span>
                <span style={{ opacity: 0.7, flex: '0 0 auto' }}>(read‑only)</span>
              </div>

              <label
                style={{
                  fontSize: tokens.font.sizeXs,
                  color: 'rgba(226,232,240,0.9)',
                  display: 'inline-flex',
                  alignItems: isMobileHeader ? 'stretch' : 'center',
                  flexDirection: isMobileHeader ? 'column' : 'row',
                  gap: isMobileHeader ? 4 : 6,
                  minWidth: isMobileHeader ? '100%' : 0,
                }}
              >
                <span>Model</span>
                <select
                  value={selectedModel}
                  onChange={(e) => setSelectedModel(e.target.value)}
                  style={{
                    padding: '6px 8px',
                    height: 30,
                    fontSize: tokens.font.sizeXs,
                    borderRadius: tokens.radius.sm,
                    border: '1px solid rgba(148,163,184,0.6)',
                    backgroundColor: '#0b1220',
                    color: '#e5e7eb',
                    cursor: 'pointer',
                    width: isMobileHeader ? '100%' : 'clamp(180px, 18vw, 220px)',
                    minWidth: isMobileHeader ? 0 : 180,
                    maxWidth: isMobileHeader ? '100%' : 220,
                  }}
                >
                  {ATTRIBUTION_MODELS.map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                style={{
                  fontSize: tokens.font.sizeXs,
                  color: 'rgba(226,232,240,0.9)',
                  display: 'inline-flex',
                  alignItems: isMobileHeader ? 'stretch' : 'center',
                  flexDirection: isMobileHeader ? 'column' : 'row',
                  gap: isMobileHeader ? 4 : 6,
                  minWidth: isMobileHeader ? '100%' : 0,
                }}
              >
                <span>Config</span>
                {modelConfigsQuery.isLoading ? (
                  <span
                    aria-hidden="true"
                    style={{
                      display: 'inline-block',
                      height: 30,
                      width: isMobileHeader ? '100%' : 'clamp(200px, 20vw, 240px)',
                      minWidth: isMobileHeader ? 0 : 200,
                      borderRadius: tokens.radius.sm,
                      backgroundColor: 'rgba(100,116,139,0.45)',
                      border: '1px solid rgba(148,163,184,0.4)',
                    }}
                  />
                ) : (
                  <select
                    value={selectedConfigId ?? ''}
                    onChange={(e) => setSelectedConfigId(e.target.value || null)}
                    style={{
                      padding: '6px 8px',
                      height: 30,
                      fontSize: tokens.font.sizeXs,
                      borderRadius: tokens.radius.sm,
                      border: '1px solid rgba(148,163,184,0.6)',
                      backgroundColor: '#0b1220',
                      color: '#e5e7eb',
                      cursor: 'pointer',
                      width: isMobileHeader ? '100%' : 'clamp(200px, 20vw, 240px)',
                      minWidth: isMobileHeader ? 0 : 200,
                      maxWidth: isMobileHeader ? '100%' : 240,
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
                )}
              </label>

              <div
                style={{
                  padding: '6px 12px',
                  borderRadius: 999,
                  fontSize: tokens.font.sizeXs,
                  fontWeight: tokens.font.weightSemibold,
                  backgroundColor: journeysLoaded ? 'rgba(22,163,74,0.26)' : 'rgba(250,204,21,0.18)',
                  color: journeysLoaded ? '#bbf7d0' : '#facc15',
                  border: `1px solid ${journeysLoaded ? 'rgba(34,197,94,0.7)' : 'rgba(234,179,8,0.6)'}`,
                  minHeight: 30,
                  display: 'inline-flex',
                  alignItems: 'center',
                  minWidth: 0,
                  width: isMobileHeader ? '100%' : 'clamp(180px, 22vw, 360px)',
                  maxWidth: '100%',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
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
                    height: 30,
                    borderRadius: 999,
                    fontSize: tokens.font.sizeXs,
                    fontWeight: tokens.font.weightSemibold,
                    backgroundColor: tokens.color.accent,
                    color: '#ffffff',
                    border: 'none',
                    cursor: loadSampleMutation.isPending ? 'wait' : 'pointer',
                    opacity: loadSampleMutation.isPending ? 0.7 : 1,
                    marginLeft: 0,
                    width: isMobileHeader ? '100%' : 'auto',
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
                    height: 30,
                    borderRadius: 999,
                    fontSize: tokens.font.sizeXs,
                    fontWeight: tokens.font.weightSemibold,
                    backgroundColor: '#f97316',
                    color: '#111827',
                    border: 'none',
                    cursor: runAllMutation.isPending ? 'wait' : 'pointer',
                    opacity: runAllMutation.isPending ? 0.85 : 1,
                    boxShadow: '0 0 0 1px rgba(15,23,42,0.4)',
                    marginLeft: 0,
                    width: isMobileHeader ? '100%' : 'auto',
                  }}
                >
                  {runAllMutation.isPending ? 'Running models…' : 'Re-run attribution models'}
                </button>
              )}
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
              {NAV_ITEMS.find((n) => n.key === page)?.breadcrumb ?? ''}
            </div>

            <Suspense fallback={PAGE_FALLBACK}>
              <ProtectedPage blocked={showNoAccess} reason={blockedReason}>
                <>
              {page === 'overview' && (
                <Overview
                  lastPage={lastPage === 'analytics_journeys' ? 'overview' : lastPage}
                  onNavigate={(p) => handleSetPage(p)}
                  onConnectDataSources={handleSetDatasources}
                  canCreateAlerts={hasPermission('alerts.manage')}
                />
              )}
              {page === 'alerts' && <AlertsPage />}
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
              {page === 'analytics_journeys' && (
                <JourneysPage
                  featureEnabled={featureFlags.journeys_enabled}
                  hasPermission={hasJourneysPermission}
                  canManageDefinitions={canManageJourneys}
                  journeyExamplesEnabled={featureFlags.journey_examples_enabled}
                  funnelBuilderEnabled={featureFlags.funnel_builder_enabled}
                />
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
                </>
              </ProtectedPage>
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
