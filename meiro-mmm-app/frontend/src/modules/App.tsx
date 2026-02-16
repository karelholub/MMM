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
import LoginPage from '../components/LoginPage'
import {
  canAccessPage as canAccessAppPage,
  canManageJourneyDefinitions,
} from '../lib/accessControl'
import {
  AppSidebar,
  AppTopBar,
  NAV_ITEMS,
  type AppPage,
  type NavItem,
} from './app/AppChrome'

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

const PAGE_FALLBACK = (
  <div style={{ padding: 48, textAlign: 'center', color: '#64748b', fontSize: 14 }}>
    Loading…
  </div>
)

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

function parseInitialPage(pathname: string): AppPage {
  if (pathname === '/analytics/journeys') return 'analytics_journeys'
  return 'overview'
}

function pageToPathname(page: AppPage): string {
  if (page === 'analytics_journeys') return '/analytics/journeys'
  return '/'
}

export default function App() {
  const [page, setPage] = useState<AppPage>(() => {
    if (typeof window === 'undefined') return 'overview'
    return parseInitialPage(window.location.pathname)
  })
  const [lastPage, setLastPage] = useState<AppPage | null>(null)
  const settingsRef = useRef<SettingsPageHandle | null>(null)
  const [settingsDirtySections, setSettingsDirtySections] = useState<SectionKey[]>([])
  const [pendingSettingsNav, setPendingSettingsNav] = useState<AppPage | null>(null)
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
  const [pinnedPages, setPinnedPages] = useState<AppPage[]>(() => {
    if (typeof window === 'undefined') return []
    try {
      const raw = window.localStorage.getItem('mmm-pinned-pages-v1')
      if (!raw) return []
      const parsed = JSON.parse(raw) as AppPage[]
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
    (nextPage: AppPage): boolean => {
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

  const navigateToPage = useCallback((next: AppPage) => {
    setLastPage((prev) => (prev === next ? prev : prev ?? 'dashboard'))
    setPage(next)
  }, [])

  const handleSetPage = useCallback(
    (next: AppPage) => {
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
  const showLogin = !permissions.isLoading && permissions.auth?.authenticated !== true

  if (showLogin) {
    return <LoginPage />
  }

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
          gridTemplateRows: 'auto 1fr',
        }}
      >
        <AppSidebar
          page={page}
          navSearch={navSearch}
          sidebarCollapsed={sidebarCollapsed}
          pinnedPages={pinnedPages}
          visibleNavItems={visibleNavItems}
          onToggleCollapse={() => {
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
          onNavSearchChange={setNavSearch}
          onSetPage={handleSetPage}
          onSetPinnedPages={setPinnedPages}
        />

        <AppTopBar
          isMobileHeader={isMobileHeader}
          periodLabel={periodLabel}
          conversionLabel={conversionLabel}
          selectedModel={selectedModel}
          selectedConfigId={selectedConfigId}
          modelConfigs={modelConfigsQuery.data ?? []}
          modelConfigsLoading={modelConfigsQuery.isLoading}
          journeysLoaded={journeysLoaded}
          journeyCount={journeyCount}
          primaryKpiLabel={primaryKpiLabel}
          primaryKpiCount={primaryKpiCount}
          convertedCount={convertedCount}
          loadingSample={loadSampleMutation.isPending}
          runningModels={runAllMutation.isPending}
          onModelChange={setSelectedModel}
          onConfigChange={setSelectedConfigId}
          onLoadSample={() => loadSampleMutation.mutate()}
          onRunModels={() => runAllMutation.mutate()}
        />

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
