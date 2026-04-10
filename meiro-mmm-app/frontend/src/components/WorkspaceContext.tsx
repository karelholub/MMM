import { createContext, useContext } from 'react'

export interface JourneyReadiness {
  status: string
  blockers: string[]
  warnings: string[]
  summary: {
    primary_kpi_coverage: number
    taxonomy_unknown_share: number
    journeys_loaded?: number
    freshness_hours?: number | null
    latest_event_replay?: {
      run_id?: string
      source?: string
      started_at?: string | null
      import_note?: string | null
      diagnostics?: {
        events_loaded?: number
        profiles_reconstructed?: number
        touchpoints_reconstructed?: number
        conversions_reconstructed?: number
        attributable_profiles?: number
        journeys_persisted?: number
        warnings?: string[]
      }
    } | null
  }
  details?: {
    latest_event_replay?: {
      run_id?: string
      source?: string
      started_at?: string | null
      import_note?: string | null
      diagnostics?: {
        events_loaded?: number
        profiles_reconstructed?: number
        touchpoints_reconstructed?: number
        conversions_reconstructed?: number
        attributable_profiles?: number
        journeys_persisted?: number
        warnings?: string[]
      }
    } | null
  }
}

// Keep this in sync with /api/attribution/journeys response shape
export interface JourneysSummary {
  loaded: boolean
  count: number
  converted: number
  non_converted: number
  data_freshness_hours?: number | null
  channels?: string[]
  total_value?: number
  primary_kpi_id?: string | null
  primary_kpi_label?: string | null
  primary_kpi_count?: number
  kpi_counts?: Record<string, number>
  date_min?: string | null
  date_max?: string | null
  readiness?: JourneyReadiness | null
  consistency_warnings?: string[]
}

interface WorkspaceContextValue {
  // Attribution model + config (global for workspace)
  attributionModel: string
  setAttributionModel: (model: string) => void
  selectedConfigId: string | null
  setSelectedConfigId: (id: string | null) => void

  // Journeys / measurement context
  journeysSummary?: JourneysSummary
  journeysLoaded: boolean
  globalDateFrom: string
  globalDateTo: string
  setGlobalDateRange: (next: { dateFrom: string; dateTo: string }) => void

  // Global actions
  isRunningAttribution: boolean
  isLoadingSampleJourneys: boolean
  runAllAttribution: () => void
  loadSampleJourneys: () => void
}

export const WorkspaceContext = createContext<WorkspaceContextValue | undefined>(undefined)

export function useWorkspaceContext(): WorkspaceContextValue {
  const ctx = useContext(WorkspaceContext)
  if (!ctx) {
    throw new Error('useWorkspaceContext must be used within a WorkspaceContext.Provider')
  }
  return ctx
}
