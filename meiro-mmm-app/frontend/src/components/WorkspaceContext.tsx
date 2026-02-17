import { createContext, useContext } from 'react'

// Keep this in sync with /api/attribution/journeys response shape
export interface JourneysSummary {
  loaded: boolean
  count: number
  converted: number
  non_converted: number
  channels?: string[]
  total_value?: number
  primary_kpi_id?: string | null
  primary_kpi_label?: string | null
  primary_kpi_count?: number
  kpi_counts?: Record<string, number>
  date_min?: string | null
  date_max?: string | null
}

export interface WorkspaceContextValue {
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
