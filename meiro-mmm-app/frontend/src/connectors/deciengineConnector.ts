import { apiSendJson } from '../lib/apiClient'

export interface DeciEngineEventsImportPayload {
  source_url: string
  user_email?: string
  user_role?: string
  user_id?: string
  profileId?: string
  campaignKey?: string
  messageId?: string
  from?: string
  to?: string
  limit?: number
}

export interface DeciEngineEventsImportResult {
  import_summary?: Record<string, unknown>
  quarantine_count?: number
  count?: number
  quarantine_run_id?: string | null
  message?: string
  validation_items?: Array<Record<string, unknown>>
}

export async function importDeciEngineActivationEvents(
  payload: DeciEngineEventsImportPayload,
): Promise<DeciEngineEventsImportResult> {
  return apiSendJson<DeciEngineEventsImportResult>(
    '/api/attribution/journeys/from-deciengine-events',
    'POST',
    payload,
    { fallbackMessage: 'Failed to import deciEngine activation events' },
  )
}
