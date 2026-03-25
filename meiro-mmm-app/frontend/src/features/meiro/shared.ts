import type { MeiroCleaningReport, MeiroImportResult, MeiroImportSummary, MeiroPullConfig } from '../../connectors/meiroConnector'

export type MeiroTab = 'overview' | 'cdp' | 'pipes' | 'normalization' | 'import'

export type DryRunResult = {
  count: number
  preview: Array<{ id: string; touchpoints: number; value: number; quality_score?: number; quality_band?: string }>
  warnings: string[]
  validation: { ok?: boolean; error?: string }
  import_summary?: MeiroImportSummary
  cleaning_report?: MeiroCleaningReport
  quarantine_count?: number
}

export interface MeiroWebhookArchiveStatus {
  available: boolean
  entries: number
  profiles_received?: number
  last_received_at?: string | null
  parser_versions?: string[]
}

export interface MeiroWebhookReprocessResult {
  reprocessed_profiles: number
  archive_entries_used: number
  persisted_to_attribution: boolean
  import_result?: MeiroImportResult
}

export const DEFAULT_MEIRO_PULL_CONFIG: MeiroPullConfig = {
  lookback_days: 30,
  session_gap_minutes: 30,
  conversion_selector: 'purchase',
  output_mode: 'single',
  dedup_interval_minutes: 5,
  dedup_mode: 'balanced',
  primary_dedup_key: 'auto',
  fallback_dedup_keys: ['conversion_id', 'order_id', 'event_id'],
  strict_ingest: true,
  quarantine_unknown_channels: true,
  quarantine_missing_utm: false,
  quarantine_duplicate_profiles: true,
  timestamp_fallback_policy: 'profile',
  value_fallback_policy: 'default',
  currency_fallback_policy: 'default',
  replay_mode: 'last_n',
  replay_archive_limit: 5000,
  replay_date_from: null,
  replay_date_to: null,
  conversion_event_aliases: {},
}

export function normalizeMeiroPullConfig(raw?: Partial<MeiroPullConfig> | Record<string, unknown> | null): MeiroPullConfig {
  const cfg = raw || {}
  const asInt = (value: unknown, fallback: number, min: number, max: number) => {
    const parsed = Number(value)
    if (!Number.isFinite(parsed)) return fallback
    return Math.max(min, Math.min(max, Math.round(parsed)))
  }
  const dedupMode = typeof cfg.dedup_mode === 'string' && ['strict', 'balanced', 'aggressive'].includes(cfg.dedup_mode)
    ? cfg.dedup_mode as MeiroPullConfig['dedup_mode']
    : DEFAULT_MEIRO_PULL_CONFIG.dedup_mode
  const primaryDedupKey = typeof cfg.primary_dedup_key === 'string' && ['auto', 'conversion_id', 'order_id', 'event_id'].includes(cfg.primary_dedup_key)
    ? cfg.primary_dedup_key as MeiroPullConfig['primary_dedup_key']
    : DEFAULT_MEIRO_PULL_CONFIG.primary_dedup_key
  const rawFallback = Array.isArray(cfg.fallback_dedup_keys)
    ? cfg.fallback_dedup_keys
    : typeof cfg.fallback_dedup_keys === 'string'
      ? cfg.fallback_dedup_keys.split(',')
      : []
  const fallback = rawFallback
    .map((value) => String(value || '').trim())
    .filter((value): value is 'conversion_id' | 'order_id' | 'event_id' => ['conversion_id', 'order_id', 'event_id'].includes(value))
    .filter((value) => value !== primaryDedupKey)
  const timestampFallbackPolicy = typeof cfg.timestamp_fallback_policy === 'string' && ['profile', 'conversion', 'quarantine'].includes(cfg.timestamp_fallback_policy)
    ? cfg.timestamp_fallback_policy as MeiroPullConfig['timestamp_fallback_policy']
    : DEFAULT_MEIRO_PULL_CONFIG.timestamp_fallback_policy
  const valueFallbackPolicy = typeof cfg.value_fallback_policy === 'string' && ['default', 'zero', 'quarantine'].includes(cfg.value_fallback_policy)
    ? cfg.value_fallback_policy as MeiroPullConfig['value_fallback_policy']
    : DEFAULT_MEIRO_PULL_CONFIG.value_fallback_policy
  const currencyFallbackPolicy = typeof cfg.currency_fallback_policy === 'string' && ['default', 'quarantine'].includes(cfg.currency_fallback_policy)
    ? cfg.currency_fallback_policy as MeiroPullConfig['currency_fallback_policy']
    : DEFAULT_MEIRO_PULL_CONFIG.currency_fallback_policy
  const replayMode = typeof cfg.replay_mode === 'string' && ['all', 'last_n', 'date_range'].includes(cfg.replay_mode)
    ? cfg.replay_mode as MeiroPullConfig['replay_mode']
    : DEFAULT_MEIRO_PULL_CONFIG.replay_mode
  return {
    lookback_days: asInt(cfg.lookback_days, DEFAULT_MEIRO_PULL_CONFIG.lookback_days, 1, 365),
    session_gap_minutes: asInt(cfg.session_gap_minutes, DEFAULT_MEIRO_PULL_CONFIG.session_gap_minutes, 1, 720),
    conversion_selector: String(cfg.conversion_selector || DEFAULT_MEIRO_PULL_CONFIG.conversion_selector).trim() || DEFAULT_MEIRO_PULL_CONFIG.conversion_selector,
    output_mode: 'single',
    dedup_interval_minutes: asInt(cfg.dedup_interval_minutes, DEFAULT_MEIRO_PULL_CONFIG.dedup_interval_minutes, 0, 1440),
    dedup_mode: dedupMode,
    primary_dedup_key: primaryDedupKey,
    fallback_dedup_keys: fallback.length ? fallback : DEFAULT_MEIRO_PULL_CONFIG.fallback_dedup_keys,
    strict_ingest: Boolean(cfg.strict_ingest ?? DEFAULT_MEIRO_PULL_CONFIG.strict_ingest),
    quarantine_unknown_channels: Boolean(cfg.quarantine_unknown_channels ?? DEFAULT_MEIRO_PULL_CONFIG.quarantine_unknown_channels),
    quarantine_missing_utm: Boolean(cfg.quarantine_missing_utm ?? DEFAULT_MEIRO_PULL_CONFIG.quarantine_missing_utm),
    quarantine_duplicate_profiles: Boolean(cfg.quarantine_duplicate_profiles ?? DEFAULT_MEIRO_PULL_CONFIG.quarantine_duplicate_profiles),
    timestamp_fallback_policy: timestampFallbackPolicy,
    value_fallback_policy: valueFallbackPolicy,
    currency_fallback_policy: currencyFallbackPolicy,
    replay_mode: replayMode,
    replay_archive_limit: asInt(cfg.replay_archive_limit, DEFAULT_MEIRO_PULL_CONFIG.replay_archive_limit || 5000, 1, 50000),
    replay_date_from: typeof cfg.replay_date_from === 'string' && cfg.replay_date_from.trim() ? cfg.replay_date_from.trim() : null,
    replay_date_to: typeof cfg.replay_date_to === 'string' && cfg.replay_date_to.trim() ? cfg.replay_date_to.trim() : null,
    conversion_event_aliases: typeof cfg.conversion_event_aliases === 'object' && cfg.conversion_event_aliases && !Array.isArray(cfg.conversion_event_aliases)
      ? Object.fromEntries(Object.entries(cfg.conversion_event_aliases).map(([key, value]) => [String(key).trim(), String(value ?? '').trim()]).filter(([key, value]) => key && value))
      : DEFAULT_MEIRO_PULL_CONFIG.conversion_event_aliases,
  }
}
