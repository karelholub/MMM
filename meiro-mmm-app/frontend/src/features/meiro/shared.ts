import type { MeiroImportResult, MeiroPullConfig } from '../../connectors/meiroConnector'

export type MeiroTab = 'overview' | 'cdp' | 'pipes' | 'normalization' | 'import'

export type DryRunResult = {
  count: number
  preview: Array<{ id: string; touchpoints: number; value: number; quality_score?: number; quality_band?: string }>
  warnings: string[]
  validation: { ok?: boolean; error?: string }
  import_summary?: Record<string, unknown>
  cleaning_report?: Record<string, unknown>
  quarantine_count?: number
}

export interface MeiroWebhookArchiveStatus {
  available: boolean
  entries: number
  profiles_received?: number
  events_received?: number
  last_received_at?: string | null
  parser_versions?: string[]
}

export interface MeiroWebhookReprocessResult {
  reprocessed_profiles: number
  archive_entries_used: number
  archive_source?: 'profiles' | 'events'
  event_reconstruction_diagnostics?: {
    archive_source: 'events'
    events_loaded: number
    profiles_reconstructed: number
    avg_events_per_profile: number
    touchpoints_reconstructed?: number
    conversions_reconstructed?: number
    profiles_with_touchpoints?: number
    profiles_with_conversions?: number
    attributable_profiles?: number
    avg_touchpoints_per_profile?: number
    avg_conversions_per_profile?: number
    journeys_valid?: number
    journeys_quarantined?: number
    journeys_invalid?: number
    journeys_persisted?: number
    journeys_converted?: number
    persisted_from_attributable_share?: number
    warnings?: string[]
  }
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
  primary_ingest_source: 'profiles',
  replay_archive_source: 'auto',
  replay_archive_limit: 5000,
  replay_date_from: null,
  replay_date_to: null,
  auto_replay_mode: 'disabled',
  auto_replay_interval_minutes: 15,
  auto_replay_require_mapping_approval: true,
  auto_replay_quarantine_spike_threshold_pct: 40,
  conversion_event_aliases: {},
  touchpoint_interaction_aliases: {
    ad_impression: 'impression',
    impression: 'impression',
    ad_click: 'click',
    email_click: 'click',
    page_view: 'visit',
  },
  adjustment_event_aliases: {
    refund: 'refund',
    partial_refund: 'partial_refund',
    cancelled: 'cancellation',
    invalid_lead: 'invalid_lead',
    disqualified_lead: 'disqualified_lead',
  },
  adjustment_linkage_keys: ['conversion_id', 'order_id', 'lead_id', 'event_id'],
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
  const primaryIngestSource = typeof cfg.primary_ingest_source === 'string' && ['profiles', 'events'].includes(cfg.primary_ingest_source)
    ? cfg.primary_ingest_source as MeiroPullConfig['primary_ingest_source']
    : DEFAULT_MEIRO_PULL_CONFIG.primary_ingest_source
  const replayArchiveSource = typeof cfg.replay_archive_source === 'string' && ['auto', 'profiles', 'events'].includes(cfg.replay_archive_source)
    ? cfg.replay_archive_source as MeiroPullConfig['replay_archive_source']
    : DEFAULT_MEIRO_PULL_CONFIG.replay_archive_source
  const autoReplayMode = typeof cfg.auto_replay_mode === 'string' && ['disabled', 'interval', 'after_batch'].includes(cfg.auto_replay_mode)
    ? cfg.auto_replay_mode as MeiroPullConfig['auto_replay_mode']
    : DEFAULT_MEIRO_PULL_CONFIG.auto_replay_mode
  const normalizeMap = (value: unknown, fallback: Record<string, string> = {}) =>
    typeof value === 'object' && value && !Array.isArray(value)
      ? Object.fromEntries(
          Object.entries(value)
            .map(([key, val]) => [String(key).trim(), String(val ?? '').trim()])
            .filter(([key, val]) => key && val),
        )
      : fallback
  const linkageKeys = Array.isArray(cfg.adjustment_linkage_keys)
    ? cfg.adjustment_linkage_keys
        .map((value) => String(value || '').trim())
        .filter((value): value is 'conversion_id' | 'order_id' | 'lead_id' | 'event_id' => ['conversion_id', 'order_id', 'lead_id', 'event_id'].includes(value))
    : (DEFAULT_MEIRO_PULL_CONFIG.adjustment_linkage_keys || [])
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
    primary_ingest_source: primaryIngestSource,
    replay_archive_source: replayArchiveSource,
    replay_archive_limit: asInt(cfg.replay_archive_limit, DEFAULT_MEIRO_PULL_CONFIG.replay_archive_limit || 5000, 1, 50000),
    replay_date_from: typeof cfg.replay_date_from === 'string' && cfg.replay_date_from.trim() ? cfg.replay_date_from.trim() : null,
    replay_date_to: typeof cfg.replay_date_to === 'string' && cfg.replay_date_to.trim() ? cfg.replay_date_to.trim() : null,
    auto_replay_mode: autoReplayMode,
    auto_replay_interval_minutes: asInt(cfg.auto_replay_interval_minutes, DEFAULT_MEIRO_PULL_CONFIG.auto_replay_interval_minutes || 15, 1, 1440),
    auto_replay_require_mapping_approval: Boolean(cfg.auto_replay_require_mapping_approval ?? DEFAULT_MEIRO_PULL_CONFIG.auto_replay_require_mapping_approval),
    auto_replay_quarantine_spike_threshold_pct: asInt(cfg.auto_replay_quarantine_spike_threshold_pct, DEFAULT_MEIRO_PULL_CONFIG.auto_replay_quarantine_spike_threshold_pct || 40, 0, 100),
    conversion_event_aliases: normalizeMap(cfg.conversion_event_aliases, DEFAULT_MEIRO_PULL_CONFIG.conversion_event_aliases),
    touchpoint_interaction_aliases: normalizeMap(cfg.touchpoint_interaction_aliases, DEFAULT_MEIRO_PULL_CONFIG.touchpoint_interaction_aliases || {}),
    adjustment_event_aliases: normalizeMap(cfg.adjustment_event_aliases, DEFAULT_MEIRO_PULL_CONFIG.adjustment_event_aliases || {}),
    adjustment_linkage_keys: linkageKeys.length ? linkageKeys : (DEFAULT_MEIRO_PULL_CONFIG.adjustment_linkage_keys || []),
  }
}
