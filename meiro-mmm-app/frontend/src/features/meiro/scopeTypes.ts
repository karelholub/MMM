export interface MeiroMeasurementScopeMeta {
  strict?: boolean
  target_sites?: string[]
  source_scope?: {
    status?: string
    target_host?: string
    legacy_unverified_entries?: number
    out_of_scope_entries?: number
  }
  event_archive_site_scope?: {
    target_site_events?: number
    out_of_scope_site_events?: number
    unknown_site_events?: number
  }
  out_of_scope_campaign_labels?: number
  campaign_rows_excluded?: number
  warnings?: string[]
}

export interface MeiroScopeFilterMeta {
  strict?: boolean
  mode?: string
  out_of_scope_campaign_labels?: number
  campaign_rows_excluded?: number
  campaign_selectors_filtered?: boolean
}
