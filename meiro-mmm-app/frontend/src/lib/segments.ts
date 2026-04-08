export type SegmentRuleField =
  | 'channel_group'
  | 'campaign_id'
  | 'device'
  | 'country'
  | 'conversion_key'
  | 'last_touch_channel'
  | 'interaction_path_type'
  | 'path_length'
  | 'lag_days'
  | 'net_revenue_total'
  | 'gross_revenue_total'
  | 'net_conversions_total'
  | 'gross_conversions_total'

export type SegmentRuleOperator = 'eq' | 'gte' | 'lte'

export interface SegmentDefinitionRule {
  field: SegmentRuleField
  op: SegmentRuleOperator
  value: string | number
}

export interface LocalSegmentDefinitionV2 {
  version: 'v2'
  match: 'all' | 'any'
  rules: SegmentDefinitionRule[]
}

export interface SegmentCompatibility {
  filter_keys?: string[]
  auto_filter_compatible?: boolean
  advanced?: boolean
}

export interface SegmentPreview {
  journey_rows?: number
  share_of_rows?: number
  profiles?: number
  conversions?: number
  revenue?: number
  median_lag_days?: number | null
  avg_path_length?: number | null
}

export interface SegmentRegistryItem {
  id: string
  workspace_id?: string | null
  name: string
  description?: string | null
  status?: string | null
  source: 'local_analytical' | 'meiro_pipes' | string
  source_label?: string | null
  kind?: 'analytical' | 'operational' | string
  supports_analysis?: boolean
  supports_activation?: boolean
  supports_hypotheses?: boolean
  supports_experiments?: boolean
  definition: Record<string, unknown>
  definition_version?: string | null
  segment_family?: string | null
  criteria_label?: string | null
  size?: number | null
  external_segment_id?: string | null
  compatibility?: SegmentCompatibility | null
  preview?: SegmentPreview | null
}

export interface SegmentRegistryResponse {
  items: SegmentRegistryItem[]
  summary: {
    local_analytical: number
    meiro_pipes: number
    analysis_ready: number
    activation_ready: number
  }
}

export interface SegmentContextDimensionValue {
  value: string
  count: number
}

export interface SegmentSuggestedRule {
  label: string
  field: SegmentRuleField
  op: SegmentRuleOperator
  value: string | number
}

export interface SegmentContextResponse {
  summary: {
    journey_rows: number
    date_from?: string | null
    date_to?: string | null
  }
  channels: SegmentContextDimensionValue[]
  campaigns: SegmentContextDimensionValue[]
  devices: SegmentContextDimensionValue[]
  countries: SegmentContextDimensionValue[]
  conversion_keys: SegmentContextDimensionValue[]
  last_touch_channels: SegmentContextDimensionValue[]
  path_types: SegmentContextDimensionValue[]
  suggested_rules?: {
    lag_days?: SegmentSuggestedRule[]
    path_length?: SegmentSuggestedRule[]
    value?: SegmentSuggestedRule[]
  }
}

export interface SegmentFilterState {
  channel: string
  campaign: string
  device: string
  geo: string
  segment: string
}

type LocalSegmentFilterDefinition = {
  channel_group?: string
  campaign_id?: string
  device?: string
  country?: string
}

const LEGACY_FILTER_FIELDS: SegmentRuleField[] = ['channel_group', 'campaign_id', 'device', 'country']

function readString(value: unknown): string | null {
  const normalized = typeof value === 'string' ? value.trim() : ''
  return normalized || null
}

function toOptionalString(value: string | null): string | undefined {
  return value ?? undefined
}

function parseRuleValue(value: unknown): string | number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  const text = readString(value)
  if (!text) return null
  const maybeNumber = Number(text)
  if (!Number.isNaN(maybeNumber) && `${maybeNumber}` === text) return maybeNumber
  return text
}

export function normalizeLocalSegmentDefinition(input: Record<string, unknown> | null | undefined): LocalSegmentDefinitionV2 {
  const raw = input || {}
  const rawRules = Array.isArray(raw.rules) ? raw.rules : null
  const rules: SegmentDefinitionRule[] = []
  if (rawRules) {
    rawRules.forEach((candidate) => {
      if (!candidate || typeof candidate !== 'object') return
      const field = readString((candidate as Record<string, unknown>).field) as SegmentRuleField | null
      const op = (readString((candidate as Record<string, unknown>).op || (candidate as Record<string, unknown>).operator) ?? '') as SegmentRuleOperator
      const value = parseRuleValue((candidate as Record<string, unknown>).value)
      if (!field || value == null) return
      if (!['eq', 'gte', 'lte'].includes(op)) return
      rules.push({ field, op, value })
    })
  } else {
    LEGACY_FILTER_FIELDS.forEach((field) => {
      const value = readString(raw[field])
      if (value) rules.push({ field, op: 'eq', value })
    })
  }
  const match = readString(raw.match) === 'any' ? 'any' : 'all'
  return { version: 'v2', match, rules }
}

export function isLocalAnalyticalSegment(item: SegmentRegistryItem): boolean {
  return item.source === 'local_analytical'
}

export function isOperationalSegment(item: SegmentRegistryItem): boolean {
  return item.source === 'meiro_pipes'
}

export function readLocalSegmentDefinition(item: SegmentRegistryItem | null | undefined): LocalSegmentFilterDefinition {
  const definition = normalizeLocalSegmentDefinition((item?.definition as Record<string, unknown>) || {})
  const extracted: LocalSegmentFilterDefinition = {}
  definition.rules.forEach((rule) => {
    if (rule.op !== 'eq') return
    if (!LEGACY_FILTER_FIELDS.includes(rule.field)) return
    if (typeof rule.value !== 'string') return
    extracted[rule.field] = rule.value
  })
  return extracted
}

export function localSegmentDefinedKeys(item: SegmentRegistryItem | null | undefined): string[] {
  const definition = readLocalSegmentDefinition(item)
  return Object.entries(definition)
    .filter(([, value]) => Boolean(value))
    .map(([key]) => key)
}

export function localSegmentCompatibleWithDimensions(
  item: SegmentRegistryItem | null | undefined,
  supportedKeys: string[],
): boolean {
  const compatibility = item?.compatibility
  if (compatibility?.auto_filter_compatible != null) {
    return Boolean(
      compatibility.auto_filter_compatible &&
        (compatibility.filter_keys || []).length > 0 &&
        (compatibility.filter_keys || []).every((key) => supportedKeys.includes(key)),
    )
  }
  const keys = localSegmentDefinedKeys(item)
  return keys.length > 0 && keys.every((key) => supportedKeys.includes(key))
}

export function applyLocalSegmentToFilterState<T extends SegmentFilterState>(
  filters: T,
  item: SegmentRegistryItem | null | undefined,
): T {
  const definition = readLocalSegmentDefinition(item)
  return {
    ...filters,
    channel: definition.channel_group || 'all',
    campaign: definition.campaign_id || 'all',
    device: definition.device || 'all',
    geo: definition.country || 'all',
    segment: item?.id || 'all',
  }
}

export function activeLocalSegmentDefinitionFromFilters(filters: SegmentFilterState): LocalSegmentFilterDefinition {
  return {
    ...(filters.channel !== 'all' ? { channel_group: filters.channel } : {}),
    ...(filters.campaign !== 'all' ? { campaign_id: filters.campaign } : {}),
    ...(filters.device !== 'all' ? { device: filters.device } : {}),
    ...(filters.geo !== 'all' ? { country: filters.geo } : {}),
  }
}

export function hasLocalSegmentCriteria(definition: LocalSegmentFilterDefinition): boolean {
  return Object.values(definition).some((value) => Boolean(value))
}

export function buildLocalSegmentDefaultName(definition: LocalSegmentFilterDefinition): string {
  const parts: string[] = []
  if (definition.channel_group) parts.push(definition.channel_group)
  if (definition.campaign_id) parts.push(definition.campaign_id)
  if (definition.device) parts.push(definition.device)
  if (definition.country) parts.push(String(definition.country).toUpperCase())
  return parts.length ? parts.join(' · ') : 'New analytical segment'
}

export function formatSegmentPreview(item: SegmentRegistryItem): string | null {
  if (!item.preview) return null
  const parts: string[] = []
  if (item.preview.journey_rows != null) parts.push(`${item.preview.journey_rows.toLocaleString()} rows`)
  if (item.preview.profiles != null) parts.push(`${item.preview.profiles.toLocaleString()} profiles`)
  if (item.preview.median_lag_days != null) parts.push(`median lag ${item.preview.median_lag_days}d`)
  return parts.join(' · ') || null
}

export function segmentOptionLabel(item: SegmentRegistryItem): string {
  const suffix = item.criteria_label ? ` · ${item.criteria_label}` : ''
  const preview = formatSegmentPreview(item)
  const previewSuffix = preview ? ` · ${preview}` : ''
  const size = item.size != null ? ` · ${item.size.toLocaleString()} profiles` : ''
  return `${item.name}${suffix}${previewSuffix}${size}`
}

export function buildSegmentReference(item: SegmentRegistryItem | null | undefined): Record<string, unknown> {
  if (!item) return {}
  if (isLocalAnalyticalSegment(item)) {
    return {
      segment_source: 'local_analytical',
      segment_id: item.id,
      segment_name: item.name,
      segment_definition_version: item.definition_version || 'v2',
      segment_family: item.segment_family || null,
      ...readLocalSegmentDefinition(item),
    }
  }
  return {
    external_segment_source: 'meiro_pipes',
    external_segment_id: item.external_segment_id || item.id,
    external_segment_name: item.name,
  }
}

export function readSelectedSegmentRegistryId(segment: Record<string, unknown> | null | undefined): string {
  const localId = readString(segment?.segment_id)
  if (localId) return localId
  const externalSource = readString(segment?.external_segment_source)
  const externalId = readString(segment?.external_segment_id)
  if (externalSource === 'meiro_pipes' && externalId) return `meiro:${externalId}`
  return ''
}

export function clearSegmentReferenceMetadata(segment: Record<string, unknown> | null | undefined): Record<string, unknown> {
  const next = { ...(segment || {}) }
  delete next.segment_source
  delete next.segment_id
  delete next.segment_name
  delete next.segment_definition_version
  delete next.segment_family
  delete next.external_segment_source
  delete next.external_segment_id
  delete next.external_segment_name
  return next
}
