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
  criteria_label?: string | null
  size?: number | null
  external_segment_id?: string | null
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
}

export interface SegmentFilterState {
  channel: string
  campaign: string
  device: string
  geo: string
  segment: string
}

type LocalSegmentDefinition = {
  channel_group?: string
  campaign_id?: string
  device?: string
  country?: string
}

function readString(value: unknown): string | null {
  const normalized = typeof value === 'string' ? value.trim() : ''
  return normalized || null
}

function toOptionalString(value: string | null): string | undefined {
  return value ?? undefined
}

export function isLocalAnalyticalSegment(item: SegmentRegistryItem): boolean {
  return item.source === 'local_analytical'
}

export function isOperationalSegment(item: SegmentRegistryItem): boolean {
  return item.source === 'meiro_pipes'
}

export function readLocalSegmentDefinition(item: SegmentRegistryItem | null | undefined): LocalSegmentDefinition {
  const definition = item?.definition || {}
  return {
    channel_group: toOptionalString(readString(definition.channel_group)),
    campaign_id: toOptionalString(readString(definition.campaign_id)),
    device: toOptionalString(readString(definition.device)),
    country: toOptionalString(readString(definition.country)),
  }
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

export function activeLocalSegmentDefinitionFromFilters(filters: SegmentFilterState): LocalSegmentDefinition {
  return {
    ...(filters.channel !== 'all' ? { channel_group: filters.channel } : {}),
    ...(filters.campaign !== 'all' ? { campaign_id: filters.campaign } : {}),
    ...(filters.device !== 'all' ? { device: filters.device } : {}),
    ...(filters.geo !== 'all' ? { country: filters.geo } : {}),
  }
}

export function hasLocalSegmentCriteria(definition: LocalSegmentDefinition): boolean {
  return Object.values(definition).some((value) => Boolean(value))
}

export function buildLocalSegmentDefaultName(definition: LocalSegmentDefinition): string {
  const parts: string[] = []
  if (definition.channel_group) parts.push(definition.channel_group)
  if (definition.campaign_id) parts.push(definition.campaign_id)
  if (definition.device) parts.push(definition.device)
  if (definition.country) parts.push(String(definition.country).toUpperCase())
  return parts.length ? parts.join(' · ') : 'New analytical segment'
}

export function segmentOptionLabel(item: SegmentRegistryItem): string {
  const suffix = item.criteria_label ? ` · ${item.criteria_label}` : ''
  const size = item.size != null ? ` · ${item.size.toLocaleString()} profiles` : ''
  return `${item.name}${suffix}${size}`
}

export function buildSegmentReference(item: SegmentRegistryItem | null | undefined): Record<string, unknown> {
  if (!item) return {}
  if (isLocalAnalyticalSegment(item)) {
    return {
      segment_source: 'local_analytical',
      segment_id: item.id,
      segment_name: item.name,
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
  delete next.external_segment_source
  delete next.external_segment_id
  delete next.external_segment_name
  return next
}
