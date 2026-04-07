export function buildJourneyHypothesisHref(args: {
  journeyDefinitionId?: string | null
  hypothesisId?: string | null
  tab?: 'policy' | 'hypotheses' | 'experiments'
}): string | null {
  const journeyDefinitionId = String(args.journeyDefinitionId || '').trim()
  const hypothesisId = String(args.hypothesisId || '').trim()
  if (!journeyDefinitionId || !hypothesisId) return null
  const params = new URLSearchParams()
  params.set('page', 'analytics_journeys')
  params.set('journey_id', journeyDefinitionId)
  params.set('tab', args.tab || 'policy')
  params.set('hypothesis_id', hypothesisId)
  return `/?${params.toString()}`
}

export function buildJourneyHypothesisSeedHref(args: {
  journeyDefinitionId?: string | null
  title?: string | null
  hypothesisText?: string | null
  pathHash?: string | null
  path?: string | null
  supportCount?: number | null
  baselineRate?: number | null
  channelGroup?: string | null
  campaignId?: string | null
  device?: string | null
  country?: string | null
}): string | null {
  const journeyDefinitionId = String(args.journeyDefinitionId || '').trim()
  if (!journeyDefinitionId) return null
  const params = new URLSearchParams()
  params.set('page', 'analytics_journeys')
  params.set('journey_id', journeyDefinitionId)
  params.set('tab', 'hypotheses')
  params.set('hypothesis_seed', 'lag_path')
  if (args.title) params.set('seed_title', String(args.title))
  if (args.hypothesisText) params.set('seed_note', String(args.hypothesisText))
  if (args.pathHash) params.set('seed_path_hash', String(args.pathHash))
  if (args.path) params.set('seed_path', String(args.path))
  if (args.supportCount != null) params.set('seed_support', String(args.supportCount))
  if (args.baselineRate != null) params.set('seed_baseline', String(args.baselineRate))
  if (args.channelGroup) params.set('seed_channel', String(args.channelGroup))
  if (args.campaignId) params.set('seed_campaign', String(args.campaignId))
  if (args.device) params.set('seed_device', String(args.device))
  if (args.country) params.set('seed_country', String(args.country))
  return `/?${params.toString()}`
}
