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
