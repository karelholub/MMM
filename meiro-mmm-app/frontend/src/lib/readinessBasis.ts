type ReadinessBasis = 'live_attribution' | 'materialized_outputs'

const MATERIALIZED_WARNING_PATTERNS = [
  'persisted journey',
  'persisted journeys',
  'materialized',
  'journey definition',
  'survived import',
  'journeys persisted',
]

const LIVE_WARNING_PATTERNS = [
  'live attribution',
  'current journeys',
  'kpi totals',
  'channel conversions',
  'campaign conversions',
]

function includesAny(value: string, patterns: string[]) {
  const normalized = value.toLowerCase()
  return patterns.some((pattern) => normalized.includes(pattern))
}

function classifyReadinessWarning(warning: string): ReadinessBasis | 'general' {
  if (includesAny(warning, MATERIALIZED_WARNING_PATTERNS)) return 'materialized_outputs'
  if (includesAny(warning, LIVE_WARNING_PATTERNS)) return 'live_attribution'
  return 'general'
}

export function formatReadinessWarningsForBasis(
  warnings: string[] = [],
  basis: ReadinessBasis,
) {
  return warnings.map((warning) => {
    const warningBasis = classifyReadinessWarning(warning)
    if (basis === 'live_attribution' && warningBasis === 'materialized_outputs') {
      return `Materialized journey output warning, not a live-attribution failure: ${warning}`
    }
    if (basis === 'materialized_outputs' && warningBasis === 'live_attribution') {
      return `Live-attribution context: ${warning}`
    }
    return warning
  })
}

export function readinessBasisSubtitle(
  warnings: string[] = [],
  basis: ReadinessBasis,
) {
  const hasMaterializedWarning = warnings.some((warning) => classifyReadinessWarning(warning) === 'materialized_outputs')
  if (basis === 'live_attribution' && hasMaterializedWarning) {
    return 'This page reads the live attribution layer. Materialized-output warnings are shown as downstream rebuild context, not as proof that the live metrics are invalid.'
  }
  if (basis === 'materialized_outputs') {
    return 'This page reads materialized journey/path outputs, so replay and persisted-output warnings are directly relevant to this view.'
  }
  return undefined
}
