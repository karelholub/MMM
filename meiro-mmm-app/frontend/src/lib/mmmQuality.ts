export type MMMQualityLevel = 'ready' | 'directional' | 'not_usable' | 'pending'

export interface MMMQualityInput {
  status?: string | null
  datasetAvailable?: boolean
  r2?: number | null
  weeks?: number
  channelsModeled?: number
  totalSpend?: number
  roi?: Array<{ roi?: number | null }> | null
  contrib?: Array<{ mean_share?: number | null }> | null
  diagnostics?: {
    rhat_max?: number | null
    ess_bulk_min?: number | null
    divergences?: number | null
  } | null
}

export interface MMMQualityResult {
  level: MMMQualityLevel
  label: string
  tone: 'success' | 'warning' | 'danger'
  reasons: string[]
  canUseResults: boolean
  canUseBudget: boolean
}

function finiteNumbers(values: Array<number | null | undefined>): number[] {
  return values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
}

function allNearZero(values: number[], epsilon = 1e-9): boolean {
  return values.length > 0 && values.every((value) => Math.abs(value) <= epsilon)
}

export function evaluateMMMRunQuality(input: MMMQualityInput): MMMQualityResult {
  const status = String(input.status || '').toLowerCase()
  if (status && status !== 'finished') {
    return {
      level: 'pending',
      label: status,
      tone: status === 'error' ? 'danger' : 'warning',
      reasons: [status === 'error' ? 'Model run ended in an error state.' : 'Model run is not finished yet.'],
      canUseResults: false,
      canUseBudget: false,
    }
  }

  const reasons: string[] = []
  const warnings: string[] = []
  const roiValues = finiteNumbers((input.roi ?? []).map((row) => row.roi))
  const contribShares = finiteNumbers((input.contrib ?? []).map((row) => row.mean_share))
  const hasOutput = roiValues.length > 0 && contribShares.length > 0
  const outputAllZero = hasOutput && allNearZero(roiValues) && allNearZero(contribShares)
  const hasSpendSignal = input.totalSpend != null
  const totalSpend = Number(input.totalSpend ?? 0)
  const hasDataset = input.datasetAvailable !== false
  const weeks = Number(input.weeks ?? 0)
  const channelsModeled = Number(input.channelsModeled ?? 0)
  const r2 = Number(input.r2)

  if (!hasOutput) reasons.push('Model output is incomplete: ROI or contribution rows are missing.')
  if (hasDataset && hasSpendSignal && totalSpend <= 0) reasons.push('Linked dataset has no mapped spend for selected model channels.')
  if (outputAllZero) reasons.push('All modeled ROI and contribution values are zero, so the run has no usable media signal.')

  if (Number.isFinite(r2) && r2 < 0.05) {
    const message = `Model fit is effectively flat (R² ${r2.toFixed(3)}).`
    if (outputAllZero) reasons.push(message)
    else warnings.push(message)
  } else if (Number.isFinite(r2) && r2 < 0.3) {
    warnings.push(`Model fit is weak (R² ${r2.toFixed(3)}).`)
  }

  if (input.datasetAvailable === false) warnings.push('Linked dataset preview is unavailable; source-row checks are disabled.')
  if (weeks > 0 && weeks < 20) warnings.push(`Short history: ${weeks.toLocaleString()} modeled weeks.`)
  if (channelsModeled > 0 && weeks > 0 && channelsModeled > weeks / 2) {
    warnings.push('Many modeled channels compared to available weeks.')
  }

  const diagnostics = input.diagnostics
  if (diagnostics?.rhat_max != null && diagnostics.rhat_max > 1.1) warnings.push(`R-hat ${Number(diagnostics.rhat_max).toFixed(2)} suggests convergence risk.`)
  if (diagnostics?.ess_bulk_min != null && diagnostics.ess_bulk_min < 200) warnings.push(`Effective sample size is low (${Number(diagnostics.ess_bulk_min).toFixed(0)}).`)
  if (diagnostics?.divergences != null && diagnostics.divergences > 0) warnings.push(`${Number(diagnostics.divergences).toLocaleString()} MCMC divergences detected.`)
  if (roiValues.some((value) => value < 0)) warnings.push('Some channels have negative ROI.')

  if (reasons.length > 0) {
    return {
      level: 'not_usable',
      label: 'Not usable',
      tone: 'danger',
      reasons: [...new Set(reasons.concat(warnings))],
      canUseResults: false,
      canUseBudget: false,
    }
  }

  if (warnings.length > 0) {
    return {
      level: 'directional',
      label: input.datasetAvailable === false ? 'Readout only' : 'Directional',
      tone: 'warning',
      reasons: [...new Set(warnings)],
      canUseResults: true,
      canUseBudget: input.datasetAvailable !== false,
    }
  }

  return {
    level: 'ready',
    label: 'Decision ready',
    tone: 'success',
    reasons: [],
    canUseResults: true,
    canUseBudget: true,
  }
}
