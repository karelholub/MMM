import { useState, useMemo } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import ContextSummaryStrip from '../components/dashboard/ContextSummaryStrip'
import DecisionStatusCard from '../components/DecisionStatusCard'
import AnalysisShareActions from '../components/dashboard/AnalysisShareActions'
import SectionCard from '../components/dashboard/SectionCard'
import AnalysisNarrativePanel from '../components/dashboard/AnalysisNarrativePanel'
import { navigateForRecommendedAction } from '../lib/recommendedActions'
import type { RecommendedActionItem } from '../components/RecommendedActionsList'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import {
  createAdsChangeRequestsFromBudgetRecommendation,
  type AdsProviderKey,
} from '../connectors/adsManagerConnector'

const t = tokens

interface BudgetOptimizerProps {
  roiData: { channel: string; roi: number }[]
  contribData: { channel: string; mean_share: number; mean_contribution?: number }[]
  baselineKPI?: number
  runId?: string | null
  datasetId?: string | null
  datasetAvailable?: boolean
  budgetActionsAvailable?: boolean
  budgetReadoutReason?: string | null
  onRecommendedActionClick?: (action: RecommendedActionItem) => void
}

interface ChannelConstraint {
  min: number
  max: number
  locked: boolean
}

interface WhatIfResult {
  baseline: { total_kpi: number; per_channel: Record<string, number> }
  scenario: {
    total_kpi: number
    per_channel: Record<string, number>
    multipliers: Record<string, number>
  }
  lift: { absolute: number; percent: number }
}

interface RecommendationActionItem {
  channel: string
  campaign_id?: string | null
  action: 'increase' | 'decrease' | 'hold'
  delta_pct?: number | null
  delta_amount?: number | null
  base_spend?: number | null
  new_spend?: number | null
  reason?: string | null
  outside_observed_range?: boolean
}

interface DecisionPayload {
  status?: string | null
  subtitle?: string | null
  blockers?: string[]
  warnings?: string[]
  actions?: Array<{
    id: string
    label: string
    benefit?: string
    requires_review?: boolean
    domain?: string
    target_page?: string
    target_section?: string
    target_tab?: string
  }>
}

interface BudgetRecommendationItem {
  id: string
  objective: string
  scope: string
  status: string
  title: string
  summary: string
  expected_impact: {
    metric: string
    delta_pct: number
    delta_abs: number
    net_budget_change?: number
    reallocated_spend?: number
  }
  confidence: { score: number; band: string }
  risk: { extrapolation: string; readiness: string }
  actions: RecommendationActionItem[]
  evidence: string[]
  decision: DecisionPayload
}

interface BudgetRecommendationsResponse {
  run_id: string
  objective: string
  recommendations: BudgetRecommendationItem[]
  decision: DecisionPayload
  summary: {
    total_budget_change_pct: number
    baseline_spend_total: number
    channels_considered: number
    periods: number
    weighted_roi: number
  }
}

interface SavedBudgetScenario {
  id: string
  objective: string
  total_budget_change_pct: number
  multipliers: Record<string, number>
  summary: Record<string, unknown>
  created_by?: string | null
  created_at?: string | null
  recommendations: BudgetRecommendationItem[]
}

interface CampaignSummaryItem {
  channel: string
  campaign: string
  mean_spend: number
  roi?: number
}

interface BudgetRealizationItem {
  scenario_id: string
  run_id: string
  objective: string
  created_at?: string | null
  expected_impact: {
    delta_pct?: number
    reallocated_spend?: number
  }
  execution: {
    counts: Record<string, number>
    proposed_budget_delta_total: number
    applied_budget_delta_total: number
    execution_progress_pct: number
    projected_realized_lift_pct: number
    latest_change_at?: string | null
  }
  change_requests: Array<{
    id: string
    provider: AdsProviderKey
    entity_id: string
    status: string
    proposed_daily_budget: number
    previous_daily_budget: number
    delta_budget: number
    error_message?: string | null
  }>
}

interface BudgetRealizationResponse {
  items: BudgetRealizationItem[]
  total: number
}

function formatCurrency(val: number): string {
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`
  if (val >= 1_000) return `$${(val / 1_000).toFixed(0)}K`
  return `$${val.toFixed(0)}`
}

function providerFromBudgetChannel(channel: string): AdsProviderKey | null {
  const key = (channel || '').toLowerCase()
  if (key.includes('google')) return 'google_ads'
  if (key.includes('meta') || key.includes('facebook') || key.includes('fb')) return 'meta_ads'
  if (key.includes('linkedin')) return 'linkedin_ads'
  return null
}

function isTallMmmDataset(rows: Record<string, unknown>[]): boolean {
  return rows.some((row) => row.channel != null && row.spend != null)
}

export default function BudgetOptimizer({
  roiData,
  contribData,
  baselineKPI = 100,
  runId,
  datasetId,
  datasetAvailable = true,
  budgetActionsAvailable = true,
  budgetReadoutReason,
  onRecommendedActionClick,
}: BudgetOptimizerProps) {
  const queryClient = useQueryClient()
  const [multipliers, setMultipliers] = useState<Record<string, number>>(() =>
    roiData.reduce((acc, { channel }) => ({ ...acc, [channel]: 1.0 }), {})
  )
  const [constraints, setConstraints] = useState<Record<string, ChannelConstraint>>(() =>
    roiData.reduce(
      (acc, { channel }) => ({ ...acc, [channel]: { min: 0.5, max: 2.0, locked: false } }),
      {}
    )
  )
  const [totalBudgetMode, setTotalBudgetMode] = useState<'constant' | 'change'>('constant')
  const [totalBudgetChangePct, setTotalBudgetChangePct] = useState(0)
  const [recommendationObjective, setRecommendationObjective] = useState<
    'protect_efficiency' | 'grow_conversions' | 'hit_target_roas'
  >('protect_efficiency')
  const [activeSavedScenario, setActiveSavedScenario] = useState<SavedBudgetScenario | null>(null)
  const [optimalMix, setOptimalMix] = useState<Record<string, number> | null>(null)
  const [optimalUplift, setOptimalUplift] = useState<number | null>(null)
  const [optimalPredictedKpi, setOptimalPredictedKpi] = useState<number | null>(null)
  const [optimizationMessage, setOptimizationMessage] = useState<string | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)
  const [whatIfResult, setWhatIfResult] = useState<WhatIfResult | null>(null)
  const [isWhatIfLoading, setIsWhatIfLoading] = useState(false)
  const [showContextPanel, setShowContextPanel] = usePersistentToggle('budget-optimizer:show-context', false)
  const [showEvidencePanel, setShowEvidencePanel] = usePersistentToggle('budget-optimizer:show-evidence', false)
  const [showRealizationPanel, setShowRealizationPanel] = usePersistentToggle('budget-optimizer:show-realization', false)
  const isDatasetReadoutOnly = datasetAvailable === false
  const isBudgetReadoutOnly = isDatasetReadoutOnly || budgetActionsAvailable === false
  const readoutOnlyReason =
    budgetReadoutReason ||
    (isDatasetReadoutOnly
      ? 'The linked MMM dataset is unavailable, so new budget scenarios cannot be created from this run.'
      : 'This MMM run is available as a readout, but it needs to be refreshed before creating new budget decisions.')

  const totalBudgetMultiplier =
    totalBudgetMode === 'constant' ? 1.0 : 1.0 + totalBudgetChangePct / 100

  const recommendationsQuery = useQuery<BudgetRecommendationsResponse>({
    queryKey: ['budget-recommendations', runId, recommendationObjective, totalBudgetChangePct],
    queryFn: async () => {
      if (!runId) {
        return {
          run_id: '',
          objective: recommendationObjective,
          recommendations: [],
          decision: { status: 'blocked', blockers: ['Model run missing'] },
          summary: {
            total_budget_change_pct: totalBudgetChangePct,
            baseline_spend_total: 0,
            channels_considered: 0,
            periods: 0,
            weighted_roi: 0,
          },
        }
      }
      return apiGetJson<BudgetRecommendationsResponse>(
        `/api/models/${runId}/budget/recommendations?objective=${encodeURIComponent(
          recommendationObjective
        )}&total_budget_change_pct=${totalBudgetChangePct}`,
        { fallbackMessage: 'Failed to load budget recommendations' }
      )
    },
    enabled: !!runId,
  })

  const savedScenariosQuery = useQuery<{ items: SavedBudgetScenario[]; total: number }>({
    queryKey: ['budget-scenarios', runId],
    queryFn: async () => {
      if (!runId) return { items: [], total: 0 }
      return apiGetJson<{ items: SavedBudgetScenario[]; total: number }>(
        `/api/models/${runId}/budget/scenarios`,
        { fallbackMessage: 'Failed to load saved budget scenarios' }
      )
    },
    enabled: !!runId,
  })

  const campaignSummaryQuery = useQuery<CampaignSummaryItem[]>({
    queryKey: ['budget-campaign-summary', runId],
    queryFn: async () => {
      if (!runId) return []
      return apiGetJson<CampaignSummaryItem[]>(`/api/models/${runId}/summary/campaign`, {
        fallbackMessage: 'Failed to load campaign summary',
      }).catch(() => [])
    },
    enabled: !!runId,
  })

  const realizationQuery = useQuery<BudgetRealizationResponse>({
    queryKey: ['budget-realization', runId],
    queryFn: async () => {
      if (!runId) return { items: [], total: 0 }
      return apiGetJson<BudgetRealizationResponse>(`/api/models/${runId}/budget/realization`, {
        fallbackMessage: 'Failed to load budget realization',
      })
    },
    enabled: !!runId,
  })

  const saveScenarioMutation = useMutation({
    mutationFn: async () => {
      if (!runId) throw new Error('Missing model run')
      if (isBudgetReadoutOnly) {
        throw new Error(readoutOnlyReason)
      }
      return apiSendJson<SavedBudgetScenario>(
        `/api/models/${runId}/budget/scenarios`,
        'POST',
        {
          objective: recommendationObjective,
          total_budget_change_pct: totalBudgetChangePct,
          multipliers,
          recommendations: recommendationsQuery.data?.recommendations ?? [],
        },
        { fallbackMessage: 'Failed to save budget scenario' }
      )
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['budget-scenarios', runId] })
    },
  })

  const createChangeRequestsMutation = useMutation({
    mutationFn: async () => {
      if (!runId || !topRecommendation) throw new Error('Recommendation is unavailable')
      const existingScenario =
        activeSavedScenario ??
        savedScenariosQuery.data?.items?.find(
          (item) =>
            item.objective === recommendationObjective &&
            Number(item.total_budget_change_pct || 0) === totalBudgetChangePct
        ) ?? null
      const scenario =
        existingScenario ?? (await saveScenarioMutation.mutateAsync())
      const recommendation =
        scenario.recommendations.find((item) => item.id === topRecommendation.id) ??
        scenario.recommendations[0]
      if (!recommendation) throw new Error('Saved recommendation is unavailable')

      const targets = recommendation.actions
        .filter((action) => action.action !== 'hold')
        .flatMap((action) => {
          const provider = providerFromBudgetChannel(action.channel)
          if (!provider) return []
          const channelCampaigns = (campaignSummaryQuery.data ?? [])
            .filter((row) => row.channel === action.channel)
            .sort((a, b) => (Number(b.roi ?? 0) * Number(b.mean_spend ?? 0)) - (Number(a.roi ?? 0) * Number(a.mean_spend ?? 0)))
            .slice(0, 2)
          return channelCampaigns.map((campaign) => ({
            channel: action.channel,
            provider,
            entity_id: campaign.campaign,
            entity_name: campaign.campaign,
            delta_pct: Number(action.delta_pct ?? 0),
            reason: action.reason ?? recommendation.summary,
          }))
        })

      if (!targets.length) {
        throw new Error('No campaign targets are available for this recommendation yet')
      }
      return createAdsChangeRequestsFromBudgetRecommendation({
        runId,
        scenarioId: scenario.id,
        recommendationId: recommendation.id,
        targets,
      })
    },
    onSuccess: async (_, _vars, _ctx) => {
      await queryClient.invalidateQueries({ queryKey: ['budget-realization', runId] })
      await queryClient.invalidateQueries({ queryKey: ['budget-scenarios', runId] })
    },
  })

  const { data: dataset = [] } = useQuery<Record<string, unknown>[]>({
    queryKey: ['dataset-preview', datasetId],
    queryFn: async () => {
      if (!datasetId) return []
      const d = await apiGetJson<any>(`/api/datasets/${datasetId}?preview_only=false`, {
        fallbackMessage: 'Failed to load dataset preview',
      }).catch(() => ({ preview_rows: [] }))
      return d.preview_rows || []
    },
    enabled: !!datasetId && datasetAvailable,
  })

  const channelList = useMemo(() => roiData.map((r) => r.channel), [roiData])

  const datasetDateCoverage = useMemo(() => {
    const dates = dataset
      .map((row) => String(row.date ?? '').slice(0, 10))
      .filter((value) => value.length === 10)
      .sort()
    if (!dates.length) return 'No dated preview rows'
    return `${dates[0]} – ${dates[dates.length - 1]}`
  }, [dataset])

  const baselineSpendByChannel = useMemo(() => {
    const out: Record<string, number> = {}
    const chs = channelList
    if (isTallMmmDataset(dataset)) {
      for (const row of dataset) {
        const channel = String(row.channel ?? '')
        if (!chs.includes(channel)) continue
        out[channel] = (out[channel] || 0) + (Number(row.spend) || 0)
      }
      return out
    }
    for (const row of dataset) {
      for (const ch of chs) {
        out[ch] = (out[ch] || 0) + (Number(row[ch]) || 0)
      }
    }
    return out
  }, [dataset, channelList])

  const observedSpendRangeByChannel = useMemo(() => {
    const out: Record<string, { min: number; max: number }> = {}
    const chs = channelList
    if (isTallMmmDataset(dataset)) {
      const spendByDateChannel = new Map<string, number>()
      for (const row of dataset) {
        const channel = String(row.channel ?? '')
        if (!chs.includes(channel)) continue
        const date = String(row.date ?? '')
        const key = `${date}|||${channel}`
        spendByDateChannel.set(key, (spendByDateChannel.get(key) || 0) + (Number(row.spend) || 0))
      }
      for (const ch of chs) {
        const values = Array.from(spendByDateChannel.entries())
          .filter(([key]) => key.endsWith(`|||${ch}`))
          .map(([, value]) => value)
          .filter((value) => value > 0)
        out[ch] = {
          min: values.length ? Math.min(...values) : 0,
          max: values.length ? Math.max(...values) : 0,
        }
      }
      return out
    }
    for (const ch of chs) {
      const values = dataset.map((row) => Number(row[ch]) || 0).filter((v) => v > 0)
      out[ch] = {
        min: values.length ? Math.min(...values) : 0,
        max: values.length ? Math.max(...values) : 0,
      }
    }
    return out
  }, [dataset, channelList])

  const totalBaselineSpend = useMemo(
    () => Object.values(baselineSpendByChannel).reduce((a, b) => a + b, 0),
    [baselineSpendByChannel]
  )

  const contribMap = useMemo(
    () => Object.fromEntries(contribData.map((c) => [c.channel, c.mean_share])),
    [contribData]
  )
  const contributionValueMap = useMemo(
    () =>
      Object.fromEntries(
        contribData.map((c) => [
          c.channel,
          Number.isFinite(Number(c.mean_contribution)) ? Number(c.mean_contribution) : undefined,
        ]),
      ),
    [contribData],
  )
  const roiMap = useMemo(() => Object.fromEntries(roiData.map((r) => [r.channel, r.roi])), [roiData])

  const baselineKpiValue = baselineKPI

  const baselineScore = useMemo(() => {
    let s = 0
    for (const ch of channelList) {
      const contributionValue = contributionValueMap[ch]
      s += contributionValue ?? (roiMap[ch] ?? 0) * (contribMap[ch] ?? 0)
    }
    return s
  }, [channelList, roiMap, contribMap, contributionValueMap])

  const predictedScore = useMemo(() => {
    let s = 0
    for (const ch of channelList) {
      const mult = constraints[ch]?.locked ? 1.0 : (multipliers[ch] ?? 1.0)
      const contributionValue = contributionValueMap[ch]
      s += (contributionValue ?? (roiMap[ch] ?? 0) * (contribMap[ch] ?? 0)) * mult
    }
    return s
  }, [channelList, roiMap, contribMap, contributionValueMap, multipliers, constraints])

  const predictedKPI = baselineScore > 0 ? (predictedScore / baselineScore) * baselineKpiValue : 0
  const upliftPercent = baselineKpiValue ? ((predictedKPI - baselineKpiValue) / baselineKpiValue) * 100 : 0

  const newSpendByChannel = useMemo(() => {
    const out: Record<string, number> = {}
    for (const ch of channelList) {
      const base = baselineSpendByChannel[ch] ?? 0
      const mult = constraints[ch]?.locked ? 1.0 : (multipliers[ch] ?? 1.0)
      out[ch] = base * mult
    }
    return out
  }, [channelList, baselineSpendByChannel, multipliers, constraints])

  const extrapolationWarnings = useMemo(() => {
    const warnings: string[] = []
    for (const ch of channelList) {
      const range = observedSpendRangeByChannel[ch]
      const newSpend = newSpendByChannel[ch] ?? 0
      if (range.max > 0 && newSpend > range.max * 1.01) {
        warnings.push(`${ch}: new spend ${formatCurrency(newSpend)} is above observed max ${formatCurrency(range.max)}`)
      }
      if (range.min > 0 && newSpend < range.min * 0.99) {
        warnings.push(`${ch}: new spend ${formatCurrency(newSpend)} is below observed min ${formatCurrency(range.min)}`)
      }
    }
    return warnings
  }, [channelList, newSpendByChannel, observedSpendRangeByChannel])

  const marginalKpiPer1k = useMemo(() => {
    return channelList
      .map((ch) => {
        const roi = roiMap[ch] ?? 0
        const baseSpend = baselineSpendByChannel[ch] ?? 0
        const kpiPer1k = baseSpend > 0 ? (roi * 1000) : roi * 1000
        return { channel: ch, marginalKpiPer1k: kpiPer1k, roi }
      })
      .sort((a, b) => b.marginalKpiPer1k - a.marginalKpiPer1k)
  }, [channelList, roiMap, baselineSpendByChannel])

  const handleSliderChange = (channel: string, value: number) => {
    setActiveSavedScenario(null)
    setMultipliers((prev) => ({ ...prev, [channel]: value }))
  }

  const handleResetChannel = (channel: string) => {
    setActiveSavedScenario(null)
    setMultipliers((prev) => ({ ...prev, [channel]: 1.0 }))
  }

  const handleResetAll = () => {
    setActiveSavedScenario(null)
    setMultipliers(channelList.reduce((acc, ch) => ({ ...acc, [ch]: 1.0 }), {}))
    setOptimalMix(null)
    setOptimalUplift(null)
    setOptimalPredictedKpi(null)
    setOptimizationMessage(null)
    setTotalBudgetMode('constant')
    setTotalBudgetChangePct(0)
    setWhatIfResult(null)
  }

  const updateConstraint = (channel: string, patch: Partial<ChannelConstraint>) => {
    setConstraints((prev) => ({
      ...prev,
      [channel]: { ...(prev[channel] ?? { min: 0.5, max: 2.0, locked: false }), ...patch },
    }))
  }

  const handleSuggestOptimal = async () => {
    if (!runId || isBudgetReadoutOnly) return
    setIsOptimizing(true)
    setOptimalMix(null)
    setOptimalUplift(null)
    setOptimizationMessage(null)
    try {
      const channel_constraints: Record<string, { min?: number; max?: number; locked?: boolean }> = {}
      channelList.forEach((ch) => {
        const c = constraints[ch] ?? { min: 0.5, max: 2.0, locked: false }
        channel_constraints[ch] = { min: c.min, max: c.max, locked: c.locked }
      })
      const data = await apiSendJson<any>(`/api/models/${runId}/optimize/auto`, 'POST', {
        total_budget: totalBudgetMultiplier,
        min_spend: 0.5,
        max_spend: 2.0,
        channel_constraints,
      }, {
        fallbackMessage: 'Optimization failed',
      })
      setOptimalMix(data.optimal_mix ?? null)
      setOptimalUplift(data.uplift ?? null)
      setOptimalPredictedKpi(data.predicted_kpi ?? null)
      setOptimizationMessage(data.message ?? 'Moves budget from low marginal ROI to high marginal ROI channels.')
    } catch (e) {
      console.error(e)
      setOptimizationMessage('Optimization failed. Try relaxing constraints.')
    } finally {
      setIsOptimizing(false)
    }
  }

  const handleApplyOptimal = () => {
    if (optimalMix) {
      setMultipliers(optimalMix)
    }
  }

  const handleRunWhatIf = async () => {
    if (!runId || isBudgetReadoutOnly) return
    setIsWhatIfLoading(true)
    setWhatIfResult(null)
    try {
      const data = await apiSendJson<WhatIfResult>(`/api/models/${runId}/what_if`, 'POST', multipliers, {
        fallbackMessage: 'What-if failed',
      })
      setWhatIfResult(data)
    } catch (e) {
      console.error(e)
    } finally {
      setIsWhatIfLoading(false)
    }
  }

  const handleExportScenario = () => {
    const rows = channelList.map((ch) => {
      const base = baselineSpendByChannel[ch] ?? 0
      const mult = multipliers[ch] ?? 1.0
      const newSpend = base * mult
      return {
        channel: ch,
        baseline_spend: base,
        new_spend: newSpend,
        multiplier: mult,
        delta_spend: newSpend - base,
      }
    })
    const csvHeaders = ['channel', 'baseline_spend', 'new_spend', 'multiplier', 'delta_spend']
    const csvRows = [
      csvHeaders.join(','),
      ...rows.map((r) =>
        [r.channel, r.baseline_spend, r.new_spend, r.multiplier, r.delta_spend].join(',')
      ),
    ]
    const summary = [
      '',
      `Predicted KPI (indexed),${predictedKPI.toFixed(2)}`,
      `Uplift %,${upliftPercent >= 0 ? '+' : ''}${upliftPercent.toFixed(1)}%`,
    ]
    const blob = new Blob(
      [csvRows.join('\n') + '\n' + summary.join('\n')],
      { type: 'text/csv;charset=utf-8' }
    )
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `mmm-budget-scenario-${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const recommendedSpendByChannel = useMemo(() => {
    if (!optimalMix) return null
    const out: Record<string, number> = {}
    channelList.forEach((ch) => {
      const base = baselineSpendByChannel[ch] ?? 0
      out[ch] = base * (optimalMix[ch] ?? 1)
    })
    return out
  }, [optimalMix, channelList, baselineSpendByChannel])

  const activeSavedRecommendation = activeSavedScenario?.recommendations?.[0] ?? null
  const isUsingSavedScenario = !!activeSavedScenario && !!activeSavedRecommendation
  const topRecommendation = activeSavedRecommendation ?? recommendationsQuery.data?.recommendations?.[0] ?? null
  const recommendationSummary = isUsingSavedScenario
    ? {
        periods: 0,
        channels_considered: Object.keys(activeSavedScenario?.multipliers || {}).length,
        weighted_roi: 0,
      }
    : recommendationsQuery.data?.summary
  const canUseDatasetBackedActions =
    !!runId &&
    !isBudgetReadoutOnly &&
    (isUsingSavedScenario || recommendationsQuery.data?.decision?.status !== 'blocked') &&
    !!topRecommendation
  const savedScenarios = useMemo(
    () =>
      (savedScenariosQuery.data?.items ?? [])
        .slice()
        .sort((a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime()),
    [savedScenariosQuery.data?.items],
  )
  const latestScenario = savedScenarios[0] ?? null
  const savedScenariosToShow = savedScenarios.slice(0, 6)
  const rolloutRecords = useMemo(
    () =>
      (realizationQuery.data?.items ?? [])
        .slice()
        .sort((a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime()),
    [realizationQuery.data?.items],
  )
  const latestRollout = rolloutRecords[0] ?? null
  const rolloutRecordsToShow = rolloutRecords.slice(0, 6)

  const budgetTrustNarrative = useMemo(() => {
    const modeledPeriods = recommendationsQuery.data?.summary.periods ?? 0
    const savedScenarioCount = savedScenariosQuery.data?.total ?? 0
    const realizationCount = realizationQuery.data?.total ?? 0
    const hasLargeExtrapolation = extrapolationWarnings.length > 0
    const headline = isBudgetReadoutOnly
      ? isDatasetReadoutOnly
        ? 'This MMM run is available as a saved readout, but new budget recommendations need the missing source dataset.'
        : 'This MMM run is available as a saved readout, but new budget recommendations need a refreshed model run.'
      : topRecommendation
      ? hasLargeExtrapolation
        ? 'Budget guidance is usable, but the active scenario is stretching beyond observed spend history.'
        : 'Budget guidance is grounded in the active MMM run and is strongest when you stay inside observed spend ranges.'
      : 'Budget guidance is not ready yet because there is no active recommendation for this run.'

    const items = [
      isDatasetReadoutOnly
        ? 'Basis: saved ROI, contribution, scenarios, and rollout records are still readable, but the linked dataset preview is unavailable in this runtime.'
        : isBudgetReadoutOnly
        ? `Basis: saved ROI, contribution, scenarios, and rollout records remain readable. ${readoutOnlyReason}`
        : `Basis: this workspace uses the selected MMM run plus ${dataset.length.toLocaleString()} linked dataset preview rows across ${datasetDateCoverage}.`,
      isDatasetReadoutOnly
        ? 'Recommendation engine basis: unavailable until the source MMM dataset is rebuilt or reattached.'
        : isBudgetReadoutOnly
        ? 'Recommendation engine basis: paused for this run until a compatible replacement run is created.'
        : modeledPeriods > 0
        ? `Recommendation engine basis: ${modeledPeriods.toLocaleString()} modeled periods across ${channelList.length.toLocaleString()} channels.`
        : 'Recommendation engine basis is still unavailable, so treat this page as setup and manual planning only.',
      isDatasetReadoutOnly
        ? 'Do not use manual sliders or optimizer output for action from this run; create a compatible new run first.'
        : isBudgetReadoutOnly
        ? 'Do not create new budget actions from this readout; use it only to review prior scenarios and rollout evidence.'
        : topRecommendation
        ? `Current top recommendation is ${topRecommendation.confidence.band.toLowerCase()} confidence with ${topRecommendation.risk.extrapolation.toLowerCase()} extrapolation risk.`
        : 'No top recommendation is available yet, so manual budget editing should be treated as exploratory only.',
      isDatasetReadoutOnly
        ? 'Observed spend ranges are unavailable, so extrapolation guardrails cannot be evaluated.'
        : isBudgetReadoutOnly
        ? 'Observed spend ranges may be visible, but optimizer guardrails are disabled until the run is refreshed.'
        : hasLargeExtrapolation
        ? `Primary trust risk: ${extrapolationWarnings.length.toLocaleString()} channel scenario${extrapolationWarnings.length === 1 ? '' : 's'} exceed observed spend ranges.`
        : 'Current scenario stays inside observed historical spend ranges, so the optimizer is not extrapolating aggressively.',
      savedScenarioCount > 0 || realizationCount > 0
        ? `Continuity: ${savedScenarioCount.toLocaleString()} saved scenario${savedScenarioCount === 1 ? '' : 's'} and ${realizationCount.toLocaleString()} rollout record${realizationCount === 1 ? '' : 's'} are available for this run.`
        : 'Continuity: no saved scenarios or rollout records exist yet for this run.',
    ]
    return { headline, items }
  }, [
    channelList.length,
    dataset.length,
    datasetDateCoverage,
    isBudgetReadoutOnly,
    isDatasetReadoutOnly,
    readoutOnlyReason,
    extrapolationWarnings.length,
    realizationQuery.data?.total,
    recommendationsQuery.data?.summary.periods,
    savedScenariosQuery.data?.total,
    topRecommendation,
  ])

  const applyRecommendation = (recommendation: BudgetRecommendationItem) => {
    const next = channelList.reduce((acc, ch) => ({ ...acc, [ch]: 1.0 }), {} as Record<string, number>)
    recommendation.actions.forEach((action) => {
      if (!action.channel) return
      next[action.channel] = 1 + Number(action.delta_pct ?? 0)
    })
    setMultipliers(next)
    setOptimalMix(null)
    setOptimalUplift(null)
    setOptimalPredictedKpi(null)
    setOptimizationMessage(
      `${recommendation.title}: ${recommendation.summary}`
    )
  }

  const loadSavedScenario = (scenario: SavedBudgetScenario) => {
    setActiveSavedScenario(scenario)
    setMultipliers({
      ...channelList.reduce((acc, ch) => ({ ...acc, [ch]: 1.0 }), {} as Record<string, number>),
      ...(scenario.multipliers || {}),
    })
    setRecommendationObjective(
      (scenario.objective as 'protect_efficiency' | 'grow_conversions' | 'hit_target_roas') ||
        'protect_efficiency'
    )
    setTotalBudgetChangePct(Number(scenario.total_budget_change_pct || 0))
    setTotalBudgetMode(
      Number(scenario.total_budget_change_pct || 0) === 0 ? 'constant' : 'change'
    )
    setOptimalMix(null)
    setOptimalUplift(null)
    setOptimalPredictedKpi(null)
    setOptimizationMessage('Loaded saved scenario')
  }
  const budgetSourceLabel = isBudgetReadoutOnly
    ? 'MMM run readout only'
    : runId
      ? 'MMM model run + dataset preview'
      : 'Awaiting model run'
  const observedRowsLabel = isDatasetReadoutOnly ? 'Unavailable' : dataset.length.toLocaleString()
  const baselineSpendLabel = isDatasetReadoutOnly ? 'Unavailable' : formatCurrency(totalBaselineSpend)
  const modeledPeriodsLabel = isDatasetReadoutOnly
    ? 'Unavailable'
    : recommendationsQuery.data?.summary.periods != null
      ? recommendationsQuery.data.summary.periods.toLocaleString()
      : '—'

  return (
    <div
      style={{
        marginTop: t.space.xxl,
        display: 'grid',
        gap: t.space.xl,
      }}
    >
      <div>
      <h2 style={{ marginTop: 0, marginBottom: t.space.sm, fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text }}>
        Budget actions
      </h2>
      <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xl }}>
        Turn MMM output into a budget plan. Start from the model-backed recommendation, then refine the allocation against observed spend ranges and rollout guardrails.
      </p>
      </div>

      <div style={{ marginBottom: t.space.lg }}>
        <ContextSummaryStrip
          items={[
            { label: 'Source', value: budgetSourceLabel },
            { label: 'Observed rows', value: observedRowsLabel },
            { label: 'Channels', value: channelList.length.toLocaleString() },
            { label: 'Baseline spend', value: baselineSpendLabel },
            { label: 'Modeled periods', value: modeledPeriodsLabel },
          ]}
        />
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <AnalysisShareActions
          fileStem="mmm-budget-actions"
          summaryTitle="MMM budget actions"
          summaryLines={[
            `Objective: ${recommendationObjective.replace(/_/g, ' ')}`,
            `Baseline spend: ${baselineSpendLabel}`,
            `Channels considered: ${channelList.length.toLocaleString()}`,
            `Observed rows: ${observedRowsLabel}`,
            `Current predicted uplift: ${upliftPercent >= 0 ? '+' : ''}${upliftPercent.toFixed(1)}%`,
            topRecommendation ? `Top recommendation: ${topRecommendation.title}` : 'Top recommendation: unavailable',
          ]}
        />
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <AnalysisNarrativePanel
          title="What to trust"
          subtitle="A short readout of what the budget workspace is using before you act on recommendations or rollout proposals."
          headline={budgetTrustNarrative.headline}
          items={budgetTrustNarrative.items}
        />
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <SectionCard
          title="Reconciliation basis"
          subtitle="What this budget workspace is anchored to, and what counts as a reusable prior result."
        >
          <div style={{ display: 'grid', gap: t.space.md }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {isDatasetReadoutOnly
                ? 'This workspace is showing saved MMM readouts, scenarios, and rollout realization for a historical run whose linked dataset is unavailable. Rebuild a compatible dataset before creating new optimizer recommendations.'
                : isBudgetReadoutOnly
                ? `This workspace is showing saved MMM readouts, scenarios, and rollout realization, but new optimizer actions are paused. ${readoutOnlyReason}`
                : 'Budget actions here are derived from the active MMM run and its linked dataset preview. Saved scenarios and rollout realization stay tied to that run, so reopening prior work preserves the same model basis instead of forcing a new run setup.'}
            </div>
            <div
              style={{
                display: 'grid',
                gap: t.space.sm,
                gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
              }}
            >
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>MMM run</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>{runId ? `${runId.slice(0, 8)}…` : '—'}</strong>
                </div>
              </div>
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Dataset coverage</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>{datasetDateCoverage}</strong>
                </div>
              </div>
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Saved scenarios</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>{(savedScenariosQuery.data?.total ?? 0).toLocaleString()}</strong>
                </div>
              </div>
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Rollout tracking</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>{(realizationQuery.data?.total ?? 0).toLocaleString()}</strong> tracked realization {((realizationQuery.data?.total ?? 0) === 1) ? 'record' : 'records'}
                </div>
              </div>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard
        title="Resume budget work"
        subtitle="Saved scenarios and rollout tracking stay tied to this MMM run, so prior optimizer work is directly reusable."
        actions={
          <button
            type="button"
            onClick={() => {
              void savedScenariosQuery.refetch()
              void realizationQuery.refetch()
            }}
            style={{
              padding: `${t.space.sm}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Refresh work
          </button>
        }
      >
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: t.space.md }}>
          <div
            style={{
              padding: t.space.lg,
              borderRadius: t.radius.md,
              border: `1px solid ${latestScenario ? t.color.accent : t.color.borderLight}`,
              background: latestScenario ? t.color.accentMuted : t.color.bg,
              display: 'grid',
              gap: t.space.sm,
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                Latest saved scenario
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                {savedScenarios.length.toLocaleString()} total
              </div>
            </div>
            {latestScenario ? (
              <>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {latestScenario.objective.replace(/_/g, ' ')}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Saved {latestScenario.created_at ? new Date(latestScenario.created_at).toLocaleString() : 'recently'} · budget{' '}
                  {Number(latestScenario.total_budget_change_pct || 0) >= 0 ? '+' : ''}
                  {Number(latestScenario.total_budget_change_pct || 0).toFixed(0)}%
                </div>
                <button
                  type="button"
                  onClick={() => loadSavedScenario(latestScenario)}
                  style={{
                    justifySelf: 'start',
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.accent,
                    color: '#ffffff',
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightSemibold,
                    cursor: 'pointer',
                  }}
                >
                  Load scenario
                </button>
              </>
            ) : (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                No saved budget scenarios yet. Save the model-backed recommendation or a manual allocation to reuse it later.
              </div>
            )}
          </div>

          <div
            style={{
              padding: t.space.lg,
              borderRadius: t.radius.md,
              border: `1px solid ${latestRollout ? t.color.success : t.color.borderLight}`,
              background: latestRollout ? t.color.successMuted : t.color.bg,
              display: 'grid',
              gap: t.space.sm,
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                Latest rollout tracking
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                {rolloutRecords.length.toLocaleString()} total
              </div>
            </div>
            {latestRollout ? (
              <>
                <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                  {latestRollout.objective.replace(/_/g, ' ')}
                </div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  {latestRollout.execution.execution_progress_pct.toFixed(0)}% applied · {latestRollout.execution.counts.pending_approval} pending ·{' '}
                  {formatCurrency(latestRollout.execution.applied_budget_delta_total)} moved
                </div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  Projected realized lift: +{latestRollout.execution.projected_realized_lift_pct.toFixed(1)}%
                </div>
              </>
            ) : (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                No rollout tracking yet. Create change requests from a recommendation to start execution tracking.
              </div>
            )}
          </div>
        </div>
        {savedScenariosToShow.length > 1 || rolloutRecordsToShow.length > 1 ? (
          <div
            style={{
              marginTop: t.space.lg,
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
              gap: t.space.md,
            }}
          >
            {savedScenariosToShow.length > 1 ? (
              <div
                style={{
                  padding: t.space.lg,
                  borderRadius: t.radius.md,
                  border: `1px solid ${t.color.borderLight}`,
                  background: t.color.surface,
                  display: 'grid',
                  gap: t.space.sm,
                }}
              >
                <div>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    Saved scenario history
                  </div>
                  <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    Load any prior allocation without rerunning MMM.
                  </div>
                </div>
                <div style={{ display: 'grid', gap: t.space.xs }}>
                  {savedScenariosToShow.map((scenario, idx) => {
                    const createdAt = scenario.created_at
                      ? new Date(scenario.created_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
                      : 'Saved scenario'
                    return (
                      <div
                        key={scenario.id}
                        style={{
                          display: 'grid',
                          gridTemplateColumns: 'minmax(0, 1fr) auto',
                          gap: t.space.md,
                          alignItems: 'center',
                          padding: t.space.sm,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${idx === 0 ? t.color.accent : t.color.borderLight}`,
                          background: idx === 0 ? t.color.accentMuted : t.color.bg,
                        }}
                      >
                        <div style={{ minWidth: 0 }}>
                          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text, textTransform: 'capitalize' }}>
                            {scenario.objective.replace(/_/g, ' ')}
                          </div>
                          <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                            {createdAt} · budget {Number(scenario.total_budget_change_pct || 0) >= 0 ? '+' : ''}
                            {Number(scenario.total_budget_change_pct || 0).toFixed(0)}% · {(scenario.recommendations || []).length.toLocaleString()} recommendation{(scenario.recommendations || []).length === 1 ? '' : 's'}
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={() => loadSavedScenario(scenario)}
                          style={{
                            padding: `${t.space.xs}px ${t.space.sm}px`,
                            borderRadius: t.radius.sm,
                            border: `1px solid ${t.color.border}`,
                            background: t.color.surface,
                            color: t.color.accent,
                            fontSize: t.font.sizeXs,
                            fontWeight: t.font.weightSemibold,
                            cursor: 'pointer',
                            whiteSpace: 'nowrap',
                          }}
                        >
                          Load
                        </button>
                      </div>
                    )
                  })}
                </div>
              </div>
            ) : null}

            {rolloutRecordsToShow.length > 1 ? (
              <div
                style={{
                  padding: t.space.lg,
                  borderRadius: t.radius.md,
                  border: `1px solid ${t.color.borderLight}`,
                  background: t.color.surface,
                  display: 'grid',
                  gap: t.space.sm,
                }}
              >
                <div>
                  <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    Rollout history
                  </div>
                  <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                    Recent execution records attached to this MMM run.
                  </div>
                </div>
                <div style={{ display: 'grid', gap: t.space.xs }}>
                  {rolloutRecordsToShow.map((record, idx) => (
                    <div
                      key={`${record.scenario_id}:${record.created_at ?? idx}`}
                      style={{
                        padding: t.space.sm,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${idx === 0 ? t.color.success : t.color.borderLight}`,
                        background: idx === 0 ? t.color.successMuted : t.color.bg,
                        display: 'grid',
                        gap: 2,
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'baseline' }}>
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text, textTransform: 'capitalize' }}>
                          {record.objective.replace(/_/g, ' ')}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          {record.execution.execution_progress_pct.toFixed(0)}% applied
                        </div>
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {record.created_at ? new Date(record.created_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : 'Tracked rollout'} · {record.execution.counts.total} request{record.execution.counts.total === 1 ? '' : 's'} · {formatCurrency(record.execution.applied_budget_delta_total)} moved
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        ) : null}
      </SectionCard>

      <div style={{ marginBottom: t.space.xl }}>
        <CollapsiblePanel
          title="Scenario caveats"
          subtitle="What can make a recommendation less reliable once you push beyond the observed range."
          open={showContextPanel}
          onToggle={() => setShowContextPanel((value) => !value)}
        >
          <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <div>
              Manual sliders scale each channel relative to observed baseline spend, so larger jumps are inherently more speculative than the top model-guided recommendation.
            </div>
            <div>
              Baseline spend{' '}
              <strong style={{ color: t.color.text }}>{formatCurrency(totalBaselineSpend)}</strong> · weighted ROI{' '}
              <strong style={{ color: t.color.text }}>{(recommendationsQuery.data?.summary.weighted_roi ?? 0).toFixed(2)}</strong>
            </div>
            <div>
              Modeled periods <strong style={{ color: t.color.text }}>{(recommendationsQuery.data?.summary.periods ?? 0).toLocaleString()}</strong>
              {' · '}channels considered <strong style={{ color: t.color.text }}>{channelList.length.toLocaleString()}</strong>
            </div>
            <div>
              Trust layer: recommendations are strongest inside the observed spend range. Extrapolation warnings appear when the scenario pushes outside those historical bounds.
            </div>
          </div>
        </CollapsiblePanel>
      </div>

      <SectionCard
        title="Recommended budget actions"
        subtitle="Start with the model-backed plan, then refine only when the business constraint needs a custom allocation."
        actions={
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
            <select
              value={recommendationObjective}
              onChange={(e) => {
                setActiveSavedScenario(null)
                setRecommendationObjective(
                  e.target.value as 'protect_efficiency' | 'grow_conversions' | 'hit_target_roas'
                )
              }}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                fontSize: t.font.sizeSm,
                background: t.color.surface,
              }}
            >
              <option value="protect_efficiency">Protect efficiency</option>
              <option value="grow_conversions">Grow conversions</option>
              <option value="hit_target_roas">Hit target ROAS</option>
            </select>
            <button
              type="button"
              onClick={() => saveScenarioMutation.mutate()}
              disabled={!canUseDatasetBackedActions || saveScenarioMutation.isPending}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.accent}`,
                background: 'transparent',
                color: t.color.accent,
                cursor: !canUseDatasetBackedActions || saveScenarioMutation.isPending ? 'not-allowed' : 'pointer',
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
              }}
            >
              {saveScenarioMutation.isPending ? 'Saving…' : 'Save scenario'}
            </button>
          </div>
        }
      >
        {activeSavedScenario ? (
          <div
            style={{
              marginBottom: t.space.md,
              padding: t.space.md,
              borderRadius: t.radius.md,
              border: `1px solid ${t.color.accent}`,
              background: t.color.accentMuted,
              display: 'flex',
              justifyContent: 'space-between',
              gap: t.space.md,
              alignItems: 'center',
              flexWrap: 'wrap',
            }}
          >
            <div style={{ display: 'grid', gap: 2 }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                Viewing saved scenario
              </div>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                {activeSavedScenario.objective.replace(/_/g, ' ')} · saved{' '}
                {activeSavedScenario.created_at
                  ? new Date(activeSavedScenario.created_at).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
                  : 'previously'} · {(activeSavedScenario.recommendations || []).length.toLocaleString()} saved recommendation{(activeSavedScenario.recommendations || []).length === 1 ? '' : 's'}
              </div>
            </div>
            <button
              type="button"
              onClick={() => setActiveSavedScenario(null)}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                color: t.color.textSecondary,
                fontSize: t.font.sizeXs,
                fontWeight: t.font.weightSemibold,
                cursor: 'pointer',
              }}
            >
              Return to live recommendation
            </button>
          </div>
        ) : null}
        {!activeSavedScenario && recommendationsQuery.data?.decision && (
          <DecisionStatusCard
            title="Budget recommendation status"
            status={recommendationsQuery.data.decision.status ?? undefined}
            subtitle={recommendationsQuery.data.decision.subtitle ?? undefined}
            blockers={recommendationsQuery.data.decision.blockers ?? []}
            warnings={recommendationsQuery.data.decision.warnings ?? []}
            actions={recommendationsQuery.data.decision.actions ?? []}
            onActionClick={(action) =>
              onRecommendedActionClick
                ? onRecommendedActionClick(action)
                : navigateForRecommendedAction(action, { defaultPage: 'mmm' })
            }
          />
        )}
        {saveScenarioMutation.isError ? (
          <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeXs, color: t.color.danger }}>
            {(saveScenarioMutation.error as Error).message}
          </div>
        ) : null}
        {saveScenarioMutation.isSuccess ? (
          <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeXs, color: t.color.success }}>
            Scenario saved and available in Resume budget work.
          </div>
        ) : null}

        {!isUsingSavedScenario && recommendationsQuery.isLoading ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Loading recommendations…
          </div>
        ) : topRecommendation ? (
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'minmax(0, 1.2fr) minmax(320px, 0.8fr)',
              gap: t.space.lg,
            }}
          >
            <div
              style={{
                padding: t.space.lg,
                borderRadius: t.radius.md,
                border: `1px solid ${t.color.borderLight}`,
                background: t.color.surface,
                display: 'grid',
                gap: t.space.md,
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
                <div>
                  <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {topRecommendation.title}
                  </div>
                  <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    {topRecommendation.summary}
                  </div>
                </div>
                <div
                  style={{
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    borderRadius: t.radius.full,
                    background: topRecommendation.status === 'ready' ? t.color.successMuted : t.color.warningSubtle,
                    color: topRecommendation.status === 'ready' ? t.color.success : t.color.warning,
                    border: `1px solid ${topRecommendation.status === 'ready' ? t.color.success : t.color.warning}`,
                    fontSize: t.font.sizeXs,
                    fontWeight: t.font.weightSemibold,
                    textTransform: 'capitalize',
                    alignSelf: 'flex-start',
                  }}
                >
                  {topRecommendation.status}
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: t.space.sm }}>
                <div style={{ padding: t.space.sm, borderRadius: t.radius.sm, background: t.color.bg, border: `1px solid ${t.color.borderLight}` }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Expected lift</div>
                  <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold }}>
                    +{topRecommendation.expected_impact.delta_pct.toFixed(1)}%
                  </div>
                </div>
                <div style={{ padding: t.space.sm, borderRadius: t.radius.sm, background: t.color.bg, border: `1px solid ${t.color.borderLight}` }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Reallocated spend</div>
                  <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold }}>
                    {formatCurrency(topRecommendation.expected_impact.reallocated_spend ?? 0)}
                  </div>
                </div>
                <div style={{ padding: t.space.sm, borderRadius: t.radius.sm, background: t.color.bg, border: `1px solid ${t.color.borderLight}` }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Confidence</div>
                  <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold, textTransform: 'capitalize' }}>
                    {topRecommendation.confidence.band}
                  </div>
                </div>
                <div style={{ padding: t.space.sm, borderRadius: t.radius.sm, background: t.color.bg, border: `1px solid ${t.color.borderLight}` }}>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Risk</div>
                  <div style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightSemibold, textTransform: 'capitalize' }}>
                    {topRecommendation.risk.extrapolation}
                  </div>
                </div>
              </div>

              <div style={{ display: 'grid', gap: t.space.sm }}>
                {topRecommendation.actions.map((action) => (
                  <div
                    key={`${topRecommendation.id}:${action.channel}:${action.action}`}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      gap: t.space.md,
                      alignItems: 'center',
                      padding: t.space.sm,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.borderLight}`,
                      background: t.color.bg,
                    }}
                  >
                    <div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                        {action.action === 'increase' ? 'Increase' : action.action === 'decrease' ? 'Decrease' : 'Hold'} {action.channel}
                      </div>
                      <div style={{ marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        {action.reason}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right', fontSize: t.font.sizeSm }}>
                      <div style={{ fontWeight: t.font.weightSemibold, color: action.action === 'increase' ? t.color.success : t.color.warning }}>
                        {Number(action.delta_pct ?? 0) >= 0 ? '+' : ''}
                        {(Number(action.delta_pct ?? 0) * 100).toFixed(1)}%
                      </div>
                      <div style={{ marginTop: 2, color: t.color.textSecondary }}>
                        {formatCurrency(action.delta_amount ?? 0)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                <button
                  type="button"
                  onClick={() => applyRecommendation(topRecommendation)}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: 'none',
                    background: t.color.accent,
                    color: '#fff',
                    cursor: 'pointer',
                    fontWeight: t.font.weightSemibold,
                  }}
                >
                  Use recommendation
                </button>
                <button
                  type="button"
                  onClick={() => saveScenarioMutation.mutate()}
                  disabled={!canUseDatasetBackedActions || saveScenarioMutation.isPending}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: 'transparent',
                    color: t.color.text,
                    cursor: !canUseDatasetBackedActions || saveScenarioMutation.isPending ? 'not-allowed' : 'pointer',
                  }}
                >
                  Save recommendation
                </button>
                <button
                  type="button"
                  onClick={() => createChangeRequestsMutation.mutate()}
                  disabled={!canUseDatasetBackedActions || createChangeRequestsMutation.isPending}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.warning}`,
                    background: 'transparent',
                    color: t.color.warning,
                    cursor: !canUseDatasetBackedActions || createChangeRequestsMutation.isPending ? 'not-allowed' : 'pointer',
                    fontWeight: t.font.weightMedium,
                  }}
                >
                  {createChangeRequestsMutation.isPending ? 'Creating proposals…' : 'Create change requests'}
                </button>
              </div>
              {createChangeRequestsMutation.isError ? (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                  {(createChangeRequestsMutation.error as Error).message}
                </div>
              ) : null}
              {createChangeRequestsMutation.isSuccess ? (
                <div style={{ fontSize: t.font.sizeXs, color: t.color.success }}>
                  Created {createChangeRequestsMutation.data.total} change request{createChangeRequestsMutation.data.total === 1 ? '' : 's'}
                  {createChangeRequestsMutation.data.skipped_total
                    ? `, skipped ${createChangeRequestsMutation.data.skipped_total}`
                    : ''}
                  .
                </div>
              ) : null}
            </div>

            <div style={{ display: 'grid', gap: t.space.md }}>
              <CollapsiblePanel
                title="Evidence"
                subtitle="Why the current recommendation is being suggested."
                open={showEvidencePanel}
                onToggle={() => setShowEvidencePanel((value) => !value)}
              >
                <div style={{ display: 'grid', gap: t.space.sm }}>
                  {topRecommendation.evidence.map((item) => (
                    <div key={item} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      {item}
                    </div>
                  ))}
                </div>
                <div style={{ marginTop: t.space.md, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  {isUsingSavedScenario ? 'Saved recommendation from prior budget work.' : 'Based on'}{' '}
                  {recommendationSummary?.periods ?? 0} modeled periods across{' '}
                  {recommendationSummary?.channels_considered ?? 0} channels.
                </div>
              </CollapsiblePanel>

              <CollapsiblePanel
                title="Rollout realization"
                subtitle="Execution progress after recommendations are turned into change requests."
                open={showRealizationPanel}
                onToggle={() => setShowRealizationPanel((value) => !value)}
              >
                {realizationQuery.data?.items?.length ? (
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    {realizationQuery.data.items.slice(0, 3).map((item) => (
                      <div
                        key={item.scenario_id}
                        style={{
                          padding: t.space.sm,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.borderLight}`,
                          background: t.color.bg,
                          display: 'grid',
                          gap: 4,
                        }}
                      >
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm }}>
                          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                            {item.objective.replace(/_/g, ' ')}
                          </div>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                            {item.execution.execution_progress_pct.toFixed(0)}% applied
                          </div>
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Requests: {item.execution.counts.total} total, {item.execution.counts.pending_approval} pending, {item.execution.counts.applied} applied
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Budget moved: {formatCurrency(item.execution.applied_budget_delta_total)} applied of {formatCurrency(item.execution.proposed_budget_delta_total)} proposed
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Projected realized lift: +{item.execution.projected_realized_lift_pct.toFixed(1)}%
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    No rollout tracking yet. Create change requests from a saved recommendation to start tracking execution.
                  </div>
                )}
              </CollapsiblePanel>
            </div>
          </div>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            No recommendation available for this run yet.
          </div>
        )}
      </SectionCard>

      {isBudgetReadoutOnly ? (
        <SectionCard
          title={isDatasetReadoutOnly ? 'Dataset-backed planning paused' : 'New budget actions paused'}
          subtitle={
            isDatasetReadoutOnly
              ? 'The historical model readout is available, but manual allocation and optimizer actions need the linked source rows.'
              : 'Prior budget work remains readable, but new optimizer actions require a refreshed MMM run.'
          }
        >
          <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <div>
              Saved ROI, contribution, scenarios, and rollout tracking remain visible above.{' '}
              {isDatasetReadoutOnly
                ? 'Baseline spend, observed spend ranges, and period-level budget checks are unavailable because the MMM dataset file is missing.'
                : readoutOnlyReason}
            </div>
            <div>
              Use <strong style={{ color: t.color.text }}>{isDatasetReadoutOnly ? 'Create compatible new run' : 'Re-run same setup'}</strong>{' '}
              from the MMM workspace to unlock new budget recommendations.
            </div>
          </div>
        </SectionCard>
      ) : (
        <>
      <SectionCard
        title="Custom allocation"
        subtitle="Adjust channel budgets manually against the modeled baseline and rollout constraints."
        actions={
          <div>
            <div style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Total budget
            </div>
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, marginBottom: t.space.xs, cursor: 'pointer', fontSize: t.font.sizeSm }}>
              <input
                type="radio"
                checked={totalBudgetMode === 'constant'}
                onChange={() => {
                  setActiveSavedScenario(null)
                  setTotalBudgetMode('constant')
                }}
              />
              Keep total constant
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, cursor: 'pointer', fontSize: t.font.sizeSm }}>
              <input
                type="radio"
                checked={totalBudgetMode === 'change'}
                onChange={() => {
                  setActiveSavedScenario(null)
                  setTotalBudgetMode('change')
                }}
              />
              Change total by %
            </label>
            {totalBudgetMode === 'change' && (
              <div style={{ marginTop: t.space.sm, display: 'flex', alignItems: 'center', gap: t.space.sm }}>
                <input
                  type="range"
                  min={-50}
                  max={100}
                  step={5}
                  value={totalBudgetChangePct}
                  onChange={(e) => {
                    setActiveSavedScenario(null)
                    setTotalBudgetChangePct(Number(e.target.value))
                  }}
                  style={{ width: 120 }}
                />
                <span style={{ fontSize: t.font.sizeSm, fontVariantNumeric: 'tabular-nums' }}>
                  {totalBudgetChangePct >= 0 ? '+' : ''}{totalBudgetChangePct}%
                </span>
              </div>
            )}
          </div>
        }
      >
        <div>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
            Adjust channel budgets manually against the modeled baseline and rollout constraints.
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, alignItems: 'center' }}>
            {channelList.map((ch) => (
              <span
                key={ch}
                style={{
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  background: t.color.surface,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.borderLight}`,
                }}
              >
                {ch}: {formatCurrency(baselineSpendByChannel[ch] ?? 0)}
              </span>
            ))}
            <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
              Total: {formatCurrency(totalBaselineSpend)}
            </span>
          </div>
        </div>
      </SectionCard>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.xl }}>
        <SectionCard
          title="Scenario guardrails"
          subtitle="Set allowed spend ranges per channel and lock channels that must remain at baseline."
          overflow="auto"
        >
          {channelList.map((ch, idx) => {
            const mult = constraints[ch]?.locked ? 1.0 : (multipliers[ch] ?? 1.0)
            const baseSpend = baselineSpendByChannel[ch] ?? 0
            const newSpend = baseSpend * mult
            const delta = newSpend - baseSpend
            const constraint = constraints[ch] ?? { min: 0.5, max: 2.0, locked: false }
            const color = t.color.chart[idx % t.color.chart.length]
            return (
              <div
                key={ch}
                style={{
                  marginBottom: t.space.lg,
                  padding: t.space.md,
                  borderRadius: t.radius.md,
                  border: `1px solid ${t.color.borderLight}`,
                  backgroundColor: constraint.locked ? t.color.bg : t.color.surface,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.xs }}>
                  <span style={{ fontWeight: t.font.weightMedium, color }}>{ch}</span>
                  <button
                    type="button"
                    onClick={() => handleResetChannel(ch)}
                    disabled={constraint.locked}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      fontSize: t.font.sizeXs,
                      color: t.color.textSecondary,
                      background: 'transparent',
                      border: `1px solid ${t.color.borderLight}`,
                      borderRadius: t.radius.sm,
                      cursor: constraint.locked ? 'not-allowed' : 'pointer',
                    }}
                  >
                    Reset channel
                  </button>
                </div>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginBottom: t.space.xs }}>
                  Baseline: {formatCurrency(baseSpend)} → New: {formatCurrency(newSpend)}
                  {Math.abs(delta) > 0.5 && (
                    <span style={{ color: delta >= 0 ? t.color.success : t.color.danger, marginLeft: t.space.sm }}>
                      {delta >= 0 ? '+' : ''}{formatCurrency(delta)}
                    </span>
                  )}
                </div>
                <input
                  type="range"
                  min={constraint.min}
                  max={constraint.max}
                  step={0.05}
                  value={mult}
                  disabled={constraint.locked}
                  onChange={(e) => handleSliderChange(ch, parseFloat(e.target.value))}
                  style={{ width: '100%', accentColor: color }}
                />
                <div style={{ display: 'flex', gap: t.space.md, marginTop: t.space.sm, alignItems: 'center', fontSize: t.font.sizeXs }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    Min
                    <input
                      type="number"
                      step={0.1}
                      min={0}
                      value={constraint.min}
                      onChange={(e) => updateConstraint(ch, { min: parseFloat(e.target.value) || 0 })}
                      style={{ width: 52, padding: 4, marginLeft: 4, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
                    />
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    Max
                    <input
                      type="number"
                      step={0.1}
                      min={0}
                      value={constraint.max}
                      onChange={(e) => updateConstraint(ch, { max: parseFloat(e.target.value) || 0 })}
                      style={{ width: 52, padding: 4, marginLeft: 4, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm }}
                    />
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={constraint.locked}
                      onChange={(e) => updateConstraint(ch, { locked: e.target.checked })}
                    />
                    Lock (keep at baseline spend)
                  </label>
                </div>
              </div>
            )
          })}
        </SectionCard>

        <SectionCard
          title="Predicted impact"
          subtitle="Read the indexed KPI impact of the current allocation before running a backend scenario check."
          actions={
            <button
              type="button"
              onClick={handleRunWhatIf}
              disabled={isWhatIfLoading || !runId}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                color: t.color.accent,
                background: 'transparent',
                border: `1px solid ${t.color.accent}`,
                borderRadius: t.radius.sm,
                cursor: runId && !isWhatIfLoading ? 'pointer' : 'not-allowed',
              }}
            >
              {isWhatIfLoading ? 'Running…' : 'Run scenario check'}
            </button>
          }
        >
            <div style={{ marginBottom: t.space.sm }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Predicted KPI</span>
                <span style={{ fontWeight: t.font.weightSemibold, fontVariantNumeric: 'tabular-nums' }}>{predictedKPI.toFixed(0)}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Baseline KPI</span>
                <span style={{ fontWeight: t.font.weightMedium, fontVariantNumeric: 'tabular-nums' }}>{baselineKpiValue.toFixed(0)}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: t.space.sm }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium }}>Delta vs baseline</span>
                <span
                  style={{
                    fontWeight: t.font.weightBold,
                    color: upliftPercent >= 0 ? t.color.success : t.color.danger,
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {upliftPercent >= 0 ? '+' : ''}{upliftPercent.toFixed(1)}%
                </span>
              </div>
            </div>

            <div style={{ marginTop: t.space.md }}>
              <div style={{ fontSize: t.font.sizeXs, fontWeight: t.font.weightMedium, color: t.color.textMuted, marginBottom: t.space.xs }}>
                Marginal KPI per $1k spend (ranked)
              </div>
              <ul style={{ listStyle: 'none', margin: 0, padding: 0, fontSize: t.font.sizeSm }}>
                {marginalKpiPer1k.map(({ channel, marginalKpiPer1k }) => (
                  <li
                    key={channel}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      padding: `${t.space.xs}px 0`,
                      borderBottom: `1px solid ${t.color.borderLight}`,
                    }}
                  >
                    <span>{channel}</span>
                    <span style={{ fontVariantNumeric: 'tabular-nums' }}>{marginalKpiPer1k.toFixed(1)}</span>
                  </li>
                ))}
              </ul>
            </div>

            {extrapolationWarnings.length > 0 && (
              <div
                style={{
                  marginTop: t.space.md,
                  padding: t.space.sm,
                  borderRadius: t.radius.sm,
                  backgroundColor: t.color.warningMuted,
                  border: `1px solid ${t.color.warning}`,
                  fontSize: t.font.sizeXs,
                  color: t.color.text,
                }}
              >
                <strong>Extrapolation warning:</strong> Prediction uses spend outside the range observed in the dataset for: {extrapolationWarnings.join('; ')}. Results may be less reliable.
              </div>
            )}

          {whatIfResult && (
            <div
              style={{
                padding: t.space.md,
                borderRadius: t.radius.md,
                border: `1px solid ${t.color.borderLight}`,
                fontSize: t.font.sizeSm,
                marginBottom: t.space.lg,
              }}
            >
              <div style={{ marginBottom: t.space.sm }}>Backend what-if: baseline KPI {whatIfResult.baseline.total_kpi.toFixed(2)} → scenario {whatIfResult.scenario.total_kpi.toFixed(2)} ({whatIfResult.lift.percent >= 0 ? '+' : ''}{whatIfResult.lift.percent.toFixed(1)}%)</div>
            </div>
          )}
        </SectionCard>
      </div>

      <SectionCard
        title="Model-guided reallocation"
        subtitle="Let the optimizer propose a bounded reallocation within the active scenario guardrails."
        actions={
          <button
            type="button"
            onClick={handleSuggestOptimal}
            disabled={isOptimizing || !runId}
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: '#fff',
              background: runId && !isOptimizing ? t.color.accent : t.color.textMuted,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: runId && !isOptimizing ? 'pointer' : 'not-allowed',
            }}
          >
            {isOptimizing ? 'Optimizing…' : 'Suggest model-guided reallocation'}
          </button>
        }
        footer={
          <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={handleExportScenario}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                color: t.color.accent,
                background: 'transparent',
                border: `1px solid ${t.color.accent}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Export scenario
            </button>
            <button
              type="button"
              onClick={handleResetAll}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
                background: 'transparent',
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Reset to baseline
            </button>
          </div>
        }
      >
      {optimalMix != null ? (
        <div
          style={{
            padding: t.space.xl,
            borderRadius: t.radius.md,
            border: `2px solid ${(optimalUplift ?? 0) > 0 ? t.color.success : t.color.border}`,
            backgroundColor: (optimalUplift ?? 0) > 0 ? t.color.successMuted : t.color.bg,
          }}
        >
          <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Recommended reallocation
          </h3>
          {optimizationMessage && (
            <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.md }}>
              {optimizationMessage}
            </p>
          )}
          <p style={{ fontSize: t.font.sizeBase, fontWeight: t.font.weightMedium, marginBottom: t.space.sm }}>
            Expected KPI uplift: {(optimalUplift ?? 0) >= 0 ? '+' : ''}{(optimalUplift ?? 0).toFixed(1)}%
            {optimalPredictedKpi != null && (
              <span style={{ marginLeft: t.space.sm, color: t.color.textSecondary }}>
                (predicted KPI index: {optimalPredictedKpi.toFixed(2)})
              </span>
            )}
          </p>
          <p style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.md }}>
            Rationale: moves budget from low marginal ROI to high marginal ROI channels within your constraints.
          </p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm, marginBottom: t.space.md }}>
            {channelList.map((ch) => {
              const rec = recommendedSpendByChannel?.[ch]
              const base = baselineSpendByChannel[ch] ?? 0
              const mult = optimalMix[ch] ?? 1
              return (
                <span
                  key={ch}
                  style={{
                    fontSize: t.font.sizeSm,
                    padding: `${t.space.xs}px ${t.space.sm}px`,
                    background: t.color.surface,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.borderLight}`,
                  }}
                >
                  {ch}: {formatCurrency(rec ?? base * mult)} ({mult.toFixed(2)}×)
                </span>
              )
            })}
          </div>
          <button
            type="button"
            onClick={handleApplyOptimal}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: '#fff',
              background: t.color.success,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
            >
              Apply recommendation
            </button>
        </div>
      ) : (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Run the optimizer to generate a model-guided reallocation that respects the active budget mode and channel guardrails.
        </div>
      )}
      </SectionCard>
        </>
      )}
    </div>
  )
}
