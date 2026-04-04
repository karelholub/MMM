import { useState, useMemo } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import DecisionStatusCard from '../components/DecisionStatusCard'
import { navigateForRecommendedAction } from '../lib/recommendedActions'

const t = tokens

interface BudgetOptimizerProps {
  roiData: { channel: string; roi: number }[]
  contribData: { channel: string; mean_share: number }[]
  baselineKPI?: number
  runId?: string | null
  datasetId?: string | null
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

function formatCurrency(val: number): string {
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`
  if (val >= 1_000) return `$${(val / 1_000).toFixed(0)}K`
  return `$${val.toFixed(0)}`
}

export default function BudgetOptimizer({
  roiData,
  contribData,
  baselineKPI = 100,
  runId,
  datasetId,
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
  const [optimalMix, setOptimalMix] = useState<Record<string, number> | null>(null)
  const [optimalUplift, setOptimalUplift] = useState<number | null>(null)
  const [optimalPredictedKpi, setOptimalPredictedKpi] = useState<number | null>(null)
  const [optimizationMessage, setOptimizationMessage] = useState<string | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)
  const [whatIfResult, setWhatIfResult] = useState<WhatIfResult | null>(null)
  const [isWhatIfLoading, setIsWhatIfLoading] = useState(false)

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

  const saveScenarioMutation = useMutation({
    mutationFn: async () => {
      if (!runId) throw new Error('Missing model run')
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

  const { data: dataset = [] } = useQuery<Record<string, unknown>[]>({
    queryKey: ['dataset-preview', datasetId],
    queryFn: async () => {
      if (!datasetId) return []
      const d = await apiGetJson<any>(`/api/datasets/${datasetId}?preview_only=false`, {
        fallbackMessage: 'Failed to load dataset preview',
      }).catch(() => ({ preview_rows: [] }))
      return d.preview_rows || []
    },
    enabled: !!datasetId,
  })

  const channelList = useMemo(() => roiData.map((r) => r.channel), [roiData])

  const baselineSpendByChannel = useMemo(() => {
    const out: Record<string, number> = {}
    const chs = channelList
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
  const roiMap = useMemo(() => Object.fromEntries(roiData.map((r) => [r.channel, r.roi])), [roiData])

  const baselineKpiValue = baselineKPI

  const baselineScore = useMemo(() => {
    let s = 0
    for (const ch of channelList) {
      s += (roiMap[ch] ?? 0) * (contribMap[ch] ?? 0)
    }
    return s
  }, [channelList, roiMap, contribMap])

  const predictedScore = useMemo(() => {
    let s = 0
    for (const ch of channelList) {
      const mult = constraints[ch]?.locked ? 1.0 : (multipliers[ch] ?? 1.0)
      s += (roiMap[ch] ?? 0) * (contribMap[ch] ?? 0) * mult
    }
    return s
  }, [channelList, roiMap, contribMap, multipliers, constraints])

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
    setMultipliers((prev) => ({ ...prev, [channel]: value }))
  }

  const handleResetChannel = (channel: string) => {
    setMultipliers((prev) => ({ ...prev, [channel]: 1.0 }))
  }

  const handleResetAll = () => {
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
    if (!runId) return
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
    if (!runId) return
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

  const topRecommendation = recommendationsQuery.data?.recommendations?.[0] ?? null

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

  return (
    <div
      style={{
        marginTop: t.space.xxl,
        backgroundColor: t.color.surface,
        padding: t.space.xl,
        borderRadius: t.radius.lg,
        border: `1px solid ${t.color.borderLight}`,
        boxShadow: t.shadowSm,
      }}
    >
      <h2 style={{ marginTop: 0, marginBottom: t.space.sm, fontSize: t.font.sizeXl, fontWeight: t.font.weightBold, color: t.color.text }}>
        Budget Optimizer
      </h2>
      <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.xl }}>
        Reallocate spend across channels using model ROI and contribution. Sliders scale each channel’s spend relative to the dataset baseline. Constraints and total budget guardrails keep scenarios realistic.
      </p>

      <div
        style={{
          display: 'grid',
          gap: t.space.lg,
          marginBottom: t.space.xl,
          padding: t.space.lg,
          borderRadius: t.radius.md,
          backgroundColor: t.color.bg,
          border: `1px solid ${t.color.borderLight}`,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            gap: t.space.md,
            alignItems: 'flex-start',
            flexWrap: 'wrap',
          }}
        >
          <div style={{ maxWidth: 720 }}>
            <h3
              style={{
                margin: `0 0 ${t.space.xs}px`,
                fontSize: t.font.sizeLg,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Recommendation Center
            </h3>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Start with the model-backed recommendation, then adjust manually if you need a custom scenario.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
            <select
              value={recommendationObjective}
              onChange={(e) =>
                setRecommendationObjective(
                  e.target.value as 'protect_efficiency' | 'grow_conversions' | 'hit_target_roas'
                )
              }
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
              disabled={!runId || saveScenarioMutation.isPending}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.accent}`,
                background: 'transparent',
                color: t.color.accent,
                cursor: !runId || saveScenarioMutation.isPending ? 'not-allowed' : 'pointer',
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
              }}
            >
              {saveScenarioMutation.isPending ? 'Saving…' : 'Save scenario'}
            </button>
          </div>
        </div>

        {recommendationsQuery.data?.decision && (
          <DecisionStatusCard
            title="Budget recommendation status"
            status={recommendationsQuery.data.decision.status ?? undefined}
            subtitle={recommendationsQuery.data.decision.subtitle ?? undefined}
            blockers={recommendationsQuery.data.decision.blockers ?? []}
            warnings={recommendationsQuery.data.decision.warnings ?? []}
            actions={recommendationsQuery.data.decision.actions ?? []}
            onActionClick={(action) => navigateForRecommendedAction(action, { defaultPage: 'mmm' })}
          />
        )}

        {recommendationsQuery.isLoading ? (
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
                  disabled={!runId || saveScenarioMutation.isPending}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    background: 'transparent',
                    color: t.color.text,
                    cursor: !runId || saveScenarioMutation.isPending ? 'not-allowed' : 'pointer',
                  }}
                >
                  Save recommendation
                </button>
              </div>
            </div>

            <div style={{ display: 'grid', gap: t.space.md }}>
              <div
                style={{
                  padding: t.space.lg,
                  borderRadius: t.radius.md,
                  border: `1px solid ${t.color.borderLight}`,
                  background: t.color.surface,
                }}
              >
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>
                  Evidence
                </div>
                <div style={{ display: 'grid', gap: t.space.sm }}>
                  {topRecommendation.evidence.map((item) => (
                    <div key={item} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      {item}
                    </div>
                  ))}
                </div>
                <div style={{ marginTop: t.space.md, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  Based on {recommendationsQuery.data?.summary.periods ?? 0} modeled periods across{' '}
                  {recommendationsQuery.data?.summary.channels_considered ?? 0} channels.
                </div>
              </div>

              <div
                style={{
                  padding: t.space.lg,
                  borderRadius: t.radius.md,
                  border: `1px solid ${t.color.borderLight}`,
                  background: t.color.surface,
                }}
              >
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>
                  Saved scenarios
                </div>
                {savedScenariosQuery.data?.items?.length ? (
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    {savedScenariosQuery.data.items.slice(0, 3).map((scenario) => (
                      <button
                        key={scenario.id}
                        type="button"
                        onClick={() => loadSavedScenario(scenario)}
                        style={{
                          textAlign: 'left',
                          padding: t.space.sm,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.borderLight}`,
                          background: t.color.bg,
                          cursor: 'pointer',
                          display: 'grid',
                          gap: 4,
                        }}
                      >
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                          {scenario.objective.replace(/_/g, ' ')}
                        </div>
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Saved {scenario.created_at ? new Date(scenario.created_at).toLocaleString() : 'recently'}
                        </div>
                      </button>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    No saved scenarios yet.
                  </div>
                )}
                {saveScenarioMutation.isError ? (
                  <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeXs, color: t.color.danger }}>
                    {(saveScenarioMutation.error as Error).message}
                  </div>
                ) : null}
                {saveScenarioMutation.isSuccess ? (
                  <div style={{ marginTop: t.space.sm, fontSize: t.font.sizeXs, color: t.color.success }}>
                    Scenario saved.
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            No recommendation available for this run yet.
          </div>
        )}
      </div>

      {/* Optimizer context: baseline spend + total budget control */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr auto',
          gap: t.space.xl,
          marginBottom: t.space.xl,
          padding: t.space.lg,
          borderRadius: t.radius.md,
          backgroundColor: t.color.bg,
          border: `1px solid ${t.color.borderLight}`,
        }}
      >
        <div>
          <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Manual scenario builder
          </h3>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
            Start from the recommended plan above or build a custom allocation here.
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
        <div>
          <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Total budget
          </h3>
          <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, marginBottom: t.space.xs, cursor: 'pointer', fontSize: t.font.sizeSm }}>
            <input
              type="radio"
              checked={totalBudgetMode === 'constant'}
              onChange={() => setTotalBudgetMode('constant')}
            />
            Keep total constant (default)
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, cursor: 'pointer', fontSize: t.font.sizeSm }}>
            <input
              type="radio"
              checked={totalBudgetMode === 'change'}
              onChange={() => setTotalBudgetMode('change')}
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
                onChange={(e) => setTotalBudgetChangePct(Number(e.target.value))}
                style={{ width: 120 }}
              />
              <span style={{ fontSize: t.font.sizeSm, fontVariantNumeric: 'tabular-nums' }}>
                {totalBudgetChangePct >= 0 ? '+' : ''}{totalBudgetChangePct}%
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Optional constraints: min/max, lock */}
      <div style={{ marginBottom: t.space.xl }}>
        <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Optional constraints
        </h3>
        <p style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.sm }}>
          Min/max: multiplier bounds per channel. Lock: keeps this channel at current spend (multiplier 1.0) so the optimizer cannot change it.
        </p>
      </div>

      {/* Enterprise sliders: baseline spend, new spend, delta, Reset per row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.xl }}>
        <div>
          <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Spend by channel
          </h3>
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
        </div>

        {/* Predicted impact */}
        <div>
          <div
            style={{
              padding: t.space.lg,
              borderRadius: t.radius.md,
              border: `1px solid ${t.color.borderLight}`,
              backgroundColor: t.color.bg,
              marginBottom: t.space.lg,
            }}
          >
            <h3 style={{ margin: `0 0 ${t.space.md}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Predicted impact
            </h3>
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
          </div>

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
              marginBottom: t.space.lg,
            }}
          >
            {isWhatIfLoading ? 'Running…' : 'Run backend what-if'}
          </button>

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
        </div>
      </div>

      {/* Suggest optimal mix */}
      <div style={{ marginTop: t.space.xl, marginBottom: t.space.xl }}>
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
          {isOptimizing ? 'Optimizing…' : 'Suggest optimal mix'}
        </button>
      </div>

      {optimalMix != null && (
        <div
          style={{
            padding: t.space.xl,
            borderRadius: t.radius.md,
            border: `2px solid ${(optimalUplift ?? 0) > 0 ? t.color.success : t.color.border}`,
            backgroundColor: (optimalUplift ?? 0) > 0 ? t.color.successMuted : t.color.bg,
            marginBottom: t.space.xl,
          }}
        >
          <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Recommended allocation
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
      )}

      {/* Actions: Export + Reset */}
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
    </div>
  )
}
