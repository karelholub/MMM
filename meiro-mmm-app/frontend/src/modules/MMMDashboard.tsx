import { useEffect, useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { tokens } from '../theme/tokens'
import { apiGetJson } from '../lib/apiClient'
import ContextSummaryStrip from '../components/dashboard/ContextSummaryStrip'
import CollapsiblePanel from '../components/dashboard/CollapsiblePanel'
import SectionCard from '../components/dashboard/SectionCard'
import DashboardTable from '../components/dashboard/DashboardTable'
import AnalysisNarrativePanel from '../components/dashboard/AnalysisNarrativePanel'
import { usePersistentToggle } from '../hooks/usePersistentToggle'
import AnalysisShareActions from '../components/dashboard/AnalysisShareActions'
import { buildSettingsHref } from '../lib/settingsLinks'
import { evaluateMMMRunQuality } from '../lib/mmmQuality'

interface MMMDashboardProps {
  runId: string
  datasetId: string
  runMetadata?: { attribution_model?: string; attribution_config_id?: string }
  onOpenDataQuality?: () => void
  onOpenBudgetActions?: () => void
}

interface KPI {
  channel: string
  roi: number
}

interface Contrib {
  channel: string
  mean_share: number
}

interface ChannelSummary {
  channel: string
  spend: number
  roi?: number
  mroas?: number
  elasticity?: number
}

interface RunData {
  config?: {
    kpi?: string
    spend_channels?: string[]
    channel_display_names?: Record<string, string>
    kpi_mode?: string
  }
  kpi_mode?: string
  engine_version?: string | null
  uplift?: number
  r2?: number
  calibration_mape?: number
  dataset_available?: boolean
  scenario_count?: number
  latest_scenario_at?: string | null
  stale_from_status?: string | null
  stale_reason?: string | null
  stale_at?: string | null
  diagnostics?: {
    rhat_max?: number
    ess_bulk_min?: number
    divergences?: number
  }
}

function formatCurrency(val: number): string {
  if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`
  if (val >= 1000) return `$${(val / 1000).toFixed(0)}K`
  return `$${val.toFixed(0)}`
}

function exportRoiCSV(roi: KPI[], contrib: Contrib[]) {
  const headers = ['Channel', 'ROI', 'Contribution Share (%)']
  const rows = roi.map((r) => {
    const c = contrib.find((x) => x.channel === r.channel)
    return [r.channel, r.roi.toFixed(4), c ? (c.mean_share * 100).toFixed(2) : '']
  })
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `mmm-roi-contrib-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function channelDisplayName(
  channel: string,
  taxonomyMap: Map<string, string> | undefined,
  overrides: Record<string, string> | undefined
): string {
  if (overrides?.[channel]) return overrides[channel]
  if (taxonomyMap?.get(channel)) return taxonomyMap.get(channel)!
  return channel.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

function MetricCard(props: {
  label: string
  value: string
  description: string
  tone?: 'neutral' | 'success' | 'warning' | 'danger'
  title?: string
}) {
  const t = tokens
  const toneColor =
    props.tone === 'success'
      ? t.color.success
      : props.tone === 'warning'
        ? t.color.warning
        : props.tone === 'danger'
          ? t.color.danger
          : t.color.text
  const borderColor =
    props.tone === 'warning'
      ? t.color.warning
      : props.tone === 'danger'
        ? t.color.danger
        : t.color.borderLight

  return (
    <div
      title={props.title}
      style={{
        background: t.color.surface,
        border: `1px solid ${borderColor}`,
        borderRadius: t.radius.md,
        padding: `${t.space.lg}px ${t.space.xl}px`,
        boxShadow: t.shadowSm,
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: t.font.sizeXs,
          fontWeight: t.font.weightMedium,
          color: t.color.textMuted,
          textTransform: 'uppercase',
          letterSpacing: '0.04em',
          marginBottom: 4,
        }}
      >
        {props.label}
      </div>
      <div
        style={{
          fontSize: t.font.sizeXl,
          fontWeight: t.font.weightBold,
          color: toneColor,
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {props.value}
      </div>
      <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
        {props.description}
      </div>
    </div>
  )
}

export default function MMMDashboard({
  runId,
  datasetId,
  runMetadata,
  onOpenDataQuality,
  onOpenBudgetActions,
}: MMMDashboardProps) {
  const t = tokens
  const [showTrustPanel, setShowTrustPanel] = usePersistentToggle('mmm-dashboard:show-trust-panel', false)
  const [showReconcilePanel, setShowReconcilePanel] = usePersistentToggle('mmm-dashboard:show-reconcile-panel', false)

  const { data: taxonomy } = useQuery<{ channel_rules: { channel: string; name: string }[] }>({
    queryKey: ['taxonomy'],
    queryFn: async () =>
      apiGetJson<{ channel_rules: { channel: string; name: string }[] }>('/api/taxonomy', {
        fallbackMessage: 'Failed to fetch taxonomy',
      }).catch(() => ({ channel_rules: [] })),
  })
  const taxonomyChannelToName = useMemo(() => {
    const m = new Map<string, string>()
    taxonomy?.channel_rules?.forEach((r) => {
      if (r.channel) m.set(r.channel, r.name || r.channel)
    })
    return m
  }, [taxonomy])
  const channelDisplay = (ch: string) =>
    channelDisplayName(ch, taxonomyChannelToName, run?.config?.channel_display_names)

  const { data: run } = useQuery<RunData>({
    queryKey: ['run', runId],
    queryFn: async () => apiGetJson<RunData>(`/api/models/${runId}`, { fallbackMessage: 'Failed to fetch run' }),
  })

  const { data: contrib } = useQuery<Contrib[]>({
    queryKey: ['contrib', runId],
    queryFn: async () => apiGetJson<Contrib[]>(`/api/models/${runId}/contrib`, {
      fallbackMessage: 'Failed to fetch contributions',
    }),
  })

  const { data: roi } = useQuery<KPI[]>({
    queryKey: ['roi', runId],
    queryFn: async () => apiGetJson<KPI[]>(`/api/models/${runId}/roi`, { fallbackMessage: 'Failed to fetch ROI' }),
  })

  const { data: channelSummary = [] } = useQuery<ChannelSummary[]>({
    queryKey: ['channel-summary', runId],
    queryFn: async () =>
      apiGetJson<ChannelSummary[]>(`/api/models/${runId}/summary/channel`, {
        fallbackMessage: 'Failed to fetch channel summary',
      }).catch(() => []),
  })

  const { data: datasetResponse } = useQuery({
    queryKey: ['dataset', datasetId],
    queryFn: async () =>
      apiGetJson<{ preview_rows?: Record<string, unknown>[]; metadata?: { period_start?: string; period_end?: string }; available?: boolean; detail?: string }>(
        `/api/datasets/${datasetId}?preview_only=false`,
        { fallbackMessage: 'Failed to fetch dataset' },
      ),
    enabled: !!datasetId && !!run && run.dataset_available !== false,
  })
  const datasetReadoutOnly = run?.dataset_available === false
  const dataset = (datasetResponse?.preview_rows ?? []) as Record<string, unknown>[]
  const datasetMetadata = datasetResponse?.metadata

  const { data: attributionWeekly } = useQuery<{ series: { date: string; attributed_value: number }[] }>({
    queryKey: ['attribution-weekly', runMetadata?.attribution_model, runMetadata?.attribution_config_id, datasetMetadata?.period_start, datasetMetadata?.period_end],
    queryFn: async () => {
      const params = new URLSearchParams({
        model: runMetadata!.attribution_model!,
        date_start: datasetMetadata!.period_start!,
        date_end: datasetMetadata!.period_end!,
      })
      if (runMetadata?.attribution_config_id) params.set('config_id', runMetadata.attribution_config_id)
      return apiGetJson<{ series: { date: string; attributed_value: number }[] }>(`/api/attribution/weekly?${params}`, {
        fallbackMessage: 'Failed to fetch attribution weekly',
      })
    },
    enabled: !!(runMetadata?.attribution_model && datasetMetadata?.period_start && datasetMetadata?.period_end),
  })

  const kpiTotal = useMemo(() => {
    const kpiCol = run?.config?.kpi
    if (!kpiCol) return 0
    return dataset.reduce((sum, row) => sum + (Number(row[kpiCol]) || 0), 0)
  }, [dataset, run?.config?.kpi])

  const totalSpend = useMemo(() => {
    const chs = run?.config?.spend_channels || []
    if (!dataset.length && channelSummary.length) {
      return channelSummary.reduce((sum, row) => sum + (Number(row.spend) || 0), 0)
    }
    return dataset.reduce((sum, row) => {
      let rowSpend = 0
      chs.forEach((ch: string) => { rowSpend += Number(row[ch]) || 0 })
      return sum + rowSpend
    }, 0)
  }, [channelSummary, dataset, run?.config?.spend_channels])

  const weightedROI = roi?.length ? roi.reduce((s: number, r: KPI) => s + r.roi, 0) / roi.length : 0
  const weeks = dataset.length
  const channelsModeled = run?.config?.spend_channels?.length ?? 0
  const r2 = typeof run?.r2 === 'number' ? run.r2 : null
  const datasetPeriodLabel =
    datasetMetadata?.period_start && datasetMetadata?.period_end
      ? `${datasetMetadata.period_start} – ${datasetMetadata.period_end}`
      : datasetReadoutOnly
        ? 'Dataset file unavailable'
        : 'Dataset period unavailable'

  const hasNegativeRoi = !!roi?.some((r) => r.roi < 0)
  const suspiciousFit = !!(r2 !== null && r2 > 0.98 && weeks > 0 && weeks < 26)
  const manyParamsFewPoints = channelsModeled > 0 && weeks > 0 && channelsModeled > weeks / 2

  const diagnostics = run?.diagnostics

  const diagnosticsIssues: string[] = []
  if (diagnostics?.rhat_max && diagnostics.rhat_max > 1.1) diagnosticsIssues.push('Some parameters have high R-hat (convergence warnings).')
  if (diagnostics?.ess_bulk_min && diagnostics.ess_bulk_min < 200) diagnosticsIssues.push('Effective sample size is low for some parameters.')
  if (diagnostics?.divergences && diagnostics.divergences > 0) diagnosticsIssues.push('There are MCMC divergences; results may be unstable.')

  const modelQuality = useMemo(
    () =>
      evaluateMMMRunQuality({
        status: 'finished',
        engineVersion: run?.engine_version,
        datasetAvailable: !datasetReadoutOnly,
        r2,
        weeks,
        channelsModeled,
        totalSpend,
        roi,
        contrib,
        diagnostics,
      }),
    [channelsModeled, contrib, datasetReadoutOnly, diagnostics, r2, roi, run?.engine_version, totalSpend, weeks],
  )
  const confidenceLevel: 'ok' | 'warn' | 'danger' =
    modelQuality.level === 'ready' && !suspiciousFit && !manyParamsFewPoints && !hasNegativeRoi && diagnosticsIssues.length === 0
      ? 'ok'
      : modelQuality.level === 'not_usable'
        ? 'danger'
        : 'warn'

  const confidenceLabel = confidenceLevel === 'ok' ? 'OK' : modelQuality.label

  const confidenceReasons: string[] = [...modelQuality.reasons]
  if (suspiciousFit) confidenceReasons.push('Very high R² with short history.')
  diagnosticsIssues.forEach((issue) => {
    if (!confidenceReasons.includes(issue)) confidenceReasons.push(issue)
  })
  const canUseModelResults = modelQuality.canUseResults

  const topRoiChannel = useMemo(() => {
    if (!roi?.length) return null
    return [...roi].sort((a, b) => b.roi - a.roi)[0] ?? null
  }, [roi])

  const topContributionChannel = useMemo(() => {
    if (!contrib?.length) return null
    return [...contrib].sort((a, b) => b.mean_share - a.mean_share)[0] ?? null
  }, [contrib])

  const negativeRoiCount = useMemo(() => roi?.filter((item) => item.roi < 0).length ?? 0, [roi])

  const reconciliationSummary = useMemo(() => {
    if (!runMetadata?.attribution_model || !attributionWeekly?.series || !dataset.length) return null
    const currentKpiKey = run?.config?.kpi || 'conversions'
    const mmmByDate = new Map<string, number>()
    dataset.forEach((row) => {
      const date = (row.date as string)?.slice(0, 10)
      if (!date) return
      mmmByDate.set(date, Number(row[currentKpiKey]) || 0)
    })
    const pairs: Array<{ attr: number; mmm: number }> = []
    attributionWeekly.series.forEach(({ date, attributed_value }) => {
      const bucket = date.slice(0, 10)
      const mmm = mmmByDate.get(bucket)
      if (mmm !== undefined) pairs.push({ attr: attributed_value, mmm })
    })
    let correlation = 0
    let meanAbsPctDiff = 0
    if (pairs.length >= 2) {
      const n = pairs.length
      const sumA = pairs.reduce((sum, pair) => sum + pair.attr, 0)
      const sumM = pairs.reduce((sum, pair) => sum + pair.mmm, 0)
      const meanA = sumA / n
      const meanM = sumM / n
      let numerator = 0
      let denominatorA = 0
      let denominatorM = 0
      pairs.forEach((pair) => {
        numerator += (pair.attr - meanA) * (pair.mmm - meanM)
        denominatorA += (pair.attr - meanA) ** 2
        denominatorM += (pair.mmm - meanM) ** 2
      })
      const denominator = Math.sqrt(denominatorA * denominatorM)
      correlation = denominator > 0 ? numerator / denominator : 0
      meanAbsPctDiff =
        pairs.reduce((sum, pair) => sum + (pair.mmm > 0 ? Math.abs(pair.attr - pair.mmm) / pair.mmm : 0), 0) / n
    }
    return {
      pairCount: pairs.length,
      correlation,
      meanAbsPctDiff,
      largeDivergence: meanAbsPctDiff > 0.25,
    }
  }, [attributionWeekly?.series, dataset, run?.config?.kpi, runMetadata?.attribution_model])

  const getKpiDisplayName = () => {
    const mode = run?.kpi_mode || run?.config?.kpi_mode || 'conversions'
    const map: Record<string, string> = {
      conversions: 'Conversions',
      aov: 'Average Order Value (AOV)',
      profit: 'Profitability',
    }
    return map[mode] || 'MMM'
  }

  const trustNarrative = useMemo(() => {
    const headline = modelQuality.level === 'not_usable'
      ? 'This MMM run should not be used for ROI, contribution, or budget decisions.'
      : datasetReadoutOnly
      ? 'This historical MMM run is open for saved readout, but source-row diagnostics are paused until the dataset is rebuilt.'
      : confidenceLevel === 'ok'
      ? 'The current MMM run looks usable for directional budget decisions.'
      : 'Use this MMM run directionally, but validate the fragile parts before acting on exact ROI values.'
    const items = [
      datasetReadoutOnly
        ? 'Basis: ROI, contribution, fit, and saved budget work come from the saved MMM run. Time-series charts, response curves, and dataset reconciliation need the missing source rows.'
        : `Basis: this page uses the saved MMM run plus the linked dataset preview for ${datasetPeriodLabel}. ROI, contribution, and response curves all come from that run context.`,
      datasetReadoutOnly
        ? `The saved model still contains ${channelsModeled.toLocaleString()} modeled channel${channelsModeled === 1 ? '' : 's'}; rebuild a compatible dataset before using it for new optimization.`
        : weeks < 20
        ? `History is short at ${weeks.toLocaleString()} weeks, so ranking channels is more reliable than trusting precise point estimates.`
        : `The run covers ${weeks.toLocaleString()} modeled weeks across ${channelsModeled.toLocaleString()} channels.`,
      confidenceReasons[0]
        ? `Primary trust risk: ${confidenceReasons[0]}`
        : 'No major convergence or fit red flags surfaced in the current model diagnostics.',
      reconciliationSummary
        ? reconciliationSummary.largeDivergence
          ? `Attribution overlay check is diverging: overlapping weekly totals differ by ${(reconciliationSummary.meanAbsPctDiff * 100).toFixed(1)}% on average. Treat MMM and attribution as competing signals until data quality is reviewed.`
          : `Attribution overlay check is broadly aligned: overlapping weekly totals differ by ${(reconciliationSummary.meanAbsPctDiff * 100).toFixed(1)}% on average.`
        : runMetadata?.attribution_model
          ? 'Attribution overlay is configured, but there are not enough overlapping dated points yet to reconcile it against the MMM KPI.'
          : 'This run uses a direct KPI source, so there is no attribution-overlay reconciliation for this model.',
    ]
    return { headline, items }
  }, [
    channelsModeled,
    confidenceLevel,
    confidenceReasons,
    datasetReadoutOnly,
    datasetPeriodLabel,
    modelQuality.level,
    reconciliationSummary,
    runMetadata?.attribution_model,
    weeks,
  ])

  // Derived helpers for charts
  const kpiKey = run?.config?.kpi || 'sales'
  const spendChannels: string[] = run?.config?.spend_channels || []

  const [visibleChannels, setVisibleChannels] = useState<string[]>(spendChannels)

  useEffect(() => {
    setVisibleChannels(spendChannels)
  }, [spendChannels.join(',')])

  const topChannelsByShare = useMemo(() => {
    if (!contrib || contrib.length === 0) return []
    return [...contrib]
      .sort((a, b) => b.mean_share - a.mean_share)
      .slice(0, 5)
      .map((c) => c.channel)
  }, [contrib])

  const contributionOverTime = useMemo(() => {
    if (!canUseModelResults || !contrib || contrib.length === 0 || !dataset || !kpiKey) return []
    const shareMap = new Map(contrib.map((c) => [c.channel, c.mean_share]))
    return dataset.map((row) => {
      const date = (row['date'] as string) || ''
      const kpiVal = Number(row[kpiKey]) || 0
      const entry: any = { date }
      topChannelsByShare.forEach((ch) => {
        const share = shareMap.get(ch) ?? 0
        entry[ch] = kpiVal * share
      })
      return entry
    })
  }, [canUseModelResults, dataset, contrib, kpiKey, topChannelsByShare])

  const responseCurves = useMemo(() => {
    if (!canUseModelResults || !spendChannels.length || !dataset.length || !roi?.length) return []
    const roiMap = new Map(roi.map((r: KPI) => [r.channel, r.roi]))
    const meanSpend: Record<string, number> = {}
    spendChannels.forEach((ch) => {
      meanSpend[ch] =
        dataset.reduce((sum, row) => sum + (Number(row[ch]) || 0), 0) / Math.max(1, dataset.length)
    })
    const topForCurves = [...spendChannels]
      .sort((a, b) => (roiMap.get(b) || 0) - (roiMap.get(a) || 0))
      .slice(0, 3)
    return topForCurves.map((ch) => {
      const baseline = meanSpend[ch] || 0
      const baseRoi = roiMap.get(ch) || 0
      const points: { spend: number; kpi: number }[] = []
      const maxSpend = baseline * 3 || 1
      const steps = 20
      for (let i = 0; i <= steps; i++) {
        const x = (maxSpend * i) / steps
        const yMax = baseRoi * baseline
        const y =
          yMax <= 0 || baseline <= 0
            ? baseRoi * x
            : yMax * (1 - Math.exp(-x / baseline))
        points.push({ spend: x, kpi: y })
      }
      return { channel: ch, points }
    })
  }, [canUseModelResults, spendChannels, dataset, roi])

  const decisionTone: 'success' | 'warning' | 'danger' =
    modelQuality.level === 'not_usable' ? 'danger' : datasetReadoutOnly || confidenceLevel !== 'ok' ? 'warning' : 'success'
  const decisionLabel = modelQuality.level === 'not_usable'
    ? 'Do not use for decisions'
    : datasetReadoutOnly
    ? 'Readout only'
    : confidenceLevel === 'ok'
      ? 'Ready for budget review'
      : 'Validate before action'
  const decisionReason = modelQuality.level === 'not_usable'
    ? confidenceReasons[0] || 'The model output has no usable signal.'
    : datasetReadoutOnly
    ? 'Saved results are visible, but source-row diagnostics and new optimization need dataset recovery.'
    : confidenceLevel === 'ok'
      ? 'Model outputs and linked dataset checks are available for directional budget planning.'
      : confidenceReasons[0] || 'Model diagnostics need review before acting on exact ROI values.'

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      <div style={{ marginBottom: t.space.md }}>
        <div
          style={{
            margin: 0,
            fontSize: t.font.sizeLg,
            fontWeight: t.font.weightSemibold,
            color: t.color.text,
          }}
        >
          Model readout
        </div>
        <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Decision-grade model fit, channel impact, and reconciliation signals for {getKpiDisplayName().toLowerCase()}.
        </p>
      </div>

      <div style={{ marginBottom: t.space.md }}>
        <ContextSummaryStrip
          items={[
            { label: 'Source', value: datasetReadoutOnly ? 'MMM run readout only' : 'MMM run + dataset preview' },
            { label: 'Dataset period', value: datasetPeriodLabel },
            { label: 'KPI', value: getKpiDisplayName() },
            { label: 'Total spend', value: formatCurrency(totalSpend) },
            { label: 'Weeks', value: datasetReadoutOnly ? 'Unavailable' : weeks.toLocaleString() },
            { label: 'Confidence', value: confidenceLabel, valueColor: confidenceLevel === 'ok' ? t.color.success : confidenceLevel === 'danger' ? t.color.danger : t.color.warning },
          ]}
        />
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <SectionCard
          title="Decision summary"
          subtitle="The compact MMM readout for what can be trusted and where to go next."
          actions={
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              <button
                type="button"
                onClick={() => onOpenBudgetActions?.()}
                disabled={!onOpenBudgetActions}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.accent}`,
                  background: t.color.accent,
                  color: '#ffffff',
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                  cursor: onOpenBudgetActions ? 'pointer' : 'not-allowed',
                }}
              >
                Open budget actions
              </button>
              <button
                type="button"
                onClick={() => onOpenDataQuality?.()}
                disabled={!onOpenDataQuality}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  color: onOpenDataQuality ? t.color.textSecondary : t.color.textMuted,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  cursor: onOpenDataQuality ? 'pointer' : 'not-allowed',
                }}
              >
                View data quality
              </button>
              <button
                type="button"
                onClick={() => {
                  window.location.assign(buildSettingsHref('mmm'))
                }}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  background: t.color.surface,
                  color: t.color.textSecondary,
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightMedium,
                  cursor: 'pointer',
                }}
              >
                MMM settings
              </button>
            </div>
          }
        >
          <div style={{ display: 'grid', gap: t.space.lg }}>
            <div
              style={{
                padding: t.space.md,
                borderRadius: t.radius.md,
                border: `1px solid ${decisionTone === 'success' ? t.color.success : decisionTone === 'danger' ? t.color.danger : t.color.warning}`,
                background: decisionTone === 'success' ? t.color.successMuted : decisionTone === 'danger' ? t.color.dangerMuted : t.color.warningMuted,
                display: 'grid',
                gap: 4,
              }}
            >
              <div
                style={{
                  fontSize: t.font.sizeSm,
                  fontWeight: t.font.weightSemibold,
                    color: decisionTone === 'success' ? t.color.success : decisionTone === 'danger' ? t.color.danger : t.color.warning,
                }}
              >
                {decisionLabel}
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                {decisionReason}
              </div>
            </div>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                gap: t.space.md,
              }}
            >
              {[
                {
                  label: 'Top ROI',
                  value: canUseModelResults && topRoiChannel ? channelDisplay(topRoiChannel.channel) : 'Unavailable',
                  helper: canUseModelResults && topRoiChannel ? `${topRoiChannel.roi.toFixed(2)}x modeled return` : 'Run quality blocks ROI ranking',
                },
                {
                  label: 'Top contribution',
                  value: canUseModelResults && topContributionChannel ? channelDisplay(topContributionChannel.channel) : 'Unavailable',
                  helper: canUseModelResults && topContributionChannel
                    ? `${(topContributionChannel.mean_share * 100).toFixed(1)}% of modeled KPI`
                    : 'No contribution output yet',
                },
                {
                  label: 'Model basis',
                  value: datasetReadoutOnly ? 'Saved readout' : `${weeks.toLocaleString()} weeks`,
                  helper: `${channelsModeled.toLocaleString()} modeled channel${channelsModeled === 1 ? '' : 's'}`,
                },
                {
                  label: 'Reconcile',
                  value: reconciliationSummary
                    ? reconciliationSummary.largeDivergence
                      ? 'Mismatch'
                      : 'Aligned'
                    : 'No overlay',
                  helper: reconciliationSummary
                    ? `${reconciliationSummary.pairCount.toLocaleString()} overlapping buckets`
                    : runMetadata?.attribution_model
                      ? 'Insufficient overlap'
                      : 'Direct KPI source',
                },
              ].map((item) => (
                <div
                  key={item.label}
                  style={{
                    padding: t.space.md,
                    borderRadius: t.radius.md,
                    border: `1px solid ${t.color.borderLight}`,
                    background: t.color.bg,
                    display: 'grid',
                    gap: 4,
                    minWidth: 0,
                  }}
                >
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                    {item.label}
                  </div>
                  <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>
                    {item.value}
                  </div>
                  <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, overflowWrap: 'anywhere' }}>
                    {item.helper}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </SectionCard>
      </div>

      {datasetReadoutOnly ? (
        <div style={{ marginBottom: t.space.xl }}>
          <SectionCard
            title="Readout-only MMM run"
            subtitle="The model output loaded, but the linked dataset file is missing in this runtime."
          >
            <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <div>
                Saved fit, ROI, contribution, and budget history are still available. Dataset-preview checks, time-series
                charts, response curves, attribution reconciliation, and new optimizer recommendations are paused.
              </div>
              <div>
                To make new budget decisions, use the recovery action above to create a compatible new run from the saved
                model settings.
              </div>
            </div>
          </SectionCard>
        </div>
      ) : null}

      <div style={{ marginBottom: t.space.xl }}>
        <AnalysisShareActions
          fileStem="mmm-dashboard"
          summaryTitle="MMM dashboard"
          summaryLines={[
            `Dataset period: ${datasetPeriodLabel}`,
            `Total spend: ${formatCurrency(totalSpend)}`,
            `Modeled weeks: ${weeks.toLocaleString()}`,
            `Channels modeled: ${channelsModeled.toLocaleString()}`,
            `Model fit (R²): ${r2 !== null ? r2.toFixed(3) : 'Unavailable'}`,
            `Confidence: ${confidenceLabel}`,
            canUseModelResults && topRoiChannel ? `Top ROI channel: ${channelDisplay(topRoiChannel.channel)} (${topRoiChannel.roi.toFixed(2)}x)` : '',
            canUseModelResults && topContributionChannel
              ? `Top contribution channel: ${channelDisplay(topContributionChannel.channel)} (${(topContributionChannel.mean_share * 100).toFixed(1)}%)`
              : '',
          ]}
        />
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <AnalysisNarrativePanel
          title="What to trust"
          subtitle="A short readout of the current MMM data contract before you act on fit, contribution, or budget recommendations."
          headline={trustNarrative.headline}
          items={trustNarrative.items}
        />
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <SectionCard
          title="Model health"
          subtitle="Fit, calibration, observation count, and modeling stability in one place."
        >
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
              gap: t.space.md,
            }}
          >
            <MetricCard
              label="Model fit (R²)"
              value={r2 !== null ? r2.toFixed(3) : '—'}
              tone={suspiciousFit ? 'warning' : 'neutral'}
              title="Proportion of KPI variance explained by the model."
              description={
                suspiciousFit
                  ? 'Very high fit on short history; treat ROI with care.'
                  : 'Higher is better; 0.7-0.9 is typical for stable MMM.'
              }
            />

            {run?.calibration_mape != null && (
              <MetricCard
                label="Holdout error (MAPE)"
                value={`${((run.calibration_mape as number) * 100).toFixed(1)}%`}
                tone={(run.calibration_mape as number) <= 0.25 ? 'success' : 'warning'}
                title="Mean Absolute Percentage Error on holdout period."
                description="Lower is better; below 20% is usually strong calibration."
              />
            )}

            <MetricCard
              label="Data points"
              value={datasetReadoutOnly ? 'Unavailable' : `${weeks} weeks`}
              tone={weeks < 20 || datasetReadoutOnly ? 'warning' : 'neutral'}
              title="Number of weekly observations used by the model."
              description={
                datasetReadoutOnly
                  ? 'Linked dataset preview is unavailable for this saved run.'
                  : 'More history increases stability; aim for 26+ weeks.'
              }
            />

            <MetricCard
              label="Channels modeled"
              value={channelsModeled.toLocaleString()}
              tone={manyParamsFewPoints ? 'warning' : 'neutral'}
              title="Media channels included in the model."
              description="Too many channels vs weeks can overfit."
            />

            <MetricCard
              label="Confidence / diagnostics"
              value={confidenceLabel}
              tone={confidenceLevel === 'ok' ? 'success' : confidenceLevel === 'danger' ? 'danger' : 'warning'}
              description={
                confidenceReasons.length === 0
                  ? 'No major red flags detected. Check data health for details.'
                  : confidenceReasons[0]
              }
            />
          </div>
        </SectionCard>
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <SectionCard
          title="Reconciliation basis"
          subtitle="How this MMM run lines up with other measurement layers in the app."
        >
          <div style={{ display: 'grid', gap: t.space.md }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {datasetReadoutOnly
                ? 'MMM diagnostics on this page are anchored to the selected saved run. Dataset-preview and attribution reconciliation checks are unavailable until the source rows are rebuilt or reattached.'
                : 'MMM diagnostics on this page are anchored to the selected run and its linked dataset preview. When attribution is available, the overlay compares overlapping dated totals only. It is a contract check, not a replacement for model fit.'}
            </div>
            <div
              style={{
                display: 'grid',
                gap: t.space.sm,
                gridTemplateColumns: 'repeat(auto-fit, minmax(min(180px, 100%), 1fr))',
              }}
            >
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>MMM run basis</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>{datasetReadoutOnly ? 'Unavailable' : weeks.toLocaleString()}</strong> dataset rows
                </div>
              </div>
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>KPI source</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>
                    {runMetadata?.attribution_model ? `Attribution (${runMetadata.attribution_model.replace(/_/g, ' ')})` : 'Direct'}
                  </strong>
                </div>
              </div>
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Overlap used for reconcile</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>{reconciliationSummary?.pairCount?.toLocaleString() ?? '—'}</strong> aligned time buckets
                </div>
              </div>
              <div style={{ display: 'grid', gap: 2 }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Reconcile status</div>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  <strong style={{ color: t.color.text }}>
                    {reconciliationSummary
                      ? reconciliationSummary.largeDivergence
                        ? 'Check mismatch'
                        : 'Broadly aligned'
                      : 'No overlay'}
                  </strong>
                </div>
              </div>
            </div>
          </div>
        </SectionCard>
      </div>

      <div style={{ marginBottom: t.space.xl }}>
        <CollapsiblePanel
          title="Supporting links & caveats"
          subtitle="Secondary cautions and navigation into the source surfaces behind this run."
          open={showTrustPanel}
          onToggle={() => setShowTrustPanel((value) => !value)}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              gap: t.space.lg,
              flexWrap: 'wrap',
            }}
          >
            <div style={{ minWidth: 0, flex: 1 }}>
              <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                {weeks < 20 && <li>Short history – results may be noisy; prefer relative ranking over exact ROI.</li>}
                {hasNegativeRoi && <li>Some channels have negative ROI – these are candidates to cut or re-evaluate.</li>}
                {manyParamsFewPoints && (
                  <li>Many channels compared to weeks – consider grouping smaller channels together.</li>
                )}
                {confidenceReasons.length === 0 && weeks >= 20 && !hasNegativeRoi && !manyParamsFewPoints && (
                  <li>No extra caveats beyond the trust summary above.</li>
                )}
                {confidenceReasons.slice(1, 3).map((reason) => (
                  <li key={reason}>{reason}</li>
                ))}
              </ul>
            </div>
            <div
              style={{
                display: 'flex',
                flexDirection: 'column',
                gap: t.space.xs,
                minWidth: 200,
              }}
            >
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Quick links
              </span>
              <button
                type="button"
                onClick={() => {
                  window.location.assign('/?page=datasources')
                }}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  fontSize: t.font.sizeXs,
                  color: t.color.accent,
                  background: 'transparent',
                  border: 'none',
                  textAlign: 'left',
                  cursor: 'pointer',
                }}
              >
                Open data sources
              </button>
              <button
                type="button"
                onClick={() => {
                  window.location.assign(buildSettingsHref('mmm'))
                }}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: 'none',
                  textAlign: 'left',
                  cursor: 'pointer',
                }}
              >
                Open MMM settings
              </button>
              <button
                type="button"
                onClick={() => onOpenDataQuality?.()}
                style={{
                  padding: `${t.space.xs}px ${t.space.sm}px`,
                  fontSize: t.font.sizeXs,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: 'none',
                  textAlign: 'left',
                  cursor: onOpenDataQuality ? 'pointer' : 'default',
                }}
              >
                View data quality
              </button>
            </div>
          </div>
        </CollapsiblePanel>
      </div>

      {/* Reconcile view: attributed vs MMM KPI (when KPI source is Attribution) */}
      {reconciliationSummary && (
        <div key="reconcile" style={{ marginBottom: t.space.xl }}>
          <CollapsiblePanel
            title="Reconcile: Attribution vs MMM"
            subtitle={`Compare weekly attributed conversions vs MMM modeled KPI for ${runMetadata?.attribution_model ?? 'the selected attribution model'}.`}
            open={showReconcilePanel}
            onToggle={() => setShowReconcilePanel((value) => !value)}
          >
            <div
              style={{
                padding: t.space.sm,
                borderRadius: t.radius.sm,
                border: `1px solid ${reconciliationSummary.largeDivergence ? t.color.warning : t.color.borderLight}`,
                background: t.color.surface,
              }}
            >
              <div style={{ marginBottom: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                This check stays on overlapping dated totals only; use the Reconciliation basis card above for the underlying contract.
              </div>
              <div style={{ display: 'flex', gap: t.space.xl, flexWrap: 'wrap', marginBottom: t.space.sm }}>
                <span style={{ fontSize: t.font.sizeSm }}>Overlapping buckets: <strong style={{ fontVariantNumeric: 'tabular-nums' }}>{reconciliationSummary.pairCount.toLocaleString()}</strong></span>
                <span style={{ fontSize: t.font.sizeSm }}>Correlation: <strong style={{ fontVariantNumeric: 'tabular-nums' }}>{reconciliationSummary.correlation.toFixed(3)}</strong></span>
                <span style={{ fontSize: t.font.sizeSm }}>Mean absolute % difference: <strong style={{ fontVariantNumeric: 'tabular-nums' }}>{(reconciliationSummary.meanAbsPctDiff * 100).toFixed(1)}%</strong></span>
              </div>
              {reconciliationSummary.largeDivergence ? (
                <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.warning }}>
                  Large divergence detected. Check data quality and attribution setup.{' '}
                  <button
                    type="button"
                    onClick={() => onOpenDataQuality?.()}
                    style={{ background: 'none', border: 'none', color: t.color.accent, cursor: 'pointer', textDecoration: 'underline', padding: 0, fontSize: 'inherit' }}
                  >
                    Open Data Quality
                  </button>
                </p>
              ) : (
                <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  MMM KPI totals and attribution totals are broadly aligned for this period.
                </p>
              )}
            </div>
          </CollapsiblePanel>
        </div>
      )}

      <style>{`@media (max-width: 900px) { .mmm-charts { grid-template-columns: 1fr !important; } }`}</style>
      <div
        className="mmm-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: t.space.xl,
          marginBottom: t.space.xl,
        }}
      >
        {dataset.length > 0 && (
          <SectionCard
            title="KPI vs spend over time"
            subtitle="Compare modeled KPI movement against visible channel spend inputs week by week."
            actions={
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.xs, alignItems: 'center' }}>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Channels shown:</span>
                {spendChannels.map((ch, idx) => {
                  const selected = visibleChannels.includes(ch)
                  return (
                    <label
                      key={ch}
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                        padding: '2px 6px',
                        borderRadius: 999,
                        background: selected ? t.color.accentMuted : 'transparent',
                        border: `1px solid ${selected ? t.color.accent : t.color.borderLight}`,
                        cursor: 'pointer',
                        fontSize: t.font.sizeXs,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={selected}
                        onChange={() => {
                          setVisibleChannels((prev) =>
                            prev.includes(ch) ? prev.filter((c) => c !== ch) : [...prev, ch],
                          )
                        }}
                        style={{ margin: 0 }}
                      />
                      <span
                        style={{
                          width: 8,
                          height: 8,
                          borderRadius: 999,
                          backgroundColor: t.color.chart[idx % t.color.chart.length],
                        }}
                      />
                      <span>{channelDisplay(ch)}</span>
                    </label>
                  )
                })}
              </div>
            }
          >
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dataset} margin={{ top: 8, right: 16, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis dataKey="date" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <YAxis
                  yAxisId="left"
                  orientation="left"
                  stroke={t.color.chart[0]}
                  tick={{ fontSize: t.font.sizeSm }}
                />
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  stroke={t.color.chart[1]}
                  tick={{ fontSize: t.font.sizeSm }}
                />
                <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
                <Legend wrapperStyle={{ fontSize: t.font.sizeSm }} />
                {spendChannels
                  .filter((ch) => visibleChannels.includes(ch))
                  .map((ch: string, idx: number) => (
                    <Line
                      key={ch}
                      yAxisId="left"
                      type="monotone"
                      dataKey={ch}
                      stroke={t.color.chart[idx % t.color.chart.length]}
                      name={`${channelDisplay(ch)} spend`}
                      dot={false}
                    />
                  ))}
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey={kpiKey}
                  stroke={t.color.warning}
                  strokeWidth={2}
                  name="KPI"
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </SectionCard>
        )}

        {dataset.length > 0 && (
          <SectionCard
            title="Spend by channel"
            subtitle="Stacked spend view for the modeled channels across the selected dataset window."
          >
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={dataset} margin={{ top: 8, right: 16, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis dataKey="date" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
                <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
                {spendChannels.map((ch, idx) => (
                  <Area
                    key={ch}
                    type="monotone"
                    stackId="spend"
                    dataKey={ch}
                    stroke={t.color.chart[idx % t.color.chart.length]}
                    fill={t.color.chart[idx % t.color.chart.length]}
                    fillOpacity={0.3}
                    name={channelDisplay(ch)}
                  />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </SectionCard>
        )}
      </div>

      {contributionOverTime.length > 0 && (
        <div style={{ marginBottom: t.space.xl }}>
          <SectionCard
            title="Contribution over time"
            subtitle="Modeled KPI contribution for the top channels by share."
          >
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={contributionOverTime} margin={{ top: 8, right: 16, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
              <XAxis dataKey="date" tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
              <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} />
              <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
              {topChannelsByShare.map((ch, idx) => (
                <Area
                  key={ch}
                  type="monotone"
                  stackId="contrib"
                  dataKey={ch}
                  stroke={t.color.chart[idx % t.color.chart.length]}
                  fill={t.color.chart[idx % t.color.chart.length]}
                  fillOpacity={0.5}
                  name={channelDisplay(ch)}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
          </SectionCard>
        </div>
      )}

      <div
        className="mmm-charts"
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: t.space.xl,
          marginBottom: t.space.xl,
        }}
      >
        {roi && roi.length > 0 && (
          <SectionCard
            title="ROI by channel"
            subtitle="Point estimates for modeled return at the current fitted spend pattern."
          >
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={roi} margin={{ top: 8, right: 16, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis
                  dataKey="channel"
                  tick={{ fontSize: t.font.sizeSm, fill: t.color.text }}
                  tickFormatter={(v) => channelDisplay(String(v))}
                />
                <YAxis
                  tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }}
                  domain={[-2, 'auto']}
                />
                <Tooltip contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
                <Bar dataKey="roi" name="ROI" radius={[4, 4, 0, 0]}>
                  {roi.map((_: { channel: string }, i: number) => (
                    <Cell key={i} fill={t.color.chart[i % t.color.chart.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </SectionCard>
        )}

        {contrib && contrib.length > 0 && (
          <SectionCard
            title="Channel contribution share"
            subtitle="Share of modeled KPI credited to each channel across the selected run."
          >
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={contrib} margin={{ top: 8, right: 16, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                <XAxis
                  dataKey="channel"
                  tick={{ fontSize: t.font.sizeSm, fill: t.color.text }}
                  tickFormatter={(v) => channelDisplay(String(v))}
                />
                <YAxis tick={{ fontSize: t.font.sizeSm, fill: t.color.textSecondary }} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                <Tooltip formatter={(value: number) => `${(value * 100).toFixed(1)}%`} contentStyle={{ fontSize: t.font.sizeSm, borderRadius: t.radius.sm }} />
                <Bar dataKey="mean_share" name="Contribution" radius={[4, 4, 0, 0]}>
                  {contrib.map((_: { channel: string }, i: number) => (
                    <Cell key={i} fill={t.color.chart[(i + 2) % t.color.chart.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </SectionCard>
        )}
      </div>

      {responseCurves.length > 0 && (
        <div style={{ marginBottom: t.space.xl }}>
          <SectionCard
            title="Response curves"
            subtitle="Relative saturation shape for the highest-ROI channels. Use for prioritization, not exact forecasting."
            footer="Curves are smoothed approximations based on current spend and ROI. Use them to compare relative saturation, not exact levels."
          >
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.lg }}>
            {responseCurves.map((curve) => (
              <div key={curve.channel}>
                <p
                  style={{
                    margin: `0 0 ${t.space.sm}px`,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    color: t.color.text,
                  }}
                >
                  {channelDisplay(curve.channel)}
                </p>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={curve.points} margin={{ top: 4, right: 8, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={t.color.borderLight} />
                    <XAxis
                      dataKey="spend"
                      tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }}
                      tickFormatter={(v) => formatCurrency(Number(v))}
                    />
                    <YAxis
                      tick={{ fontSize: t.font.sizeXs, fill: t.color.textSecondary }}
                      tickFormatter={(v) => v.toFixed(0)}
                    />
                    <Tooltip
                      contentStyle={{ fontSize: t.font.sizeXs, borderRadius: t.radius.sm }}
                      formatter={(value: any) => [Number(value).toFixed(0), 'Modeled KPI']}
                    />
                    <Line
                      type="monotone"
                      dataKey="kpi"
                      stroke={t.color.accent}
                      strokeWidth={2}
                      dot={false}
                      name="Modeled KPI"
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ))}
          </div>
          </SectionCard>
        </div>
      )}

      <SectionCard
        title="Summary & channel metrics"
        subtitle="Unified spend, contribution, and efficiency view for the modeled channels."
        actions={
          roi?.length && contrib?.length ? (
            <button
              type="button"
              onClick={() => exportRoiCSV(roi, contrib)}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                color: t.color.accent,
                background: 'transparent',
                border: `1px solid ${t.color.accent}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Export CSV
            </button>
          ) : null
        }
        overflow="auto"
      >
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
            gap: t.space.md,
            marginBottom: roi?.length ? t.space.xl : 0,
          }}
        >
          <MetricCard
            label="Top ROI channel"
            value={canUseModelResults && topRoiChannel ? channelDisplay(topRoiChannel.channel) : '—'}
            tone={canUseModelResults && topRoiChannel && topRoiChannel.roi > 0 ? 'success' : 'neutral'}
            description={canUseModelResults && topRoiChannel ? `${topRoiChannel.roi.toFixed(2)}x modeled return` : 'Run quality blocks ROI ranking.'}
          />
          <MetricCard
            label="Top contribution"
            value={canUseModelResults && topContributionChannel ? channelDisplay(topContributionChannel.channel) : '—'}
            tone="neutral"
            description={
              canUseModelResults && topContributionChannel
                ? `${(topContributionChannel.mean_share * 100).toFixed(1)}% of modeled KPI`
                : 'Run quality blocks contribution ranking.'
            }
          />
          <MetricCard
            label="Channels to review"
            value={canUseModelResults ? negativeRoiCount.toLocaleString() : 'All'}
            tone={canUseModelResults ? (negativeRoiCount > 0 ? 'warning' : 'success') : 'danger'}
            description={canUseModelResults ? (negativeRoiCount > 0 ? 'Negative ROI channels need validation before action.' : 'No negative ROI channels detected.') : confidenceReasons[0] || 'Run is not usable for channel decisions.'}
          />
        </div>

        {!canUseModelResults && (
          <div
            style={{
              marginBottom: t.space.lg,
              padding: t.space.md,
              borderRadius: t.radius.md,
              border: `1px solid ${t.color.danger}`,
              background: t.color.dangerMuted,
              color: t.color.textSecondary,
              fontSize: t.font.sizeSm,
              lineHeight: 1.5,
            }}
          >
            <strong style={{ color: t.color.danger }}>Model output is not usable.</strong>{' '}
            {confidenceReasons[0] || 'The run did not produce reliable ROI or contribution signal.'} Create a new run after confirming the dataset has spend and KPI variation.
          </div>
        )}

        {roi && roi.length > 0 && contrib && contrib.length > 0 && (
          <DashboardTable density="compact">
              <thead>
                <tr>
                  <th>Channel</th>
                  <th style={{ textAlign: 'right' }} title="Total spend over the modeled period.">Spend</th>
                  <th style={{ textAlign: 'right' }} title="Estimated KPI generated by this channel.">Contribution</th>
                  <th style={{ textAlign: 'right' }} title="Modeled return per 1 unit of spend.">ROI</th>
                  <th style={{ textAlign: 'right' }} title="Incremental ROI at current spend (if available).">Marginal ROI</th>
                  <th style={{ textAlign: 'right' }} title="Share of total modeled KPI.">Share</th>
                  <th style={{ textAlign: 'right' }} title="Heuristic efficiency tier based on ROI.">Efficiency</th>
                </tr>
              </thead>
              <tbody>
                {roi.map((r: { channel: string; roi: number }) => {
                  const c = contrib.find((x: { channel: string }) => x.channel === r.channel)
                  const summary = channelSummary.find((s) => s.channel === r.channel)
                  const channelSpend = summary?.spend ?? 0
                  const contributionShare = c ? c.mean_share : 0
                  const contributionAmount = kpiTotal * contributionShare
                  const contributionDisplay =
                    c && kpiTotal > 0
                      ? `${contributionAmount.toFixed(0)} (${(contributionShare * 100).toFixed(1)}%)`
                      : c
                        ? `${(contributionShare * 100).toFixed(1)}% share`
                        : '—'
                  const marginalRoi = summary?.mroas ?? r.roi
                  const efficiencyTier =
                    !canUseModelResults
                      ? 'Not usable'
                      : channelSpend <= 0
                        ? 'No spend'
                        : Math.abs(weightedROI) <= 1e-9
                          ? 'Unavailable'
                          : r.roi < 0
                      ? 'Unprofitable'
                      : r.roi >= weightedROI * 1.2
                        ? 'High'
                        : r.roi <= weightedROI * 0.8
                          ? 'Low'
                          : 'Medium'
                  const metricColor = !canUseModelResults || channelSpend <= 0 ? t.color.textMuted : r.roi >= 0 ? t.color.success : t.color.danger
                  return (
                    <tr key={r.channel}>
                      <td
                        style={{
                          fontWeight: t.font.weightMedium,
                          color: t.color.text,
                        }}
                      >
                        {channelDisplay(r.channel)}
                      </td>
                      <td
                        style={{
                          textAlign: 'right',
                          fontVariantNumeric: 'tabular-nums',
                        }}
                        title="Sum of modeled spend for this channel."
                      >
                        {channelSpend ? formatCurrency(channelSpend) : '—'}
                      </td>
                      <td
                        style={{
                          textAlign: 'right',
                          fontVariantNumeric: 'tabular-nums',
                        }}
                        title={
                          c && kpiTotal > 0
                            ? `Approx. ${contributionAmount.toFixed(
                                0,
                              )} KPI (${(contributionShare * 100).toFixed(1)}% of total).`
                            : c
                              ? 'Contribution share from the saved MMM run. KPI total is unavailable because the linked dataset preview is missing.'
                            : undefined
                        }
                      >
                        {contributionDisplay}
                      </td>
                      <td
                        style={{
                          textAlign: 'right',
                          color: metricColor,
                          fontVariantNumeric: 'tabular-nums',
                        }}
                      >
                        {canUseModelResults && channelSpend > 0 ? `${r.roi.toFixed(3)}×` : '—'}
                      </td>
                      <td
                        style={{
                          textAlign: 'right',
                          color: !canUseModelResults || channelSpend <= 0 ? t.color.textMuted : marginalRoi >= 0 ? t.color.success : t.color.danger,
                          fontVariantNumeric: 'tabular-nums',
                        }}
                      >
                        {canUseModelResults && channelSpend > 0 ? `${marginalRoi.toFixed(3)}×` : '—'}
                      </td>
                      <td
                        style={{
                          textAlign: 'right',
                          fontVariantNumeric: 'tabular-nums',
                        }}
                      >
                        {c ? `${(c.mean_share * 100).toFixed(2)}%` : '—'}
                      </td>
                      <td
                        style={{
                          textAlign: 'right',
                          fontSize: t.font.sizeXs,
                          color:
                            efficiencyTier === 'High'
                              ? t.color.success
                              : efficiencyTier === 'Low' || efficiencyTier === 'Unprofitable' || efficiencyTier === 'Not usable'
                                ? t.color.danger
                                : t.color.textSecondary,
                        }}
                      >
                        {efficiencyTier}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
          </DashboardTable>
        )}
      </SectionCard>
    </div>
  )
}
