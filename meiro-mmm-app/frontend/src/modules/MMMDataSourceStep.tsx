import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import DatasetUploader from './DatasetUploader'
import { apiGetJson, apiSendJson } from '../lib/apiClient'
import { useWorkspaceContext } from '../components/WorkspaceContext'
import { segmentOptionLabel, type SegmentRegistryItem, type SegmentRegistryResponse } from '../lib/segments'
import MeiroTargetInstanceBadge from '../features/meiro/MeiroTargetInstanceBadge'
import { getMeiroConfig, type MeiroConfig } from '../connectors/meiroConnector'
import MeiroMeasurementScopeNotice from '../features/meiro/MeiroMeasurementScopeNotice'

const t = tokens

type DataSourceType = 'platform' | 'csv'

interface PlatformOptions {
  spend_channels: string[]
  covariates: string[]
  walled_garden_channels?: string[]
  media_input_modes?: Array<{ id: 'spend' | 'synthetic_impressions'; label: string; description: string }>
}

interface BuildResponse {
  dataset_id: string
  columns: string[]
  preview_rows: Record<string, unknown>[]
  coverage: {
    n_weeks: number
    missing_spend_weeks: Record<string, number>
    missing_kpi_weeks: number
    spend_totals?: Record<string, number>
    channels_with_spend?: string[]
    all_zero_spend_channels?: string[]
    total_spend?: number
    synthetic_impression_totals?: Record<string, number>
    synthetic_impression_columns?: string[]
    channels_with_synthetic_impressions?: string[]
    delivery?: {
      channels?: Record<string, {
        confidence?: 'high' | 'medium' | 'low'
        confidence_score?: number
        caveats?: string[]
        synthetic_impressions?: number
        impressions?: number
      }>
    }
  }
  metadata: {
    period_start: string
    period_end: string
    kpi_target: string
    kpi_column: string
    spend_channels: string[]
    source_spend_channels?: string[]
    covariates: string[]
    currency: string
    source?: string
    source_detail?: string
    source_contract?: Record<string, unknown>
    media_input_mode?: 'spend' | 'synthetic_impressions'
    synthetic_impressions?: {
      enabled: boolean
      columns: string[]
      totals: Record<string, number>
    }
    attribution_model?: string
    attribution_config_id?: string
    measurement_audience?: {
      id?: string
      name?: string
      profile_count?: number
      journey_rows?: number
      materialization_status?: string
    }
  }
}

const CHANNEL_LABELS: Record<string, string> = {
  google_ads: 'Google',
  meta_ads: 'Meta',
  linkedin_ads: 'LinkedIn',
  email: 'Email',
  whatsapp: 'WhatsApp',
}

function channelLabel(ch: string): string {
  return CHANNEL_LABELS[ch] ?? ch.replace(/_/g, ' ')
}

interface MMMDataSourceStepProps {
  onMappingComplete: (mapping: {
    dataset_id: string
    columns: { kpi: string; spend_channels: string[]; covariates: string[] }
  }) => void
  initialPlatformDraft?: {
    kpiTarget?: 'sales' | 'attribution'
    spendChannels?: string[]
    attributionModel?: string
    attributionConfigId?: string | null
    notice?: string
  }
}

export default function MMMDataSourceStep({ onMappingComplete, initialPlatformDraft }: MMMDataSourceStepProps) {
  const { journeysSummary } = useWorkspaceContext()
  const [sourceType, setSourceType] = useState<DataSourceType>('platform')
  const [kpiTarget, setKpiTarget] = useState<'sales' | 'attribution'>('sales')
  const [dateStart, setDateStart] = useState(() => {
    const d = new Date()
    d.setMonth(d.getMonth() - 6)
    return d.toISOString().slice(0, 10)
  })
  const [dateEnd, setDateEnd] = useState(() => new Date().toISOString().slice(0, 10))
  const [selectedChannels, setSelectedChannels] = useState<string[]>([])
  const [mediaInputMode, setMediaInputMode] = useState<'spend' | 'synthetic_impressions'>('spend')
  const [platformResult, setPlatformResult] = useState<BuildResponse | null>(null)
  const [attributionModel, setAttributionModel] = useState<string>('linear')
  const [attributionConfigId, setAttributionConfigId] = useState<string | null>(null)
  const [measurementAudienceId, setMeasurementAudienceId] = useState('')
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const seededDraftChannelsRef = useRef<string | null>(null)

  const { data: platformOptions } = useQuery<PlatformOptions>({
    queryKey: ['mmm-platform-options'],
    queryFn: async () => apiGetJson<PlatformOptions>('/api/mmm/platform-options', {
      fallbackMessage: 'Failed to load options',
    }),
  })

  const { data: attributionModelsData } = useQuery<{ models: string[] }>({
    queryKey: ['attribution-models'],
    queryFn: async () =>
      apiGetJson<{ models: string[] }>('/api/attribution/models', {
        fallbackMessage: 'Failed to load attribution models',
      }).catch(() => ({ models: ['linear', 'last_touch', 'first_touch', 'time_decay', 'position_based', 'markov'] })),
  })
  const { data: modelConfigs } = useQuery<{ id: string; name: string }[]>({
    queryKey: ['model-configs'],
    queryFn: async () =>
      apiGetJson<any>('/api/model-configs', { fallbackMessage: 'Failed to load model configs' })
        .then((list) => (Array.isArray(list) ? list : []))
        .catch(() => []),
  })
  const { data: segmentRegistry } = useQuery<SegmentRegistryResponse>({
    queryKey: ['segment-registry', 'mmm-measurement-audience'],
    queryFn: async () =>
      apiGetJson<SegmentRegistryResponse>('/api/segments/registry', {
        fallbackMessage: 'Failed to load measurement audiences',
      }),
  })
  const { data: meiroConfig } = useQuery<MeiroConfig>({
    queryKey: ['meiro-config'],
    queryFn: getMeiroConfig,
  })

  const attributionModelOptions = attributionModelsData?.models ?? ['linear', 'last_touch', 'first_touch', 'time_decay', 'position_based', 'markov']
  const spendChannelOptions = platformOptions?.spend_channels ?? []
  const covariateOptions = platformOptions?.covariates ?? []
  const walledGardenChannels = new Set(platformOptions?.walled_garden_channels ?? [])
  const initialSpendChannelsKey = initialPlatformDraft?.spendChannels?.join(',') ?? ''
  const operationalAudiences = (segmentRegistry?.items ?? []).filter((item) => item.source !== 'local_analytical')
  const measurementReadyAudiences = operationalAudiences.filter((item) => item.audience_capability?.membership_backed || item.audience_capability?.measurement_ready)
  const definitionOnlyAudiences = operationalAudiences.filter((item) => !item.audience_capability?.membership_backed && !item.audience_capability?.measurement_ready)
  const audienceCapabilitySummary = segmentRegistry?.summary
  const selectedMeasurementAudience: SegmentRegistryItem | null =
    measurementReadyAudiences.find((item) => item.id === measurementAudienceId) ?? null
  const latestEventReplay =
    journeysSummary?.readiness?.details?.latest_event_replay ??
    journeysSummary?.readiness?.summary?.latest_event_replay ??
    null
  const latestEventReplayDiagnostics = latestEventReplay?.diagnostics
  const attributionSourceLabel = latestEventReplayDiagnostics?.events_loaded
    ? 'Pipes raw events -> replay/import -> live journeys'
    : 'Current live journeys'
  const sourceContractRows = [
    {
      label: 'Attribution / KPI source',
      value: kpiTarget === 'attribution' ? attributionSourceLabel : `${attributionSourceLabel} for journey-derived sales`,
      helper: latestEventReplayDiagnostics?.events_loaded
        ? `${latestEventReplayDiagnostics.events_loaded.toLocaleString()} raw events loaded; ${(latestEventReplayDiagnostics.journeys_persisted ?? 0).toLocaleString()} journeys persisted.`
        : 'No latest raw-event replay diagnostics are attached to the current journey summary.',
    },
    {
      label: 'Spend source',
      value: 'Platform expenses',
      helper: 'MMM spend comes from Expenses/imported spend for the selected channels and period.',
    },
    {
      label: 'Audience scope',
      value: selectedMeasurementAudience ? selectedMeasurementAudience.name : 'Workspace aggregate',
      helper: selectedMeasurementAudience
        ? `Membership-backed audience selected for measurement context; ${selectedMeasurementAudience.audience_capability?.profile_count?.toLocaleString() ?? 'observed'} profiles are available from profile-state membership.`
        : 'CDP/profile audiences are enrichment or planning context unless a membership-backed measurement audience is selected for the MMM dataset contract.',
    },
  ]
  const measurementAudiencePayload = selectedMeasurementAudience
    ? {
        id: selectedMeasurementAudience.id,
        name: selectedMeasurementAudience.name,
        source: selectedMeasurementAudience.source,
        external_segment_id: selectedMeasurementAudience.external_segment_id || selectedMeasurementAudience.id,
        capability: selectedMeasurementAudience.audience_capability || null,
        materialization_status: 'membership_backed_reference',
      }
    : null
  const sourceContractPayload = {
    attribution_source: attributionSourceLabel,
    spend_source: 'Platform expenses',
    audience_scope: selectedMeasurementAudience ? `Measurement audience: ${selectedMeasurementAudience.name}` : 'Workspace aggregate',
    measurement_audience: measurementAudiencePayload,
    profile_cdp_role: 'Enrichment only unless a membership-backed measurement audience is materialized into the dataset',
    latest_event_replay: latestEventReplayDiagnostics
      ? {
          events_loaded: latestEventReplayDiagnostics.events_loaded ?? 0,
          profiles_reconstructed: latestEventReplayDiagnostics.profiles_reconstructed ?? 0,
          journeys_persisted: latestEventReplayDiagnostics.journeys_persisted ?? 0,
        }
      : null,
  }

  useEffect(() => {
    if (initialPlatformDraft?.kpiTarget) setKpiTarget(initialPlatformDraft.kpiTarget)
    if (initialPlatformDraft?.attributionModel) setAttributionModel(initialPlatformDraft.attributionModel)
    if (initialPlatformDraft?.attributionConfigId !== undefined) setAttributionConfigId(initialPlatformDraft.attributionConfigId)
  }, [initialPlatformDraft?.kpiTarget, initialPlatformDraft?.attributionModel, initialPlatformDraft?.attributionConfigId])

  useEffect(() => {
    const draftChannels = initialPlatformDraft?.spendChannels ?? []
    if (draftChannels.length && spendChannelOptions.length && seededDraftChannelsRef.current !== initialSpendChannelsKey) {
      const availableDraftChannels = draftChannels.filter((ch) => spendChannelOptions.includes(ch))
      setSelectedChannels(availableDraftChannels)
      seededDraftChannelsRef.current = initialSpendChannelsKey
      return
    }
    if (spendChannelOptions.length && !selectedChannels.length) {
      const preferred = ['paid_search', 'paid_social', 'facebook_ads', 'organic_search', 'referral', 'direct']
      const preferredAvailable = preferred.filter((ch) => spendChannelOptions.includes(ch))
      setSelectedChannels(preferredAvailable.length ? preferredAvailable : spendChannelOptions.slice(0, 6))
    }
  }, [initialSpendChannelsKey, spendChannelOptions.join(','), selectedChannels.length])

  const buildMutation = useMutation({
    mutationFn: async () => {
      return apiSendJson<BuildResponse>('/api/mmm/datasets/build-from-platform', 'POST', {
        date_start: dateStart,
        date_end: dateEnd,
        kpi_target: kpiTarget,
        spend_channels: selectedChannels,
        covariates: covariateOptions.length ? [] : undefined,
        currency: 'USD',
        media_input_mode: mediaInputMode,
        include_synthetic_impressions: true,
        source_contract: sourceContractPayload,
        ...(measurementAudiencePayload && { measurement_audience: measurementAudiencePayload }),
        ...(kpiTarget === 'attribution' && {
          attribution_model: attributionModel,
          ...(attributionConfigId && { attribution_config_id: attributionConfigId }),
        }),
      }, {
        fallbackMessage: 'Failed to generate dataset',
      })
    },
    onSuccess: (data) => setPlatformResult(data),
  })

  const toggleChannel = (ch: string) => {
    setSelectedChannels((prev) => (prev.includes(ch) ? prev.filter((c) => c !== ch) : [...prev, ch]))
  }

  const handleUsePlatformDataset = () => {
    if (!platformResult) return
    onMappingComplete({
      dataset_id: platformResult.dataset_id,
      columns: {
        kpi: platformResult.metadata.kpi_column,
        spend_channels: platformResult.metadata.spend_channels,
        covariates: platformResult.metadata.covariates ?? [],
      },
    })
  }

  // —— Source choice (only when no platform result yet) ——
  if (sourceType === 'csv') {
    return (
      <div style={{ maxWidth: 920, margin: '0 auto' }}>
        <div style={{ marginBottom: t.space.lg, display: 'flex', alignItems: 'center', gap: t.space.md }}>
          <button
            type="button"
            onClick={() => setSourceType('platform')}
            style={{
              padding: `${t.space.xs}px ${t.space.sm}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            ← Back to data source options
          </button>
        </div>
        <DatasetUploader onMappingComplete={onMappingComplete} />
      </div>
    )
  }

  // —— Platform: show result (preview + coverage + Use this dataset) ——
  if (platformResult) {
    const { coverage, metadata } = platformResult
    const totalSpend = Number(coverage.total_spend ?? 0)
    const allZeroSpendChannels = coverage.all_zero_spend_channels ?? []
    const hasUsableSpend = totalSpend > 0
    const syntheticTotals = coverage.synthetic_impression_totals ?? {}
    const totalSynthetic = Object.values(syntheticTotals).reduce((sum, value) => sum + Number(value || 0), 0)
    const hasSyntheticSignal = totalSynthetic > 0
    const canUseDataset = metadata.media_input_mode === 'synthetic_impressions' ? hasSyntheticSignal : hasUsableSpend
    const deliveryChannels = coverage.delivery?.channels ?? {}
    return (
      <div style={{ maxWidth: 920, margin: '0 auto' }}>
        <div style={{ marginBottom: t.space.lg }}>
          <h3 style={{ margin: `0 0 ${t.space.xs}px`, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Dataset generated
          </h3>
          <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            <code style={{ background: t.color.bg, padding: '2px 6px', borderRadius: t.radius.sm }}>{platformResult.dataset_id}</code>
            {' · '}
            {metadata.period_start} → {metadata.period_end} · {metadata.kpi_column}
            {' · '}
            {metadata.source_detail === 'platform_journeys_expenses' ? 'Platform journeys + expenses' : metadata.source || 'Platform dataset'}
          </p>
        </div>

        <div
          style={{
            marginBottom: t.space.xl,
            padding: t.space.lg,
            borderRadius: t.radius.lg,
            border: `1px solid ${t.color.borderLight}`,
            background: t.color.surface,
            boxShadow: t.shadowSm,
          }}
        >
          <div style={{ marginBottom: t.space.md }}>
            <h4 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Measurement source contract
            </h4>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Build one MMM dataset from one attribution source. Profile/CDP data can enrich labels and audiences, but it does not replace the raw-event journey source in this run.
            </p>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: t.space.md }}>
            {sourceContractRows.map((row) => (
              <div key={row.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.md, background: t.color.bg }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', fontWeight: t.font.weightMedium }}>
                  {row.label}
                </div>
                <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                  {row.value}
                </div>
                <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  {row.helper}
                </div>
              </div>
            ))}
          </div>
        </div>

        {metadata.measurement_audience ? (
          <div
            style={{
              marginBottom: t.space.xl,
              padding: t.space.lg,
              borderRadius: t.radius.lg,
              border: `1px solid ${t.color.success}`,
              background: t.color.successMuted,
              color: t.color.textSecondary,
              fontSize: t.font.sizeSm,
            }}
          >
            <strong style={{ color: t.color.text }}>Measurement audience materialized:</strong>{' '}
            {metadata.measurement_audience.name || metadata.measurement_audience.id || 'Selected audience'}
            {' · '}
            {(metadata.measurement_audience.profile_count ?? 0).toLocaleString()} profiles
            {' · '}
            {(metadata.measurement_audience.journey_rows ?? 0).toLocaleString()} matching journeys.
            <div style={{ marginTop: t.space.xs }}>
              Status: {String(metadata.measurement_audience.materialization_status || 'journey_rows_filtered').replace(/_/g, ' ')}.
            </div>
          </div>
        ) : null}

        {/* Coverage */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
            gap: t.space.md,
            marginBottom: t.space.xl,
          }}
        >
          <div
            style={{
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              padding: t.space.md,
              boxShadow: t.shadowSm,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Weeks
            </div>
            <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightBold, color: t.color.text, marginTop: 2 }}>
              {coverage.n_weeks}
            </div>
          </div>
          <div
            style={{
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.md,
              padding: t.space.md,
              boxShadow: t.shadowSm,
            }}
          >
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
              Missing KPI weeks
            </div>
            <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightBold, color: coverage.missing_kpi_weeks > 0 ? t.color.warning : t.color.text, marginTop: 2 }}>
              {coverage.missing_kpi_weeks}
            </div>
          </div>
          {Object.entries(coverage.missing_spend_weeks || {}).map(([ch, count]) => (
            <div
              key={ch}
              style={{
                background: t.color.surface,
                border: `1px solid ${t.color.borderLight}`,
                borderRadius: t.radius.md,
                padding: t.space.md,
                boxShadow: t.shadowSm,
              }}
            >
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                {channelLabel(ch)} missing
              </div>
              <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightBold, color: (count as number) > 0 ? t.color.warning : t.color.text, marginTop: 2 }}>
                {(count as number)}
              </div>
            </div>
          ))}
        </div>

        <div
          style={{
            marginBottom: t.space.xl,
            padding: t.space.lg,
            borderRadius: t.radius.lg,
            border: `1px solid ${hasUsableSpend && allZeroSpendChannels.length === 0 ? t.color.success : t.color.warning}`,
            background: hasUsableSpend && allZeroSpendChannels.length === 0 ? t.color.successMuted : t.color.warningMuted,
            color: t.color.textSecondary,
            fontSize: t.font.sizeSm,
          }}
        >
          <strong style={{ color: t.color.text }}>Spend coverage:</strong>{' '}
          {hasUsableSpend
            ? `$${totalSpend.toLocaleString(undefined, { maximumFractionDigits: 0 })} mapped across ${coverage.channels_with_spend?.length ?? 0} selected channels.`
            : 'No spend was found for the selected channels and period.'}
          {allZeroSpendChannels.length > 0 && (
            <div style={{ marginTop: t.space.xs }}>
              No spend found for {allZeroSpendChannels.map(channelLabel).join(', ')}. Remove those channels or add expenses before using this dataset.
            </div>
          )}
        </div>

        <div
          style={{
            marginBottom: t.space.xl,
            padding: t.space.lg,
            borderRadius: t.radius.lg,
            border: `1px solid ${hasSyntheticSignal ? t.color.success : t.color.borderLight}`,
            background: t.color.surface,
            color: t.color.textSecondary,
            fontSize: t.font.sizeSm,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
            <div>
              <strong style={{ color: t.color.text }}>Walled-garden exposure:</strong>{' '}
              {hasSyntheticSignal
                ? `${Math.round(totalSynthetic).toLocaleString()} synthetic impressions built from imported delivery metrics.`
                : 'No synthetic impressions were found for this period.'}
            </div>
            <div style={{ color: t.color.textMuted }}>
              Modeled from {metadata.media_input_mode === 'synthetic_impressions' ? 'synthetic impressions' : 'spend'}
            </div>
          </div>
          {Object.entries(deliveryChannels).length > 0 && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm, marginTop: t.space.md }}>
              {Object.entries(deliveryChannels).map(([channel, detail]) => (
                <div key={channel} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bg }}>
                  <div style={{ fontWeight: t.font.weightSemibold, color: t.color.text }}>{channelLabel(channel)}</div>
                  <div style={{ fontSize: t.font.sizeXs }}>
                    {Math.round(Number(detail.synthetic_impressions || 0)).toLocaleString()} synthetic impressions
                  </div>
                  <div style={{ fontSize: t.font.sizeXs, color: detail.confidence === 'high' ? t.color.success : detail.confidence === 'low' ? t.color.warning : t.color.textSecondary }}>
                    {detail.confidence ?? 'low'} confidence
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Preview table */}
        <div
          style={{
            background: t.color.surface,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.lg,
            overflow: 'hidden',
            marginBottom: t.space.xl,
            boxShadow: t.shadowSm,
          }}
        >
          <div style={{ padding: `${t.space.md}px ${t.space.lg}px`, borderBottom: `1px solid ${t.color.borderLight}`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Preview (first 10 rows)
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}`, backgroundColor: t.color.bg }}>
                  {platformResult.columns.map((col) => (
                    <th key={col} style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', fontWeight: t.font.weightSemibold, color: t.color.textSecondary }}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {platformResult.preview_rows.map((row: Record<string, unknown>, idx: number) => (
                  <tr key={idx} style={{ borderBottom: `1px solid ${t.color.borderLight}`, backgroundColor: idx % 2 === 0 ? t.color.surface : t.color.bg }}>
                    {platformResult.columns.map((col) => (
                      <td key={col} style={{ padding: `${t.space.sm}px ${t.space.md}px`, color: t.color.text }}>
                        {row[col] != null ? String(row[col]) : '—'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: t.space.md }}>
          <button
            type="button"
            onClick={() => setPlatformResult(null)}
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
            Generate a different dataset
          </button>
          <button
            type="button"
            onClick={handleUsePlatformDataset}
            disabled={!canUseDataset}
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              color: '#fff',
              background: canUseDataset ? t.color.accent : t.color.border,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: canUseDataset ? 'pointer' : 'not-allowed',
            }}
          >
            Use this dataset →
          </button>
        </div>
      </div>
    )
  }

  // —— Platform: form (no result yet) ——
  return (
    <div style={{ maxWidth: 920, margin: '0 auto' }}>
      <div style={{ marginBottom: t.space.xl }}>
        <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Choose data source
        </h3>
        <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => setSourceType('platform')}
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              borderRadius: t.radius.sm,
              border: `2px solid ${t.color.accent}`,
              background: t.color.accentMuted,
              color: t.color.accent,
              cursor: 'pointer',
            }}
          >
            Use platform data (recommended)
          </button>
          <button
            type="button"
            onClick={() => setSourceType('csv')}
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              borderRadius: t.radius.sm,
              border: `2px solid ${t.color.border}`,
              background: t.color.surface,
              color: t.color.textSecondary,
              cursor: 'pointer',
            }}
          >
            Upload CSV (advanced)
          </button>
        </div>
      </div>

      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          marginBottom: t.space.xl,
          boxShadow: t.shadowSm,
        }}
      >
        <h4 style={{ margin: `0 0 ${t.space.lg}px`, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
          Use platform data
        </h4>
        {initialPlatformDraft?.notice && (
          <div
            style={{
              marginBottom: t.space.lg,
              padding: t.space.md,
              borderRadius: t.radius.md,
              border: `1px solid ${t.color.warning}`,
              background: t.color.warningMuted,
              color: t.color.textSecondary,
              fontSize: t.font.sizeSm,
            }}
          >
            <strong style={{ color: t.color.text }}>Compatible rebuild:</strong> {initialPlatformDraft.notice}
          </div>
        )}

        <div style={{ marginBottom: t.space.lg }}>
          <MeiroTargetInstanceBadge config={meiroConfig} compact />
        </div>
        <div style={{ marginBottom: t.space.lg }}>
          <MeiroMeasurementScopeNotice />
        </div>

        <div
          style={{
            marginBottom: t.space.lg,
            padding: t.space.lg,
            borderRadius: t.radius.lg,
            border: `1px solid ${latestEventReplayDiagnostics?.events_loaded ? t.color.success : t.color.warning}`,
            background: latestEventReplayDiagnostics?.events_loaded ? t.color.successMuted : t.color.warningMuted,
          }}
        >
          <h4 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
            Measurement source contract
          </h4>
          <p style={{ margin: `${t.space.xs}px 0 ${t.space.md}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            MMM run creation uses a single attribution basis. Raw-event replay feeds attribution journeys; profile/CDP payloads stay as enrichment unless you materialize a measurement audience before modeling.
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: t.space.md }}>
            {sourceContractRows.map((row) => (
              <div key={row.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.md, background: t.color.surface }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', fontWeight: t.font.weightMedium }}>
                  {row.label}
                </div>
                <div style={{ marginTop: 4, fontSize: t.font.sizeSm, color: t.color.text, fontWeight: t.font.weightSemibold }}>
                  {row.value}
                </div>
                <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  {row.helper}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div style={{ marginBottom: t.space.lg }}>
          <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
            KPI target
          </label>
          <div style={{ display: 'flex', gap: t.space.lg }}>
            <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="radio" name="kpiTarget" checked={kpiTarget === 'sales'} onChange={() => setKpiTarget('sales')} style={{ marginRight: t.space.sm }} />
              Sales (total revenue)
            </label>
            <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="radio" name="kpiTarget" checked={kpiTarget === 'attribution'} onChange={() => setKpiTarget('attribution')} style={{ marginRight: t.space.sm }} />
              Marketing-driven conversions (Attribution)
            </label>
          </div>
          {kpiTarget === 'attribution' && (
            <div style={{ marginTop: t.space.md, padding: t.space.md, background: t.color.bg, borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}` }}>
              <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
                Attribution model used for KPI series
              </label>
              <select
                value={attributionModel}
                onChange={(e) => setAttributionModel(e.target.value)}
                style={{
                  width: '100%',
                  maxWidth: 280,
                  padding: t.space.sm,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  marginBottom: t.space.sm,
                }}
              >
                {attributionModelOptions.map((id) => (
                  <option key={id} value={id}>
                    {id === 'markov' ? 'Data-Driven (Markov)' : id.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}
                  </option>
                ))}
              </select>
              {modelConfigs && modelConfigs.length > 0 && (
                <>
                  <label style={{ display: 'block', fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.xs }}>Model config (optional)</label>
                  <select
                    value={attributionConfigId ?? ''}
                    onChange={(e) => setAttributionConfigId(e.target.value || null)}
                    style={{
                      width: '100%',
                      maxWidth: 280,
                      padding: t.space.sm,
                      fontSize: t.font.sizeSm,
                      border: `1px solid ${t.color.border}`,
                      borderRadius: t.radius.sm,
                    }}
                  >
                    <option value="">Default</option>
                    {modelConfigs.map((c) => (
                      <option key={c.id} value={c.id}>{c.name}</option>
                    ))}
                  </select>
                </>
              )}
            </div>
          )}
        </div>

        <div style={{ marginBottom: t.space.lg }}>
          <label
            htmlFor="mmm-measurement-audience"
            style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}
          >
            Measurement audience
          </label>
          <select
            id="mmm-measurement-audience"
            value={measurementAudienceId}
            onChange={(e) => setMeasurementAudienceId(e.target.value)}
            style={{
              width: '100%',
              maxWidth: 520,
              padding: t.space.sm,
              fontSize: t.font.sizeSm,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              background: t.color.surface,
              color: t.color.text,
            }}
          >
            <option value="">Workspace aggregate</option>
            {measurementReadyAudiences.map((audience) => (
              <option key={audience.id} value={audience.id}>
                {segmentOptionLabel(audience)}
              </option>
            ))}
          </select>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            Only membership-backed Meiro/Pipes audiences can be attached to an MMM measurement contract. Definition-only audiences remain available for activation and planning, but cannot silently scope MMM.
            {audienceCapabilitySummary
              ? ` Available: ${Number(audienceCapabilitySummary.measurement_ready || 0).toLocaleString()} measurement-ready, ${Number(audienceCapabilitySummary.definition_only || 0).toLocaleString()} definition-only, ${Number(audienceCapabilitySummary.estimated_reach || 0).toLocaleString()} estimated-reach.`
              : ''}
          </p>
          {definitionOnlyAudiences.length > 0 ? (
            <div
              style={{
                marginTop: t.space.sm,
                padding: t.space.sm,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                background: t.color.bg,
                fontSize: t.font.sizeXs,
                color: t.color.textSecondary,
              }}
            >
              Blocked from MMM scoping until membership is observed: {definitionOnlyAudiences.slice(0, 3).map((audience) => audience.name).join(', ')}
              {definitionOnlyAudiences.length > 3 ? `, +${definitionOnlyAudiences.length - 3} more` : ''}.
            </div>
          ) : null}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: t.space.lg, marginBottom: t.space.lg }}>
          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Period start (weekly)
            </label>
            <input
              type="date"
              value={dateStart}
              onChange={(e) => setDateStart(e.target.value)}
              style={{
                width: '100%',
                padding: t.space.sm,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Period end (weekly)
            </label>
            <input
              type="date"
              value={dateEnd}
              onChange={(e) => setDateEnd(e.target.value)}
              style={{
                width: '100%',
                padding: t.space.sm,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>
        </div>

        <div style={{ marginBottom: t.space.lg }}>
          <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.sm }}>
            Spend sources (toggle)
          </label>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.sm }}>
            {spendChannelOptions.map((ch) => {
              const selected = selectedChannels.includes(ch)
              const hasWalledGardenSignal = walledGardenChannels.has(ch)
              return (
                <button
                  key={ch}
                  type="button"
                  onClick={() => toggleChannel(ch)}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    fontWeight: t.font.weightMedium,
                    borderRadius: 999,
                    border: `1px solid ${selected ? t.color.accent : t.color.border}`,
                    background: selected ? t.color.accent : t.color.surface,
                    color: selected ? '#fff' : t.color.textSecondary,
                    cursor: 'pointer',
                  }}
                >
                  {channelLabel(ch)}
                  {hasWalledGardenSignal ? ' · exposure' : ''}
                </button>
              )
            })}
          </div>
          {!spendChannelOptions.length && (
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>
              No spend sources in platform. Add expenses in Data sources / Expenses, or use CSV upload.
            </p>
          )}
        </div>

        <div
          style={{
            marginBottom: t.space.lg,
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            background: t.color.bg,
          }}
        >
          <button
            type="button"
            onClick={() => setShowAdvancedControls((value) => !value)}
            aria-expanded={showAdvancedControls}
            style={{
              width: '100%',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              gap: t.space.md,
              padding: t.space.md,
              border: 'none',
              background: 'transparent',
              color: t.color.text,
              cursor: 'pointer',
              textAlign: 'left',
            }}
          >
            <span>
              <span style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                Advanced dataset controls
              </span>
              <span style={{ display: 'block', marginTop: 2, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                Media basis: {mediaInputMode === 'synthetic_impressions' ? 'synthetic impressions' : 'spend'}{covariateOptions.length ? ` · ${covariateOptions.length} covariates available` : ''}
              </span>
            </span>
            <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              {showAdvancedControls ? 'Hide' : 'Show'}
            </span>
          </button>
          {showAdvancedControls && (
            <div style={{ padding: `${t.space.sm}px ${t.space.md}px ${t.space.md}`, borderTop: `1px solid ${t.color.borderLight}` }}>
              <div style={{ marginBottom: t.space.lg }}>
                <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                  Media response basis
                </label>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
                  {(platformOptions?.media_input_modes ?? [
                    { id: 'spend', label: 'Spend response', description: 'Model from spend and use synthetic impressions as diagnostics.' },
                    { id: 'synthetic_impressions', label: 'Synthetic impression response', description: 'Model from normalized exposure pressure.' },
                  ]).map((mode) => {
                    const selected = mediaInputMode === mode.id
                    return (
                      <button
                        key={mode.id}
                        type="button"
                        onClick={() => setMediaInputMode(mode.id)}
                        style={{
                          textAlign: 'left',
                          padding: t.space.md,
                          borderRadius: t.radius.md,
                          border: `1px solid ${selected ? t.color.accent : t.color.borderLight}`,
                          background: selected ? t.color.accentMuted : t.color.surface,
                          color: t.color.text,
                          cursor: 'pointer',
                        }}
                      >
                        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>{mode.label}</div>
                        <div style={{ marginTop: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{mode.description}</div>
                      </button>
                    )
                  })}
                </div>
              </div>

              {covariateOptions.length > 0 && (
                <div>
                  <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                    Optional covariates
                  </label>
                  <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>
                    {covariateOptions.join(', ')} (not yet used in builder)
                  </p>
                </div>
              )}
            </div>
          )}
        </div>

        <button
          type="button"
          onClick={() => buildMutation.mutate()}
          disabled={!selectedChannels.length || buildMutation.isPending}
          style={{
            padding: `${t.space.md}px ${t.space.xl}px`,
            fontSize: t.font.sizeBase,
            fontWeight: t.font.weightSemibold,
            color: '#fff',
            background: !selectedChannels.length || buildMutation.isPending ? t.color.border : t.color.accent,
            border: 'none',
            borderRadius: t.radius.sm,
            cursor: !selectedChannels.length || buildMutation.isPending ? 'not-allowed' : 'pointer',
          }}
        >
          {buildMutation.isPending ? 'Generating…' : 'Generate dataset'}
        </button>
        {buildMutation.isError && (
          <p style={{ marginTop: t.space.sm, fontSize: t.font.sizeSm, color: t.color.danger }}>
            {(buildMutation.error as Error).message}
          </p>
        )}
      </div>
    </div>
  )
}
