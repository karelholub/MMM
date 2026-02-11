import { useState, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens } from '../theme/tokens'
import DatasetUploader from './DatasetUploader'

const t = tokens

export type DataSourceType = 'platform' | 'csv'

interface PlatformOptions {
  spend_channels: string[]
  covariates: string[]
}

interface BuildResponse {
  dataset_id: string
  columns: string[]
  preview_rows: Record<string, unknown>[]
  coverage: { n_weeks: number; missing_spend_weeks: Record<string, number>; missing_kpi_weeks: number }
  metadata: {
    period_start: string
    period_end: string
    kpi_target: string
    kpi_column: string
    spend_channels: string[]
    covariates: string[]
    currency: string
    attribution_model?: string
    attribution_config_id?: string
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
}

export default function MMMDataSourceStep({ onMappingComplete }: MMMDataSourceStepProps) {
  const [sourceType, setSourceType] = useState<DataSourceType>('platform')
  const [kpiTarget, setKpiTarget] = useState<'sales' | 'attribution'>('sales')
  const [dateStart, setDateStart] = useState(() => {
    const d = new Date()
    d.setMonth(d.getMonth() - 6)
    return d.toISOString().slice(0, 10)
  })
  const [dateEnd, setDateEnd] = useState(() => new Date().toISOString().slice(0, 10))
  const [selectedChannels, setSelectedChannels] = useState<string[]>([])
  const [platformResult, setPlatformResult] = useState<BuildResponse | null>(null)
  const [attributionModel, setAttributionModel] = useState<string>('linear')
  const [attributionConfigId, setAttributionConfigId] = useState<string | null>(null)

  const { data: platformOptions } = useQuery<PlatformOptions>({
    queryKey: ['mmm-platform-options'],
    queryFn: async () => {
      const res = await fetch('/api/mmm/platform-options')
      if (!res.ok) throw new Error('Failed to load options')
      return res.json()
    },
  })

  const { data: attributionModelsData } = useQuery<{ models: string[] }>({
    queryKey: ['attribution-models'],
    queryFn: async () => {
      const res = await fetch('/api/attribution/models')
      if (!res.ok) return { models: ['linear', 'last_touch', 'first_touch', 'time_decay', 'position_based', 'markov'] }
      return res.json()
    },
  })
  const { data: modelConfigs } = useQuery<{ id: string; name: string }[]>({
    queryKey: ['model-configs'],
    queryFn: async () => {
      const res = await fetch('/api/model-configs')
      if (!res.ok) return []
      const list = await res.json()
      return Array.isArray(list) ? list : []
    },
  })

  const attributionModelOptions = attributionModelsData?.models ?? ['linear', 'last_touch', 'first_touch', 'time_decay', 'position_based', 'markov']
  const spendChannelOptions = platformOptions?.spend_channels ?? []
  const covariateOptions = platformOptions?.covariates ?? []

  useEffect(() => {
    if (spendChannelOptions.length && !selectedChannels.length) {
      setSelectedChannels(spendChannelOptions.filter((ch) => ['google_ads', 'meta_ads', 'linkedin_ads'].includes(ch)))
    }
  }, [spendChannelOptions.join(','), selectedChannels.length])

  const buildMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/mmm/datasets/build-from-platform', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date_start: dateStart,
          date_end: dateEnd,
          kpi_target: kpiTarget,
          spend_channels: selectedChannels,
          covariates: covariateOptions.length ? [] : undefined,
          currency: 'USD',
          ...(kpiTarget === 'attribution' && {
            attribution_model: attributionModel,
            ...(attributionConfigId && { attribution_config_id: attributionConfigId }),
          }),
        }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Failed to generate dataset')
      }
      return res.json() as Promise<BuildResponse>
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
          </p>
        </div>

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
            style={{
              padding: `${t.space.md}px ${t.space.xl}px`,
              fontSize: t.font.sizeBase,
              fontWeight: t.font.weightSemibold,
              color: '#fff',
              background: t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: 'pointer',
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
              border: `2px solid ${sourceType === 'platform' ? t.color.accent : t.color.border}`,
              background: sourceType === 'platform' ? t.color.accentMuted : t.color.surface,
              color: sourceType === 'platform' ? t.color.accent : t.color.textSecondary,
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
              border: `2px solid ${sourceType === 'csv' ? t.color.accent : t.color.border}`,
              background: sourceType === 'csv' ? t.color.accentMuted : t.color.surface,
              color: sourceType === 'csv' ? t.color.accent : t.color.textSecondary,
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

        {covariateOptions.length > 0 && (
          <div style={{ marginBottom: t.space.lg }}>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.sm }}>
              Optional covariates
            </label>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>
              {covariateOptions.join(', ')} (not yet used in builder)
            </p>
          </div>
        )}

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
