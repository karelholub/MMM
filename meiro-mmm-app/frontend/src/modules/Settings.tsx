import { useEffect, useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'

interface AttributionSettings {
  lookback_window_days: number
  use_converted_flag: boolean
  min_conversion_value: number
  time_decay_half_life_days: number
  position_first_pct: number
  position_last_pct: number
  markov_min_paths: number
}

interface MMMSettings {
  frequency: string
}

interface NBASettings {
  min_prefix_support: number
  min_conversion_rate: number
  max_prefix_depth: number
}

interface Settings {
  attribution: AttributionSettings
  mmm: MMMSettings
  nba: NBASettings
}

interface ChannelRule {
  name: string
  channel: string
  source_regex?: string
  medium_regex?: string
}

interface Taxonomy {
  channel_rules: ChannelRule[]
  source_aliases: Record<string, string>
  medium_aliases: Record<string, string>
}

interface KpiDefinition {
  id: string
  label: string
  type: 'primary' | 'micro'
  event_name: string
  value_field?: string | null
  weight: number
  lookback_days?: number | null
}

interface KpiConfig {
  definitions: KpiDefinition[]
  primary_kpi_id?: string | null
}

interface ModelConfigSummary {
  id: string
  name: string
  status: string
  version: number
  parent_id?: string | null
  created_at?: string
  updated_at?: string
  activated_at?: string | null
}

interface ModelConfigDetail extends ModelConfigSummary {
  config_json: Record<string, any>
}

const DEFAULT_MODEL_CONFIG_JSON: Record<string, any> = {
  attribution: {
    eligible_touchpoints: {
      include_channels: ['paid_search', 'paid_social', 'email', 'affiliate'],
      exclude_channels: ['direct'],
      include_event_types: ['ad_click', 'ad_impression', 'email_click', 'site_visit'],
      exclude_event_types: [],
    },
    dedup_rules: {
      prefer_server_over_client: true,
      dedup_key_fields: ['event_id', 'platform_event_id', 'meiro_event_hash'],
    },
  },
  windows: {
    click_lookback_days: 30,
    impression_lookback_days: 7,
    session_timeout_minutes: 30,
    conversion_latency_days: 7,
  },
  conversions: {
    primary_conversion_key: 'purchase',
    conversion_definitions: [
      {
        key: 'purchase',
        name: 'Purchase',
        event_name: 'order_completed',
        filters: [
          { field: 'currency', op: 'in', value: ['EUR', 'CZK'] },
          { field: 'revenue', op: '>', value: 0 },
        ],
        value_field: 'revenue',
        dedup_mode: 'order_id',
        attribution_model_default: 'data_driven',
      },
      {
        key: 'lead',
        name: 'Qualified Lead',
        event_name: 'lead_submitted',
        filters: [{ field: 'lead_quality', op: '>=', value: 60 }],
        value_field: null,
        dedup_mode: 'lead_id',
        attribution_model_default: 'position_based',
      },
    ],
  },
}

const DEFAULT_SETTINGS: Settings = {
  attribution: {
    lookback_window_days: 30,
    use_converted_flag: true,
    min_conversion_value: 0,
    time_decay_half_life_days: 7,
    position_first_pct: 0.4,
    position_last_pct: 0.4,
    markov_min_paths: 5,
  },
  mmm: {
    frequency: 'W',
  },
  nba: {
    min_prefix_support: 5,
    min_conversion_rate: 0.01,
    max_prefix_depth: 5,
  },
}

export default function SettingsPage() {
  const settingsQuery = useQuery<Settings>({
    queryKey: ['settings'],
    queryFn: async () => {
      const res = await fetch('/api/settings')
      if (!res.ok) throw new Error('Failed to load settings')
      return res.json()
    },
  })

  const [local, setLocal] = useState<Settings>(DEFAULT_SETTINGS)
  const [savedAt, setSavedAt] = useState<string | null>(null)

  const taxonomyQuery = useQuery<Taxonomy>({
    queryKey: ['taxonomy'],
    queryFn: async () => {
      const res = await fetch('/api/taxonomy')
      if (!res.ok) throw new Error('Failed to load taxonomy')
      return res.json()
    },
  })

  const [taxonomyLocal, setTaxonomyLocal] = useState<Taxonomy>({
    channel_rules: [],
    source_aliases: {},
    medium_aliases: {},
  })

  const kpiQuery = useQuery<KpiConfig>({
    queryKey: ['kpis'],
    queryFn: async () => {
      const res = await fetch('/api/kpis')
      if (!res.ok) throw new Error('Failed to load KPI config')
      return res.json()
    },
  })

  const [kpiLocal, setKpiLocal] = useState<KpiConfig>({ definitions: [], primary_kpi_id: undefined })
  const [modelConfigs, setModelConfigs] = useState<ModelConfigSummary[]>([])
  const [selectedModelConfigId, setSelectedModelConfigId] = useState<string | null>(null)
  const [modelConfigJson, setModelConfigJson] = useState<string>('')

  useEffect(() => {
    if (kpiQuery.data) {
      setKpiLocal(kpiQuery.data)
    }
  }, [kpiQuery.data])

  useEffect(() => {
    if (taxonomyQuery.data) {
      setTaxonomyLocal(taxonomyQuery.data)
    }
  }, [taxonomyQuery.data])

  useEffect(() => {
    if (settingsQuery.data) {
      setLocal(settingsQuery.data)
    }
  }, [settingsQuery.data])

  const modelConfigsQuery = useQuery<ModelConfigSummary[]>({
    queryKey: ['model-configs'],
    queryFn: async () => {
      const res = await fetch('/api/model-configs')
      if (!res.ok) throw new Error('Failed to load model configs')
      return res.json()
    },
  })

  useEffect(() => {
    if (modelConfigsQuery.data) {
      setModelConfigs(modelConfigsQuery.data)
    }
  }, [modelConfigsQuery.data])

  const loadModelConfigDetail = async (id: string) => {
    setSelectedModelConfigId(id)
    try {
      const res = await fetch(`/api/model-configs/${id}`)
      if (!res.ok) throw new Error('Failed to load config detail')
      const data: ModelConfigDetail = await res.json()
      setModelConfigJson(JSON.stringify(data.config_json, null, 2))
    } catch {
      // leave as-is on error
    }
  }

  const saveMutation = useMutation({
    mutationFn: async (settings: Settings) => {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
      })
      if (!res.ok) throw new Error('Failed to save settings')
      return res.json()
    },
    onSuccess: () => {
      setSavedAt(new Date().toLocaleTimeString())
      taxonomyQuery.refetch()
    },
  })

  const saveTaxonomyMutation = useMutation({
    mutationFn: async (tax: Taxonomy) => {
      const res = await fetch('/api/taxonomy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(tax),
      })
      if (!res.ok) throw new Error('Failed to save taxonomy')
      return res.json()
    },
    onSuccess: () => {
      taxonomyQuery.refetch()
    },
  })

  const saveKpiMutation = useMutation({
    mutationFn: async (cfg: KpiConfig) => {
      const res = await fetch('/api/kpis', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      })
      if (!res.ok) throw new Error('Failed to save KPI config')
      return res.json()
    },
    onSuccess: () => {
      kpiQuery.refetch()
    },
  })

  const s = local

  if (settingsQuery.isLoading) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl * 2,
          textAlign: 'center',
          boxShadow: t.shadowSm,
        }}
      >
        <p style={{ fontSize: t.font.sizeBase, color: t.color.textSecondary, margin: 0 }}>
          Loading settings…
        </p>
      </div>
    )
  }

  if (settingsQuery.isError) {
    return (
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.danger}`,
          borderRadius: t.radius.lg,
          padding: t.space.xxl,
          boxShadow: t.shadowSm,
        }}
      >
        <h3 style={{ margin: '0 0 8px', fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.danger }}>
          Failed to load settings
        </h3>
        <p style={{ margin: 0, fontSize: t.font.sizeMd, color: t.color.textSecondary }}>
          {(settingsQuery.error as Error)?.message}
        </p>
      </div>
    )
  }

  const handleNumberChange = (path: (s: Settings) => number, setter: (s: Settings, v: number) => Settings) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = parseFloat(e.target.value)
    if (!Number.isFinite(v)) return
    setLocal((prev) => setter(prev, v))
  }

  const handleCheckboxChange = (setter: (s: Settings, v: boolean) => Settings) => (e: React.ChangeEvent<HTMLInputElement>) => {
    setLocal((prev) => setter(prev, e.target.checked))
  }

  const handleSelectChange = (setter: (s: Settings, v: string) => Settings) => (e: React.ChangeEvent<HTMLSelectElement>) => {
    setLocal((prev) => setter(prev, e.target.value))
  }

  return (
    <div style={{ maxWidth: 1000, margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: t.space.xl, flexWrap: 'wrap', gap: t.space.md }}>
        <div>
          <h1 style={{ margin: 0, fontSize: t.font.size2xl, fontWeight: t.font.weightBold, color: t.color.text, letterSpacing: '-0.02em' }}>
            Settings
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Configure attribution models, MMM defaults, and Next Best Action thresholds.
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
          {savedAt && (
            <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              Last saved at {savedAt}
            </span>
          )}
          <button
            type="button"
            onClick={() => saveMutation.mutate(local)}
            disabled={saveMutation.isPending}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.surface,
              backgroundColor: t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: saveMutation.isPending ? 'wait' : 'pointer',
            }}
          >
            {saveMutation.isPending ? 'Saving…' : 'Save settings'}
          </button>
        </div>
      </div>

      {/* Attribution settings */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Attribution</h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Adjust how multi-touch attribution models treat paths and decay credit over time.
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.lg }}>
          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Attribution window (days)
            </label>
            <input
              type="number"
              min={1}
              max={365}
              value={s.attribution.lookback_window_days}
              onChange={handleNumberChange(
                (st) => st.attribution.lookback_window_days,
                (st, v) => ({ ...st, attribution: { ...st.attribution, lookback_window_days: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>

          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Minimum conversion value
            </label>
            <input
              type="number"
              min={0}
              step={1}
              value={s.attribution.min_conversion_value}
              onChange={handleNumberChange(
                (st) => st.attribution.min_conversion_value,
                (st, v) => ({ ...st, attribution: { ...st.attribution, min_conversion_value: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm, marginTop: t.space.lg }}>
            <input
              id="use-converted-flag"
              type="checkbox"
              checked={s.attribution.use_converted_flag}
              onChange={handleCheckboxChange(
                (st, v) => ({ ...st, attribution: { ...st.attribution, use_converted_flag: v } }),
              )}
            />
            <label htmlFor="use-converted-flag" style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Use <code>converted</code> flag when present
            </label>
          </div>
        </div>

        <hr style={{ margin: `${t.space.lg}px 0`, border: 0, borderTop: `1px solid ${t.color.borderLight}` }} />

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.lg }}>
          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Time-decay half-life (days)
            </label>
            <input
              type="number"
              min={1}
              max={60}
              value={s.attribution.time_decay_half_life_days}
              onChange={handleNumberChange(
                (st) => st.attribution.time_decay_half_life_days,
                (st, v) => ({ ...st, attribution: { ...st.attribution, time_decay_half_life_days: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>

          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Position-based weights (first / last)
            </label>
            <div style={{ display: 'flex', gap: t.space.sm }}>
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={s.attribution.position_first_pct}
                onChange={handleNumberChange(
                  (st) => st.attribution.position_first_pct,
                  (st, v) => ({ ...st, attribution: { ...st.attribution, position_first_pct: v } }),
                )}
                style={{
                  flex: 1,
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                }}
              />
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={s.attribution.position_last_pct}
                onChange={handleNumberChange(
                  (st) => st.attribution.position_last_pct,
                  (st, v) => ({ ...st, attribution: { ...st.attribution, position_last_pct: v } }),
                )}
                style={{
                  flex: 1,
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                }}
              />
            </div>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeXs, color: t.color.textMuted }}>
              Middle touches share the remaining weight evenly.
            </p>
          </div>

          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Markov: minimum paths per channel
            </label>
            <input
              type="number"
              min={1}
              max={100}
              value={s.attribution.markov_min_paths}
              onChange={handleNumberChange(
                (st) => st.attribution.markov_min_paths,
                (st, v) => ({ ...st, attribution: { ...st.attribution, markov_min_paths: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>
        </div>
      </div>

      {/* MMM settings */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>MMM defaults</h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Defaults applied when starting new MMM runs (can be overridden per run via the Model Configurator).
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.lg }}>
          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Time aggregation
            </label>
            <select
              value={s.mmm.frequency}
              onChange={handleSelectChange(
                (st, v) => ({ ...st, mmm: { ...st.mmm, frequency: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                background: t.color.surface,
                color: t.color.text,
              }}
            >
              <option value="W">Weekly</option>
              <option value="M">Monthly</option>
            </select>
          </div>
        </div>
      </div>

      {/* NBA settings */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Next Best Action (NBA)</h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Control how aggressive NBA suggestions are by tuning support and conversion rate thresholds.
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.lg }}>
          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Minimum journeys per prefix
            </label>
            <input
              type="number"
              min={1}
              max={1000}
              value={s.nba.min_prefix_support}
              onChange={handleNumberChange(
                (st) => st.nba.min_prefix_support,
                (st, v) => ({ ...st, nba: { ...st.nba, min_prefix_support: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>

          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Minimum conversion rate for recommendation
            </label>
            <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
              <input
                type="number"
                min={0}
                max={100}
                step={0.5}
                value={s.nba.min_conversion_rate * 100}
                onChange={(e) => {
                  const v = parseFloat(e.target.value)
                  if (!Number.isFinite(v)) return
                  setLocal((prev) => ({ ...prev, nba: { ...prev.nba, min_conversion_rate: v / 100 } }))
                }}
                style={{
                  width: '100%',
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                }}
              />
              <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>%</span>
            </div>
          </div>

          <div>
            <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
              Maximum prefix depth
            </label>
            <input
              type="number"
              min={1}
              max={10}
              value={s.nba.max_prefix_depth}
              onChange={handleNumberChange(
                (st) => st.nba.max_prefix_depth,
                (st, v) => ({ ...st, nba: { ...st.nba, max_prefix_depth: v } }),
              )}
              style={{
                width: '100%',
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
              }}
            />
          </div>
        </div>
      </div>

      {/* KPI & conversion settings */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>KPI & conversions</h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Define primary KPIs (e.g. Purchases, Leads, Sign-ups) and micro-conversions (e.g. Add to cart, Product view). These can be used to map
          events and prioritize what matters in reporting and optimization.
        </p>

        {kpiQuery.isError && (
          <p style={{ margin: '0 0 12px', fontSize: t.font.sizeSm, color: t.color.danger }}>
            Failed to load KPI config: {(kpiQuery.error as Error)?.message}
          </p>
        )}

        <div style={{ overflowX: 'auto', marginBottom: t.space.md }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>ID</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Label</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Type</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Event name</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Value field</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', color: t.color.textSecondary }}>Weight</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right', color: t.color.textSecondary }}>Lookback (days)</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center', color: t.color.textSecondary }}>Primary</th>
                <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center', color: t.color.textSecondary }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {kpiLocal.definitions.map((d, idx) => (
                <tr key={d.id || idx} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                    <input
                      type="text"
                      value={d.id}
                      onChange={(e) =>
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], id: e.target.value }
                          return { ...prev, definitions: defs }
                        })
                      }
                      style={{
                        width: '100%',
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                      }}
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                    <input
                      type="text"
                      value={d.label}
                      onChange={(e) =>
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], label: e.target.value }
                          return { ...prev, definitions: defs }
                        })
                      }
                      style={{
                        width: '100%',
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                      }}
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                    <select
                      value={d.type}
                      onChange={(e) =>
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], type: e.target.value as 'primary' | 'micro' }
                          return { ...prev, definitions: defs }
                        })
                      }
                      style={{
                        width: '100%',
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                        background: t.color.surface,
                      }}
                    >
                      <option value="primary">Primary</option>
                      <option value="micro">Micro</option>
                    </select>
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                    <input
                      type="text"
                      value={d.event_name}
                      onChange={(e) =>
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], event_name: e.target.value }
                          return { ...prev, definitions: defs }
                        })
                      }
                      placeholder="e.g. purchase, lead_submitted"
                      style={{
                        width: '100%',
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                      }}
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                    <input
                      type="text"
                      value={d.value_field ?? ''}
                      onChange={(e) =>
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], value_field: e.target.value || undefined }
                          return { ...prev, definitions: defs }
                        })
                      }
                      placeholder="e.g. revenue"
                      style={{
                        width: '100%',
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                      }}
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                    <input
                      type="number"
                      step={0.1}
                      value={d.weight}
                      onChange={(e) => {
                        const v = parseFloat(e.target.value)
                        if (!Number.isFinite(v)) return
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], weight: v }
                          return { ...prev, definitions: defs }
                        })
                      }}
                      style={{
                        width: 80,
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                        textAlign: 'right',
                      }}
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                    <input
                      type="number"
                      min={1}
                      value={d.lookback_days ?? ''}
                      onChange={(e) => {
                        const v = parseFloat(e.target.value)
                        setKpiLocal((prev) => {
                          const defs = [...prev.definitions]
                          defs[idx] = { ...defs[idx], lookback_days: Number.isFinite(v) ? v : undefined }
                          return { ...prev, definitions: defs }
                        })
                      }}
                      style={{
                        width: 80,
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeSm,
                        border: `1px solid ${t.color.border}`,
                        borderRadius: t.radius.sm,
                        textAlign: 'right',
                      }}
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                    <input
                      type="radio"
                      name="primary-kpi"
                      checked={kpiLocal.primary_kpi_id === d.id}
                      onChange={() =>
                        setKpiLocal((prev) => ({
                          ...prev,
                          primary_kpi_id: d.id,
                        }))
                      }
                    />
                  </td>
                  <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                    <button
                      type="button"
                      onClick={() =>
                        setKpiLocal((prev) => ({
                          ...prev,
                          definitions: prev.definitions.filter((_, i) => i !== idx),
                          primary_kpi_id: prev.primary_kpi_id === d.id ? undefined : prev.primary_kpi_id,
                        }))
                      }
                      style={{
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        fontSize: t.font.sizeXs,
                        color: t.color.danger,
                        background: 'transparent',
                        border: 'none',
                        cursor: 'pointer',
                      }}
                    >
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() =>
              setKpiLocal((prev) => ({
                ...prev,
                definitions: [
                  ...prev.definitions,
                  { id: 'new_kpi', label: 'New KPI', type: 'primary', event_name: '', weight: 1, lookback_days: undefined },
                ],
              }))
            }
            style={{
              padding: `${t.space.sm}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.accent,
              background: 'transparent',
              border: `1px solid ${t.color.accent}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Add KPI
          </button>

          <button
            type="button"
            onClick={() => saveKpiMutation.mutate(kpiLocal)}
            disabled={saveKpiMutation.isPending}
            style={{
              padding: `${t.space.sm}px ${t.space.lg}px`,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              color: t.color.surface,
              backgroundColor: t.color.accent,
              border: 'none',
              borderRadius: t.radius.sm,
              cursor: saveKpiMutation.isPending ? 'wait' : 'pointer',
            }}
          >
            {saveKpiMutation.isPending ? 'Saving…' : 'Save KPIs'}
          </button>
        </div>
      </div>

      {/* Taxonomy */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>Taxonomy</h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Define how raw <code>source</code>, <code>medium</code>, and <code>utm_*</code> fields are mapped into standard channels and campaign structures.
        </p>

        {taxonomyQuery.isError && (
          <p style={{ margin: '0 0 12px', fontSize: t.font.sizeSm, color: t.color.danger }}>
            Failed to load taxonomy: {(taxonomyQuery.error as Error)?.message}
          </p>
        )}

        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: t.space.lg }}>
          <div>
            <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Channel mapping rules
            </h3>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Name</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Channel</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Source regex</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left', color: t.color.textSecondary }}>Medium regex</th>
                </tr>
              </thead>
              <tbody>
                {taxonomyLocal.channel_rules.map((r, idx) => (
                  <tr key={idx} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                    <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                      <input
                        type="text"
                        value={r.name}
                        onChange={(e) =>
                          setTaxonomyLocal((prev) => {
                            const next = { ...prev, channel_rules: [...prev.channel_rules] }
                            next.channel_rules[idx] = { ...next.channel_rules[idx], name: e.target.value }
                            return next
                          })
                        }
                        style={{
                          width: '100%',
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          fontSize: t.font.sizeSm,
                          border: `1px solid ${t.color.border}`,
                          borderRadius: t.radius.sm,
                        }}
                      />
                    </td>
                    <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                      <input
                        type="text"
                        value={r.channel}
                        onChange={(e) =>
                          setTaxonomyLocal((prev) => {
                            const next = { ...prev, channel_rules: [...prev.channel_rules] }
                            next.channel_rules[idx] = { ...next.channel_rules[idx], channel: e.target.value }
                            return next
                          })
                        }
                        style={{
                          width: '100%',
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          fontSize: t.font.sizeSm,
                          border: `1px solid ${t.color.border}`,
                          borderRadius: t.radius.sm,
                        }}
                      />
                    </td>
                    <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                      <input
                        type="text"
                        value={r.source_regex ?? ''}
                        placeholder="e.g. google|bing"
                        onChange={(e) =>
                          setTaxonomyLocal((prev) => {
                            const next = { ...prev, channel_rules: [...prev.channel_rules] }
                            next.channel_rules[idx] = { ...next.channel_rules[idx], source_regex: e.target.value || undefined }
                            return next
                          })
                        }
                        style={{
                          width: '100%',
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          fontSize: t.font.sizeSm,
                          border: `1px solid ${t.color.border}`,
                          borderRadius: t.radius.sm,
                        }}
                      />
                    </td>
                    <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                      <input
                        type="text"
                        value={r.medium_regex ?? ''}
                        placeholder="e.g. cpc|ppc"
                        onChange={(e) =>
                          setTaxonomyLocal((prev) => {
                            const next = { ...prev, channel_rules: [...prev.channel_rules] }
                            next.channel_rules[idx] = { ...next.channel_rules[idx], medium_regex: e.target.value || undefined }
                            return next
                          })
                        }
                        style={{
                          width: '100%',
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          fontSize: t.font.sizeSm,
                          border: `1px solid ${t.color.border}`,
                          borderRadius: t.radius.sm,
                        }}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <button
              type="button"
              onClick={() =>
                setTaxonomyLocal((prev) => ({
                  ...prev,
                  channel_rules: [...prev.channel_rules, { name: 'New rule', channel: '', source_regex: '', medium_regex: '' }],
                }))
              }
              style={{
                marginTop: t.space.md,
                padding: `${t.space.sm}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                color: t.color.accent,
                background: 'transparent',
                border: `1px solid ${t.color.accent}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Add rule
            </button>
          </div>

          <div>
            <h3 style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Aliases
            </h3>
            <p style={{ margin: '0 0 8px', fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              Normalize common variations of <code>source</code> and <code>medium</code> (e.g. fb → facebook).
            </p>
            <div style={{ marginBottom: t.space.md }}>
              <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
                Source aliases (JSON)
              </label>
              <textarea
                value={JSON.stringify(taxonomyLocal.source_aliases, null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value || '{}')
                    setTaxonomyLocal((prev) => ({ ...prev, source_aliases: parsed }))
                  } catch {
                    // ignore parse errors for now
                  }
                }}
                rows={4}
                style={{
                  width: '100%',
                  fontFamily: 'monospace',
                  fontSize: t.font.sizeXs,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                }}
              />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.textSecondary, marginBottom: t.space.xs }}>
                Medium aliases (JSON)
              </label>
              <textarea
                value={JSON.stringify(taxonomyLocal.medium_aliases, null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value || '{}')
                    setTaxonomyLocal((prev) => ({ ...prev, medium_aliases: parsed }))
                  } catch {
                    // ignore parse errors
                  }
                }}
                rows={4}
                style={{
                  width: '100%',
                  fontFamily: 'monospace',
                  fontSize: t.font.sizeXs,
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                }}
              />
            </div>

            <button
              type="button"
              onClick={() => saveTaxonomyMutation.mutate(taxonomyLocal)}
              disabled={saveTaxonomyMutation.isPending}
              style={{
                marginTop: t.space.md,
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                color: t.color.surface,
                backgroundColor: t.color.accent,
                border: 'none',
                borderRadius: t.radius.sm,
                cursor: saveTaxonomyMutation.isPending ? 'wait' : 'pointer',
              }}
            >
              {saveTaxonomyMutation.isPending ? 'Saving…' : 'Save taxonomy'}
            </button>
          </div>
        </div>
      </div>

      {/* Measurement model configs (versioned attribution model configs) */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.xl,
          boxShadow: t.shadowSm,
          marginBottom: t.space.xl,
        }}
      >
        <h2
          style={{
            margin: '0 0 8px',
            fontSize: t.font.sizeMd,
            fontWeight: t.font.weightSemibold,
            color: t.color.text,
          }}
        >
          Measurement model configs
        </h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Versioned attribution configurations that define eligible touchpoints, conversion definitions, and time windows. Draft
          configs can be edited and then activated to become the default for new reports.
        </p>

        {modelConfigsQuery.isError && (
          <p style={{ margin: '0 0 12px', fontSize: t.font.sizeSm, color: t.color.danger }}>
            Failed to load model configs: {(modelConfigsQuery.error as Error)?.message}
          </p>
        )}

        {modelConfigs.length === 0 && !modelConfigsQuery.isLoading && (
          <div
            style={{
              marginBottom: t.space.md,
              padding: t.space.md,
              borderRadius: t.radius.md,
              background: t.color.bg,
              border: `1px dashed ${t.color.borderLight}`,
            }}
          >
            <p style={{ margin: `0 0 ${t.space.sm}px`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              No measurement configs yet. Create a default paid media config based on the recommended template.
            </p>
            <button
              type="button"
              onClick={async () => {
                try {
                  const res = await fetch('/api/model-configs', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      name: 'Default Paid Media Config',
                      created_by: 'ui',
                      change_note: 'Initial default config from Settings UI',
                      config_json: DEFAULT_MODEL_CONFIG_JSON,
                    }),
                  })
                  if (!res.ok) throw new Error('Failed to create config')
                  const created = await res.json()
                  await modelConfigsQuery.refetch()
                  if (created?.id) {
                    void loadModelConfigDetail(created.id)
                  }
                } catch (err) {
                  // Fallback simple notification
                  alert((err as Error).message)
                }
              }}
              style={{
                padding: `${t.space.sm}px ${t.space.lg}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                color: t.color.surface,
                backgroundColor: t.color.accent,
                border: 'none',
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Create default config
            </button>
          </div>
        )}

        <div style={{ display: 'grid', gridTemplateColumns: '260px 1fr', gap: t.space.lg, alignItems: 'stretch' }}>
          <div
            style={{
              borderRight: `1px solid ${t.color.borderLight}`,
              paddingRight: t.space.lg,
              maxHeight: 360,
              overflowY: 'auto',
            }}
          >
            <h3
              style={{
                margin: `0 0 ${t.space.sm}px`,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightSemibold,
                color: t.color.text,
              }}
            >
              Config versions
            </h3>
            <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
              {modelConfigs.map((cfg) => (
                <li key={cfg.id}>
                  <button
                    type="button"
                    onClick={() => loadModelConfigDetail(cfg.id)}
                    style={{
                      display: 'block',
                      width: '100%',
                      textAlign: 'left',
                      padding: `${t.space.sm}px ${t.space.sm}px`,
                      marginBottom: t.space.xs,
                      borderRadius: t.radius.sm,
                      border:
                        selectedModelConfigId === cfg.id
                          ? `1px solid ${t.color.accent}`
                          : '1px solid transparent',
                      backgroundColor:
                        selectedModelConfigId === cfg.id ? t.color.accentMuted : 'transparent',
                      cursor: 'pointer',
                      fontSize: t.font.sizeSm,
                      color: t.color.text,
                    }}
                  >
                    <div style={{ fontWeight: t.font.weightMedium }}>
                      {cfg.name} <span style={{ color: t.color.textMuted }}>v{cfg.version}</span>
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      {cfg.status}{' '}
                      {cfg.activated_at ? `• activated ${new Date(cfg.activated_at).toLocaleDateString()}` : ''}
                    </div>
                  </button>
                </li>
              ))}
            </ul>
          </div>

          <div>
            {selectedModelConfigId ? (
              <>
                <p
                  style={{
                    margin: '0 0 8px',
                    fontSize: t.font.sizeSm,
                    color: t.color.textSecondary,
                  }}
                >
                  Edit the JSON config for the selected draft. For most setups you can start from the template below and adjust
                  channels, conversion definitions, and windows.
                </p>
                <textarea
                  value={modelConfigJson}
                  onChange={(e) => setModelConfigJson(e.target.value)}
                  rows={18}
                  style={{
                    width: '100%',
                    fontFamily: 'monospace',
                    fontSize: t.font.sizeXs,
                    border: `1px solid ${t.color.border}`,
                    borderRadius: t.radius.sm,
                    padding: t.space.sm,
                    marginBottom: t.space.md,
                    whiteSpace: 'pre',
                  }}
                />
                <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap' }}>
                  <button
                    type="button"
                    onClick={async () => {
                      if (!selectedModelConfigId) return
                      let parsed: any
                      try {
                        parsed = JSON.parse(modelConfigJson || '{}')
                      } catch {
                        alert('Config JSON is not valid JSON.')
                        return
                      }
                      await fetch(`/api/model-configs/${selectedModelConfigId}`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ config_json: parsed, actor: 'ui' }),
                      })
                      modelConfigsQuery.refetch()
                    }}
                    style={{
                      padding: `${t.space.sm}px ${t.space.lg}px`,
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.surface,
                      backgroundColor: t.color.accent,
                      border: 'none',
                      borderRadius: t.radius.sm,
                      cursor: 'pointer',
                    }}
                  >
                    Save draft
                  </button>
                  <button
                    type="button"
                    onClick={async () => {
                      if (!selectedModelConfigId) return
                      await fetch(`/api/model-configs/${selectedModelConfigId}/activate`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ actor: 'ui', set_as_default: true }),
                      })
                      modelConfigsQuery.refetch()
                    }}
                    style={{
                      padding: `${t.space.sm}px ${t.space.lg}px`,
                      fontSize: t.font.sizeSm,
                      fontWeight: t.font.weightMedium,
                      color: t.color.surface,
                      backgroundColor: t.color.success,
                      border: 'none',
                      borderRadius: t.radius.sm,
                      cursor: 'pointer',
                    }}
                  >
                    Activate & set as default
                  </button>
                </div>
              </>
            ) : (
              <p
                style={{
                  margin: 0,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                }}
              >
                Select a config on the left to view and edit its JSON definition. Use the KPI & taxonomy sections above to define
                which events and channels feed into these configs.
              </p>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

