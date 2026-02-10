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

  useEffect(() => {
    if (settingsQuery.data) {
      setLocal(settingsQuery.data)
    }
  }, [settingsQuery.data])

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
    </div>
  )
}

