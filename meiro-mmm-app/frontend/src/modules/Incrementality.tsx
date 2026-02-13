import { useState, useMemo } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { apiGetJson, apiSendJson } from '../lib/apiClient'

const OWNED_CHANNELS = ['email', 'push', 'sms', 'whatsapp', 'onsite']

interface ExperimentSummary {
  id: number
  name: string
  channel: string
  start_at: string
  end_at: string
  status: string
  conversion_key?: string | null
}

interface ExperimentDetail extends ExperimentSummary {
  notes?: string | null
}

interface ExperimentResults {
  experiment_id: number
  status: string
  treatment?: {
    n: number
    conversions: number
    conversion_rate: number
    total_value: number
  }
  control?: {
    n: number
    conversions: number
    conversion_rate: number
    total_value: number
  }
  uplift_abs?: number | null
  uplift_rel?: number | null
  ci_low?: number | null
  ci_high?: number | null
  p_value?: number | null
  insufficient_data?: boolean
}

interface TimeSeriesPoint {
  date: string
  treatment_n: number
  treatment_conversions: number
  treatment_rate: number
  control_n: number
  control_conversions: number
  control_rate: number
  uplift_abs: number
  uplift_rel: number | null
}

interface PowerAnalysisResult {
  total_sample_size: number
  treatment_size: number
  control_size: number
  baseline_rate: number
  mde: number
  alpha: number
  power: number
}

type ReadyLabel = 'not_ready' | 'early' | 'ready'

interface ExperimentHealth {
  experiment_id: number
  sample: { treatment: number; control: number }
  exposures: { treatment: number; control: number }
  outcomes: { treatment: number; control: number }
  balance: { status: 'ok' | 'warn'; expected_share: number; observed_share: number }
  data_completeness: {
    assignments: { status: 'ok' | 'fail' }
    outcomes: { status: 'ok' | 'fail' }
    exposures: { status: 'ok' | 'warn' }
  }
  overlap_risk: { status: 'ok' | 'warn'; overlapping_profiles: number }
  ready_state: { label: ReadyLabel; reasons: string[] }
}

export default function IncrementalityPage() {
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [showPowerCalc, setShowPowerCalc] = useState(false)
  const [showTimeSeries, setShowTimeSeries] = useState(false)
  const [showAdvancedSetup, setShowAdvancedSetup] = useState(false)
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<'all' | 'draft' | 'running' | 'stopped'>('all')
  const [powerPlans, setPowerPlans] = useState<Record<number, PowerAnalysisResult>>({})
  const [form, setForm] = useState({
    name: '',
    channel: '',
    conversion_key: '',
    start_at: '',
    end_at: '',
    notes: '',
    experiment_type: 'holdout' as 'holdout',
    treatment_rate: 0.9,
    min_runtime_days: '',
    stop_rule: '',
    exclusion_window_days: '',
  })
  const [powerForm, setPowerForm] = useState({
    baseline_rate: 0.05,
    mde: 0.01,
    alpha: 0.05,
    power: 0.8,
    treatment_rate: 0.5,
  })

  const experimentsQuery = useQuery<ExperimentSummary[]>({
    queryKey: ['experiments'],
    queryFn: async () => apiGetJson<ExperimentSummary[]>('/api/experiments', { fallbackMessage: 'Failed to load experiments' }),
  })

  const detailQuery = useQuery<ExperimentDetail>({
    queryKey: ['experiment', selectedId],
    queryFn: async () => apiGetJson<ExperimentDetail>(`/api/experiments/${selectedId}`, { fallbackMessage: 'Failed to load experiment' }),
    enabled: selectedId != null,
  })

  const resultsQuery = useQuery<ExperimentResults>({
    queryKey: ['experiment-results', selectedId],
    queryFn: async () => apiGetJson<ExperimentResults>(`/api/experiments/${selectedId}/results`, { fallbackMessage: 'Failed to load results' }),
    enabled: selectedId != null,
  })

  const timeSeriesQuery = useQuery<{ data: TimeSeriesPoint[] }>({
    queryKey: ['experiment-timeseries', selectedId],
    queryFn: async () => apiGetJson<{ data: TimeSeriesPoint[] }>(`/api/experiments/${selectedId}/time-series?freq=D`, {
      fallbackMessage: 'Failed to load time series',
    }),
    enabled: selectedId != null && showTimeSeries,
  })

  const powerMutation = useMutation<PowerAnalysisResult>({
    mutationFn: async () =>
      apiSendJson<PowerAnalysisResult>('/api/experiments/power-analysis', 'POST', powerForm, {
        fallbackMessage: 'Failed to compute power analysis',
      }),
  })

  const statusMutation = useMutation<ExperimentSummary, Error, { id: number; nextStatus: 'draft' | 'running' | 'completed' }>({
    mutationFn: async ({ id, nextStatus }) => {
      return apiSendJson<ExperimentSummary>(`/api/experiments/${id}/status`, 'POST', { status: nextStatus }, {
        fallbackMessage: 'Failed to update experiment status',
      })
    },
    onSuccess: () => {
      experimentsQuery.refetch()
      resultsQuery.refetch()
    },
  })

  const healthQuery = useQuery<ExperimentHealth>({
    queryKey: ['experiment-health', selectedId],
    queryFn: async () => apiGetJson<ExperimentHealth>(`/api/experiments/${selectedId}/health`, {
      fallbackMessage: 'Failed to load experiment health',
    }),
    enabled: selectedId != null,
  })

  const createMutation = useMutation({
    mutationFn: async () => {
      if (!form.name || !form.channel || !form.start_at || !form.end_at || !form.conversion_key) {
        throw new Error('Name, channel, primary metric, start and end are required')
      }
      const plannedBits: string[] = []
      plannedBits.push(`Type: holdout (unit: profile_id, assignment: random hash)`)
      plannedBits.push(
        `Planned split: ${(form.treatment_rate * 100).toFixed(0)}% treatment / ${(
          (1 - form.treatment_rate) *
          100
        ).toFixed(0)}% control`
      )
      if (form.min_runtime_days) {
        plannedBits.push(`Minimum runtime: ${form.min_runtime_days} days (not auto-enforced)`)
      }
      if (form.stop_rule) {
        plannedBits.push(`Stop rule: ${form.stop_rule} (operator-reviewed, not auto-enforced)`)
      }
      if (form.exclusion_window_days) {
        plannedBits.push(
          `Exclusion rule: exclude profiles converted in last ${form.exclusion_window_days} days (not enforced yet)`
        )
      }
      const planNote = plannedBits.length ? `Experiment plan (not enforced by system yet):\n- ${plannedBits.join('\n- ')}` : ''
      const combinedNotes = [form.notes, planNote].filter(Boolean).join('\n\n')
      const body = {
        name: form.name,
        channel: form.channel,
        conversion_key: form.conversion_key,
        start_at: new Date(form.start_at).toISOString(),
        end_at: new Date(form.end_at).toISOString(),
        notes: combinedNotes || null,
      }
      return apiSendJson<ExperimentSummary>('/api/experiments', 'POST', body, {
        fallbackMessage: 'Failed to create experiment',
      })
    },
    onSuccess: (data) => {
      experimentsQuery.refetch()
      setSelectedId(data.id)
      setForm({
        name: '',
        channel: '',
        conversion_key: '',
        start_at: '',
        end_at: '',
        notes: '',
        experiment_type: 'holdout',
        treatment_rate: 0.9,
        min_runtime_days: '',
        stop_rule: '',
        exclusion_window_days: '',
      })
    },
  })

  const tkn = t

  const selectedSummary =
    selectedId != null ? experimentsQuery.data?.find((e) => e.id === selectedId) : undefined

  const statusLabel = (status: string): string => {
    if (status === 'completed') return 'Stopped'
    if (status === 'running') return 'Running'
    if (status === 'draft') return 'Draft'
    return status
  }

  const readyBadgeLabel = (ready?: ReadyLabel): string => {
    if (!ready) return 'Unknown'
    if (ready === 'ready') return 'Ready'
    if (ready === 'early') return 'Early'
    return 'Not ready'
  }

  const filteredExperiments: ExperimentSummary[] = useMemo(() => {
    const base = experimentsQuery.data ?? []
    const q = search.trim().toLowerCase()
    let items = base.filter((e) => {
      const matchesSearch =
        !q ||
        e.name.toLowerCase().includes(q) ||
        e.channel.toLowerCase().includes(q) ||
        (e.conversion_key ?? '').toLowerCase().includes(q)
      let matchesStatus = true
      if (statusFilter === 'draft') {
        matchesStatus = e.status === 'draft'
      } else if (statusFilter === 'running') {
        matchesStatus = e.status === 'running'
      } else if (statusFilter === 'stopped') {
        matchesStatus = e.status === 'completed'
      }
      return matchesSearch && matchesStatus
    })
    items = [...items].sort((a, b) => {
      const order = (s: string) => (s === 'running' ? 0 : s === 'draft' ? 1 : 2)
      const sa = order(a.status)
      const sb = order(b.status)
      if (sa !== sb) return sa - sb
      return b.id - a.id
    })
    return items
  }, [experimentsQuery.data, search, statusFilter])

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: tkn.space.xl,
          gap: tkn.space.md,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <h1
            style={{
              margin: 0,
              fontSize: tkn.font.size2xl,
              fontWeight: tkn.font.weightBold,
              color: tkn.color.text,
              letterSpacing: '-0.02em',
            }}
          >
            Incrementality experiments
          </h1>
          <p style={{ margin: `${tkn.space.xs}px 0 0`, fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
            Define simple holdout tests for owned channels (e.g. email or push) and monitor uplift in conversion rate.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowPowerCalc(!showPowerCalc)}
          style={{
            padding: `${tkn.space.sm}px ${tkn.space.lg}px`,
            fontSize: tkn.font.sizeSm,
            fontWeight: tkn.font.weightMedium,
            color: tkn.color.accent,
            backgroundColor: tkn.color.surface,
            border: `1px solid ${tkn.color.accent}`,
            borderRadius: tkn.radius.sm,
            cursor: 'pointer',
          }}
        >
          {showPowerCalc ? 'Hide' : 'Show'} power calculator
        </button>
      </div>

      {showPowerCalc && (
        <div
          style={{
            background: tkn.color.surface,
            border: `1px solid ${tkn.color.borderLight}`,
            borderRadius: tkn.radius.lg,
            padding: tkn.space.xl,
            boxShadow: tkn.shadowSm,
            marginBottom: tkn.space.lg,
          }}
        >
          <h2
            style={{
              margin: '0 0 8px',
              fontSize: tkn.font.sizeMd,
              fontWeight: tkn.font.weightSemibold,
              color: tkn.color.text,
            }}
          >
            Power analysis
          </h2>
          <p style={{ margin: '0 0 12px', fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
            Estimate required sample size to detect a given effect size with statistical confidence.
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: tkn.space.md }}>
            <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
              Baseline rate (e.g., 0.05 = 5%)
              <input
                type="number"
                step="0.001"
                value={powerForm.baseline_rate}
                onChange={(e) => setPowerForm((f) => ({ ...f, baseline_rate: parseFloat(e.target.value) || 0 }))}
                style={{
                  width: '100%',
                  marginTop: 4,
                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                  border: `1px solid ${tkn.color.border}`,
                  borderRadius: tkn.radius.sm,
                  fontSize: tkn.font.sizeSm,
                }}
              />
            </label>
            <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
              MDE (e.g., 0.01 = 1pp)
              <input
                type="number"
                step="0.001"
                value={powerForm.mde}
                onChange={(e) => setPowerForm((f) => ({ ...f, mde: parseFloat(e.target.value) || 0 }))}
                style={{
                  width: '100%',
                  marginTop: 4,
                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                  border: `1px solid ${tkn.color.border}`,
                  borderRadius: tkn.radius.sm,
                  fontSize: tkn.font.sizeSm,
                }}
              />
            </label>
            <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
              Significance (α)
              <input
                type="number"
                step="0.01"
                value={powerForm.alpha}
                onChange={(e) => setPowerForm((f) => ({ ...f, alpha: parseFloat(e.target.value) || 0 }))}
                style={{
                  width: '100%',
                  marginTop: 4,
                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                  border: `1px solid ${tkn.color.border}`,
                  borderRadius: tkn.radius.sm,
                  fontSize: tkn.font.sizeSm,
                }}
              />
            </label>
            <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
              Power (1-β)
              <input
                type="number"
                step="0.01"
                value={powerForm.power}
                onChange={(e) => setPowerForm((f) => ({ ...f, power: parseFloat(e.target.value) || 0 }))}
                style={{
                  width: '100%',
                  marginTop: 4,
                  padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                  border: `1px solid ${tkn.color.border}`,
                  borderRadius: tkn.radius.sm,
                  fontSize: tkn.font.sizeSm,
                }}
              />
            </label>
          </div>
          <button
            type="button"
            onClick={() => powerMutation.mutate()}
            disabled={powerMutation.isPending}
            style={{
              marginTop: tkn.space.md,
              padding: `${tkn.space.sm}px ${tkn.space.lg}px`,
              fontSize: tkn.font.sizeSm,
              fontWeight: tkn.font.weightMedium,
              color: tkn.color.surface,
              backgroundColor: tkn.color.accent,
              border: 'none',
              borderRadius: tkn.radius.sm,
              cursor: powerMutation.isPending ? 'wait' : 'pointer',
            }}
          >
            {powerMutation.isPending ? 'Computing…' : 'Calculate sample size'}
          </button>
          {powerMutation.data && (
            <div
              style={{
                marginTop: tkn.space.md,
                padding: tkn.space.md,
                background: tkn.color.accentMuted,
                borderRadius: tkn.radius.sm,
                fontSize: tkn.font.sizeSm,
                color: tkn.color.text,
              }}
            >
              <strong>Required sample size: {powerMutation.data.total_sample_size.toLocaleString()}</strong>
              <br />
              Treatment: {powerMutation.data.treatment_size.toLocaleString()} | Control:{' '}
              {powerMutation.data.control_size.toLocaleString()}
              {selectedId != null && (
                <>
                  <br />
                  <button
                    type="button"
                    onClick={() =>
                      setPowerPlans((plans) => ({
                        ...plans,
                        [selectedId]: powerMutation.data!,
                      }))
                    }
                    style={{
                      marginTop: tkn.space.xs,
                      padding: `${tkn.space.xs}px ${tkn.space.md}px`,
                      fontSize: tkn.font.sizeXs,
                      fontWeight: tkn.font.weightMedium,
                      color: tkn.color.accent,
                      backgroundColor: tkn.color.surface,
                      border: `1px solid ${tkn.color.accent}`,
                      borderRadius: tkn.radius.sm,
                      cursor: 'pointer',
                    }}
                    title="Store this power calculation as the target plan for the selected experiment so health can show progress to target."
                  >
                    Apply to selected experiment
                  </button>
                </>
              )}
            </div>
          )}
        </div>
      )}

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'minmax(260px, 1.4fr) minmax(0, 2fr)',
          gap: tkn.space.lg,
          alignItems: 'flex-start',
        }}
      >
        {/* Left: list + create form */}
        <div>
          <div
            style={{
              background: tkn.color.surface,
              border: `1px solid ${tkn.color.borderLight}`,
              borderRadius: tkn.radius.lg,
              padding: tkn.space.lg,
              boxShadow: tkn.shadowSm,
              marginBottom: tkn.space.lg,
            }}
          >
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: tkn.space.sm,
                marginBottom: tkn.space.sm,
              }}
            >
              <h2
                style={{
                  margin: 0,
                  fontSize: tkn.font.sizeMd,
                  fontWeight: tkn.font.weightSemibold,
                  color: tkn.color.text,
                }}
              >
                Experiments
              </h2>
              <input
                type="search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search name, channel, metric"
                style={{
                  flex: 1,
                  minWidth: 0,
                  padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                  borderRadius: tkn.radius.sm,
                  border: `1px solid ${tkn.color.borderLight}`,
                  fontSize: tkn.font.sizeXs,
                }}
              />
            </div>
            <div style={{ display: 'flex', gap: tkn.space.xs, marginBottom: tkn.space.sm, flexWrap: 'wrap' }}>
              {(['all', 'draft', 'running', 'stopped'] as const).map((s) => (
                <button
                  key={s}
                  type="button"
                  onClick={() => setStatusFilter(s)}
                  style={{
                    padding: `${tkn.space.xs}px ${tkn.space.sm}px`,
                    fontSize: tkn.font.sizeXs,
                    borderRadius: tkn.radius.sm,
                    border:
                      statusFilter === s ? `1px solid ${tkn.color.accent}` : `1px solid ${tkn.color.borderLight}`,
                    backgroundColor: statusFilter === s ? tkn.color.accentMuted : 'transparent',
                    color: statusFilter === s ? tkn.color.accent : tkn.color.textSecondary,
                    cursor: 'pointer',
                  }}
                >
                  {s === 'all' ? 'All' : s === 'stopped' ? 'Stopped' : s[0].toUpperCase() + s.slice(1)}
                </button>
              ))}
            </div>
            {experimentsQuery.isLoading ? (
              <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Loading experiments…</p>
            ) : (
              <ul style={{ listStyle: 'none', padding: 0, margin: 0, maxHeight: 260, overflowY: 'auto' }}>
                {filteredExperiments.map((e) => {
                  const isSelected = selectedId === e.id
                  const ready =
                    isSelected && healthQuery.data ? readyBadgeLabel(healthQuery.data.ready_state.label) : null
                  return (
                    <li key={e.id}>
                      <button
                        type="button"
                        onClick={() => setSelectedId(e.id)}
                        style={{
                          width: '100%',
                          textAlign: 'left',
                          padding: `${tkn.space.sm}px ${tkn.space.sm}px`,
                          marginBottom: tkn.space.xs,
                          borderRadius: tkn.radius.sm,
                          border: isSelected ? `1px solid ${tkn.color.accent}` : `1px solid transparent`,
                          backgroundColor: isSelected ? tkn.color.accentMuted : 'transparent',
                          cursor: 'pointer',
                          fontSize: tkn.font.sizeSm,
                          color: tkn.color.text,
                        }}
                      >
                        <div
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                            gap: tkn.space.xs,
                          }}
                        >
                          <div style={{ fontWeight: tkn.font.weightMedium }}>{e.name}</div>
                          <span
                            title="Readiness based on sample size and data completeness"
                            style={{
                              fontSize: tkn.font.sizeXs,
                              padding: `0 ${tkn.space.xs}px`,
                              borderRadius: 999,
                              border: `1px solid ${tkn.color.borderLight}`,
                              color: tkn.color.textMuted,
                              whiteSpace: 'nowrap',
                            }}
                          >
                            {ready ?? (e.status === 'running' ? 'Early' : e.status === 'completed' ? 'Ready' : 'Not ready')}
                          </span>
                        </div>
                        <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                          {e.channel} • {statusLabel(e.status)} •{' '}
                          {new Date(e.start_at).toLocaleDateString()} –{' '}
                          {new Date(e.end_at).toLocaleDateString()}
                        </div>
                      </button>
                    </li>
                  )
                })}
                {filteredExperiments.length === 0 && (
                  <li
                    style={{
                      fontSize: tkn.font.sizeSm,
                      color: tkn.color.textSecondary,
                    }}
                  >
                    No experiments match this filter. Adjust search or create a new experiment below.
                  </li>
                )}
              </ul>
            )}
          </div>

          <div
            style={{
              background: tkn.color.surface,
              border: `1px solid ${tkn.color.borderLight}`,
              borderRadius: tkn.radius.lg,
              padding: tkn.space.lg,
              boxShadow: tkn.shadowSm,
            }}
          >
            <h2
              style={{
                margin: '0 0 8px',
                fontSize: tkn.font.sizeMd,
                fontWeight: tkn.font.weightSemibold,
                color: tkn.color.text,
              }}
            >
              New experiment
            </h2>
            <p style={{ margin: '0 0 12px', fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
              Start small: pick one channel (e.g. <code>email</code>) and hold out a share of audience as control.
            </p>
            <div style={{ display: 'grid', gap: tkn.space.sm }}>
              <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                Name
                <input
                  type="text"
                  value={form.name}
                  onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                  style={{
                    width: '100%',
                    marginTop: 4,
                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                    border: `1px solid ${tkn.color.border}`,
                    borderRadius: tkn.radius.sm,
                    fontSize: tkn.font.sizeSm,
                  }}
                />
              </label>
              <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                Channel
                <select
                  value={form.channel}
                  onChange={(e) => setForm((f) => ({ ...f, channel: e.target.value }))}
                  style={{
                    width: '100%',
                    marginTop: 4,
                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                    border: `1px solid ${tkn.color.border}`,
                    borderRadius: tkn.radius.sm,
                    fontSize: tkn.font.sizeSm,
                    backgroundColor: 'white',
                  }}
                >
                  <option value="">Select owned channel</option>
                  {OWNED_CHANNELS.map((ch) => (
                    <option key={ch} value={ch}>
                      {ch}
                    </option>
                  ))}
                </select>
              </label>
              <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                Primary metric (conversion key)
                <input
                  type="text"
                  value={form.conversion_key}
                  onChange={(e) => setForm((f) => ({ ...f, conversion_key: e.target.value }))}
                  placeholder="e.g. purchase, lead"
                  style={{
                    width: '100%',
                    marginTop: 4,
                    padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                    border: `1px solid ${tkn.color.border}`,
                    borderRadius: tkn.radius.sm,
                    fontSize: tkn.font.sizeSm,
                  }}
                />
              </label>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: tkn.space.sm }}>
                <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                  Start
                  <input
                    type="date"
                    value={form.start_at}
                    onChange={(e) => setForm((f) => ({ ...f, start_at: e.target.value }))}
                    style={{
                      width: '100%',
                      marginTop: 4,
                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                      border: `1px solid ${tkn.color.border}`,
                      borderRadius: tkn.radius.sm,
                      fontSize: tkn.font.sizeSm,
                    }}
                  />
                </label>
                <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                  End
                  <input
                    type="date"
                    value={form.end_at}
                    onChange={(e) => setForm((f) => ({ ...f, end_at: e.target.value }))}
                    style={{
                      width: '100%',
                      marginTop: 4,
                      padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                      border: `1px solid ${tkn.color.border}`,
                      borderRadius: tkn.radius.sm,
                      fontSize: tkn.font.sizeSm,
                    }}
                  />
                </label>
              </div>
              <div
                style={{
                  marginTop: tkn.space.sm,
                  paddingTop: tkn.space.sm,
                  borderTop: `1px dashed ${tkn.color.borderLight}`,
                }}
              >
                <button
                  type="button"
                  onClick={() => setShowAdvancedSetup((v) => !v)}
                  style={{
                    border: 'none',
                    padding: 0,
                    background: 'none',
                    color: tkn.color.accent,
                    fontSize: tkn.font.sizeXs,
                    cursor: 'pointer',
                  }}
                >
                  {showAdvancedSetup ? 'Hide advanced setup' : 'Show advanced setup'}
                </button>
                {showAdvancedSetup && (
                  <div style={{ marginTop: tkn.space.sm, display: 'grid', gap: tkn.space.sm }}>
                    <div
                      style={{
                        fontSize: tkn.font.sizeXs,
                        color: tkn.color.textSecondary,
                        background: tkn.color.surfaceMuted ?? tkn.color.surface,
                        borderRadius: tkn.radius.sm,
                        padding: tkn.space.sm,
                      }}
                    >
                      <strong>Explain setup.</strong> A holdout experiment keeps a control group that does not
                      receive the channel, and compares their conversion rate to the treated group. Make sure you log
                      <strong> assignments</strong> (who is treatment vs control), <strong>exposures</strong> (who was
                      actually contacted), and <strong>outcomes</strong> (who converted on the primary metric).
                    </div>
                    <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                      Experiment type
                      <select
                        value={form.experiment_type}
                        onChange={(e) =>
                          setForm((f) => ({ ...f, experiment_type: e.target.value as 'holdout' }))
                        }
                        style={{
                          width: '100%',
                          marginTop: 4,
                          padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                          border: `1px solid ${tkn.color.border}`,
                          borderRadius: tkn.radius.sm,
                          fontSize: tkn.font.sizeSm,
                          backgroundColor: 'white',
                        }}
                      >
                        <option value="holdout">Holdout (supported)</option>
                        {/* Geo holdout could be added later when supported */}
                      </select>
                    </label>
                    <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                      Split ratio (treatment % / control %)
                      <input
                        type="number"
                        min={50}
                        max={99}
                        value={(form.treatment_rate * 100).toFixed(0)}
                        onChange={(e) => {
                          const v = Number(e.target.value) || 0
                          const clamped = Math.min(99, Math.max(50, v))
                          setForm((f) => ({ ...f, treatment_rate: clamped / 100 }))
                        }}
                        style={{
                          width: '100%',
                          marginTop: 4,
                          padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                          border: `1px solid ${tkn.color.border}`,
                          borderRadius: tkn.radius.sm,
                          fontSize: tkn.font.sizeSm,
                        }}
                      />
                      <span style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                        Recommended: 80/20 or 90/10 depending on risk tolerance.
                      </span>
                    </label>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: tkn.space.sm }}>
                      <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                        Unit of randomization
                        <input
                          type="text"
                          value="profile_id"
                          readOnly
                          style={{
                            width: '100%',
                            marginTop: 4,
                            padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                            border: `1px dashed ${tkn.color.borderLight}`,
                            borderRadius: tkn.radius.sm,
                            fontSize: tkn.font.sizeSm,
                            backgroundColor: tkn.color.surface,
                          }}
                        />
                      </label>
                      <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                        Assignment method
                        <input
                          type="text"
                          value="Random (hash-based, deterministic)"
                          readOnly
                          style={{
                            width: '100%',
                            marginTop: 4,
                            padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                            border: `1px dashed ${tkn.color.borderLight}`,
                            borderRadius: tkn.radius.sm,
                            fontSize: tkn.font.sizeSm,
                            backgroundColor: tkn.color.surface,
                          }}
                        />
                      </label>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: tkn.space.sm }}>
                      <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                        Minimum runtime (days)
                        <input
                          type="number"
                          min={1}
                          value={form.min_runtime_days}
                          onChange={(e) => setForm((f) => ({ ...f, min_runtime_days: e.target.value }))}
                          placeholder="e.g. 14"
                          style={{
                            width: '100%',
                            marginTop: 4,
                            padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                            border: `1px solid ${tkn.color.border}`,
                            borderRadius: tkn.radius.sm,
                            fontSize: tkn.font.sizeSm,
                          }}
                        />
                      </label>
                      <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                        Exclusion window (days, not enforced yet)
                        <input
                          type="number"
                          min={0}
                          value={form.exclusion_window_days}
                          onChange={(e) =>
                            setForm((f) => ({ ...f, exclusion_window_days: e.target.value }))
                          }
                          placeholder="e.g. 30"
                          style={{
                            width: '100%',
                            marginTop: 4,
                            padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                            border: `1px solid ${tkn.color.border}`,
                            borderRadius: tkn.radius.sm,
                            fontSize: tkn.font.sizeSm,
                          }}
                        />
                      </label>
                    </div>
                    <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                      Stop rule (not auto-enforced)
                      <input
                        type="text"
                        value={form.stop_rule}
                        onChange={(e) => setForm((f) => ({ ...f, stop_rule: e.target.value }))}
                        placeholder='e.g. "Run at least 14 days and until 200 conversions per arm"'
                        style={{
                          width: '100%',
                          marginTop: 4,
                          padding: `${tkn.space.sm}px ${tkn.space.md}px`,
                          border: `1px solid ${tkn.color.border}`,
                          borderRadius: tkn.radius.sm,
                          fontSize: tkn.font.sizeSm,
                        }}
                      />
                      <span style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                        Document your operational stop rule. This dashboard does not yet enforce it automatically.
                      </span>
                    </label>
                  </div>
                )}
              </div>
              <label style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                Notes (optional)
                <textarea
                  value={form.notes}
                  onChange={(e) => setForm((f) => ({ ...f, notes: e.target.value }))}
                  rows={3}
                  style={{
                    width: '100%',
                    marginTop: 4,
                    padding: tkn.space.sm,
                    border: `1px solid ${tkn.color.border}`,
                    borderRadius: tkn.radius.sm,
                    fontSize: tkn.font.sizeSm,
                    resize: 'vertical',
                  }}
                />
              </label>
              {createMutation.isError && (
                <p style={{ fontSize: tkn.font.sizeXs, color: tkn.color.danger }}>
                  {(createMutation.error as Error).message}
                </p>
              )}
              <button
                type="button"
                onClick={() => createMutation.mutate()}
                disabled={createMutation.isPending}
                style={{
                  marginTop: tkn.space.sm,
                  padding: `${tkn.space.sm}px ${tkn.space.lg}px`,
                  fontSize: tkn.font.sizeSm,
                  fontWeight: tkn.font.weightMedium,
                  color: tkn.color.surface,
                  backgroundColor: tkn.color.accent,
                  border: 'none',
                  borderRadius: tkn.radius.sm,
                  cursor: createMutation.isPending ? 'wait' : 'pointer',
                }}
              >
                {createMutation.isPending ? 'Creating…' : 'Create experiment'}
              </button>
            </div>
          </div>
        </div>

        {/* Right: selected experiment details and results */}
        <div>
          {selectedId == null ? (
            <div
              style={{
                background: tkn.color.surface,
                border: `1px solid ${tkn.color.borderLight}`,
                borderRadius: tkn.radius.lg,
                padding: tkn.space.xl,
                boxShadow: tkn.shadowSm,
              }}
            >
              <h2
                style={{
                  margin: '0 0 8px',
                  fontSize: tkn.font.sizeMd,
                  fontWeight: tkn.font.weightSemibold,
                  color: tkn.color.text,
                }}
              >
                Experiment details
              </h2>
              <p style={{ margin: 0, fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                Select an experiment on the left to see its configuration and measured uplift.
              </p>
            </div>
          ) : (
            <>
              <div
                style={{
                  background: tkn.color.surface,
                  border: `1px solid ${tkn.color.borderLight}`,
                  borderRadius: tkn.radius.lg,
                  padding: tkn.space.xl,
                  boxShadow: tkn.shadowSm,
                  marginBottom: tkn.space.lg,
                }}
              >
                <h2
                  style={{
                    margin: '0 0 8px',
                    fontSize: tkn.font.sizeMd,
                    fontWeight: tkn.font.weightSemibold,
                    color: tkn.color.text,
                  }}
                >
                  {selectedSummary?.name}
                </h2>
                <p style={{ margin: '0 0 4px', fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                  Channel <strong>{selectedSummary?.channel}</strong> • Status{' '}
                  <strong title="Draft = configured, Running = in flight, Stopped = completed">
                    {selectedSummary && statusLabel(selectedSummary.status)}
                  </strong>
                  {healthQuery.data && (
                    <>
                      {' • '}
                      <span
                        title="Based on sample size, conversions, balance, data completeness, and overlap risk"
                        style={{
                          fontSize: tkn.font.sizeXs,
                          padding: `0 ${tkn.space.xs}px`,
                          borderRadius: 999,
                          border: `1px solid ${tkn.color.borderLight}`,
                          marginLeft: tkn.space.xs,
                        }}
                      >
                        Ready to read: {readyBadgeLabel(healthQuery.data.ready_state.label)}
                      </span>
                    </>
                  )}
                </p>
                <p style={{ margin: '0 0 4px', fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                  Period {selectedSummary && new Date(selectedSummary.start_at).toLocaleDateString()}
                  {' – '}
                  {selectedSummary && new Date(selectedSummary.end_at).toLocaleDateString()}
                  {selectedSummary?.conversion_key && (
                    <>
                      {' • '}Conversion key <strong>{selectedSummary.conversion_key}</strong>
                    </>
                  )}
                </p>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: tkn.space.xs, marginTop: tkn.space.sm }}>
                  {selectedSummary?.status === 'draft' && (
                    <button
                      type="button"
                      onClick={() =>
                        selectedSummary &&
                        statusMutation.mutate({ id: selectedSummary.id, nextStatus: 'running' })
                      }
                      style={{
                        padding: `${tkn.space.xs}px ${tkn.space.md}px`,
                        fontSize: tkn.font.sizeXs,
                        fontWeight: tkn.font.weightMedium,
                        color: tkn.color.surface,
                        backgroundColor: tkn.color.accent,
                        border: 'none',
                        borderRadius: tkn.radius.sm,
                        cursor: 'pointer',
                      }}
                      title="Start experiment. Make sure assignments, exposures, and outcomes are being logged."
                    >
                      Start experiment
                    </button>
                  )}
                  {selectedSummary?.status === 'running' && (
                    <button
                      type="button"
                      onClick={() =>
                        selectedSummary &&
                        statusMutation.mutate({ id: selectedSummary.id, nextStatus: 'completed' })
                      }
                      style={{
                        padding: `${tkn.space.xs}px ${tkn.space.md}px`,
                        fontSize: tkn.font.sizeXs,
                        fontWeight: tkn.font.weightMedium,
                        color: tkn.color.surface,
                        backgroundColor: tkn.color.danger,
                        border: 'none',
                        borderRadius: tkn.radius.sm,
                        cursor: 'pointer',
                      }}
                      title="Stop experiment. This will freeze status as Stopped; you can still read results."
                    >
                      Stop experiment
                    </button>
                  )}
                  {selectedSummary?.status === 'completed' && (
                    <button
                      type="button"
                      onClick={() => {
                        if (!selectedSummary) return
                        setForm((f) => ({
                          ...f,
                          name: `${selectedSummary.name} (rerun)`,
                          channel: selectedSummary.channel,
                          conversion_key: selectedSummary.conversion_key ?? '',
                          start_at: '',
                          end_at: '',
                          notes: detailQuery.data?.notes ?? '',
                        }))
                      }}
                      style={{
                        padding: `${tkn.space.xs}px ${tkn.space.md}px`,
                        fontSize: tkn.font.sizeXs,
                        fontWeight: tkn.font.weightMedium,
                        color: tkn.color.accent,
                        backgroundColor: 'transparent',
                        border: `1px solid ${tkn.color.accent}`,
                        borderRadius: tkn.radius.sm,
                        cursor: 'pointer',
                      }}
                      title="Create a new draft using this configuration."
                    >
                      Duplicate to new draft
                    </button>
                  )}
                  {statusMutation.isPending && (
                    <span style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                      Updating status…
                    </span>
                  )}
                  {statusMutation.isError && (
                    <span style={{ fontSize: tkn.font.sizeXs, color: tkn.color.danger }}>
                      {(statusMutation.error as Error).message}
                    </span>
                  )}
                </div>
                {detailQuery.data?.notes && (
                  <p style={{ margin: '8px 0 0', fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                    {detailQuery.data.notes}
                  </p>
                )}
              </div>

              {/* Health + results */}
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'minmax(0, 1.3fr) minmax(0, 1.7fr)',
                  gap: tkn.space.md,
                  marginBottom: tkn.space.lg,
                }}
              >
                {/* Health panel */}
                <div
                  style={{
                    background: tkn.color.surface,
                    border: `1px solid ${tkn.color.borderLight}`,
                    borderRadius: tkn.radius.lg,
                    padding: tkn.space.lg,
                    boxShadow: tkn.shadowSm,
                  }}
                >
                  <h3
                    style={{
                      margin: 0,
                      fontSize: tkn.font.sizeSm,
                      fontWeight: tkn.font.weightSemibold,
                      color: tkn.color.text,
                    }}
                  >
                    Health
                  </h3>
                  <p style={{ margin: '4px 0 8px', fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                    Quick check of sample size, balance, data completeness, and overlap risk for this experiment.
                  </p>
                  {healthQuery.isLoading || !healthQuery.data ? (
                    <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Loading health…</p>
                  ) : (
                    <>
                      <div
                        style={{
                          display: 'grid',
                          gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
                          gap: tkn.space.xs,
                          marginBottom: tkn.space.sm,
                          fontSize: tkn.font.sizeXs,
                        }}
                      >
                        <div title="Number of profiles assigned to each group">
                          <strong>Sample size</strong>
                          <br />
                          T: {healthQuery.data.sample.treatment.toLocaleString()} • C:{' '}
                          {healthQuery.data.sample.control.toLocaleString()}
                        </div>
                        <div title="Profiles with at least one exposure event logged">
                          <strong>Exposed</strong>
                          <br />
                          T: {healthQuery.data.exposures.treatment.toLocaleString()} • C:{' '}
                          {healthQuery.data.exposures.control.toLocaleString()}
                        </div>
                        <div title="Profiles with at least one outcome logged on the primary metric">
                          <strong>Outcomes</strong>
                          <br />
                          T: {healthQuery.data.outcomes.treatment.toLocaleString()} • C:{' '}
                          {healthQuery.data.outcomes.control.toLocaleString()}
                        </div>
                        <div title="Observed treatment share vs expected split. Large deviations can bias results.">
                          <strong>Balance</strong>
                          <br />
                          {healthQuery.data.balance.status === 'ok' ? 'OK' : 'Warning'} • T share:{' '}
                          {(healthQuery.data.balance.observed_share * 100).toFixed(1)}%
                        </div>
                        <div title="Are assignments and outcomes being recorded in the underlying tables?">
                          <strong>Data completeness</strong>
                          <br />
                          Assign: {healthQuery.data.data_completeness.assignments.status === 'ok' ? 'OK' : 'Fail'} •
                          Outcomes:{' '}
                          {healthQuery.data.data_completeness.outcomes.status === 'ok' ? 'OK' : 'Fail'} • Exposures:{' '}
                          {healthQuery.data.data_completeness.exposures.status === 'ok' ? 'OK' : 'Warn'}
                        </div>
                        <div title="If the same profiles are in multiple running experiments on this channel and period, results may be contaminated.">
                          <strong>Overlap risk</strong>
                          <br />
                          {healthQuery.data.overlap_risk.status === 'ok'
                            ? 'Low'
                            : `Warning (${healthQuery.data.overlap_risk.overlapping_profiles} profiles)`}
                        </div>
                      </div>
                      {powerPlans[selectedId!] && (
                        <div
                          style={{
                            marginTop: tkn.space.xs,
                            padding: tkn.space.xs,
                            borderRadius: tkn.radius.sm,
                            background: tkn.color.accentMuted,
                            fontSize: tkn.font.sizeXs,
                          }}
                          title="Target per group from the power calculator and current progress based on assignments."
                        >
                          Target sample size:{' '}
                          <strong>
                            {powerPlans[selectedId!].treatment_size.toLocaleString()} T /{' '}
                            {powerPlans[selectedId!].control_size.toLocaleString()} C
                          </strong>
                          <br />
                          {(() => {
                            const plan = powerPlans[selectedId!]
                            const h = healthQuery.data!
                            const tProg =
                              plan.treatment_size > 0
                                ? Math.min(1, h.sample.treatment / plan.treatment_size)
                                : 0
                            const cProg =
                              plan.control_size > 0 ? Math.min(1, h.sample.control / plan.control_size) : 0
                            const pct = Math.round(Math.min(tProg, cProg) * 100)
                            return <>Progress: {pct}%</>
                          })()}
                        </div>
                      )}
                      {healthQuery.data.ready_state.reasons.length > 0 && (
                        <div style={{ marginTop: tkn.space.xs }}>
                          <strong style={{ fontSize: tkn.font.sizeXs }}>Top blockers</strong>
                          <ul
                            style={{
                              margin: '4px 0 0',
                              paddingLeft: tkn.space.md,
                              fontSize: tkn.font.sizeXs,
                              color: tkn.color.textSecondary,
                            }}
                          >
                            {healthQuery.data.ready_state.reasons.slice(0, 3).map((r) => (
                              <li key={r}>{r}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </>
                  )}
                </div>

                {/* Uplift results */}
                <div
                  style={{
                    background: tkn.color.surface,
                    border: `1px solid ${tkn.color.borderLight}`,
                    borderRadius: tkn.radius.lg,
                    padding: tkn.space.lg,
                    boxShadow: tkn.shadowSm,
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      marginBottom: tkn.space.sm,
                    }}
                  >
                    <h3
                      style={{
                        margin: 0,
                        fontSize: tkn.font.sizeSm,
                        fontWeight: tkn.font.weightSemibold,
                        color: tkn.color.text,
                      }}
                    >
                      Uplift results
                    </h3>
                    <button
                      type="button"
                      onClick={() => {
                        if (!selectedSummary) return
                        if (!resultsQuery.data) return
                        const h = healthQuery.data
                        const rows: string[] = []
                        rows.push(
                          [
                            'experiment_id',
                            'name',
                            'channel',
                            'status',
                            'start_at',
                            'end_at',
                            'conversion_key',
                            'assigned_treatment',
                            'assigned_control',
                            'exposed_treatment',
                            'exposed_control',
                            'conversions_treatment',
                            'conversions_control',
                            'rate_treatment',
                            'rate_control',
                            'uplift_abs',
                            'uplift_rel',
                            'ci_low',
                            'ci_high',
                            'p_value',
                            'ready_state',
                            'balance_status',
                            'overlap_status',
                          ]
                            .map((h) => `"${h}"`)
                            .join(',')
                        )
                        const r = resultsQuery.data
                        const t = r.treatment
                        const c = r.control
                        const line = [
                          selectedSummary.id,
                          selectedSummary.name,
                          selectedSummary.channel,
                          statusLabel(selectedSummary.status),
                          selectedSummary.start_at,
                          selectedSummary.end_at,
                          selectedSummary.conversion_key ?? '',
                          h ? h.sample.treatment : '',
                          h ? h.sample.control : '',
                          h ? h.exposures.treatment : '',
                          h ? h.exposures.control : '',
                          t ? t.conversions : '',
                          c ? c.conversions : '',
                          t ? t.conversion_rate : '',
                          c ? c.conversion_rate : '',
                          r.uplift_abs ?? '',
                          r.uplift_rel ?? '',
                          r.ci_low ?? '',
                          r.ci_high ?? '',
                          r.p_value ?? '',
                          h ? h.ready_state.label : '',
                          h ? h.balance.status : '',
                          h ? h.overlap_risk.status : '',
                        ]
                          .map((v) => `"${String(v ?? '').replace(/"/g, '""')}"`)
                          .join(',')
                        rows.push(line)
                        const csv = rows.join('\n')
                        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
                        const url = URL.createObjectURL(blob)
                        const a = document.createElement('a')
                        a.href = url
                        a.download = `experiment-${selectedSummary.id}-results.csv`
                        a.click()
                        URL.revokeObjectURL(url)
                      }}
                      style={{
                        padding: `${tkn.space.xs}px ${tkn.space.md}px`,
                        fontSize: tkn.font.sizeXs,
                        fontWeight: tkn.font.weightMedium,
                        color: tkn.color.accent,
                        backgroundColor: 'transparent',
                        border: `1px solid ${tkn.color.accent}`,
                        borderRadius: tkn.radius.sm,
                        cursor: 'pointer',
                        marginRight: tkn.space.xs,
                      }}
                      title="Download a CSV summary of experiment configuration, counts, rates, uplift, confidence interval, p-value, and current health warnings."
                    >
                      Export results (CSV)
                    </button>
                    <button
                      type="button"
                      onClick={() => setShowTimeSeries(!showTimeSeries)}
                      style={{
                        padding: `${tkn.space.xs}px ${tkn.space.md}px`,
                        fontSize: tkn.font.sizeXs,
                        fontWeight: tkn.font.weightMedium,
                        color: tkn.color.accent,
                        backgroundColor: 'transparent',
                        border: `1px solid ${tkn.color.accent}`,
                        borderRadius: tkn.radius.sm,
                        cursor: 'pointer',
                      }}
                      title="Show cumulative daily conversion rate and sample size for treatment and control."
                    >
                      {showTimeSeries ? 'Hide' : 'Show'} time series
                    </button>
                  </div>
                  {resultsQuery.isLoading ? (
                    <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Computing results…</p>
                  ) : (
                    <>
                      <div
                        style={{
                          display: 'grid',
                          gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                          gap: tkn.space.sm,
                          marginBottom: tkn.space.sm,
                        }}
                      >
                        <KpiCard
                          label="Treatment conversion rate"
                          value={
                            resultsQuery.data?.treatment
                              ? `${(resultsQuery.data.treatment.conversion_rate * 100).toFixed(2)}%`
                              : '—'
                          }
                        />
                        <KpiCard
                          label="Control conversion rate"
                          value={
                            resultsQuery.data?.control
                              ? `${(resultsQuery.data.control.conversion_rate * 100).toFixed(2)}%`
                              : '—'
                          }
                        />
                        <KpiCard
                          label="Absolute lift (pp)"
                          value={
                            resultsQuery.data?.uplift_abs != null
                              ? `${(resultsQuery.data.uplift_abs * 100).toFixed(2)} pp`
                              : '—'
                          }
                        />
                        <KpiCard
                          label="Relative lift (%)"
                          value={
                            resultsQuery.data?.uplift_rel != null
                              ? `${(resultsQuery.data.uplift_rel * 100).toFixed(1)}%`
                              : '—'
                          }
                        />
                        <KpiCard
                          label="95% CI (pp)"
                          value={
                            resultsQuery.data?.ci_low != null && resultsQuery.data.ci_high != null
                              ? `${(resultsQuery.data.ci_low * 100).toFixed(2)} – ${(resultsQuery.data.ci_high * 100).toFixed(2)}`
                              : '—'
                          }
                        />
                        <KpiCard
                          label="p-value"
                          value={
                            resultsQuery.data?.p_value != null
                              ? resultsQuery.data.p_value.toFixed(4)
                              : resultsQuery.data?.insufficient_data
                              ? 'Insufficient data'
                              : '—'
                          }
                        />
                      </div>
                      <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                        {(() => {
                          if (!resultsQuery.data || resultsQuery.data.insufficient_data) {
                            return (
                              <>
                                <strong>Interpretation:</strong> Inconclusive – not enough data to estimate uplift
                                reliably yet.
                              </>
                            )
                          }
                          const p = resultsQuery.data.p_value
                          const lift = resultsQuery.data.uplift_abs ?? 0
                          let label = 'inconclusive'
                          if (p != null && p < 0.05) {
                            label = lift > 0 ? 'positive' : 'negative'
                          }
                          return (
                            <>
                              <strong>Interpretation:</strong>{' '}
                              {p != null && p < 0.05
                                ? `Evidence suggests a ${label} uplift with p=${p.toFixed(4)}.`
                                : p != null
                                ? `Evidence is inconclusive (p=${p.toFixed(4)}).`
                                : 'Evidence is directional only; significance could not be computed.'}
                            </>
                          )
                        })()}
                      </div>
                      <div
                        style={{
                          marginTop: tkn.space.xs,
                          fontSize: tkn.font.sizeXs,
                          color: tkn.color.textMuted,
                        }}
                      >
                        <strong>Guardrails:</strong>{' '}
                        {(() => {
                          const guards: string[] = []
                          const h = healthQuery.data
                          if (h) {
                            if (h.ready_state.label !== 'ready') guards.push('Low sample size vs target')
                            if (h.balance.status === 'warn') guards.push('Imbalance between treatment and control')
                            if (h.data_completeness.outcomes.status !== 'ok')
                              guards.push('Missing outcomes logging')
                            if (h.overlap_risk.status === 'warn') guards.push('Overlap risk with other experiments')
                          }
                          if (!guards.length) {
                            guards.push(
                              'Assumes random assignment, stable tracking, and independent outcomes; validate large decisions with additional evidence.'
                            )
                          }
                          return guards.join(' • ')
                        })()}
                      </div>
                    </>
                  )}
                </div>
              </div>

              {showTimeSeries && (
                <div
                  style={{
                    background: tkn.color.surface,
                    border: `1px solid ${tkn.color.borderLight}`,
                    borderRadius: tkn.radius.lg,
                    padding: tkn.space.xl,
                    boxShadow: tkn.shadowSm,
                  }}
                >
                  <h3
                    style={{
                      margin: '0 0 12px',
                      fontSize: tkn.font.sizeMd,
                      fontWeight: tkn.font.weightSemibold,
                      color: tkn.color.text,
                    }}
                  >
                    Cumulative metrics over time
                  </h3>
                  {timeSeriesQuery.isLoading ? (
                    <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Loading time series…</p>
                  ) : timeSeriesQuery.data && timeSeriesQuery.data.data.length > 0 ? (
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={timeSeriesQuery.data.data}>
                        <CartesianGrid strokeDasharray="3 3" stroke={tkn.color.borderLight} />
                        <XAxis dataKey="date" tick={{ fontSize: 11 }} stroke={tkn.color.textSecondary} />
                        <YAxis
                          yAxisId="left"
                          tick={{ fontSize: 11 }}
                          stroke={tkn.color.textSecondary}
                          label={{ value: 'Conversion rate', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }}
                        />
                        <YAxis
                          yAxisId="right"
                          orientation="right"
                          tick={{ fontSize: 11 }}
                          stroke={tkn.color.textSecondary}
                          label={{ value: 'Sample size', angle: 90, position: 'insideRight', style: { fontSize: 11 } }}
                        />
                        <Tooltip
                          contentStyle={{
                            background: tkn.color.surface,
                            border: `1px solid ${tkn.color.border}`,
                            borderRadius: tkn.radius.sm,
                            fontSize: 12,
                          }}
                        />
                        <Legend wrapperStyle={{ fontSize: 12 }} />
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="treatment_rate"
                          stroke={tkn.color.success}
                          name="Treatment rate"
                          strokeWidth={2}
                          dot={false}
                        />
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="control_rate"
                          stroke={tkn.color.textSecondary}
                          name="Control rate"
                          strokeWidth={2}
                          dot={false}
                        />
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey="treatment_n"
                          stroke={tkn.color.accent}
                          name="Treatment n"
                          strokeWidth={1}
                          strokeDasharray="5 5"
                          dot={false}
                        />
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey="control_n"
                          stroke={tkn.color.warning}
                          name="Control n"
                          strokeWidth={1}
                          strokeDasharray="5 5"
                          dot={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                      No time series data available yet.
                    </p>
                  )}
                  {timeSeriesQuery.data && timeSeriesQuery.data.data.length > 2 && (
                    <p
                      style={{
                        marginTop: tkn.space.sm,
                        fontSize: tkn.font.sizeXs,
                        color: tkn.color.textMuted,
                      }}
                    >
                      {(() => {
                        const pts = timeSeriesQuery.data!.data
                        let maxJump = 0
                        let day: string | null = null
                        for (let i = 1; i < pts.length; i++) {
                          const prev = pts[i - 1]
                          const curr = pts[i]
                          const diff = Math.abs((curr.uplift_abs ?? 0) - (prev.uplift_abs ?? 0))
                          if (diff > maxJump) {
                            maxJump = diff
                            day = curr.date
                          }
                        }
                        if (!day || maxJump < 0.02) {
                          return 'Daily points show cumulative conversion rate and sample size; large day-to-day jumps may indicate tracking or campaign changes.'
                        }
                        return `Change point note: ${day} shows the largest day-on-day change in uplift (${(maxJump * 100).toFixed(
                          2
                        )}pp). Check for campaign or tracking changes around this date.`
                      })()}
                    </p>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function KpiCard(props: { label: string; value: string }) {
  const k = t
  return (
    <div
      style={{
        background: k.color.surface,
        border: `1px solid ${k.color.borderLight}`,
        borderRadius: k.radius.md,
        padding: `${k.space.lg}px ${k.space.xl}px`,
        boxShadow: k.shadowSm,
      }}
    >
      <div
        style={{
          fontSize: k.font.sizeXs,
          fontWeight: k.font.weightMedium,
          color: k.color.textMuted,
          textTransform: 'uppercase',
          letterSpacing: '0.04em',
        }}
      >
        {props.label}
      </div>
      <div
        style={{
          fontSize: k.font.sizeXl,
          fontWeight: k.font.weightBold,
          color: k.color.text,
          marginTop: k.space.xs,
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {props.value}
      </div>
    </div>
  )
}
