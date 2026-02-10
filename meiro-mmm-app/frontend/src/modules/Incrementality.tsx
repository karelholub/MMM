import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'

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

export default function IncrementalityPage() {
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [form, setForm] = useState({
    name: '',
    channel: '',
    conversion_key: '',
    start_at: '',
    end_at: '',
    notes: '',
  })

  const experimentsQuery = useQuery<ExperimentSummary[]>({
    queryKey: ['experiments'],
    queryFn: async () => {
      const res = await fetch('/api/experiments')
      if (!res.ok) throw new Error('Failed to load experiments')
      return res.json()
    },
  })

  const detailQuery = useQuery<ExperimentDetail>({
    queryKey: ['experiment', selectedId],
    queryFn: async () => {
      const res = await fetch(`/api/experiments/${selectedId}`)
      if (!res.ok) throw new Error('Failed to load experiment')
      return res.json()
    },
    enabled: selectedId != null,
  })

  const resultsQuery = useQuery<ExperimentResults>({
    queryKey: ['experiment-results', selectedId],
    queryFn: async () => {
      const res = await fetch(`/api/experiments/${selectedId}/results`)
      if (!res.ok) throw new Error('Failed to load results')
      return res.json()
    },
    enabled: selectedId != null,
  })

  const createMutation = useMutation({
    mutationFn: async () => {
      if (!form.name || !form.channel || !form.start_at || !form.end_at) {
        throw new Error('Name, channel, start and end are required')
      }
      const body = {
        name: form.name,
        channel: form.channel,
        conversion_key: form.conversion_key || null,
        start_at: new Date(form.start_at).toISOString(),
        end_at: new Date(form.end_at).toISOString(),
        notes: form.notes || null,
      }
      const res = await fetch('/api/experiments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) throw new Error('Failed to create experiment')
      return res.json() as Promise<ExperimentSummary>
    },
    onSuccess: (data) => {
      experimentsQuery.refetch()
      setSelectedId(data.id)
    },
  })

  const tkn = t

  const selectedSummary =
    selectedId != null ? experimentsQuery.data?.find((e) => e.id === selectedId) : undefined

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
      </div>

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
            <h2
              style={{
                margin: '0 0 8px',
                fontSize: tkn.font.sizeMd,
                fontWeight: tkn.font.weightSemibold,
                color: tkn.color.text,
              }}
            >
              Experiments
            </h2>
            {experimentsQuery.isLoading ? (
              <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Loading experiments…</p>
            ) : (
              <ul style={{ listStyle: 'none', padding: 0, margin: 0, maxHeight: 260, overflowY: 'auto' }}>
                {(experimentsQuery.data ?? []).map((e) => (
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
                        border:
                          selectedId === e.id
                            ? `1px solid ${tkn.color.accent}`
                            : `1px solid transparent`,
                        backgroundColor:
                          selectedId === e.id ? tkn.color.accentMuted : 'transparent',
                        cursor: 'pointer',
                        fontSize: tkn.font.sizeSm,
                        color: tkn.color.text,
                      }}
                    >
                      <div style={{ fontWeight: tkn.font.weightMedium }}>{e.name}</div>
                      <div style={{ fontSize: tkn.font.sizeXs, color: tkn.color.textSecondary }}>
                        {e.channel} • {e.status} •{' '}
                        {new Date(e.start_at).toLocaleDateString()} –{' '}
                        {new Date(e.end_at).toLocaleDateString()}
                      </div>
                    </button>
                  </li>
                ))}
                {experimentsQuery.data && experimentsQuery.data.length === 0 && (
                  <li
                    style={{
                      fontSize: tkn.font.sizeSm,
                      color: tkn.color.textSecondary,
                    }}
                  >
                    No experiments yet. Create your first holdout test below.
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
                <input
                  type="text"
                  value={form.channel}
                  onChange={(e) => setForm((f) => ({ ...f, channel: e.target.value }))}
                  placeholder="e.g. email, push, sms"
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
                Conversion key (optional)
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
                  <strong>{selectedSummary?.status}</strong>
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
                {detailQuery.data?.notes && (
                  <p style={{ margin: '8px 0 0', fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                    {detailQuery.data.notes}
                  </p>
                )}
              </div>

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
                    margin: '0 0 8px',
                    fontSize: tkn.font.sizeMd,
                    fontWeight: tkn.font.weightSemibold,
                    color: tkn.color.text,
                  }}
                >
                  Uplift results
                </h3>
                {resultsQuery.isLoading ? (
                  <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>Computing results…</p>
                ) : resultsQuery.data?.insufficient_data ? (
                  <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                    Not enough assignments or outcomes yet to estimate uplift. Once both treatment and control have
                    sufficient conversions, this section will show the difference in conversion rate and confidence
                    interval.
                  </p>
                ) : resultsQuery.data ? (
                  <>
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
                        gap: tkn.space.md,
                        marginBottom: tkn.space.lg,
                      }}
                    >
                      <KpiCard
                        label="Treatment conversion rate"
                        value={
                          resultsQuery.data.treatment
                            ? `${(resultsQuery.data.treatment.conversion_rate * 100).toFixed(2)}%`
                            : '—'
                        }
                      />
                      <KpiCard
                        label="Control conversion rate"
                        value={
                          resultsQuery.data.control
                            ? `${(resultsQuery.data.control.conversion_rate * 100).toFixed(2)}%`
                            : '—'
                        }
                      />
                      <KpiCard
                        label="Absolute uplift"
                        value={
                          resultsQuery.data.uplift_abs != null
                            ? `${(resultsQuery.data.uplift_abs * 100).toFixed(2)} pp`
                            : '—'
                        }
                      />
                      <KpiCard
                        label="Relative uplift"
                        value={
                          resultsQuery.data.uplift_rel != null
                            ? `${(resultsQuery.data.uplift_rel * 100).toFixed(1)}%`
                            : '—'
                        }
                      />
                      <KpiCard
                        label="95% CI (abs)"
                        value={
                          resultsQuery.data.ci_low != null && resultsQuery.data.ci_high != null
                            ? `${(resultsQuery.data.ci_low * 100).toFixed(2)} – ${(resultsQuery.data.ci_high * 100).toFixed(2)} pp`
                            : '—'
                        }
                      />
                    </div>
                    <p style={{ margin: 0, fontSize: tkn.font.sizeXs, color: tkn.color.textMuted }}>
                      This is a simple two-sample difference-in-proportions estimate. It assumes random assignment and
                      independent outcomes; use it as directional evidence, and validate large decisions with
                      higher-powered tests where possible.
                    </p>
                  </>
                ) : (
                  <p style={{ fontSize: tkn.font.sizeSm, color: tkn.color.textSecondary }}>
                    No results yet. Once assignments and outcomes are recorded for this experiment, you can refresh to
                    compute uplift.
                  </p>
                )}
              </div>
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

