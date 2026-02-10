import { useQuery, useMutation } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'

interface DQSnapshot {
  id: number
  ts_bucket: string
  source: string
  metric_key: string
  metric_value: number
  meta?: Record<string, any>
}

interface DQAlert {
  id: number
  rule_id: number
  triggered_at: string
  ts_bucket: string
  metric_value: number
  baseline_value?: number | null
  status: string
  message: string
  rule?: {
    name?: string | null
    metric_key?: string | null
    source?: string | null
    severity?: string | null
  } | null
}

export default function DataQuality() {
  const runMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch('/api/data-quality/run', { method: 'POST' })
      if (!res.ok) throw new Error('Failed to run data quality job')
      return res.json()
    },
  })

  const snapshotsQuery = useQuery<DQSnapshot[]>({
    queryKey: ['dq-snapshots'],
    queryFn: async () => {
      const res = await fetch('/api/data-quality/snapshots?limit=200')
      if (!res.ok) throw new Error('Failed to load data quality snapshots')
      return res.json()
    },
  })

  const alertsQuery = useQuery<DQAlert[]>({
    queryKey: ['dq-alerts'],
    queryFn: async () => {
      const res = await fetch('/api/data-quality/alerts?limit=100')
      if (!res.ok) throw new Error('Failed to load data quality alerts')
      return res.json()
    },
  })

  const updateAlertStatus = useMutation({
    mutationFn: async (payload: { id: number; status: string }) => {
      const res = await fetch(`/api/data-quality/alerts/${payload.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: payload.status }),
      })
      if (!res.ok) throw new Error('Failed to update alert status')
      return res.json()
    },
    onSuccess: () => {
      alertsQuery.refetch()
    },
  })

  const latest = snapshotsQuery.data ?? []

  const freshness = latest.filter((s) => s.metric_key === 'freshness_lag_minutes')
  const completenessMissingProfile = latest.find((s) => s.metric_key === 'missing_profile_pct')
  const completenessMissingTs = latest.find((s) => s.metric_key === 'missing_timestamp_pct')
  const duplication = latest.find((s) => s.metric_key === 'duplicate_id_pct')
  const joinRate = latest.find((s) => s.metric_key === 'conversion_attributable_pct')

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.xl,
          gap: t.space.md,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <h1
            style={{
              margin: 0,
              fontSize: t.font.size2xl,
              fontWeight: t.font.weightBold,
              color: t.color.text,
              letterSpacing: '-0.02em',
            }}
          >
            Data quality
          </h1>
          <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Monitor freshness, completeness, and join rates across Meiro CDP and ad platform data. Alerts highlight issues that may
            impact attribution accuracy.
          </p>
        </div>
        <button
          type="button"
          onClick={() => runMutation.mutate()}
          disabled={runMutation.isPending}
          style={{
            padding: `${t.space.sm}px ${t.space.lg}px`,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightMedium,
            color: t.color.surface,
            backgroundColor: t.color.accent,
            border: 'none',
            borderRadius: t.radius.sm,
            cursor: runMutation.isPending ? 'wait' : 'pointer',
          }}
        >
          {runMutation.isPending ? 'Running checks…' : 'Run data quality checks'}
        </button>
      </div>

      {/* KPI tiles */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        <Tile
          label="Max freshness lag (hours)"
          value={
            freshness.length
              ? (Math.max(...freshness.map((f) => f.metric_value)) / 60).toFixed(1)
              : '--'
          }
          description="Across Meiro and cost sources"
        />
        <Tile
          label="Journeys missing profile ID"
          value={completenessMissingProfile ? `${completenessMissingProfile.metric_value.toFixed(1)}%` : '--'}
          description="Higher values mean poor joinability"
        />
        <Tile
          label="Journeys missing timestamps"
          value={completenessMissingTs ? `${completenessMissingTs.metric_value.toFixed(1)}%` : '--'}
          description="Without timestamps, windowing & paths break"
        />
        <Tile
          label="Duplicate IDs"
          value={duplication ? `${duplication.metric_value.toFixed(1)}%` : '--'}
          description="Potential double counting"
        />
        <Tile
          label="Attributable conversions"
          value={joinRate ? `${joinRate.metric_value.toFixed(1)}%` : '--'}
          description="Conversions with at least one eligible touchpoint"
        />
      </div>

      {/* Alerts list */}
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
          Alerts
        </h2>
        <p style={{ margin: '0 0 16px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Recent issues detected by data quality rules. Acknowledge or resolve once investigated.
        </p>

        {alertsQuery.isLoading ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading alerts…</p>
        ) : alertsQuery.data && alertsQuery.data.length > 0 ? (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Time</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Rule</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Source</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Metric</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Value</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Baseline</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Severity</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Status</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {alertsQuery.data.map((a) => {
                  const sev = a.rule?.severity ?? 'warn'
                  const sevColor =
                    sev === 'critical' ? t.color.danger : sev === 'info' ? t.color.textSecondary : t.color.warning
                  return (
                    <tr key={a.id} style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                        {new Date(a.triggered_at).toLocaleString()}
                      </td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>{a.rule?.name ?? a.message}</td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>{a.rule?.source ?? '—'}</td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>{a.rule?.metric_key ?? '—'}</td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                        {a.metric_value.toFixed(2)}
                      </td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                        {a.baseline_value != null ? a.baseline_value.toFixed(2) : '—'}
                      </td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center', color: sevColor }}>
                        {sev}
                      </td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>{a.status}</td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                        {a.status !== 'acked' && (
                          <button
                            type="button"
                            onClick={() => updateAlertStatus.mutate({ id: a.id, status: 'acked' })}
                            style={{
                              marginRight: t.space.xs,
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              fontSize: t.font.sizeXs,
                              color: t.color.textSecondary,
                              background: 'transparent',
                              border: `1px solid ${t.color.border}`,
                              borderRadius: t.radius.sm,
                              cursor: 'pointer',
                            }}
                          >
                            Ack
                          </button>
                        )}
                        {a.status !== 'resolved' && (
                          <button
                            type="button"
                            onClick={() => updateAlertStatus.mutate({ id: a.id, status: 'resolved' })}
                            style={{
                              padding: `${t.space.xs}px ${t.space.sm}px`,
                              fontSize: t.font.sizeXs,
                              color: t.color.success,
                              background: 'transparent',
                              border: `1px solid ${t.color.success}`,
                              borderRadius: t.radius.sm,
                              cursor: 'pointer',
                            }}
                          >
                            Resolve
                          </button>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No alerts yet. Run checks to populate data.</p>
        )}
      </div>
    </div>
  )
}

function Tile(props: { label: string; value: string; description: string }) {
  return (
    <div
      style={{
        background: t.color.surface,
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.lg,
        padding: t.space.lg,
        boxShadow: t.shadowSm,
      }}
    >
      <p
        style={{
          margin: '0 0 4px',
          fontSize: t.font.sizeXs,
          fontWeight: t.font.weightMedium,
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color: t.color.textSecondary,
        }}
      >
        {props.label}
      </p>
      <p
        style={{
          margin: '0 0 6px',
          fontSize: t.font.sizeXl,
          fontWeight: t.font.weightBold,
          color: t.color.text,
        }}
      >
        {props.value}
      </p>
      <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textMuted }}>{props.description}</p>
    </div>
  )
}

