import { Fragment, useState, useCallback, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { apiGetJson, apiSendJson, withQuery } from '../lib/apiClient'

// --- Types ---

interface DQSnapshot {
  id: number
  ts_bucket: string
  source: string
  metric_key: string
  metric_value: number
  meta?: Record<string, unknown>
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
  note?: string | null
  rule?: {
    name?: string | null
    metric_key?: string | null
    source?: string | null
    severity?: string | null
  } | null
}

interface LastRun {
  last_bucket: string | null
  has_data: boolean
}

interface DQDrilldown {
  metric_key: string
  definition: string
  breakdown: { source: string; value: number }[]
  top_offenders: { key?: string; value?: unknown }[]
  recommended_actions: string[]
}

interface RunResult {
  snapshots_created: number
  alerts_created: number
  latest_bucket: string | null
  duration_ms?: number
}

interface MeiroWebhookEvent {
  received_at: string
  received_count: number
  stored_total: number
  replace: boolean
  ip?: string | null
  user_agent?: string | null
  payload_shape?: string | null
  payload_excerpt?: string | null
  payload_truncated?: boolean
  payload_bytes?: number
  payload_json_valid?: boolean
  conversion_event_names?: string[]
  channels_detected?: string[]
}

interface MeiroWebhookSuggestions {
  generated_at: string
  events_analyzed: number
  total_conversions_observed: number
  total_touchpoints_observed: number
  dedup_key_suggestion: string
  kpi_suggestions: Array<{
    id: string
    label: string
    type: string
    event_name: string
    value_field?: string | null
    coverage_pct?: number
  }>
  taxonomy_suggestions: {
    channel_rules: Array<{ name: string; channel: string; observed_count?: number }>
    source_aliases: Record<string, string>
    medium_aliases: Record<string, string>
  }
  apply_payloads: {
    kpis: {
      definitions: Array<{
        id: string
        label: string
        type: string
        event_name: string
        value_field?: string | null
        weight: number
        lookback_days?: number | null
      }>
      primary_kpi_id?: string | null
    }
    taxonomy: {
      channel_rules: Array<Record<string, unknown>>
      source_aliases: Record<string, string>
      medium_aliases: Record<string, string>
    }
  }
}

// --- Threshold config per metric ---


const METRIC_CONFIG: Record<
  string,
  { okThreshold?: number; warnThreshold?: number; criticalThreshold?: number; unit: string; invert?: boolean }
> = {
  freshness_lag_minutes: {
    okThreshold: 60,
    warnThreshold: 180,
    criticalThreshold: 360,
    unit: 'min',
    invert: false,
  },
  missing_profile_pct: {
    okThreshold: 2,
    warnThreshold: 5,
    criticalThreshold: 15,
    unit: '%',
    invert: false,
  },
  missing_timestamp_pct: {
    okThreshold: 2,
    warnThreshold: 5,
    criticalThreshold: 15,
    unit: '%',
    invert: false,
  },
  duplicate_id_pct: {
    okThreshold: 1,
    warnThreshold: 3,
    criticalThreshold: 10,
    unit: '%',
    invert: false,
  },
  conversion_attributable_pct: {
    okThreshold: 80,
    warnThreshold: 60,
    criticalThreshold: 40,
    unit: '%',
    invert: true,
  },
}

function getStatus(
  value: number,
  cfg: { okThreshold?: number; warnThreshold?: number; criticalThreshold?: number; invert?: boolean }
): 'ok' | 'warning' | 'critical' {
  if (!cfg) return 'ok'
  const { okThreshold = 0, warnThreshold = 100, invert = false } = cfg
  const fn = invert
    ? (v: number, o: number, w: number) => (v >= o ? 'ok' : v >= w ? 'warning' : 'critical')
    : (v: number, o: number, w: number) => (v <= o ? 'ok' : v <= w ? 'warning' : 'critical')
  return fn(value, okThreshold, warnThreshold) as 'ok' | 'warning' | 'critical'
}

// --- Main component ---

export default function DataQuality() {
  const queryClient = useQueryClient()
  const tilesRef = useRef<HTMLDivElement>(null)

  const [period] = useState<string>(() => {
    const d = new Date()
    const start = new Date(d)
    start.setDate(start.getDate() - 7)
    return `${start.toISOString().slice(0, 10)} to ${d.toISOString().slice(0, 10)}`
  })
  const [scope, setScope] = useState<string>('overall')
  const [drilldownMetric, setDrilldownMetric] = useState<string | null>(null)
  const [alertStatusFilter, setAlertStatusFilter] = useState<string>('')
  const [alertSeverityFilter, setAlertSeverityFilter] = useState<string>('')
  const [alertSourceFilter, setAlertSourceFilter] = useState<string>('')
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null)
  const [lastRunMeta, setLastRunMeta] = useState<{ bucket: string | null; duration_ms?: number } | null>(null)
  const [expandedWebhookRow, setExpandedWebhookRow] = useState<number | null>(null)
  const [showApplyAllPreview, setShowApplyAllPreview] = useState(false)

  const showToast = useCallback((message: string, type: 'success' | 'error') => {
    setToast({ message, type })
    setTimeout(() => setToast(null), 4000)
  }, [])

  // Run DQ checks
  const runMutation = useMutation({
    mutationFn: async () => {
      return apiSendJson<RunResult>('/api/data-quality/run', 'POST', undefined, {
        fallbackMessage: 'Failed to run data quality job',
      })
    },
    onSuccess: (data) => {
      setLastRunMeta({
        bucket: data.latest_bucket,
        duration_ms: data.duration_ms,
      })
      queryClient.invalidateQueries({ queryKey: ['dq-snapshots'] })
      queryClient.invalidateQueries({ queryKey: ['dq-alerts'] })
      queryClient.invalidateQueries({ queryKey: ['dq-last-run'] })
      showToast(`DQ run complete at ${new Date().toLocaleTimeString()}`, 'success')
    },
    onError: (err: Error) => {
      showToast(err.message || 'DQ run failed', 'error')
    },
  })

  const lastRunQuery = useQuery<LastRun>({
    queryKey: ['dq-last-run'],
    queryFn: async () => apiGetJson<LastRun>('/api/data-quality/last-run', { fallbackMessage: 'Failed to load last run' }),
  })

  const snapshotsQuery = useQuery<DQSnapshot[]>({
    queryKey: ['dq-snapshots'],
    queryFn: async () =>
      apiGetJson<DQSnapshot[]>(withQuery('/api/data-quality/snapshots', { limit: 200 }), {
        fallbackMessage: 'Failed to load data quality snapshots',
      }),
  })

  const alertsQuery = useQuery<DQAlert[]>({
    queryKey: ['dq-alerts', alertStatusFilter, alertSeverityFilter, alertSourceFilter],
    queryFn: async () => {
      const params = new URLSearchParams({ limit: '100' })
      if (alertStatusFilter) params.set('status', alertStatusFilter)
      if (alertSeverityFilter) params.set('severity', alertSeverityFilter)
      if (alertSourceFilter) params.set('source', alertSourceFilter)
      return apiGetJson<DQAlert[]>(`/api/data-quality/alerts?${params}`, {
        fallbackMessage: 'Failed to load data quality alerts',
      })
    },
  })

  const meiroWebhookEventsQuery = useQuery<{ items: MeiroWebhookEvent[]; total: number }>({
    queryKey: ['meiro-webhook-events-dq'],
    queryFn: async () =>
      apiGetJson<{ items: MeiroWebhookEvent[]; total: number }>(
        withQuery('/api/connectors/meiro/webhook/events', { limit: 100, include_payload: 1 }),
        { fallbackMessage: 'Failed to load Meiro webhook debug log' },
      ),
  })

  const meiroWebhookSuggestionsQuery = useQuery<MeiroWebhookSuggestions>({
    queryKey: ['meiro-webhook-suggestions'],
    queryFn: async () =>
      apiGetJson<MeiroWebhookSuggestions>(
        withQuery('/api/connectors/meiro/webhook/suggestions', { limit: 100 }),
        { fallbackMessage: 'Failed to build webhook suggestions' },
      ),
  })

  const applyKpiSuggestionsMutation = useMutation({
    mutationFn: async (payload: MeiroWebhookSuggestions['apply_payloads']['kpis']) =>
      apiSendJson('/api/kpis', 'POST', payload, { fallbackMessage: 'Failed to apply KPI suggestions' }),
    onSuccess: () => showToast('KPI suggestions applied.', 'success'),
    onError: (err: Error) => showToast(err.message || 'Failed to apply KPI suggestions', 'error'),
  })

  const applyTaxonomySuggestionsMutation = useMutation({
    mutationFn: async (payload: MeiroWebhookSuggestions['apply_payloads']['taxonomy']) =>
      apiSendJson('/api/taxonomy', 'POST', payload, { fallbackMessage: 'Failed to apply taxonomy suggestions' }),
    onSuccess: () => showToast('Taxonomy suggestions applied.', 'success'),
    onError: (err: Error) => showToast(err.message || 'Failed to apply taxonomy suggestions', 'error'),
  })

  const applyAllSuggestionsMutation = useMutation({
    mutationFn: async (payloads: MeiroWebhookSuggestions['apply_payloads']) => {
      await apiSendJson('/api/kpis', 'POST', payloads.kpis, { fallbackMessage: 'Failed to apply KPI suggestions' })
      await apiSendJson('/api/taxonomy', 'POST', payloads.taxonomy, { fallbackMessage: 'Failed to apply taxonomy suggestions' })
    },
    onSuccess: () => {
      setShowApplyAllPreview(false)
      showToast('Applied KPI and taxonomy suggestions.', 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to apply all suggestions', 'error'),
  })

  const drilldownQuery = useQuery<DQDrilldown>({
    queryKey: ['dq-drilldown', drilldownMetric],
    queryFn: async () => {
      if (!drilldownMetric) throw new Error('No metric')
      return apiGetJson<DQDrilldown>(withQuery('/api/data-quality/drilldown', { metric_key: drilldownMetric }), {
        fallbackMessage: 'Failed to load drilldown',
      })
    },
    enabled: !!drilldownMetric,
  })

  const updateAlertStatus = useMutation({
    mutationFn: async (payload: { id: number; status: string }) => {
      return apiSendJson<any>(`/api/data-quality/alerts/${payload.id}/status`, 'POST', { status: payload.status }, {
        fallbackMessage: 'Failed to update alert status',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dq-alerts'] })
    },
  })

  const updateAlertNote = useMutation({
    mutationFn: async (payload: { id: number; note: string }) => {
      return apiSendJson<any>(`/api/data-quality/alerts/${payload.id}/note`, 'POST', { note: payload.note }, {
        fallbackMessage: 'Failed to update alert note',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dq-alerts'] })
    },
  })

  const lastBucket = lastRunMeta?.bucket ?? lastRunQuery.data?.last_bucket ?? null
  const lastRunDuration = lastRunMeta?.duration_ms

  // Derive tile data from snapshots (filter by scope)
  const snapshots = snapshotsQuery.data ?? []
  const latest = snapshots.filter((s) => {
    if (scope === 'overall') return true
    return s.source === scope
  })

  const freshness = latest.filter((s) => s.metric_key === 'freshness_lag_minutes')
  const completenessMissingProfile = latest.find((s) => s.metric_key === 'missing_profile_pct')
  const completenessMissingTs = latest.find((s) => s.metric_key === 'missing_timestamp_pct')
  const duplication = latest.find((s) => s.metric_key === 'duplicate_id_pct')
  const joinRate = latest.find((s) => s.metric_key === 'conversion_attributable_pct')

  // Trend: compare latest vs previous run (by ts_bucket)
  const buckets = [...new Set(snapshots.map((s) => s.ts_bucket))].sort().reverse()
  const latestBucket = buckets[0]
  const prevBucket = buckets[1]
  const getTrend = (metricKey: string, source?: string) => {
    if (!latestBucket || !prevBucket) return null
    const latestSnap = snapshots.find(
      (s) => s.metric_key === metricKey && s.ts_bucket === latestBucket && (!source || s.source === source)
    )
    const prevSnap = snapshots.find(
      (s) => s.metric_key === metricKey && s.ts_bucket === prevBucket && (!source || s.source === source)
    )
    if (!latestSnap || !prevSnap) return null
    const delta = latestSnap.metric_value - prevSnap.metric_value
    return { delta, improved: metricKey === 'conversion_attributable_pct' ? delta > 0 : delta < 0 }
  }

  const exportAlerts = () => {
    const alerts = alertsQuery.data ?? []
    const headers = ['id', 'triggered_at', 'rule_name', 'source', 'metric_key', 'severity', 'status', 'metric_value', 'baseline_value', 'message', 'note']
    const rows = alerts.map((a) =>
      [
        a.id,
        a.triggered_at,
        a.rule?.name ?? '',
        a.rule?.source ?? '',
        a.rule?.metric_key ?? '',
        a.rule?.severity ?? '',
        a.status,
        a.metric_value,
        a.baseline_value ?? '',
        `"${(a.message ?? '').replace(/"/g, '""')}"`,
        `"${(a.note ?? '').replace(/"/g, '""')}"`,
      ].join(',')
    )
    const csv = [headers.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `dq-alerts-${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  // DQ Score: derive from metrics
  const scoreInputs = {
    missing_profile: completenessMissingProfile?.metric_value ?? 0,
    missing_ts: completenessMissingTs?.metric_value ?? 0,
    dup: duplication?.metric_value ?? 0,
    freshness_hours: freshness.length ? Math.max(...freshness.map((f) => f.metric_value)) / 60 : 0,
    attributable: joinRate?.metric_value ?? 0,
  }
  let dqScore = 100
  dqScore -= scoreInputs.missing_profile * 1.5
  dqScore -= scoreInputs.missing_ts * 1.5
  dqScore -= scoreInputs.dup * 0.5
  dqScore -= Math.min(20, scoreInputs.freshness_hours * 2)
  dqScore -= Math.max(0, 100 - scoreInputs.attributable) * 0.3
  dqScore = Math.max(0, Math.min(100, Math.round(dqScore)))
  const dqLabel = dqScore >= 80 ? 'High' : dqScore >= 50 ? 'Medium' : 'Low'
  const topDrivers: string[] = []
  if (scoreInputs.missing_profile > 5) topDrivers.push(`${scoreInputs.missing_profile.toFixed(1)}% missing profile_id`)
  if (scoreInputs.missing_ts > 5) topDrivers.push(`${scoreInputs.missing_ts.toFixed(1)}% missing timestamps`)
  if (scoreInputs.dup > 3) topDrivers.push(`${scoreInputs.dup.toFixed(1)}% duplicate IDs`)
  if (scoreInputs.freshness_hours > 6) topDrivers.push(`${scoreInputs.freshness_hours.toFixed(1)}h freshness lag`)
  if (scoreInputs.attributable < 70) topDrivers.push(`${scoreInputs.attributable.toFixed(1)}% attributable conversions`)

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      {/* Toast */}
      {toast && (
        <div
          style={{
            position: 'fixed',
            top: 16,
            right: 16,
            padding: `${t.space.sm}px ${t.space.lg}px`,
            borderRadius: t.radius.sm,
            background: toast.type === 'success' ? t.color.successMuted : t.color.dangerMuted,
            color: toast.type === 'success' ? t.color.success : t.color.danger,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightMedium,
            zIndex: 9999,
            boxShadow: t.shadow,
          }}
        >
          {toast.message}
        </div>
      )}

      {/* Header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: t.space.md,
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
            Monitor freshness, completeness, and join rates. Alerts highlight issues that may impact attribution accuracy.
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
            display: 'flex',
            alignItems: 'center',
            gap: t.space.sm,
          }}
        >
          {runMutation.isPending && (
            <span
              style={{
                width: 14,
                height: 14,
                border: `2px solid ${t.color.surface}`,
                borderTopColor: 'transparent',
                borderRadius: '50%',
                animation: 'spin 0.8s linear infinite',
              }}
            />
          )}
          {runMutation.isPending ? 'Running checks…' : 'Run data quality checks'}
        </button>
      </div>

      {/* Measurement context bar */}
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: t.space.lg,
          alignItems: 'center',
          padding: `${t.space.sm}px ${t.space.md}px`,
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.sm,
          marginBottom: t.space.xl,
          fontSize: t.font.sizeSm,
          color: t.color.textSecondary,
        }}
      >
        <span title="Evaluation period">
          <strong style={{ color: t.color.text }}>Period:</strong> {period}
        </span>
        <span title="Conversion type">
          <strong style={{ color: t.color.text }}>Conversion:</strong> purchase (primary)
        </span>
        <span>
          <strong style={{ color: t.color.text }}>Scope:</strong>{' '}
          <select
            value={scope}
            onChange={(e) => setScope(e.target.value)}
            style={{
              marginLeft: t.space.xs,
              padding: '2px 6px',
              fontSize: t.font.sizeSm,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              background: t.color.surface,
              color: t.color.text,
            }}
          >
            <option value="overall">Overall</option>
            <option value="meiro_web">Meiro</option>
            <option value="google_ads_cost">Google</option>
            <option value="meta_cost">Meta</option>
            <option value="linkedin_cost">LinkedIn</option>
            <option value="journeys">Journeys</option>
          </select>
        </span>
        <span>
          <strong style={{ color: t.color.text }}>Last run:</strong>{' '}
          {lastBucket ? new Date(lastBucket).toLocaleString() : '—'}
          {lastRunDuration != null && ` (${(lastRunDuration / 1000).toFixed(1)}s)`}
        </span>
      </div>

      {/* DQ Score card */}
      <div
        style={{
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          padding: t.space.lg,
          marginBottom: t.space.xl,
          boxShadow: t.shadowSm,
          display: 'flex',
          alignItems: 'center',
          gap: t.space.xl,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: t.space.md }}>
          <span style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Data Confidence{' '}
            <span
              title="How is this computed? Combined score (0–100) from: missing profile_id, missing timestamps, duplicate IDs, freshness lag, and attributable conversions. Penalties reduce the score; higher attributable % increases it."
              style={{ cursor: 'help', color: t.color.textMuted }}
            >
              ?
            </span>
          </span>
          <span
            title="Combined score from freshness, completeness, duplication, and attributable conversions. Higher is better."
            style={{
              fontSize: t.font.size2xl,
              fontWeight: t.font.weightBold,
              color:
                dqLabel === 'High'
                  ? t.color.success
                  : dqLabel === 'Medium'
                    ? t.color.warning
                    : t.color.danger,
            }}
          >
            {dqScore}
          </span>
          <span
            style={{
              padding: '2px 8px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              background:
                dqLabel === 'High'
                  ? t.color.successMuted
                  : dqLabel === 'Medium'
                    ? t.color.warningMuted
                    : t.color.dangerMuted,
              color:
                dqLabel === 'High'
                  ? t.color.success
                  : dqLabel === 'Medium'
                    ? t.color.warning
                    : t.color.danger,
            }}
          >
            {dqLabel}
          </span>
        </div>
        {topDrivers.length > 0 && (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Top drivers lowering score: {topDrivers.slice(0, 2).join('; ')}
          </div>
        )}
      </div>

      {/* KPI tiles */}
      <div
        ref={tilesRef}
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          gap: t.space.md,
          marginBottom: t.space.xl,
        }}
      >
        <Tile
          label="Max freshness lag (hours)"
          metricKey="freshness_lag_minutes"
          value={
            freshness.length
              ? (Math.max(...freshness.map((f) => f.metric_value)) / 60).toFixed(1)
              : null
          }
          naReason={
            !freshness.length
              ? 'No freshness data: no sources with timestamps, or no DQ run yet.'
              : undefined
          }
          description="Across Meiro and cost sources"
          trend={getTrend('freshness_lag_minutes')}
          onClick={() => setDrilldownMetric('freshness_lag_minutes')}
        />
        <Tile
          label="Journeys missing profile ID"
          metricKey="missing_profile_pct"
          value={completenessMissingProfile ? completenessMissingProfile.metric_value.toFixed(1) : null}
          description="Higher values mean poor joinability"
          trend={getTrend('missing_profile_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('missing_profile_pct')}
        />
        <Tile
          label="Journeys missing timestamps"
          metricKey="missing_timestamp_pct"
          value={completenessMissingTs ? completenessMissingTs.metric_value.toFixed(1) : null}
          description="Without timestamps, windowing & paths break"
          trend={getTrend('missing_timestamp_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('missing_timestamp_pct')}
        />
        <Tile
          label="Duplicate IDs"
          metricKey="duplicate_id_pct"
          value={duplication ? duplication.metric_value.toFixed(1) : null}
          description="Potential double counting"
          trend={getTrend('duplicate_id_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('duplicate_id_pct')}
        />
        <Tile
          label="Attributable conversions"
          metricKey="conversion_attributable_pct"
          value={joinRate ? joinRate.metric_value.toFixed(1) : null}
          description="Conversions with at least one eligible touchpoint"
          trend={getTrend('conversion_attributable_pct', scope === 'overall' ? undefined : scope)}
          onClick={() => setDrilldownMetric('conversion_attributable_pct')}
        />
      </div>

      {/* Meiro webhook debug log */}
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
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: t.space.md,
            marginBottom: t.space.md,
          }}
        >
          <div>
            <h2 style={{ margin: '0 0 4px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Meiro webhook event log
            </h2>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Last 100 webhook requests with payload excerpts for ingestion debugging.
            </p>
          </div>
          <button
            type="button"
            onClick={() => meiroWebhookEventsQuery.refetch()}
            style={{
              padding: `${t.space.xs}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Refresh
          </button>
        </div>

        {meiroWebhookEventsQuery.isLoading ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading webhook events…</p>
        ) : meiroWebhookEventsQuery.isError ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>
            {(meiroWebhookEventsQuery.error as Error)?.message || 'Failed to load webhook events'}
          </p>
        ) : (meiroWebhookEventsQuery.data?.items?.length || 0) > 0 ? (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Received</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Profiles</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Mode</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Payload size</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Detected events</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Detected channels</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Source</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Payload</th>
                </tr>
              </thead>
              <tbody>
                {(meiroWebhookEventsQuery.data?.items || []).map((event, idx) => {
                  const isExpanded = expandedWebhookRow === idx
                  return (
                    <Fragment key={`evt-${idx}`}>
                      <tr style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                          {event.received_at ? new Date(event.received_at).toLocaleString() : '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                          {(event.received_count ?? 0).toLocaleString()}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>{event.replace ? 'replace' : 'append'}</td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>
                          {event.payload_bytes != null ? `${event.payload_bytes.toLocaleString()} B` : '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                          {event.conversion_event_names?.length ? event.conversion_event_names.join(', ') : '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                          {event.channels_detected?.length ? event.channels_detected.join(', ') : '—'}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                          {event.ip || '—'}
                          {event.user_agent ? (
                            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                              {event.user_agent}
                            </div>
                          ) : null}
                        </td>
                        <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6, flexWrap: 'wrap' }}>
                            <span
                              style={{
                                padding: '2px 6px',
                                borderRadius: 999,
                                fontSize: t.font.sizeXs,
                                fontWeight: t.font.weightMedium,
                                background: event.payload_json_valid === false ? t.color.dangerMuted : t.color.successMuted,
                                color: event.payload_json_valid === false ? t.color.danger : t.color.success,
                              }}
                            >
                              {event.payload_json_valid === false ? 'JSON invalid' : 'JSON valid'}
                            </span>
                            {event.payload_truncated ? (
                              <span style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>Truncated</span>
                            ) : null}
                            <button
                              type="button"
                              onClick={() => setExpandedWebhookRow(isExpanded ? null : idx)}
                              style={{
                                padding: '2px 8px',
                                fontSize: t.font.sizeXs,
                                color: t.color.textSecondary,
                                background: 'transparent',
                                border: `1px solid ${t.color.border}`,
                                borderRadius: t.radius.sm,
                                cursor: 'pointer',
                              }}
                            >
                              {isExpanded ? 'Hide' : 'View'}
                            </button>
                            <button
                              type="button"
                              onClick={async () => {
                                try {
                                  await navigator.clipboard.writeText(event.payload_excerpt || '')
                                  showToast('Payload copied to clipboard.', 'success')
                                } catch {
                                  showToast('Failed to copy payload.', 'error')
                                }
                              }}
                              style={{
                                padding: '2px 8px',
                                fontSize: t.font.sizeXs,
                                color: t.color.textSecondary,
                                background: 'transparent',
                                border: `1px solid ${t.color.border}`,
                                borderRadius: t.radius.sm,
                                cursor: 'pointer',
                              }}
                            >
                              Copy
                            </button>
                          </div>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr style={{ borderBottom: `1px solid ${t.color.borderLight}` }}>
                          <td colSpan={8} style={{ padding: `${t.space.sm}px ${t.space.md}px` }}>
                            <pre
                              style={{
                                margin: 0,
                                fontSize: t.font.sizeXs,
                                background: t.color.bgSubtle,
                                border: `1px solid ${t.color.borderLight}`,
                                borderRadius: t.radius.sm,
                                padding: t.space.sm,
                                whiteSpace: 'pre-wrap',
                                wordBreak: 'break-word',
                                maxHeight: 260,
                                overflow: 'auto',
                              }}
                            >
                              {event.payload_excerpt || 'No payload captured'}
                              {event.payload_truncated ? '\n\n[Payload truncated in log]' : ''}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            No webhook events captured yet. Send data to `/api/connectors/meiro/profiles` first.
          </p>
        )}
      </div>

      {/* Webhook-based setup suggestions */}
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
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.md, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ margin: '0 0 4px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Auto-setup suggestions from Meiro payloads
            </h2>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Suggested KPI/conversion and taxonomy defaults based on recent webhook events.
            </p>
          </div>
          <button
            type="button"
            onClick={() => meiroWebhookSuggestionsQuery.refetch()}
            style={{
              padding: `${t.space.xs}px ${t.space.md}px`,
              fontSize: t.font.sizeSm,
              color: t.color.textSecondary,
              background: 'transparent',
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              cursor: 'pointer',
            }}
          >
            Refresh suggestions
          </button>
        </div>

        {meiroWebhookSuggestionsQuery.isLoading ? (
          <p style={{ marginTop: t.space.md, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Analyzing webhook payloads…</p>
        ) : meiroWebhookSuggestionsQuery.isError ? (
          <p style={{ marginTop: t.space.md, fontSize: t.font.sizeSm, color: t.color.danger }}>
            {(meiroWebhookSuggestionsQuery.error as Error)?.message || 'Failed to generate suggestions'}
          </p>
        ) : meiroWebhookSuggestionsQuery.data ? (
          <div style={{ marginTop: t.space.md, display: 'grid', gap: t.space.md }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Analyzed <strong>{meiroWebhookSuggestionsQuery.data.events_analyzed}</strong> events,
              {' '}<strong>{meiroWebhookSuggestionsQuery.data.total_conversions_observed.toLocaleString()}</strong> conversions,
              {' '}<strong>{meiroWebhookSuggestionsQuery.data.total_touchpoints_observed.toLocaleString()}</strong> touchpoints.
              {' '}Suggested dedup key: <strong>{meiroWebhookSuggestionsQuery.data.dedup_key_suggestion}</strong>.
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                KPI / conversion suggestions
              </div>
              {(meiroWebhookSuggestionsQuery.data.kpi_suggestions || []).length ? (
                <div style={{ display: 'grid', gap: 6 }}>
                  {meiroWebhookSuggestionsQuery.data.kpi_suggestions.slice(0, 6).map((kpi) => (
                    <div key={kpi.id} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                      <strong>{kpi.label}</strong> ({kpi.type}) · event: <code>{kpi.event_name}</code>
                      {kpi.value_field ? <> · value field: <code>{kpi.value_field}</code></> : null}
                      {kpi.coverage_pct != null ? <> · coverage: {kpi.coverage_pct.toFixed(1)}%</> : null}
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No KPI suggestions yet.</div>
              )}
              <div style={{ marginTop: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => applyKpiSuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads.kpis)}
                  disabled={applyKpiSuggestionsMutation.isPending || !(meiroWebhookSuggestionsQuery.data.apply_payloads.kpis.definitions || []).length}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: '#fff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: applyKpiSuggestionsMutation.isPending ? 'wait' : 'pointer',
                    opacity: applyKpiSuggestionsMutation.isPending ? 0.75 : 1,
                  }}
                >
                  {applyKpiSuggestionsMutation.isPending ? 'Applying…' : 'Apply KPI suggestions'}
                </button>
              </div>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>
                Taxonomy suggestions
              </div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                {(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.channel_rules || []).length} channel rules,
                {' '}{Object.keys(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.source_aliases || {}).length} source aliases,
                {' '}{Object.keys(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.medium_aliases || {}).length} medium aliases.
              </div>
              {(meiroWebhookSuggestionsQuery.data.taxonomy_suggestions.channel_rules || []).slice(0, 5).map((rule) => (
                <div key={rule.name} style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginTop: 4 }}>
                  <strong>{rule.name}</strong> → <code>{rule.channel}</code>
                  {rule.observed_count != null ? <> · observed: {rule.observed_count}</> : null}
                </div>
              ))}
              <div style={{ marginTop: t.space.sm }}>
                <button
                  type="button"
                  onClick={() => applyTaxonomySuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy)}
                  disabled={applyTaxonomySuggestionsMutation.isPending}
                  style={{
                    padding: `${t.space.xs}px ${t.space.md}px`,
                    fontSize: t.font.sizeSm,
                    color: '#fff',
                    background: t.color.accent,
                    border: 'none',
                    borderRadius: t.radius.sm,
                    cursor: applyTaxonomySuggestionsMutation.isPending ? 'wait' : 'pointer',
                    opacity: applyTaxonomySuggestionsMutation.isPending ? 0.75 : 1,
                  }}
                >
                  {applyTaxonomySuggestionsMutation.isPending ? 'Applying…' : 'Apply taxonomy suggestions'}
                </button>
              </div>
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
              <button
                type="button"
                onClick={() => setShowApplyAllPreview(true)}
                disabled={applyAllSuggestionsMutation.isPending}
                style={{
                  padding: `${t.space.xs}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  color: '#fff',
                  background: t.color.accent,
                  border: 'none',
                  borderRadius: t.radius.sm,
                  cursor: applyAllSuggestionsMutation.isPending ? 'wait' : 'pointer',
                  opacity: applyAllSuggestionsMutation.isPending ? 0.75 : 1,
                }}
              >
                Apply all (preview first)
              </button>
            </div>
          </div>
        ) : null}
      </div>

      {/* Alerts */}
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
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: t.space.md,
            marginBottom: t.space.md,
          }}
        >
          <div>
            <h2 style={{ margin: '0 0 4px', fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>
              Alerts
            </h2>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Issues detected by data quality rules. Acknowledge or resolve once investigated.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, alignItems: 'center' }}>
            <button
              type="button"
              onClick={exportAlerts}
              style={{
                padding: `${t.space.xs}px ${t.space.md}px`,
                fontSize: t.font.sizeSm,
                color: t.color.textSecondary,
                background: 'transparent',
                border: `1px solid ${t.color.border}`,
                borderRadius: t.radius.sm,
                cursor: 'pointer',
              }}
            >
              Export DQ report
            </button>
            <a
              href="#"
              onClick={(e) => {
                e.preventDefault()
                tilesRef.current?.scrollIntoView({ behavior: 'smooth' })
              }}
              style={{ fontSize: t.font.sizeSm, color: t.color.accent }}
            >
              View metric trends
            </a>
          </div>
        </div>

        {/* Alert filters */}
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: t.space.sm,
            marginBottom: t.space.md,
          }}
        >
          {['', 'open', 'acked', 'resolved'].map((s) => (
            <button
              key={s || 'all'}
              type="button"
              onClick={() => setAlertStatusFilter(s)}
              style={{
                padding: '4px 10px',
                fontSize: t.font.sizeXs,
                borderRadius: 999,
                border: `1px solid ${alertStatusFilter === s ? t.color.accent : t.color.border}`,
                background: alertStatusFilter === s ? t.color.accentMuted : 'transparent',
                color: alertStatusFilter === s ? t.color.accent : t.color.textSecondary,
                cursor: 'pointer',
              }}
            >
              {s || 'All'}
            </button>
          ))}
          <select
            value={alertSeverityFilter}
            onChange={(e) => setAlertSeverityFilter(e.target.value)}
            style={{
              padding: '4px 8px',
              fontSize: t.font.sizeXs,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              background: t.color.surface,
              color: t.color.text,
            }}
          >
            <option value="">All severity</option>
            <option value="info">Info</option>
            <option value="warn">Warning</option>
            <option value="critical">Critical</option>
          </select>
          <select
            value={alertSourceFilter}
            onChange={(e) => setAlertSourceFilter(e.target.value)}
            style={{
              padding: '4px 8px',
              fontSize: t.font.sizeXs,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              background: t.color.surface,
              color: t.color.text,
            }}
          >
            <option value="">All sources</option>
            <option value="journeys">Journeys</option>
            <option value="meiro_web">Meiro</option>
            <option value="google_ads_cost">Google</option>
            <option value="meta_cost">Meta</option>
            <option value="linkedin_cost">LinkedIn</option>
            <option value="taxonomy">Taxonomy</option>
          </select>
        </div>

        {alertsQuery.isLoading ? (
          <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading alerts…</p>
        ) : alertsQuery.data && alertsQuery.data.length > 0 ? (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: t.font.sizeSm }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${t.color.border}` }}>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>First seen</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Rule</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Source</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'left' }}>Metric</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Value</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'right' }}>Threshold</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Severity</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Status</th>
                  <th style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {alertsQuery.data.map((a) => {
                  const sev = (a.rule?.severity ?? 'warn') as string
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
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                        {a.status === 'acked' ? 'Acknowledged' : a.status === 'resolved' ? 'Resolved' : 'Open'}
                      </td>
                      <td style={{ padding: `${t.space.sm}px ${t.space.md}px`, textAlign: 'center' }}>
                        <AlertActions
                          alert={a}
                          onStatus={(s) => updateAlertStatus.mutate({ id: a.id, status: s })}
                          onNote={(n) => updateAlertNote.mutate({ id: a.id, note: n })}
                        />
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div
            style={{
              padding: t.space.xl,
              textAlign: 'center',
              color: t.color.textSecondary,
              fontSize: t.font.sizeSm,
            }}
          >
            <p style={{ margin: '0 0 8px' }}>No alerts detected in selected period.</p>
            <a
              href="#"
              onClick={(e) => {
                e.preventDefault()
                tilesRef.current?.scrollIntoView({ behavior: 'smooth' })
              }}
              style={{ color: t.color.accent }}
            >
              View metric trends
            </a>
          </div>
        )}
      </div>

      {/* Drilldown drawer + backdrop */}
      {drilldownMetric && (
        <>
          <div
            role="presentation"
            onClick={() => setDrilldownMetric(null)}
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(0,0,0,0.3)',
              zIndex: 999,
            }}
          />
          <DrilldownDrawer
            metricKey={drilldownMetric}
            data={drilldownQuery.data}
            loading={drilldownQuery.isLoading}
            onClose={() => setDrilldownMetric(null)}
          />
        </>
      )}

      {/* Apply-all dry-run preview */}
      {showApplyAllPreview && meiroWebhookSuggestionsQuery.data && (
        <>
          <div
            role="presentation"
            onClick={() => setShowApplyAllPreview(false)}
            style={{
              position: 'fixed',
              inset: 0,
              background: 'rgba(0,0,0,0.3)',
              zIndex: 999,
            }}
          />
          <div
            role="dialog"
            aria-modal="true"
            aria-label="Apply all suggestions preview"
            style={{
              position: 'fixed',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: 'min(860px, calc(100vw - 32px))',
              maxHeight: '80vh',
              overflow: 'auto',
              background: t.color.surface,
              border: `1px solid ${t.color.borderLight}`,
              borderRadius: t.radius.lg,
              boxShadow: t.shadowSm,
              zIndex: 1000,
              padding: t.space.lg,
              display: 'grid',
              gap: t.space.md,
            }}
          >
            <div>
              <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Apply all suggestions (dry-run preview)</h3>
              <p style={{ margin: '6px 0 0', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                This preview shows what will be saved to KPI and taxonomy settings.
              </p>
            </div>

            <div style={{ display: 'grid', gap: t.space.sm, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              <div>
                KPI definitions to save: <strong>{meiroWebhookSuggestionsQuery.data.apply_payloads.kpis.definitions.length}</strong>
                {' '}· primary KPI: <strong>{meiroWebhookSuggestionsQuery.data.apply_payloads.kpis.primary_kpi_id || '—'}</strong>
              </div>
              <div>
                Taxonomy rules to save: <strong>{meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy.channel_rules.length}</strong>
                {' '}· source aliases: <strong>{Object.keys(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy.source_aliases || {}).length}</strong>
                {' '}· medium aliases: <strong>{Object.keys(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy.medium_aliases || {}).length}</strong>
              </div>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 6 }}>KPI payload preview</div>
              <pre style={{ margin: 0, fontSize: t.font.sizeXs, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {JSON.stringify(meiroWebhookSuggestionsQuery.data.apply_payloads.kpis, null, 2)}
              </pre>
            </div>

            <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.bgSubtle }}>
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: 6 }}>Taxonomy payload preview</div>
              <pre style={{ margin: 0, fontSize: t.font.sizeXs, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {JSON.stringify(meiroWebhookSuggestionsQuery.data.apply_payloads.taxonomy, null, 2)}
              </pre>
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
              <button
                type="button"
                onClick={() => setShowApplyAllPreview(false)}
                style={{
                  padding: `${t.space.xs}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  color: t.color.textSecondary,
                  background: 'transparent',
                  border: `1px solid ${t.color.border}`,
                  borderRadius: t.radius.sm,
                  cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => applyAllSuggestionsMutation.mutate(meiroWebhookSuggestionsQuery.data.apply_payloads)}
                disabled={applyAllSuggestionsMutation.isPending}
                style={{
                  padding: `${t.space.xs}px ${t.space.md}px`,
                  fontSize: t.font.sizeSm,
                  color: '#fff',
                  background: t.color.accent,
                  border: 'none',
                  borderRadius: t.radius.sm,
                  cursor: applyAllSuggestionsMutation.isPending ? 'wait' : 'pointer',
                  opacity: applyAllSuggestionsMutation.isPending ? 0.75 : 1,
                }}
              >
                {applyAllSuggestionsMutation.isPending ? 'Applying…' : 'Confirm apply all'}
              </button>
            </div>
          </div>
        </>
      )}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}

// --- Tile ---

function Tile(props: {
  label: string
  metricKey: string
  value: string | null
  naReason?: string
  description: string
  trend: { delta: number; improved: boolean } | null
  onClick: () => void
}) {
  const cfg = METRIC_CONFIG[props.metricKey]
  const numVal = props.value != null ? parseFloat(props.value) : null
  const status = cfg && numVal != null ? getStatus(numVal, cfg) : 'ok'
  const statusColor = status === 'ok' ? t.color.success : status === 'warning' ? t.color.warning : t.color.danger
  const thresholdHint =
    cfg && props.metricKey !== 'freshness_lag_minutes'
      ? `Warning > ${cfg.warnThreshold}${cfg.unit}; Critical > ${cfg.criticalThreshold}${cfg.unit}`
      : cfg && props.metricKey === 'conversion_attributable_pct'
        ? `Warning < ${cfg.warnThreshold}%; Critical < ${cfg.criticalThreshold}%`
        : cfg
          ? `Warning > ${cfg.warnThreshold}h; Critical > ${cfg.criticalThreshold}h`
          : ''
  const displayValue = props.value != null ? (props.metricKey === 'freshness_lag_minutes' ? props.value : `${props.value}%`) : 'N/A'

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={props.onClick}
      onKeyDown={(e) => e.key === 'Enter' && props.onClick()}
      title={`${props.description}${thresholdHint ? ` Thresholds: ${thresholdHint}` : ''}${props.naReason ? ` ${props.naReason}` : ''}`}
      style={{
        background: t.color.surface,
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.lg,
        padding: t.space.lg,
        boxShadow: t.shadowSm,
        cursor: 'pointer',
        transition: 'box-shadow 0.15s',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.08)'
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.boxShadow = t.shadowSm
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 4 }}>
        <p
          style={{
            margin: 0,
            fontSize: t.font.sizeXs,
            fontWeight: t.font.weightMedium,
            textTransform: 'uppercase',
            letterSpacing: '0.06em',
            color: t.color.textSecondary,
          }}
        >
          {props.label}
        </p>
        {numVal != null && cfg && (
          <span
            style={{
              padding: '2px 6px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              fontWeight: t.font.weightMedium,
              background: status === 'ok' ? t.color.successMuted : status === 'warning' ? t.color.warningMuted : t.color.dangerMuted,
              color: statusColor,
            }}
          >
            {status === 'ok' ? 'OK' : status === 'warning' ? 'Warning' : 'Critical'}
          </span>
        )}
        {props.value == null && props.naReason && (
          <span
            title={props.naReason}
            style={{
              padding: '2px 6px',
              borderRadius: 999,
              fontSize: t.font.sizeXs,
              color: t.color.textMuted,
            }}
          >
            N/A
          </span>
        )}
      </div>
      <p
        style={{
          margin: '0 0 4px',
          fontSize: t.font.sizeXl,
          fontWeight: t.font.weightBold,
          color: t.color.text,
        }}
      >
        {displayValue}
      </p>
      <div style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
        {props.trend != null && (
          <span
            style={{
              fontSize: t.font.sizeXs,
              color: props.trend.improved ? t.color.success : t.color.danger,
            }}
          >
            {props.trend.improved ? '↓' : '↑'} {Math.abs(props.trend.delta).toFixed(1)}
            {props.metricKey === 'freshness_lag_minutes' ? 'min' : '%'}
          </span>
        )}
        <p style={{ margin: 0, fontSize: t.font.sizeXs, color: t.color.textMuted }}>{props.description}</p>
      </div>
    </div>
  )
}

// --- AlertActions ---

function AlertActions(props: {
  alert: DQAlert
  onStatus: (status: string) => void
  onNote: (note: string) => void
}) {
  const [showNote, setShowNote] = useState(false)
  const [noteText, setNoteText] = useState(props.alert.note ?? '')
  const { alert, onStatus } = props
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: t.space.xs, alignItems: 'center' }}>
      {alert.status !== 'acked' && alert.status !== 'resolved' && (
        <button
          type="button"
          onClick={() => onStatus('acked')}
          style={{
            padding: '2px 6px',
            fontSize: t.font.sizeXs,
            color: t.color.textSecondary,
            background: 'transparent',
            border: `1px solid ${t.color.border}`,
            borderRadius: t.radius.sm,
            cursor: 'pointer',
          }}
        >
          Acknowledge
        </button>
      )}
      {alert.status !== 'resolved' && (
        <button
          type="button"
          onClick={() => onStatus('resolved')}
          style={{
            padding: '2px 6px',
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
      <button
        type="button"
        onClick={() => setShowNote(!showNote)}
        style={{
          padding: '2px 6px',
          fontSize: t.font.sizeXs,
          color: t.color.textSecondary,
          background: 'transparent',
          border: `1px solid ${t.color.border}`,
          borderRadius: t.radius.sm,
          cursor: 'pointer',
        }}
      >
        Add note
      </button>
      {showNote && (
        <div style={{ display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            type="text"
            value={noteText}
            onChange={(e) => setNoteText(e.target.value)}
            placeholder="Note..."
            style={{
              padding: '2px 6px',
              fontSize: t.font.sizeXs,
              border: `1px solid ${t.color.border}`,
              borderRadius: t.radius.sm,
              minWidth: 120,
            }}
          />
          <button
            type="button"
            onClick={() => {
              props.onNote(noteText)
              setShowNote(false)
            }}
            style={{
              padding: '2px 6px',
              fontSize: t.font.sizeXs,
              color: t.color.accent,
              background: 'transparent',
              border: 'none',
              cursor: 'pointer',
            }}
          >
            Save
          </button>
        </div>
      )}
    </div>
  )
}

// --- DrilldownDrawer ---

function DrilldownDrawer(props: {
  metricKey: string
  data: DQDrilldown | undefined
  loading: boolean
  onClose: () => void
}) {
  const { metricKey, data, loading, onClose } = props
  const labelMap: Record<string, string> = {
    freshness_lag_minutes: 'Max freshness lag',
    missing_profile_pct: 'Journeys missing profile ID',
    missing_timestamp_pct: 'Journeys missing timestamps',
    duplicate_id_pct: 'Duplicate IDs',
    conversion_attributable_pct: 'Attributable conversions',
  }
  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        right: 0,
        width: 420,
        maxWidth: '100%',
        height: '100%',
        background: t.color.surface,
        borderLeft: `1px solid ${t.color.border}`,
        boxShadow: '-4px 0 20px rgba(0,0,0,0.1)',
        zIndex: 1000,
        overflow: 'auto',
        padding: t.space.xl,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: t.space.lg }}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>
          {labelMap[metricKey] ?? metricKey}
        </h3>
        <button
          type="button"
          onClick={onClose}
          style={{
            padding: t.space.xs,
            background: 'transparent',
            border: 'none',
            cursor: 'pointer',
            fontSize: t.font.sizeLg,
            color: t.color.textSecondary,
          }}
        >
          ×
        </button>
      </div>
      {loading ? (
        <p style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading…</p>
      ) : data ? (
        <>
          <section style={{ marginBottom: t.space.xl }}>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Definition</h4>
            <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.text, lineHeight: 1.5 }}>
              {data.definition}
            </p>
          </section>
          <section style={{ marginBottom: t.space.xl }}>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Breakdown by source</h4>
            {data.breakdown.length > 0 ? (
              <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeSm, color: t.color.text }}>
                {data.breakdown.map((b) => (
                  <li key={b.source}>
                    {b.source}: {typeof b.value === 'number' ? (metricKey === 'freshness_lag_minutes' ? `${(b.value / 60).toFixed(1)}h` : `${b.value.toFixed(1)}%`) : b.value}
                  </li>
                ))}
              </ul>
            ) : (
              <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>N/A</p>
            )}
          </section>
          <section style={{ marginBottom: t.space.xl }}>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Top offenders</h4>
            {data.top_offenders.length > 0 ? (
              <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeSm, color: t.color.text }}>
                {data.top_offenders.map((o, i) => (
                  <li key={i}>{o.key ?? JSON.stringify(o)}</li>
                ))}
              </ul>
            ) : (
              <p style={{ margin: 0, fontSize: t.font.sizeSm, color: t.color.textMuted }}>N/A</p>
            )}
          </section>
          <section>
            <h4 style={{ margin: '0 0 8px', fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Recommended actions</h4>
            <ul style={{ margin: 0, paddingLeft: t.space.lg, fontSize: t.font.sizeSm, color: t.color.text, lineHeight: 1.6 }}>
              {data.recommended_actions.map((action, i) => (
                <li key={i}>{action}</li>
              ))}
            </ul>
          </section>
        </>
      ) : (
        <p style={{ fontSize: t.font.sizeSm, color: t.color.textMuted }}>No data</p>
      )}
    </div>
  )
}
