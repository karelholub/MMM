import { useMemo, useState } from 'react'
import type { CSSProperties } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { apiGetJson, apiSendJson, getUserContext, withQuery } from '../lib/apiClient'

type FeatureFlags = {
  journeys_enabled: boolean
  journey_examples_enabled: boolean
  funnel_builder_enabled: boolean
  funnel_diagnostics_enabled: boolean
}

type Status = 'draft' | 'active' | 'archived'

interface JourneySettingsVersion {
  id: string
  status: Status
  version_label: string
  description?: string | null
  created_at?: string | null
  updated_at?: string | null
  created_by?: string | null
  activated_at?: string | null
  activated_by?: string | null
  settings_json: Record<string, any>
  validation_json?: {
    valid?: boolean
    errors?: Array<{ path: string; message: string }>
    warnings?: Array<{ path: string; message: string }>
  } | null
  diff_json?: Record<string, any> | null
}

function formatDateTime(value?: string | null): string {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleString()
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

function setIn(obj: any, path: Array<string | number>, value: any): any {
  const next = deepClone(obj)
  let cursor = next
  for (let i = 0; i < path.length - 1; i += 1) {
    const key = path[i]
    if (cursor[key] == null) {
      cursor[key] = typeof path[i + 1] === 'number' ? [] : {}
    }
    cursor = cursor[key]
  }
  cursor[path[path.length - 1]] = value
  return next
}

function asInt(value: string, fallback: number): number {
  const n = Number(value)
  return Number.isFinite(n) ? Math.trunc(n) : fallback
}

function asFloat(value: string, fallback: number): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function defaultJourneysTemplate() {
  return {
    schema_version: '1.0',
    sessionization: {
      session_timeout_minutes: 30,
      start_new_session_on_events: [],
      lookback_window_days: 30,
      conversion_journeys_only: true,
      allow_all_journeys: false,
      max_steps_per_journey: 20,
      dedup_consecutive_identical_steps: true,
      timezone_handling: 'platform_default',
    },
    step_canonicalization: {
      first_match_wins: true,
      fallback_step: 'Other',
      collapse_rare_steps_into_other_default: true,
      rules: [
        { step_name: 'Paid Landing', priority: 10, enabled: true, channel_group_equals: ['paid'] },
        { step_name: 'Organic Landing', priority: 20, enabled: true, channel_group_equals: ['organic'] },
        { step_name: 'Product View / Content View', priority: 30, enabled: true, event_name_equals: ['product_view', 'content_view'] },
        { step_name: 'Add to Cart / Form Start', priority: 40, enabled: true, event_name_equals: ['add_to_cart', 'form_start'] },
        { step_name: 'Checkout / Form Submit', priority: 50, enabled: true, event_name_equals: ['checkout', 'form_submit'] },
        { step_name: 'Purchase / Lead Won', priority: 60, enabled: true, event_name_equals: ['purchase', 'lead_won'] },
      ],
    },
    paths_explorer_defaults: {
      default_sort: 'conversions_desc',
      top_paths_limit: 50,
      group_low_frequency_paths_into_other: true,
      trend_metric: 'conversion_rate',
      comparison_window: 'previous_period',
    },
    flow_defaults: {
      max_depth: 4,
      min_volume_threshold: 20,
      rare_event_threshold: 10,
      collapse_rare_into_other: true,
      max_nodes: 30,
      always_show_conversion_terminal_node: true,
    },
    funnels_defaults: {
      default_counting_method: 'uniques',
      default_conversion_window_seconds: 604800,
      step_to_step_max_time_enabled: false,
      step_to_step_max_time_seconds: null,
      attribution_model_default: 'data_driven',
      breakdown_top_n: 5,
    },
    diagnostics_defaults: {
      enabled: false,
      baseline_mode: 'previous_period',
      sensitivity: 'medium',
      signals: {
        time_to_next_step_spike: true,
        device_skew_change: true,
        geo_skew_change: true,
        consent_opt_out_spike: false,
        error_event_rate_spike: false,
        landing_page_group_change: false,
      },
      output_policy: 'hypotheses_only',
      require_evidence_for_every_claim: true,
      confidence_thresholds: { low_max: 39, medium_max: 69, high_min: 70 },
    },
    performance_guardrails: {
      aggregation_reprocess_window_days: 3,
      journey_instance_sampling_retention_days: null,
      sampling_rate_example_journeys: 0,
      max_flow_query_lookback_window_days: 90,
      max_flow_date_range_days_before_weekly: 45,
    },
  }
}

export default function JourneysSettingsSection({
  featureFlags,
}: {
  featureFlags: FeatureFlags
}) {
  const queryClient = useQueryClient()
  const role = useMemo(() => getUserContext().role.trim().toLowerCase(), [])
  const canActivate = role === 'admin' || role === 'power_user' || role === 'editor'

  const [statusFilter, setStatusFilter] = useState<'all' | Status>('all')
  const [search, setSearch] = useState('')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [editorMode, setEditorMode] = useState<'basic' | 'advanced'>('basic')
  const [draftJson, setDraftJson] = useState('')
  const [draftObj, setDraftObj] = useState<Record<string, any> | null>(null)
  const [draftDescription, setDraftDescription] = useState('')
  const [parseError, setParseError] = useState<string | null>(null)
  const [validationResult, setValidationResult] = useState<any | null>(null)
  const [previewResult, setPreviewResult] = useState<any | null>(null)

  const versionsQuery = useQuery<JourneySettingsVersion[]>({
    queryKey: ['journeys-settings-versions'],
    queryFn: async () => apiGetJson<JourneySettingsVersion[]>('/api/settings/journeys/versions', {
      fallbackMessage: 'Failed to load journey settings versions',
    }),
  })

  const createDraftMutation = useMutation({
    mutationFn: async (payload: { description?: string; settings_json?: Record<string, any> }) => {
      return apiSendJson<JourneySettingsVersion>('/api/settings/journeys/versions', 'POST', {
        created_by: 'ui',
        ...payload,
      }, {
        fallbackMessage: 'Failed to create journey settings draft',
      })
    },
    onSuccess: (next) => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
      setSelectedId(next.id)
      setDraftObj(next.settings_json)
      setDraftJson(JSON.stringify(next.settings_json, null, 2))
      setDraftDescription(next.description ?? '')
      setParseError(null)
    },
  })

  const updateDraftMutation = useMutation({
    mutationFn: async (payload: { id: string; settings_json: Record<string, any>; description?: string }) => {
      return apiSendJson<JourneySettingsVersion>(`/api/settings/journeys/versions/${payload.id}`, 'PATCH', {
        actor: 'ui',
        settings_json: payload.settings_json,
        description: payload.description,
      }, {
        fallbackMessage: 'Failed to save draft',
      })
    },
    onSuccess: (next) => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
      setDraftObj(next.settings_json)
      setDraftJson(JSON.stringify(next.settings_json, null, 2))
      setDraftDescription(next.description ?? '')
    },
  })

  const archiveMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiSendJson<JourneySettingsVersion>(withQuery(`/api/settings/journeys/versions/${id}/archive`, { actor: 'ui' }), 'POST', undefined, {
        fallbackMessage: 'Failed to archive draft',
      })
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
    },
  })

  const validateMutation = useMutation({
    mutationFn: async (settings: Record<string, any>) => {
      return apiSendJson<any>('/api/settings/journeys/validate', 'POST', {
        settings_json: settings,
      }, {
        fallbackMessage: 'Failed to validate draft',
      })
    },
    onSuccess: (data) => setValidationResult(data),
  })

  const previewMutation = useMutation({
    mutationFn: async (settings: Record<string, any>) => {
      return apiSendJson<any>('/api/settings/journeys/preview', 'POST', {
        settings_json: settings,
      }, {
        fallbackMessage: 'Failed to preview draft impact',
      })
    },
    onSuccess: (data) => setPreviewResult(data),
  })

  const activateMutation = useMutation({
    mutationFn: async (payload: { version_id: string; activation_note?: string }) => {
      return apiSendJson<any>('/api/settings/journeys/activate', 'POST', {
        ...payload,
        actor: 'ui',
        confirm: true,
      }, {
        fallbackMessage: 'Failed to activate settings',
      })
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['journeys-settings-versions'] })
    },
  })

  const versions = versionsQuery.data ?? []
  const activeVersion = useMemo(() => versions.find((v) => v.status === 'active') ?? null, [versions])
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    return versions.filter((v) => {
      if (statusFilter !== 'all' && v.status !== statusFilter) return false
      if (!q) return true
      return (
        v.version_label.toLowerCase().includes(q) ||
        (v.description || '').toLowerCase().includes(q)
      )
    })
  }, [search, statusFilter, versions])

  const selected = useMemo(
    () => versions.find((v) => v.id === selectedId) ?? filtered[0] ?? null,
    [filtered, selectedId, versions],
  )

  const isDraft = selected?.status === 'draft'
  const hasDraft = !!draftObj && isDraft

  const loadSelected = (item: JourneySettingsVersion) => {
    setSelectedId(item.id)
    setDraftObj(item.settings_json)
    setDraftJson(JSON.stringify(item.settings_json, null, 2))
    setDraftDescription(item.description ?? '')
    setValidationResult(item.validation_json ?? null)
    setPreviewResult(null)
    setParseError(null)
  }

  const updateDraft = (next: Record<string, any>) => {
    setDraftObj(next)
    setDraftJson(JSON.stringify(next, null, 2))
  }

  const cardStyle: CSSProperties = {
    border: `1px solid ${t.color.borderLight}`,
    borderRadius: t.radius.md,
    background: t.color.surface,
    padding: t.space.md,
    display: 'grid',
    gap: t.space.sm,
  }

  return (
    <div style={{ display: 'grid', gap: t.space.xl }}>
      <div style={cardStyle}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>Journeys settings versions</h3>
            <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Draft/Active/Archived lifecycle for paths, flow, funnels, and diagnostics defaults.
            </p>
          </div>
          <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={() => createDraftMutation.mutate({ settings_json: activeVersion?.settings_json ?? defaultJourneysTemplate(), description: 'New journeys draft' })}
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.accent}`,
                background: t.color.accentMuted,
                color: t.color.accent,
                cursor: 'pointer',
                fontSize: t.font.sizeXs,
              }}
            >
              {createDraftMutation.isPending ? 'Creating…' : 'New draft'}
            </button>
            <button
              type="button"
              disabled={!activeVersion}
              onClick={() =>
                createDraftMutation.mutate({
                  settings_json: activeVersion?.settings_json ?? defaultJourneysTemplate(),
                  description: activeVersion ? `Cloned from ${activeVersion.version_label}` : 'Cloned draft',
                })
              }
              style={{
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: 'transparent',
                color: t.color.text,
                cursor: activeVersion ? 'pointer' : 'not-allowed',
                opacity: activeVersion ? 1 : 0.6,
                fontSize: t.font.sizeXs,
              }}
            >
              Duplicate active
            </button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(260px, 340px) 1fr', gap: t.space.md }}>
          <div style={{ display: 'grid', gap: t.space.xs, alignContent: 'start' }}>
            <div style={{ display: 'flex', gap: t.space.xs }}>
              <input
                type="search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search versions"
                style={{ flex: 1, padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}` }}
              />
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value as any)}
                style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}` }}
              >
                <option value="all">All</option>
                <option value="draft">Draft</option>
                <option value="active">Active</option>
                <option value="archived">Archived</option>
              </select>
            </div>

            {filtered.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => loadSelected(item)}
                style={{
                  textAlign: 'left',
                  borderRadius: t.radius.sm,
                  border: `1px solid ${selected?.id === item.id ? t.color.accent : t.color.borderLight}`,
                  background: selected?.id === item.id ? t.color.accentMuted : t.color.surface,
                  padding: t.space.sm,
                  cursor: 'pointer',
                  display: 'grid',
                  gap: 4,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: t.space.sm }}>
                  <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>
                    {item.version_label}
                  </span>
                  <span
                    style={{
                      fontSize: t.font.sizeXs,
                      padding: '2px 8px',
                      borderRadius: 999,
                      border: `1px solid ${
                        item.status === 'active' ? t.color.success : item.status === 'draft' ? t.color.warning : t.color.border
                      }`,
                      color: item.status === 'active' ? t.color.success : item.status === 'draft' ? t.color.warning : t.color.textSecondary,
                    }}
                  >
                    {item.status}
                  </span>
                </div>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  {item.description || 'No description'}
                </span>
                <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  Created {formatDateTime(item.created_at)}
                </span>
              </button>
            ))}

            {filtered.length === 0 && (
              <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, padding: t.space.sm }}>
                No versions found.
              </div>
            )}
          </div>

          <div style={{ display: 'grid', gap: t.space.sm }}>
            {!selected ? (
              <div style={{ ...cardStyle, color: t.color.textSecondary, fontSize: t.font.sizeSm }}>
                Select a version to edit or review.
              </div>
            ) : (
              <>
                <div style={cardStyle}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap' }}>
                    <div>
                      <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        {selected.version_label} • {selected.status}
                      </div>
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                        Created by {selected.created_by || 'unknown'} · {formatDateTime(selected.created_at)}
                      </div>
                      {selected.activated_at && (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                          Activated {formatDateTime(selected.activated_at)}
                        </div>
                      )}
                    </div>
                    <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                      <button
                        type="button"
                        onClick={() => setEditorMode('basic')}
                        style={{
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${editorMode === 'basic' ? t.color.accent : t.color.borderLight}`,
                          background: editorMode === 'basic' ? t.color.accentMuted : 'transparent',
                          color: editorMode === 'basic' ? t.color.accent : t.color.text,
                        }}
                      >
                        Basic
                      </button>
                      <button
                        type="button"
                        onClick={() => setEditorMode('advanced')}
                        style={{
                          padding: `${t.space.xs}px ${t.space.sm}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${editorMode === 'advanced' ? t.color.accent : t.color.borderLight}`,
                          background: editorMode === 'advanced' ? t.color.accentMuted : 'transparent',
                          color: editorMode === 'advanced' ? t.color.accent : t.color.text,
                        }}
                      >
                        Advanced JSON
                      </button>
                    </div>
                  </div>

                  <label style={{ display: 'grid', gap: 4 }}>
                    <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Description</span>
                    <input
                      value={draftDescription}
                      onChange={(e) => setDraftDescription(e.target.value)}
                      disabled={!isDraft}
                      style={{
                        padding: `${t.space.xs}px ${t.space.sm}px`,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.borderLight}`,
                        fontSize: t.font.sizeSm,
                        background: isDraft ? t.color.surface : t.color.bgSubtle,
                      }}
                    />
                  </label>

                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                    <button
                      type="button"
                      disabled={!hasDraft || updateDraftMutation.isPending}
                      onClick={() => {
                        if (!draftObj || !selected) return
                        updateDraftMutation.mutate({
                          id: selected.id,
                          settings_json: draftObj,
                          description: draftDescription.trim() || undefined,
                        })
                      }}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: 'none', background: t.color.accent, color: t.color.surface, fontSize: t.font.sizeXs, cursor: hasDraft ? 'pointer' : 'not-allowed', opacity: hasDraft ? 1 : 0.6 }}
                    >
                      {updateDraftMutation.isPending ? 'Saving…' : 'Save draft'}
                    </button>
                    <button
                      type="button"
                      disabled={!hasDraft || validateMutation.isPending}
                      onClick={() => draftObj && validateMutation.mutate(draftObj)}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: 'transparent', fontSize: t.font.sizeXs }}
                    >
                      {validateMutation.isPending ? 'Validating…' : 'Validate draft'}
                    </button>
                    <button
                      type="button"
                      disabled={!hasDraft || previewMutation.isPending}
                      onClick={() => draftObj && previewMutation.mutate(draftObj)}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: 'transparent', fontSize: t.font.sizeXs }}
                    >
                      {previewMutation.isPending ? 'Calculating…' : 'Impact preview'}
                    </button>
                    <button
                      type="button"
                      disabled={!selected || !canActivate || activateMutation.isPending}
                      onClick={() => {
                        if (!selected) return
                        const ok = window.confirm(`Activate ${selected.version_label}? This archives the previously active version.`)
                        if (!ok) return
                        activateMutation.mutate({ version_id: selected.id })
                      }}
                      style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: 'none', background: t.color.success, color: t.color.surface, fontSize: t.font.sizeXs, opacity: canActivate ? 1 : 0.6 }}
                    >
                      {activateMutation.isPending ? 'Activating…' : 'Activate'}
                    </button>
                    {isDraft && (
                      <button
                        type="button"
                        onClick={() => selected && archiveMutation.mutate(selected.id)}
                        style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: 'none', background: t.color.dangerSubtle, color: t.color.danger, fontSize: t.font.sizeXs }}
                      >
                        Archive draft
                      </button>
                    )}
                  </div>

                  {!canActivate && (
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                      Activation requires `admin` or `power_user` role.
                    </div>
                  )}
                </div>

                {editorMode === 'advanced' ? (
                  <div style={cardStyle}>
                    <textarea
                      value={draftJson}
                      onChange={(e) => {
                        const nextText = e.target.value
                        setDraftJson(nextText)
                        try {
                          const parsed = JSON.parse(nextText)
                          setDraftObj(parsed)
                          setParseError(null)
                        } catch (error) {
                          setParseError((error as Error).message)
                        }
                      }}
                      disabled={!isDraft}
                      rows={22}
                      style={{
                        width: '100%',
                        minHeight: 360,
                        fontFamily: 'monospace',
                        fontSize: t.font.sizeXs,
                        borderRadius: t.radius.sm,
                        border: `1px solid ${parseError ? t.color.danger : t.color.borderLight}`,
                        background: isDraft ? t.color.bgSubtle : t.color.surface,
                        padding: t.space.sm,
                      }}
                    />
                    {parseError && (
                      <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                        JSON parse error: {parseError}
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ display: 'grid', gap: t.space.sm }}>
                    <details open style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        Sessionization & Journey Construction
                      </summary>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: t.space.sm }}>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Session timeout (minutes)</span>
                          <input type="number" value={draftObj?.sessionization?.session_timeout_minutes ?? 30} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'session_timeout_minutes'], asInt(e.target.value, 30)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Lookback window (days)</span>
                          <input type="number" value={draftObj?.sessionization?.lookback_window_days ?? 30} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'lookback_window_days'], asInt(e.target.value, 30)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Max steps per journey</span>
                          <input type="number" value={draftObj?.sessionization?.max_steps_per_journey ?? 20} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'max_steps_per_journey'], asInt(e.target.value, 20)))} disabled={!isDraft} />
                        </label>
                      </div>
                      <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: t.font.sizeXs }}>
                        <input type="checkbox" checked={!!draftObj?.sessionization?.conversion_journeys_only} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'conversion_journeys_only'], e.target.checked))} disabled={!isDraft} />
                        Conversion journeys only (default ON)
                      </label>
                      <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: t.font.sizeXs }}>
                        <input type="checkbox" checked={!!draftObj?.sessionization?.allow_all_journeys} onChange={(e) => updateDraft(setIn(draftObj, ['sessionization', 'allow_all_journeys'], e.target.checked))} disabled={!isDraft} />
                        Allow all journeys
                      </label>
                    </details>

                    <details style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        Step Canonicalization
                      </summary>
                      <label style={{ display: 'grid', gap: 4 }}>
                        <span style={{ fontSize: t.font.sizeXs }}>Fallback step</span>
                        <input value={draftObj?.step_canonicalization?.fallback_step ?? 'Other'} onChange={(e) => updateDraft(setIn(draftObj, ['step_canonicalization', 'fallback_step'], e.target.value))} disabled={!isDraft} />
                      </label>
                      <label style={{ display: 'grid', gap: 4 }}>
                        <span style={{ fontSize: t.font.sizeXs }}>Step mapping rules (advanced JSON list)</span>
                        <textarea
                          rows={8}
                          value={JSON.stringify(draftObj?.step_canonicalization?.rules ?? [], null, 2)}
                          onChange={(e) => {
                            try {
                              const parsed = JSON.parse(e.target.value)
                              updateDraft(setIn(draftObj, ['step_canonicalization', 'rules'], parsed))
                              setParseError(null)
                            } catch (error) {
                              setParseError((error as Error).message)
                            }
                          }}
                          disabled={!isDraft}
                          style={{ fontFamily: 'monospace', fontSize: t.font.sizeXs }}
                        />
                      </label>
                    </details>

                    <details style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        Paths / Flow defaults
                      </summary>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: t.space.sm }}>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Top paths limit</span>
                          <input type="number" value={draftObj?.paths_explorer_defaults?.top_paths_limit ?? 50} onChange={(e) => updateDraft(setIn(draftObj, ['paths_explorer_defaults', 'top_paths_limit'], asInt(e.target.value, 50)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Flow max depth</span>
                          <input type="number" value={draftObj?.flow_defaults?.max_depth ?? 4} onChange={(e) => updateDraft(setIn(draftObj, ['flow_defaults', 'max_depth'], asInt(e.target.value, 4)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Min volume threshold</span>
                          <input type="number" value={draftObj?.flow_defaults?.min_volume_threshold ?? 20} onChange={(e) => updateDraft(setIn(draftObj, ['flow_defaults', 'min_volume_threshold'], asInt(e.target.value, 20)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Max nodes</span>
                          <input type="number" value={draftObj?.flow_defaults?.max_nodes ?? 30} onChange={(e) => updateDraft(setIn(draftObj, ['flow_defaults', 'max_nodes'], asInt(e.target.value, 30)))} disabled={!isDraft} />
                        </label>
                      </div>
                    </details>

                    <details style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        Funnels & Diagnostics
                      </summary>
                      {!featureFlags.funnel_builder_enabled ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Funnel builder feature is disabled for this workspace.
                        </div>
                      ) : (
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
                          <label style={{ display: 'grid', gap: 4 }}>
                            <span style={{ fontSize: t.font.sizeXs }}>Funnel counting method</span>
                            <select value={draftObj?.funnels_defaults?.default_counting_method ?? 'uniques'} onChange={(e) => updateDraft(setIn(draftObj, ['funnels_defaults', 'default_counting_method'], e.target.value))} disabled={!isDraft}>
                              <option value="uniques">Uniques</option>
                              <option value="totals">Totals</option>
                            </select>
                          </label>
                          <label style={{ display: 'grid', gap: 4 }}>
                            <span style={{ fontSize: t.font.sizeXs }}>Conversion window (seconds)</span>
                            <input type="number" value={draftObj?.funnels_defaults?.default_conversion_window_seconds ?? 604800} onChange={(e) => updateDraft(setIn(draftObj, ['funnels_defaults', 'default_conversion_window_seconds'], asInt(e.target.value, 604800)))} disabled={!isDraft} />
                          </label>
                        </div>
                      )}

                      {!featureFlags.funnel_diagnostics_enabled ? (
                        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                          Diagnostics feature is disabled for this workspace.
                        </div>
                      ) : (
                        <>
                          <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: t.font.sizeXs }}>
                            <input type="checkbox" checked={!!draftObj?.diagnostics_defaults?.enabled} onChange={(e) => updateDraft(setIn(draftObj, ['diagnostics_defaults', 'enabled'], e.target.checked))} disabled={!isDraft} />
                            Enable diagnostics
                          </label>
                          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                            Output policy is fixed to hypotheses with evidence-required claims.
                          </div>
                        </>
                      )}
                    </details>

                    <details style={cardStyle}>
                      <summary style={{ cursor: 'pointer', fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                        Performance & retention guardrails
                      </summary>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Aggregation reprocess window (days)</span>
                          <input type="number" value={draftObj?.performance_guardrails?.aggregation_reprocess_window_days ?? 3} onChange={(e) => updateDraft(setIn(draftObj, ['performance_guardrails', 'aggregation_reprocess_window_days'], asInt(e.target.value, 3)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Example sampling rate (0-1)</span>
                          <input type="number" step="0.01" min={0} max={1} value={draftObj?.performance_guardrails?.sampling_rate_example_journeys ?? 0} onChange={(e) => updateDraft(setIn(draftObj, ['performance_guardrails', 'sampling_rate_example_journeys'], asFloat(e.target.value, 0)))} disabled={!isDraft} />
                        </label>
                        <label style={{ display: 'grid', gap: 4 }}>
                          <span style={{ fontSize: t.font.sizeXs }}>Max flow query lookback (days)</span>
                          <input type="number" value={draftObj?.performance_guardrails?.max_flow_query_lookback_window_days ?? 90} onChange={(e) => updateDraft(setIn(draftObj, ['performance_guardrails', 'max_flow_query_lookback_window_days'], asInt(e.target.value, 90)))} disabled={!isDraft} />
                        </label>
                      </div>
                    </details>
                  </div>
                )}

                {validationResult && (
                  <div style={{ ...cardStyle, borderColor: validationResult.valid ? t.color.success : t.color.warning }}>
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: validationResult.valid ? t.color.success : t.color.warning }}>
                      {validationResult.valid ? 'Validation passed' : 'Validation issues found'}
                    </div>
                    {!!validationResult.errors?.length && (
                      <ul style={{ margin: 0, paddingLeft: 16, fontSize: t.font.sizeXs }}>
                        {validationResult.errors.map((err: any, idx: number) => (
                          <li key={`${err.path}-${idx}`}>{err.path}: {err.message}</li>
                        ))}
                      </ul>
                    )}
                    {!!validationResult.warnings?.length && (
                      <ul style={{ margin: 0, paddingLeft: 16, fontSize: t.font.sizeXs, color: t.color.warning }}>
                        {validationResult.warnings.map((warn: any, idx: number) => (
                          <li key={`${warn.path}-${idx}`}>{warn.path}: {warn.message}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}

                {previewResult?.preview_available && (
                  <div style={cardStyle}>
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>
                      Impact preview (last 7 days aggregates)
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Changed sections: {(previewResult.changed_keys || []).join(', ') || 'none'}
                    </div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                      Estimated paths returned: {previewResult.estimated_paths_returned ?? 0}
                    </div>
                    {!!previewResult.warnings?.length && (
                      <ul style={{ margin: 0, paddingLeft: 16, fontSize: t.font.sizeXs, color: t.color.warning }}>
                        {previewResult.warnings.map((warn: string) => (
                          <li key={warn}>{warn}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
