import { useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../../theme/tokens'
import SectionCard from '../../components/dashboard/SectionCard'
import DashboardTable from '../../components/dashboard/DashboardTable'
import { apiGetJson, apiSendJson } from '../../lib/apiClient'
import {
  RULE_TYPES,
  SEVERITIES,
  SCHEDULES,
  DEFAULT_PARAMS,
  PARAMS_FIELDS,
  getDefaultFormValues,
  validateAlertRuleForm,
  type RuleType,
  type AlertRuleFormValues,
} from './alertRuleSchema'

interface AlertRuleRow {
  id: number
  name: string
  description: string | null
  is_enabled: boolean
  severity: string
  rule_type: string
  kpi_key: string | null
  params_json: Record<string, unknown> | null
  schedule: string
  created_at: string | null
  updated_at: string | null
}

const SCOPE_DEFAULT = 'workspace'

export default function AlertRulesTab() {
  const queryClient = useQueryClient()
  const [editingId, setEditingId] = useState<number | null>(null)
  const [creating, setCreating] = useState(false)
  const [formValues, setFormValues] = useState<AlertRuleFormValues>(() =>
    getDefaultFormValues('anomaly_kpi'),
  )
  const [formErrors, setFormErrors] = useState<{ field: string; message: string }[]>([])

  const rulesQuery = useQuery<AlertRuleRow[]>({
    queryKey: ['alert-rules'],
    queryFn: async () => apiGetJson<AlertRuleRow[]>('/api/alert-rules', { fallbackMessage: 'Failed to load rules' }),
  })

  const toggleMutation = useMutation({
    mutationFn: async ({ ruleId, is_enabled }: { ruleId: number; is_enabled: boolean }) => {
      return apiSendJson<any>(`/api/alert-rules/${ruleId}`, 'PUT', { is_enabled }, {
        fallbackMessage: 'Failed to update rule',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules'] })
    },
  })

  const createMutation = useMutation({
    mutationFn: async (body: AlertRuleFormValues) => {
      return apiSendJson<any>('/api/alert-rules', 'POST', {
        name: body.name.trim(),
        description: body.description?.trim() || null,
        is_enabled: body.is_enabled,
        scope: body.scope.trim() || SCOPE_DEFAULT,
        severity: body.severity,
        rule_type: body.rule_type,
        kpi_key: body.kpi_key?.trim() || null,
        dimension_filters_json: body.dimension_filters_json,
        params_json: body.params_json,
        schedule: body.schedule,
      }, {
        fallbackMessage: 'Failed to create rule',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules'] })
      setCreating(false)
      setFormValues(getDefaultFormValues('anomaly_kpi'))
      setFormErrors([])
    },
    onError: (err: Error) => {
      setFormErrors([{ field: '_', message: err.message }])
    },
  })

  const updateMutation = useMutation({
    mutationFn: async ({
      ruleId,
      body,
    }: {
      ruleId: number
      body: Partial<AlertRuleFormValues>
    }) => {
      return apiSendJson<any>(`/api/alert-rules/${ruleId}`, 'PUT', {
        name: body.name?.trim(),
        description: body.description?.trim() ?? undefined,
        is_enabled: body.is_enabled,
        severity: body.severity,
        kpi_key: body.kpi_key?.trim() ?? undefined,
        dimension_filters_json: body.dimension_filters_json,
        params_json: body.params_json,
        schedule: body.schedule,
      }, {
        fallbackMessage: 'Failed to update rule',
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules'] })
      setEditingId(null)
      setFormErrors([])
    },
    onError: (err: Error) => {
      setFormErrors([{ field: '_', message: err.message }])
    },
  })

  const startCreate = useCallback(() => {
    setCreating(true)
    setEditingId(null)
    setFormValues(getDefaultFormValues('anomaly_kpi'))
    setFormErrors([])
  }, [])

  const startEdit = useCallback((rule: AlertRuleRow) => {
    setEditingId(rule.id)
    setCreating(false)
    const rt = (rule.rule_type || 'anomaly_kpi') as RuleType
    const defaults = DEFAULT_PARAMS[rt] || {}
    setFormValues({
      name: rule.name,
      description: rule.description ?? '',
      is_enabled: rule.is_enabled,
      scope: SCOPE_DEFAULT,
      severity: (rule.severity === 'warning' ? 'warn' : rule.severity) as AlertRuleFormValues['severity'],
      rule_type: rt,
      kpi_key: rule.kpi_key ?? '',
      dimension_filters_json: null,
      params_json: { ...defaults, ...(rule.params_json || {}) },
      schedule: (rule.schedule || 'daily') as AlertRuleFormValues['schedule'],
    })
    setFormErrors([])
  }, [])

  const cancelForm = useCallback(() => {
    setCreating(false)
    setEditingId(null)
    setFormErrors([])
  }, [])

  const submitCreate = useCallback(() => {
    const errs = validateAlertRuleForm(formValues)
    if (errs.length) {
      setFormErrors(errs)
      return
    }
    createMutation.mutate(formValues)
  }, [formValues, createMutation])

  const submitUpdate = useCallback(() => {
    if (editingId == null) return
    const errs = validateAlertRuleForm(formValues)
    if (errs.length) {
      setFormErrors(errs)
      return
    }
    updateMutation.mutate({ ruleId: editingId, body: formValues })
  }, [editingId, formValues, updateMutation])

  const rules = rulesQuery.data ?? []
  const ruleType = formValues.rule_type
  const paramsFields = PARAMS_FIELDS[ruleType] ?? []

  return (
    <div style={{ display: 'grid', gap: t.space.xl }}>
      <SectionCard
        title="Alert rules"
        subtitle="Define when to trigger alerts. Toggle rules on or off without deleting."
        actions={
          <button
            type="button"
            onClick={startCreate}
            style={{
              padding: `${t.space.sm}px ${t.space.md}px`,
              borderRadius: t.radius.sm,
              border: 'none',
              background: t.color.accent,
              color: t.color.surface,
              fontSize: t.font.sizeSm,
              fontWeight: t.font.weightMedium,
              cursor: 'pointer',
            }}
          >
            Add rule
          </button>
        }
      >
        {rulesQuery.isLoading && (
          <div style={{ padding: t.space.xl, textAlign: 'center', color: t.color.textSecondary }}>
            Loading rules…
          </div>
        )}
        {rulesQuery.isError && (
          <div style={{ padding: t.space.lg, color: t.color.danger, fontSize: t.font.sizeSm }}>
            {(rulesQuery.error as Error).message}
          </div>
        )}
        {!rulesQuery.isLoading && !rulesQuery.isError && (
          <DashboardTable>
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Severity</th>
                <th>Schedule</th>
                <th>Enabled</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rules.map((rule) => (
                <tr key={rule.id}>
                  <td style={{ fontWeight: t.font.weightMedium }}>{rule.name}</td>
                  <td>{rule.rule_type}</td>
                  <td>{rule.severity === 'warn' ? 'Warning' : rule.severity}</td>
                  <td>{rule.schedule}</td>
                  <td>
                    <button
                      type="button"
                      onClick={() =>
                        toggleMutation.mutate({ ruleId: rule.id, is_enabled: !rule.is_enabled })
                      }
                      disabled={toggleMutation.isPending}
                      style={{
                        padding: '2px 8px',
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.color.border}`,
                        background: rule.is_enabled ? t.color.successMuted : t.color.bg,
                        color: rule.is_enabled ? t.color.success : t.color.textSecondary,
                        fontSize: t.font.sizeXs,
                        cursor: toggleMutation.isPending ? 'wait' : 'pointer',
                      }}
                    >
                      {rule.is_enabled ? 'On' : 'Off'}
                    </button>
                  </td>
                  <td>
                    <button
                      type="button"
                      onClick={() => startEdit(rule)}
                      style={{
                        padding: '2px 8px',
                        border: 'none',
                        background: 'transparent',
                        color: t.color.accent,
                        fontSize: t.font.sizeSm,
                        cursor: 'pointer',
                      }}
                    >
                      Edit
                    </button>
                  </td>
                </tr>
              ))}
              {rules.length === 0 && (
                <tr>
                  <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary, padding: t.space.lg }}>
                    No alert rules yet. Add a rule to get started.
                  </td>
                </tr>
              )}
            </tbody>
          </DashboardTable>
        )}
      </SectionCard>

      {(creating || editingId != null) && (
        <SectionCard
          title={creating ? 'New alert rule' : 'Edit alert rule'}
          subtitle="Rule-type-specific params use safe defaults (e.g. anomaly z-score threshold)."
        >
          {formErrors.some((e) => e.field === '_') && (
            <div
              style={{
                marginBottom: t.space.md,
                padding: t.space.sm,
                background: t.color.dangerMuted,
                color: t.color.danger,
                borderRadius: t.radius.sm,
                fontSize: t.font.sizeSm,
              }}
            >
              {formErrors.find((e) => e.field === '_')?.message}
            </div>
          )}
          <div style={{ display: 'grid', gap: t.space.md, maxWidth: 520 }}>
            <label style={{ display: 'grid', gap: 4 }}>
              <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                Name
              </span>
              <input
                type="text"
                value={formValues.name}
                onChange={(e) => setFormValues((v) => ({ ...v, name: e.target.value }))}
                placeholder="e.g. Conversion rate drop"
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  fontSize: t.font.sizeSm,
                }}
              />
              {formErrors.find((e) => e.field === 'name') && (
                <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                  {formErrors.find((e) => e.field === 'name')?.message}
                </span>
              )}
            </label>
            <label style={{ display: 'grid', gap: 4 }}>
              <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                Description
              </span>
              <textarea
                value={formValues.description}
                onChange={(e) => setFormValues((v) => ({ ...v, description: e.target.value }))}
                placeholder="Optional"
                rows={2}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.color.border}`,
                  fontSize: t.font.sizeSm,
                  resize: 'vertical',
                }}
              />
            </label>
            <div style={{ display: 'flex', gap: t.space.lg, flexWrap: 'wrap' }}>
              <label style={{ display: 'grid', gap: 4 }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  Rule type
                </span>
                <select
                  value={formValues.rule_type}
                  onChange={(e) => {
                    const rt = e.target.value as RuleType
                    setFormValues((v) => ({
                      ...v,
                      rule_type: rt,
                      params_json: { ...DEFAULT_PARAMS[rt] },
                    }))
                  }}
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    fontSize: t.font.sizeSm,
                    minWidth: 140,
                  }}
                >
                  {RULE_TYPES.map((r) => (
                    <option key={r.value} value={r.value}>
                      {r.label}
                    </option>
                  ))}
                </select>
              </label>
              <label style={{ display: 'grid', gap: 4 }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  Severity
                </span>
                <select
                  value={formValues.severity}
                  onChange={(e) =>
                    setFormValues((v) => ({ ...v, severity: e.target.value as AlertRuleFormValues['severity'] }))
                  }
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    fontSize: t.font.sizeSm,
                    minWidth: 100,
                  }}
                >
                  {SEVERITIES.map((s) => (
                    <option key={s.value} value={s.value}>
                      {s.label}
                    </option>
                  ))}
                </select>
              </label>
              <label style={{ display: 'grid', gap: 4 }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  Schedule
                </span>
                <select
                  value={formValues.schedule}
                  onChange={(e) =>
                    setFormValues((v) => ({ ...v, schedule: e.target.value as AlertRuleFormValues['schedule'] }))
                  }
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    fontSize: t.font.sizeSm,
                    minWidth: 100,
                  }}
                >
                  {SCHEDULES.map((s) => (
                    <option key={s.value} value={s.value}>
                      {s.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            {(ruleType === 'anomaly_kpi' || ruleType === 'threshold') && (
              <label style={{ display: 'grid', gap: 4 }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  KPI key
                </span>
                <input
                  type="text"
                  value={formValues.kpi_key}
                  onChange={(e) => setFormValues((v) => ({ ...v, kpi_key: e.target.value }))}
                  placeholder="e.g. conversions, revenue"
                  style={{
                    padding: `${t.space.sm}px ${t.space.md}px`,
                    borderRadius: t.radius.sm,
                    border: `1px solid ${t.color.border}`,
                    fontSize: t.font.sizeSm,
                  }}
                />
                {formErrors.find((e) => e.field === 'kpi_key') && (
                  <span style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>
                    {formErrors.find((e) => e.field === 'kpi_key')?.message}
                  </span>
                )}
              </label>
            )}
            {paramsFields.length > 0 && (
              <div style={{ display: 'grid', gap: t.space.sm }}>
                <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>
                  Params
                </span>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: t.space.md }}>
                  {paramsFields.map((f) => (
                    <label key={f.key} style={{ display: 'grid', gap: 4 }}>
                      <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{f.label}</span>
                      <input
                        type={f.type}
                        value={
                          formValues.params_json[f.key] !== undefined &&
                          formValues.params_json[f.key] !== null
                            ? String(formValues.params_json[f.key])
                            : ''
                        }
                        onChange={(e) => {
                          const val = f.type === 'number' ? Number(e.target.value) : e.target.value
                          setFormValues((v) => ({
                            ...v,
                            params_json: { ...v.params_json, [f.key]: val },
                          }))
                        }}
                        style={{
                          padding: `${t.space.sm}px ${t.space.md}px`,
                          borderRadius: t.radius.sm,
                          border: `1px solid ${t.color.border}`,
                          fontSize: t.font.sizeSm,
                        }}
                      />
                    </label>
                  ))}
                </div>
              </div>
            )}
            <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
              <input
                type="checkbox"
                checked={formValues.is_enabled}
                onChange={(e) => setFormValues((v) => ({ ...v, is_enabled: e.target.checked }))}
              />
              <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>Enabled</span>
            </label>
          </div>
          <div style={{ display: 'flex', gap: t.space.sm, marginTop: t.space.lg }}>
            <button
              type="button"
              onClick={creating ? submitCreate : submitUpdate}
              disabled={createMutation.isPending || updateMutation.isPending}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: 'none',
                background: t.color.accent,
                color: t.color.surface,
                fontSize: t.font.sizeSm,
                fontWeight: t.font.weightMedium,
                cursor: createMutation.isPending || updateMutation.isPending ? 'wait' : 'pointer',
              }}
            >
              {creating ? 'Create rule' : 'Save changes'}
            </button>
            <button
              type="button"
              onClick={cancelForm}
              style={{
                padding: `${t.space.sm}px ${t.space.md}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                background: t.color.surface,
                fontSize: t.font.sizeSm,
                cursor: 'pointer',
                color: t.color.text,
              }}
            >
              Cancel
            </button>
          </div>
        </SectionCard>
      )}
    </div>
  )
}
