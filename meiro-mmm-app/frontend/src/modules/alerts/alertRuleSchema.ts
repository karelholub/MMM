/**
 * Alert rule form schema and validation.
 * Rule-type-specific params with safe defaults (e.g. anomaly thresholds prefilled).
 */

export const RULE_TYPES = [
  { value: 'anomaly_kpi', label: 'KPI anomaly' },
  { value: 'threshold', label: 'Threshold' },
  { value: 'data_freshness', label: 'Data freshness' },
  { value: 'pipeline_health', label: 'Pipeline health' },
] as const

export const SEVERITIES = [
  { value: 'info', label: 'Info' },
  { value: 'warn', label: 'Warning' },
  { value: 'critical', label: 'Critical' },
] as const

export const SCHEDULES = [
  { value: 'hourly', label: 'Hourly' },
  { value: 'daily', label: 'Daily' },
] as const

export type RuleType = (typeof RULE_TYPES)[number]['value']
export type Severity = (typeof SEVERITIES)[number]['value']
export type Schedule = (typeof SCHEDULES)[number]['value']

/** Safe default params per rule_type (backend PARAMS_SAFE_KEYS). */
export const DEFAULT_PARAMS: Record<RuleType, Record<string, number | string | boolean>> = {
  anomaly_kpi: {
    zscore_threshold: 2.5,
    lookback_days: 14,
    lookback_periods: 28,
    min_volume: 100,
  },
  threshold: {
    threshold_value: 0,
    threshold_direction: 'above',
    operator: 'gte',
    lookback_days: 7,
    min_volume: 50,
  },
  data_freshness: {
    max_lag_hours: 24,
    max_age_hours: 48,
    lookback_days: 1,
  },
  pipeline_health: {
    max_failure_count: 3,
    window_hours: 24,
    tolerance_hours: 2,
  },
}

export interface AlertRuleFormValues {
  name: string
  description: string
  is_enabled: boolean
  scope: string
  severity: Severity
  rule_type: RuleType
  kpi_key: string
  dimension_filters_json: Record<string, unknown> | null
  params_json: Record<string, unknown>
  schedule: Schedule
}

const DEFAULT_SCOPE = 'workspace'

export function getDefaultFormValues(ruleType: RuleType = 'anomaly_kpi'): AlertRuleFormValues {
  return {
    name: '',
    description: '',
    is_enabled: true,
    scope: DEFAULT_SCOPE,
    severity: 'warn',
    rule_type: ruleType,
    kpi_key: '',
    dimension_filters_json: null,
    params_json: { ...DEFAULT_PARAMS[ruleType] },
    schedule: 'daily',
  }
}

export interface ValidationError {
  field: string
  message: string
}

export function validateAlertRuleForm(values: Partial<AlertRuleFormValues>): ValidationError[] {
  const errors: ValidationError[] = []
  if (!values.name?.trim()) {
    errors.push({ field: 'name', message: 'Name is required' })
  }
  if (values.name && values.name.length > 255) {
    errors.push({ field: 'name', message: 'Name must be 255 characters or less' })
  }
  if (!values.scope?.trim()) {
    errors.push({ field: 'scope', message: 'Scope is required' })
  }
  if (!values.severity || !SEVERITIES.some((s) => s.value === values.severity)) {
    errors.push({ field: 'severity', message: 'Valid severity is required' })
  }
  if (!values.rule_type || !RULE_TYPES.some((r) => r.value === values.rule_type)) {
    errors.push({ field: 'rule_type', message: 'Valid rule type is required' })
  }
  if (!values.schedule || !SCHEDULES.some((s) => s.value === values.schedule)) {
    errors.push({ field: 'schedule', message: 'Valid schedule is required' })
  }
  const ruleType = values.rule_type as RuleType | undefined
  if (ruleType === 'anomaly_kpi' || ruleType === 'threshold') {
    if (!values.kpi_key?.trim()) {
      errors.push({ field: 'kpi_key', message: 'KPI key is required for this rule type' })
    }
  }
  if (values.params_json && typeof values.params_json !== 'object') {
    errors.push({ field: 'params_json', message: 'Params must be an object' })
  }
  return errors
}

/** Params field metadata for rule-type-specific form fields. */
export const PARAMS_FIELDS: Record<
  RuleType,
  { key: string; label: string; type: 'number' | 'string'; default: number | string }[]
> = {
  anomaly_kpi: [
    { key: 'zscore_threshold', label: 'Z-score threshold', type: 'number', default: 2.5 },
    { key: 'lookback_days', label: 'Lookback (days)', type: 'number', default: 14 },
    { key: 'min_volume', label: 'Min volume', type: 'number', default: 100 },
  ],
  threshold: [
    { key: 'threshold_value', label: 'Threshold value', type: 'number', default: 0 },
    { key: 'lookback_days', label: 'Lookback (days)', type: 'number', default: 7 },
    { key: 'min_volume', label: 'Min volume', type: 'number', default: 50 },
  ],
  data_freshness: [
    { key: 'max_lag_hours', label: 'Max lag (hours)', type: 'number', default: 24 },
    { key: 'max_age_hours', label: 'Max age (hours)', type: 'number', default: 48 },
  ],
  pipeline_health: [
    { key: 'max_failure_count', label: 'Max failures', type: 'number', default: 3 },
    { key: 'window_hours', label: 'Window (hours)', type: 'number', default: 24 },
  ],
}
