import { useEffect, useMemo, useState } from 'react'
import { tokens as t } from '../../theme/tokens'
import type {
  LocalSegmentDefinitionV2,
  SegmentContextDimensionValue,
  SegmentContextResponse,
  SegmentDefinitionRule,
  SegmentRegistryItem,
  SegmentRuleField,
  SegmentRuleOperator,
} from '../../lib/segments'
import { normalizeLocalSegmentDefinition } from '../../lib/segments'

interface SegmentEditorDialogProps {
  open: boolean
  mode: 'create' | 'edit'
  item?: SegmentRegistryItem | null
  context?: SegmentContextResponse | null
  error?: string | null
  submitting?: boolean
  onClose: () => void
  onSubmit: (payload: {
    name: string
    description: string
    definition: LocalSegmentDefinitionV2
  }) => void
}

const FIELD_OPTIONS: Array<{ value: SegmentRuleField; label: string; group: string }> = [
  { value: 'channel_group', label: 'Channel group', group: 'Dimension' },
  { value: 'campaign_id', label: 'Campaign', group: 'Dimension' },
  { value: 'device', label: 'Device', group: 'Dimension' },
  { value: 'country', label: 'Country', group: 'Dimension' },
  { value: 'conversion_key', label: 'Conversion KPI', group: 'Dimension' },
  { value: 'last_touch_channel', label: 'Last-touch channel', group: 'Dimension' },
  { value: 'interaction_path_type', label: 'Path type', group: 'Journey behavior' },
  { value: 'path_length', label: 'Path length', group: 'Journey behavior' },
  { value: 'lag_days', label: 'Lag (days)', group: 'Lag & timing' },
  { value: 'net_revenue_total', label: 'Net revenue', group: 'Value & conversion' },
  { value: 'gross_revenue_total', label: 'Gross revenue', group: 'Value & conversion' },
  { value: 'net_conversions_total', label: 'Net conversions', group: 'Value & conversion' },
  { value: 'gross_conversions_total', label: 'Gross conversions', group: 'Value & conversion' },
]

const DIMENSION_FIELD_OPTIONS: SegmentRuleField[] = [
  'channel_group',
  'campaign_id',
  'device',
  'country',
  'conversion_key',
  'last_touch_channel',
  'interaction_path_type',
]

function getDefaultOperator(field: SegmentRuleField): SegmentRuleOperator {
  return DIMENSION_FIELD_OPTIONS.includes(field) ? 'eq' : 'gte'
}

function getFieldLabel(field: SegmentRuleField): string {
  return FIELD_OPTIONS.find((option) => option.value === field)?.label || field
}

function getFieldOptions(
  context: SegmentContextResponse | null | undefined,
  field: SegmentRuleField,
): SegmentContextDimensionValue[] {
  if (!context) return []
  switch (field) {
    case 'channel_group':
      return context.channels
    case 'campaign_id':
      return context.campaigns
    case 'device':
      return context.devices
    case 'country':
      return context.countries
    case 'conversion_key':
      return context.conversion_keys
    case 'last_touch_channel':
      return context.last_touch_channels
    case 'interaction_path_type':
      return context.path_types
    default:
      return []
  }
}

function buildEmptyRule(field: SegmentRuleField = 'channel_group'): SegmentDefinitionRule {
  return {
    field,
    op: getDefaultOperator(field),
    value: DIMENSION_FIELD_OPTIONS.includes(field) ? '' : 1,
  }
}

function formatSuggestedLabel(rule: SegmentDefinitionRule): string {
  const suffix = rule.field === 'lag_days' ? 'd' : ''
  const operatorLabel = rule.op === 'gte' ? '>=' : rule.op === 'lte' ? '<=' : '='
  return `${getFieldLabel(rule.field)} ${operatorLabel} ${rule.value}${suffix}`
}

function segmentTypeFromRules(rules: SegmentDefinitionRule[]): string {
  const fields = new Set(rules.map((rule) => rule.field))
  if (!fields.size) return 'No rules yet'
  if ([...fields].every((field) => ['channel_group', 'campaign_id', 'device', 'country'].includes(field))) return 'Dimension slice'
  if (fields.has('lag_days')) return 'Lag & timing'
  if (fields.has('path_length') || fields.has('interaction_path_type')) return 'Journey behavior'
  if (
    fields.has('net_revenue_total') ||
    fields.has('gross_revenue_total') ||
    fields.has('net_conversions_total') ||
    fields.has('gross_conversions_total')
  ) {
    return 'Value & conversion'
  }
  return 'Mixed analytical segment'
}

export default function SegmentEditorDialog({
  open,
  mode,
  item,
  context,
  error,
  submitting = false,
  onClose,
  onSubmit,
}: SegmentEditorDialogProps) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [match, setMatch] = useState<'all' | 'any'>('all')
  const [rules, setRules] = useState<SegmentDefinitionRule[]>([])

  useEffect(() => {
    if (!open) return
    const definition = normalizeLocalSegmentDefinition((item?.definition as Record<string, unknown>) || {})
    setName(item?.name || '')
    setDescription(item?.description || '')
    setMatch(definition.match)
    setRules(definition.rules.length ? definition.rules : [buildEmptyRule()])
  }, [item, open])

  const validRules = useMemo(
    () =>
      rules.filter((rule) => {
        if (DIMENSION_FIELD_OPTIONS.includes(rule.field)) return `${rule.value}`.trim().length > 0
        return Number.isFinite(Number(rule.value))
      }),
    [rules],
  )

  const segmentType = useMemo(() => segmentTypeFromRules(validRules), [validRules])

  if (!open) return null

  const canSubmit = !!name.trim() && validRules.length > 0

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(15, 23, 42, 0.38)',
        display: 'grid',
        placeItems: 'center',
        zIndex: 72,
        padding: t.space.lg,
      }}
    >
      <div
        role="dialog"
        aria-label={mode === 'create' ? 'Create analytical segment' : 'Edit analytical segment'}
        style={{
          width: 'min(920px, 100%)',
          maxHeight: 'min(90vh, 960px)',
          overflow: 'auto',
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          boxShadow: t.shadow,
          padding: t.space.lg,
          display: 'grid',
          gap: t.space.md,
        }}
      >
        <div style={{ display: 'grid', gap: 4 }}>
          <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>
            {mode === 'create' ? 'Create smart analytical segment' : 'Edit smart analytical segment'}
          </h3>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Build a reusable analytical audience from journey dimensions, lag, value, and path behavior. Meiro Pipes segments remain operational audiences and are not edited here.
          </div>
        </div>

        <div
          style={{
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.sm,
            background: t.color.bg,
            padding: t.space.sm,
            fontSize: t.font.sizeSm,
            color: t.color.textSecondary,
            display: 'flex',
            gap: t.space.md,
            flexWrap: 'wrap',
          }}
        >
          <span>
            Observed workspace rows: <strong style={{ color: t.color.text }}>{context?.summary.journey_rows?.toLocaleString() ?? '—'}</strong>
          </span>
          <span>
            {context?.summary.date_from && context?.summary.date_to
              ? `${context.summary.date_from} – ${context.summary.date_to}`
              : 'No observed journey rows yet'}
          </span>
          <span>Segment family: <strong style={{ color: t.color.text }}>{segmentType}</strong></span>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(240px, 100%), 1fr))', gap: t.space.md }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Segment name
            <input
              value={name}
              onChange={(event) => setName(event.target.value)}
              style={{
                width: '100%',
                padding: '10px 12px',
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.border}`,
                fontSize: t.font.sizeSm,
              }}
            />
          </label>

          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Match logic
            <select
              value={match}
              onChange={(event) => setMatch(event.target.value === 'any' ? 'any' : 'all')}
              style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
            >
              <option value="all">All rules must match</option>
              <option value="any">Any rule can match</option>
            </select>
          </label>
        </div>

        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Description
          <textarea
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            rows={2}
            style={{
              width: '100%',
              padding: '10px 12px',
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              fontSize: t.font.sizeSm,
              resize: 'vertical',
            }}
          />
        </label>

        <div
          style={{
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.md,
            padding: t.space.md,
            display: 'grid',
            gap: t.space.sm,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.md, flexWrap: 'wrap', alignItems: 'center' }}>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Rules define the analytical audience. Use dimension rules for filter-compatible slices, or add lag, path, and value rules for smarter MTA audiences.
            </div>
            <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
              <button
                type="button"
                onClick={() => setRules((current) => [...current, buildEmptyRule('channel_group')])}
                style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}
              >
                Add dimension rule
              </button>
              <button
                type="button"
                onClick={() => setRules((current) => [...current, buildEmptyRule('lag_days')])}
                style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}
              >
                Add lag rule
              </button>
              <button
                type="button"
                onClick={() => setRules((current) => [...current, buildEmptyRule('net_revenue_total')])}
                style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}
              >
                Add value rule
              </button>
            </div>
          </div>

          {rules.map((rule, index) => {
            const fieldOptions = getFieldOptions(context, rule.field)
            const isDimensionField = DIMENSION_FIELD_OPTIONS.includes(rule.field)
            return (
              <div
                key={`${rule.field}-${index}`}
                style={{
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.sm,
                  padding: t.space.sm,
                  display: 'grid',
                  gap: t.space.sm,
                }}
              >
                <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.2fr) minmax(0, 0.8fr) minmax(0, 1fr) auto', gap: t.space.sm, alignItems: 'end' }}>
                  <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Field
                    <select
                      value={rule.field}
                      onChange={(event) => {
                        const nextField = event.target.value as SegmentRuleField
                        setRules((current) =>
                          current.map((candidate, candidateIndex) =>
                            candidateIndex === index
                              ? buildEmptyRule(nextField)
                              : candidate,
                          ),
                        )
                      }}
                      style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                    >
                      {Array.from(new Set(FIELD_OPTIONS.map((option) => option.group))).map((group) => (
                        <optgroup key={group} label={group}>
                          {FIELD_OPTIONS.filter((option) => option.group === group).map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </optgroup>
                      ))}
                    </select>
                  </label>

                  <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Operator
                    <select
                      value={rule.op}
                      onChange={(event) =>
                        setRules((current) =>
                          current.map((candidate, candidateIndex) =>
                            candidateIndex === index
                              ? { ...candidate, op: event.target.value as SegmentRuleOperator }
                              : candidate,
                          ),
                        )
                      }
                      style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                    >
                      {isDimensionField ? (
                        <option value="eq">equals</option>
                      ) : (
                        <>
                          <option value="gte">≥ at least</option>
                          <option value="lte">≤ at most</option>
                        </>
                      )}
                    </select>
                  </label>

                  <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                    Value
                    {isDimensionField ? (
                      <select
                        value={String(rule.value || '')}
                        onChange={(event) =>
                          setRules((current) =>
                            current.map((candidate, candidateIndex) =>
                              candidateIndex === index ? { ...candidate, value: event.target.value } : candidate,
                            ),
                          )
                        }
                        style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      >
                        <option value="">Select value</option>
                        {fieldOptions.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.value} ({option.count.toLocaleString()})
                          </option>
                        ))}
                      </select>
                    ) : (
                      <input
                        type="number"
                        step={rule.field === 'lag_days' ? '0.5' : '1'}
                        value={String(rule.value ?? '')}
                        onChange={(event) =>
                          setRules((current) =>
                            current.map((candidate, candidateIndex) =>
                              candidateIndex === index
                                ? { ...candidate, value: Number(event.target.value || 0) }
                                : candidate,
                            ),
                          )
                        }
                        style={{ width: '100%', padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                      />
                    )}
                  </label>

                  <button
                    type="button"
                    onClick={() => setRules((current) => current.filter((_, candidateIndex) => candidateIndex !== index))}
                    style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '10px 12px', cursor: 'pointer', height: 'fit-content' }}
                  >
                    Remove
                  </button>
                </div>

                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                  {formatSuggestedLabel(rule)}
                </div>
              </div>
            )
          })}

          {context?.suggested_rules ? (
            <div style={{ display: 'grid', gap: 8 }}>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Smart starting points
              </div>
              <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
                {[...(context.suggested_rules.lag_days || []), ...(context.suggested_rules.path_length || []), ...(context.suggested_rules.value || [])].map((suggested) => (
                  <button
                    key={`${suggested.field}-${suggested.op}-${suggested.value}`}
                    type="button"
                    onClick={() =>
                      setRules((current) => [
                        ...current,
                        {
                          field: suggested.field,
                          op: suggested.op,
                          value: suggested.value,
                        },
                      ])
                    }
                    style={{
                      border: `1px solid ${t.color.border}`,
                      background: t.color.bg,
                      borderRadius: 999,
                      padding: '6px 10px',
                      cursor: 'pointer',
                      fontSize: t.font.sizeXs,
                    }}
                  >
                    {suggested.label}
                  </button>
                ))}
              </div>
            </div>
          ) : null}
        </div>

        {error ? <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{error}</div> : null}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
          <button
            type="button"
            onClick={onClose}
            style={{
              border: `1px solid ${t.color.border}`,
              background: 'transparent',
              borderRadius: t.radius.sm,
              padding: '8px 12px',
              cursor: 'pointer',
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={submitting || !canSubmit}
            onClick={() =>
              onSubmit({
                name: name.trim(),
                description: description.trim(),
                definition: {
                  version: 'v2',
                  match,
                  rules: validRules,
                },
              })
            }
            style={{
              border: `1px solid ${t.color.accent}`,
              background: t.color.accent,
              color: t.color.surface,
              borderRadius: t.radius.sm,
              padding: '8px 12px',
              cursor: submitting ? 'default' : 'pointer',
              opacity: submitting || !canSubmit ? 0.7 : 1,
            }}
          >
            {submitting ? (mode === 'create' ? 'Creating…' : 'Saving…') : mode === 'create' ? 'Create segment' : 'Save changes'}
          </button>
        </div>
      </div>
    </div>
  )
}
