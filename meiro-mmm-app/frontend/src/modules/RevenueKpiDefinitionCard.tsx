import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../theme/tokens'
import { apiGetJson, apiSendJson } from '../lib/apiClient'

type DedupKey = 'conversion_id' | 'order_id' | 'event_id'
type FxMode = 'none' | 'static_rates'

interface RevenueConfig {
  conversion_names: string[]
  value_field_path: string
  currency_field_path: string
  dedup_key: DedupKey
  base_currency: string
  fx_enabled: boolean
  fx_mode: FxMode
  fx_rates_json: Record<string, number>
  source_type?: string
}

const DEFAULT_CONFIG: RevenueConfig = {
  conversion_names: ['purchase'],
  value_field_path: 'value',
  currency_field_path: 'currency',
  dedup_key: 'conversion_id',
  base_currency: 'EUR',
  fx_enabled: false,
  fx_mode: 'none',
  fx_rates_json: {},
  source_type: 'conversion_event',
}

export default function RevenueKpiDefinitionCard() {
  const queryClient = useQueryClient()
  const [draft, setDraft] = useState<RevenueConfig>(DEFAULT_CONFIG)
  const [status, setStatus] = useState<string>('')

  const configQuery = useQuery<RevenueConfig>({
    queryKey: ['settings', 'revenue-config'],
    queryFn: async () =>
      apiGetJson<RevenueConfig>('/api/settings/revenue-config', {
        fallbackMessage: 'Failed to load revenue KPI definition',
      }),
  })

  useEffect(() => {
    if (configQuery.data) {
      setDraft({ ...DEFAULT_CONFIG, ...configQuery.data })
    }
  }, [configQuery.data])

  const saveMutation = useMutation({
    mutationFn: async (payload: RevenueConfig) =>
      apiSendJson<RevenueConfig>('/api/settings/revenue-config', 'PUT', payload, {
        fallbackMessage: 'Failed to save revenue KPI definition',
      }),
    onSuccess: () => {
      setStatus('Saved')
      void queryClient.invalidateQueries({ queryKey: ['settings', 'revenue-config'] })
      void queryClient.invalidateQueries({ queryKey: ['overview-summary'] })
      void queryClient.invalidateQueries({ queryKey: ['overview-drivers'] })
      void queryClient.invalidateQueries({ queryKey: ['channel-performance-summary'] })
      void queryClient.invalidateQueries({ queryKey: ['campaign-performance-summary'] })
      void queryClient.invalidateQueries({ queryKey: ['journeys-summary'] })
      void queryClient.invalidateQueries({ queryKey: ['journeys', 'paths'] })
    },
    onError: (err: Error) => {
      setStatus(err.message || 'Failed to save')
    },
  })

  const rateRows = Object.entries(draft.fx_rates_json || {}).sort((a, b) => a[0].localeCompare(b[0]))

  return (
    <div
      style={{
        border: `1px solid ${t.color.borderLight}`,
        borderRadius: t.radius.lg,
        padding: t.space.lg,
        display: 'grid',
        gap: t.space.md,
        background: t.color.surface,
      }}
    >
      <div>
        <h3
          style={{
            margin: 0,
            fontSize: t.font.sizeMd,
            fontWeight: t.font.weightSemibold,
            color: t.color.text,
          }}
        >
          Revenue KPI definition
        </h3>
        <p style={{ margin: `${t.space.xs}px 0 0`, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          This affects revenue and ROI across the platform.
        </p>
      </div>

      {configQuery.isError ? (
        <div
          style={{
            border: `1px solid ${t.color.danger}`,
            background: t.color.dangerSubtle,
            color: t.color.danger,
            borderRadius: t.radius.sm,
            padding: t.space.sm,
            fontSize: t.font.sizeXs,
          }}
        >
          {(configQuery.error as Error)?.message}
        </div>
      ) : null}

      <div style={{ display: 'grid', gap: t.space.md, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
        <label style={{ display: 'grid', gap: 4 }}>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Conversion events</span>
          <input
            value={(draft.conversion_names || []).join(', ')}
            onChange={(e) =>
              setDraft((prev) => ({
                ...prev,
                conversion_names: e.target.value
                  .split(',')
                  .map((s) => s.trim())
                  .filter(Boolean),
              }))
            }
            placeholder="purchase, checkout_complete"
            style={{ padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
          />
        </label>
        <label style={{ display: 'grid', gap: 4 }}>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Dedup key</span>
          <select
            value={draft.dedup_key}
            onChange={(e) => setDraft((prev) => ({ ...prev, dedup_key: e.target.value as DedupKey }))}
            style={{ padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
          >
            <option value="conversion_id">conversion_id</option>
            <option value="order_id">order_id (recommended)</option>
            <option value="event_id">event_id</option>
          </select>
        </label>
        <label style={{ display: 'grid', gap: 4 }}>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Value field selector</span>
          <input
            value={draft.value_field_path}
            onChange={(e) => setDraft((prev) => ({ ...prev, value_field_path: e.target.value }))}
            style={{ padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
          />
        </label>
        <label style={{ display: 'grid', gap: 4 }}>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Currency field selector</span>
          <input
            value={draft.currency_field_path}
            onChange={(e) => setDraft((prev) => ({ ...prev, currency_field_path: e.target.value }))}
            style={{ padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
          />
        </label>
        <label style={{ display: 'grid', gap: 4 }}>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Base currency</span>
          <input
            value={draft.base_currency}
            onChange={(e) => setDraft((prev) => ({ ...prev, base_currency: e.target.value.toUpperCase() }))}
            style={{ padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
          />
        </label>
      </div>

      <div style={{ display: 'grid', gap: t.space.sm, border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md }}>
        <label style={{ display: 'flex', alignItems: 'center', gap: t.space.sm }}>
          <input
            type="checkbox"
            checked={draft.fx_enabled}
            onChange={(e) => setDraft((prev) => ({ ...prev, fx_enabled: e.target.checked, fx_mode: e.target.checked ? prev.fx_mode : 'none' }))}
          />
          <span style={{ fontSize: t.font.sizeSm, color: t.color.text }}>Convert currencies to base currency</span>
        </label>
        <label style={{ display: 'grid', gap: 4, maxWidth: 240 }}>
          <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>FX mode</span>
          <select
            value={draft.fx_mode}
            onChange={(e) => setDraft((prev) => ({ ...prev, fx_mode: e.target.value as FxMode }))}
            disabled={!draft.fx_enabled}
            style={{ padding: `${t.space.sm}px ${t.space.md}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
          >
            <option value="none">No conversion</option>
            <option value="static_rates">Static rates</option>
          </select>
        </label>
        {draft.fx_enabled && draft.fx_mode === 'static_rates' ? (
          <div style={{ display: 'grid', gap: t.space.xs }}>
            {rateRows.map(([code, rate]) => (
              <div key={code} style={{ display: 'grid', gridTemplateColumns: '110px 1fr auto', gap: t.space.xs }}>
                <input
                  value={code}
                  disabled
                  style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.borderLight}`, background: t.color.bgSubtle }}
                />
                <input
                  type="number"
                  step="0.0001"
                  min="0"
                  value={Number.isFinite(rate) ? rate : ''}
                  onChange={(e) =>
                    setDraft((prev) => ({
                      ...prev,
                      fx_rates_json: {
                        ...prev.fx_rates_json,
                        [code]: Number(e.target.value) || 0,
                      },
                    }))
                  }
                  style={{ padding: `${t.space.xs}px ${t.space.sm}px`, borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
                />
                <button
                  type="button"
                  onClick={() =>
                    setDraft((prev) => {
                      const next = { ...prev.fx_rates_json }
                      delete next[code]
                      return { ...prev, fx_rates_json: next }
                    })
                  }
                  style={{ border: 'none', background: 'transparent', color: t.color.danger, cursor: 'pointer' }}
                >
                  Remove
                </button>
              </div>
            ))}
            <button
              type="button"
              onClick={() =>
                setDraft((prev) => ({
                  ...prev,
                  fx_rates_json: {
                    ...prev.fx_rates_json,
                    USD: prev.fx_rates_json.USD ?? 1,
                  },
                }))
              }
              style={{
                marginTop: t.space.xs,
                width: 'fit-content',
                padding: `${t.space.xs}px ${t.space.sm}px`,
                borderRadius: t.radius.sm,
                border: `1px solid ${t.color.borderLight}`,
                background: t.color.bgSubtle,
                color: t.color.text,
                cursor: 'pointer',
              }}
            >
              Add rate
            </button>
          </div>
        ) : null}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.sm }}>
        <span style={{ fontSize: t.font.sizeXs, color: status.includes('Failed') ? t.color.danger : t.color.textSecondary }}>{status}</span>
        <button
          type="button"
          onClick={() => {
            setStatus('')
            void saveMutation.mutateAsync({
              ...draft,
              conversion_names: draft.conversion_names.length ? draft.conversion_names : ['purchase'],
              base_currency: (draft.base_currency || 'EUR').toUpperCase(),
            })
          }}
          disabled={saveMutation.isPending}
          style={{
            padding: `${t.space.sm}px ${t.space.lg}px`,
            borderRadius: t.radius.sm,
            border: 'none',
            background: t.color.accent,
            color: t.color.surface,
            fontSize: t.font.sizeSm,
            fontWeight: t.font.weightSemibold,
            cursor: saveMutation.isPending ? 'wait' : 'pointer',
          }}
        >
          {saveMutation.isPending ? 'Saving…' : 'Save revenue definition'}
        </button>
      </div>
    </div>
  )
}
