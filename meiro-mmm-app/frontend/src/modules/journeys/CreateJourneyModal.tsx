import { tokens as t } from '../../theme/tokens'

interface JourneyDraft {
  name: string
  description: string
  conversion_kpi_id: string
  lookback_window_days: number
  mode_default: 'conversion_only' | 'all_journeys'
}

interface KpiOption {
  id: string
  label: string
}

interface CreateJourneyModalProps {
  draft: JourneyDraft
  kpiOptions: KpiOption[]
  createError: string | null
  creating: boolean
  onClose: () => void
  onSubmit: () => void
  onDraftChange: (next: JourneyDraft) => void
  onClampLookback: (value: number) => number
}

export default function CreateJourneyModal({
  draft,
  kpiOptions,
  createError,
  creating,
  onClose,
  onSubmit,
  onDraftChange,
  onClampLookback,
}: CreateJourneyModalProps) {
  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 50, background: 'rgba(15, 23, 42, 0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
      <div style={{ width: 'min(620px, 100%)', background: t.color.surface, borderRadius: t.radius.lg, border: `1px solid ${t.color.border}`, boxShadow: t.shadowLg, padding: t.space.xl, display: 'grid', gap: t.space.md }}>
        <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Create journey</h3>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Name
          <input value={draft.name} onChange={(e) => onDraftChange({ ...draft, name: e.target.value })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Description
          <textarea value={draft.description} onChange={(e) => onDraftChange({ ...draft, description: e.target.value })} rows={3} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Conversion KPI
          <select value={draft.conversion_kpi_id} onChange={(e) => onDraftChange({ ...draft, conversion_kpi_id: e.target.value })} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
            {kpiOptions.map((kpi) => <option key={kpi.id} value={kpi.id}>{kpi.label} ({kpi.id})</option>)}
          </select>
        </label>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>Lookback window (days)
          <input type="number" min={1} max={365} value={draft.lookback_window_days} onChange={(e) => onDraftChange({ ...draft, lookback_window_days: onClampLookback(Number(e.target.value)) })} style={{ width: 180, padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <div style={{ display: 'flex', gap: t.space.sm }}>
          <button type="button" onClick={() => onDraftChange({ ...draft, mode_default: 'conversion_only' })} style={{ border: `1px solid ${draft.mode_default === 'conversion_only' ? t.color.accent : t.color.border}`, background: draft.mode_default === 'conversion_only' ? t.color.accentMuted : t.color.surface, borderRadius: t.radius.full, padding: '6px 12px', cursor: 'pointer' }}>Conversion only</button>
          <button type="button" onClick={() => onDraftChange({ ...draft, mode_default: 'all_journeys' })} style={{ border: `1px solid ${draft.mode_default === 'all_journeys' ? t.color.accent : t.color.border}`, background: draft.mode_default === 'all_journeys' ? t.color.accentMuted : t.color.surface, borderRadius: t.radius.full, padding: '6px 12px', cursor: 'pointer' }}>All journeys</button>
        </div>
        {createError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{createError}</div>}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
          <button type="button" onClick={onClose} style={{ border: `1px solid ${t.color.border}`, background: 'transparent', borderRadius: t.radius.sm, padding: '8px 12px', cursor: 'pointer' }}>Cancel</button>
          <button type="button" onClick={onSubmit} disabled={creating} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 14px', cursor: creating ? 'wait' : 'pointer' }}>{creating ? 'Creating…' : 'Create'}</button>
        </div>
      </div>
    </div>
  )
}
