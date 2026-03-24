import type { MeiroMappingState, MeiroWebhookSuggestions } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'

interface MeiroNormalizationProps {
  meiroMappingState?: MeiroMappingState
  meiroWebhookSuggestions?: MeiroWebhookSuggestions
  applyMeiroMappingSuggestionPending: boolean
  updateMeiroMappingApprovalPending: boolean
  relativeTime: (iso?: string | null) => string
  onSaveMeiroMapping: (payload: Record<string, unknown>) => void
  onApplyMeiroMappingSuggestion: () => void
  onApproveMeiroMapping: () => void
  onRejectMeiroMapping: () => void
}

export default function MeiroNormalization({
  meiroMappingState,
  meiroWebhookSuggestions,
  applyMeiroMappingSuggestionPending,
  updateMeiroMappingApprovalPending,
  relativeTime,
  onSaveMeiroMapping,
  onApplyMeiroMappingSuggestion,
  onApproveMeiroMapping,
  onRejectMeiroMapping,
}: MeiroNormalizationProps) {
  return (
    <div style={{ display: 'grid', gap: t.space.sm }}>
      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Normalization status</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Mapping version <strong>{meiroMappingState?.version ?? 0}</strong> · approval <strong>{meiroMappingState?.approval?.status || 'unreviewed'}</strong>
          {meiroMappingState?.approval?.updated_at ? <> · updated {relativeTime(meiroMappingState.approval.updated_at)}</> : null}
        </div>
        {meiroMappingState?.approval?.note ? <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Note: {meiroMappingState.approval.note}</div> : null}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Suggested mapping from Pipes payloads</div>
        <div style={{ display: 'grid', gap: 4, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          <div>Touchpoints: <code>{String((meiroWebhookSuggestions as any)?.apply_payloads?.mapping?.touchpoint_attr || 'touchpoints')}</code></div>
          <div>Value: <code>{String((meiroWebhookSuggestions as any)?.apply_payloads?.mapping?.value_attr || 'conversion_value')}</code></div>
          <div>Source: <code>{String((meiroWebhookSuggestions as any)?.apply_payloads?.mapping?.source_field || 'source')}</code> · Medium: <code>{String((meiroWebhookSuggestions as any)?.apply_payloads?.mapping?.medium_field || 'medium')}</code></div>
          <div>Campaign: <code>{String((meiroWebhookSuggestions as any)?.apply_payloads?.mapping?.campaign_field || 'campaign')}</code> · Channel: <code>{String((meiroWebhookSuggestions as any)?.apply_payloads?.mapping?.channel_field || 'channel')}</code></div>
        </div>
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
          <button type="button" onClick={onApplyMeiroMappingSuggestion} disabled={applyMeiroMappingSuggestionPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', opacity: applyMeiroMappingSuggestionPending ? 0.7 : 1 }}>
            {applyMeiroMappingSuggestionPending ? 'Applying…' : 'Apply mapping suggestion'}
          </button>
          <button type="button" onClick={onApproveMeiroMapping} disabled={updateMeiroMappingApprovalPending} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            Approve mapping
          </button>
          <button type="button" onClick={onRejectMeiroMapping} disabled={updateMeiroMappingApprovalPending} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            Reject mapping
          </button>
        </div>
      </div>

      <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Field mapping editor (advanced JSON).</div>
      <textarea
        defaultValue={JSON.stringify(meiroMappingState?.mapping || {}, null, 2)}
        rows={10}
        onBlur={(e) => {
          try {
            const parsed = JSON.parse(e.target.value)
            onSaveMeiroMapping(parsed)
          } catch {
            // ignore parse error in blur
          }
        }}
        style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontFamily: 'monospace' }}
      />
    </div>
  )
}
