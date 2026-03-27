import { useEffect, useState, type Dispatch, type SetStateAction } from 'react'

import DashboardTable from '../../components/dashboard/DashboardTable'
import type { MeiroConfig, MeiroPullConfig, MeiroWebhookDiagnostics, MeiroWebhookEvent } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import { DEFAULT_MEIRO_PULL_CONFIG, type MeiroWebhookArchiveStatus } from './shared'

interface MeiroPipesSettingsProps {
  meiroConfig?: MeiroConfig
  meiroPullDraft: MeiroPullConfig
  setMeiroPullDraft: Dispatch<SetStateAction<MeiroPullConfig>>
  webhookSecretValue: string | null
  meiroWebhookEvents?: { items: MeiroWebhookEvent[]; total: number }
  meiroWebhookDiagnostics?: MeiroWebhookDiagnostics
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  meiroEventArchiveStatus?: MeiroWebhookArchiveStatus
  meiroWebhookEventsLoading: boolean
  meiroWebhookEventsError?: string | null
  meiroWebhookDiagnosticsError?: string | null
  relativeTime: (iso?: string | null) => string
  setOauthToast: Dispatch<SetStateAction<string | null>>
  onRotateWebhookSecret: () => void
  onSaveMeiroPull: () => void
  saveMeiroPullPending: boolean
}

export default function MeiroPipesSettings({
  meiroConfig,
  meiroPullDraft,
  setMeiroPullDraft,
  webhookSecretValue,
  meiroWebhookEvents,
  meiroWebhookDiagnostics,
  meiroWebhookArchiveStatus,
  meiroEventArchiveStatus,
  meiroWebhookEventsLoading,
  meiroWebhookEventsError,
  meiroWebhookDiagnosticsError,
  relativeTime,
  setOauthToast,
  onRotateWebhookSecret,
  onSaveMeiroPull,
  saveMeiroPullPending,
}: MeiroPipesSettingsProps) {
  const webhookUrl = meiroConfig?.webhook_url || 'http://localhost:8000/api/connectors/meiro/profiles'
  const eventWebhookUrl = meiroConfig?.event_webhook_url || 'http://localhost:8000/api/connectors/meiro/events'
  const webhookHealthUrl = `${webhookUrl.replace(/\/$/, '')}/health`
  const eventWebhookHealthUrl = `${eventWebhookUrl.replace(/\/$/, '')}/health`
  const aliasText = Object.entries(meiroPullDraft.conversion_event_aliases || {})
    .map(([raw, canonical]) => `${raw}=${canonical}`)
    .join('\n')
  const interactionAliasText = Object.entries(meiroPullDraft.touchpoint_interaction_aliases || {})
    .map(([raw, canonical]) => `${raw}=${canonical}`)
    .join('\n')
  const adjustmentAliasText = Object.entries(meiroPullDraft.adjustment_event_aliases || {})
    .map(([raw, canonical]) => `${raw}=${canonical}`)
    .join('\n')
  const [aliasDraft, setAliasDraft] = useState(aliasText)
  const [interactionAliasDraft, setInteractionAliasDraft] = useState(interactionAliasText)
  const [adjustmentAliasDraft, setAdjustmentAliasDraft] = useState(adjustmentAliasText)

  useEffect(() => {
    setAliasDraft(aliasText)
  }, [aliasText])
  useEffect(() => {
    setInteractionAliasDraft(interactionAliasText)
  }, [interactionAliasText])
  useEffect(() => {
    setAdjustmentAliasDraft(adjustmentAliasText)
  }, [adjustmentAliasText])

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro Pipes webhook</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Use this when Meiro Pipes pushes normalized profiles and conversion payloads into Meiro Measurement.</div>
        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
          <code style={{ display: 'block', padding: '6px 8px', borderRadius: t.radius.sm, background: t.color.surface, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeXs }}>
            {webhookUrl}
          </code>
          <button
            type="button"
            onClick={async () => {
              const val = webhookUrl
              try {
                await navigator.clipboard.writeText(val)
                setOauthToast('Webhook URL copied.')
              } catch {
                setOauthToast(val)
              }
            }}
            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 10px', cursor: 'pointer', fontSize: t.font.sizeXs }}
          >
            Copy URL
          </button>
          <button type="button" onClick={onRotateWebhookSecret} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 10px', cursor: 'pointer', fontSize: t.font.sizeXs }}>
            Rotate webhook secret
          </button>
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Received: {Number(meiroConfig?.webhook_received_count ?? 0).toLocaleString()} payloads · Last received: {relativeTime(meiroConfig?.webhook_last_received_at)} · Archive: {meiroWebhookArchiveStatus?.available ? `${Number(meiroWebhookArchiveStatus.entries || 0).toLocaleString()} batches / ${Number(meiroWebhookArchiveStatus.profiles_received || 0).toLocaleString()} payloads` : 'empty'}
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Health check URL: <code>{webhookHealthUrl}</code>
        </div>
        {webhookSecretValue && <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>New secret (shown once): <code>{webhookSecretValue}</code></div>}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro Pipes raw events webhook</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Use this when Meiro Pipes pushes raw events instead of already assembled profiles. Events are archived immutably first and then reconstructed into journeys for replay/import.</div>
        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
          <code style={{ display: 'block', padding: '6px 8px', borderRadius: t.radius.sm, background: t.color.surface, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeXs }}>
            {eventWebhookUrl}
          </code>
          <button
            type="button"
            onClick={async () => {
              const val = eventWebhookUrl
              try {
                await navigator.clipboard.writeText(val)
                setOauthToast('Event webhook URL copied.')
              } catch {
                setOauthToast(val)
              }
            }}
            style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 10px', cursor: 'pointer', fontSize: t.font.sizeXs }}
          >
            Copy URL
          </button>
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Received: {Number(meiroConfig?.event_webhook_received_count ?? 0).toLocaleString()} raw events · Last received: {relativeTime(meiroConfig?.event_webhook_last_received_at)} · Archive: {meiroEventArchiveStatus?.available ? `${Number(meiroEventArchiveStatus.entries || 0).toLocaleString()} batches / ${Number(meiroEventArchiveStatus.events_received || 0).toLocaleString()} events` : 'empty'}
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Health check URL: <code>{eventWebhookHealthUrl}</code>
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
          Uses the same <code>X-Meiro-Webhook-Secret</code> as the profile webhook.
        </div>
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Source selection</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Choose which Meiro webhook stream should be treated as the primary attribution source. Keep both active only while validating the raw-event path.
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Primary attribution source
            <select
              value={meiroPullDraft.primary_ingest_source || DEFAULT_MEIRO_PULL_CONFIG.primary_ingest_source}
              onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, primary_ingest_source: e.target.value as MeiroPullConfig['primary_ingest_source'] }))}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
            >
              <option value="profiles">Profiles webhook</option>
              <option value="events">Raw events webhook</option>
            </select>
          </label>
        </div>
        {(Number(meiroConfig?.webhook_received_count || 0) > 0 && Number(meiroConfig?.event_webhook_received_count || 0) > 0) ? (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.warning }}>
            Both profile and raw-event webhooks are receiving live traffic. Replay source should usually be pinned to the same source as the primary attribution source.
          </div>
        ) : null}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Auto-replay raw events into attribution</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Let the app rebuild journeys from the raw-event archive automatically after successful event batches or at a controlled interval. Guardrails stop auto-replay when mappings are unapproved or quarantine spikes.
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Auto-replay mode
            <select
              value={meiroPullDraft.auto_replay_mode || DEFAULT_MEIRO_PULL_CONFIG.auto_replay_mode}
              onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, auto_replay_mode: e.target.value as MeiroPullConfig['auto_replay_mode'] }))}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
            >
              <option value="disabled">Disabled</option>
              <option value="interval">Every N minutes</option>
              <option value="after_batch">After each successful event batch</option>
            </select>
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Interval minutes
            <input
              type="number"
              min={1}
              max={1440}
              value={meiroPullDraft.auto_replay_interval_minutes ?? DEFAULT_MEIRO_PULL_CONFIG.auto_replay_interval_minutes}
              onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, auto_replay_interval_minutes: Number(e.target.value || DEFAULT_MEIRO_PULL_CONFIG.auto_replay_interval_minutes) }))}
              disabled={(meiroPullDraft.auto_replay_mode || DEFAULT_MEIRO_PULL_CONFIG.auto_replay_mode) !== 'interval'}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
            />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Quarantine spike threshold (%)
            <input
              type="number"
              min={0}
              max={100}
              value={meiroPullDraft.auto_replay_quarantine_spike_threshold_pct ?? DEFAULT_MEIRO_PULL_CONFIG.auto_replay_quarantine_spike_threshold_pct}
              onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, auto_replay_quarantine_spike_threshold_pct: Number(e.target.value || DEFAULT_MEIRO_PULL_CONFIG.auto_replay_quarantine_spike_threshold_pct) }))}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
            />
          </label>
        </div>
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.text }}>
          <input
            type="checkbox"
            checked={Boolean(meiroPullDraft.auto_replay_require_mapping_approval ?? DEFAULT_MEIRO_PULL_CONFIG.auto_replay_require_mapping_approval)}
            onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, auto_replay_require_mapping_approval: e.target.checked }))}
          />
          Require approved mapping before auto-replay
        </label>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Last auto-replay attempt: {relativeTime(meiroConfig?.auto_replay_state?.last_attempted_at || null)} ·
          Last completion: {relativeTime(meiroConfig?.auto_replay_state?.last_completed_at || null)} ·
          Status: <strong>{String(meiroConfig?.auto_replay_state?.last_status || 'idle')}</strong>
        </div>
        {meiroConfig?.auto_replay_state?.last_reason ? (
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            Last note: {meiroConfig.auto_replay_state.last_reason}
          </div>
        ) : null}
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
          Duplicate reprocessing is avoided by checkpointing the last processed raw-event archive snapshot. Auto-replay only runs for the raw-event primary source.
        </div>
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Webhook sanitation & quarantine</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          These rules run on Meiro Pipes payloads before journeys are persisted into attribution. Use them to quarantine noisy webhook records instead of mixing them into production journeys.
        </div>
        <div style={{ display: 'grid', gap: t.space.sm }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="checkbox" checked={meiroPullDraft.strict_ingest} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, strict_ingest: e.target.checked }))} />
              Strict ingest before persistence
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="checkbox" checked={meiroPullDraft.quarantine_unknown_channels} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, quarantine_unknown_channels: e.target.checked }))} />
              Quarantine unknown channels
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="checkbox" checked={meiroPullDraft.quarantine_missing_utm} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, quarantine_missing_utm: e.target.checked }))} />
              Quarantine missing source / medium
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: t.color.text }}>
              <input type="checkbox" checked={Boolean(meiroPullDraft.quarantine_duplicate_profiles)} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, quarantine_duplicate_profiles: e.target.checked }))} />
              Quarantine duplicate profile IDs
            </label>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Timestamp fallback
              <select value={meiroPullDraft.timestamp_fallback_policy} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, timestamp_fallback_policy: e.target.value as MeiroPullConfig['timestamp_fallback_policy'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}>
                <option value="profile">Fallback to profile timestamp</option>
                <option value="conversion">Fallback to conversion timestamp</option>
                <option value="quarantine">Quarantine missing timestamps</option>
              </select>
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Value fallback
              <select value={meiroPullDraft.value_fallback_policy} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, value_fallback_policy: e.target.value as MeiroPullConfig['value_fallback_policy'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}>
                <option value="default">Use revenue defaults</option>
                <option value="zero">Coerce missing values to zero</option>
                <option value="quarantine">Quarantine missing values</option>
              </select>
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Currency fallback
              <select value={meiroPullDraft.currency_fallback_policy} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, currency_fallback_policy: e.target.value as MeiroPullConfig['currency_fallback_policy'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}>
                <option value="default">Use default / base currency</option>
                <option value="quarantine">Quarantine missing currencies</option>
              </select>
            </label>
          </div>

          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Conversion aliases
            <textarea
              value={aliasDraft}
              onChange={(e) => {
                const nextDraft = e.target.value
                setAliasDraft(nextDraft)
                const next = Object.fromEntries(
                  nextDraft
                    .split('\n')
                    .map((line) => line.trim())
                    .filter(Boolean)
                    .map((line) => line.split('=').map((part) => part.trim()))
                    .filter((parts): parts is [string, string] => parts.length === 2 && Boolean(parts[0]) && Boolean(parts[1]))
                )
                setMeiroPullDraft((prev) => ({ ...prev, conversion_event_aliases: next }))
              }}
              placeholder={'order completed=purchase\ncheckout started=begin_checkout'}
              rows={4}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontFamily: 'monospace' }}
            />
          </label>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
            Use one alias per line in the form <code>raw_event=canonical_event</code>. These aliases run before deterministic conversion mapping and quarantine checks.
          </div>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Touchpoint interaction aliases
            <textarea
              value={interactionAliasDraft}
              onChange={(e) => {
                const nextDraft = e.target.value
                setInteractionAliasDraft(nextDraft)
                const next = Object.fromEntries(
                  nextDraft
                    .split('\n')
                    .map((line) => line.trim())
                    .filter(Boolean)
                    .map((line) => line.split('=').map((part) => part.trim()))
                    .filter((parts): parts is [string, string] => parts.length === 2 && Boolean(parts[0]) && Boolean(parts[1]))
                )
                setMeiroPullDraft((prev) => ({ ...prev, touchpoint_interaction_aliases: next }))
              }}
              placeholder={'ad_impression=impression\nemail_click=click\npage_view=visit'}
              rows={4}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontFamily: 'monospace' }}
            />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Refund / invalid event aliases
            <textarea
              value={adjustmentAliasDraft}
              onChange={(e) => {
                const nextDraft = e.target.value
                setAdjustmentAliasDraft(nextDraft)
                const next = Object.fromEntries(
                  nextDraft
                    .split('\n')
                    .map((line) => line.trim())
                    .filter(Boolean)
                    .map((line) => line.split('=').map((part) => part.trim()))
                    .filter((parts): parts is [string, string] => parts.length === 2 && Boolean(parts[0]) && Boolean(parts[1]))
                )
                setMeiroPullDraft((prev) => ({ ...prev, adjustment_event_aliases: next }))
              }}
              placeholder={'order_refunded=refund\norder_cancelled=cancellation\ninvalid_lead=invalid_lead'}
              rows={4}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, fontFamily: 'monospace' }}
            />
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Adjustment linkage keys
            <input
              value={(meiroPullDraft.adjustment_linkage_keys || []).join(', ')}
              onChange={(e) =>
                setMeiroPullDraft((prev) => ({
                  ...prev,
                  adjustment_linkage_keys: e.target.value
                    .split(',')
                    .map((value) => value.trim())
                    .filter((value): value is 'conversion_id' | 'order_id' | 'lead_id' | 'event_id' =>
                      ['conversion_id', 'order_id', 'lead_id', 'event_id'].includes(value),
                    ),
                }))
              }
              placeholder="conversion_id, order_id, lead_id, event_id"
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}
            />
          </label>
          <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
            <button type="button" onClick={onSaveMeiroPull} disabled={saveMeiroPullPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', opacity: saveMeiroPullPending ? 0.7 : 1 }}>
              {saveMeiroPullPending ? 'Saving…' : 'Save webhook sanitation'}
            </button>
            <button type="button" onClick={() => setMeiroPullDraft(DEFAULT_MEIRO_PULL_CONFIG)} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
              Reset to defaults
            </button>
          </div>
        </div>
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Webhook diagnostics</div>
        {meiroWebhookDiagnosticsError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{meiroWebhookDiagnosticsError}</div>
        ) : meiroWebhookDiagnostics ? (
          <>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Recent successes</div>
                <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(meiroWebhookDiagnostics.recent_success_count || 0).toLocaleString()}</div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Server-side errors</div>
                <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold }}>{Number(meiroWebhookDiagnostics.recent_error_count || 0).toLocaleString()}</div>
              </div>
              <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm }}>
                <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Last receipt</div>
                <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold }}>{relativeTime(meiroWebhookDiagnostics.last_received_at)}</div>
              </div>
            </div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
              Health check: <code>{meiroWebhookDiagnostics.health_url}</code>
            </div>
            {!!Object.keys(meiroWebhookDiagnostics.recent_error_classes || {}).length && (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Error classes: {Object.entries(meiroWebhookDiagnostics.recent_error_classes || {}).map(([key, value]) => `${key} (${value})`).join(' · ')}
              </div>
            )}
            {meiroWebhookDiagnostics.latest_error ? (
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                Latest server-side error: <strong>{String(meiroWebhookDiagnostics.latest_error.error_class || 'unknown')}</strong>
                {meiroWebhookDiagnostics.latest_error.error_detail ? <> · {String(meiroWebhookDiagnostics.latest_error.error_detail)}</> : null}
              </div>
            ) : null}
            {(meiroWebhookDiagnostics.notes || []).map((note, index) => (
              <div key={`${note}-${index}`} style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                {note}
              </div>
            ))}
          </>
        ) : (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Diagnostics not available yet.</div>
        )}
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Replay archived Pipes payloads</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Controls how replay rebuilds journeys from the webhook archive. Use <strong>all</strong> only when you want to rebuild the full archive, because it can be much larger than the recent replay subset.
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Replay scope
            <select
              value={meiroPullDraft.replay_mode || DEFAULT_MEIRO_PULL_CONFIG.replay_mode}
              onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, replay_mode: e.target.value as MeiroPullConfig['replay_mode'] }))}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
            >
              <option value="last_n">Last N archived batches</option>
              <option value="all">Entire archive</option>
              <option value="date_range">Date range</option>
            </select>
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
            Replay source
            <select
              value={meiroPullDraft.replay_archive_source || DEFAULT_MEIRO_PULL_CONFIG.replay_archive_source}
              onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, replay_archive_source: e.target.value as MeiroPullConfig['replay_archive_source'] }))}
              style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
            >
              <option value="auto">Auto (primary source, then freshest archive)</option>
              <option value="profiles">Profiles archive</option>
              <option value="events">Raw events archive</option>
            </select>
          </label>
          {(meiroPullDraft.replay_mode || DEFAULT_MEIRO_PULL_CONFIG.replay_mode) === 'last_n' ? (
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Archived batches to replay
              <input
                type="number"
                min={1}
                max={50000}
                value={meiroPullDraft.replay_archive_limit || DEFAULT_MEIRO_PULL_CONFIG.replay_archive_limit}
                onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, replay_archive_limit: Math.max(1, Math.min(50000, Number(e.target.value) || DEFAULT_MEIRO_PULL_CONFIG.replay_archive_limit || 5000)) }))}
                style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
              />
            </label>
          ) : null}
          {(meiroPullDraft.replay_mode || DEFAULT_MEIRO_PULL_CONFIG.replay_mode) === 'date_range' ? (
            <>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                From
                <input
                  type="datetime-local"
                  value={meiroPullDraft.replay_date_from || ''}
                  onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, replay_date_from: e.target.value || null }))}
                  style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
                />
              </label>
              <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
                To
                <input
                  type="datetime-local"
                  value={meiroPullDraft.replay_date_to || ''}
                  onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, replay_date_to: e.target.value || null }))}
                  style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}
                />
              </label>
            </>
          ) : null}
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
          Profile archive: {meiroWebhookArchiveStatus?.available ? `${Number(meiroWebhookArchiveStatus.entries || 0).toLocaleString()} batches containing ${Number(meiroWebhookArchiveStatus.profiles_received || 0).toLocaleString()} payloads.` : 'empty'}{' '}
          · Event archive: {meiroEventArchiveStatus?.available ? `${Number(meiroEventArchiveStatus.entries || 0).toLocaleString()} batches containing ${Number(meiroEventArchiveStatus.events_received || 0).toLocaleString()} events.` : 'empty'}
        </div>
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Recent webhook events</div>
        {meiroWebhookEventsLoading ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading events…</div>
        ) : meiroWebhookEventsError ? (
          <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{meiroWebhookEventsError}</div>
        ) : (
          <DashboardTable density="compact">
            <thead>
              <tr>
                <th>Received</th>
                <th>Records</th>
                <th>Kind</th>
                <th>Mode</th>
                <th>Stored total</th>
                <th>Source IP</th>
              </tr>
            </thead>
            <tbody>
              {(meiroWebhookEvents?.items || []).map((event, idx) => (
                <tr key={`${event.received_at || 'event'}-${idx}`}>
                  <td>{relativeTime(event.received_at)}</td>
                  <td>{Number(event.received_count || 0).toLocaleString()}</td>
                  <td>{String((event as MeiroWebhookEvent & { ingest_kind?: string }).ingest_kind || 'profiles')}</td>
                  <td>{event.replace ? 'replace' : 'append'}</td>
                  <td>{Number(event.stored_total || 0).toLocaleString()}</td>
                  <td>{event.ip || '—'}</td>
                </tr>
              ))}
              {!(meiroWebhookEvents?.items || []).length && (
                <tr>
                  <td colSpan={6} style={{ textAlign: 'center', color: t.color.textSecondary }}>No webhook events received yet.</td>
                </tr>
              )}
            </tbody>
          </DashboardTable>
        )}
      </div>
    </div>
  )
}
