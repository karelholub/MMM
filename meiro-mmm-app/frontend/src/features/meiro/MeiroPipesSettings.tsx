import type { Dispatch, SetStateAction } from 'react'

import DashboardTable from '../../components/dashboard/DashboardTable'
import type { MeiroConfig, MeiroPullConfig, MeiroWebhookEvent } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import { DEFAULT_MEIRO_PULL_CONFIG, type MeiroWebhookArchiveStatus } from './shared'

interface MeiroPipesSettingsProps {
  meiroConfig?: MeiroConfig
  meiroPullDraft: MeiroPullConfig
  setMeiroPullDraft: Dispatch<SetStateAction<MeiroPullConfig>>
  webhookSecretValue: string | null
  meiroWebhookEvents?: { items: MeiroWebhookEvent[]; total: number }
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  meiroWebhookEventsLoading: boolean
  meiroWebhookEventsError?: string | null
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
  meiroWebhookArchiveStatus,
  meiroWebhookEventsLoading,
  meiroWebhookEventsError,
  relativeTime,
  setOauthToast,
  onRotateWebhookSecret,
  onSaveMeiroPull,
  saveMeiroPullPending,
}: MeiroPipesSettingsProps) {
  const aliasText = Object.entries(meiroPullDraft.conversion_event_aliases || {})
    .map(([raw, canonical]) => `${raw}=${canonical}`)
    .join('\n')

  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro Pipes webhook</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Use this when Meiro Pipes pushes normalized profiles and conversion payloads into Meiro Measurement.</div>
        <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center' }}>
          <code style={{ display: 'block', padding: '6px 8px', borderRadius: t.radius.sm, background: t.color.surface, border: `1px solid ${t.color.border}`, fontSize: t.font.sizeXs }}>
            {meiroConfig?.webhook_url || 'http://localhost:8000/api/connectors/meiro/profiles'}
          </code>
          <button
            type="button"
            onClick={async () => {
              const val = meiroConfig?.webhook_url || 'http://localhost:8000/api/connectors/meiro/profiles'
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
          Received: {meiroConfig?.webhook_received_count ?? 0} · Last received: {relativeTime(meiroConfig?.webhook_last_received_at)} · Archive: {meiroWebhookArchiveStatus?.available ? `${meiroWebhookArchiveStatus.entries} batches` : 'empty'}
        </div>
        {webhookSecretValue && <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>New secret (shown once): <code>{webhookSecretValue}</code></div>}
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
              value={aliasText}
              onChange={(e) => {
                const next = Object.fromEntries(
                  e.target.value
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
                <th>Profiles</th>
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
                  <td>{event.replace ? 'replace' : 'append'}</td>
                  <td>{Number(event.stored_total || 0).toLocaleString()}</td>
                  <td>{event.ip || '—'}</td>
                </tr>
              ))}
              {!(meiroWebhookEvents?.items || []).length && (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', color: t.color.textSecondary }}>No webhook events received yet.</td>
                </tr>
              )}
            </tbody>
          </DashboardTable>
        )}
      </div>
    </div>
  )
}
