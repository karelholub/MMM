import type { Dispatch, SetStateAction } from 'react'

import DashboardTable from '../../components/dashboard/DashboardTable'
import type { MeiroConfig, MeiroWebhookEvent } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import type { MeiroWebhookArchiveStatus } from './shared'

interface MeiroPipesSettingsProps {
  meiroConfig?: MeiroConfig
  webhookSecretValue: string | null
  meiroWebhookEvents?: { items: MeiroWebhookEvent[]; total: number }
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  meiroWebhookEventsLoading: boolean
  meiroWebhookEventsError?: string | null
  relativeTime: (iso?: string | null) => string
  setOauthToast: Dispatch<SetStateAction<string | null>>
  onRotateWebhookSecret: () => void
}

export default function MeiroPipesSettings({
  meiroConfig,
  webhookSecretValue,
  meiroWebhookEvents,
  meiroWebhookArchiveStatus,
  meiroWebhookEventsLoading,
  meiroWebhookEventsError,
  relativeTime,
  setOauthToast,
  onRotateWebhookSecret,
}: MeiroPipesSettingsProps) {
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
