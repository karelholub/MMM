import type { MeiroConfig, MeiroMappingState } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import type { MeiroWebhookArchiveStatus } from './shared'

interface MeiroOverviewProps {
  meiroConfig?: MeiroConfig
  meiroMappingState?: MeiroMappingState
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  runMeiroPullPending: boolean
  importFromMeiroPending: boolean
  relativeTime: (iso?: string | null) => string
  setMeiroTab: (tab: 'cdp' | 'pipes' | 'normalization') => void
  onTestMeiro: () => void
  onRunMeiroPull: () => void
  onImportFromMeiro: () => void
}

export default function MeiroOverview({
  meiroConfig,
  meiroMappingState,
  meiroWebhookArchiveStatus,
  runMeiroPullPending,
  importFromMeiroPending,
  relativeTime,
  setMeiroTab,
  onTestMeiro,
  onRunMeiroPull,
  onImportFromMeiro,
}: MeiroOverviewProps) {
  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
        {[
          {
            label: 'CDP status',
            value: meiroConfig?.connected ? 'Connected' : 'Not connected',
            detail: `Last test ${relativeTime(meiroConfig?.last_test_at)}`,
          },
          {
            label: 'Pipes status',
            value: (meiroConfig?.webhook_received_count || 0) > 0 ? 'Receiving' : 'Waiting',
            detail: `Last payload ${relativeTime(meiroConfig?.webhook_last_received_at)}`,
          },
          {
            label: 'Mapping approval',
            value: meiroMappingState?.approval?.status || 'unreviewed',
            detail: `Version ${meiroMappingState?.version ?? 0}`,
          },
          {
            label: 'Replay archive',
            value: meiroWebhookArchiveStatus?.available ? `${meiroWebhookArchiveStatus.entries} batches` : 'Not available',
            detail: `Current source ${meiroConfig?.connected ? 'CDP + Pipes ready' : 'Pipes only'}`,
          },
        ].map((item) => (
          <div key={item.label} style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: 4 }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase', letterSpacing: 0.4 }}>{item.label}</div>
            <div style={{ fontSize: t.font.sizeMd, fontWeight: t.font.weightSemibold, color: t.color.text }}>{item.value}</div>
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{item.detail}</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: t.space.xs }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro CDP</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Audience API and pull-based export flow.</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Endpoint: <strong>{meiroConfig?.api_base_url || 'Not configured'}</strong>
          </div>
          <button type="button" onClick={() => setMeiroTab('cdp')} style={{ justifySelf: 'flex-start', border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            Configure CDP pull
          </button>
        </div>
        <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: t.space.xs }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro Pipes</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Webhook push from Pipes into stored payloads and archive.</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Received {Number(meiroConfig?.webhook_received_count || 0).toLocaleString()} payloads
          </div>
          <button type="button" onClick={() => setMeiroTab('pipes')} style={{ justifySelf: 'flex-start', border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            Configure Pipes webhook
          </button>
        </div>
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.md, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Quick actions</div>
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
          <button type="button" onClick={onTestMeiro} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Test CDP</button>
          <button type="button" onClick={onRunMeiroPull} style={{ border: `1px solid ${t.color.accent}`, background: '#fff', color: t.color.accent, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            {runMeiroPullPending ? 'Running…' : 'Run pull now'}
          </button>
          <button type="button" onClick={onImportFromMeiro} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            {importFromMeiroPending ? 'Importing…' : 'Import to attribution'}
          </button>
          <button type="button" onClick={() => setMeiroTab('normalization')} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
            Review normalization
          </button>
        </div>
      </div>
    </div>
  )
}
