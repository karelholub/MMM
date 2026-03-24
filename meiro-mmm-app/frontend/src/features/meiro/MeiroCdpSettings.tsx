import type { Dispatch, SetStateAction } from 'react'

import type { MeiroConfig, MeiroPullConfig, MeiroWebhookSuggestions } from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import { DEFAULT_MEIRO_PULL_CONFIG } from './shared'

interface MeiroCdpSettingsProps {
  meiroUrl: string
  setMeiroUrl: Dispatch<SetStateAction<string>>
  meiroKey: string
  setMeiroKey: Dispatch<SetStateAction<string>>
  meiroPullDraft: MeiroPullConfig
  setMeiroPullDraft: Dispatch<SetStateAction<MeiroPullConfig>>
  meiroConfig?: MeiroConfig
  meiroWebhookSuggestions?: MeiroWebhookSuggestions
  meiroWebhookSuggestionsLoading: boolean
  meiroWebhookSuggestionsError?: string | null
  testMeiroResult?: { message?: string } | null
  saveMeiroPullPending: boolean
  runMeiroPullPending: boolean
  relativeTime: (iso?: string | null) => string
  onTestMeiro: () => void
  onConnectMeiro: () => void
  onDisconnectMeiro: () => void
  onSaveMeiroPull: () => void
  onRunMeiroPull: () => void
}

export default function MeiroCdpSettings({
  meiroUrl,
  setMeiroUrl,
  meiroKey,
  setMeiroKey,
  meiroPullDraft,
  setMeiroPullDraft,
  meiroConfig,
  meiroWebhookSuggestions,
  meiroWebhookSuggestionsLoading,
  meiroWebhookSuggestionsError,
  testMeiroResult,
  saveMeiroPullPending,
  runMeiroPullPending,
  relativeTime,
  onTestMeiro,
  onConnectMeiro,
  onDisconnectMeiro,
  onSaveMeiroPull,
  onRunMeiroPull,
}: MeiroCdpSettingsProps) {
  return (
    <div style={{ display: 'grid', gap: t.space.md }}>
      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, background: t.color.bg, padding: t.space.sm, display: 'grid', gap: t.space.sm }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Meiro CDP connection</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Use this when Meiro CDP is the source of truth and you want audience API access plus pull-based journey imports.</div>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
          API base URL
          <input value={meiroUrl || meiroConfig?.api_base_url || ''} onChange={(e) => setMeiroUrl(e.target.value)} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
          API key
          <input type="password" value={meiroKey} onChange={(e) => setMeiroKey(e.target.value)} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
          <button type="button" onClick={onTestMeiro} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Test</button>
          <button type="button" onClick={onConnectMeiro} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Save</button>
          <button type="button" onClick={onDisconnectMeiro} style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>Disconnect</button>
        </div>
        <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>
          Status: {meiroConfig?.connected ? 'Connected' : 'Not connected'} · Last test: {relativeTime(meiroConfig?.last_test_at)}
        </div>
        {testMeiroResult && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>{testMeiroResult.message || 'Test completed'}</div>}
      </div>

      <div style={{ display: 'grid', gap: 6 }}>
        <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Pull window</div>
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          These settings control how raw Meiro event exports are grouped into journeys before import.
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
          Lookback days
          <input type="number" min={1} max={365} value={meiroPullDraft.lookback_days} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, lookback_days: Number(e.target.value || 30) }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
          Session gap (minutes)
          <input type="number" min={1} max={720} value={meiroPullDraft.session_gap_minutes} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, session_gap_minutes: Number(e.target.value || 30) }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
          Conversion event
          <input value={meiroPullDraft.conversion_selector} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, conversion_selector: e.target.value }))} placeholder="purchase" style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
        </label>
      </div>

      <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.sm, background: t.color.bg }}>
        <div style={{ display: 'grid', gap: 6 }}>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>Deduplication & identity</div>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Use observed webhook payloads to choose the best conversion identifier, then tune how aggressively repeated touches are collapsed in the pull path.
          </div>
        </div>

        <div style={{ marginTop: t.space.sm, display: 'grid', gap: t.space.sm }}>
          {meiroWebhookSuggestionsLoading ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading dedup hints…</div>
          ) : meiroWebhookSuggestionsError ? (
            <div style={{ fontSize: t.font.sizeSm, color: t.color.warning }}>{meiroWebhookSuggestionsError}</div>
          ) : (
            <>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: t.space.sm, flexWrap: 'wrap' }}>
                <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                  Recommended dedup key: <strong style={{ color: t.color.text }}>{meiroWebhookSuggestions?.dedup_key_suggestion || 'auto'}</strong> from {Number(meiroWebhookSuggestions?.total_conversions_observed || 0).toLocaleString()} observed conversions
                </div>
                <button
                  type="button"
                  onClick={() => {
                    const suggestion = meiroWebhookSuggestions?.dedup_key_suggestion
                    const nextPrimary = suggestion && ['conversion_id', 'order_id', 'event_id'].includes(suggestion)
                      ? suggestion as MeiroPullConfig['primary_dedup_key']
                      : 'auto'
                    const fallback = ['conversion_id', 'order_id', 'event_id'].filter((key) => key !== nextPrimary) as MeiroPullConfig['fallback_dedup_keys']
                    setMeiroPullDraft((prev) => ({
                      ...prev,
                      primary_dedup_key: nextPrimary,
                      fallback_dedup_keys: fallback.length ? fallback : DEFAULT_MEIRO_PULL_CONFIG.fallback_dedup_keys,
                    }))
                  }}
                  style={{ border: `1px solid ${t.color.accent}`, background: '#fff', color: t.color.accent, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}
                >
                  Apply recommendation
                </button>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: t.space.sm }}>
                {(meiroWebhookSuggestions?.dedup_key_candidates || []).map((candidate) => (
                  <div key={candidate.key} style={{ border: `1px solid ${candidate.recommended ? t.color.accent : t.color.borderLight}`, background: candidate.recommended ? t.color.accentMuted : '#fff', borderRadius: t.radius.sm, padding: t.space.sm, display: 'grid', gap: 4 }}>
                    <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{candidate.key}</div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Coverage {candidate.coverage_pct.toFixed(1)}%</div>
                    <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>Seen {Number(candidate.count || 0).toLocaleString()} times</div>
                  </div>
                ))}
              </div>
            </>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: t.space.sm }}>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Primary dedup key
              <select value={meiroPullDraft.primary_dedup_key} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, primary_dedup_key: e.target.value as MeiroPullConfig['primary_dedup_key'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}>
                <option value="auto">Auto (recommended)</option>
                <option value="conversion_id">conversion_id</option>
                <option value="order_id">order_id</option>
                <option value="event_id">event_id</option>
              </select>
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Dedup mode
              <select value={meiroPullDraft.dedup_mode} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, dedup_mode: e.target.value as MeiroPullConfig['dedup_mode'] }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}`, background: '#fff' }}>
                <option value="strict">Strict: same channel and same raw event fingerprint</option>
                <option value="balanced">Balanced: same channel within the dedup window</option>
                <option value="aggressive">Aggressive: same channel or same source/medium/campaign cluster</option>
              </select>
            </label>
            <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm }}>
              Duplicate collapse window (minutes)
              <input type="number" min={0} max={1440} value={meiroPullDraft.dedup_interval_minutes} onChange={(e) => setMeiroPullDraft((prev) => ({ ...prev, dedup_interval_minutes: Number(e.target.value || 0) }))} style={{ padding: '8px 10px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }} />
            </label>
          </div>

          <div style={{ display: 'grid', gap: 6 }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightMedium, color: t.color.text }}>Fallback keys</div>
            <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
              {(['conversion_id', 'order_id', 'event_id'] as const).map((key) => {
                const checked = meiroPullDraft.fallback_dedup_keys.includes(key)
                const disabled = meiroPullDraft.primary_dedup_key === key
                return (
                  <label key={key} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: t.font.sizeSm, color: disabled ? t.color.textMuted : t.color.text }}>
                    <input
                      type="checkbox"
                      checked={checked || disabled}
                      disabled={disabled}
                      onChange={(e) => {
                        setMeiroPullDraft((prev) => ({
                          ...prev,
                          fallback_dedup_keys: e.target.checked
                            ? [...prev.fallback_dedup_keys, key].filter((value, index, values) => values.indexOf(value) === index)
                            : prev.fallback_dedup_keys.filter((value) => value !== key),
                        }))
                      }}
                    />
                    {key}
                  </label>
                )
              })}
            </div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
              Duplicate-ID alerts in Data Quality still reflect repeated journey/profile identifiers. These settings control Meiro import grouping and store the preferred conversion key for replay and normalization.
            </div>
          </div>
        </div>
      </div>

      <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
        <button type="button" onClick={onSaveMeiroPull} disabled={saveMeiroPullPending} style={{ border: `1px solid ${t.color.accent}`, background: t.color.accent, color: '#fff', borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer', opacity: saveMeiroPullPending ? 0.7 : 1 }}>
          {saveMeiroPullPending ? 'Saving…' : 'Save pull settings'}
        </button>
        <button type="button" onClick={onRunMeiroPull} style={{ border: `1px solid ${t.color.accent}`, background: '#fff', color: t.color.accent, borderRadius: t.radius.sm, padding: '8px 10px', cursor: 'pointer' }}>
          {runMeiroPullPending ? 'Running…' : 'Run pull now'}
        </button>
      </div>
    </div>
  )
}
