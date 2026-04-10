import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { tokens as t } from '../../theme/tokens'
import {
  type AdsActionType,
  type AdsEntityType,
  type AdsProviderKey,
  applyAdsChangeRequest,
  approveAdsChangeRequest,
  createAdsChangeRequest,
  getAdsState,
} from '../../connectors/adsManagerConnector'
import { buildJourneyHypothesisHref } from '../../lib/journeyLinks'

interface AdsActionsDrawerProps {
  open: boolean
  onClose: () => void
  provider: AdsProviderKey
  accountId: string
  entityType: AdsEntityType
  entityId: string
  entityName?: string | null
  previewMetrics?: {
    spend7d?: number | null
    conversions7d?: number | null
    revenue7d?: number | null
    roas?: number | null
    cpa?: number | null
  } | null
  decisionContext?: {
    source: 'performance_recommendation' | 'deployed_journey_policy'
    scope_label?: string | null
    recommended_channel?: string | null
    recommended_campaign?: string | null
    conversion_rate?: number | null
    journey_count?: number | null
    avg_value?: number | null
    policy_title?: string | null
    hypothesis_id?: string | null
    journey_definition_id?: string | null
  } | null
}

function formatMoney(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—'
  return v.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })
}

function formatNumber(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—'
  return v.toLocaleString(undefined, { maximumFractionDigits: 1 })
}

export default function AdsActionsDrawer({
  open,
  onClose,
  provider,
  accountId,
  entityType,
  entityId,
  entityName,
  previewMetrics,
  decisionContext,
}: AdsActionsDrawerProps) {
  const queryClient = useQueryClient()
  const [actionType, setActionType] = useState<AdsActionType>('pause')
  const [budgetInput, setBudgetInput] = useState('')
  const [latestRequest, setLatestRequest] = useState<any | null>(null)

  const stateQuery = useQuery({
    queryKey: ['ads-state', provider, accountId, entityType, entityId],
    queryFn: () => getAdsState({ provider, accountId, entityType, entityId }),
    enabled: open,
    staleTime: 10_000,
  })

  const createMutation = useMutation({
    mutationFn: () => {
      const payload: Record<string, unknown> = actionType === 'update_budget'
        ? { daily_budget: Number(budgetInput || 0), currency: 'USD' }
        : {}
      if (decisionContext) {
        payload.decision_context = decisionContext
      }
      return createAdsChangeRequest({
        provider,
        accountId,
        entityType,
        entityId,
        actionType,
        actionPayload: payload,
      })
    },
    onSuccess: (res) => {
      setLatestRequest(res)
      void queryClient.invalidateQueries({ queryKey: ['ads-state', provider, accountId, entityType, entityId] })
    },
  })

  const approveMutation = useMutation({
    mutationFn: (id: string) => approveAdsChangeRequest(id),
    onSuccess: (res) => setLatestRequest(res),
  })

  const applyMutation = useMutation({
    mutationFn: (id: string) => applyAdsChangeRequest(id),
    onSuccess: (res) => {
      setLatestRequest(res)
      void queryClient.invalidateQueries({ queryKey: ['ads-state', provider, accountId, entityType, entityId] })
    },
  })

  const canPropose = useMemo(() => {
    if (actionType !== 'update_budget') return true
    const value = Number(budgetInput)
    return Number.isFinite(value) && value > 0
  }, [actionType, budgetInput])

  if (!open) return null

  const state = stateQuery.data
  const isBusy = createMutation.isPending || approveMutation.isPending || applyMutation.isPending
  const requestDecisionContext = (latestRequest?.action_payload?.decision_context as AdsActionsDrawerProps['decisionContext']) || decisionContext
  const recommendedTargetLabel = requestDecisionContext?.recommended_campaign
    ? `${requestDecisionContext.recommended_channel || 'Unknown'} / ${requestDecisionContext.recommended_campaign}`
    : requestDecisionContext?.recommended_channel || null
  const policyHref = buildJourneyHypothesisHref({
    journeyDefinitionId: requestDecisionContext?.journey_definition_id,
    hypothesisId: requestDecisionContext?.hypothesis_id,
  })

  return (
    <>
      <div
        role="presentation"
        onClick={onClose}
        style={{
          position: 'fixed',
          inset: 0,
          zIndex: 48,
          background: 'rgba(15,23,42,0.4)',
        }}
      />
      <aside
        role="dialog"
        aria-label="Ads actions"
        style={{
          position: 'fixed',
          top: 0,
          right: 0,
          width: 'min(480px, 100vw)',
          height: '100vh',
          zIndex: 49,
          background: t.color.surface,
          borderLeft: `1px solid ${t.color.borderLight}`,
          boxShadow: t.shadow,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        <div
          style={{
            padding: t.space.lg,
            borderBottom: `1px solid ${t.color.borderLight}`,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <div>
            <div style={{ fontSize: t.font.sizeLg, fontWeight: t.font.weightSemibold, color: t.color.text }}>Ads Actions</div>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary, marginTop: 2 }}>
              {provider} • {entityType} • {entityName || entityId}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            style={{ border: 'none', background: 'transparent', fontSize: t.font.sizeLg, cursor: 'pointer', color: t.color.textSecondary }}
            aria-label="Close"
          >
            ×
          </button>
        </div>

        <div style={{ padding: t.space.lg, display: 'grid', gap: t.space.lg, overflowY: 'auto' }}>
          <section style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
            <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, marginBottom: t.space.xs }}>Current provider state</div>
            {stateQuery.isLoading && <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading…</div>}
            {stateQuery.isError && <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{(stateQuery.error as Error).message}</div>}
            {state && (
              <div style={{ display: 'grid', gap: 4, fontSize: t.font.sizeSm }}>
                <div>Status: <strong>{state.status || 'unknown'}</strong></div>
                <div>Budget: <strong>{state.budget != null ? formatMoney(state.budget) : '—'}</strong></div>
                <div>Needs re-auth: <strong>{state.needs_reauth ? 'Yes' : 'No'}</strong></div>
                {state.deep_link && (
                  <a href={state.deep_link} target="_blank" rel="noreferrer" style={{ color: t.color.accent, textDecoration: 'none', fontWeight: t.font.weightMedium }}>
                    Open in provider ↗
                  </a>
                )}
              </div>
            )}
          </section>

          {requestDecisionContext && (
            <section style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>
                Decision context
              </div>
              <div style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
                <div>
                  Source:{' '}
                  <strong style={{ color: t.color.text }}>
                    {requestDecisionContext.source === 'deployed_journey_policy' ? 'Deployed Journey Lab policy' : 'Performance recommendation'}
                  </strong>
                </div>
                {requestDecisionContext.policy_title ? (
                  <div>
                    Policy: <strong style={{ color: t.color.text }}>{requestDecisionContext.policy_title}</strong>
                  </div>
                ) : null}
                {requestDecisionContext.scope_label ? (
                  <div>
                    Current entity context: <strong style={{ color: t.color.text }}>{requestDecisionContext.scope_label}</strong>
                  </div>
                ) : null}
                {recommendedTargetLabel ? (
                  <div>
                    Suggested next step: <strong style={{ color: t.color.text }}>{recommendedTargetLabel}</strong>
                  </div>
                ) : null}
                <div style={{ display: 'flex', gap: t.space.md, flexWrap: 'wrap' }}>
                  {requestDecisionContext.journey_count != null ? (
                    <span>Journeys: <strong style={{ color: t.color.text }}>{formatNumber(requestDecisionContext.journey_count)}</strong></span>
                  ) : null}
                  {requestDecisionContext.conversion_rate != null ? (
                    <span>CVR: <strong style={{ color: t.color.text }}>{(requestDecisionContext.conversion_rate * 100).toFixed(1)}%</strong></span>
                  ) : null}
                  {requestDecisionContext.avg_value != null ? (
                    <span>Avg value: <strong style={{ color: t.color.text }}>{formatMoney(requestDecisionContext.avg_value)}</strong></span>
                  ) : null}
                </div>
                {requestDecisionContext.hypothesis_id ? (
                  <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap', alignItems: 'center', fontSize: t.font.sizeXs, color: t.color.textMuted }}>
                    <span>Hypothesis: {requestDecisionContext.hypothesis_id}</span>
                    {policyHref ? (
                      <a href={policyHref} style={{ color: t.color.accent, textDecoration: 'none' }}>
                        Open in Journey Lab
                      </a>
                    ) : null}
                  </div>
                ) : null}
              </div>
            </section>
          )}

          <section style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.surface }}>
            <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.sm }}>Propose change</div>
            <div style={{ display: 'grid', gap: t.space.sm }}>
              <label style={{ display: 'grid', gap: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                Action
                <select
                  value={actionType}
                  onChange={(e) => setActionType(e.target.value as AdsActionType)}
                  style={{ padding: `${t.space.xs}px ${t.space.sm}px`, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, fontSize: t.font.sizeSm }}
                >
                  <option value="pause">Pause</option>
                  <option value="enable">Enable</option>
                  <option value="update_budget">Update daily budget</option>
                </select>
              </label>

              {actionType === 'update_budget' && (
                <label style={{ display: 'grid', gap: 4, fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                  New daily budget (USD)
                  <input
                    type="number"
                    min={0}
                    step={1}
                    value={budgetInput}
                    onChange={(e) => setBudgetInput(e.target.value)}
                    style={{ padding: `${t.space.xs}px ${t.space.sm}px`, border: `1px solid ${t.color.border}`, borderRadius: t.radius.sm, fontSize: t.font.sizeSm }}
                  />
                </label>
              )}

              <div style={{
                border: `1px dashed ${t.color.border}`,
                background: t.color.bg,
                borderRadius: t.radius.sm,
                padding: t.space.sm,
                fontSize: t.font.sizeXs,
                color: t.color.textSecondary,
                display: 'grid',
                gap: 4,
              }}>
                <div style={{ fontWeight: t.font.weightSemibold, color: t.color.text }}>Impact preview (last 7d)</div>
                <div>Spend: {formatMoney(previewMetrics?.spend7d ?? null)}</div>
                <div>Conversions: {formatNumber(previewMetrics?.conversions7d ?? null)}</div>
                <div>Revenue: {formatMoney(previewMetrics?.revenue7d ?? null)}</div>
                <div>ROAS / CPA: {formatNumber(previewMetrics?.roas ?? null)} / {formatMoney(previewMetrics?.cpa ?? null)}</div>
              </div>

              <button
                type="button"
                disabled={!canPropose || isBusy}
                onClick={() => createMutation.mutate()}
                style={{
                  padding: `${t.space.sm}px ${t.space.md}px`,
                  border: 'none',
                  borderRadius: t.radius.sm,
                  background: canPropose ? t.color.accent : t.color.textMuted,
                  color: '#fff',
                  cursor: canPropose ? 'pointer' : 'not-allowed',
                  fontWeight: t.font.weightSemibold,
                }}
              >
                {isBusy ? 'Submitting…' : 'Submit proposal'}
              </button>
              {createMutation.isError && <div style={{ fontSize: t.font.sizeXs, color: t.color.danger }}>{(createMutation.error as Error).message}</div>}
            </div>
          </section>

          {latestRequest && (
            <section style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: t.space.md, background: t.color.bg }}>
              <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, marginBottom: t.space.xs }}>Latest request</div>
              <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary, marginBottom: t.space.sm }}>
                Status: <strong style={{ color: t.color.text }}>{latestRequest.status}</strong>
                {latestRequest.error_message ? ` • ${latestRequest.error_message}` : ''}
              </div>
              <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap' }}>
                {latestRequest.status === 'pending_approval' && (
                  <button
                    type="button"
                    onClick={() => approveMutation.mutate(latestRequest.id)}
                    disabled={approveMutation.isPending || applyMutation.isPending}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.color.border}`,
                      background: t.color.surface,
                      color: t.color.text,
                      cursor: 'pointer',
                    }}
                  >
                    Approve
                  </button>
                )}
                {(latestRequest.status === 'approved' || latestRequest.status === 'pending_approval') && (
                  <button
                    type="button"
                    onClick={() => applyMutation.mutate(latestRequest.id)}
                    disabled={approveMutation.isPending || applyMutation.isPending}
                    style={{
                      padding: `${t.space.xs}px ${t.space.sm}px`,
                      borderRadius: t.radius.sm,
                      border: 'none',
                      background: t.color.success,
                      color: '#fff',
                      cursor: 'pointer',
                    }}
                  >
                    Apply now
                  </button>
                )}
              </div>
              {(approveMutation.isError || applyMutation.isError) && (
                <div style={{ marginTop: t.space.xs, fontSize: t.font.sizeXs, color: t.color.danger }}>
                  {((approveMutation.error || applyMutation.error) as Error)?.message}
                </div>
              )}
            </section>
          )}
        </div>
      </aside>
    </>
  )
}
