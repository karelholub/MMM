import { tokens as t } from '../../theme/tokens'
import type { MeiroConfig } from '../../connectors/meiroConnector'

type Props = {
  config?: Pick<MeiroConfig, 'target_instance_url' | 'target_instance_host' | 'cdp_instance_scope'> | null
  compact?: boolean
  showWarning?: boolean
}

export default function MeiroTargetInstanceBadge({ config, compact = false, showWarning = true }: Props) {
  const targetUrl = config?.target_instance_url || 'https://meiro-internal.eu.pipes.meiro.io'
  const targetHost = config?.target_instance_host || 'meiro-internal.eu.pipes.meiro.io'
  const scope = config?.cdp_instance_scope
  const outOfScope = scope?.status === 'out_of_scope'

  return (
    <div style={{ display: 'grid', gap: compact ? 4 : t.space.sm }}>
      <div
        style={{
          display: 'inline-flex',
          width: 'fit-content',
          maxWidth: '100%',
          alignItems: 'center',
          gap: t.space.xs,
          border: `1px solid ${outOfScope ? t.color.warning : t.color.borderLight}`,
          borderRadius: t.radius.full,
          background: outOfScope ? t.color.warningMuted : t.color.bg,
          color: t.color.textSecondary,
          padding: compact ? '3px 8px' : '5px 9px',
          fontSize: t.font.sizeXs,
          overflowWrap: 'anywhere',
        }}
        title={targetUrl}
      >
        Target Pipes instance: <strong style={{ color: t.color.text }}>{targetHost}</strong>
      </div>
      {showWarning && outOfScope ? (
        <div
          style={{
            border: `1px solid ${t.color.warning}`,
            background: t.color.warningMuted,
            color: t.color.text,
            borderRadius: t.radius.md,
            padding: compact ? t.space.sm : t.space.md,
            fontSize: t.font.sizeSm,
          }}
        >
          <strong>Out-of-scope Meiro instance ignored:</strong>{' '}
          <code>{scope?.configured_host || scope?.configured_url || 'unknown'}</code> does not match <code>{targetUrl}</code>.
          Measurement should use the target Pipes webhooks/registry; CDP/profile data from the other host is enrichment only.
        </div>
      ) : null}
    </div>
  )
}
