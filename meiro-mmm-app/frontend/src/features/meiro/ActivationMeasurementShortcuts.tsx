import { useEffect, useState } from 'react'

import { apiGetJson, withQuery } from '../../lib/apiClient'
import { tokens as t } from '../../theme/tokens'

type ActivationMeasurementObject = {
  object_type?: string
  object_id?: string
  label?: string
  aliases?: string[]
  matched_touchpoints?: number
  matched_journeys?: number
  conversions?: number
  revenue?: number
}

type ActivationMeasurementShortcutsProps = {
  title?: string
  subtitle?: string
  limit?: number
  compact?: boolean
}

function activationMeasurementHref(item: ActivationMeasurementObject) {
  const params = new URLSearchParams({ page: 'meiro', meiro_tab: 'import' })
  if (item.object_type) params.set('activation_object_type', item.object_type)
  if (item.object_id) params.set('activation_object_id', item.object_id)
  return `/?${params.toString()}`
}

export default function ActivationMeasurementShortcuts({
  title = 'Activation measurement shortcuts',
  subtitle = 'Jump from source health into measured campaign, decision, asset, and offer evidence.',
  limit = 4,
  compact = false,
}: ActivationMeasurementShortcutsProps) {
  const [items, setItems] = useState<ActivationMeasurementObject[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const loadItems = async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await apiGetJson<{ items?: ActivationMeasurementObject[] }>(
        withQuery('/api/measurement/activation-objects', { limit }),
        { fallbackMessage: 'Failed to load activation measurement shortcuts' },
      )
      setItems(result.items || [])
    } catch (err) {
      setError((err as Error)?.message || 'Failed to load activation measurement shortcuts')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadItems()
  }, [limit])

  return (
    <div style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.md, padding: compact ? t.space.sm : t.space.md, display: 'grid', gap: t.space.sm, background: compact ? t.color.surface : t.color.bg }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: t.space.sm, alignItems: 'center', flexWrap: 'wrap' }}>
        <div>
          <div style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text }}>{title}</div>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>{subtitle}</div>
        </div>
        <button
          type="button"
          onClick={() => void loadItems()}
          disabled={loading}
          style={{ border: `1px solid ${t.color.border}`, background: t.color.surface, borderRadius: t.radius.sm, padding: '6px 9px', cursor: loading ? 'wait' : 'pointer', opacity: loading ? 0.7 : 1, fontSize: t.font.sizeXs }}
        >
          {loading ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>
      {error ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{error}</div>
      ) : items.length ? (
        <div style={{ display: 'grid', gridTemplateColumns: `repeat(auto-fit, minmax(${compact ? 170 : 210}px, 1fr))`, gap: t.space.sm }}>
          {items.map((item) => (
            <a
              key={`${item.object_type || 'object'}:${item.object_id || item.label || 'unknown'}`}
              href={activationMeasurementHref(item)}
              style={{ border: `1px solid ${t.color.borderLight}`, borderRadius: t.radius.sm, padding: t.space.sm, background: t.color.surface, display: 'grid', gap: 4, textDecoration: 'none' }}
            >
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textMuted, textTransform: 'uppercase' }}>{item.object_type || 'object'}</span>
              <span style={{ fontSize: t.font.sizeSm, fontWeight: t.font.weightSemibold, color: t.color.text, overflowWrap: 'anywhere' }}>{item.label || item.object_id || 'Unknown object'}</span>
              <span style={{ fontSize: t.font.sizeXs, color: t.color.textSecondary }}>
                {Number(item.matched_touchpoints || 0).toLocaleString()} touchpoints · {Number(item.conversions || 0).toLocaleString()} conversions
              </span>
              <span style={{ fontSize: t.font.sizeXs, color: t.color.accent, fontWeight: t.font.weightSemibold }}>Open evidence</span>
            </a>
          ))}
        </div>
      ) : loading ? (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>Loading activation measurement shortcuts...</div>
      ) : (
        <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>No measured activation objects are available yet. Import or replay Meiro/deciEngine activation events first.</div>
      )}
    </div>
  )
}
