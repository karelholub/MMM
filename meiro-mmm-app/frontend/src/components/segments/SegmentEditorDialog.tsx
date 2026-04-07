import { useEffect, useState } from 'react'
import { tokens as t } from '../../theme/tokens'
import type { SegmentContextResponse, SegmentRegistryItem } from '../../lib/segments'

interface SegmentEditorDialogProps {
  open: boolean
  mode: 'create' | 'edit'
  item?: SegmentRegistryItem | null
  context?: SegmentContextResponse | null
  error?: string | null
  submitting?: boolean
  onClose: () => void
  onSubmit: (payload: {
    name: string
    description: string
    definition: Record<string, string>
  }) => void
}

function readField(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function renderOptions(
  label: string,
  values: Array<{ value: string; count: number }>,
) {
  return (
    <>
      <option value="">Any {label.toLowerCase()}</option>
      {values.map((item) => (
        <option key={item.value} value={item.value}>
          {item.value} ({item.count.toLocaleString()})
        </option>
      ))}
    </>
  )
}

export default function SegmentEditorDialog({
  open,
  mode,
  item,
  context,
  error,
  submitting = false,
  onClose,
  onSubmit,
}: SegmentEditorDialogProps) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [channel, setChannel] = useState('')
  const [campaign, setCampaign] = useState('')
  const [device, setDevice] = useState('')
  const [country, setCountry] = useState('')

  useEffect(() => {
    if (!open) return
    setName(item?.name || '')
    setDescription(item?.description || '')
    setChannel(readField(item?.definition?.channel_group))
    setCampaign(readField(item?.definition?.campaign_id))
    setDevice(readField(item?.definition?.device))
    setCountry(readField(item?.definition?.country))
  }, [item, open])

  if (!open) return null

  const definition = {
    ...(channel ? { channel_group: channel } : {}),
    ...(campaign ? { campaign_id: campaign } : {}),
    ...(device ? { device } : {}),
    ...(country ? { country } : {}),
  }
  const canSubmit = !!name.trim() && Object.keys(definition).length > 0

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(15, 23, 42, 0.38)',
        display: 'grid',
        placeItems: 'center',
        zIndex: 72,
        padding: t.space.lg,
      }}
    >
      <div
        role="dialog"
        aria-label={mode === 'create' ? 'Create analytical segment' : 'Edit analytical segment'}
        style={{
          width: 'min(680px, 100%)',
          background: t.color.surface,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.lg,
          boxShadow: t.shadow,
          padding: t.space.lg,
          display: 'grid',
          gap: t.space.md,
        }}
      >
        <div style={{ display: 'grid', gap: 4 }}>
          <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>
            {mode === 'create' ? 'Create analytical segment' : 'Edit analytical segment'}
          </h3>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Define a reusable analytical slice from observed workspace journey dimensions. Meiro Pipes segments remain operational audiences and are not edited here.
          </div>
        </div>

        <div
          style={{
            border: `1px solid ${t.color.borderLight}`,
            borderRadius: t.radius.sm,
            background: t.color.bg,
            padding: t.space.sm,
            fontSize: t.font.sizeSm,
            color: t.color.textSecondary,
          }}
        >
          Observed workspace rows: <strong style={{ color: t.color.text }}>{context?.summary.journey_rows?.toLocaleString() ?? '—'}</strong>
          {context?.summary.date_from && context?.summary.date_to
            ? ` · ${context.summary.date_from} – ${context.summary.date_to}`
            : ''}
        </div>

        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Segment name
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            style={{
              width: '100%',
              padding: '10px 12px',
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              fontSize: t.font.sizeSm,
            }}
          />
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Description
          <textarea
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            rows={2}
            style={{
              width: '100%',
              padding: '10px 12px',
              borderRadius: t.radius.sm,
              border: `1px solid ${t.color.border}`,
              fontSize: t.font.sizeSm,
              resize: 'vertical',
            }}
          />
        </label>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(220px, 100%), 1fr))', gap: t.space.md }}>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Channel group
            <select value={channel} onChange={(event) => setChannel(event.target.value)} style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
              {renderOptions('channel', context?.channels ?? [])}
            </select>
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Campaign
            <select value={campaign} onChange={(event) => setCampaign(event.target.value)} style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
              {renderOptions('campaign', context?.campaigns ?? [])}
            </select>
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Device
            <select value={device} onChange={(event) => setDevice(event.target.value)} style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
              {renderOptions('device', context?.devices ?? [])}
            </select>
          </label>
          <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            Country
            <select value={country} onChange={(event) => setCountry(event.target.value)} style={{ padding: '10px 12px', borderRadius: t.radius.sm, border: `1px solid ${t.color.border}` }}>
              {renderOptions('country', context?.countries ?? [])}
            </select>
          </label>
        </div>

        {error ? <div style={{ fontSize: t.font.sizeSm, color: t.color.danger }}>{error}</div> : null}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: t.space.sm }}>
          <button
            type="button"
            onClick={onClose}
            style={{
              border: `1px solid ${t.color.border}`,
              background: 'transparent',
              borderRadius: t.radius.sm,
              padding: '8px 12px',
              cursor: 'pointer',
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={submitting || !canSubmit}
            onClick={() => onSubmit({ name: name.trim(), description: description.trim(), definition })}
            style={{
              border: `1px solid ${t.color.accent}`,
              background: t.color.accent,
              color: t.color.surface,
              borderRadius: t.radius.sm,
              padding: '8px 12px',
              cursor: submitting ? 'default' : 'pointer',
              opacity: submitting || !canSubmit ? 0.7 : 1,
            }}
          >
            {submitting ? (mode === 'create' ? 'Creating…' : 'Saving…') : mode === 'create' ? 'Create segment' : 'Save changes'}
          </button>
        </div>
      </div>
    </div>
  )
}
