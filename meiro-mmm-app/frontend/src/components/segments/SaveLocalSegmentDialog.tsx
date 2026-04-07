import { useEffect, useState } from 'react'
import { tokens as t } from '../../theme/tokens'

interface SaveLocalSegmentDialogProps {
  open: boolean
  initialName: string
  criteriaLabel: string
  error?: string | null
  saving?: boolean
  onClose: () => void
  onSubmit: (payload: { name: string; description: string }) => void
}

export default function SaveLocalSegmentDialog({
  open,
  initialName,
  criteriaLabel,
  error,
  saving = false,
  onClose,
  onSubmit,
}: SaveLocalSegmentDialogProps) {
  const [name, setName] = useState(initialName)
  const [description, setDescription] = useState('')

  useEffect(() => {
    if (!open) return
    setName(initialName)
    setDescription('')
  }, [initialName, open])

  if (!open) return null

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(15, 23, 42, 0.38)',
        display: 'grid',
        placeItems: 'center',
        zIndex: 70,
        padding: t.space.lg,
      }}
    >
      <div
        role="dialog"
        aria-label="Save local analytical segment"
        style={{
          width: 'min(520px, 100%)',
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
          <h3 style={{ margin: 0, fontSize: t.font.sizeLg, color: t.color.text }}>Save local analytical segment</h3>
          <div style={{ fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
            This saved segment will be analysis-ready in Journeys and Conversion Paths, and reusable in hypotheses and experiments.
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
          Current slice: <strong style={{ color: t.color.text }}>{criteriaLabel || 'No criteria'}</strong>
        </div>

        <label style={{ display: 'grid', gap: 6, fontSize: t.font.sizeSm, color: t.color.textSecondary }}>
          Segment name
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="e.g. Paid search mobile CZ"
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
            rows={3}
            placeholder="Optional note about when to use this slice."
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
            disabled={saving || !name.trim()}
            onClick={() => onSubmit({ name: name.trim(), description: description.trim() })}
            style={{
              border: `1px solid ${t.color.accent}`,
              background: t.color.accent,
              color: t.color.surface,
              borderRadius: t.radius.sm,
              padding: '8px 12px',
              cursor: saving ? 'default' : 'pointer',
              opacity: saving || !name.trim() ? 0.7 : 1,
            }}
          >
            {saving ? 'Saving…' : 'Save segment'}
          </button>
        </div>
      </div>
    </div>
  )
}
