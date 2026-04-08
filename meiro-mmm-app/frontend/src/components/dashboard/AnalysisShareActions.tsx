import { useEffect, useState } from 'react'
import { tokens as t } from '../../theme/tokens'

interface AnalysisShareActionsProps {
  fileStem: string
  summaryTitle: string
  summaryLines: string[]
}

interface SavedAnalysisPreset {
  id: string
  name: string
  url: string
  saved_at: string
}

function sanitizeFileStem(value: string): string {
  return value.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '') || 'analysis-summary'
}

export default function AnalysisShareActions({
  fileStem,
  summaryTitle,
  summaryLines,
}: AnalysisShareActionsProps) {
  const [copied, setCopied] = useState(false)
  const [savedPresets, setSavedPresets] = useState<SavedAnalysisPreset[]>(() => {
    if (typeof window === 'undefined') return []
    try {
      const raw = window.localStorage.getItem(`analysis-presets:${sanitizeFileStem(fileStem)}`)
      if (!raw) return []
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  })

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      window.localStorage.setItem(
        `analysis-presets:${sanitizeFileStem(fileStem)}`,
        JSON.stringify(savedPresets.slice(0, 8)),
      )
    } catch {
      // Ignore persistence errors and keep reporting actions usable.
    }
  }, [fileStem, savedPresets])

  const handleCopyLink = async () => {
    if (typeof window === 'undefined') return
    try {
      await navigator.clipboard.writeText(window.location.href)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch {
      setCopied(false)
    }
  }

  const handleExportBrief = () => {
    if (typeof window === 'undefined') return
    const now = new Date()
    const content = [
      summaryTitle,
      `Generated: ${now.toLocaleString()}`,
      `Link: ${window.location.href}`,
      '',
      ...summaryLines.filter((line) => line && line.trim().length > 0),
      '',
    ].join('\n')
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${sanitizeFileStem(fileStem)}-${now.toISOString().slice(0, 10)}.txt`
    link.click()
    URL.revokeObjectURL(url)
  }

  const handleSavePreset = () => {
    if (typeof window === 'undefined') return
    const suggestedName = `${summaryTitle} · ${new Date().toLocaleDateString()}`
    const name = window.prompt('Save this analysis view as:', suggestedName)?.trim()
    if (!name) return
    const nextPreset: SavedAnalysisPreset = {
      id: `${Date.now()}`,
      name,
      url: window.location.href,
      saved_at: new Date().toISOString(),
    }
    setSavedPresets((current) => {
      const withoutSameUrl = current.filter((item) => item.url !== nextPreset.url)
      return [nextPreset, ...withoutSameUrl].slice(0, 8)
    })
  }

  const handleRemovePreset = (presetId: string) => {
    setSavedPresets((current) => current.filter((item) => item.id !== presetId))
  }

  const buttonStyle: React.CSSProperties = {
    padding: `${t.space.sm}px ${t.space.md}px`,
    borderRadius: t.radius.sm,
    border: `1px solid ${t.color.border}`,
    background: t.color.surface,
    color: t.color.text,
    fontSize: t.font.sizeSm,
    fontWeight: t.font.weightMedium,
    cursor: 'pointer',
  }

  return (
    <div style={{ display: 'grid', gap: t.space.sm }}>
      <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
        <button type="button" onClick={handleCopyLink} style={buttonStyle}>
          {copied ? 'Link copied' : 'Copy page link'}
        </button>
        <button type="button" onClick={handleExportBrief} style={buttonStyle}>
          Export brief
        </button>
        <button type="button" onClick={handleSavePreset} style={buttonStyle}>
          Save preset
        </button>
      </div>
      {savedPresets.length ? (
        <div style={{ display: 'grid', gap: 6 }}>
          <div style={{ fontSize: t.font.sizeXs, color: t.color.textMuted }}>Saved presets</div>
          <div style={{ display: 'flex', gap: t.space.xs, flexWrap: 'wrap' }}>
            {savedPresets.map((preset) => (
              <div
                key={preset.id}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  border: `1px solid ${t.color.borderLight}`,
                  borderRadius: t.radius.full,
                  background: t.color.bgSubtle,
                  padding: '6px 10px',
                  maxWidth: '100%',
                }}
              >
                <a
                  href={preset.url}
                  style={{
                    color: t.color.text,
                    textDecoration: 'none',
                    fontSize: t.font.sizeSm,
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    maxWidth: 220,
                  }}
                  title={`${preset.name} · ${new Date(preset.saved_at).toLocaleString()}`}
                >
                  {preset.name}
                </a>
                <button
                  type="button"
                  onClick={() => handleRemovePreset(preset.id)}
                  style={{
                    border: 'none',
                    background: 'transparent',
                    color: t.color.textMuted,
                    cursor: 'pointer',
                    fontSize: t.font.sizeSm,
                    padding: 0,
                  }}
                  aria-label={`Remove preset ${preset.name}`}
                  title="Remove preset"
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  )
}
