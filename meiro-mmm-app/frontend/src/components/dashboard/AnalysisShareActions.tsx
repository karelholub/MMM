import { useState } from 'react'
import { tokens as t } from '../../theme/tokens'

interface AnalysisShareActionsProps {
  fileStem: string
  summaryTitle: string
  summaryLines: string[]
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
    <div style={{ display: 'flex', gap: t.space.sm, flexWrap: 'wrap', alignItems: 'center' }}>
      <button type="button" onClick={handleCopyLink} style={buttonStyle}>
        {copied ? 'Link copied' : 'Copy page link'}
      </button>
      <button type="button" onClick={handleExportBrief} style={buttonStyle}>
        Export brief
      </button>
    </div>
  )
}
