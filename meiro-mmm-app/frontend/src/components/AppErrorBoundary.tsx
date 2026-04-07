import React from 'react'
import { tokens } from '../theme/tokens'

interface AppErrorBoundaryProps {
  areaLabel: string
  children: React.ReactNode
}

interface AppErrorBoundaryState {
  hasError: boolean
  errorMessage: string | null
}

export default class AppErrorBoundary extends React.Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
  constructor(props: AppErrorBoundaryProps) {
    super(props)
    this.state = { hasError: false, errorMessage: null }
  }

  static getDerivedStateFromError(error: unknown): AppErrorBoundaryState {
    return {
      hasError: true,
      errorMessage: error instanceof Error ? error.message : 'Unknown render error',
    }
  }

  componentDidCatch(error: unknown) {
    console.error('App render error:', error)
  }

  render() {
    if (!this.state.hasError) return this.props.children
    return (
      <div
        style={{
          border: `1px solid ${tokens.color.border}`,
          background: tokens.color.surface,
          borderRadius: tokens.radius.lg,
          padding: 24,
          display: 'grid',
          gap: 12,
          boxShadow: tokens.shadow,
        }}
      >
        <div style={{ fontSize: tokens.font.sizeLg, fontWeight: tokens.font.weightSemibold, color: tokens.color.text }}>
          This page failed to render
        </div>
        <div style={{ fontSize: tokens.font.sizeSm, color: tokens.color.textSecondary }}>
          Area: {this.props.areaLabel}
        </div>
        {this.state.errorMessage && (
          <div style={{ fontSize: tokens.font.sizeSm, color: tokens.color.danger }}>
            {this.state.errorMessage}
          </div>
        )}
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => window.location.reload()}
            style={{
              padding: '8px 14px',
              borderRadius: tokens.radius.sm,
              border: `1px solid ${tokens.color.accent}`,
              background: tokens.color.accent,
              color: tokens.color.surface,
              cursor: 'pointer',
              fontWeight: tokens.font.weightSemibold,
            }}
          >
            Reload page
          </button>
          <a
            href="/?page=overview"
            style={{
              padding: '8px 14px',
              borderRadius: tokens.radius.sm,
              border: `1px solid ${tokens.color.border}`,
              color: tokens.color.text,
              textDecoration: 'none',
            }}
          >
            Open Overview
          </a>
        </div>
      </div>
    )
  }
}
