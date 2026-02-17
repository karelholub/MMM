/**
 * Design tokens for enterprise-style data dashboards.
 * Neutral base, single accent, consistent density and typography.
 */
export const tokens = {
  color: {
    bg: '#f8fafc',
    bgSubtle: '#f8fafc',
    surface: '#ffffff',
    surfaceMuted: '#f1f5f9',
    border: '#e2e8f0',
    borderLight: '#f1f5f9',
    text: '#0f172a',
    textSecondary: '#64748b',
    textMuted: '#94a3b8',
    accent: '#3b82f6',
    accentMuted: '#93c5fd',
    success: '#059669',
    successMuted: '#d1fae5',
    danger: '#dc2626',
    dangerMuted: '#fee2e2',
    dangerSubtle: '#fee2e2',
    warning: '#d97706',
    warningMuted: '#fef3c7',
    warningSubtle: '#fef3c7',
    chart: ['#3b82f6', '#059669', '#d97706', '#7c3aed', '#0ea5e9', '#84cc16', '#ec4899', '#64748b'],
  },
  space: {
    xs: 4,
    sm: 8,
    md: 12,
    lg: 16,
    xl: 24,
    xxl: 32,
  },
  radius: {
    sm: 6,
    md: 8,
    lg: 12,
    full: 9999,
  },
  font: {
    sizeXs: 11,
    sizeSm: 12,
    sizeMd: 13,
    sizeBase: 14,
    sizeLg: 16,
    sizeXl: 18,
    size2xl: 22,
    weightNormal: 400,
    weightMedium: 500,
    weightSemibold: 600,
    weightBold: 700,
  },
  shadow: '0 1px 3px rgba(0,0,0,0.06)',
  shadowSm: '0 1px 2px rgba(0,0,0,0.04)',
  shadowXs: '0 1px 1px rgba(0,0,0,0.04)',
  shadowLg: '0 10px 20px rgba(15, 23, 42, 0.08)',
} as const

export type Tokens = typeof tokens
