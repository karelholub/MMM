/**
 * Design tokens for Meiro-style enterprise dashboards.
 * Light canvas, white surfaces, purple primary actions, compact density.
 */
export const tokens = {
  color: {
    bg: '#f7f7f8',
    bgSubtle: '#f4f4f6',
    surface: '#ffffff',
    surfaceMuted: '#efeff2',
    border: '#dedfe4',
    borderLight: '#e8e8ec',
    text: '#252b3a',
    textSecondary: '#646873',
    textMuted: '#8b9099',
    accent: '#963cf2',
    accentMuted: '#efe3ff',
    success: '#15935f',
    successMuted: '#e6f6ee',
    danger: '#ff3b3f',
    dangerMuted: '#ffe9e9',
    dangerSubtle: '#fff1f1',
    warning: '#e6a23c',
    warningMuted: '#fff2d8',
    warningSubtle: '#fff7e8',
    chart: ['#963cf2', '#6ca951', '#f6ad4d', '#ff7d73', '#19b9bf', '#4a83e6', '#e642bc', '#ff3b3f'],
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
    lg: 8,
    full: 9999,
  },
  font: {
    sizeXs: 12,
    sizeSm: 14,
    sizeMd: 15,
    sizeBase: 16,
    sizeLg: 18,
    sizeXl: 22,
    size2xl: 34,
    weightNormal: 400,
    weightMedium: 500,
    weightSemibold: 600,
    weightBold: 700,
  },
  shadow: '0 2px 8px rgba(37, 43, 58, 0.14)',
  shadowSm: '0 1px 4px rgba(37, 43, 58, 0.10)',
  shadowXs: '0 1px 2px rgba(37, 43, 58, 0.08)',
  shadowLg: '0 10px 24px rgba(37, 43, 58, 0.14)',
} as const

type Tokens = typeof tokens
