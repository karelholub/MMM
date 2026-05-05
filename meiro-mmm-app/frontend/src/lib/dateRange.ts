function formatLocalIsoDate(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

export function defaultRecentDateRange(days = 30): { dateFrom: string; dateTo: string } {
  const end = new Date()
  const start = new Date(end)
  start.setDate(end.getDate() - Math.max(1, days))
  return {
    dateFrom: formatLocalIsoDate(start),
    dateTo: formatLocalIsoDate(end),
  }
}
