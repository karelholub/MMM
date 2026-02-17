export function defaultRecentDateRange(days = 30): { dateFrom: string; dateTo: string } {
  const end = new Date()
  const start = new Date(end)
  start.setDate(end.getDate() - Math.max(1, days))
  return {
    dateFrom: start.toISOString().slice(0, 10),
    dateTo: end.toISOString().slice(0, 10),
  }
}
