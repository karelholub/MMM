const adminHeaders = {
  'X-User-Role': 'admin',
  'X-User-Id': 'cypress-uat',
}

type Range = { date_from: string; date_to: string }

type ChannelSummaryResponse = {
  current_period: Range
  previous_period: Range
  items: Array<{
    channel: string
    current: { spend: number; conversions: number; revenue: number }
    previous?: { spend: number; conversions: number; revenue: number } | null
  }>
}

type CampaignSummaryResponse = {
  current_period: Range
  previous_period: Range
  items: Array<{
    campaign_id: string
    current: { spend: number; conversions: number; revenue: number }
    previous?: { spend: number; conversions: number; revenue: number } | null
  }>
}

function toDateOnly(value: string | null | undefined): string | null {
  if (!value) return null
  return value.slice(0, 10)
}

function defaultRecentDateRange(days = 30): { from: string; to: string } {
  const now = new Date()
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()))
  const start = new Date(end)
  start.setUTCDate(start.getUTCDate() - (days - 1))
  const fmt = (d: Date) => d.toISOString().slice(0, 10)
  return { from: fmt(start), to: fmt(end) }
}

function periodLength(range: Range): number {
  const from = new Date(`${range.date_from}T00:00:00Z`).getTime()
  const to = new Date(`${range.date_to}T00:00:00Z`).getTime()
  return Math.floor((to - from) / (24 * 60 * 60 * 1000)) + 1
}

function approxEqual(actual: number, expected: number, tolerance = 1e-6): boolean {
  return Math.abs(actual - expected) <= tolerance
}

describe('UAT parity: performance trend/summary consistency', () => {
  let dateFrom: string
  let dateTo: string

  before(() => {
    cy.request({
      method: 'GET',
      url: '/api/attribution/journeys',
      headers: adminHeaders,
      failOnStatusCode: false,
    }).then((res) => {
      if (res.status === 200) {
        const min = toDateOnly(res.body?.date_min)
        const max = toDateOnly(res.body?.date_max)
        if (min && max) {
          dateFrom = min
          dateTo = max
          return
        }
      }
      const fallback = defaultRecentDateRange(30)
      dateFrom = fallback.from
      dateTo = fallback.to
    })
  })

  it('channel summary totals match channel trend totals for additive KPIs', () => {
    const base = {
      date_from: dateFrom,
      date_to: dateTo,
      timezone: 'UTC',
      compare: '1',
    }

    cy.request<ChannelSummaryResponse>({
      method: 'GET',
      url: '/api/performance/channel/summary',
      qs: base,
      headers: adminHeaders,
    }).then((summaryRes) => {
      expect(summaryRes.status).to.eq(200)
      const summary = summaryRes.body
      const sumCurrent = summary.items.reduce(
        (acc, row) => {
          acc.spend += row.current.spend || 0
          acc.conversions += row.current.conversions || 0
          acc.revenue += row.current.revenue || 0
          return acc
        },
        { spend: 0, conversions: 0, revenue: 0 },
      )

      const sumPrevious = summary.items.reduce(
        (acc, row) => {
          acc.spend += row.previous?.spend || 0
          acc.conversions += row.previous?.conversions || 0
          acc.revenue += row.previous?.revenue || 0
          return acc
        },
        { spend: 0, conversions: 0, revenue: 0 },
      )

      ;(['spend', 'conversions', 'revenue'] as const).forEach((kpiKey) => {
        cy.request({
          method: 'GET',
          url: '/api/performance/channel/trend',
          qs: {
            ...base,
            grain: 'daily',
            kpi_key: kpiKey,
          },
          headers: adminHeaders,
        }).then((trendRes) => {
          expect(trendRes.status).to.eq(200)
          const currentTotal = (trendRes.body.series || []).reduce(
            (sum: number, row: { value: number | null }) => sum + (typeof row.value === 'number' ? row.value : 0),
            0,
          )
          const previousTotal = (trendRes.body.series_prev || []).reduce(
            (sum: number, row: { value: number | null }) => sum + (typeof row.value === 'number' ? row.value : 0),
            0,
          )

          expect(approxEqual(currentTotal, sumCurrent[kpiKey])).to.eq(true)
          expect(approxEqual(previousTotal, sumPrevious[kpiKey])).to.eq(true)
        })
      })

      expect(periodLength(summary.current_period)).to.eq(periodLength(summary.previous_period))
    })
  })

  it('campaign summary totals match campaign trend totals for additive KPIs', () => {
    const base = {
      date_from: dateFrom,
      date_to: dateTo,
      timezone: 'UTC',
      compare: '1',
    }

    cy.request<CampaignSummaryResponse>({
      method: 'GET',
      url: '/api/performance/campaign/summary',
      qs: base,
      headers: adminHeaders,
    }).then((summaryRes) => {
      expect(summaryRes.status).to.eq(200)
      const summary = summaryRes.body
      const sumCurrent = summary.items.reduce(
        (acc, row) => {
          acc.spend += row.current.spend || 0
          acc.conversions += row.current.conversions || 0
          acc.revenue += row.current.revenue || 0
          return acc
        },
        { spend: 0, conversions: 0, revenue: 0 },
      )

      const sumPrevious = summary.items.reduce(
        (acc, row) => {
          acc.spend += row.previous?.spend || 0
          acc.conversions += row.previous?.conversions || 0
          acc.revenue += row.previous?.revenue || 0
          return acc
        },
        { spend: 0, conversions: 0, revenue: 0 },
      )

      ;(['spend', 'conversions', 'revenue'] as const).forEach((kpiKey) => {
        cy.request({
          method: 'GET',
          url: '/api/performance/campaign/trend',
          qs: {
            ...base,
            grain: 'daily',
            kpi_key: kpiKey,
          },
          headers: adminHeaders,
        }).then((trendRes) => {
          expect(trendRes.status).to.eq(200)
          const currentTotal = (trendRes.body.series || []).reduce(
            (sum: number, row: { value: number | null }) => sum + (typeof row.value === 'number' ? row.value : 0),
            0,
          )
          const previousTotal = (trendRes.body.series_prev || []).reduce(
            (sum: number, row: { value: number | null }) => sum + (typeof row.value === 'number' ? row.value : 0),
            0,
          )

          expect(approxEqual(currentTotal, sumCurrent[kpiKey])).to.eq(true)
          expect(approxEqual(previousTotal, sumPrevious[kpiKey])).to.eq(true)
        })
      })

      expect(periodLength(summary.current_period)).to.eq(periodLength(summary.previous_period))
    })
  })
})
