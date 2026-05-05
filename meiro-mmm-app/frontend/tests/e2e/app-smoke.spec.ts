import { expect, test, type Page } from '@playwright/test'

const PERIOD = {
  dateFrom: '2026-04-01',
  dateTo: '2026-04-14',
}

type AppSmokePage = {
  pageParam?: string
  navLabel: string
  visibleText: string | RegExp
}

const DIRECT_PAGES: AppSmokePage[] = [
  { navLabel: 'Overview', visibleText: /Overview|What changed|Workspace Assistant/i },
  { pageParam: 'dashboard', navLabel: 'Channel performance', visibleText: /Channel performance|Channel Detail/i },
  { pageParam: 'campaigns', navLabel: 'Campaign performance', visibleText: /Campaign performance|Campaign Detail/i },
  { pageParam: 'roles', navLabel: 'Attribution roles', visibleText: 'Attribution Roles' },
  { pageParam: 'paths', navLabel: 'Conversion paths', visibleText: 'Conversion Paths' },
  { pageParam: 'path_archetypes', navLabel: 'Path archetypes', visibleText: /Path Archetypes/i },
  { pageParam: 'analytics_journeys', navLabel: 'Journeys', visibleText: 'Journey workspace' },
  { pageParam: 'alerts', navLabel: 'Alerts', visibleText: 'Alerts' },
  { pageParam: 'dq', navLabel: 'Data quality', visibleText: 'Data quality' },
  { pageParam: 'settings', navLabel: 'Settings', visibleText: 'Settings sections' },
  { pageParam: 'mmm', navLabel: 'MMM', visibleText: 'Marketing Mix Modeling' },
]

function appUrl(pageParam?: string): string {
  const params = new URLSearchParams({
    date_from: PERIOD.dateFrom,
    date_to: PERIOD.dateTo,
  })
  if (pageParam) params.set('page', pageParam)
  return `/?${params.toString()}`
}

function localIsoDate(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function defaultRecentRange(days: number): { dateFrom: string; dateTo: string } {
  const end = new Date()
  const start = new Date(end)
  start.setDate(end.getDate() - Math.max(1, days))
  return { dateFrom: localIsoDate(start), dateTo: localIsoDate(end) }
}

async function installSmokeAuth(page: Page) {
  await page.addInitScript(() => {
    window.localStorage.setItem('mmm-user-role', 'admin')
    window.localStorage.setItem('mmm-user-id', 'playwright-smoke')
    window.localStorage.setItem('mmm-csrf-token', 'playwright-smoke-token')
  })
  await page.route('**/api/auth/me', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        authenticated: true,
        user: {
          id: 'playwright-smoke',
          username: 'playwright-smoke',
          role_name: 'Admin',
          status: 'active',
        },
        workspace: {
          id: 'default',
          slug: 'default',
          name: 'Default workspace',
        },
        permissions: [
          'alerts.manage',
          'alerts.view',
          'journeys.manage',
          'journeys.view',
          'settings.manage',
          'settings.view',
        ],
        csrf_token: 'playwright-smoke-token',
      }),
    })
  })
  await page.route('**/api/settings', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        attribution: {
          lookback_window_days: 31,
          use_converted_flag: true,
          conversion_value_mode: 'gross_only',
          min_journey_quality_score: 0,
          min_conversion_value: 0,
          time_decay_half_life_days: 7,
          position_first_pct: 0.4,
          position_last_pct: 0.4,
          markov_min_paths: 5,
        },
        mmm: { frequency: 'W' },
        nba: {
          min_prefix_support: 5,
          min_conversion_rate: 0.01,
          max_prefix_depth: 5,
          min_next_support: 5,
          max_suggestions_per_prefix: 3,
          min_uplift_pct: null,
          excluded_channels: ['direct'],
          promoted_journey_policies: [],
        },
        feature_flags: {
          mmm_enabled: true,
          journeys_enabled: true,
          journey_examples_enabled: true,
          funnel_builder_enabled: true,
          funnel_diagnostics_enabled: false,
          access_control_enabled: false,
          custom_roles_enabled: true,
          audit_log_enabled: true,
          scim_enabled: false,
          sso_enabled: false,
        },
        ads_governance: { require_approval: true, max_budget_change_pct: 30 },
        revenue_config: {
          conversion_names: ['conversion'],
          value_field_path: 'value',
          currency_field_path: 'currency',
          dedup_key: 'event_id',
          default_value: 120,
          default_value_mode: 'missing_or_zero',
          per_conversion_overrides: [],
          base_currency: 'CZK',
          fx_enabled: false,
          fx_mode: 'none',
          fx_rates_json: {},
          source_type: 'conversion_event',
        },
      }),
    })
  })
  await page.route('**/api/attribution/run-all', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ ok: true, skipped: true, source: 'playwright-smoke' }),
    })
  })
}

async function installMeasurementAudienceFixture(page: Page) {
  await page.route('**/api/segments/registry', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        items: [
          {
            id: 'meiro:vip_buyers',
            external_segment_id: 'vip_buyers',
            name: 'VIP Buyers',
            description: 'Membership-backed smoke audience',
            status: 'active',
            source: 'meiro_pipes',
            source_label: 'Meiro Pipes',
            kind: 'operational',
            supports_analysis: false,
            supports_activation: true,
            supports_hypotheses: true,
            supports_experiments: true,
            definition: { external_segment_id: 'vip_buyers', derived_from: 'webhook_payload' },
            criteria_label: 'Operational audience from Meiro Pipes webhook payloads',
            definition_version: 'external',
            segment_family: 'operational_external',
            size: 2,
            audience_capability: {
              level: 'membership_backed',
              label: 'Membership backed',
              membership_backed: true,
              activation_ready: true,
              measurement_ready: true,
              profile_count: 2,
              reason: 'Profile membership was observed in Meiro Pipes webhook/profile state.',
            },
          },
          {
            id: 'meiro:definition_only',
            external_segment_id: 'definition_only',
            name: 'Definition Only Audience',
            status: 'active',
            source: 'meiro_pipes_registry',
            source_label: 'Meiro Pipes registry',
            kind: 'operational',
            supports_analysis: false,
            supports_activation: true,
            supports_hypotheses: true,
            supports_experiments: true,
            definition: { external_segment_id: 'definition_only', derived_from: 'deciengine_pipes_field_registry' },
            criteria_label: 'Operational audience from deciEngine Pipes registry',
            definition_version: 'external',
            segment_family: 'operational_external',
            size: 42,
            audience_capability: {
              level: 'definition_only',
              label: 'Definition only',
              membership_backed: false,
              activation_ready: true,
              measurement_ready: false,
              profile_count: 42,
              reason: 'Measurement scoping needs observed membership.',
            },
          },
        ],
        summary: {
          local_analytical: 0,
          meiro_pipes: 2,
          analysis_ready: 0,
          activation_ready: 2,
        },
      }),
    })
  })
}

function installRuntimeGuards(page: Page) {
  const issues: string[] = []

  page.on('console', (msg) => {
    if (msg.type() !== 'error') return
    const text = msg.text()
    if (/favicon|ResizeObserver loop/i.test(text)) return
    issues.push(`console error: ${text}`)
  })

  page.on('requestfailed', (request) => {
    const url = request.url()
    if (!url.includes('/api/')) return
    const errorText = request.failure()?.errorText ?? ''
    if (/ERR_ABORTED/i.test(errorText)) return
    issues.push(`request failed: ${request.method()} ${url} ${errorText}`)
  })

  page.on('response', (response) => {
    const url = response.url()
    const status = response.status()
    if (!url.includes('/api/')) return
    if (status >= 500) {
      issues.push(`server error: ${status} ${url}`)
    }
  })

  return issues
}

async function expectHealthyPage(page: Page, expected: string | RegExp) {
  await page.waitForLoadState('domcontentloaded', { timeout: 10_000 }).catch(() => undefined)
  await expect(page.getByText('This page failed to render')).toHaveCount(0)
  await expect(page.locator('body')).toContainText(expected, { timeout: 30_000 })
}

test.describe('Meiro app smoke', () => {
  test.setTimeout(360_000)

  test.beforeEach(async ({ page }) => {
    await installSmokeAuth(page)
  })

  test('loads core deep links without render failures or server errors', async ({ page }) => {
    const issues = installRuntimeGuards(page)

    for (const target of DIRECT_PAGES) {
      await page.goto(appUrl(target.pageParam), { waitUntil: 'domcontentloaded' })
      await expectHealthyPage(page, target.visibleText)
    }

    expect(issues, issues.join('\n')).toEqual([])
  })

  test('left navigation changes pages and preserves analysis period', async ({ page }) => {
    const issues = installRuntimeGuards(page)

    await page.goto(appUrl('paths'), { waitUntil: 'domcontentloaded' })
    await expectHealthyPage(page, 'Conversion Paths')

    await page.getByRole('button', { name: /Overview/i }).click()
    await expectHealthyPage(page, /Overview|What changed|Workspace Assistant/i)
    await expect(page).toHaveURL(/date_from=2026-04-01/)
    await expect(page).toHaveURL(/date_to=2026-04-14/)

    await page.getByRole('button', { name: /Attribution roles/i }).click()
    await expectHealthyPage(page, 'Attribution Roles')
    await expect(page).toHaveURL(/page=roles/)
    await expect(page).toHaveURL(/date_from=2026-04-01/)
    await expect(page).toHaveURL(/date_to=2026-04-14/)

    expect(issues, issues.join('\n')).toEqual([])
  })

  test('overview defaults to a current rolling lookback when no dates are supplied', async ({ page }) => {
    const issues = installRuntimeGuards(page)
    const expected = defaultRecentRange(30)

    await page.goto('/', { waitUntil: 'domcontentloaded' })
    await expectHealthyPage(page, /Overview|What changed|Workspace Assistant/i)
    await expect(page).toHaveURL(new RegExp(`date_from=${expected.dateFrom}`))
    await expect(page).toHaveURL(new RegExp(`date_to=${expected.dateTo}`))

    expect(issues, issues.join('\n')).toEqual([])
  })

  test('MMM setup only allows membership-backed measurement audiences', async ({ page }) => {
    const issues = installRuntimeGuards(page)
    await installMeasurementAudienceFixture(page)

    await page.goto(appUrl('mmm'), { waitUntil: 'domcontentloaded' })
    await expectHealthyPage(page, 'Marketing Mix Modeling')

    await page.getByRole('button', { name: /Create new model/i }).last().click()
    await expectHealthyPage(page, 'Prepare MMM dataset')

    const audienceSelect = page.getByLabel('Measurement audience')
    await expect(audienceSelect).toContainText('VIP Buyers')
    await expect(audienceSelect).not.toContainText('Definition Only Audience')
    await expect(page.getByText(/Blocked from MMM scoping until membership is observed: Definition Only Audience/i)).toBeVisible()

    await audienceSelect.selectOption('meiro:vip_buyers')
    await expect(page.getByText('VIP Buyers', { exact: true }).first()).toBeVisible()
    await expect(page.getByText(/Membership-backed audience selected for measurement context/i)).toBeVisible()

    expect(issues, issues.join('\n')).toEqual([])
  })
})
