# Cover Dashboard – UX notes (states)

## Loading
- **Skeletons**: KPI row shows 6 `KpiTileSkeleton` placeholders; "What changed" and "Recent alerts" show block placeholders. No heavy charts; layout is stable so CLS is minimal.
- **When**: Shown while `overview/summary` or `overview/drivers` is loading. Alerts load in parallel and do not block the main content.
- **Copy**: Default DashboardPage loading state is "Loading…" (can be overridden via `loadingState` with the skeleton grid).

## Empty
- **Condition**: Summary and drivers have finished loading, and there is no meaningful data (all KPI values zero, no channels, no alerts, no highlights).
- **UI**: Centred card with title "Cover Dashboard", short explanation, and primary actions: **Load sample data** and **Connect data sources**. Matches existing workspace onboarding.
- **Deep-links**: Both actions route to existing flows (sample loader, Data sources page).

## Error
- **Condition**: Any of the overview summary or drivers requests fail.
- **UI**: DashboardPage `errorState`: message from the first failed request. No partial content; user can retry by re-opening the page or refreshing.
- **Recovery**: No inline retry button; consistent with other dashboard error handling. User can use sidebar to go to Data sources or Alerts and retry from there.

## Consistency
- Header (date range, "Create alert", Back) and AlertsBell match the rest of the app (menu + settings styling).
- All section CTAs deep-link: Channel performance → dashboard, Campaign performance → campaigns, Data sources / Data quality → dq/datasources, Alerts → alerts.
