# UAT-Focused Refactor Plan (Phase 0 + Phase 1)

## Scope

Major journeys through the platform:
- Channel performance
- Campaign performance
- Conversion paths
- Journeys (paths + flow + funnels)

## UAT risks identified

- Mixed backend contracts in the same page (legacy attribution endpoint + new trend endpoint).
- Different path engines across Conversion Paths vs Journeys pages.
- Inconsistent RBAC enforcement across analytics read endpoints.
- Timezone/date defaults hardcoded to UTC in several frontend modules.
- Campaign trend spend is channel-level spend projected into campaign rows (proxy metric).

## Phase 0 (baseline and parity checks)

- Define canonical parity assertions per flow:
  - Same date range + model + filters => consistent totals across KPI tile, trend panel, and table where applicable.
  - Previous-period windows must be equal-length and immediately preceding.
  - Empty/sparse data must return empty arrays or null points, not synthetic flat lines.
- Add test cases for endpoint-level contract shape and permission denial contract.

## Phase 1 (backward-compatible backend hardening)

Implemented in this phase:

1. Enforce `attribution.view` permission on read endpoints used by core analytics views:
   - `GET /api/overview/summary`
   - `GET /api/overview/drivers`
   - `GET /api/performance/channel/trend`
   - `GET /api/performance/campaign/trend`

2. Introduce normalized trend query-context parsing (`PerformanceQueryContext`) with:
   - date/grain normalization (reusing period resolver)
   - timezone normalization
   - channel filter normalization (dedupe, comma splitting, `all` handling)

3. Return normalized query metadata in trend endpoint responses (`meta.query_context`) for UI/debug parity checks.

4. Add endpoint tests for:
   - permission denial contract (`permission_denied` + permission key)
   - normalized channel filter behavior
   - query-context metadata shape

## Next phases (planned)

- Phase 2: Unify channel/campaign services so table and trends share one core metric source.
- Phase 3: Frontend query-context hooks and removal of hardcoded date defaults.
- Phase 4: Conversion Paths and Journeys path-source convergence.
- Phase 5: Full Cypress UAT parity suite as release gate.

## Progress update (current branch)

Completed after Phase 1:

1. Phase 2 (in progress): unified performance metric source
   - Channel and Campaign UI KPI totals now derive from unified summary endpoints:
     - `/api/performance/channel/summary`
     - `/api/performance/campaign/summary`
   - Conversion Paths UI now uses unified endpoints only:
     - `/api/conversion-paths/analysis`
     - `/api/conversion-paths/details`
   - Legacy attribution payloads are retained only for non-core metadata where needed (confidence, mapping coverage, config context).

2. Phase 3 (in progress): frontend defaults/query-context hardening
   - Removed hardcoded fixed date defaults in major pages; fallback now uses rolling recent range.
   - Trend panel fallbacks explicitly disclose when default range is used.

3. Phase 5 (started): UAT parity gate
   - Added Cypress API parity spec:
     - `frontend/cypress/e2e/uat_performance_parity.cy.ts`
   - Spec validates trend-vs-summary consistency for channel/campaign additive KPIs and equal-length previous-period windows.
