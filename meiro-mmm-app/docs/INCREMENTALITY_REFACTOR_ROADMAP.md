# Incrementality Refactor Roadmap

This roadmap replaces the current manual incrementality experiment setup with a data-backed planner that uses:

- application settings
- observed journey data
- KPI definitions
- real channel availability

The target outcome is simple: users should only be able to create experiments that are valid for the current workspace.

## Current Gaps

The current implementation in [frontend/src/modules/Incrementality.tsx](/Users/kh/MMM/meiro-mmm-app/frontend/src/modules/Incrementality.tsx) still behaves like a manual scaffold:

- channels are hard-coded
- `conversion_key` is free text
- advanced setup is written into notes instead of structured fields
- power analysis is separate from the create flow
- experiment health assumes a 50/50 split because the configured split is not persisted as first-class setup

The backend already has pieces that should be used more directly:

- KPI definitions in [backend/app/utils/kpi_config.py](/Users/kh/MMM/meiro-mmm-app/backend/app/utils/kpi_config.py)
- experiment `segment`, `policy`, and `guardrails` in [backend/app/models_config_dq.py](/Users/kh/MMM/meiro-mmm-app/backend/app/models_config_dq.py)
- journey data loaded through [backend/app/services_conversions.py](/Users/kh/MMM/meiro-mmm-app/backend/app/services_conversions.py)
- workspace journey summary exposed through [frontend/src/components/WorkspaceContext.tsx](/Users/kh/MMM/meiro-mmm-app/frontend/src/components/WorkspaceContext.tsx)

## Target Product Shape

Incrementality should become a planner-driven flow:

1. choose an eligible owned channel from observed data
2. choose a KPI from settings-backed KPI definitions
3. review observed audience volume, conversion mix, and recommended defaults
4. adjust experiment design only where needed
5. create a structured experiment with persisted setup
6. monitor health/results against the planned design, not generic assumptions

## Phase Plan

### Phase 1: Setup Context

Goal: remove hard-coded channels and free-text KPI selection.

Backend:

- add `GET /api/experiments/setup-context`
- source channels from loaded journeys / sample data in the selected date window
- source KPIs from settings
- compute per-channel observed counts:
  - journeys touched
  - converted journeys
  - non-converted journeys
  - baseline conversion rate
  - observed profile count
  - latest activity date
- return planner defaults:
  - default channel
  - default KPI
  - default treatment rate
  - default runtime
  - default alpha/power/MDE
- return caveats when the workspace does not have enough non-converted journeys for reliable planning

Frontend:

- replace hard-coded channel list with setup-context values
- replace free-text KPI input with settings-backed KPI selector
- use workspace date range by default
- prefill power-calculator inputs from setup-context
- show observed volume and conversion-rate hints next to the selected channel

### Phase 2: Structured Experiment Setup

Goal: stop storing experiment design as notes.

Backend:

- extend experiment create/update payloads to persist:
  - `treatment_rate`
  - `assignment_unit`
  - `baseline_rate_estimate`
  - `mde_target`
  - `alpha`
  - `power`
  - `min_runtime_days`
  - `exclusion_window_days`
  - `stop_rule`
  - `setup_source`
  - `config_id`
  - `config_version`
- keep these fields in structured JSON first if a schema migration needs to stay small

Frontend:

- send structured `policy` and `guardrails`
- display structured setup in experiment detail
- stop labeling core fields as “not enforced” when they are persisted and used

### Phase 3: Planner Recommendations

Goal: move from neutral form inputs to guided design.

Backend:

- add `POST /api/experiments/recommend-design`
- for a selected channel/KPI/date window, return:
  - recommended split
  - recommended runtime
  - baseline rate estimate
  - estimated sample size
  - readiness score
  - contamination warnings

Frontend:

- add a planner summary card
- show “safe to launch”, “needs more volume”, or “insufficient signal”
- add direct apply-to-form controls

### Phase 4: Health and Results Alignment

Goal: evaluate experiments against planned setup.

Backend:

- make health use persisted split instead of fixed 50/50
- compare actual assignments vs planned assignments
- add readiness against planned sample target
- add guardrail warnings based on stored exclusion/min-runtime rules

Frontend:

- show plan vs actual in health cards
- show whether the experiment is underpowered against the original plan

### Phase 5: Operational Integration

Goal: connect experiment setup to actual channel execution.

Backend:

- map supported channels to actual delivery/exposure sources
- improve auto-assignment/exposure ingestion per supported channel
- add setup provenance so results can be traced back to the settings/data snapshot used at launch

Frontend:

- show which channels are “planner-ready” vs “manual only”
- surface missing instrumentation blockers before launch

## Phase 1 API Contract

`GET /api/experiments/setup-context?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD`

Response shape:

```json
{
  "date_from": "2026-03-01",
  "date_to": "2026-03-31",
  "defaults": {
    "channel": "email",
    "conversion_key": "purchase",
    "treatment_rate": 0.9,
    "min_runtime_days": 14,
    "alpha": 0.05,
    "power": 0.8,
    "mde": 0.01
  },
  "channels": [
    {
      "channel": "email",
      "label": "email",
      "journeys": 1280,
      "converted_journeys": 142,
      "non_converted_journeys": 1138,
      "observed_profiles": 1210,
      "baseline_conversion_rate": 0.1109,
      "last_seen_at": "2026-03-31T00:00:00Z",
      "eligible": true,
      "notes": []
    }
  ],
  "kpis": [
    {
      "id": "purchase",
      "label": "Purchase",
      "type": "primary",
      "event_name": "purchase",
      "count": 142,
      "is_primary": true
    }
  ],
  "warnings": []
}
```

## Implementation Notes

- Phase 1 should stay read-only.
- Use the existing loaded journeys path first, because it already works with both sample data and imported data.
- Avoid a schema migration in Phase 1 unless strictly necessary.
- Prefer small vertical commits:
  - spec/doc
  - backend setup-context
  - frontend setup-context consumption

## Success Criteria

- users no longer type channels manually
- users no longer type KPI ids manually
- planner defaults reflect the active settings + observed data
- sample data and imported data both populate the planner
- the create form becomes materially harder to misconfigure
