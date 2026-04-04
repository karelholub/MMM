# Budget Decisioning and Journey Lab Implementation Roadmap

Date: 2026-04-03
Status: Planned
Owner: Codex

## Goal

Implement two connected capabilities on top of the current app:

1. Budget Decisioning
2. Journey Lab

The product goal is to move users from analytics to action:

1. Detect an issue or opportunity
2. Generate a recommendation with evidence and guardrails
3. Create an operational change or experiment
4. Measure realized outcome
5. Feed results back into future recommendations

## Why This Approach

The current codebase already has the main primitives:

- MMM outputs and what-if optimization in `frontend/src/modules/BudgetOptimizer.tsx`
- Channel and campaign drilldowns in `frontend/src/modules/ChannelPerformance.tsx` and `frontend/src/modules/CampaignPerformance.tsx`
- Ads actioning and approvals in `frontend/src/components/ads/AdsActionsDrawer.tsx` and `backend/app/modules/ads_governance/router.py`
- Journey paths, transitions, examples, funnels, and saved views in `frontend/src/modules/Journeys.tsx` and `backend/app/modules/journeys/router.py`
- NBA preview and test logic in `backend/app/services_nba_defaults.py` and `backend/app/main.py`
- Incrementality experiments in `frontend/src/modules/Incrementality.tsx` and `backend/app/main.py`

The best implementation path is not a large rewrite. It is a staged set of vertical slices that reuse those primitives and add a decision layer around them.

## Delivery Strategy

Implement in vertical slices, not by frontend/backend separately.

Each slice should include:

- DB migration if needed
- backend service and API
- frontend UI on the destination page
- trust layer: confidence, warnings, blockers, and recommended next actions
- at least one integration test path

This reduces the risk of building disconnected infrastructure with no usable user flow.

## Principles

- Prefer extending existing modules before creating new top-level pages.
- Keep the first release read-heavy and recommendation-heavy before adding automated actioning.
- Distinguish observational insights from causal evidence everywhere in the UI.
- Do not surface recommendations without confidence, readiness, and guardrail context.
- Reuse the existing experiment framework instead of building a second one for journeys.
- Persist decision objects and outcomes so the app can learn from realized performance.

## Existing Entry Points To Extend

### Frontend

- `frontend/src/modules/MMMWizardShell.tsx`
- `frontend/src/modules/BudgetOptimizer.tsx`
- `frontend/src/modules/MMMDashboard.tsx`
- `frontend/src/modules/ChannelPerformance.tsx`
- `frontend/src/modules/CampaignPerformance.tsx`
- `frontend/src/modules/Journeys.tsx`
- `frontend/src/modules/Incrementality.tsx`
- `frontend/src/components/DecisionStatusCard.tsx`
- `frontend/src/components/ads/AdsActionsDrawer.tsx`
- `frontend/src/lib/recommendedActions.ts`

### Backend

- `backend/app/modules/performance/router.py`
- `backend/app/modules/journeys/router.py`
- `backend/app/modules/ads_governance/router.py`
- `backend/app/services_ads_ops.py`
- `backend/app/services_nba_defaults.py`
- `backend/app/services_incrementality.py`
- `backend/app/main.py`
- `backend/app/models_config_dq.py`

## Target Product Surfaces

## A. Budget Decisioning

### User Outcomes

- Show ranked budget moves instead of only manual sliders.
- Explain why each move is recommended.
- Let users choose conservative vs growth vs target-based objectives.
- Turn accepted recommendations into ads change requests.
- Measure whether realized outcome matched prediction.

### UX Surface

Primary home:

- `frontend/src/modules/BudgetOptimizer.tsx`

Secondary drilldowns:

- `frontend/src/modules/ChannelPerformance.tsx`
- `frontend/src/modules/CampaignPerformance.tsx`

### First-Class Objects

- Recommendation set
- Recommendation action
- Scenario
- Scenario outcome
- Realization snapshot

### Core Recommendation Modes

- Protect efficiency
- Grow conversions
- Hit target CPA / ROAS

### Recommendation Card Shape

Each card should include:

- objective
- scope: portfolio / channel / campaign
- actions: increase, decrease, hold
- expected delta spend
- expected KPI lift
- confidence label and score
- extrapolation risk
- data readiness warnings
- operational guardrails
- evidence summary
- CTA: view details, create scenario, create change requests

## B. Journey Lab

### User Outcomes

- Identify leakage and opportunity in detailed journeys.
- Convert path observations into explicit hypotheses.
- Compare possible next actions and policies.
- Launch experiments directly from journey hypotheses.
- Promote validated policies into recommended defaults.

### UX Surface

Primary home:

- `frontend/src/modules/Journeys.tsx`

Experiment destination:

- `frontend/src/modules/Incrementality.tsx`

### New Tabs To Add In Journeys

- Insights
- Hypotheses
- Policy Sandbox
- Experiments

Existing tabs remain:

- Paths
- Flow
- Examples
- Funnels

### First-Class Objects

- Journey insight
- Journey hypothesis
- Policy preview
- Policy run
- Linked experiment

### Hypothesis Shape

- trigger or path prefix
- target segment
- current action
- proposed action
- target KPI
- expected mechanism
- support and baseline rate
- sample size requirement
- owner
- status
- linked experiment id
- result summary

## Phase Plan

## Phase 1: Budget Decisioning MVP

### Scope

- Upgrade `BudgetOptimizer` from slider-first to recommendation-first.
- Keep current optimizer and what-if API as fallback and details layer.
- Add recommendation cards and scenario save/load.
- Add confidence, risk, and guardrail metadata.

### Backend Work

Add service:

- `backend/app/services_budget_recommendations.py`

Add read endpoints:

- `GET /api/performance/budget/recommendations`
- `POST /api/performance/budget/scenarios`
- `GET /api/performance/budget/scenarios/{scenario_id}`

Implementation notes:

- Reuse MMM run outputs: ROI, contribution, channel summary, current spend, observed range.
- Build ranked recommendations by objective.
- Penalize actions outside observed historical spend range.
- Add readiness and mapping coverage checks before marking a recommendation as `ready`.
- Return a `decision` payload compatible with `DecisionStatusCard`.

### Data Model

Add tables:

- `budget_scenarios`
- `budget_recommendations`
- `budget_recommendation_actions`

Initial persistence can be minimal:

- scenario input payload
- generated recommendation payload
- objective
- created_at
- status

### Frontend Work

Primary file:

- `frontend/src/modules/BudgetOptimizer.tsx`

Changes:

- Add recommendation mode selector
- Replace top-of-page slider emphasis with recommendation cards
- Keep sliders in a “manual scenario” section
- Add scenario comparison view
- Add trust panel using `DecisionStatusCard`

Secondary files:

- `frontend/src/modules/MMMWizardShell.tsx`
- `frontend/src/modules/ChannelPerformance.tsx`
- `frontend/src/modules/CampaignPerformance.tsx`

### Definition of Done

- User can fetch ranked recommendations for a run
- User can inspect rationale, warnings, and confidence
- User can save a scenario
- User can still fall back to manual what-if controls

## Phase 2: Budget Execution and Realization

### Scope

- Convert accepted recommendation actions into ads governance proposals.
- Support grouped proposals for multiple campaigns.
- Measure realized post-change performance against expected impact.

### Backend Work

Extend:

- `backend/app/modules/ads_governance/router.py`
- `backend/app/services_ads_ops.py`

Add endpoints:

- `POST /api/ads/change-requests/from-recommendation`
- `GET /api/performance/budget/realization`

Add service:

- `backend/app/services_budget_realization.py`

### Data Model

Add tables:

- `budget_realization_snapshots`
- `recommendation_feedback`

Store:

- recommendation id
- linked change request ids
- baseline metrics
- expected metrics
- actual metrics after N days
- variance and status

### Frontend Work

Files:

- `frontend/src/modules/BudgetOptimizer.tsx`
- `frontend/src/components/ads/AdsActionsDrawer.tsx`
- `frontend/src/modules/CampaignPerformance.tsx`

Changes:

- “Create change requests” CTA from recommendation details
- batch review drawer
- recommendation lifecycle state: proposed, approved, applied, measured
- realized vs predicted visualization

### Definition of Done

- User can convert recommendation actions into change requests
- User can see approval/apply status
- User can see realized vs predicted performance after rollout

## Phase 3: Journey Lab MVP

### Scope

- Add `Insights` and `Hypotheses` tabs to Journeys.
- Generate structured insights from existing path, transition, and funnel data.
- Let users save a hypothesis from an insight or NBA recommendation.

### Backend Work

Add services:

- `backend/app/services_journey_insights.py`
- `backend/app/services_journey_hypotheses.py`

Extend router:

- `backend/app/modules/journeys/router.py`

Add endpoints:

- `GET /api/journeys/{definition_id}/insights`
- `GET /api/journeys/hypotheses`
- `POST /api/journeys/hypotheses`
- `PUT /api/journeys/hypotheses/{hypothesis_id}`

Insight generation should use:

- path conversion rate vs baseline
- dropoff by step
- time-to-convert delay
- segment breakdowns already available in path dimensions
- NBA candidate actions from current prefix logic

### Data Model

Add table:

- `journey_hypotheses`

Recommended columns:

- id
- journey_definition_id
- title
- trigger_json
- segment_json
- current_action_json
- proposed_action_json
- target_kpi
- hypothesis_text
- support_count
- baseline_rate
- sample_size_target
- status
- owner_user_id
- linked_experiment_id
- result_json
- created_at
- updated_at

### Frontend Work

Primary file:

- `frontend/src/modules/Journeys.tsx`

Changes:

- Add new tabs
- Add insight cards with severity and confidence
- Add “Create hypothesis” action
- Add hypothesis list with statuses and saved filters

### Definition of Done

- User can view journey insights
- User can convert an insight into a saved hypothesis
- User can review open hypotheses in the Journeys page

## Phase 4: Policy Sandbox and Experiment Linking

### Scope

- Compare next best action policies in a structured way.
- Use the existing experiment system for journey hypotheses.
- Let users create experiments from a hypothesis or policy preview.

### Backend Work

Add services:

- `backend/app/services_nba_policy_simulation.py`

Extend:

- `backend/app/main.py`
- `backend/app/services_incrementality.py`

Add endpoints:

- `POST /api/journeys/hypotheses/{hypothesis_id}/simulate`
- `POST /api/journeys/hypotheses/{hypothesis_id}/create-experiment`
- `GET /api/journeys/policies/preview`

### Data Model

Extend `experiments` table in `backend/app/models_config_dq.py`:

- `experiment_type`
- `source_type`
- `source_id`
- `segment_json`
- `policy_json`
- `guardrails_json`

Do not build a second experiment framework.

### Frontend Work

Files:

- `frontend/src/modules/Journeys.tsx`
- `frontend/src/modules/Incrementality.tsx`

Changes:

- Add Policy Sandbox tab
- Add “Create experiment” from hypothesis
- Route into existing Incrementality detail view
- Show experiment provenance: channel / journey / policy

### Definition of Done

- User can simulate a policy for a hypothesis
- User can create an experiment from that hypothesis
- Experiment appears and runs inside the current incrementality module

## Phase 5: Learning Loop and Policy Promotion

### Scope

- Connect experiment results back into recommendations.
- Rank policies and hypotheses by measured performance, not just observed path differences.
- Allow validated policies to become recommended defaults.

### Backend Work

Add tables:

- `nba_policies`
- `nba_policy_results`

Add services:

- `backend/app/services_policy_learning.py`

Add endpoints:

- `GET /api/journeys/policies/{policy_id}/results`
- `POST /api/journeys/policies/{policy_id}/promote`

### Frontend Work

Files:

- `frontend/src/modules/Journeys.tsx`
- `frontend/src/modules/Settings.tsx`

Changes:

- Show observational vs experimental evidence
- Show promoted / validated / deprecated policy states
- Allow promotion of validated policies into default recommendations

### Definition of Done

- User can see which policies have causal evidence
- User can promote a validated policy
- Future recommendations can reference validated policy outcomes

## API Contract Guidelines

### Decision Payload Shape

Every recommendation or insight endpoint should return:

- `status`: ready / warning / blocked
- `subtitle`
- `blockers[]`
- `warnings[]`
- `actions[]`

This keeps the trust layer consistent with `frontend/src/components/DecisionStatusCard.tsx`.

### Recommendation Payload Shape

Use a stable envelope:

```json
{
  "id": "rec_123",
  "objective": "protect_efficiency",
  "scope": "channel",
  "status": "ready",
  "summary": "Reduce Meta by 10% and move budget to branded search.",
  "expected_impact": {
    "metric": "conversions",
    "delta_abs": 120,
    "delta_pct": 4.2
  },
  "confidence": {
    "label": "medium",
    "score": 0.68
  },
  "risk": {
    "extrapolation": "low",
    "readiness": "medium"
  },
  "actions": [],
  "evidence": [],
  "decision": {}
}
```

### Hypothesis Payload Shape

Use a stable envelope:

```json
{
  "id": "hyp_123",
  "journey_definition_id": "uuid",
  "title": "Recover pricing-page exits",
  "trigger": {},
  "segment": {},
  "current_action": {},
  "proposed_action": {},
  "target_kpi": "purchase",
  "support_count": 1840,
  "baseline_rate": 0.037,
  "sample_size_target": 6200,
  "status": "draft",
  "decision": {}
}
```

## Migration Order

Recommended migration order:

1. Budget scenario and recommendation tables
2. Journey hypothesis table
3. Experiment table extension for journey/policy provenance
4. Policy result tables

Use numbered SQL migrations in `backend/migrations/`.

## Testing Strategy

### Backend

- Unit tests for recommendation ranking and guardrails
- Unit tests for insight generation and hypothesis creation
- API tests for scenario creation, hypothesis CRUD, and experiment linking
- regression tests for existing experiment endpoints after table extensions

### Frontend

- Component tests for recommendation cards and trust states
- component tests for hypothesis flows
- end-to-end flow for:
  - fetch budget recommendation
  - create change request
  - save hypothesis
  - create experiment from hypothesis

## Implementation Order Inside Each Phase

For each phase, build in this order:

1. migration
2. backend model/service
3. backend endpoint
4. frontend query layer
5. frontend page integration
6. trust layer and edge cases
7. tests

## Risks and Controls

### Risk: Recommendations look precise when evidence is weak

Control:

- always include confidence and risk
- block or downgrade recommendations when readiness is low
- explicitly label observational vs experimental evidence

### Risk: Journey Lab becomes too complex

Control:

- default to summary cards
- keep drilldowns behind drawers or expandable sections
- make hypothesis creation the main workflow, not raw tables

### Risk: Duplicate experiment systems

Control:

- extend the existing `experiments` framework only

### Risk: Operational workflows drift from recommendation logic

Control:

- persist recommendation ids on change requests and realization snapshots

## Immediate Next Step

Start with Phase 1.

The first implementation slice should be:

1. add budget recommendation persistence migration
2. add `services_budget_recommendations.py`
3. add `GET /api/performance/budget/recommendations`
4. refactor `BudgetOptimizer.tsx` into recommendation-first layout
5. keep existing sliders and what-if controls as a secondary manual mode

That gives the fastest path to a usable feature without destabilizing journeys or experiments yet.
