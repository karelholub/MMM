# Modular Monolith Refactor Blueprint

## Why this refactor

This repository is not suffering from "monolith" because it is a single deployable unit. The maintainability problem is that composition is centralized:

- `backend/app/main.py` owns routing for most of the backend surface.
- `frontend/src/modules/App.tsx` owns too much cross-page state and navigation orchestration.
- Domain logic already exists, but it is attached to large central entrypoints instead of clear module boundaries.

The goal is to keep one deployable app while refactoring into a modular monolith with explicit vertical slices.

## Current repo signals

Backend:

- `backend/app/main.py` contains 250+ route handlers.
- Route clusters already exist by domain: journeys, overview, performance, alerts, auth, ads, settings, attribution, MMM, experiments.
- Business logic is partially extracted into `services_*` modules, which gives us a migration seam.

Frontend:

- `frontend/src/modules/App.tsx` is acting as app shell, coordinator, permissions gate, and state hub.
- Several domain pages already exist and can be reorganized into feature folders without changing product behavior.
- `frontend/src/lib/apiClient.ts` is a good shared-infrastructure precedent.

## Target architecture

### Backend

Target shape:

```text
backend/app/
  core/
    auth.py
    db.py
    errors.py
    permissions.py
    settings.py
  modules/
    journeys/
      router.py
      schemas.py
      service.py
      repository.py
    performance/
      router.py
      schemas.py
      service.py
      repository.py
    overview/
    alerts/
    auth_access/
    mmm/
    incrementality/
  main.py
```

Rules:

- `main.py` should only create the app, register middleware, and include routers.
- Route-local schemas belong in the module, not in `main.py`.
- Route handlers should orchestrate HTTP concerns only.
- Repositories own SQLAlchemy queries.
- Services own domain use cases and cross-repository orchestration.
- Shared concerns such as auth, permission enforcement, DB session wiring, and common query parsing belong in `core/`.

### Frontend

Target shape:

```text
frontend/src/
  app/
    router.tsx
    shell/
  features/
    journeys/
      pages/
      components/
      queries.ts
      mutations.ts
      types.ts
    performance/
    alerts/
    settings/
    attribution/
  shared/
    api/
    auth/
    ui/
    lib/
```

Rules:

- `App.tsx` becomes a thin shell or is replaced by a router entrypoint.
- Feature folders own their query hooks, page-local state, and UI composition.
- Shared infrastructure stays in `shared/`.
- Feature-to-feature imports should be minimized; cross-feature reuse should move to `shared/`.

## Module boundaries for this repo

Suggested backend module ownership:

- `journeys`
  - journey definitions
  - journey paths
  - journey transitions
  - journey attribution summary
  - journey settings
  - funnels
  - journey alerts
- `performance`
  - overview summary/drivers/funnels/trends
  - channel/campaign trend and summary endpoints
  - performance diagnostics
- `attribution`
  - attribution runs/results
  - conversion paths analysis/details
  - source-state and journey ingestion endpoints
- `data_sources`
  - saved data sources
  - OAuth account selection
  - connector testing
- `auth_access`
  - login/session/workspace endpoints
  - roles/users/memberships/invitations
- `mmm`
  - datasets
  - model runs and summaries
  - platform dataset build/mapping validation
- `incrementality`
  - experiments
  - exposures/outcomes/results/health
- `settings`
  - global settings
  - KPI config
  - taxonomy
  - notification preferences/channels
- `ads`
  - ads state
  - change requests
  - audit

## Migration strategy

### Phase 1: Router extraction without behavior changes

Goal: remove route ownership from `main.py` while keeping services and DB logic intact.

Steps:

1. Create `backend/app/modules/<domain>/router.py`.
2. Move route-local request models to `schemas.py`.
3. Register the router from `main.py`.
4. Keep service imports and response shapes unchanged.
5. Verify with existing tests before changing internals.

This is the lowest-risk first move because it reduces central file size and clarifies ownership immediately.

### Phase 2: Extract shared dependencies from `main.py`

Move these concerns into `backend/app/core/`:

- permission context and permission dependencies
- workspace scoping helpers
- settings loading/persistence
- common query parsing utilities that are HTTP-agnostic

This removes the need for module routers to depend on `main.py` internals.

### Phase 3: Split service vs repository responsibilities

Current `services_*` files are a useful start, but some likely mix query logic, business rules, and response shaping. For each module:

- extract direct SQLAlchemy access into `repository.py`
- keep business rules in `service.py`
- keep response/request mapping in `router.py` or `schemas.py`

### Phase 4: Frontend feature modularization

Start with the same domain order as the backend:

1. `journeys`
2. `performance`
3. `alerts`

For each feature:

- move page-specific queries/mutations out of `frontend/src/modules/App.tsx`
- colocate API calls and typed responses under the feature
- keep `frontend/src/lib/apiClient.ts` as shared infra

### Phase 5: Architecture guardrails

Add guardrails once the first 2-3 modules are extracted:

- import-layer rules
- module ownership documentation
- test helpers per module
- route registration inventory

## First extraction order

Recommended order for this repository:

1. `journeys`
   - cohesive routes already backed by dedicated services
   - strong test coverage already exists
   - minimal cross-domain coupling compared with auth/settings
2. `performance`
   - already documented as an active refactor area
   - high user-facing value
3. `alerts`
   - good candidate after performance/journeys stabilize
4. `auth_access`
   - important, but more cross-cutting and higher risk
5. `mmm` and `incrementality`

## Concrete short-term milestones

### Milestone 1

- Add this blueprint.
- Extract `/api/journeys/*` definitions/paths/transitions/attribution-summary routes into `backend/app/modules/journeys/`.
- Keep tests green.

### Milestone 2

- Extract shared permission helpers from `main.py` into `backend/app/core/permissions.py`.
- Convert journeys router to consume shared core dependencies directly.

### Milestone 3

- Extract performance router and its request parsing.
- Move frontend journeys page state and API wiring out of `frontend/src/modules/App.tsx`.

## Success criteria

This refactor is successful when:

- adding a journey feature no longer requires editing `backend/app/main.py`
- route-local schemas live with their module
- tests can be run by domain
- frontend pages own their own data flows
- the app remains one deployable unit with unchanged public contracts
