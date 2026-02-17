# Journeys Module Discovery Notes

Date: 2026-02-12

## Routing and Navigation
- Frontend navigation is managed in `frontend/src/modules/App.tsx` with:
  - `type Page` union for page IDs.
  - `NAV_ITEMS` for sidebar + breadcrumbs.
  - conditional rendering (`page === ...`) instead of React Router.
- No previous pathname-based routing was implemented.

## Global Filters
- No single shared global filter bar existed for date/channel/campaign/device/geo/segment.
- Existing pages use local controls and pass header content into `DashboardPage` (`dateRange`, `filters` slots).
- `DashboardPage` is the standard place for top-of-page filter UI composition.

## RBAC / Permissions
- Backend has explicit RBAC checks for alert mutations (admin/editor) in `backend/app/main.py` via `X-User-Role`.
- Read operations generally remain open; no frontend-wide RBAC helper existed.

## Settings / Config
- Global settings are modeled in `backend/app/main.py` (`Settings`) and persisted at `backend/app/data/settings.json`.
- Frontend Settings module reads/writes full settings payload at `/api/settings`.

## Feature Flags
- No dedicated feature-flag service existed.
- Existing best-fit pattern is extending `Settings` with a `feature_flags` section.

## API Client and Error Handling
- Frontend convention is page-local `fetch(...)` in React Query hooks.
- Typical pattern: `if (!res.ok) throw new Error(...)`; some non-critical queries return safe fallbacks.

## Page Layout Conventions
- `frontend/src/components/dashboard/DashboardPage.tsx` is the shared shell:
  - title, description
  - optional date range and filter slots
  - loading/error/empty states
  - page alert bell launcher
