# Access Control and RBAC Architecture Note

## Scope
This note documents:
- current auth/authorization behavior in the repository,
- the target enterprise RBAC model for implementation,
- rollout flags introduced in this change.

This change is documentation + feature flags only. No auth behavior is changed yet.

## Current State (Audit)

### Identity and auth
- No first-party user login/session system is present for product users.
- OAuth endpoints in `backend/app/main.py` (`/api/auth/*`) are connector integrations (Meta/Google/LinkedIn), not app-user authentication.
- API write authorization is currently driven by request headers:
  - `X-User-Id`
  - `X-User-Role`
- Frontend currently derives role from query/localStorage in places:
  - `frontend/src/modules/App.tsx`
  - `frontend/src/modules/Journeys.tsx`

### Authorization checks
- Permission checks are ad hoc and role-string based in endpoint handlers.
- Example pattern in `backend/app/main.py`:
  - alert/journey/funnel write endpoints gated to `admin/editor`.
  - view endpoints generally open to all.
- No central permission registry or middleware exists yet.

### Settings and feature flag patterns
- Settings are stored in `backend/app/data/settings.json`.
- Settings APIs:
  - `GET /api/settings`
  - `POST /api/settings`
- Frontend settings shell lives in `frontend/src/modules/Settings.tsx`.
- Feature flags already exist under `settings.feature_flags`.

### Audit logging
- There are domain-specific audits (e.g. model config audit table, expenses audit views), but no centralized workspace security audit log for user/role admin actions.

## Target Model

### Tenant model
- Adopt **workspace-scoped** access control as the primary tenant boundary.
- Default workspace id remains `default` until full workspace switching is implemented.

### Membership model
- A user belongs to a workspace through a membership.
- Membership points to a role (default/effective role for that workspace).

### RBAC model
- Use **atomic permissions** (e.g. `journeys.view`, `settings.manage`) as enforcement units.
- Roles are bundles of permissions.
- Authorization checks should be centralized in middleware/service helpers:
  - `has_permission(user, workspace, permission_key)`
  - `require_permission(permission_key)`
- Role checks should be exceptional (e.g. bootstrap safety), not the default.

### Enforcement location
- Resolve `current_user + current_workspace + effective_permissions` per request.
- Enforce at API boundary (dependency/middleware) and optionally service layer for defense in depth.

## Rollout Flags Added

Added to `settings.feature_flags`:
- `access_control_enabled` (default `false`)
- `custom_roles_enabled` (default `false`)
- `audit_log_enabled` (default `false`)
- `scim_enabled` (default `false`) placeholder
- `sso_enabled` (default `false`) placeholder

These flags are now present in:
- backend `FeatureFlags` model (`backend/app/main.py`)
- frontend feature flag typing/defaults (`frontend/src/modules/App.tsx`, `frontend/src/modules/Settings.tsx`)
- persisted default settings file (`backend/app/data/settings.json`)

## Implementation Plan (Next Prompts)
1. Add RBAC schema and migrations (`users/workspaces/memberships/roles/permissions`).
2. Introduce server-side session-backed auth context.
3. Add centralized permission resolver/dependencies.
4. Replace endpoint role-header checks with permission checks.
5. Build access control admin APIs + UI.
6. Add security audit log pipeline + UI.
