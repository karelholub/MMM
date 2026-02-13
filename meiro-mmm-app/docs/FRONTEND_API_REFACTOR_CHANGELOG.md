# Frontend API Refactor Changelog

## Scope
Refactor frontend network calls to use `src/lib/apiClient.ts` and remove duplicated request/auth/error logic.

## Included
- Added shared API helper module:
  - `frontend/src/lib/apiClient.ts`
- Migrated modules/connectors to use `apiGetJson`, `apiSendJson`, `apiRequest`, `withQuery`.
- Removed direct `fetch` usage in the migrated modules/connectors.
- Applied safe cleanup for unused imports/local variables in touched files.
- Added usage documentation:
  - `docs/FRONTEND_API_CLIENT.md`

## Verification
- `cd frontend && npx tsc --noEmit --noUnusedLocals --noUnusedParameters`
- `cd frontend && npm run -s build`

Both commands pass.
