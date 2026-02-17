# Cypress RBAC E2E

This suite validates critical workspace access-control flows against a running backend.

## Preconditions

- Backend API is running (default: `http://localhost:8000`).
- Access-control flags can be updated by admin headers.
- Cypress is installed in `frontend` (`npm i -D cypress`).

## Run

- Full Cypress run: `npm run e2e:cypress`
- RBAC suite only: `npm run e2e:cypress:rbac`
- Custom API base URL: `CYPRESS_BASE_URL=http://localhost:8000 npm run e2e:cypress:rbac`

## Covered flows

- Admin invites user; invited user accepts and becomes Viewer.
- Viewer denied from admin access-control endpoints with standard RBAC denial payload.
- Admin creates custom role and assigns membership role.
- Assigned user permissions reflect custom role constraints.
- Audit log contains membership role-change event.
