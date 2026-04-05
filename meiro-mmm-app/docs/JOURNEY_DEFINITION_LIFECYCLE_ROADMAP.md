# Journey Definition Lifecycle Roadmap

## Goal

Turn journey definitions into manageable product assets instead of one-way metadata rows.

## User Value

- Users can safely archive a definition without guessing what breaks.
- Archived definitions remain visible and restorable.
- Users can duplicate a definition to branch analysis without manual recreation.
- The UI clearly separates active definitions from archived/read-only ones.

## Lifecycle Model

- `active`
  - selectable for analysis
  - rebuildable
  - can be archived or duplicated
- `archived`
  - hidden by default
  - visible with `show archived`
  - read-only in the Journeys workspace
  - can be restored or duplicated

## First Slice

### Backend

- Add definition lifecycle summary endpoint:
  - `GET /api/journeys/definitions/{id}/lifecycle`
- Add restore endpoint:
  - `POST /api/journeys/definitions/{id}/restore`
- Add duplicate endpoint:
  - `POST /api/journeys/definitions/{id}/duplicate`
- Keep archive available:
  - `DELETE /api/journeys/definitions/{id}`
  - `POST /api/journeys/definitions/{id}/archive`
- Make archive non-destructive:
  - preserve generated outputs
  - preserve downstream references

### Dependency Summary

- saved views
- funnels
- hypotheses
- linked experiments
- alerts
- generated outputs:
  - journey instances
  - path daily rows
  - transition daily rows

### Frontend

- Add `Show archived` toggle in Journeys.
- Show lifecycle state on the selected definition.
- Show dependency summary and actionability.
- Add actions:
  - `Duplicate`
  - `Archive`
  - `Restore`
- Make archived definitions read-only in the workspace.

## Follow-up Slices

- Add `stale` / `needs rebuild` state.
  - implemented in the next slice via output timestamp comparison and direct rebuild CTA
- Add owner / usage metadata.
- Add hard delete only for definitions with zero dependencies and zero outputs.
- Add cross-links from lifecycle summary to dependent objects.
