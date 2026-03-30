# Reliability Refactor Roadmap

## Goal

Increase reliability of profile/raw-event processing, stabilize calculations and suggestions, improve runtime performance, and consolidate UI patterns for large entity sets.

## Phase 1: Dataset Loading and Invalidation

- Replace ad hoc module-level journey state with a centralized cache and explicit invalidation.
- Ensure every import path invalidates the resolved dataset after persistence.
- Remove stale-data coupling between settings changes, revenue config changes, and downstream analytics reads.
- Add focused tests for cache reuse and invalidation behavior.

Status:
- Implemented in this change set.

## Phase 2: Meiro Ingestion Hardening

- Move Meiro profile/raw-event ingestion away from mutable JSON files toward DB-backed batch records.
- Introduce immutable raw batch tables with checksums, parser version, source kind, and replay status.
- Persist canonicalized touchpoints, conversions, and quarantine outcomes with deterministic dedupe keys.
- Make replay idempotent per batch instead of rebuilding from whole-file archives.

## Phase 3: Stable Facts and Aggregates

- Split conversion-path storage into canonical facts plus derived analytics views.
- Stop recomputing revenue entries and legacy journey payloads on every hot-path read.
- Add incremental daily aggregates for channel, campaign, path, KPI, and taxonomy coverage.
- Separate measured campaign spend from allocated fallback spend in reporting.

## Phase 4: Suggestions and Diagnostics

- Persist profiling metrics for event naming, identity coverage, source/medium completeness, dedupe coverage, and conversion linkage.
- Generate KPI, taxonomy, and sanitation suggestions from persisted profiles instead of repeated full-dataset scans.
- Track suggestion acceptance and post-apply quality deltas to improve future ranking.

## Phase 5: UI Consolidation and Scale

- Consolidate repeated diagnostics/status widgets into shared components.
- Build one reusable analytics table with server-side pagination, sticky columns, and virtualization for large datasets.
- Add dedicated reliability views:
  - Ingestion funnel chart
  - Unknown-share and dedupe-coverage trends
  - Campaign health table with confidence and allocated-spend share
  - Source/medium unresolved-pattern table with projected coverage lift
