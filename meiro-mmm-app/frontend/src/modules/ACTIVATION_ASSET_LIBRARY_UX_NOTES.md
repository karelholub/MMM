# Activation Asset Library UX Notes

## State Model

- Asset lifecycle is shown as a separate badge: `draft`, `active`, or `archived`.
- Launch readiness is shown as a separate badge: `ready`, `warning`, `blocked`, or `No requirements`.
- `No requirements` is not a readiness pass. It means no campaign or calendar slot is connected yet, so launch readiness is unavailable.
- Reuse is factual: linked slot count only. The UI does not imply reuse is approved or preferred.

## Library Rows

Rows show the asset or missing required slot name, lifecycle, readiness, owner, market/team, approval status, review freshness, compatibility, reuse count, and evidence source with timestamp.

Readiness copy:

- Missing required link: "Required slot has no linked asset."
- Unresolved approval: "Required approval is not resolved."
- Incompatible channel: "Channel does not match this slot."
- Incompatible template: "Template does not match this slot."
- Incompatible placement: "Placement does not match this slot."
- Missing metadata: "Required metadata is missing."
- Missing owner: "Owner is missing."
- Expired review: "Review has expired."
- Draft or archived asset: "Asset lifecycle is not active."

## Detail Panel

The detail panel keeps required slots separate from the asset record. Each slot lists object type, object id, whether the slot is required, the required channel/template/placement, and the slot readiness. Missing links use blocking copy and do not fall back to asset lifecycle.

Evidence is shown as source links, reference evidence source, and update/review timestamps when available. Compatibility copy is evidence-backed and limited to configured channel/template/placement fields.

## Bulk Filters

The first pass includes filters for owner, channel, market/team, readiness, approval, review freshness, and reuse.

Reuse filters use:

- Reused: two or more linked slots.
- Single linked slot: exactly one linked slot.
- No linked slots: no campaign or calendar requirements attached.

## API Fields Needed Beyond KAR-23

- Asset-list endpoint should return per-asset readiness and reuse summaries, not only object-level readiness. The current UI derives these from calendar references and asset records.
- References need a display label for the campaign, calendar item, or decision, not only `object_type` and `object_id`.
- Evidence should include a normalized `source`, `source_url`, `observed_at`, and `updated_by` for both asset records and readiness checks.
- Required slots should expose slot names such as "Hero image" or "Primary social video" rather than only placement/template fields.
- Approval should expose normalized `status`, `owner`, `updated_at`, and `resolved_at`.
- Review freshness should expose normalized `last_reviewed_at`, `review_expires_at`, and `review_policy`.
- Compatibility should expose explicit match results per field so the UI does not need to infer from blocker codes.
- Bulk filters need backend support for owner, channel, market/team, readiness, approval, review, and reuse once the asset count is large enough for server-side pagination.
