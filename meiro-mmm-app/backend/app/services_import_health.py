from __future__ import annotations

from typing import Any, Dict


IMPORT_SOURCES = ["google_ads", "meta_ads", "linkedin_ads"]


def import_status_from_state(state: Dict[str, Any]) -> str:
    if not state:
        return "unknown"
    status = state.get("status") or "unknown"
    if status in ("Healthy", "Stale", "Broken", "Partial"):
        return status
    if state.get("last_error"):
        return "Broken"
    if state.get("last_success_at"):
        return "Healthy"
    if state.get("last_attempt_at"):
        return "Partial"
    return "unknown"


def build_import_health(
    import_sync_state: Dict[str, Dict[str, Any]],
    sync_in_progress: set[str],
) -> Dict[str, Any]:
    result = []
    for source in IMPORT_SOURCES:
        state = import_sync_state.get(source, {})
        result.append(
            {
                "source": source,
                "status": import_status_from_state(state),
                "last_success_at": state.get("last_success_at"),
                "last_attempt_at": state.get("last_attempt_at"),
                "records_imported": state.get("records_imported"),
                "period_start": state.get("period_start"),
                "period_end": state.get("period_end"),
                "last_error": state.get("last_error"),
                "action_hint": state.get("action_hint"),
                "syncing": source in sync_in_progress,
            }
        )

    status_order = {"Broken": 3, "Stale": 2, "Partial": 1, "Healthy": 0, "unknown": -1}
    attempted = [row for row in result if row["last_attempt_at"]]
    overall = "Healthy"
    if attempted:
        worst = max(attempted, key=lambda row: status_order.get(row["status"], -1))
        overall = worst["status"] if worst["status"] != "unknown" else "Stale"
    return {"sources": result, "overall_freshness": overall}
