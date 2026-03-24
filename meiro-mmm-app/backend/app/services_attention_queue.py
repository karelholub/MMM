from __future__ import annotations

from typing import Any, Dict, List


STATUS_PRIORITY = {
    "blocked": 300,
    "warning": 200,
    "stale": 180,
    "ready": 100,
}

SOURCE_PRIORITY = {
    "taxonomy": 60,
    "kpi": 55,
    "journeys": 50,
    "meiro": 45,
    "data_sources": 40,
}


def _normalize_confidence_score(confidence: Dict[str, Any]) -> float:
    raw = confidence.get("score")
    if raw is None:
        return 50.0
    try:
        score = float(raw)
    except (TypeError, ValueError):
        return 50.0
    return score * 100.0 if score <= 1.0 else score


def build_attention_queue(
    *,
    decisions: Dict[str, Dict[str, Any]],
    limit: int = 8,
) -> List[Dict[str, Any]]:
    ranked: Dict[str, Dict[str, Any]] = {}

    for source, decision in decisions.items():
        if not isinstance(decision, dict):
            continue
        source_status = str(decision.get("status") or "ready").strip().lower()
        status_score = STATUS_PRIORITY.get(source_status, 0)
        source_score = SOURCE_PRIORITY.get(source, 0)
        confidence_score = _normalize_confidence_score(decision.get("confidence") or {})

        for index, action in enumerate(decision.get("recommended_actions") or []):
            if not isinstance(action, dict):
                continue
            action_id = str(action.get("id") or "").strip()
            if not action_id:
                continue
            priority_score = (
                status_score
                + source_score
                + max(0.0, 100.0 - confidence_score) * 0.2
                + (5 if action.get("requires_review") else 0)
                - index * 3
            )
            enriched = {
                **action,
                "domain": action.get("domain") or source,
                "source": source,
                "source_status": source_status,
                "source_confidence": decision.get("confidence"),
                "priority_score": round(priority_score, 2),
            }
            existing = ranked.get(action_id)
            if not existing or enriched["priority_score"] > existing["priority_score"]:
                ranked[action_id] = enriched

    return sorted(
        ranked.values(),
        key=lambda item: (-float(item.get("priority_score") or 0.0), str(item.get("label") or "")),
    )[:limit]
