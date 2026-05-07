"""Audit persistence for upstream Meiro Pipes fix proposals."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


PROPOSALS_FILE = Path(__file__).resolve().parent / "data" / "meiro_pipes_fix_proposals.json"
MAX_PROPOSALS = 200
MAX_EVENTS_PER_PROPOSAL = 40
_CACHE: Dict[str, Any] = {"signature": None, "items": []}


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _file_signature(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
        return int(stat.st_mtime_ns), int(stat.st_size)
    except Exception:
        return 0, 0


def _load_items() -> List[Dict[str, Any]]:
    if not PROPOSALS_FILE.exists():
        return []
    signature = _file_signature(PROPOSALS_FILE)
    if _CACHE.get("signature") == signature:
        return list(_CACHE.get("items") or [])
    try:
        data = json.loads(PROPOSALS_FILE.read_text(encoding="utf-8"))
        items = data if isinstance(data, list) else []
        _CACHE["signature"] = signature
        _CACHE["items"] = list(items)
        return items
    except Exception:
        return []


def _save_items(items: List[Dict[str, Any]]) -> None:
    PROPOSALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    trimmed = items[:MAX_PROPOSALS]
    PROPOSALS_FILE.write_text(json.dumps(trimmed, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    _CACHE["signature"] = _file_signature(PROPOSALS_FILE)
    _CACHE["items"] = list(trimmed)


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _diagnostics_snapshot(diagnostics: Optional[Dict[str, Any]], *, events_analyzed: int = 0) -> Dict[str, Any]:
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    return {
        "events_analyzed": int(events_analyzed or diagnostics.get("events_examined") or 0),
        "events_examined": int(diagnostics.get("events_examined") or 0),
        "source_medium_share": _float_or_none(diagnostics.get("source_medium_share")),
        "conversion_linkage_share": _float_or_none(diagnostics.get("conversion_linkage_share")),
        "usable_event_name_share": _float_or_none(diagnostics.get("usable_event_name_share")),
        "identity_share": _float_or_none(diagnostics.get("identity_share")),
        "touchpoint_like_events": int(diagnostics.get("touchpoint_like_events") or 0),
        "conversion_like_events": int(diagnostics.get("conversion_like_events") or 0),
        "warnings": list(diagnostics.get("warnings") or [])[:10],
    }


def _route_snapshot(route_health: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    route_health = route_health if isinstance(route_health, dict) else {}
    routes = route_health.get("routes") if isinstance(route_health.get("routes"), list) else []
    return {
        "status": route_health.get("status"),
        "routes": [
            {
                "id": route.get("id"),
                "status": route.get("status"),
                "destination_slug": (route.get("destination") or {}).get("slug") if isinstance(route.get("destination"), dict) else None,
                "pipe_count": route.get("pipe_count"),
                "enabled_pipe_count": route.get("enabled_pipe_count"),
                "delivery_count_last_hour": route.get("delivery_count_last_hour"),
                "issues": route.get("issues") if isinstance(route.get("issues"), list) else [],
            }
            for route in routes
            if isinstance(route, dict)
        ][:8],
    }


def _delta(current: Optional[float], baseline: Optional[float]) -> Optional[float]:
    if current is None or baseline is None:
        return None
    return round(current - baseline, 4)


def _impact(item: Dict[str, Any]) -> Dict[str, Any]:
    baseline = item.get("baseline") if isinstance(item.get("baseline"), dict) else {}
    latest = item.get("latest_diagnostics") if isinstance(item.get("latest_diagnostics"), dict) else {}
    return {
        "source_medium_share_delta": _delta(
            _float_or_none(latest.get("source_medium_share")),
            _float_or_none(baseline.get("source_medium_share")),
        ),
        "conversion_linkage_share_delta": _delta(
            _float_or_none(latest.get("conversion_linkage_share")),
            _float_or_none(baseline.get("conversion_linkage_share")),
        ),
        "usable_event_name_share_delta": _delta(
            _float_or_none(latest.get("usable_event_name_share")),
            _float_or_none(baseline.get("usable_event_name_share")),
        ),
        "events_analyzed_delta": int(latest.get("events_analyzed") or 0) - int(baseline.get("events_analyzed") or 0),
    }


def _audit_summary(item: Dict[str, Any]) -> Dict[str, Any]:
    events = item.get("events") if isinstance(item.get("events"), list) else []
    last_event = events[-1] if events and isinstance(events[-1], dict) else {}
    return {
        "status": item.get("status") or "open",
        "first_seen_at": item.get("first_seen_at"),
        "last_seen_at": item.get("last_seen_at"),
        "seen_count": int(item.get("seen_count") or 0),
        "last_action": last_event.get("action"),
        "last_action_at": last_event.get("at"),
    }


def _with_tracking_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    proposal = dict(item.get("latest_proposal") or {})
    proposal["audit"] = _audit_summary(item)
    proposal["impact"] = _impact(item)
    return proposal


def upsert_pipes_fix_proposals(
    proposals: List[Dict[str, Any]],
    *,
    diagnostics: Optional[Dict[str, Any]] = None,
    route_health: Optional[Dict[str, Any]] = None,
    events_analyzed: int = 0,
) -> List[Dict[str, Any]]:
    """Persist generated proposals and return proposals with audit/impact fields."""
    now = _now_iso()
    current_diagnostics = _diagnostics_snapshot(diagnostics, events_analyzed=events_analyzed)
    current_route_health = _route_snapshot(route_health)
    items = _load_items()
    by_id = {str(item.get("id")): item for item in items if item.get("id")}

    tracked: List[Dict[str, Any]] = []
    for proposal in proposals:
        if not isinstance(proposal, dict) or not proposal.get("id"):
            continue
        proposal_id = str(proposal["id"])
        existing = by_id.get(proposal_id)
        if not existing:
            existing = {
                "id": proposal_id,
                "type": proposal.get("type"),
                "title": proposal.get("title"),
                "severity": proposal.get("severity"),
                "status": "open",
                "first_seen_at": now,
                "last_seen_at": now,
                "seen_count": 0,
                "events": [],
                "baseline": current_diagnostics,
            }
            items.insert(0, existing)
            by_id[proposal_id] = existing

        existing["type"] = proposal.get("type") or existing.get("type")
        existing["title"] = proposal.get("title") or existing.get("title")
        existing["severity"] = proposal.get("severity") or existing.get("severity")
        existing["last_seen_at"] = now
        existing["seen_count"] = int(existing.get("seen_count") or 0) + 1
        existing["latest_proposal"] = dict(proposal)
        existing["latest_diagnostics"] = current_diagnostics
        existing["route_health"] = current_route_health
        tracked.append(_with_tracking_fields(existing))

    _save_items(sorted(items, key=lambda item: str(item.get("last_seen_at") or ""), reverse=True))
    return tracked


def list_pipes_fix_proposals(limit: int = 50) -> List[Dict[str, Any]]:
    return [_with_tracking_fields(item) for item in _load_items()[: max(1, min(limit, MAX_PROPOSALS))]]


def record_pipes_fix_proposal_event(
    proposal_id: str,
    *,
    action: str,
    note: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    allowed_actions = {"copied_prompt", "copied_transform", "marked_applied", "marked_verified"}
    if action not in allowed_actions:
        raise ValueError(f"Unsupported proposal action: {action}")
    items = _load_items()
    now = _now_iso()
    for item in items:
        if str(item.get("id") or "") != str(proposal_id):
            continue
        events = item.get("events") if isinstance(item.get("events"), list) else []
        events.append({"at": now, "action": action, "note": note, "metadata": metadata or {}})
        item["events"] = events[-MAX_EVENTS_PER_PROPOSAL:]
        if action == "marked_verified":
            item["status"] = "verified"
        elif action == "marked_applied":
            item["status"] = "applied"
        elif item.get("status") in {None, "open"}:
            item["status"] = "copied"
        _save_items(items)
        return _with_tracking_fields(item)
    return None
