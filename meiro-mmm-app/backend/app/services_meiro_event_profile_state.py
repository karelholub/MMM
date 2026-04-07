"""DB-backed canonical state for event-derived Meiro profiles."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import MeiroEventProfileState


def _stable_item_key(item: Dict[str, Any], *, keys: Iterable[str]) -> str:
    for key in keys:
        value = item.get(key)
        if value not in (None, "", []):
            return f"{key}:{value}"
    digest = hashlib.sha256(json.dumps(item, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"sha:{digest}"


def _merge_profile(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing)
    merged["customer_id"] = str(incoming.get("customer_id") or existing.get("customer_id") or "")
    merged["converted"] = bool(existing.get("converted")) or bool(incoming.get("converted"))
    try:
        merged["conversion_value"] = max(
            float(existing.get("conversion_value") or 0.0),
            float(incoming.get("conversion_value") or 0.0),
        )
    except Exception:
        merged["conversion_value"] = float(existing.get("conversion_value") or incoming.get("conversion_value") or 0.0)

    touchpoints: Dict[str, Dict[str, Any]] = {}
    for item in (existing.get("touchpoints") or []):
        if isinstance(item, dict):
            touchpoints[_stable_item_key(item, keys=("id", "click_id", "impression_id"))] = item
    for item in (incoming.get("touchpoints") or []):
        if isinstance(item, dict):
            touchpoints[_stable_item_key(item, keys=("id", "click_id", "impression_id"))] = item
    conversions: Dict[str, Dict[str, Any]] = {}
    for item in (existing.get("conversions") or []):
        if isinstance(item, dict):
            conversions[_stable_item_key(item, keys=("conversion_id", "order_id", "lead_id", "event_id", "id"))] = item
    for item in (incoming.get("conversions") or []):
        if isinstance(item, dict):
            conversions[_stable_item_key(item, keys=("conversion_id", "order_id", "lead_id", "event_id", "id"))] = item

    merged["touchpoints"] = sorted(
        touchpoints.values(),
        key=lambda item: str(item.get("timestamp") or item.get("ts") or ""),
    )
    merged["conversions"] = sorted(
        conversions.values(),
        key=lambda item: str(item.get("timestamp") or item.get("ts") or ""),
    )
    segments: Dict[str, Dict[str, Any]] = {}
    for item in (existing.get("segments") or []):
        if isinstance(item, dict):
            key = str(item.get("id") or item.get("name") or "").strip()
            if key:
                segments[key] = {"id": str(item.get("id") or key), "name": str(item.get("name") or key)}
    for item in (incoming.get("segments") or []):
        if isinstance(item, dict):
            key = str(item.get("id") or item.get("name") or "").strip()
            if key:
                current = segments.get(key) or {"id": str(item.get("id") or key), "name": str(item.get("name") or key)}
                if not current.get("name") and item.get("name"):
                    current["name"] = str(item.get("name"))
                segments[key] = current
    merged["segments"] = list(segments.values())
    merged["_event_count"] = int(existing.get("_event_count") or 0) + int(incoming.get("_event_count") or 0)
    return merged


def upsert_meiro_event_profile_state(
    db: Session,
    *,
    profiles: List[Dict[str, Any]],
    latest_event_batch_db_id: Optional[int] = None,
    source_snapshot_id: Optional[str] = None,
    reset: bool = False,
) -> int:
    normalized_profiles = {
        str(profile.get("customer_id") or "").strip(): profile
        for profile in profiles
        if isinstance(profile, dict) and str(profile.get("customer_id") or "").strip()
    }
    if reset:
        db.query(MeiroEventProfileState).delete(synchronize_session=False)
        db.commit()
    if not normalized_profiles:
        return 0

    existing_rows = (
        db.query(MeiroEventProfileState)
        .filter(MeiroEventProfileState.profile_id.in_(list(normalized_profiles.keys())))
        .all()
    )
    existing_by_profile_id = {row.profile_id: row for row in existing_rows}
    updated = 0
    now = datetime.utcnow()
    for profile_id, incoming_profile in normalized_profiles.items():
        existing_row = existing_by_profile_id.get(profile_id)
        merged_profile = (
            _merge_profile(existing_row.profile_json or {}, incoming_profile)
            if existing_row is not None
            else incoming_profile
        )
        if existing_row is None:
            db.add(
                MeiroEventProfileState(
                    profile_id=profile_id,
                    latest_event_batch_db_id=latest_event_batch_db_id,
                    source_snapshot_id=source_snapshot_id,
                    profile_json=merged_profile,
                    updated_at=now,
                )
            )
        else:
            existing_row.profile_json = merged_profile
            existing_row.latest_event_batch_db_id = latest_event_batch_db_id
            existing_row.source_snapshot_id = source_snapshot_id
            existing_row.updated_at = now
        updated += 1
    db.commit()
    return updated


def list_meiro_event_profile_state(
    db: Session,
    *,
    profile_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    query = db.query(MeiroEventProfileState).order_by(MeiroEventProfileState.updated_at.desc(), MeiroEventProfileState.id.desc())
    normalized_profile_ids = [str(item).strip() for item in (profile_ids or []) if str(item).strip()]
    if normalized_profile_ids:
        query = query.filter(MeiroEventProfileState.profile_id.in_(normalized_profile_ids))
    if limit is not None:
        query = query.limit(max(1, min(50000, int(limit))))
    return [dict(row.profile_json or {}) for row in query.all()]
