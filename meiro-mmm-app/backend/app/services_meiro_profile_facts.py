"""Canonical DB-backed Meiro profile facts."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import MeiroProfileFact


def _profile_id(profile: Dict[str, Any], fallback_idx: int) -> str:
    customer = profile.get("customer")
    user = profile.get("user")
    value = (
        profile.get("customer_id")
        or profile.get("profile_id")
        or profile.get("id")
        or (customer.get("id") if isinstance(customer, dict) else None)
        or (user.get("id") if isinstance(user, dict) else None)
    )
    return str(value or f"anon-profile-{fallback_idx}")


def upsert_meiro_profile_facts(
    db: Session,
    *,
    profiles: List[Any],
    raw_batch_db_id: Optional[int] = None,
    source_snapshot_id: Optional[str] = None,
    reset: bool = False,
) -> int:
    normalized_profiles = {
        _profile_id(profile, idx): profile
        for idx, profile in enumerate(profiles)
        if isinstance(profile, dict)
    }
    if reset:
        db.query(MeiroProfileFact).delete(synchronize_session=False)
        db.commit()
    if not normalized_profiles:
        return 0

    existing_rows = (
        db.query(MeiroProfileFact)
        .filter(MeiroProfileFact.profile_id.in_(list(normalized_profiles.keys())))
        .all()
    )
    existing_by_profile_id = {row.profile_id: row for row in existing_rows}
    now = datetime.utcnow()
    updated = 0
    for profile_id, profile in normalized_profiles.items():
        row = existing_by_profile_id.get(profile_id)
        if row is None:
            db.add(
                MeiroProfileFact(
                    profile_id=profile_id,
                    raw_batch_db_id=raw_batch_db_id,
                    source_snapshot_id=source_snapshot_id,
                    profile_json=profile,
                    updated_at=now,
                )
            )
        else:
            row.raw_batch_db_id = raw_batch_db_id
            row.source_snapshot_id = source_snapshot_id
            row.profile_json = profile
            row.updated_at = now
        updated += 1
    db.commit()
    return updated


def list_meiro_profile_facts(
    db: Session,
    *,
    profile_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    query = db.query(MeiroProfileFact).order_by(MeiroProfileFact.updated_at.desc(), MeiroProfileFact.id.desc())
    normalized_profile_ids = [str(item).strip() for item in (profile_ids or []) if str(item).strip()]
    if normalized_profile_ids:
        query = query.filter(MeiroProfileFact.profile_id.in_(normalized_profile_ids))
    if limit is not None:
        query = query.limit(max(1, min(50000, int(limit))))
    return [dict(row.profile_json or {}) for row in query.all()]
