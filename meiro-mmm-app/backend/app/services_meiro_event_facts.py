"""Canonical DB-backed Meiro raw event facts."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy.exc import DatabaseError, OperationalError

from app.models_config_dq import MeiroEventFact

logger = logging.getLogger(__name__)


class MeiroEventFactsUnavailableError(RuntimeError):
    pass


def _is_retryable_or_corrupt_sqlite_error(exc: Exception) -> bool:
    message = str(getattr(exc, "orig", exc) or exc).lower()
    markers = (
        "disk i/o error",
        "database is locked",
        "database table is locked",
        "unable to open database file",
        "readonly database",
        "cannot commit transaction",
        "database disk image is malformed",
        "malformed",
    )
    return any(marker in message for marker in markers)


def _unwrap_event(item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, dict) and isinstance(item.get("event_payload"), dict):
        return item.get("event_payload")
    return item if isinstance(item, dict) else None


def _parse_event_ts(value: Any) -> Optional[datetime]:
    if value in (None, "", []):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _event_uid(event: Dict[str, Any]) -> str:
    explicit = event.get("event_id") or event.get("id")
    if explicit not in (None, "", []):
        return str(explicit)
    return hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _profile_id(event: Dict[str, Any], fallback_idx: int) -> str:
    customer = event.get("customer")
    profile = event.get("profile")
    candidate = (
        event.get("customer_id")
        or event.get("profile_id")
        or event.get("lead_id")
        or event.get("user_id")
        or event.get("person_id")
        or event.get("external_id")
        or (customer.get("id") if isinstance(customer, dict) else None)
        or (profile.get("id") if isinstance(profile, dict) else None)
        or event.get("id")
    )
    return str(candidate or f"anon-event-{fallback_idx}")


def upsert_meiro_event_facts(
    db: Session,
    *,
    raw_events: List[Any],
    raw_batch_db_id: Optional[int] = None,
    source_snapshot_id: Optional[str] = None,
    reset: bool = False,
) -> int:
    if reset:
        db.query(MeiroEventFact).delete(synchronize_session=False)
        db.commit()

    normalized: Dict[str, Dict[str, Any]] = {}
    for idx, item in enumerate(raw_events):
        event = _unwrap_event(item)
        if event is None:
            continue
        uid = _event_uid(event)
        normalized[uid] = {
            "event_uid": uid,
            "profile_id": _profile_id(event, idx),
            "event_ts": _parse_event_ts(
                event.get("ts")
                or event.get("timestamp")
                or event.get("occurred_at")
                or event.get("created_at")
                or event.get("event_date")
                or event.get("date")
            ),
            "event_name": str(event.get("event_name") or event.get("event_type") or event.get("name") or event.get("type") or ""),
            "event_json": event,
        }
    if not normalized:
        return 0

    try:
        existing_rows = (
            db.query(MeiroEventFact)
            .filter(MeiroEventFact.event_uid.in_(list(normalized.keys())))
            .all()
        )
    except (OperationalError, DatabaseError) as exc:
        db.rollback()
        if _is_retryable_or_corrupt_sqlite_error(exc):
            raise MeiroEventFactsUnavailableError(str(exc)) from exc
        raise
    existing_by_uid = {row.event_uid: row for row in existing_rows}
    now = datetime.utcnow()
    upserted = 0
    try:
        for uid, payload in normalized.items():
            row = existing_by_uid.get(uid)
            if row is None:
                db.add(
                    MeiroEventFact(
                        event_uid=uid,
                        profile_id=payload["profile_id"],
                        raw_batch_db_id=raw_batch_db_id,
                        source_snapshot_id=source_snapshot_id,
                        event_ts=payload["event_ts"],
                        event_name=payload["event_name"] or None,
                        event_json=payload["event_json"],
                        updated_at=now,
                    )
                )
            else:
                row.profile_id = payload["profile_id"]
                row.raw_batch_db_id = raw_batch_db_id
                row.source_snapshot_id = source_snapshot_id
                row.event_ts = payload["event_ts"]
                row.event_name = payload["event_name"] or None
                row.event_json = payload["event_json"]
                row.updated_at = now
            upserted += 1
        db.commit()
    except (OperationalError, DatabaseError) as exc:
        db.rollback()
        if _is_retryable_or_corrupt_sqlite_error(exc):
            raise MeiroEventFactsUnavailableError(str(exc)) from exc
        raise
    return upserted


def list_meiro_event_facts(
    db: Session,
    *,
    profile_ids: Optional[List[str]] = None,
    after_raw_batch_db_id: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    try:
        query = db.query(MeiroEventFact).order_by(MeiroEventFact.event_ts.asc(), MeiroEventFact.id.asc())
        normalized_profile_ids = [str(item).strip() for item in (profile_ids or []) if str(item).strip()]
        if normalized_profile_ids:
            query = query.filter(MeiroEventFact.profile_id.in_(normalized_profile_ids))
        if after_raw_batch_db_id is not None:
            query = query.filter(MeiroEventFact.raw_batch_db_id > max(0, int(after_raw_batch_db_id)))
        if limit is not None:
            query = query.limit(max(1, min(50000, int(limit))))
        return [dict(row.event_json or {}) for row in query.all()]
    except (OperationalError, DatabaseError) as exc:
        db.rollback()
        if _is_retryable_or_corrupt_sqlite_error(exc):
            logger.warning("Meiro event facts unavailable; falling back to archive/state paths", exc_info=True)
            return []
        raise
