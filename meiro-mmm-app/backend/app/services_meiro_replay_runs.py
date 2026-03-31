"""DB-backed persistence for Meiro replay run history."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import MeiroReplayRun


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _serialize(row: MeiroReplayRun) -> Dict[str, Any]:
    started_at = row.started_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z") if row.started_at else None
    completed_at = row.completed_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z") if row.completed_at else None
    payload = dict(row.result_json or {})
    at = completed_at or started_at
    return {
        "run_id": row.run_id,
        "scope": row.scope,
        "status": row.status,
        "trigger": row.trigger,
        "archive_source": row.archive_source,
        "replay_mode": row.replay_mode,
        "reason": row.reason,
        "started_at": started_at,
        "completed_at": completed_at,
        "latest_event_batch_db_id": row.latest_event_batch_db_id,
        "archive_entries_seen": row.archive_entries_seen,
        "archive_entries_used": row.archive_entries_used,
        "profiles_reconstructed": row.profiles_reconstructed,
        "quarantine_count": row.quarantine_count,
        "persisted_count": row.persisted_count,
        "at": at,
        "result_summary": payload,
        "result_json": payload,
    }


def record_meiro_replay_run(
    db: Session,
    *,
    scope: str,
    status: str,
    trigger: Optional[str] = None,
    archive_source: Optional[str] = None,
    replay_mode: Optional[str] = None,
    reason: Optional[str] = None,
    started_at: Optional[str] = None,
    completed_at: Optional[str] = None,
    latest_event_batch_db_id: Optional[int] = None,
    archive_entries_seen: Optional[int] = None,
    archive_entries_used: Optional[int] = None,
    profiles_reconstructed: Optional[int] = None,
    quarantine_count: Optional[int] = None,
    persisted_count: Optional[int] = None,
    result_json: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    item = MeiroReplayRun(
        run_id=str(uuid.uuid4()),
        scope=str(scope or "").strip().lower() or "auto",
        status=str(status or "").strip().lower() or "unknown",
        trigger=str(trigger or "").strip().lower() or None,
        archive_source=str(archive_source or "").strip().lower() or None,
        replay_mode=str(replay_mode or "").strip().lower() or None,
        reason=reason,
        started_at=_parse_iso_datetime(started_at) or datetime.utcnow(),
        completed_at=_parse_iso_datetime(completed_at),
        latest_event_batch_db_id=int(latest_event_batch_db_id) if latest_event_batch_db_id is not None else None,
        archive_entries_seen=int(archive_entries_seen) if archive_entries_seen is not None else None,
        archive_entries_used=int(archive_entries_used) if archive_entries_used is not None else None,
        profiles_reconstructed=int(profiles_reconstructed) if profiles_reconstructed is not None else None,
        quarantine_count=int(quarantine_count) if quarantine_count is not None else None,
        persisted_count=int(persisted_count) if persisted_count is not None else None,
        result_json=result_json or {},
    )
    db.add(item)
    db.flush()
    payload = _serialize(item)
    db.commit()
    return payload


def list_meiro_replay_runs(
    db: Session,
    *,
    scope: Optional[str] = None,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    query = db.query(MeiroReplayRun)
    if scope:
        query = query.filter(MeiroReplayRun.scope == str(scope or "").strip().lower())
    rows = (
        query.order_by(MeiroReplayRun.started_at.desc(), MeiroReplayRun.id.desc())
        .limit(max(1, min(500, int(limit or 25))))
        .all()
    )
    return [_serialize(row) for row in rows]
