"""DB-backed storage for replayed Meiro profile snapshots."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import MeiroReplaySnapshot


def _serialize(row: MeiroReplaySnapshot) -> Dict[str, Any]:
    created_at = row.created_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z") if row.created_at else None
    return {
        "snapshot_id": row.snapshot_id,
        "source_kind": row.source_kind,
        "replay_mode": row.replay_mode,
        "latest_event_batch_db_id": row.latest_event_batch_db_id,
        "archive_entries_used": row.archive_entries_used,
        "profiles_count": row.profiles_count,
        "profiles_json": list(row.profiles_json or []),
        "context_json": dict(row.context_json or {}),
        "created_at": created_at,
    }


def create_meiro_replay_snapshot(
    db: Session,
    *,
    source_kind: str,
    profiles_json: List[Dict[str, Any]],
    replay_mode: Optional[str] = None,
    latest_event_batch_db_id: Optional[int] = None,
    archive_entries_used: Optional[int] = None,
    context_json: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    item = MeiroReplaySnapshot(
        snapshot_id=str(uuid.uuid4()),
        source_kind=str(source_kind or "").strip().lower() or "profiles",
        replay_mode=str(replay_mode or "").strip().lower() or None,
        latest_event_batch_db_id=int(latest_event_batch_db_id) if latest_event_batch_db_id is not None else None,
        archive_entries_used=int(archive_entries_used) if archive_entries_used is not None else None,
        profiles_count=len(profiles_json or []),
        profiles_json=list(profiles_json or []),
        context_json=context_json or {},
    )
    db.add(item)
    db.flush()
    payload = _serialize(item)
    db.commit()
    return payload


def get_meiro_replay_snapshot(db: Session, snapshot_id: str) -> Optional[Dict[str, Any]]:
    row = db.query(MeiroReplaySnapshot).filter(MeiroReplaySnapshot.snapshot_id == str(snapshot_id or "").strip()).first()
    if not row:
        return None
    return _serialize(row)
