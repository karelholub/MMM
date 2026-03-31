"""Persistence helpers for immutable Meiro raw ingestion batches."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models_config_dq import MeiroRawBatch


def _parse_received_at(value: Optional[str]) -> datetime:
    if not value:
        return datetime.utcnow()
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()


def _normalize_source_kind(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"profiles", "events"}:
        return normalized
    return "unknown"


def _base_query(
    db: Session,
    *,
    source_kind: str,
    after_db_id: Optional[int] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
):
    query = db.query(MeiroRawBatch).filter(MeiroRawBatch.source_kind == _normalize_source_kind(source_kind))
    if after_db_id is not None:
        query = query.filter(MeiroRawBatch.id > max(0, int(after_db_id)))
    if since:
        query = query.filter(MeiroRawBatch.received_at >= _parse_received_at(since))
    if until:
        query = query.filter(MeiroRawBatch.received_at <= _parse_received_at(until))
    return query


def _serialize_batch(row: MeiroRawBatch) -> Dict[str, Any]:
    payload = dict(row.payload_json or {})
    payload.setdefault("batch_id", row.batch_id)
    payload.setdefault("db_row_id", int(row.id))
    payload.setdefault("source_kind", row.source_kind)
    payload.setdefault("ingestion_channel", row.ingestion_channel)
    payload.setdefault("replace", bool(row.replace))
    payload.setdefault("received_count", int(row.records_count or 0))
    if row.parser_version:
        payload.setdefault("parser_version", row.parser_version)
    if row.payload_shape:
        payload.setdefault("payload_shape", row.payload_shape)
    if row.received_at and not payload.get("received_at"):
        payload["received_at"] = row.received_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return payload


def _clone_batch(row: MeiroRawBatch) -> MeiroRawBatch:
    clone = MeiroRawBatch(
        batch_id=row.batch_id,
        source_kind=row.source_kind,
        ingestion_channel=row.ingestion_channel,
        received_at=row.received_at,
        parser_version=row.parser_version,
        payload_shape=row.payload_shape,
        replace=bool(row.replace),
        records_count=int(row.records_count or 0),
        payload_json=dict(row.payload_json or {}),
        metadata_json=dict(row.metadata_json or {}),
    )
    clone.id = int(row.id) if row.id is not None else None
    return clone


def record_meiro_raw_batch(
    db: Session,
    *,
    source_kind: str,
    ingestion_channel: str,
    payload_json: Dict[str, Any],
    received_at: Optional[str] = None,
    parser_version: Optional[str] = None,
    payload_shape: Optional[str] = None,
    replace: bool = False,
    records_count: int = 0,
    metadata_json: Optional[Dict[str, Any]] = None,
) -> MeiroRawBatch:
    item = MeiroRawBatch(
        batch_id=str(uuid.uuid4()),
        source_kind=str(source_kind or "").strip().lower() or "unknown",
        ingestion_channel=str(ingestion_channel or "").strip().lower() or "unknown",
        received_at=_parse_received_at(received_at),
        parser_version=parser_version,
        payload_shape=payload_shape,
        replace=bool(replace),
        records_count=max(0, int(records_count or 0)),
        payload_json=payload_json,
        metadata_json=metadata_json or {},
    )
    db.add(item)
    db.flush()
    detached = _clone_batch(item)
    db.commit()
    return detached


def list_meiro_raw_batches(
    db: Session,
    *,
    source_kind: str,
    limit: Optional[int] = 100,
    after_db_id: Optional[int] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> List[Dict[str, Any]]:
    query = _base_query(db, source_kind=source_kind, after_db_id=after_db_id, since=since, until=until).order_by(
        MeiroRawBatch.received_at.desc(),
        MeiroRawBatch.id.desc(),
    )
    if limit is not None:
        query = query.limit(max(1, min(50000, int(limit))))
    return [_serialize_batch(row) for row in query.all()]


def get_meiro_raw_batch_status(db: Session, *, source_kind: str) -> Dict[str, Any]:
    normalized = _normalize_source_kind(source_kind)
    query = _base_query(db, source_kind=normalized)
    entries = int(query.count())
    label = "profiles_received" if normalized == "profiles" else "events_received"
    if entries <= 0:
        return {
            "available": False,
            "entries": 0,
            label: 0,
            "last_received_at": None,
            "latest_batch_db_id": None,
            "parser_versions": [],
        }

    records_received = (
        db.query(func.coalesce(func.sum(MeiroRawBatch.records_count), 0))
        .filter(MeiroRawBatch.source_kind == normalized)
        .scalar()
        or 0
    )
    latest = (
        db.query(MeiroRawBatch.id, MeiroRawBatch.received_at)
        .filter(MeiroRawBatch.source_kind == normalized)
        .order_by(MeiroRawBatch.received_at.desc(), MeiroRawBatch.id.desc())
        .first()
    )
    parser_versions = [
        value
        for (value,) in (
            db.query(MeiroRawBatch.parser_version)
            .filter(MeiroRawBatch.source_kind == normalized, MeiroRawBatch.parser_version.isnot(None))
            .distinct()
            .all()
        )
        if value
    ]
    last_received_at = None
    latest_batch_db_id = None
    if latest:
        latest_batch_db_id = int(latest[0]) if latest[0] is not None else None
        if latest[1]:
            last_received_at = latest[1].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "available": True,
        "entries": entries,
        label: int(records_received),
        "last_received_at": last_received_at,
        "latest_batch_db_id": latest_batch_db_id,
        "parser_versions": sorted(parser_versions),
    }


def rebuild_profiles_from_meiro_profile_batches(
    db: Session,
    *,
    limit: Optional[int] = None,
    after_db_id: Optional[int] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows_desc = list_meiro_raw_batches(
        db,
        source_kind="profiles",
        limit=limit,
        after_db_id=after_db_id,
        since=since,
        until=until,
    )
    rebuilt: List[Dict[str, Any]] = []
    for row in reversed(rows_desc):
        profiles = row.get("profiles")
        if not isinstance(profiles, list):
            continue
        if bool(row.get("replace", True)):
            rebuilt = list(profiles)
        else:
            rebuilt.extend(list(profiles))
    return rebuilt
