from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.connectors import meiro_cdp
from app.models_config_dq import (
    JourneyDefinitionInstanceFact,
    LocalAnalyticalSegment,
    MeiroEventProfileState,
    MeiroProfileFact,
)


ALLOWED_LOCAL_SEGMENT_KEYS = {"channel_group", "campaign_id", "device", "country"}


def _new_id() -> str:
    return str(uuid.uuid4())


def _clean_definition(definition: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    raw = definition or {}
    for key in ALLOWED_LOCAL_SEGMENT_KEYS:
        value = raw.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            cleaned[key] = normalized
    return cleaned


def serialize_local_segment(row: LocalAnalyticalSegment) -> Dict[str, Any]:
    definition = dict(row.definition_json or {})
    return {
        "id": row.id,
        "workspace_id": row.workspace_id,
        "owner_user_id": row.owner_user_id,
        "name": row.name,
        "description": row.description,
        "status": row.status,
        "source": "local_analytical",
        "source_label": "Local analytical",
        "kind": "analytical",
        "supports_analysis": True,
        "supports_activation": False,
        "supports_hypotheses": True,
        "supports_experiments": True,
        "definition": definition,
        "criteria_label": " · ".join(f"{key}={value}" for key, value in definition.items()) or "No criteria",
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "archived_at": row.archived_at.isoformat() if row.archived_at else None,
    }


def list_local_segments(
    db: Session,
    *,
    workspace_id: str,
    include_archived: bool = False,
) -> List[Dict[str, Any]]:
    query = db.query(LocalAnalyticalSegment).filter(LocalAnalyticalSegment.workspace_id == workspace_id)
    if not include_archived:
        query = query.filter(LocalAnalyticalSegment.status != "archived")
    rows = query.order_by(LocalAnalyticalSegment.created_at.desc()).all()
    return [serialize_local_segment(row) for row in rows]


def create_local_segment(
    db: Session,
    *,
    workspace_id: str,
    owner_user_id: str,
    name: str,
    description: Optional[str],
    definition: Dict[str, Any],
) -> Dict[str, Any]:
    row = LocalAnalyticalSegment(
        id=_new_id(),
        workspace_id=workspace_id,
        owner_user_id=owner_user_id,
        name=name.strip(),
        description=(description or "").strip() or None,
        definition_json=_clean_definition(definition),
        status="active",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_local_segment(row)


def update_local_segment(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
    name: str,
    description: Optional[str],
    definition: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    row = (
        db.query(LocalAnalyticalSegment)
        .filter(
            LocalAnalyticalSegment.id == segment_id,
            LocalAnalyticalSegment.workspace_id == workspace_id,
        )
        .first()
    )
    if not row:
        return None
    row.name = name.strip()
    row.description = (description or "").strip() or None
    row.definition_json = _clean_definition(definition)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_local_segment(row)


def set_local_segment_status(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
    status: str,
) -> Optional[Dict[str, Any]]:
    row = (
        db.query(LocalAnalyticalSegment)
        .filter(
            LocalAnalyticalSegment.id == segment_id,
            LocalAnalyticalSegment.workspace_id == workspace_id,
        )
        .first()
    )
    if not row:
        return None
    row.status = status
    row.archived_at = datetime.utcnow() if status == "archived" else None
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_local_segment(row)


def _normalize_meiro_segment(raw: Dict[str, Any], *, workspace_id: str) -> Dict[str, Any]:
    segment_id = (
        raw.get("id")
        or raw.get("segment_id")
        or raw.get("uuid")
        or raw.get("key")
        or raw.get("slug")
        or ""
    )
    name = raw.get("name") or raw.get("title") or raw.get("label") or str(segment_id or "Unnamed segment")
    count = raw.get("profiles_count") or raw.get("count") or raw.get("size") or raw.get("estimated_count")
    return {
        "id": f"meiro:{segment_id}" if segment_id else f"meiro:{name}",
        "external_segment_id": str(segment_id or name),
        "workspace_id": workspace_id,
        "name": str(name),
        "description": raw.get("description"),
        "status": "active",
        "source": "meiro_pipes",
        "source_label": "Meiro Pipes",
        "kind": "operational",
        "supports_analysis": False,
        "supports_activation": True,
        "supports_hypotheses": True,
        "supports_experiments": True,
        "definition": {"external_segment_id": str(segment_id or name)},
        "criteria_label": "Operational audience from Meiro Pipes",
        "size": int(count) if isinstance(count, (int, float)) else None,
        "raw": raw,
    }


def _normalize_segment_membership(raw: Any) -> Optional[Dict[str, str]]:
    if raw in (None, "", []):
        return None
    if isinstance(raw, dict):
        segment_id = (
            raw.get("id")
            or raw.get("segment_id")
            or raw.get("key")
            or raw.get("slug")
            or raw.get("uuid")
        )
        name = raw.get("name") or raw.get("segment_name") or raw.get("title") or raw.get("label")
        if segment_id in (None, "", []) and name in (None, "", []):
            return None
        resolved_id = str(segment_id or name).strip()
        resolved_name = str(name or segment_id).strip()
        if not resolved_id or not resolved_name:
            return None
        return {"id": resolved_id, "name": resolved_name}
    if isinstance(raw, (str, int, float)):
        value = str(raw).strip()
        if not value:
            return None
        return {"id": value, "name": value}
    return None


def _extract_segment_memberships(record: Any) -> List[Dict[str, str]]:
    memberships: List[Dict[str, str]] = []

    def _capture(raw: Any) -> None:
        if isinstance(raw, list):
            for item in raw:
                _capture(item)
            return
        normalized = _normalize_segment_membership(raw)
        if normalized is None:
            return
        if any(item["id"] == normalized["id"] for item in memberships):
            return
        memberships.append(normalized)

    def _inspect(container: Any) -> None:
        if not isinstance(container, dict):
            return
        if container.get("segments") not in (None, "", []):
            _capture(container.get("segments"))
        if container.get("segment") not in (None, "", []):
            _capture(container.get("segment"))
        segment_id = container.get("segment_id")
        segment_name = container.get("segment_name") or container.get("segment_label")
        if segment_id not in (None, "", []) or segment_name not in (None, "", []):
            _capture({"id": segment_id or segment_name, "name": segment_name or segment_id})

    if not isinstance(record, dict):
        return memberships
    _inspect(record)
    for key in ("attributes", "profile_attributes", "traits", "properties"):
        _inspect(record.get(key))
    for key in ("customer", "profile", "user", "person"):
        nested = record.get(key)
        _inspect(nested)
        if isinstance(nested, dict):
            for nested_key in ("attributes", "profile_attributes", "traits", "properties"):
                _inspect(nested.get(nested_key))
    return memberships


def _list_webhook_derived_meiro_segments(
    db: Session,
    *,
    workspace_id: str,
) -> List[Dict[str, Any]]:
    memberships: Dict[str, Dict[str, Any]] = {}

    def _record(profile_id: Any, profile_json: Any, source_label: str) -> None:
        normalized_profile_id = str(profile_id or "").strip()
        if not normalized_profile_id or not isinstance(profile_json, dict):
            return
        for membership in _extract_segment_memberships(profile_json):
            external_segment_id = membership["id"]
            bucket = memberships.setdefault(
                external_segment_id,
                {
                    "id": external_segment_id,
                    "name": membership["name"],
                    "profile_ids": set(),
                    "sources": set(),
                },
            )
            bucket["profile_ids"].add(normalized_profile_id)
            bucket["sources"].add(source_label)
            if not bucket.get("name") and membership.get("name"):
                bucket["name"] = membership["name"]

    try:
        for profile_id, profile_json in db.query(MeiroProfileFact.profile_id, MeiroProfileFact.profile_json).all():
            _record(profile_id, profile_json, "profiles_webhook")
    except (SQLAlchemyError, UnicodeDecodeError, ValueError):
        pass
    try:
        for profile_id, profile_json in db.query(MeiroEventProfileState.profile_id, MeiroEventProfileState.profile_json).all():
            _record(profile_id, profile_json, "raw_events_replay")
    except (SQLAlchemyError, UnicodeDecodeError, ValueError):
        pass

    items: List[Dict[str, Any]] = []
    for payload in memberships.values():
        sources = sorted(str(item) for item in payload["sources"])
        segment = _normalize_meiro_segment(
            {
                "id": payload["id"],
                "name": payload["name"],
                "profiles_count": len(payload["profile_ids"]),
                "description": "Derived from Meiro Pipes webhook payloads",
            },
            workspace_id=workspace_id,
        )
        segment["definition"] = {
            "external_segment_id": payload["id"],
            "derived_from": "webhook_payload",
            "ingestion_sources": sources,
        }
        segment["criteria_label"] = "Operational audience from Meiro Pipes webhook payloads"
        segment["description"] = (
            f"Derived from Meiro Pipes webhook payloads ({', '.join(sources)})"
            if sources
            else "Derived from Meiro Pipes webhook payloads"
        )
        items.append(segment)
    return items


def _merge_meiro_registry_items(*collections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for collection in collections:
        for item in collection:
            key = str(item.get("external_segment_id") or item.get("id") or "").strip()
            if not key:
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = dict(item)
                continue
            if not existing.get("description") and item.get("description"):
                existing["description"] = item.get("description")
            current_size = existing.get("size")
            incoming_size = item.get("size")
            if incoming_size is not None:
                if current_size is None:
                    existing["size"] = incoming_size
                else:
                    try:
                        existing["size"] = max(int(current_size), int(incoming_size))
                    except Exception:
                        existing["size"] = current_size
            existing_definition = dict(existing.get("definition") or {})
            incoming_definition = dict(item.get("definition") or {})
            ingestion_sources = {
                str(value)
                for value in [*(existing_definition.get("ingestion_sources") or []), *(incoming_definition.get("ingestion_sources") or [])]
                if str(value or "").strip()
            }
            if ingestion_sources:
                existing_definition["ingestion_sources"] = sorted(ingestion_sources)
            if incoming_definition.get("derived_from") and not existing_definition.get("derived_from"):
                existing_definition["derived_from"] = incoming_definition.get("derived_from")
            if incoming_definition.get("external_segment_id") and not existing_definition.get("external_segment_id"):
                existing_definition["external_segment_id"] = incoming_definition.get("external_segment_id")
            existing["definition"] = existing_definition
            existing["criteria_label"] = "Operational audience from Meiro Pipes"
            merged[key] = existing
    return sorted(
        merged.values(),
        key=lambda item: (
            -int(item.get("size") or 0),
            str(item.get("name") or "").lower(),
        ),
    )


def list_segment_registry(
    db: Session,
    *,
    workspace_id: str,
    include_archived: bool = False,
) -> Dict[str, Any]:
    local_segments = list_local_segments(db, workspace_id=workspace_id, include_archived=include_archived)
    meiro_cdp_segments: List[Dict[str, Any]] = []
    if meiro_cdp.is_connected():
        try:
            raw_segments = meiro_cdp.list_segments()
            if isinstance(raw_segments, list):
                meiro_cdp_segments = [
                    _normalize_meiro_segment(item, workspace_id=workspace_id) for item in raw_segments if isinstance(item, dict)
                ]
        except Exception:
            meiro_cdp_segments = []
    webhook_meiro_segments = _list_webhook_derived_meiro_segments(db, workspace_id=workspace_id)
    meiro_segments = _merge_meiro_registry_items(meiro_cdp_segments, webhook_meiro_segments)
    return {
        "items": [*local_segments, *meiro_segments],
        "summary": {
            "local_analytical": len(local_segments),
            "meiro_pipes": len(meiro_segments),
            "analysis_ready": sum(1 for item in local_segments if item.get("supports_analysis")),
            "activation_ready": sum(1 for item in meiro_segments if item.get("supports_activation")),
        },
    }


def _top_segment_dimension_values(db: Session, column: Any, *, limit: int = 100) -> List[Dict[str, Any]]:
    rows = (
        db.query(column, func.count(JourneyDefinitionInstanceFact.id).label("count"))
        .filter(column.isnot(None))
        .filter(column != "")
        .group_by(column)
        .order_by(func.count(JourneyDefinitionInstanceFact.id).desc(), column.asc())
        .limit(max(1, min(limit, 500)))
        .all()
    )
    return [{"value": str(value), "count": int(count or 0)} for value, count in rows if str(value or "").strip()]


def build_segment_context(db: Session) -> Dict[str, Any]:
    total_rows = db.query(func.count(JourneyDefinitionInstanceFact.id)).scalar() or 0
    date_from, date_to = (
        db.query(
            func.min(JourneyDefinitionInstanceFact.date),
            func.max(JourneyDefinitionInstanceFact.date),
        ).first()
        or (None, None)
    )
    return {
        "summary": {
            "journey_rows": int(total_rows),
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
        },
        "channels": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.channel_group),
        "campaigns": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.campaign_id),
        "devices": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.device),
        "countries": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.country),
    }
