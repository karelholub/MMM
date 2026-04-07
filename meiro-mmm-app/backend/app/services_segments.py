from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.connectors import meiro_cdp
from app.models_config_dq import JourneyDefinitionInstanceFact, LocalAnalyticalSegment


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


def list_segment_registry(
    db: Session,
    *,
    workspace_id: str,
    include_archived: bool = False,
) -> Dict[str, Any]:
    local_segments = list_local_segments(db, workspace_id=workspace_id, include_archived=include_archived)
    meiro_segments: List[Dict[str, Any]] = []
    if meiro_cdp.is_connected():
        try:
            raw_segments = meiro_cdp.list_segments()
            if isinstance(raw_segments, list):
                meiro_segments = [
                    _normalize_meiro_segment(item, workspace_id=workspace_id) for item in raw_segments if isinstance(item, dict)
                ]
        except Exception:
            meiro_segments = []
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
