"""Query helpers for journey definition metadata."""

from __future__ import annotations

from datetime import datetime
import uuid
from typing import Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .models_config_dq import JourneyDefinition, JourneyDefinitionMode


def _serialize(item: JourneyDefinition) -> dict:
    return {
        "id": item.id,
        "name": item.name,
        "description": item.description,
        "conversion_kpi_id": item.conversion_kpi_id,
        "lookback_window_days": item.lookback_window_days,
        "mode_default": item.mode_default,
        "is_archived": bool(getattr(item, "is_archived", False)),
        "created_by": item.created_by,
        "updated_by": getattr(item, "updated_by", None),
        "archived_by": getattr(item, "archived_by", None),
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
        "archived_at": item.archived_at.isoformat() if getattr(item, "archived_at", None) else None,
    }


def list_journey_definitions(
    db: Session,
    *,
    page: int = 1,
    per_page: int = 20,
    search: Optional[str] = None,
    sort_dir: str = "desc",
    include_archived: bool = False,
) -> dict:
    q = db.query(JourneyDefinition)
    if not include_archived:
        q = q.filter(JourneyDefinition.is_archived == False)  # noqa: E712
    if search:
        term = f"%{search.strip()}%"
        q = q.filter(or_(JourneyDefinition.name.ilike(term), JourneyDefinition.description.ilike(term)))
    total = q.count()
    order_col = JourneyDefinition.updated_at.asc() if sort_dir == "asc" else JourneyDefinition.updated_at.desc()
    q = q.order_by(order_col)
    offset = max(0, page - 1) * max(1, min(per_page, 100))
    rows = q.offset(offset).limit(max(1, min(per_page, 100))).all()
    return {"items": [_serialize(row) for row in rows], "total": total, "page": page, "per_page": per_page}


def get_journey_definition(db: Session, definition_id: str) -> Optional[JourneyDefinition]:
    return db.get(JourneyDefinition, definition_id)


def create_journey_definition(
    db: Session,
    *,
    name: str,
    description: Optional[str] = None,
    conversion_kpi_id: Optional[str] = None,
    lookback_window_days: int = 30,
    mode_default: str = JourneyDefinitionMode.CONVERSION_ONLY,
    created_by: str = "system",
) -> JourneyDefinition:
    now = datetime.utcnow()
    item = JourneyDefinition(
        id=str(uuid.uuid4()),
        name=name.strip(),
        description=description,
        conversion_kpi_id=conversion_kpi_id,
        lookback_window_days=max(1, int(lookback_window_days)),
        mode_default=mode_default,
        created_by=created_by,
        updated_by=created_by,
        is_archived=False,
        archived_at=None,
        archived_by=None,
        created_at=now,
        updated_at=now,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def ensure_default_journey_definition(db: Session, *, created_by: str = "dev-seed") -> JourneyDefinition:
    existing = db.query(JourneyDefinition).order_by(JourneyDefinition.created_at.asc()).first()
    if existing:
        return existing
    return create_journey_definition(
        db,
        name="Default Journey",
        description="Default journey definition for local development.",
        conversion_kpi_id=None,
        lookback_window_days=30,
        mode_default=JourneyDefinitionMode.CONVERSION_ONLY,
        created_by=created_by,
    )


def update_journey_definition(
    db: Session,
    definition_id: str,
    *,
    name: str,
    description: Optional[str],
    conversion_kpi_id: Optional[str],
    lookback_window_days: int,
    mode_default: str,
    updated_by: str,
) -> Optional[JourneyDefinition]:
    item = db.get(JourneyDefinition, definition_id)
    if not item or item.is_archived:
        return None
    item.name = name.strip()
    item.description = description
    item.conversion_kpi_id = conversion_kpi_id
    item.lookback_window_days = max(1, int(lookback_window_days))
    item.mode_default = mode_default
    item.updated_by = updated_by
    item.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(item)
    return item


def archive_journey_definition(db: Session, definition_id: str, *, archived_by: str) -> Optional[JourneyDefinition]:
    item = db.get(JourneyDefinition, definition_id)
    if not item:
        return None
    if item.is_archived:
        return item
    now = datetime.utcnow()
    item.is_archived = True
    item.archived_at = now
    item.archived_by = archived_by
    item.updated_by = archived_by
    item.updated_at = now
    db.commit()
    db.refresh(item)
    return item


def serialize_journey_definition(item: JourneyDefinition) -> dict:
    return _serialize(item)
