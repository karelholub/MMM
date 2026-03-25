from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models_config_dq import JourneyDefinition, JourneySavedView


def serialize_journey_saved_view(item: JourneySavedView) -> dict:
    return {
        "id": item.id,
        "workspace_id": item.workspace_id,
        "user_id": item.user_id,
        "journey_definition_id": item.journey_definition_id,
        "name": item.name,
        "state": item.state_json or {},
        "created_by": item.created_by,
        "updated_by": item.updated_by,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


def list_journey_saved_views(
    db: Session,
    *,
    workspace_id: str,
    user_id: str,
    journey_definition_id: str | None = None,
) -> dict:
    resolved_workspace_id = (workspace_id or "default").strip() or "default"
    resolved_user_id = (user_id or "default").strip() or "default"
    q = db.query(JourneySavedView).filter(
        JourneySavedView.workspace_id == resolved_workspace_id,
        JourneySavedView.user_id == resolved_user_id,
    )
    if journey_definition_id:
        q = q.filter(JourneySavedView.journey_definition_id == journey_definition_id)
    items = q.order_by(JourneySavedView.updated_at.desc(), JourneySavedView.created_at.desc()).all()
    return {"items": [serialize_journey_saved_view(item) for item in items], "total": len(items)}


def create_journey_saved_view(
    db: Session,
    *,
    workspace_id: str,
    user_id: str,
    journey_definition_id: str | None,
    name: str,
    state: dict,
    actor_user_id: str,
) -> JourneySavedView:
    if journey_definition_id:
        jd = db.query(JourneyDefinition).filter(JourneyDefinition.id == journey_definition_id).first()
        if not jd or jd.is_archived:
            raise ValueError("Journey definition not found")
    now = datetime.utcnow()
    resolved_workspace_id = (workspace_id or "default").strip() or "default"
    resolved_user_id = (user_id or "default").strip() or "default"
    item = JourneySavedView(
        id=str(uuid4()),
        workspace_id=resolved_workspace_id,
        user_id=resolved_user_id,
        journey_definition_id=journey_definition_id,
        name=name.strip(),
        state_json=state or {},
        created_by=actor_user_id or "system",
        updated_by=actor_user_id or "system",
        created_at=now,
        updated_at=now,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def update_journey_saved_view(
    db: Session,
    *,
    view_id: str,
    workspace_id: str,
    user_id: str,
    name: str,
    state: dict,
    journey_definition_id: str | None,
    actor_user_id: str,
) -> JourneySavedView | None:
    resolved_workspace_id = (workspace_id or "default").strip() or "default"
    resolved_user_id = (user_id or "default").strip() or "default"
    item = (
        db.query(JourneySavedView)
        .filter(
            JourneySavedView.id == view_id,
            JourneySavedView.workspace_id == resolved_workspace_id,
            JourneySavedView.user_id == resolved_user_id,
        )
        .first()
    )
    if not item:
        return None
    if journey_definition_id:
        jd = db.query(JourneyDefinition).filter(JourneyDefinition.id == journey_definition_id).first()
        if not jd or jd.is_archived:
            raise ValueError("Journey definition not found")
    item.name = name.strip()
    item.state_json = state or {}
    item.journey_definition_id = journey_definition_id
    item.updated_by = actor_user_id or "system"
    item.updated_at = datetime.utcnow()
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def delete_journey_saved_view(
    db: Session,
    *,
    view_id: str,
    workspace_id: str,
    user_id: str,
) -> bool:
    resolved_workspace_id = (workspace_id or "default").strip() or "default"
    resolved_user_id = (user_id or "default").strip() or "default"
    item = (
        db.query(JourneySavedView)
        .filter(
            JourneySavedView.id == view_id,
            JourneySavedView.workspace_id == resolved_workspace_id,
            JourneySavedView.user_id == resolved_user_id,
        )
        .first()
    )
    if not item:
        return False
    db.delete(item)
    db.commit()
    return True
