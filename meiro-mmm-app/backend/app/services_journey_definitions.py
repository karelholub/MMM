"""Query helpers for journey definition metadata."""

from __future__ import annotations

from datetime import datetime
import uuid
from typing import Iterable, Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from .models_config_dq import (
    Experiment,
    FunnelDefinition,
    JourneyAlertDefinition,
    JourneyDefinition,
    JourneyExampleFact,
    JourneyDefinitionInstanceFact,
    JourneyDefinitionMode,
    JourneyHypothesis,
    JourneyPathDaily,
    JourneySavedView,
    JourneyTransitionDaily,
)


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
        "lifecycle_status": "archived" if bool(getattr(item, "is_archived", False)) else "active",
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


def list_active_journey_definitions(
    db: Session,
    *,
    conversion_kpi_ids: Optional[Iterable[str]] = None,
) -> list[JourneyDefinition]:
    q = db.query(JourneyDefinition).filter(JourneyDefinition.is_archived == False)  # noqa: E712
    if conversion_kpi_ids is not None:
        normalized_ids = sorted({str(value).strip() for value in conversion_kpi_ids if str(value).strip()})
        if not normalized_ids:
            return []
        q = q.filter(JourneyDefinition.conversion_kpi_id.in_(normalized_ids))
    return q.order_by(JourneyDefinition.updated_at.desc(), JourneyDefinition.created_at.desc()).all()


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


def restore_journey_definition(db: Session, definition_id: str, *, restored_by: str) -> Optional[JourneyDefinition]:
    item = db.get(JourneyDefinition, definition_id)
    if not item:
        return None
    if not item.is_archived:
        return item
    now = datetime.utcnow()
    item.is_archived = False
    item.archived_at = None
    item.archived_by = None
    item.updated_by = restored_by
    item.updated_at = now
    db.commit()
    db.refresh(item)
    return item


def duplicate_journey_definition(
    db: Session,
    definition_id: str,
    *,
    created_by: str,
    name: Optional[str] = None,
) -> Optional[JourneyDefinition]:
    item = db.get(JourneyDefinition, definition_id)
    if not item:
        return None
    next_name = (name or "").strip() or f"{item.name} copy"
    return create_journey_definition(
        db,
        name=next_name,
        description=item.description,
        conversion_kpi_id=item.conversion_kpi_id,
        lookback_window_days=item.lookback_window_days,
        mode_default=item.mode_default,
        created_by=created_by,
    )


def get_journey_definition_lifecycle(db: Session, definition_id: str) -> Optional[dict]:
    item = db.get(JourneyDefinition, definition_id)
    if not item:
        return None
    hypothesis_ids = [
        row_id
        for (row_id,) in db.query(JourneyHypothesis.id)
        .filter(JourneyHypothesis.journey_definition_id == definition_id)
        .all()
    ]
    alerts = 0
    for alert in db.query(JourneyAlertDefinition).filter(JourneyAlertDefinition.domain == "journeys").all():
        scope = alert.scope_json or {}
        if str(scope.get("journey_definition_id") or "").strip() == definition_id:
            alerts += 1
    dependency_counts = {
        "saved_views": int(
            db.query(func.count(JourneySavedView.id))
            .filter(JourneySavedView.journey_definition_id == definition_id)
            .scalar()
            or 0
        ),
        "funnels": int(
            db.query(func.count(FunnelDefinition.id))
            .filter(FunnelDefinition.journey_definition_id == definition_id, FunnelDefinition.is_archived == False)  # noqa: E712
            .scalar()
            or 0
        ),
        "hypotheses": int(
            db.query(func.count(JourneyHypothesis.id))
            .filter(JourneyHypothesis.journey_definition_id == definition_id)
            .scalar()
            or 0
        ),
        "experiments": int(
            db.query(func.count(Experiment.id))
            .filter(
                Experiment.source_type == "journey_hypothesis",
                Experiment.source_id.in_(hypothesis_ids or ["__none__"]),
            )
            .scalar()
            or 0
        ),
        "alerts": alerts,
    }
    output_counts = {
        "journey_instances": int(
            db.query(func.count(JourneyDefinitionInstanceFact.id))
            .filter(JourneyDefinitionInstanceFact.journey_definition_id == definition_id)
            .scalar()
            or 0
        ),
        "path_days": int(
            db.query(func.count(JourneyPathDaily.id))
            .filter(JourneyPathDaily.journey_definition_id == definition_id)
            .scalar()
            or 0
        ),
        "transition_days": int(
            db.query(func.count(JourneyTransitionDaily.id))
            .filter(JourneyTransitionDaily.journey_definition_id == definition_id)
            .scalar()
            or 0
        ),
        "example_days": int(
            db.query(func.count(JourneyExampleFact.id))
            .filter(JourneyExampleFact.journey_definition_id == definition_id)
            .scalar()
            or 0
        ),
    }
    dependency_total = sum(dependency_counts.values())
    output_total = sum(output_counts.values())
    output_updated_candidates = [
        db.query(func.max(JourneyDefinitionInstanceFact.updated_at))
        .filter(JourneyDefinitionInstanceFact.journey_definition_id == definition_id)
        .scalar(),
        db.query(func.max(JourneyPathDaily.updated_at))
        .filter(JourneyPathDaily.journey_definition_id == definition_id)
        .scalar(),
        db.query(func.max(JourneyTransitionDaily.updated_at))
        .filter(JourneyTransitionDaily.journey_definition_id == definition_id)
        .scalar(),
        db.query(func.max(JourneyExampleFact.updated_at))
        .filter(JourneyExampleFact.journey_definition_id == definition_id)
        .scalar(),
    ]
    latest_output_updated_at = max((value for value in output_updated_candidates if value is not None), default=None)
    stale_reason: Optional[str] = None
    lifecycle_status = "archived" if bool(item.is_archived) else "active"
    if not item.is_archived:
        if latest_output_updated_at is None:
            lifecycle_status = "stale"
            stale_reason = "no_outputs_built"
        elif item.updated_at and latest_output_updated_at and item.updated_at > latest_output_updated_at:
            lifecycle_status = "stale"
            stale_reason = "definition_changed_since_build"
    warnings: list[str] = []
    if dependency_total:
        warnings.append(
            f"This definition is referenced by {dependency_total} downstream items."
        )
    if output_total:
        warnings.append(
            f"This definition has {output_total} generated journey-output rows that should be preserved for restore/history."
        )
    if stale_reason == "no_outputs_built":
        warnings.append("Outputs have not been built for this definition yet.")
    elif stale_reason == "definition_changed_since_build":
        warnings.append("Definition metadata changed after the last output build. Rebuild is recommended.")
    definition_payload = _serialize(item)
    definition_payload["lifecycle_status"] = lifecycle_status
    return {
        "definition": definition_payload,
        "dependency_counts": dependency_counts,
        "output_counts": output_counts,
        "allowed_actions": {
            "can_archive": not bool(item.is_archived),
            "can_restore": bool(item.is_archived),
            "can_duplicate": True,
            "can_delete": not bool(item.is_archived) and dependency_total == 0 and output_total == 0,
            "can_rebuild": not bool(item.is_archived),
        },
        "rebuild_state": {
            "status": lifecycle_status,
            "stale_reason": stale_reason,
            "last_rebuilt_at": latest_output_updated_at.isoformat() if latest_output_updated_at else None,
        },
        "warnings": warnings,
    }


def serialize_journey_definition(item: JourneyDefinition) -> dict:
    return _serialize(item)
