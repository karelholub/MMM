from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from .models_config_dq import JourneyDefinitionAudit


def write_journey_definition_audit(
    db: Session,
    *,
    journey_definition_id: str,
    actor: str,
    action: str,
    diff_json: Optional[dict[str, Any]] = None,
) -> JourneyDefinitionAudit:
    item = JourneyDefinitionAudit(
        journey_definition_id=journey_definition_id,
        actor=(actor or "system").strip() or "system",
        action=action.strip(),
        diff_json=diff_json or None,
        created_at=datetime.utcnow(),
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def list_journey_definition_audit(
    db: Session,
    *,
    journey_definition_id: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = (
        db.query(JourneyDefinitionAudit)
        .filter(JourneyDefinitionAudit.journey_definition_id == journey_definition_id)
        .order_by(JourneyDefinitionAudit.created_at.desc(), JourneyDefinitionAudit.id.desc())
        .limit(max(1, min(limit, 200)))
        .all()
    )
    return [
        {
            "id": row.id,
            "journey_definition_id": row.journey_definition_id,
            "actor": row.actor,
            "action": row.action,
            "diff_json": row.diff_json,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]
