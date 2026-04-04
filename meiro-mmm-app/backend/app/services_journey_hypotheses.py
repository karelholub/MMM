from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import JourneyHypothesis


def _new_id() -> str:
    return str(uuid.uuid4())


def serialize_journey_hypothesis(row: JourneyHypothesis) -> Dict[str, Any]:
    return {
        "id": row.id,
        "workspace_id": row.workspace_id,
        "journey_definition_id": row.journey_definition_id,
        "owner_user_id": row.owner_user_id,
        "title": row.title,
        "target_kpi": row.target_kpi,
        "hypothesis_text": row.hypothesis_text,
        "trigger": dict(row.trigger_json or {}),
        "segment": dict(row.segment_json or {}),
        "current_action": dict(row.current_action_json or {}),
        "proposed_action": dict(row.proposed_action_json or {}),
        "support_count": int(row.support_count or 0),
        "baseline_rate": row.baseline_rate,
        "sample_size_target": row.sample_size_target,
        "status": row.status,
        "linked_experiment_id": row.linked_experiment_id,
        "result": dict(row.result_json or {}),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def list_journey_hypotheses(
    db: Session,
    *,
    workspace_id: str,
    journey_definition_id: Optional[str] = None,
    status: Optional[str] = None,
    owner_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    q = db.query(JourneyHypothesis).filter(JourneyHypothesis.workspace_id == workspace_id)
    if journey_definition_id:
        q = q.filter(JourneyHypothesis.journey_definition_id == journey_definition_id)
    if status:
        q = q.filter(JourneyHypothesis.status == status)
    if owner_user_id:
        q = q.filter(JourneyHypothesis.owner_user_id == owner_user_id)
    rows = q.order_by(JourneyHypothesis.updated_at.desc(), JourneyHypothesis.created_at.desc()).all()
    return {"items": [serialize_journey_hypothesis(row) for row in rows], "total": len(rows)}


def create_journey_hypothesis(
    db: Session,
    *,
    workspace_id: str,
    owner_user_id: str,
    journey_definition_id: str,
    title: str,
    target_kpi: Optional[str],
    hypothesis_text: str,
    trigger: Dict[str, Any],
    segment: Dict[str, Any],
    current_action: Dict[str, Any],
    proposed_action: Dict[str, Any],
    support_count: int,
    baseline_rate: Optional[float],
    sample_size_target: Optional[int],
    status: str,
    linked_experiment_id: Optional[int],
    result: Dict[str, Any],
) -> JourneyHypothesis:
    row = JourneyHypothesis(
        id=_new_id(),
        workspace_id=workspace_id,
        journey_definition_id=journey_definition_id,
        owner_user_id=owner_user_id,
        title=title.strip(),
        target_kpi=(target_kpi or "").strip() or None,
        hypothesis_text=hypothesis_text.strip(),
        trigger_json=trigger or {},
        segment_json=segment or {},
        current_action_json=current_action or {},
        proposed_action_json=proposed_action or {},
        support_count=max(0, int(support_count or 0)),
        baseline_rate=baseline_rate,
        sample_size_target=sample_size_target,
        status=(status or "draft").strip() or "draft",
        linked_experiment_id=linked_experiment_id,
        result_json=result or {},
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_journey_hypothesis(
    db: Session,
    *,
    workspace_id: str,
    hypothesis_id: str,
    title: str,
    target_kpi: Optional[str],
    hypothesis_text: str,
    trigger: Dict[str, Any],
    segment: Dict[str, Any],
    current_action: Dict[str, Any],
    proposed_action: Dict[str, Any],
    support_count: int,
    baseline_rate: Optional[float],
    sample_size_target: Optional[int],
    status: str,
    linked_experiment_id: Optional[int],
    result: Dict[str, Any],
) -> Optional[JourneyHypothesis]:
    row = (
        db.query(JourneyHypothesis)
        .filter(JourneyHypothesis.id == hypothesis_id, JourneyHypothesis.workspace_id == workspace_id)
        .first()
    )
    if not row:
        return None
    row.title = title.strip()
    row.target_kpi = (target_kpi or "").strip() or None
    row.hypothesis_text = hypothesis_text.strip()
    row.trigger_json = trigger or {}
    row.segment_json = segment or {}
    row.current_action_json = current_action or {}
    row.proposed_action_json = proposed_action or {}
    row.support_count = max(0, int(support_count or 0))
    row.baseline_rate = baseline_rate
    row.sample_size_target = sample_size_target
    row.status = (status or "draft").strip() or "draft"
    row.linked_experiment_id = linked_experiment_id
    row.result_json = result or {}
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
