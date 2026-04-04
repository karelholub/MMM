from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import JourneyHypothesis
from app.services_journey_hypotheses import (
    refresh_journey_hypothesis_learning,
    refresh_journey_hypothesis_learning_batch,
    serialize_journey_hypothesis,
)


def _normalize_steps(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(">") if part.strip()]
    return []


def _segment_summary(segment: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("channel_group", "campaign_id", "device", "country"):
        value = segment.get(key)
        if value:
            parts.append(f"{key}={value}")
    return " · ".join(parts) if parts else "All eligible users"


def _policy_step(proposed_action: Dict[str, Any]) -> str:
    return str(proposed_action.get("step") or proposed_action.get("channel") or proposed_action.get("type") or "Unknown action").strip()


def _promotion_state(result: Dict[str, Any]) -> Dict[str, Any]:
    value = result.get("policy_promotion")
    if isinstance(value, dict):
        return dict(value)
    return {}


def _status_score(stage: str) -> float:
    return {
        "validated": 100.0,
        "in_experiment": 55.0,
        "experiment_draft": 45.0,
        "ready_to_test": 35.0,
        "inconclusive": 20.0,
        "draft": 10.0,
        "rejected": -40.0,
    }.get(stage, 0.0)


def _build_policy_candidate(row: JourneyHypothesis) -> Dict[str, Any]:
    trigger = dict(row.trigger_json or {})
    segment = dict(row.segment_json or {})
    proposed_action = dict(row.proposed_action_json or {})
    result = dict(row.result_json or {})
    promotion = _promotion_state(result)
    trigger_steps = _normalize_steps(trigger.get("steps"))
    prefix_steps = trigger_steps[:-1] if len(trigger_steps) > 1 else trigger_steps
    prefix_label = " > ".join(prefix_steps) if prefix_steps else "Unspecified prefix"
    stage = str(result.get("learning_stage") or row.status or "draft")
    uplift_abs = float(result.get("uplift_abs") or 0.0)
    p_value = result.get("p_value")
    treatment = result.get("treatment") if isinstance(result.get("treatment"), dict) else {}
    control = result.get("control") if isinstance(result.get("control"), dict) else {}
    sample_support = int((treatment.get("n") or 0) + (control.get("n") or 0))
    support_count = max(int(row.support_count or 0), sample_support)
    score = _status_score(stage)
    score += max(-25.0, min(40.0, uplift_abs * 1000.0))
    score += min(20.0, support_count / 25.0)
    if isinstance(p_value, (int, float)):
        score += max(0.0, 12.0 - (float(p_value) * 60.0))
    if promotion.get("active"):
        score += 15.0

    recommendation = "observe"
    recommendation_reason = "Keep monitoring this policy before making it a default."
    if promotion.get("active"):
        recommendation = "promoted"
        recommendation_reason = "This policy has already been promoted from a validated journey experiment."
    elif stage == "validated" and uplift_abs > 0:
        recommendation = "promote"
        recommendation_reason = "Validated uplift is positive and ready for operational rollout."
    elif stage == "rejected":
        recommendation = "avoid"
        recommendation_reason = "Experiment evidence is negative. Avoid promoting this policy."
    elif stage in {"in_experiment", "experiment_draft"}:
        recommendation = "wait_for_readout"
        recommendation_reason = "An experiment is already linked. Wait for the completed readout."
    elif stage == "inconclusive":
        recommendation = "rerun_or_refine"
        recommendation_reason = "Evidence is inconclusive. Refine the policy or run a larger test."
    elif stage == "ready_to_test":
        recommendation = "test_next"
        recommendation_reason = "This policy has enough structure to launch an experiment next."

    return {
        "hypothesis_id": row.id,
        "title": row.title,
        "journey_definition_id": row.journey_definition_id,
        "status": row.status,
        "learning_stage": stage,
        "linked_experiment_id": row.linked_experiment_id,
        "prefix": {
            "steps": prefix_steps,
            "label": prefix_label,
        },
        "segment": segment,
        "segment_label": _segment_summary(segment),
        "policy": {
            "step": _policy_step(proposed_action),
            "action": proposed_action,
        },
        "support_count": support_count,
        "sample_size_target": row.sample_size_target,
        "uplift_abs": result.get("uplift_abs"),
        "uplift_rel": result.get("uplift_rel"),
        "p_value": result.get("p_value"),
        "summary": result.get("summary") or result.get("note"),
        "recommendation": recommendation,
        "recommendation_reason": recommendation_reason,
        "score": round(score, 2),
        "promotion": promotion,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def list_journey_policy_candidates(
    db: Session,
    *,
    workspace_id: str,
    journey_definition_id: str,
    limit: int = 12,
) -> Dict[str, Any]:
    rows = (
        db.query(JourneyHypothesis)
        .filter(
            JourneyHypothesis.workspace_id == workspace_id,
            JourneyHypothesis.journey_definition_id == journey_definition_id,
        )
        .order_by(JourneyHypothesis.updated_at.desc(), JourneyHypothesis.created_at.desc())
        .all()
    )
    refresh_journey_hypothesis_learning_batch(db, rows)

    items = [_build_policy_candidate(row) for row in rows if dict(row.proposed_action_json or {})]
    items.sort(
        key=lambda item: (
            not bool((item.get("promotion") or {}).get("active")),
            -float(item.get("score") or 0.0),
            item.get("title") or "",
        )
    )
    limited = items[: max(1, int(limit or 12))]
    return {
        "items": limited,
        "summary": {
            "total": len(items),
            "promoted": sum(1 for item in items if (item.get("promotion") or {}).get("active")),
            "ready_to_promote": sum(1 for item in items if item.get("recommendation") == "promote"),
            "validated": sum(1 for item in items if item.get("learning_stage") == "validated"),
            "in_flight": sum(1 for item in items if item.get("learning_stage") in {"in_experiment", "experiment_draft"}),
            "rejected": sum(1 for item in items if item.get("learning_stage") == "rejected"),
        },
    }


def set_journey_policy_promotion(
    db: Session,
    *,
    workspace_id: str,
    hypothesis_id: str,
    actor_user_id: str,
    active: bool,
    notes: Optional[str] = None,
) -> JourneyHypothesis:
    row = (
        db.query(JourneyHypothesis)
        .filter(JourneyHypothesis.id == hypothesis_id, JourneyHypothesis.workspace_id == workspace_id)
        .first()
    )
    if not row:
        raise ValueError("Journey hypothesis not found")

    refresh_journey_hypothesis_learning(db, row, commit=True)
    if active and row.status != "validated":
        raise ValueError("Only validated journey hypotheses can be promoted")

    result = dict(row.result_json or {})
    promotion = _promotion_state(result)
    timestamp = datetime.utcnow().isoformat()
    if active:
        promotion.update(
            {
                "active": True,
                "promoted_at": timestamp,
                "promoted_by": actor_user_id,
                "notes": notes or promotion.get("notes"),
                "step": _policy_step(dict(row.proposed_action_json or {})),
                "source": "validated_experiment",
            }
        )
    else:
        promotion.update(
            {
                "active": False,
                "demoted_at": timestamp,
                "demoted_by": actor_user_id,
                "notes": notes or promotion.get("notes"),
            }
        )

    result["policy_promotion"] = promotion
    row.result_json = result
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def serialize_journey_policy_candidate_response(row: JourneyHypothesis) -> Dict[str, Any]:
    return {
        "hypothesis": serialize_journey_hypothesis(row),
        "policy_candidate": _build_policy_candidate(row),
    }
