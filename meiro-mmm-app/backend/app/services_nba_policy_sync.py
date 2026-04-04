from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy.orm import Session

from app.models_config_dq import JourneyHypothesis


def _normalize_steps(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(">") if part.strip()]
    return []


def _policy_step(proposed_action: Dict[str, Any]) -> str:
    return str(proposed_action.get("step") or proposed_action.get("channel") or proposed_action.get("type") or "").strip()


def build_promoted_journey_policy_overrides(
    db: Session,
    *,
    workspace_id: str,
) -> List[Dict[str, Any]]:
    rows = (
        db.query(JourneyHypothesis)
        .filter(JourneyHypothesis.workspace_id == workspace_id)
        .order_by(JourneyHypothesis.updated_at.desc(), JourneyHypothesis.created_at.desc())
        .all()
    )

    items: List[Dict[str, Any]] = []
    for row in rows:
        result = dict(row.result_json or {})
        promotion = result.get("policy_promotion")
        if not isinstance(promotion, dict) or not promotion.get("active"):
            continue

        trigger = dict(row.trigger_json or {})
        segment = dict(row.segment_json or {})
        proposed_action = dict(row.proposed_action_json or {})
        trigger_steps = _normalize_steps(trigger.get("steps"))
        prefix_steps = trigger_steps[:-1] if len(trigger_steps) > 1 else trigger_steps
        prefix = " > ".join(prefix_steps)
        step = _policy_step(proposed_action)
        if not prefix or not step:
            continue

        items.append(
            {
                "hypothesis_id": row.id,
                "title": row.title,
                "journey_definition_id": row.journey_definition_id,
                "prefix": prefix,
                "prefix_steps": prefix_steps,
                "step": step,
                "channel": step,
                "campaign": proposed_action.get("campaign"),
                "segment": segment,
                "promoted_at": promotion.get("promoted_at"),
                "promoted_by": promotion.get("promoted_by"),
                "notes": promotion.get("notes"),
                "source": promotion.get("source") or "journey_lab",
            }
        )

    items.sort(
        key=lambda item: (
            item.get("prefix") or "",
            item.get("step") or "",
            item.get("title") or "",
        )
    )
    return items


def apply_promoted_policy_overrides(
    nba_raw: Dict[str, List[Dict[str, Any]]],
    promoted_policies: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    overrides: Dict[str, Dict[str, Any]] = {}
    for policy in promoted_policies or []:
        prefix = str(policy.get("prefix") or "").strip()
        step = str(policy.get("step") or policy.get("channel") or "").strip().lower()
        if not prefix or not step:
            continue
        overrides[f"{prefix}::{step}"] = dict(policy)
    return overrides
