from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import Experiment, JourneyHypothesis
from app.services_incrementality import compute_experiment_results


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


def _format_pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _build_learning_payload(
    *,
    experiment: Experiment,
    existing_result: Dict[str, Any],
    computed_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = dict(existing_result or {})
    payload["experiment_id"] = experiment.id
    payload["experiment_name"] = experiment.name
    payload["experiment_status"] = experiment.status
    payload["experiment_type"] = experiment.experiment_type

    if experiment.status != "completed" or not computed_result:
        learning_stage = "experiment_draft" if experiment.status == "draft" else "in_experiment"
        summary = (
            "Experiment is configured and waiting to start."
            if experiment.status == "draft"
            else "Experiment is running. Wait for a completed readout before promoting the policy."
        )
        payload.update(
            {
                "learning_stage": learning_stage,
                "summary": summary,
                "evaluated_at": datetime.utcnow().isoformat(),
                "verdict": None,
                "insufficient_data": None,
                "uplift_abs": None,
                "uplift_rel": None,
                "ci_low": None,
                "ci_high": None,
                "p_value": None,
                "treatment": {},
                "control": {},
            }
        )
        return payload

    treatment = dict(computed_result.get("treatment") or {})
    control = dict(computed_result.get("control") or {})
    uplift_abs = computed_result.get("uplift_abs")
    uplift_rel = computed_result.get("uplift_rel")
    ci_low = computed_result.get("ci_low")
    ci_high = computed_result.get("ci_high")
    p_value = computed_result.get("p_value")
    insufficient_data = bool(computed_result.get("insufficient_data"))

    if insufficient_data:
        verdict = "inconclusive"
        summary = "Completed experiment is still underpowered. Keep collecting exposure and outcome data."
    elif ci_low is not None and ci_low > 0:
        verdict = "validated"
        summary = (
            f"Treatment beat control by {_format_pct(uplift_abs)} with "
            f"{treatment.get('conversions', 0)}/{treatment.get('n', 0)} vs "
            f"{control.get('conversions', 0)}/{control.get('n', 0)} conversions."
        )
    elif ci_high is not None and ci_high < 0:
        verdict = "rejected"
        summary = (
            f"Treatment underperformed control by {_format_pct(abs(uplift_abs or 0.0))}; "
            "do not promote this policy as default."
        )
    elif p_value is not None and p_value <= 0.1 and uplift_abs is not None and uplift_abs > 0:
        verdict = "validated"
        summary = f"Treatment shows a positive readout of {_format_pct(uplift_abs)}. Statistical certainty is still moderate."
    elif p_value is not None and p_value <= 0.1 and uplift_abs is not None and uplift_abs < 0:
        verdict = "rejected"
        summary = f"Treatment shows a negative readout of {_format_pct(abs(uplift_abs))}. Statistical certainty is still moderate."
    else:
        verdict = "inconclusive"
        summary = "Completed experiment did not clear the evidence threshold yet. Keep this policy in review."

    payload.update(
        {
            "learning_stage": verdict,
            "verdict": verdict,
            "summary": summary,
            "evaluated_at": datetime.utcnow().isoformat(),
            "insufficient_data": insufficient_data,
            "uplift_abs": uplift_abs,
            "uplift_rel": uplift_rel,
            "ci_low": ci_low,
            "ci_high": ci_high,
            "p_value": p_value,
            "treatment": treatment,
            "control": control,
        }
    )
    return payload


def refresh_journey_hypothesis_learning(
    db: Session,
    row: JourneyHypothesis,
    *,
    commit: bool = True,
) -> bool:
    if not row.linked_experiment_id:
        return False

    experiment = db.get(Experiment, row.linked_experiment_id)
    if not experiment:
        return False

    computed_result: Optional[Dict[str, Any]] = None
    desired_status = row.status
    if experiment.status == "completed":
        computed_result = compute_experiment_results(db, experiment.id)
        if (computed_result or {}).get("insufficient_data"):
            desired_status = "inconclusive"
        elif computed_result and computed_result.get("ci_low") is not None and float(computed_result["ci_low"]) > 0:
            desired_status = "validated"
        elif computed_result and computed_result.get("ci_high") is not None and float(computed_result["ci_high"]) < 0:
            desired_status = "rejected"
        elif computed_result and computed_result.get("p_value") is not None and float(computed_result["p_value"]) <= 0.1:
            uplift_abs = computed_result.get("uplift_abs")
            if uplift_abs is not None and float(uplift_abs) > 0:
                desired_status = "validated"
            elif uplift_abs is not None and float(uplift_abs) < 0:
                desired_status = "rejected"
            else:
                desired_status = "inconclusive"
        else:
            desired_status = "inconclusive"
    elif experiment.status in {"draft", "running"}:
        desired_status = "in_experiment"

    next_result = _build_learning_payload(
        experiment=experiment,
        existing_result=dict(row.result_json or {}),
        computed_result=computed_result,
    )
    changed = row.status != desired_status or dict(row.result_json or {}) != next_result
    if not changed:
        return False

    row.status = desired_status
    row.result_json = next_result
    row.updated_at = datetime.utcnow()
    db.add(row)
    if commit:
        db.commit()
        db.refresh(row)
    return True


def refresh_journey_hypothesis_learning_batch(db: Session, rows: list[JourneyHypothesis]) -> bool:
    changed = False
    touched: list[JourneyHypothesis] = []
    for row in rows:
        if refresh_journey_hypothesis_learning(db, row, commit=False):
            changed = True
            touched.append(row)
    if changed:
        db.commit()
        for row in touched:
            db.refresh(row)
    return changed


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
    refresh_journey_hypothesis_learning_batch(db, rows)
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
