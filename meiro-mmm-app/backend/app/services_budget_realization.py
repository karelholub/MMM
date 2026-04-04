from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import (
    AdsChangeRequest,
    BudgetRealizationSnapshot,
    BudgetRecommendation,
    BudgetScenario,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed != parsed:
        return default
    return parsed


def _source_meta(row: AdsChangeRequest) -> Dict[str, Any]:
    payload = row.action_payload_json or {}
    return dict(payload.get("source") or {})


def _matches_run(row: AdsChangeRequest, run_id: str) -> bool:
    source = _source_meta(row)
    return str(source.get("run_id") or "") == str(run_id)


def _matches_scenario(row: AdsChangeRequest, scenario_id: str) -> bool:
    source = _source_meta(row)
    return str(source.get("scenario_id") or "") == str(scenario_id)


def build_realization_summary(
    db: Session,
    *,
    run_id: str,
    scenario: BudgetScenario,
) -> Dict[str, Any]:
    recommendations = (
        db.query(BudgetRecommendation)
        .filter(BudgetRecommendation.scenario_id == scenario.id)
        .order_by(BudgetRecommendation.rank.asc())
        .all()
    )
    top = recommendations[0] if recommendations else None
    expected_impact = dict(top.expected_impact_json or {}) if top else {}

    all_requests = (
        db.query(AdsChangeRequest)
        .order_by(AdsChangeRequest.created_at.desc())
        .all()
    )
    linked = [row for row in all_requests if _matches_run(row, run_id) and _matches_scenario(row, scenario.id)]

    counts = {
        "total": len(linked),
        "pending_approval": 0,
        "approved": 0,
        "applied": 0,
        "failed": 0,
        "rejected": 0,
        "cancelled": 0,
    }
    proposed_delta = 0.0
    applied_delta = 0.0
    latest_change_at: Optional[datetime] = None
    items: List[Dict[str, Any]] = []

    for row in linked:
        counts["total"] += 0
        if row.status in counts:
            counts[row.status] += 1
        payload = row.action_payload_json or {}
        previous_budget = _safe_float(payload.get("previous_daily_budget"))
        proposed_budget = _safe_float(payload.get("daily_budget"))
        delta = proposed_budget - previous_budget
        proposed_delta += abs(delta)
        if row.status == "applied":
            applied_delta += abs(delta)
        updated_at = row.updated_at or row.created_at
        if updated_at and (latest_change_at is None or updated_at > latest_change_at):
            latest_change_at = updated_at
        items.append(
            {
                "id": row.id,
                "provider": row.provider,
                "account_id": row.account_id,
                "entity_id": row.entity_id,
                "status": row.status,
                "action_type": row.action_type,
                "previous_daily_budget": previous_budget,
                "proposed_daily_budget": proposed_budget,
                "delta_budget": round(delta, 2),
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                "source": _source_meta(row),
                "error_message": row.error_message,
            }
        )

    execution_progress = (counts["applied"] / counts["total"]) if counts["total"] else 0.0
    projected_realized_lift_pct = _safe_float(expected_impact.get("delta_pct")) * execution_progress

    return {
        "scenario_id": scenario.id,
        "run_id": run_id,
        "objective": scenario.objective,
        "created_at": scenario.created_at.isoformat() if scenario.created_at else None,
        "expected_impact": expected_impact,
        "execution": {
            "counts": counts,
            "proposed_budget_delta_total": round(proposed_delta, 2),
            "applied_budget_delta_total": round(applied_delta, 2),
            "execution_progress_pct": round(execution_progress * 100, 1),
            "projected_realized_lift_pct": round(projected_realized_lift_pct, 2),
            "latest_change_at": latest_change_at.isoformat() if latest_change_at else None,
        },
        "change_requests": items,
    }


def record_budget_realization_snapshot(
    db: Session,
    *,
    run_id: str,
    scenario_id: str,
) -> Dict[str, Any]:
    scenario = (
        db.query(BudgetScenario)
        .filter(BudgetScenario.id == scenario_id, BudgetScenario.run_id == run_id)
        .first()
    )
    if not scenario:
        raise ValueError("Budget scenario not found")
    summary = build_realization_summary(db, run_id=run_id, scenario=scenario)
    row = BudgetRealizationSnapshot(
        scenario_id=scenario.id,
        run_id=run_id,
        snapshot_json=summary,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    return summary


def list_budget_realization(db: Session, *, run_id: str) -> Dict[str, Any]:
    scenarios = (
        db.query(BudgetScenario)
        .filter(BudgetScenario.run_id == run_id)
        .order_by(BudgetScenario.created_at.desc())
        .limit(20)
        .all()
    )
    items = [build_realization_summary(db, run_id=run_id, scenario=scenario) for scenario in scenarios]
    return {"items": items, "total": len(items)}
