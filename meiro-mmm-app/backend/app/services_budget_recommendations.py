from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping

from sqlalchemy.orm import Session

from app.models_config_dq import BudgetRecommendation, BudgetRecommendationAction, BudgetScenario


OBJECTIVES = {"protect_efficiency", "grow_conversions", "hit_target_roas"}


def _new_id() -> str:
    return str(uuid.uuid4())


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed != parsed:  # NaN
        return default
    return parsed


def _band_from_score(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def _build_recommended_actions(status: str, low_confidence: bool, has_extrapolation: bool) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    if status == "blocked":
        actions.append(
            {
                "id": "review-mmm-inputs",
                "label": "Review MMM inputs",
                "benefit": "Recommendations are blocked until the model has ROI and contribution outputs.",
                "domain": "mmm",
                "target_page": "mmm",
            }
        )
        return actions
    if low_confidence:
        actions.append(
            {
                "id": "review-model-trust",
                "label": "Review model trust",
                "benefit": "Recommendation confidence is limited by the current model and data history.",
                "domain": "mmm",
                "target_page": "mmm",
            }
        )
    if has_extrapolation:
        actions.append(
            {
                "id": "keep-within-history",
                "label": "Keep changes within observed range",
                "benefit": "Moves outside historical spend ranges are more likely to miss forecast.",
                "domain": "budget",
                "target_page": "mmm",
            }
        )
    if not actions:
        actions.append(
            {
                "id": "save-budget-scenario",
                "label": "Save this scenario",
                "benefit": "Use saved scenarios to compare options before proposing changes.",
                "domain": "budget",
                "target_page": "mmm",
            }
        )
    return actions


def summarize_run_for_budget(
    run: Mapping[str, Any],
    dataset_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    roi_rows = run.get("roi") or []
    contrib_rows = run.get("contrib") or []
    config = run.get("config") or {}
    channels = [str(row.get("channel")) for row in roi_rows if row.get("channel")]
    roi_map = {str(row.get("channel")): _safe_float(row.get("roi")) for row in roi_rows if row.get("channel")}
    contrib_map = {
        str(row.get("channel")): _safe_float(row.get("mean_share"))
        for row in contrib_rows
        if row.get("channel")
    }
    channel_summary_map = {
        str(row.get("channel")): row for row in (run.get("channel_summary") or []) if row.get("channel")
    }

    baseline_spend: Dict[str, float] = {ch: 0.0 for ch in channels}
    observed_ranges: Dict[str, Dict[str, float]] = {ch: {"min": 0.0, "max": 0.0} for ch in channels}
    row_count = 0
    for row in dataset_rows:
        row_count += 1
        for ch in channels:
            value = _safe_float(row.get(ch))
            baseline_spend[ch] = baseline_spend.get(ch, 0.0) + value
            if value > 0:
                current = observed_ranges.setdefault(ch, {"min": 0.0, "max": 0.0})
                current["min"] = value if current["min"] == 0 else min(current["min"], value)
                current["max"] = max(current["max"], value)

    total_spend = sum(baseline_spend.values())
    weighted_roi = sum(roi_map.get(ch, 0.0) * max(contrib_map.get(ch, 0.0), 0.0) for ch in channels)
    diagnostics = run.get("diagnostics") or {}
    negative_roi_channels = [ch for ch, val in roi_map.items() if val < 0]

    confidence_score = 0.55
    if row_count >= 52:
        confidence_score += 0.12
    elif row_count >= 26:
        confidence_score += 0.06
    if len(channels) >= 3:
        confidence_score += 0.08
    if negative_roi_channels:
        confidence_score -= min(0.15, 0.05 * len(negative_roi_channels))
    if _safe_float(diagnostics.get("rhat_max"), 1.0) > 1.1:
        confidence_score -= 0.08
    if _safe_float(diagnostics.get("ess_bulk_min"), 300.0) < 200:
        confidence_score -= 0.08
    if _safe_float(diagnostics.get("divergences"), 0.0) > 0:
        confidence_score -= 0.08
    confidence_score = min(0.95, max(0.1, confidence_score))

    return {
        "channels": channels,
        "roi_map": roi_map,
        "contrib_map": contrib_map,
        "channel_summary_map": channel_summary_map,
        "baseline_spend": baseline_spend,
        "observed_ranges": observed_ranges,
        "total_spend": total_spend,
        "weighted_roi": weighted_roi,
        "row_count": row_count,
        "negative_roi_channels": negative_roi_channels,
        "confidence_score": confidence_score,
        "config": config,
    }


def _proposed_delta_pct(objective: str, index: int, direction: str) -> float:
    if objective == "protect_efficiency":
        return [0.08, 0.06, 0.04][min(index, 2)] * (1 if direction == "increase" else -1)
    if objective == "grow_conversions":
        return [0.18, 0.12, 0.08][min(index, 2)] * (1 if direction == "increase" else -1)
    return [0.12, 0.08, 0.05][min(index, 2)] * (1 if direction == "increase" else -1)


def build_budget_recommendations(
    *,
    run_id: str,
    run: Mapping[str, Any],
    dataset_rows: Iterable[Mapping[str, Any]],
    objective: str,
    total_budget_change_pct: float = 0.0,
) -> Dict[str, Any]:
    objective = (objective or "protect_efficiency").strip().lower()
    if objective not in OBJECTIVES:
        objective = "protect_efficiency"

    summary = summarize_run_for_budget(run, dataset_rows)
    channels = summary["channels"]
    if not channels:
        decision = {
            "status": "blocked",
            "subtitle": "The MMM run has no ROI or contribution outputs yet.",
            "blockers": ["Finish an MMM run with ROI and contribution outputs before requesting budget recommendations."],
            "warnings": [],
            "actions": _build_recommended_actions("blocked", False, False),
        }
        return {
            "run_id": run_id,
            "objective": objective,
            "recommendations": [],
            "decision": decision,
            "summary": {
                "total_budget_change_pct": total_budget_change_pct,
                "baseline_spend_total": 0.0,
            },
        }

    roi_map = summary["roi_map"]
    contrib_map = summary["contrib_map"]
    baseline_spend = summary["baseline_spend"]
    observed_ranges = summary["observed_ranges"]
    total_spend = summary["total_spend"]
    confidence_score = summary["confidence_score"]

    scored_channels = []
    for ch in channels:
        roi = roi_map.get(ch, 0.0)
        contrib = max(contrib_map.get(ch, 0.0), 0.0)
        base_spend = baseline_spend.get(ch, 0.0)
        channel_summary = summary["channel_summary_map"].get(ch) or {}
        mroas = _safe_float(channel_summary.get("mroas"), roi)
        elasticity = _safe_float(channel_summary.get("elasticity"), 0.0)
        score = (roi * 0.55) + (mroas * 0.3) + (contrib * 0.1) + (elasticity * 0.05)
        scored_channels.append(
            {
                "channel": ch,
                "roi": roi,
                "contrib": contrib,
                "score": score,
                "base_spend": base_spend,
                "mroas": mroas,
                "elasticity": elasticity,
            }
        )

    scored_channels.sort(key=lambda item: item["score"], reverse=True)
    increases = scored_channels[: min(2, len(scored_channels))]
    decreases = sorted(scored_channels, key=lambda item: item["score"])[: min(2, len(scored_channels))]

    if objective == "hit_target_roas":
        increases = [item for item in scored_channels if item["roi"] >= max(summary["weighted_roi"], 1.0)][:2] or increases
        decreases = [item for item in sorted(scored_channels, key=lambda item: item["roi"]) if item["roi"] < summary["weighted_roi"]][:2] or decreases

    net_reallocated = 0.0
    actions: List[Dict[str, Any]] = []
    evidence: List[str] = []
    has_extrapolation = False

    for index, item in enumerate(increases):
        delta_pct = _proposed_delta_pct(objective, index, "increase")
        delta_amount = item["base_spend"] * delta_pct
        new_spend = item["base_spend"] + delta_amount
        observed_max = observed_ranges.get(item["channel"], {}).get("max", 0.0)
        extrapolation = observed_max > 0 and new_spend > observed_max * 1.05
        has_extrapolation = has_extrapolation or extrapolation
        net_reallocated += delta_amount
        actions.append(
            {
                "channel": item["channel"],
                "action": "increase",
                "delta_pct": round(delta_pct, 4),
                "delta_amount": round(delta_amount, 2),
                "base_spend": round(item["base_spend"], 2),
                "new_spend": round(new_spend, 2),
                "reason": "High modeled return and contribution support additional spend.",
                "outside_observed_range": extrapolation,
            }
        )
        evidence.append(
            f"{item['channel']} ranks near the top on modeled return and contribution share."
        )

    for index, item in enumerate(decreases):
        if any(existing["channel"] == item["channel"] for existing in actions):
            continue
        delta_pct = _proposed_delta_pct(objective, index, "decrease")
        delta_amount = item["base_spend"] * delta_pct
        new_spend = max(0.0, item["base_spend"] + delta_amount)
        observed_min = observed_ranges.get(item["channel"], {}).get("min", 0.0)
        extrapolation = observed_min > 0 and new_spend < observed_min * 0.95
        has_extrapolation = has_extrapolation or extrapolation
        net_reallocated += delta_amount
        actions.append(
            {
                "channel": item["channel"],
                "action": "decrease",
                "delta_pct": round(delta_pct, 4),
                "delta_amount": round(delta_amount, 2),
                "base_spend": round(item["base_spend"], 2),
                "new_spend": round(new_spend, 2),
                "reason": "Lower modeled return than other channels in the current mix.",
                "outside_observed_range": extrapolation,
            }
        )
        evidence.append(
            f"{item['channel']} trails the portfolio on modeled return, so it funds the reallocation."
        )

    absolute_budget_change = total_spend * (total_budget_change_pct / 100.0)
    expected_delta_pct = (
        (sum(max(action["delta_pct"], 0.0) * max(roi_map.get(action["channel"], 0.0), 0.0) for action in actions) * 10.0)
        + (total_budget_change_pct * max(summary["weighted_roi"], 0.0) * 0.02)
    )
    if objective == "protect_efficiency":
        expected_delta_pct = max(1.0, expected_delta_pct * 0.65)
    elif objective == "hit_target_roas":
        expected_delta_pct = max(0.5, expected_delta_pct * 0.55)
    else:
        expected_delta_pct = max(1.5, expected_delta_pct * 0.85)

    low_confidence = confidence_score < 0.55
    status = "ready"
    blockers: List[str] = []
    warnings: List[str] = []
    if low_confidence:
        status = "warning"
        warnings.append("Model confidence is limited. Treat the recommendation as directional and validate with a scenario review.")
    if has_extrapolation:
        status = "warning"
        warnings.append("At least one action pushes spend outside the historical range observed in the selected dataset.")
    if summary["negative_roi_channels"]:
        warnings.append("Some channels have negative modeled ROI, which usually signals unstable or constrained model fit.")

    if summary["row_count"] < 12:
        status = "blocked"
        blockers.append("There are fewer than 12 periods in the MMM dataset, so budget recommendations are not reliable enough.")

    decision = {
        "status": status,
        "subtitle": "Recommendations combine modeled ROI, contribution share, and spend-range guardrails.",
        "blockers": blockers,
        "warnings": warnings,
        "actions": _build_recommended_actions(status, low_confidence, has_extrapolation),
    }

    recommendation = {
        "id": f"{objective}:{run_id}:top",
        "objective": objective,
        "scope": "channel",
        "status": status,
        "title": {
            "protect_efficiency": "Protect efficiency",
            "grow_conversions": "Grow conversions",
            "hit_target_roas": "Hit target ROAS",
        }[objective],
        "summary": {
            "protect_efficiency": "Trim weaker channels and protect spend in the highest-confidence performers.",
            "grow_conversions": "Shift budget toward the strongest modeled growth channels while keeping total spend near plan.",
            "hit_target_roas": "Concentrate budget in channels clearing the current modeled portfolio return.",
        }[objective],
        "expected_impact": {
            "metric": "kpi_index",
            "delta_pct": round(expected_delta_pct, 2),
            "delta_abs": round((expected_delta_pct / 100.0) * max(total_spend, 1.0), 2),
            "net_budget_change": round(absolute_budget_change, 2),
            "reallocated_spend": round(abs(net_reallocated), 2),
        },
        "confidence": {
            "score": round(confidence_score, 2),
            "band": _band_from_score(confidence_score),
        },
        "risk": {
            "extrapolation": "medium" if has_extrapolation else "low",
            "readiness": "medium" if low_confidence else "low",
        },
        "actions": actions,
        "evidence": evidence[:4],
        "decision": decision,
    }

    return {
        "run_id": run_id,
        "objective": objective,
        "recommendations": [recommendation],
        "decision": decision,
        "summary": {
            "total_budget_change_pct": total_budget_change_pct,
            "baseline_spend_total": round(total_spend, 2),
            "channels_considered": len(channels),
            "periods": summary["row_count"],
            "weighted_roi": round(summary["weighted_roi"], 4),
        },
    }


def create_budget_scenario(
    db: Session,
    *,
    run_id: str,
    objective: str,
    total_budget_change_pct: float,
    multipliers: Mapping[str, float],
    recommendations: List[Mapping[str, Any]],
    summary: Mapping[str, Any],
    created_by: str,
) -> BudgetScenario:
    scenario = BudgetScenario(
        id=_new_id(),
        run_id=run_id,
        objective=objective,
        total_budget_change_pct=total_budget_change_pct,
        multipliers_json={str(key): _safe_float(value, 1.0) for key, value in multipliers.items()},
        summary_json=dict(summary or {}),
        created_by=created_by or "ui",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(scenario)
    for rank, recommendation in enumerate(recommendations, start=1):
        rec = BudgetRecommendation(
            id=_new_id(),
            scenario_id=scenario.id,
            run_id=run_id,
            objective=str(recommendation.get("objective") or objective),
            rank=rank,
            scope=str(recommendation.get("scope") or "channel"),
            status=str(recommendation.get("status") or "warning"),
            title=str(recommendation.get("title") or "Budget recommendation"),
            summary=str(recommendation.get("summary") or ""),
            expected_impact_json=dict(recommendation.get("expected_impact") or {}),
            confidence_json=dict(recommendation.get("confidence") or {}),
            risk_json=dict(recommendation.get("risk") or {}),
            evidence_json=list(recommendation.get("evidence") or []),
            decision_json=dict(recommendation.get("decision") or {}),
            created_at=datetime.utcnow(),
        )
        db.add(rec)
        for action in recommendation.get("actions") or []:
            db.add(
                BudgetRecommendationAction(
                    recommendation_id=rec.id,
                    channel=str(action.get("channel") or ""),
                    campaign_id=action.get("campaign_id"),
                    action=str(action.get("action") or "hold"),
                    delta_pct=_safe_float(action.get("delta_pct"), None) if action.get("delta_pct") is not None else None,
                    delta_amount=_safe_float(action.get("delta_amount"), None) if action.get("delta_amount") is not None else None,
                    metadata_json={
                        "base_spend": action.get("base_spend"),
                        "new_spend": action.get("new_spend"),
                        "reason": action.get("reason"),
                        "outside_observed_range": bool(action.get("outside_observed_range")),
                    },
                    created_at=datetime.utcnow(),
                )
            )
    db.commit()
    db.refresh(scenario)
    return scenario


def serialize_budget_scenario(db: Session, scenario: BudgetScenario) -> Dict[str, Any]:
    recommendations = (
        db.query(BudgetRecommendation)
        .filter(BudgetRecommendation.scenario_id == scenario.id)
        .order_by(BudgetRecommendation.rank.asc())
        .all()
    )
    recommendation_ids = [rec.id for rec in recommendations]
    actions_by_rec: Dict[str, List[BudgetRecommendationAction]] = {rec_id: [] for rec_id in recommendation_ids}
    if recommendation_ids:
        rows = (
            db.query(BudgetRecommendationAction)
            .filter(BudgetRecommendationAction.recommendation_id.in_(recommendation_ids))
            .all()
        )
        for row in rows:
            actions_by_rec.setdefault(row.recommendation_id, []).append(row)

    return {
        "id": scenario.id,
        "run_id": scenario.run_id,
        "objective": scenario.objective,
        "total_budget_change_pct": scenario.total_budget_change_pct,
        "multipliers": dict(scenario.multipliers_json or {}),
        "summary": dict(scenario.summary_json or {}),
        "created_by": scenario.created_by,
        "created_at": scenario.created_at.isoformat() if scenario.created_at else None,
        "recommendations": [
            {
                "id": rec.id,
                "objective": rec.objective,
                "scope": rec.scope,
                "status": rec.status,
                "title": rec.title,
                "summary": rec.summary,
                "expected_impact": dict(rec.expected_impact_json or {}),
                "confidence": dict(rec.confidence_json or {}),
                "risk": dict(rec.risk_json or {}),
                "evidence": list(rec.evidence_json or []),
                "decision": dict(rec.decision_json or {}),
                "actions": [
                    {
                        "channel": action.channel,
                        "campaign_id": action.campaign_id,
                        "action": action.action,
                        "delta_pct": action.delta_pct,
                        "delta_amount": action.delta_amount,
                        **dict(action.metadata_json or {}),
                    }
                    for action in actions_by_rec.get(rec.id, [])
                ],
            }
            for rec in recommendations
        ],
    }
