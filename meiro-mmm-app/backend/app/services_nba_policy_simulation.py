from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import JourneyHypothesis, JourneyPathDaily


def _normalize_steps(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(">") if part.strip()]
    return []


def _confidence_label(support_count: int, uplift_abs: float) -> str:
    if support_count >= 250 and uplift_abs >= 0.03:
        return "high"
    if support_count >= 80 and uplift_abs >= 0.015:
        return "medium"
    return "low"


def build_journey_policy_simulation(
    db: Session,
    *,
    hypothesis: JourneyHypothesis,
    proposed_step: Optional[str] = None,
) -> Dict[str, Any]:
    segment = dict(hypothesis.segment_json or {})
    trigger = dict(hypothesis.trigger_json or {})
    proposed_action = dict(hypothesis.proposed_action_json or {})
    trigger_steps = _normalize_steps(trigger.get("steps"))
    prefix_steps = trigger_steps[:-1] if len(trigger_steps) > 1 else trigger_steps
    current_step = trigger_steps[-1] if len(trigger_steps) > 1 else None

    if not prefix_steps:
        return {
            "previewAvailable": False,
            "reason": "Simulation unavailable because the hypothesis has no trigger prefix steps.",
            "summary": {},
            "top_candidates": [],
            "selected_policy": None,
            "current_path": None,
        }

    q = db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == hypothesis.journey_definition_id,
    )
    if segment.get("channel_group"):
        q = q.filter(JourneyPathDaily.channel_group == str(segment["channel_group"]))
    if segment.get("campaign_id"):
        q = q.filter(JourneyPathDaily.campaign_id == str(segment["campaign_id"]))
    if segment.get("device"):
        q = q.filter(JourneyPathDaily.device == str(segment["device"]))
    if segment.get("country"):
        q = q.filter(JourneyPathDaily.country.ilike(str(segment["country"])))

    rows = q.order_by(JourneyPathDaily.date.asc(), JourneyPathDaily.path_hash.asc()).all()
    if not rows:
        return {
            "previewAvailable": False,
            "reason": "Simulation unavailable because no journey paths match the hypothesis segment.",
            "summary": {},
            "top_candidates": [],
            "selected_policy": None,
            "current_path": None,
        }

    candidate_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0.0, "conversions": 0.0, "value": 0.0})
    prefix_support = 0.0
    prefix_conversions = 0.0
    exact_path_support = 0.0
    exact_path_conversions = 0.0
    dates = [row.date for row in rows if row.date is not None]

    for row in rows:
        steps = _normalize_steps(row.path_steps)
        if len(steps) < len(prefix_steps) or steps[: len(prefix_steps)] != prefix_steps:
            continue
        row_journeys = float(row.count_journeys or 0)
        row_conversions = float(row.count_conversions or 0)
        prefix_support += row_journeys
        prefix_conversions += row_conversions

        if trigger_steps and steps == trigger_steps:
            exact_path_support += row_journeys
            exact_path_conversions += row_conversions

        if len(steps) <= len(prefix_steps):
            continue
        next_step = steps[len(prefix_steps)]
        candidate_stats[next_step]["count"] += row_journeys
        candidate_stats[next_step]["conversions"] += row_conversions
        candidate_stats[next_step]["value"] += float(row.gross_revenue_total or 0.0)

    if prefix_support <= 0 or not candidate_stats:
        return {
            "previewAvailable": False,
            "reason": "Simulation unavailable because the trigger prefix has no observed next-step candidates.",
            "summary": {},
            "top_candidates": [],
            "selected_policy": None,
            "current_path": None,
        }

    baseline_rate = prefix_conversions / prefix_support if prefix_support > 0 else 0.0
    current_rate = exact_path_conversions / exact_path_support if exact_path_support > 0 else baseline_rate

    candidates: List[Dict[str, Any]] = []
    for rank, (step, stats) in enumerate(
        sorted(
            candidate_stats.items(),
            key=lambda item: (
                (item[1]["conversions"] / item[1]["count"]) if item[1]["count"] > 0 else 0.0,
                (item[1]["value"] / item[1]["count"]) if item[1]["count"] > 0 else 0.0,
                item[1]["count"],
            ),
            reverse=True,
        ),
        start=1,
    ):
        support_count = int(stats["count"])
        conv_rate = (stats["conversions"] / stats["count"]) if stats["count"] > 0 else 0.0
        avg_value = (stats["value"] / stats["count"]) if stats["count"] > 0 else 0.0
        uplift_abs = conv_rate - current_rate
        uplift_rel = (uplift_abs / current_rate) if current_rate > 0 else None
        candidates.append(
            {
                "rank": rank,
                "step": step,
                "support_count": support_count,
                "conversion_rate": round(conv_rate, 4),
                "avg_value": round(avg_value, 2),
                "uplift_abs": round(uplift_abs, 4),
                "uplift_rel": round(uplift_rel, 4) if uplift_rel is not None else None,
                "estimated_incremental_conversions": int(max(0.0, prefix_support * max(0.0, uplift_abs))),
                "confidence": _confidence_label(support_count, max(0.0, uplift_abs)),
                "is_current_step": step == current_step,
            }
        )

    requested_step = (
        (proposed_step or "").strip()
        or str(proposed_action.get("step") or "").strip()
        or str(proposed_action.get("channel") or "").strip()
    )
    selected_candidate = None
    if requested_step:
        selected_candidate = next((item for item in candidates if item["step"] == requested_step), None)
    if selected_candidate is None:
        selected_candidate = candidates[0]

    selected_candidate = {
        **selected_candidate,
        "rationale": (
            "Selected from observed next-step candidates for the same prefix and segment."
            if selected_candidate["step"] != current_step
            else "Represents the current terminal step for this prefix."
        ),
    }

    source_window = {
        "date_from": min(dates).isoformat() if dates else None,
        "date_to": max(dates).isoformat() if dates else None,
    }
    status = "ready" if selected_candidate["support_count"] >= 50 else "warning"
    warnings: List[str] = []
    if selected_candidate["support_count"] < 50:
        warnings.append("Selected next-step evidence is thin; treat the uplift estimate as directional.")
    if selected_candidate["step"] == current_step:
        warnings.append("The selected policy matches the current observed path step, so this preview is a baseline comparison.")

    return {
        "previewAvailable": True,
        "reason": None,
        "source_window": source_window,
        "prefix": {
            "steps": prefix_steps,
            "label": " > ".join(prefix_steps),
            "current_step": current_step,
        },
        "summary": {
            "eligible_journeys": int(prefix_support),
            "baseline_conversion_rate": round(baseline_rate, 4),
            "current_path_support": int(exact_path_support),
            "current_path_conversion_rate": round(current_rate, 4),
            "candidate_count": len(candidates),
            "sample_size_target": hypothesis.sample_size_target,
            "observational_only": True,
        },
        "top_candidates": candidates[:6],
        "selected_policy": selected_candidate,
        "current_path": {
            "step": current_step,
            "support_count": int(exact_path_support),
            "conversion_rate": round(current_rate, 4),
        },
        "decision": {
            "status": status,
            "warnings": warnings,
            "recommended_action": (
                "Promote this candidate into an experiment if operationally feasible."
                if selected_candidate["step"] != current_step
                else "Select an alternative next step to estimate directional upside before launching a test."
            ),
        },
    }
