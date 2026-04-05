"""
Incrementality experiment automation service.

Features:
- Automated assignment: hash-based deterministic assignment for stable control/treatment groups
- Exposure tracking: log when profiles are exposed to treatment (e.g., message sent)
- Nightly reporting: scheduled job to compute daily snapshots and send alerts
- Power analysis: estimate required sample size for target MDE
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta
from statistics import NormalDist
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from .models_config_dq import (
    Experiment,
    ExperimentAssignment,
    ExperimentExposure,
    ExperimentOutcome,
    ExperimentResult,
)

logger = logging.getLogger(__name__)

OWNED_CHANNEL_HINTS = {"email", "push", "sms", "whatsapp", "onsite", "in_app", "app_push"}
NON_EXPERIMENTABLE_CHANNELS = {"direct", "unknown"}


def create_experiment_record(
    db: Session,
    *,
    name: str,
    channel: str,
    start_at: datetime,
    end_at: datetime,
    conversion_key: Optional[str] = None,
    notes: Optional[str] = None,
    status: str = "draft",
    experiment_type: str = "holdout",
    source_type: Optional[str] = None,
    source_id: Optional[str] = None,
    segment: Optional[Dict[str, Any]] = None,
    policy: Optional[Dict[str, Any]] = None,
    guardrails: Optional[Dict[str, Any]] = None,
    config_id: Optional[str] = None,
    config_version: Optional[int] = None,
) -> Experiment:
    exp = Experiment(
        name=name.strip(),
        channel=(channel or "journey").strip() or "journey",
        start_at=start_at,
        end_at=end_at,
        status=(status or "draft").strip() or "draft",
        experiment_type=(experiment_type or "holdout").strip() or "holdout",
        source_type=(source_type or "").strip() or None,
        source_id=(source_id or "").strip() or None,
        segment_json=segment or {},
        policy_json=policy or {},
        guardrails_json=guardrails or {},
        config_id=(config_id or "").strip() or None,
        config_version=config_version,
        conversion_key=(conversion_key or "").strip() or None,
        notes=notes,
        created_at=datetime.utcnow(),
    )
    db.add(exp)
    db.commit()
    db.refresh(exp)
    return exp


def serialize_experiment_setup(exp: Experiment) -> Dict[str, Any]:
    policy = dict(exp.policy_json or {})
    guardrails = dict(exp.guardrails_json or {})
    power_plan = dict(guardrails.get("power_plan") or {})
    planner_window = dict(guardrails.get("planner_window") or {})
    return {
        "setup_source": policy.get("setup_source"),
        "assignment_unit": policy.get("assignment_unit"),
        "assignment_method": policy.get("assignment_method"),
        "treatment_rate": policy.get("treatment_rate"),
        "baseline_rate_estimate": policy.get("baseline_rate_estimate"),
        "kpi_label": policy.get("kpi_label"),
        "min_runtime_days": guardrails.get("min_runtime_days"),
        "exclusion_window_days": guardrails.get("exclusion_window_days"),
        "stop_rule": guardrails.get("stop_rule"),
        "planner_window": {
            "date_from": planner_window.get("date_from"),
            "date_to": planner_window.get("date_to"),
        },
        "power_plan": {
            "baseline_rate": power_plan.get("baseline_rate"),
            "mde": power_plan.get("mde"),
            "alpha": power_plan.get("alpha"),
            "power": power_plan.get("power"),
            "treatment_rate": power_plan.get("treatment_rate"),
        },
        "config_id": exp.config_id,
        "config_version": exp.config_version,
    }


def build_experiment_execution_capability(
    channel: Optional[str],
    *,
    has_observed_data: bool = True,
    non_converted_journeys: Optional[int] = None,
) -> Dict[str, Any]:
    normalized_channel = str(channel or "").strip() or "unknown"
    if normalized_channel in NON_EXPERIMENTABLE_CHANNELS:
        return {
            "channel": normalized_channel,
            "status": "not_supported",
            "planner_ready": False,
            "can_auto_assign_history": False,
            "can_log_exposures": False,
            "can_log_outcomes": True,
            "can_start_with_manual_logging": False,
            "assignment_mode": "none",
            "exposure_mode": "none",
            "delivery_support": "unsupported",
            "history_support": "none",
            "tracking_requirements": ["outcomes"],
            "launch_blockers": ["Direct and unknown traffic are not valid holdout targets."],
            "launch_warnings": [],
            "notes": ["Direct and unknown traffic are not valid holdout targets."],
        }

    notes: List[str] = []
    launch_warnings: List[str] = []
    planner_ready = normalized_channel in OWNED_CHANNEL_HINTS
    if non_converted_journeys is not None and non_converted_journeys <= 0:
        notes.append("No non-converted journeys were observed for this channel in the selected window.")
    if planner_ready:
        notes.append("Planner-backed setup is available for this owned channel.")
        launch_warnings.append(
            "Delivery and exposure capture still need operator wiring even when planner defaults are available."
        )
    else:
        notes.append("This channel can be analyzed, but delivery and exposure instrumentation are still manual.")
        launch_warnings.append("Assignment, delivery, and exposure logging need to be operated manually.")
    if has_observed_data:
        notes.append("Historical assignments can be seeded from observed journeys.")
    else:
        notes.append("No observed journeys available for historical auto-assignment.")
        launch_warnings.append("Historical auto-assignment is unavailable because no observed journeys were found.")

    return {
        "channel": normalized_channel,
        "status": "planner_ready" if planner_ready else "manual_only",
        "planner_ready": planner_ready,
        "can_auto_assign_history": has_observed_data,
        "can_log_exposures": True,
        "can_log_outcomes": True,
        "can_start_with_manual_logging": True,
        "assignment_mode": "auto_assign_from_journeys" if has_observed_data else "manual_batch",
        "exposure_mode": "manual_batch",
        "delivery_support": "planner_guided" if planner_ready else "manual",
        "history_support": "observed_journeys" if has_observed_data else "none",
        "tracking_requirements": ["assignments", "exposures", "outcomes"],
        "launch_blockers": [],
        "launch_warnings": launch_warnings,
        "notes": notes,
    }


def serialize_experiment_summary(
    exp: Experiment,
    *,
    source_name: Optional[str] = None,
    source_journey_definition_id: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "id": exp.id,
        "name": exp.name,
        "channel": exp.channel,
        "start_at": exp.start_at,
        "end_at": exp.end_at,
        "status": exp.status,
        "conversion_key": exp.conversion_key,
        "experiment_type": exp.experiment_type or "holdout",
        "source_type": exp.source_type,
        "source_id": exp.source_id,
        "source_name": source_name,
        "source_journey_definition_id": source_journey_definition_id,
        "config_id": exp.config_id,
        "config_version": exp.config_version,
        "execution": build_experiment_execution_capability(exp.channel, has_observed_data=True),
    }


def serialize_experiment_detail(
    exp: Experiment,
    *,
    source_name: Optional[str] = None,
    source_journey_definition_id: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        **serialize_experiment_summary(
            exp,
            source_name=source_name,
            source_journey_definition_id=source_journey_definition_id,
        ),
        "notes": exp.notes,
        "segment": dict(exp.segment_json or {}),
        "policy": dict(exp.policy_json or {}),
        "guardrails": dict(exp.guardrails_json or {}),
        "setup": serialize_experiment_setup(exp),
        "execution": build_experiment_execution_capability(
            exp.channel,
            has_observed_data=True,
        ),
    }


def _journey_profile_id(journey: Dict[str, Any]) -> str:
    for key in ("customer_id", "profile_id", "id"):
        value = str(journey.get(key) or "").strip()
        if value:
            return value
    customer = journey.get("customer") or {}
    if isinstance(customer, dict):
        value = str(customer.get("id") or "").strip()
        if value:
            return value
    return ""


def _touchpoint_datetime(touchpoint: Dict[str, Any]) -> Optional[datetime]:
    raw = touchpoint.get("timestamp") or touchpoint.get("ts")
    if not raw:
        return None
    try:
        ts = pd.to_datetime(raw, errors="coerce", utc=True)
    except Exception:
        return None
    if ts is None or pd.isna(ts):
        return None
    try:
        ts_py = ts.to_pydatetime()
    except Exception:
        return None
    if getattr(ts_py, "tzinfo", None) is not None:
        return ts_py.replace(tzinfo=None)
    return ts_py


def build_channel_observation_provenance(
    *,
    journeys: List[Dict[str, Any]],
    channel: Optional[str],
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized_channel = str(channel or "").strip()
    source_counts: Dict[str, int] = {}
    medium_counts: Dict[str, int] = {}
    campaign_counts: Dict[str, int] = {}
    recent_examples: List[Tuple[datetime, Dict[str, Any]]] = []
    touchpoint_count = 0

    if not normalized_channel:
        return {
            "touchpoints": 0,
            "source_examples": [],
            "medium_examples": [],
            "campaign_examples": [],
            "recent_examples": [],
        }

    def _top_examples(counter: Dict[str, int], limit: int = 5) -> List[str]:
        return [
            key
            for key, _count in sorted(counter.items(), key=lambda item: (-int(item[1] or 0), str(item[0])))
            if key
        ][:limit]

    for journey in journeys or []:
        filtered_touchpoints = _filtered_touchpoints_for_window(
            journey,
            date_from=date_from,
            date_to=date_to,
        )
        for tp, tp_dt in filtered_touchpoints:
            channel_value = str((tp.get("channel") or "unknown")).strip() or "unknown"
            if channel_value != normalized_channel:
                continue
            touchpoint_count += 1
            source_value = str(
                tp.get("source")
                or (tp.get("utm") or {}).get("source")
                or ""
            ).strip()
            medium_value = str(
                tp.get("medium")
                or (tp.get("utm") or {}).get("medium")
                or ""
            ).strip()
            campaign = tp.get("campaign") or {}
            campaign_value = ""
            if isinstance(campaign, dict):
                campaign_value = str(campaign.get("name") or campaign.get("id") or "").strip()
            if not campaign_value:
                campaign_value = str(
                    (tp.get("utm") or {}).get("campaign")
                    or tp.get("campaign_name")
                    or ""
                ).strip()
            if source_value:
                source_counts[source_value] = source_counts.get(source_value, 0) + 1
            if medium_value:
                medium_counts[medium_value] = medium_counts.get(medium_value, 0) + 1
            if campaign_value:
                campaign_counts[campaign_value] = campaign_counts.get(campaign_value, 0) + 1
            if tp_dt is not None:
                recent_examples.append(
                    (
                        tp_dt,
                        {
                            "timestamp": tp_dt.isoformat(),
                            "source": source_value or None,
                            "medium": medium_value or None,
                            "campaign": campaign_value or None,
                        },
                    )
                )

    recent_examples.sort(key=lambda item: item[0], reverse=True)
    return {
        "touchpoints": touchpoint_count,
        "source_examples": _top_examples(source_counts),
        "medium_examples": _top_examples(medium_counts),
        "campaign_examples": _top_examples(campaign_counts),
        "recent_examples": [item[1] for item in recent_examples[:5]],
    }


def build_experiment_setup_context(
    *,
    journeys: List[Dict[str, Any]],
    kpi_config: Any,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    channel_stats: Dict[str, Dict[str, Any]] = {}
    kpi_counts: Dict[str, int] = {}
    observed_kpis: set[str] = set()
    total_window_journeys = 0
    window_converted = 0
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None

    for journey in journeys or []:
        touchpoints = journey.get("touchpoints") or []
        if not isinstance(touchpoints, list):
            continue
        filtered_touchpoints: List[Tuple[Dict[str, Any], Optional[datetime]]] = []
        for tp in touchpoints:
            if not isinstance(tp, dict):
                continue
            tp_dt = _touchpoint_datetime(tp)
            if date_from and tp_dt and tp_dt < date_from:
                continue
            if date_to and tp_dt and tp_dt >= date_to:
                continue
            if date_from and date_to and tp_dt is None:
                continue
            filtered_touchpoints.append((tp, tp_dt))
            if tp_dt is not None:
                first_seen = tp_dt if first_seen is None else min(first_seen, tp_dt)
                last_seen = tp_dt if last_seen is None else max(last_seen, tp_dt)
        if not filtered_touchpoints:
            continue

        total_window_journeys += 1
        converted = bool(journey.get("converted", True))
        if converted:
            window_converted += 1

        kpi_id = str(journey.get("kpi_type") or "").strip()
        if kpi_id:
            observed_kpis.add(kpi_id)
            kpi_counts[kpi_id] = kpi_counts.get(kpi_id, 0) + 1

        profile_id = _journey_profile_id(journey)
        unique_channels = {str((tp.get("channel") or "unknown")).strip() or "unknown" for tp, _tp_dt in filtered_touchpoints}
        for channel in unique_channels:
            stat = channel_stats.setdefault(
                channel,
                {
                    "channel": channel,
                    "label": channel,
                    "journeys": 0,
                    "converted_journeys": 0,
                    "non_converted_journeys": 0,
                    "profiles": set(),
                    "last_seen_at": None,
                },
            )
            stat["journeys"] += 1
            if converted:
                stat["converted_journeys"] += 1
            else:
                stat["non_converted_journeys"] += 1
            if profile_id:
                stat["profiles"].add(profile_id)
            channel_touch_times = [tp_dt for tp, tp_dt in filtered_touchpoints if str((tp.get("channel") or "unknown")).strip() == channel and tp_dt is not None]
            if channel_touch_times:
                ch_last = max(channel_touch_times)
                stat["last_seen_at"] = ch_last if stat["last_seen_at"] is None else max(stat["last_seen_at"], ch_last)

    channel_rows: List[Dict[str, Any]] = []
    for channel, stat in channel_stats.items():
        journeys_count = int(stat["journeys"] or 0)
        converted_journeys = int(stat["converted_journeys"] or 0)
        non_converted_journeys = int(stat["non_converted_journeys"] or 0)
        eligible = channel not in NON_EXPERIMENTABLE_CHANNELS and journeys_count > 0
        notes: List[str] = []
        if non_converted_journeys <= 0:
            notes.append("No non-converted journeys observed in the selected window.")
        elif non_converted_journeys < 10:
            notes.append("Very few non-converted journeys observed; baseline may be noisy.")
        if channel in NON_EXPERIMENTABLE_CHANNELS:
            notes.append("This channel is not a useful holdout target.")
        channel_rows.append(
            {
                "channel": channel,
                "label": channel,
                "journeys": journeys_count,
                "converted_journeys": converted_journeys,
                "non_converted_journeys": non_converted_journeys,
                "observed_profiles": len(stat["profiles"]),
                "baseline_conversion_rate": round(converted_journeys / float(journeys_count or 1), 4),
                "share_of_journeys": round(journeys_count / float(total_window_journeys or 1), 4),
                "last_seen_at": stat["last_seen_at"].isoformat() if stat["last_seen_at"] else None,
                "eligible": eligible,
                "delivery_class": "owned" if channel in OWNED_CHANNEL_HINTS else "observed",
                "provenance": build_channel_observation_provenance(
                    journeys=journeys,
                    channel=channel,
                    date_from=date_from,
                    date_to=date_to,
                ),
                "notes": notes,
                "execution": build_experiment_execution_capability(
                    channel,
                    has_observed_data=journeys_count > 0,
                    non_converted_journeys=non_converted_journeys,
                ),
            }
        )
    channel_rows.sort(
        key=lambda item: (
            0 if item["eligible"] else 1,
            0 if item["delivery_class"] == "owned" else 1,
            -int(item["journeys"] or 0),
            str(item["channel"]),
        )
    )

    definitions = list(getattr(kpi_config, "definitions", []) or [])
    configured_ids = {str(definition.id) for definition in definitions if getattr(definition, "id", None)}
    kpi_rows: List[Dict[str, Any]] = []
    for definition in definitions:
        definition_id = str(getattr(definition, "id", "") or "").strip()
        if not definition_id:
            continue
        kpi_rows.append(
            {
                "id": definition_id,
                "label": str(getattr(definition, "label", definition_id) or definition_id),
                "type": str(getattr(definition, "type", "primary") or "primary"),
                "event_name": str(getattr(definition, "event_name", "") or ""),
                "count": int(kpi_counts.get(definition_id, 0)),
                "is_primary": definition_id == str(getattr(kpi_config, "primary_kpi_id", None) or ""),
            }
        )
    kpi_rows.sort(key=lambda item: (0 if item["is_primary"] else 1, -int(item["count"] or 0), item["label"]))

    default_channel_row = next(
        (
            row
            for row in channel_rows
            if row["eligible"] and row["non_converted_journeys"] > 0
        ),
        next((row for row in channel_rows if row["eligible"]), None),
    )
    default_kpi = str(getattr(kpi_config, "primary_kpi_id", None) or "") or (kpi_rows[0]["id"] if kpi_rows else "")
    default_treatment_rate = 0.9 if (default_channel_row or {}).get("delivery_class") == "owned" else 0.8
    default_runtime = 14 if ((default_channel_row or {}).get("journeys") or 0) >= 100 else 21

    warnings: List[str] = []
    if total_window_journeys <= 0:
        warnings.append("No journey data found in the selected date range.")
    if channel_rows and not any(row["eligible"] for row in channel_rows):
        warnings.append("Only direct or unknown channels were observed in the selected date range.")
    if total_window_journeys > 0 and window_converted >= total_window_journeys:
        warnings.append("Only converted journeys were observed; baseline conversion rates may be inflated.")
    unmapped_kpis = sorted(observed_kpis - configured_ids)
    if unmapped_kpis:
        warnings.append(f"Observed KPI ids not present in Settings: {', '.join(unmapped_kpis[:5])}.")

    return {
        "date_from": date_from.date().isoformat() if date_from else (first_seen.date().isoformat() if first_seen else None),
        "date_to": ((date_to - timedelta(days=1)).date().isoformat() if date_to else (last_seen.date().isoformat() if last_seen else None)),
        "defaults": {
            "channel": (default_channel_row or {}).get("channel"),
            "conversion_key": default_kpi or None,
            "treatment_rate": default_treatment_rate,
            "min_runtime_days": default_runtime,
            "alpha": 0.05,
            "power": 0.8,
            "mde": 0.01,
        },
        "channels": channel_rows,
        "kpis": kpi_rows,
        "summary": {
            "journeys": total_window_journeys,
            "converted_journeys": window_converted,
            "non_converted_journeys": max(total_window_journeys - window_converted, 0),
            "observed_channels": len(channel_rows),
        },
        "warnings": warnings,
    }


def _filtered_touchpoints_for_window(
    journey: Dict[str, Any],
    *,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> List[Tuple[Dict[str, Any], Optional[datetime]]]:
    touchpoints = journey.get("touchpoints") or []
    if not isinstance(touchpoints, list):
        return []
    filtered: List[Tuple[Dict[str, Any], Optional[datetime]]] = []
    for tp in touchpoints:
        if not isinstance(tp, dict):
            continue
        tp_dt = _touchpoint_datetime(tp)
        if date_from and tp_dt and tp_dt < date_from:
            continue
        if date_to and tp_dt and tp_dt >= date_to:
            continue
        if date_from and date_to and tp_dt is None:
            continue
        filtered.append((tp, tp_dt))
    return filtered


def build_experiment_design_recommendation(
    *,
    journeys: List[Dict[str, Any]],
    channel: str,
    conversion_key: Optional[str],
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    alpha: float = 0.05,
    power: float = 0.8,
    mde: float = 0.01,
) -> Dict[str, Any]:
    normalized_channel = str(channel or "").strip()
    normalized_conversion_key = str(conversion_key or "").strip() or None
    warnings: List[str] = []

    if not normalized_channel:
        return {
            "channel": None,
            "conversion_key": normalized_conversion_key,
            "recommendation": {"readiness": "no_data"},
            "warnings": ["Channel is required."],
            "observed": {},
        }

    candidate_rates = [0.9, 0.8, 0.7, 0.5] if normalized_channel in OWNED_CHANNEL_HINTS else [0.8, 0.7, 0.5]
    matched_journeys: List[Dict[str, Any]] = []
    profile_ids: set[str] = set()
    touch_dates: List[datetime] = []
    matching_kpi_conversions = 0
    any_converted = 0

    for journey in journeys or []:
        filtered_touchpoints = _filtered_touchpoints_for_window(journey, date_from=date_from, date_to=date_to)
        if not filtered_touchpoints:
            continue
        if not any(str((tp.get("channel") or "unknown")).strip() == normalized_channel for tp, _tp_dt in filtered_touchpoints):
            continue
        matched_journeys.append(journey)
        profile_id = _journey_profile_id(journey)
        if profile_id:
            profile_ids.add(profile_id)
        for _tp, tp_dt in filtered_touchpoints:
            if tp_dt is not None:
                touch_dates.append(tp_dt)
        converted = bool(journey.get("converted", True))
        if converted:
            any_converted += 1
        journey_kpi = str(journey.get("kpi_type") or "").strip() or None
        if converted and (normalized_conversion_key is None or journey_kpi == normalized_conversion_key):
            matching_kpi_conversions += 1

    total_journeys = len(matched_journeys)
    non_converted_journeys = max(total_journeys - matching_kpi_conversions, 0)
    window_days = 0
    if date_from and date_to and date_to > date_from:
        window_days = max(int((date_to - date_from).days), 1)
    elif touch_dates:
        window_days = max(int((max(touch_dates).date() - min(touch_dates).date()).days) + 1, 1)
    baseline_rate = matching_kpi_conversions / float(total_journeys or 1) if total_journeys > 0 else 0.0
    avg_daily_eligible = total_journeys / float(window_days or 1) if total_journeys > 0 else 0.0

    observed = {
        "journeys": total_journeys,
        "matching_kpi_conversions": matching_kpi_conversions,
        "non_converted_journeys": non_converted_journeys,
        "converted_journeys_any_kpi": any_converted,
        "observed_profiles": len(profile_ids),
        "baseline_conversion_rate": round(baseline_rate, 4),
        "avg_daily_eligible": round(avg_daily_eligible, 2),
        "window_days": window_days,
    }

    if total_journeys <= 0:
        return {
            "channel": normalized_channel,
            "conversion_key": normalized_conversion_key,
            "date_from": date_from.date().isoformat() if date_from else None,
            "date_to": ((date_to - timedelta(days=1)).date().isoformat() if date_to else None),
            "observed": observed,
            "recommendation": {
                "readiness": "no_data",
                "reason": "No journeys touched this channel in the selected window.",
            },
            "warnings": ["No eligible journeys found for this channel in the selected date range."],
        }

    if normalized_conversion_key and matching_kpi_conversions <= 0:
        warnings.append("No conversions were observed for the selected KPI on this channel in the selected window.")

    effective_baseline = min(max(baseline_rate, 0.01), 0.99)
    candidate_recommendations: List[Dict[str, Any]] = []
    for rate in candidate_rates:
        sample_target_total = estimate_sample_size(
            baseline_rate=effective_baseline,
            mde=max(mde, 0.001),
            alpha=alpha,
            power=power,
            treatment_rate=rate,
        )
        treatment_size = int(sample_target_total * rate)
        control_size = sample_target_total - treatment_size
        projected_runtime_days = int(max(1, round(sample_target_total / float(avg_daily_eligible or 1))))
        candidate_recommendations.append(
            {
                "treatment_rate": rate,
                "sample_target_total": sample_target_total,
                "sample_target_treatment": treatment_size,
                "sample_target_control": control_size,
                "projected_runtime_days": projected_runtime_days,
            }
        )

    selected_recommendation = candidate_recommendations[-1]
    for candidate in candidate_recommendations:
        if candidate["projected_runtime_days"] <= 21:
            selected_recommendation = candidate
            break
        selected_recommendation = candidate

    recommended_runtime = max(14, selected_recommendation["projected_runtime_days"])
    readiness = "ready_to_launch"
    reason = "Observed volume and baseline are sufficient for a practical holdout design."

    if total_journeys < 25 or len(profile_ids) < 25:
        readiness = "insufficient_signal"
        reason = "Too few observed journeys/profiles for a reliable holdout design."
        warnings.append("Observed audience is too small; experiment results will be unstable.")
    elif matching_kpi_conversions < 5:
        readiness = "insufficient_signal"
        reason = "Very few KPI conversions were observed for this channel."
        warnings.append("Observed KPI conversion count is low; baseline is noisy.")
    elif selected_recommendation["projected_runtime_days"] > 42:
        readiness = "needs_more_volume"
        reason = "Projected runtime is long for the target MDE; widen the effect threshold or use a larger audience."
        warnings.append("Projected runtime exceeds six weeks for the requested MDE.")
    elif selected_recommendation["projected_runtime_days"] > 21:
        readiness = "needs_more_volume"
        reason = "The design is possible, but the selected MDE implies a slower readout."
        warnings.append("Projected runtime is longer than three weeks.")

    recommendation = {
        **selected_recommendation,
        "min_runtime_days": recommended_runtime,
        "alpha": alpha,
        "power": power,
        "mde": mde,
        "readiness": readiness,
        "reason": reason,
    }

    return {
        "channel": normalized_channel,
        "conversion_key": normalized_conversion_key,
        "date_from": date_from.date().isoformat() if date_from else None,
        "date_to": ((date_to - timedelta(days=1)).date().isoformat() if date_to else None),
        "observed": observed,
        "recommendation": recommendation,
        "execution": build_experiment_execution_capability(
            normalized_channel,
            has_observed_data=total_journeys > 0,
            non_converted_journeys=non_converted_journeys,
        ),
        "provenance": build_channel_observation_provenance(
            journeys=journeys,
            channel=normalized_channel,
            date_from=date_from,
            date_to=date_to,
        ),
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Deterministic assignment
# ---------------------------------------------------------------------------


def deterministic_assignment(
    profile_id: str,
    experiment_id: int,
    treatment_rate: float = 0.5,
    salt: str = "",
) -> str:
    """
    Hash-based deterministic assignment.

    Returns "treatment" or "control" based on hash(profile_id + experiment_id + salt).
    This ensures:
    - Same profile always gets same group for a given experiment
    - No need to store assignments before exposure (lazy assignment)
    - Stable across restarts and distributed systems
    """
    key = f"{profile_id}:{experiment_id}:{salt}"
    h = hashlib.sha256(key.encode("utf-8")).digest()
    # Use first 8 bytes as uint64
    val = int.from_bytes(h[:8], byteorder="big")
    # Map to [0, 1)
    ratio = val / (2**64)
    return "treatment" if ratio < treatment_rate else "control"


def assign_profiles_deterministic(
    db: Session,
    experiment_id: int,
    profile_ids: List[str],
    treatment_rate: float = 0.5,
    salt: str = "",
    force_reassign: bool = False,
) -> Dict[str, int]:
    """
    Assign profiles to treatment/control using deterministic hashing.

    Parameters
    ----------
    db : database session
    experiment_id : experiment ID
    profile_ids : list of profile IDs to assign
    treatment_rate : fraction assigned to treatment (0.0 - 1.0)
    salt : optional salt for hash (use to re-randomize if needed)
    force_reassign : if True, overwrite existing assignments

    Returns
    -------
    {"treatment": count, "control": count}
    """
    if not profile_ids:
        return {"treatment": 0, "control": 0}

    # De-duplicate
    unique_ids = list(dict.fromkeys(profile_ids))

    # Check existing
    existing = {
        a.profile_id: a
        for a in db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .filter(ExperimentAssignment.profile_id.in_(unique_ids))
        .all()
    }

    counts = {"treatment": 0, "control": 0}
    now = datetime.utcnow()

    for pid in unique_ids:
        group = deterministic_assignment(pid, experiment_id, treatment_rate, salt)

        if pid in existing:
            if force_reassign:
                existing[pid].group = group
                existing[pid].assigned_at = now
        else:
            db.add(
                ExperimentAssignment(
                    experiment_id=experiment_id,
                    profile_id=pid,
                    group=group,
                    assigned_at=now,
                )
            )
        counts[group] += 1

    db.commit()
    return counts


# ---------------------------------------------------------------------------
# Exposure tracking
# ---------------------------------------------------------------------------


def record_exposure(
    db: Session,
    experiment_id: int,
    profile_id: str,
    exposure_ts: Optional[datetime] = None,
    campaign_id: Optional[str] = None,
    message_id: Optional[str] = None,
) -> None:
    """
    Record that a profile was exposed to the treatment (e.g., message sent).

    Exposures are logged separately from assignments to track actual delivery.
    """
    if exposure_ts is None:
        exposure_ts = datetime.utcnow()

    db.add(
        ExperimentExposure(
            experiment_id=experiment_id,
            profile_id=profile_id,
            exposure_ts=exposure_ts,
            campaign_id=campaign_id,
            message_id=message_id,
        )
    )
    db.commit()


def record_exposures_batch(
    db: Session,
    experiment_id: int,
    exposures: List[Dict[str, Any]],
) -> int:
    """
    Batch record exposures.

    Each exposure dict should have:
    - profile_id: str
    - exposure_ts: datetime (optional, defaults to now)
    - campaign_id: str (optional)
    - message_id: str (optional)

    Returns count of exposures recorded.
    """
    if not exposures:
        return 0

    now = datetime.utcnow()
    for exp in exposures:
        db.add(
            ExperimentExposure(
                experiment_id=experiment_id,
                profile_id=exp["profile_id"],
                exposure_ts=exp.get("exposure_ts", now),
                campaign_id=exp.get("campaign_id"),
                message_id=exp.get("message_id"),
            )
        )

    db.commit()
    return len(exposures)


# ---------------------------------------------------------------------------
# Outcome tracking
# ---------------------------------------------------------------------------


def record_outcome(
    db: Session,
    experiment_id: int,
    profile_id: str,
    conversion_ts: datetime,
    value: float = 0.0,
) -> None:
    """Record a conversion outcome for a profile in an experiment."""
    db.add(
        ExperimentOutcome(
            experiment_id=experiment_id,
            profile_id=profile_id,
            conversion_ts=conversion_ts,
            value=value,
        )
    )
    db.commit()


def record_outcomes_batch(
    db: Session,
    experiment_id: int,
    outcomes: List[Dict[str, Any]],
) -> int:
    """
    Batch record outcomes.

    Each outcome dict should have:
    - profile_id: str
    - conversion_ts: datetime
    - value: float (optional, defaults to 0.0)

    Returns count of outcomes recorded.
    """
    if not outcomes:
        return 0

    for out in outcomes:
        db.add(
            ExperimentOutcome(
                experiment_id=experiment_id,
                profile_id=out["profile_id"],
                conversion_ts=out["conversion_ts"],
                value=out.get("value", 0.0),
            )
        )

    db.commit()
    return len(outcomes)


# ---------------------------------------------------------------------------
# Results computation
# ---------------------------------------------------------------------------


def compute_experiment_results(
    db: Session,
    experiment_id: int,
    min_sample_size: int = 10,
) -> Dict[str, Any]:
    """
    Compute uplift results for an experiment.

    Returns dict with:
    - experiment_id
    - status
    - treatment: {n, conversions, conversion_rate, total_value}
    - control: {n, conversions, conversion_rate, total_value}
    - uplift_abs, uplift_rel, ci_low, ci_high, p_value
    - insufficient_data: bool
    """
    import math
    from math import erf

    exp = db.get(Experiment, experiment_id)
    if not exp:
        raise ValueError(f"Experiment {experiment_id} not found")

    assignments = (
        db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .all()
    )

    if not assignments:
        return {
            "experiment_id": experiment_id,
            "status": exp.status,
            "insufficient_data": True,
        }

    outcomes = {
        o.profile_id: o
        for o in db.query(ExperimentOutcome)
        .filter(ExperimentOutcome.experiment_id == experiment_id)
        .all()
    }

    treat_n = 0
    control_n = 0
    treat_conv = 0
    control_conv = 0
    treat_value = 0.0
    control_value = 0.0

    for a in assignments:
        out = outcomes.get(a.profile_id)
        if a.group == "treatment":
            treat_n += 1
            if out is not None:
                treat_conv += 1
                treat_value += float(out.value or 0.0)
        else:
            control_n += 1
            if out is not None:
                control_conv += 1
                control_value += float(out.value or 0.0)

    if treat_n < min_sample_size or control_n < min_sample_size:
        return {
            "experiment_id": experiment_id,
            "status": exp.status,
            "insufficient_data": True,
            "treatment": {"n": treat_n, "conversions": treat_conv},
            "control": {"n": control_n, "conversions": control_conv},
        }

    p_t = treat_conv / treat_n if treat_n > 0 else 0.0
    p_c = control_conv / control_n if control_n > 0 else 0.0
    diff = p_t - p_c
    uplift_abs = diff
    uplift_rel = diff / p_c if p_c > 0 else None

    # Normal-approx CI for diff in proportions
    se = math.sqrt(p_t * (1 - p_t) / treat_n + p_c * (1 - p_c) / control_n)
    if se > 0:
        z = 1.96
        ci_low = diff - z * se
        ci_high = diff + z * se
        z_score = diff / se
        p_value = 2 * (1 - 0.5 * (1 + erf(abs(z_score) / math.sqrt(2))))
    else:
        ci_low = None
        ci_high = None
        p_value = None

    # Upsert ExperimentResult
    res = (
        db.query(ExperimentResult)
        .filter(ExperimentResult.experiment_id == experiment_id)
        .first()
    )
    if res is None:
        res = ExperimentResult(
            experiment_id=experiment_id,
            computed_at=datetime.utcnow(),
            uplift_abs=uplift_abs,
            uplift_rel=uplift_rel,
            ci_low=ci_low,
            ci_high=ci_high,
            p_value=p_value,
            treatment_size=treat_n,
            control_size=control_n,
            meta_json={
                "treatment_conversions": treat_conv,
                "control_conversions": control_conv,
                "treatment_value": treat_value,
                "control_value": control_value,
            },
        )
        db.add(res)
    else:
        res.computed_at = datetime.utcnow()
        res.uplift_abs = uplift_abs
        res.uplift_rel = uplift_rel
        res.ci_low = ci_low
        res.ci_high = ci_high
        res.p_value = p_value
        res.treatment_size = treat_n
        res.control_size = control_n
        res.meta_json = {
            "treatment_conversions": treat_conv,
            "control_conversions": control_conv,
            "treatment_value": treat_value,
            "control_value": control_value,
        }

    db.commit()

    return {
        "experiment_id": experiment_id,
        "status": exp.status,
        "treatment": {
            "n": treat_n,
            "conversions": treat_conv,
            "conversion_rate": p_t,
            "total_value": treat_value,
        },
        "control": {
            "n": control_n,
            "conversions": control_conv,
            "conversion_rate": p_c,
            "total_value": control_value,
        },
        "uplift_abs": uplift_abs,
        "uplift_rel": uplift_rel,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": p_value,
        "insufficient_data": False,
    }


# ---------------------------------------------------------------------------
# Power analysis
# ---------------------------------------------------------------------------


def estimate_sample_size(
    baseline_rate: float,
    mde: float,
    alpha: float = 0.05,
    power: float = 0.8,
    treatment_rate: float = 0.5,
) -> int:
    """
    Estimate required sample size for a two-sample proportion test.

    Parameters
    ----------
    baseline_rate : control group conversion rate (e.g., 0.05 = 5%)
    mde : minimum detectable effect (absolute, e.g., 0.01 = 1pp)
    alpha : significance level (default 0.05)
    power : statistical power (default 0.8)
    treatment_rate : fraction in treatment (default 0.5 for balanced)

    Returns
    -------
    Total sample size (treatment + control)
    """
    import math

    dist = NormalDist()
    z_alpha = dist.inv_cdf(1 - (alpha / 2.0))
    z_beta = dist.inv_cdf(power)

    p1 = baseline_rate
    p2 = baseline_rate + mde
    p_avg = (p1 + p2) / 2

    # Formula for two-proportion z-test
    n_per_group = (
        (z_alpha * math.sqrt(2 * p_avg * (1 - p_avg)) + z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
    ) / (mde ** 2)

    n_treatment = n_per_group / treatment_rate
    n_control = n_per_group / (1 - treatment_rate)
    total = int(math.ceil(n_treatment + n_control))

    return total


# ---------------------------------------------------------------------------
# Nightly reporting
# ---------------------------------------------------------------------------


def run_nightly_report(
    db: Session,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Compute results for all running experiments and return summary.

    Intended to be called by a scheduled job (cron, Celery, etc.).

    Returns dict with:
    - as_of_date
    - experiments: list of {id, name, channel, status, results}
    - alerts: list of {experiment_id, message, severity}
    """
    if as_of_date is None:
        as_of_date = datetime.utcnow()

    # Find all running experiments
    running = (
        db.query(Experiment)
        .filter(Experiment.status == "running")
        .filter(Experiment.start_at <= as_of_date)
        .filter(Experiment.end_at >= as_of_date)
        .all()
    )

    results = []
    alerts = []

    for exp in running:
        try:
            res = compute_experiment_results(db, exp.id)
            results.append({
                "id": exp.id,
                "name": exp.name,
                "channel": exp.channel,
                "status": exp.status,
                "results": res,
            })

            # Generate alerts
            if res.get("insufficient_data"):
                alerts.append({
                    "experiment_id": exp.id,
                    "message": f"Experiment '{exp.name}' has insufficient data for uplift estimation",
                    "severity": "info",
                })
            elif res.get("p_value") is not None and res["p_value"] < 0.05:
                direction = "positive" if res["uplift_abs"] > 0 else "negative"
                alerts.append({
                    "experiment_id": exp.id,
                    "message": f"Experiment '{exp.name}' shows significant {direction} uplift (p={res['p_value']:.4f})",
                    "severity": "success" if direction == "positive" else "warning",
                })

        except Exception as e:
            logger.error(f"Failed to compute results for experiment {exp.id}: {e}")
            alerts.append({
                "experiment_id": exp.id,
                "message": f"Failed to compute results: {str(e)}",
                "severity": "error",
            })

    return {
        "as_of_date": as_of_date.isoformat(),
        "experiments": results,
        "alerts": alerts,
    }


def get_experiment_time_series(
    db: Session,
    experiment_id: int,
    freq: str = "D",
) -> pd.DataFrame:
    """
    Get daily/weekly time series of experiment metrics.

    Returns DataFrame with columns:
    - date
    - treatment_n, treatment_conversions, treatment_rate
    - control_n, control_conversions, control_rate
    - uplift_abs, uplift_rel
    """
    exp = db.get(Experiment, experiment_id)
    if not exp:
        raise ValueError(f"Experiment {experiment_id} not found")

    assignments = (
        db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .all()
    )

    if not assignments:
        return pd.DataFrame()

    outcomes = (
        db.query(ExperimentOutcome)
        .filter(ExperimentOutcome.experiment_id == experiment_id)
        .all()
    )

    # Build daily cumulative metrics
    assignment_dates = [a.assigned_at.date() for a in assignments]
    outcome_dates = [o.conversion_ts.date() for o in outcomes]

    if not assignment_dates:
        return pd.DataFrame()

    min_date = min(assignment_dates)
    max_date = max(assignment_dates + outcome_dates) if outcome_dates else min_date

    date_range = pd.date_range(start=min_date, end=max_date, freq="D")

    rows = []
    for d in date_range:
        # Assignments up to this date
        treat_n = sum(1 for a in assignments if a.group == "treatment" and a.assigned_at.date() <= d)
        control_n = sum(1 for a in assignments if a.group == "control" and a.assigned_at.date() <= d)

        # Outcomes up to this date
        assignment_pids = {a.profile_id: a.group for a in assignments if a.assigned_at.date() <= d}
        treat_conv = sum(
            1 for o in outcomes
            if o.conversion_ts.date() <= d and assignment_pids.get(o.profile_id) == "treatment"
        )
        control_conv = sum(
            1 for o in outcomes
            if o.conversion_ts.date() <= d and assignment_pids.get(o.profile_id) == "control"
        )

        treat_rate = treat_conv / treat_n if treat_n > 0 else 0.0
        control_rate = control_conv / control_n if control_n > 0 else 0.0
        uplift_abs = treat_rate - control_rate
        uplift_rel = uplift_abs / control_rate if control_rate > 0 else None

        rows.append({
            "date": d,
            "treatment_n": treat_n,
            "treatment_conversions": treat_conv,
            "treatment_rate": treat_rate,
            "control_n": control_n,
            "control_conversions": control_conv,
            "control_rate": control_rate,
            "uplift_abs": uplift_abs,
            "uplift_rel": uplift_rel,
        })

    df = pd.DataFrame(rows)

    if freq == "W":
        df["week"] = pd.to_datetime(df["date"]).dt.to_period("W").dt.start_time
        df = df.groupby("week").last().reset_index()
        df.rename(columns={"week": "date"}, inplace=True)

    return df


# ---------------------------------------------------------------------------
# Health checks / readiness
# ---------------------------------------------------------------------------


def compute_experiment_health(
    db: Session,
    experiment_id: int,
    min_assignments_per_group: int = 50,
    min_conversions_per_group: int = 20,
) -> Dict[str, Any]:
    """
    Compute high-level health signals for an experiment.

    Returns dict with:
    - experiment_id
    - sample: {"treatment": n, "control": n}
    - exposures: {"treatment": n, "control": n}
    - outcomes: {"treatment": n, "control": n}
    - balance: {"status": "ok"/"warn", "expected_share": float, "observed_share": float}
    - data_completeness: {
          "assignments": {"status": "ok"/"fail"},
          "outcomes": {"status": "ok"/"fail"},
          "exposures": {"status": "ok"/"warn"},
      }
    - overlap_risk: {"status": "ok"/"warn", "overlapping_profiles": int}
    - ready_state: {"label": "not_ready"/"early"/"ready", "reasons": [str]}
    """
    exp = db.get(Experiment, experiment_id)
    if not exp:
        raise ValueError(f"Experiment {experiment_id} not found")

    # Assignments and outcomes (reuse logic similar to compute_experiment_results)
    assignments = (
        db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .all()
    )
    assignment_groups = {a.profile_id: a.group for a in assignments}

    treat_n = sum(1 for a in assignments if a.group == "treatment")
    control_n = sum(1 for a in assignments if a.group == "control")

    outcomes_rows = (
        db.query(ExperimentOutcome)
        .filter(ExperimentOutcome.experiment_id == experiment_id)
        .all()
    )
    treat_conv = 0
    control_conv = 0
    for o in outcomes_rows:
        g = assignment_groups.get(o.profile_id)
        if g == "treatment":
            treat_conv += 1
        elif g == "control":
            control_conv += 1

    # Exposures joined with assignments (if any)
    exposures_rows = (
        db.query(ExperimentExposure)
        .filter(ExperimentExposure.experiment_id == experiment_id)
        .all()
    )
    treat_exp = 0
    control_exp = 0
    for e in exposures_rows:
        g = assignment_groups.get(e.profile_id)
        if g == "treatment":
            treat_exp += 1
        elif g == "control":
            control_exp += 1

    total_n = treat_n + control_n
    execution = build_experiment_execution_capability(
        exp.channel,
        has_observed_data=bool(total_n or treat_exp or control_exp or treat_conv or control_conv),
    )
    policy = dict(exp.policy_json or {})
    expected_share = float(policy.get("treatment_rate") or 0.5)
    expected_share = min(0.99, max(0.01, expected_share))
    observed_share = (treat_n / total_n) if total_n > 0 else 0.0
    balance_status = "ok"
    if total_n > 0 and abs(observed_share - expected_share) > 0.1:
        balance_status = "warn"

    data_completeness = {
        "assignments": {"status": "ok" if total_n > 0 else "fail"},
        "outcomes": {"status": "ok" if (treat_conv + control_conv) > 0 else "fail"},
        "exposures": {
            "status": "ok" if (treat_exp + control_exp) > 0 else "warn"
        },
    }
    tracking_gaps: List[str] = []
    if data_completeness["assignments"]["status"] != "ok":
        tracking_gaps.append("Assignments have not been logged yet.")
    if execution.get("can_log_exposures") and data_completeness["exposures"]["status"] != "ok":
        tracking_gaps.append("Exposure logging has not been observed yet.")
    if execution.get("can_log_outcomes") and data_completeness["outcomes"]["status"] != "ok":
        tracking_gaps.append("Outcome logging has not been observed yet.")

    # Minimal contamination / overlap heuristic:
    # any profile assigned in this experiment also appearing in another running
    # experiment on the same channel with overlapping period.
    overlapping_profiles = 0
    if assignments:
        profile_ids = {a.profile_id for a in assignments}
        other_experiments = (
            db.query(Experiment)
            .filter(Experiment.id != experiment_id)
            .filter(Experiment.channel == exp.channel)
            .filter(Experiment.status == "running")
            .filter(Experiment.start_at <= exp.end_at)
            .filter(Experiment.end_at >= exp.start_at)
            .all()
        )
        if other_experiments:
            other_ids = [e.id for e in other_experiments]
            overlaps = (
                db.query(ExperimentAssignment.profile_id)
                .filter(ExperimentAssignment.experiment_id.in_(other_ids))
                .filter(ExperimentAssignment.profile_id.in_(profile_ids))
                .distinct()
                .all()
            )
            overlapping_profiles = len(overlaps)
    overlap_status = "ok" if overlapping_profiles == 0 else "warn"

    guardrails = dict(exp.guardrails_json or {})
    power_plan = dict(guardrails.get("power_plan") or {})
    planned_total_target: Optional[int] = None
    planned_treatment_target: Optional[int] = None
    planned_control_target: Optional[int] = None
    if power_plan:
        try:
            planned_total_target = estimate_sample_size(
                baseline_rate=float(power_plan.get("baseline_rate") or 0.0),
                mde=float(power_plan.get("mde") or 0.0),
                alpha=float(power_plan.get("alpha") or 0.05),
                power=float(power_plan.get("power") or 0.8),
                treatment_rate=expected_share,
            )
        except Exception:
            planned_total_target = None
    if planned_total_target:
        planned_treatment_target = int(planned_total_target * expected_share)
        planned_control_target = planned_total_target - planned_treatment_target

    now = datetime.utcnow()
    runtime_cursor = min(now, exp.end_at)
    elapsed_days = 0
    if runtime_cursor > exp.start_at:
        elapsed_days = max(1, int((runtime_cursor - exp.start_at).total_seconds() / 86400))
    scheduled_days = 0
    if exp.end_at > exp.start_at:
        scheduled_days = max(1, int((exp.end_at - exp.start_at).total_seconds() / 86400))
    planned_min_runtime_days = None
    if guardrails.get("min_runtime_days") not in (None, "", []):
        try:
            planned_min_runtime_days = max(0, int(guardrails.get("min_runtime_days") or 0))
        except Exception:
            planned_min_runtime_days = None
    runtime_status = "ok"
    if exp.status == "draft":
        runtime_status = "not_started"
    elif planned_min_runtime_days is not None and elapsed_days < planned_min_runtime_days:
        runtime_status = "warn"

    sample_target_status = "ok"
    if planned_treatment_target and treat_n < planned_treatment_target:
        sample_target_status = "warn"
    if planned_control_target and control_n < planned_control_target:
        sample_target_status = "warn"

    # Readiness classification
    reasons: List[str] = []
    if total_n < min_assignments_per_group * 2:
        reasons.append(
            f"Need at least {min_assignments_per_group} assignments per group; currently "
            f"{treat_n} treatment / {control_n} control."
        )
    if min(treat_conv, control_conv) < min_conversions_per_group:
        reasons.append(
            f"Need at least {min_conversions_per_group} conversions per group; currently "
            f"{treat_conv} treatment / {control_conv} control."
        )
    if planned_treatment_target is not None and treat_n < planned_treatment_target:
        reasons.append(
            f"Planned treatment sample target is {planned_treatment_target}; currently {treat_n} assigned."
        )
    if planned_control_target is not None and control_n < planned_control_target:
        reasons.append(
            f"Planned control sample target is {planned_control_target}; currently {control_n} assigned."
        )
    if planned_min_runtime_days is not None and elapsed_days < planned_min_runtime_days:
        reasons.append(
            f"Planned minimum runtime is {planned_min_runtime_days} days; currently {elapsed_days} days elapsed."
        )

    if not reasons:
        ready_label = "ready"
    elif min(treat_conv, control_conv) > 0:
        ready_label = "early"
    else:
        ready_label = "not_ready"

    launch_blockers = list(execution.get("launch_blockers") or [])
    launch_warnings = list(execution.get("launch_warnings") or [])
    if exp.status in {"draft", "running"} and data_completeness["assignments"]["status"] != "ok":
        launch_blockers.append("Assignments must be logged before this experiment can run cleanly.")
    if exp.status in {"running", "completed"} and data_completeness["exposures"]["status"] != "ok":
        launch_warnings.append("Exposure logging is still missing from the recorded experiment data.")
    if exp.status in {"running", "completed"} and data_completeness["outcomes"]["status"] != "ok":
        launch_warnings.append("Outcome logging is still missing from the recorded experiment data.")

    def _dedupe(items: List[str]) -> List[str]:
        seen: set[str] = set()
        result: List[str] = []
        for item in items:
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    launch_blockers = _dedupe(launch_blockers)
    launch_warnings = _dedupe(launch_warnings)
    tracking_gaps = _dedupe(tracking_gaps)
    launch_status = "ready"
    if launch_blockers:
        launch_status = "blocked"
    elif launch_warnings or tracking_gaps:
        launch_status = "manual_review"

    return {
        "experiment_id": experiment_id,
        "execution": execution,
        "sample": {"treatment": treat_n, "control": control_n},
        "exposures": {"treatment": treat_exp, "control": control_exp},
        "outcomes": {"treatment": treat_conv, "control": control_conv},
        "balance": {
            "status": balance_status,
            "expected_share": expected_share,
            "observed_share": observed_share,
        },
        "data_completeness": data_completeness,
        "overlap_risk": {
            "status": overlap_status,
            "overlapping_profiles": overlapping_profiles,
        },
        "plan": {
            "treatment_rate": expected_share,
            "sample_target_total": planned_total_target,
            "sample_target_treatment": planned_treatment_target,
            "sample_target_control": planned_control_target,
            "sample_target_status": sample_target_status,
            "progress_treatment": (
                min(1.0, treat_n / float(planned_treatment_target))
                if planned_treatment_target and planned_treatment_target > 0
                else None
            ),
            "progress_control": (
                min(1.0, control_n / float(planned_control_target))
                if planned_control_target and planned_control_target > 0
                else None
            ),
        },
        "runtime": {
            "status": runtime_status,
            "elapsed_days": elapsed_days,
            "planned_min_days": planned_min_runtime_days,
            "scheduled_days": scheduled_days,
        },
        "launch_readiness": {
            "status": launch_status,
            "blockers": launch_blockers,
            "warnings": launch_warnings,
            "tracking_gaps": tracking_gaps,
        },
        "ready_state": {"label": ready_label, "reasons": reasons},
    }


# ---------------------------------------------------------------------------
# Auto-assignment from conversion paths
# ---------------------------------------------------------------------------


def auto_assign_from_conversion_paths(
    db: Session,
    experiment_id: int,
    channel: str,
    start_date: datetime,
    end_date: datetime,
    treatment_rate: float = 0.5,
) -> Dict[str, int]:
    """
    Automatically assign profiles who had touchpoints in the experiment channel
    during the experiment period.

    Useful for post-hoc analysis of historical data.

    Returns {"treatment": count, "control": count}
    """
    from .models_config_dq import ConversionPath
    from .services_conversions import conversion_path_payload, conversion_path_touchpoints

    # Find all conversion paths with touchpoints in the channel during the period
    paths = (
        db.query(ConversionPath.profile_id, ConversionPath.path_json)
        .filter(ConversionPath.conversion_ts >= start_date)
        .filter(ConversionPath.conversion_ts <= end_date)
        .all()
    )

    eligible_profiles = set()
    for profile_id, path_json in paths:
        path = SimpleNamespace(
            profile_id=profile_id,
            path_json=path_json if isinstance(path_json, dict) else {},
        )
        path_json = conversion_path_payload(path)
        touchpoints = conversion_path_touchpoints(path)
        for tp in touchpoints:
            if tp.get("channel") == channel:
                eligible_profiles.add(path.profile_id)
                break

    return assign_profiles_deterministic(
        db=db,
        experiment_id=experiment_id,
        profile_ids=list(eligible_profiles),
        treatment_rate=treatment_rate,
    )
