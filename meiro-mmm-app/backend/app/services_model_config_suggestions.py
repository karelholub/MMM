"""Data-backed suggestion helpers for measurement model configs."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from app.services_model_config_decisions import build_model_config_preview_decision


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _parse_ts(value: Any) -> Optional[datetime]:
    if value in (None, "", []):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _percentile(values: Sequence[float], q: float) -> float:
    vals = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    q = max(0.0, min(1.0, q))
    idx = q * (len(vals) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return vals[lo]
    weight = idx - lo
    return vals[lo] * (1.0 - weight) + vals[hi] * weight


def _round_int(value: float, *, low: int, high: int) -> int:
    return max(low, min(high, int(round(value))))


def _resolve_kpi_for_conversion(
    conversion_name: str,
    kpi_by_id: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    needle = _normalize_token(conversion_name)
    if not needle:
        return None
    if needle in kpi_by_id:
        return needle
    for kpi_id, item in kpi_by_id.items():
        event_name = _normalize_token(item.get("event_name"))
        label = _normalize_token(item.get("label"))
        if needle == event_name or needle == label:
            return kpi_id
    for kpi_id, item in kpi_by_id.items():
        event_name = _normalize_token(item.get("event_name"))
        label = _normalize_token(item.get("label"))
        if needle in {event_name, label}:
            return kpi_id
        if needle and ((event_name and needle in event_name) or (label and needle in label)):
            return kpi_id
    return None


def suggest_model_config_from_journeys(
    journeys: List[Dict[str, Any]],
    *,
    kpi_definitions: List[Dict[str, Any]],
    strategy: str = "balanced",
) -> Dict[str, Any]:
    if not journeys:
        return {
            "config_json": None,
            "preview_available": False,
            "reason": "No journeys loaded",
            "reasons": [],
            "warnings": ["Load journeys before requesting a suggested config."],
            "data_summary": {"journeys": 0},
            "confidence": {"score": 0.0, "label": "low"},
            "status": "blocked",
            "recommended_actions": [
                {
                    "id": "load_model_config_data",
                    "label": "Load journeys before requesting a suggestion",
                    "benefit": "Enable data-backed config calibration",
                    "domain": "measurement_model",
                    "target_page": "datasources",
                    "requires_review": True,
                }
            ],
        }

    strategy = (strategy or "balanced").strip().lower()
    if strategy not in {"conservative", "balanced", "coverage_first"}:
        strategy = "balanced"

    kpi_by_id = {
        str(item.get("id")): item
        for item in kpi_definitions
        if isinstance(item, dict) and item.get("id")
    }

    converted = []
    channel_counts: Dict[str, int] = {}
    conversion_counts: Dict[str, int] = {}
    first_touch_to_conv_days: List[float] = []
    last_touch_to_conv_days: List[float] = []
    session_gaps_minutes: List[float] = []
    defaulted_entries = 0
    inferred_journeys = 0
    unresolved_touchpoints = 0
    total_revenue_entries = 0
    total_touchpoints = 0

    for journey in journeys:
        tps = [tp for tp in (journey.get("touchpoints") or []) if isinstance(tp, dict)]
        convs = [cv for cv in (journey.get("conversions") or []) if isinstance(cv, dict)]
        if ((journey.get("meta") or {}).get("parser") or {}).get("used_inferred_mapping"):
            inferred_journeys += 1
        for tp in tps:
            total_touchpoints += 1
            channel = str(tp.get("channel") or "").strip().lower()
            if channel:
                channel_counts[channel] = channel_counts.get(channel, 0) + 1
            if channel in {"unknown", ""}:
                unresolved_touchpoints += 1
        revenue_entries = journey.get("_revenue_entries") or []
        if isinstance(revenue_entries, list):
            for entry in revenue_entries:
                if not isinstance(entry, dict):
                    continue
                total_revenue_entries += 1
                if entry.get("default_applied"):
                    defaulted_entries += 1
        if not convs:
            continue
        converted.append(journey)
        primary_name = str(convs[0].get("name") or journey.get("kpi_type") or "").strip()
        if primary_name:
            conversion_counts[primary_name] = conversion_counts.get(primary_name, 0) + 1
        conv_ts = _parse_ts(convs[0].get("ts"))
        tp_times = [_parse_ts(tp.get("ts") or tp.get("timestamp")) for tp in tps]
        tp_times = [ts for ts in tp_times if ts is not None]
        if conv_ts and tp_times:
            first_touch_to_conv_days.append(max(0.0, (conv_ts - min(tp_times)).total_seconds() / 86400.0))
            last_touch_to_conv_days.append(max(0.0, (conv_ts - max(tp_times)).total_seconds() / 86400.0))
            for prev, nxt in zip(tp_times, tp_times[1:]):
                session_gaps_minutes.append(max(0.0, (nxt - prev).total_seconds() / 60.0))

    top_conversion = sorted(conversion_counts.items(), key=lambda kv: kv[1], reverse=True)
    top_channels = sorted(channel_counts.items(), key=lambda kv: kv[1], reverse=True)

    include_channels = [
        name
        for name, count in top_channels
        if name not in {"direct", "unknown"}
        and (count / float(max(total_touchpoints, 1))) >= (0.03 if strategy == "conservative" else 0.015 if strategy == "balanced" else 0.0)
    ][:8]
    if not include_channels:
        include_channels = [name for name, _count in top_channels if name not in {"direct", "unknown"}][:5]

    exclude_channels = ["direct"]
    if any(name == "unknown" for name, _count in top_channels):
        exclude_channels.append("unknown")

    click_p90 = _percentile(first_touch_to_conv_days, 0.9)
    latency_p90 = _percentile(last_touch_to_conv_days, 0.9)
    gap_p50 = _percentile(session_gaps_minutes, 0.5)

    click_days = _round_int(
        click_p90 if click_p90 > 0 else (21 if strategy == "coverage_first" else 14),
        low=7 if strategy == "conservative" else 3,
        high=60 if strategy == "coverage_first" else 45,
    )
    impression_days = _round_int(
        max(1.0, click_days * (0.25 if strategy == "conservative" else 0.35 if strategy == "balanced" else 0.5)),
        low=1,
        high=min(21, click_days),
    )
    session_timeout = _round_int(
        gap_p50 if gap_p50 > 0 else (30 if strategy != "coverage_first" else 45),
        low=15,
        high=180,
    )
    conversion_latency = _round_int(
        latency_p90 if latency_p90 > 0 else 3,
        low=0,
        high=30,
    )

    conversion_definitions = []
    suggested_keys: List[str] = []
    for observed_name, _count in top_conversion[: min(8, len(top_conversion))]:
        key = _resolve_kpi_for_conversion(observed_name, kpi_by_id)
        if not key or key in suggested_keys:
            continue
        suggested_keys.append(key)
        kpi = kpi_by_id.get(key, {})
        conversion_definitions.append(
            {
                "key": key,
                "name": kpi.get("label") or key.replace("_", " ").title(),
                "event_name": kpi.get("event_name") or key,
                "value_field": kpi.get("value_field"),
                "dedup_mode": "conversion_id",
                "attribution_model_default": "last_touch" if strategy == "conservative" else "linear" if strategy == "coverage_first" else "data_driven",
            }
        )
    if not conversion_definitions:
        fallback_id = next(iter(kpi_by_id.keys()), None)
        if fallback_id:
            kpi = kpi_by_id.get(fallback_id, {})
            conversion_definitions = [
                {
                    "key": fallback_id,
                    "name": kpi.get("label") or fallback_id.replace("_", " ").title(),
                    "event_name": kpi.get("event_name") or fallback_id,
                    "value_field": kpi.get("value_field"),
                    "dedup_mode": "conversion_id",
                    "attribution_model_default": "data_driven",
                }
            ]
            suggested_keys = [fallback_id]

    primary_key = suggested_keys[0] if suggested_keys else (next(iter(kpi_by_id.keys()), "purchase"))

    defaulted_share = (defaulted_entries / float(total_revenue_entries or 1)) * 100.0 if total_revenue_entries else 0.0
    inferred_share = (inferred_journeys / float(len(journeys) or 1)) * 100.0
    unresolved_share = (unresolved_touchpoints / float(total_touchpoints or 1)) * 100.0 if total_touchpoints else 0.0

    reasons = [
        f"{len(converted)} of {len(journeys)} journeys contain conversions and were used for calibration.",
        f"90% of conversions happen within about {click_days} days of first touch.",
        f"90% of conversions happen within about {conversion_latency} days of last touch.",
        f"Median inter-touch gap suggests a session timeout near {session_timeout} minutes.",
    ]
    if include_channels:
        reasons.append(f"Suggested include channels are based on observed touchpoint share: {', '.join(include_channels[:5])}.")

    warnings: List[str] = []
    if unresolved_share > 20:
        warnings.append(f"Unresolved touchpoint share is {unresolved_share:.1f}%; fix taxonomy before trusting aggressive optimization.")
    if defaulted_share > 20:
        warnings.append(f"Defaulted conversion value share is {defaulted_share:.1f}%; revenue-based recommendations may be unstable.")
    if inferred_share > 25:
        warnings.append(f"{inferred_share:.1f}% of journeys still rely on inferred mappings; review Meiro mapping approval and replay status.")
    if len(converted) < 50:
        warnings.append("Low converted journey volume; treat this suggestion as directional rather than final.")

    confidence_score = max(0.0, min(100.0, 100.0 - unresolved_share - defaulted_share - inferred_share * 0.5))
    confidence_label = "high" if confidence_score >= 80 else "medium" if confidence_score >= 55 else "low"

    config_json = {
        "eligible_touchpoints": {
            "include_channels": include_channels,
            "exclude_channels": exclude_channels,
            "include_event_types": [],
            "exclude_event_types": [],
        },
        "windows": {
            "click_lookback_days": click_days,
            "impression_lookback_days": impression_days,
            "session_timeout_minutes": session_timeout,
            "conversion_latency_days": conversion_latency,
        },
        "conversions": {
            "primary_conversion_key": primary_key,
            "conversion_definitions": conversion_definitions,
        },
    }

    preview_decision = build_model_config_preview_decision(
        preview_available=True,
        reason=None,
        warnings=warnings,
        coverage_warning=False,
        changed_keys=list(config_json.keys()),
    )

    return {
        "config_json": config_json,
        "preview_available": True,
        "reason": None,
        "reasons": reasons,
        "warnings": warnings,
        "data_summary": {
            "journeys": len(journeys),
            "converted_journeys": len(converted),
            "touchpoints": total_touchpoints,
            "top_channels": [{"channel": name, "count": count} for name, count in top_channels[:8]],
            "top_conversions": [{"key": name, "count": count} for name, count in top_conversion[:8]],
            "defaulted_conversion_value_pct": round(defaulted_share, 2),
            "inferred_mapping_journey_pct": round(inferred_share, 2),
            "unresolved_touchpoint_pct": round(unresolved_share, 2),
        },
        "confidence": {"score": round(confidence_score, 1), "label": confidence_label},
        "status": "ready" if not warnings else "warning",
        "recommended_actions": preview_decision["recommended_actions"],
    }
