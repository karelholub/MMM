from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from app.modules.settings.schemas import MMMSettings


def _confidence(score: float) -> Dict[str, Any]:
    bounded = max(0.0, min(100.0, float(score)))
    band = "high" if bounded >= 80 else "medium" if bounded >= 55 else "low"
    return {"score": round(bounded, 1), "band": band}


def _action(
    action_id: str,
    label: str,
    *,
    benefit: Optional[str] = None,
    target_page: str = "settings",
    target_section: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "benefit": benefit,
        "domain": "settings",
        "target_page": target_page,
        "target_section": target_section,
        "requires_review": True,
    }


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _extract_touchpoint_dates(journeys: List[Dict[str, Any]]) -> Tuple[Optional[datetime], Optional[datetime], int, int]:
    earliest: Optional[datetime] = None
    latest: Optional[datetime] = None
    week_keys: Set[str] = set()
    month_keys: Set[str] = set()
    for journey in journeys:
        for tp in journey.get("touchpoints", []):
            ts = _parse_timestamp(tp.get("timestamp"))
            if ts is None:
                continue
            if earliest is None or ts < earliest:
                earliest = ts
            if latest is None or ts > latest:
                latest = ts
            iso = ts.isocalendar()
            week_keys.add(f"{iso.year}-W{iso.week:02d}")
            month_keys.add(f"{ts.year}-{ts.month:02d}")
    return earliest, latest, len(week_keys), len(month_keys)


def build_mmm_defaults_preview(
    *,
    journeys: List[Dict[str, Any]],
    settings: MMMSettings,
) -> Dict[str, Any]:
    if not journeys:
        reason = "Preview unavailable (no journeys loaded)"
        return {
            "previewAvailable": False,
            "summary": {
                "journeys_total": 0,
                "touchpoint_span_days": 0,
                "distinct_weeks": 0,
                "distinct_months": 0,
                "frequency": settings.frequency,
            },
            "reason": reason,
            "decision": {
                "status": "blocked",
                "confidence": _confidence(30.0),
                "blockers": [reason],
                "warnings": [],
                "reasons": ["MMM defaults cannot be validated without journeys."],
                "recommended_actions": [
                    _action(
                        "load_mmm_data_for_defaults",
                        "Load journeys before changing MMM defaults",
                        benefit="Validate aggregation defaults on actual journey time coverage",
                        target_page="datasources",
                    )
                ],
            },
        }

    earliest, latest, distinct_weeks, distinct_months = _extract_touchpoint_dates(journeys)
    span_days = max((latest - earliest).days, 0) if earliest and latest else 0
    blockers: List[str] = []
    warnings: List[str] = []
    reasons: List[str] = [f"Preview is based on {len(journeys)} loaded journeys."]
    actions: List[Dict[str, Any]] = []

    if earliest is None or latest is None:
        blockers.append("Journeys do not include usable touchpoint timestamps for MMM aggregation.")
        actions.append(
            _action(
                "fix_journey_timestamps_for_mmm",
                "Ensure touchpoint timestamps are populated",
                benefit="Enable reliable weekly/monthly MMM aggregation",
                target_page="datasources",
            )
        )

    freq = (settings.frequency or "W").upper()
    if freq == "M":
        if distinct_months < 3 and not blockers:
            warnings.append("Monthly aggregation has limited history (<3 distinct months).")
            actions.append(
                _action(
                    "use_weekly_until_more_months",
                    "Consider weekly aggregation until monthly history grows",
                    benefit="Avoid unstable monthly estimates with sparse month coverage",
                    target_section="mmm",
                )
            )
    else:
        if distinct_weeks < 8 and not blockers:
            warnings.append("Weekly aggregation has limited history (<8 distinct weeks).")
            actions.append(
                _action(
                    "use_monthly_until_more_weeks",
                    "Consider monthly aggregation until weekly history grows",
                    benefit="Reduce variance when weekly sample support is low",
                    target_section="mmm",
                )
            )

    status = "ready"
    score = 86.0
    if blockers:
        status = "blocked"
        score = 30.0
    elif warnings:
        status = "warning"
        score = 62.0

    return {
        "previewAvailable": True,
        "summary": {
            "journeys_total": len(journeys),
            "touchpoint_span_days": span_days,
            "distinct_weeks": distinct_weeks,
            "distinct_months": distinct_months,
            "frequency": freq,
        },
        "reason": None,
        "decision": {
            "status": status,
            "confidence": _confidence(score),
            "blockers": blockers,
            "warnings": warnings,
            "reasons": reasons,
            "recommended_actions": actions,
        },
    }
