from __future__ import annotations

from typing import Any, Dict, List, Optional


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


def build_attribution_preview_decision(
    *,
    preview_available: bool,
    reason: Optional[str],
    total_journeys: int,
    window_impact_count: int,
    window_direction: str,
    converted_impact_count: int,
    converted_direction: str,
) -> Dict[str, Any]:
    if not preview_available:
        return {
            "status": "blocked",
            "confidence": _confidence(30.0),
            "blockers": [reason or "Attribution preview unavailable"],
            "warnings": [],
            "reasons": [reason or "There are no journeys available to estimate attribution impact."],
            "recommended_actions": [
                _action(
                    "load_attribution_data",
                    "Load journeys for attribution preview",
                    benefit="Enable preview and safer settings changes",
                    target_page="datasources",
                )
            ],
        }

    warnings: List[str] = []
    reasons = [f"Preview is based on {total_journeys} loaded journeys."]
    actions: List[Dict[str, Any]] = []
    score = 88.0
    status = "ready"

    if window_direction == "tighten" and window_impact_count > 0:
        warnings.append(
            f"{window_impact_count} journeys would fall outside the tighter lookback window."
        )
        actions.append(
            _action(
                "review_attribution_window",
                "Review lookback-window impact",
                benefit="Avoid unintentionally excluding converted journeys",
                target_section="attribution",
            )
        )
        status = "warning"
        score = 64.0
    elif window_direction == "loosen" and window_impact_count > 0:
        reasons.append(
            f"{window_impact_count} journeys would become newly eligible with the wider window."
        )

    if converted_direction != "none" and converted_impact_count > 0:
        warnings.append(
            f"{converted_impact_count} journeys change inclusion when the converted flag setting flips."
        )
        actions.append(
            _action(
                "review_converted_flag",
                "Review converted-flag impact",
                benefit="Keep journey inclusion aligned with reporting intent",
                target_section="attribution",
            )
        )
        status = "warning"
        score = min(score, 62.0)

    if not warnings:
        reasons.append("Proposed attribution defaults do not introduce a material eligibility shift.")

    return {
        "status": status,
        "confidence": _confidence(score),
        "blockers": [],
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": actions,
    }


def build_nba_preview_decision(
    *,
    preview_available: bool,
    reason: Optional[str],
    dataset_journeys: int,
    prefixes_eligible: int,
    total_prefixes: int,
    total_recommendations: int,
    filtered_by_support_pct: float,
    filtered_by_conversion_pct: float,
) -> Dict[str, Any]:
    if not preview_available:
        return {
            "status": "blocked",
            "confidence": _confidence(28.0),
            "blockers": [reason or "NBA preview unavailable"],
            "warnings": [],
            "reasons": [reason or "There are no journeys available to evaluate NBA thresholds."],
            "recommended_actions": [
                _action(
                    "load_nba_data",
                    "Load journeys for NBA preview",
                    benefit="Enable threshold preview and recommendation testing",
                    target_page="datasources",
                )
            ],
        }

    warnings: List[str] = []
    reasons = [f"Preview is based on {dataset_journeys} journeys and {total_prefixes} prefixes."]
    actions: List[Dict[str, Any]] = []
    score = 86.0
    status = "ready"

    if total_prefixes > 0 and prefixes_eligible == 0:
        warnings.append("Current thresholds filter out all prefixes.")
        actions.append(
            _action(
                "loosen_nba_thresholds",
                "Loosen NBA thresholds",
                benefit="Recover recommendation coverage",
                target_section="nba",
            )
        )
        status = "warning"
        score = 40.0
    elif filtered_by_support_pct >= 70 or filtered_by_conversion_pct >= 70:
        warnings.append("A large share of candidate recommendations is being filtered out.")
        actions.append(
            _action(
                "review_nba_thresholds",
                "Review NBA threshold strictness",
                benefit="Balance recommendation quality and coverage",
                target_section="nba",
            )
        )
        status = "warning"
        score = 60.0

    if total_recommendations == 0:
        warnings.append("No recommendations remain under the current defaults.")
        status = "warning"
        score = min(score, 44.0)

    return {
        "status": status,
        "confidence": _confidence(score),
        "blockers": [],
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": actions,
    }


def build_nba_test_decision(
    *,
    preview_available: bool,
    reason: Optional[str],
    total_prefix_support: int,
    recommendations_count: int,
) -> Dict[str, Any]:
    if not preview_available:
        return {
            "status": "blocked",
            "confidence": _confidence(28.0),
            "blockers": [reason or "NBA test unavailable"],
            "warnings": [],
            "reasons": [reason or "The recommendation console cannot evaluate this prefix yet."],
            "recommended_actions": [
                _action(
                    "load_nba_test_data",
                    "Load journeys for recommendation testing",
                    benefit="Enable prefix-level recommendation testing",
                    target_page="datasources",
                )
            ],
        }

    warnings: List[str] = []
    reasons = [f"Console evaluated a prefix with {total_prefix_support} supporting journeys."]
    actions: List[Dict[str, Any]] = []
    status = "ready"
    score = 84.0

    if total_prefix_support == 0:
        warnings.append("No journeys match this prefix under the current dataset.")
        actions.append(
            _action(
                "adjust_nba_test_prefix",
                "Try a broader or different prefix",
                benefit="Find prefixes with enough support for recommendation testing",
                target_section="nba",
            )
        )
        status = "warning"
        score = 42.0
    elif recommendations_count == 0:
        warnings.append("No recommendations meet the current thresholds for this prefix.")
        actions.append(
            _action(
                "loosen_nba_test_thresholds",
                "Loosen NBA thresholds for this prefix",
                benefit="Surface more recommendation candidates for review",
                target_section="nba",
            )
        )
        status = "warning"
        score = 58.0

    return {
        "status": status,
        "confidence": _confidence(score),
        "blockers": [],
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": actions,
    }
