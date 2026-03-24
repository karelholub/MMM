from __future__ import annotations

from typing import Any, Dict, List, Optional


def _confidence(score: float) -> Dict[str, Any]:
    bounded = max(0.0, min(100.0, float(score)))
    band = "high" if bounded >= 80 else "medium" if bounded >= 55 else "low"
    return {"score": round(bounded, 1), "band": band}


def _settings_action(
    action_id: str,
    label: str,
    *,
    benefit: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "benefit": benefit,
        "domain": "measurement_model",
        "target_page": "settings",
        "target_section": "measurement-models",
        "requires_review": True,
    }


def build_model_config_validation_decision(
    *,
    valid: bool,
    errors: List[str],
    warnings: List[str],
    schema_errors: List[str],
    missing_conversions: List[str],
) -> Dict[str, Any]:
    blockers = list(schema_errors) + list(errors)
    reasons: List[str] = []
    recommended_actions: List[Dict[str, Any]] = []
    status = "ready"
    score = 92.0

    if blockers:
        status = "blocked"
        score = 25.0
        reasons.append("Draft validation failed and activation should stay blocked.")
        recommended_actions.append(
            _settings_action(
                "fix_model_config_validation",
                "Fix draft validation issues",
                benefit="Restore a safe path to preview and activation",
            )
        )
    elif warnings:
        status = "warning"
        score = 68.0
        reasons.append("Draft is structurally valid but still carries cautionary warnings.")
        recommended_actions.append(
            _settings_action(
                "review_model_config_warnings",
                "Review draft warnings",
                benefit="Reduce activation risk before promoting the config",
            )
        )
    else:
        reasons.append("Draft passes structural validation checks.")

    if missing_conversions:
        recommended_actions.append(
            _settings_action(
                "review_model_config_conversions",
                "Review KPI conversion mappings",
                benefit="Keep measurement configs aligned with the KPI catalog",
            )
        )
        if status == "ready":
            status = "warning"
            score = 60.0

    return {
        "status": status,
        "confidence": _confidence(score),
        "blockers": blockers,
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": recommended_actions,
    }


def build_model_config_preview_decision(
    *,
    preview_available: bool,
    reason: Optional[str],
    warnings: List[str],
    coverage_warning: bool,
    changed_keys: List[str],
) -> Dict[str, Any]:
    reasons: List[str] = []
    recommended_actions: List[Dict[str, Any]] = []

    if not preview_available:
        reasons.append(reason or "Impact preview is unavailable for this draft.")
        recommended_actions.append(
            _settings_action(
                "restore_model_config_preview",
                "Restore preview availability",
                benefit="Expose projected measurement impact before activation",
            )
        )
        return {
            "status": "blocked",
            "confidence": _confidence(30.0),
            "blockers": [reason or "Impact preview unavailable"],
            "warnings": [],
            "reasons": reasons,
            "recommended_actions": recommended_actions,
        }

    if changed_keys:
        reasons.append(f"Draft changes {len(changed_keys)} top-level configuration sections.")
    else:
        reasons.append("Draft has no material top-level changes versus the active config.")

    if coverage_warning:
        reasons.append("Projected attributable conversions fall materially versus the active config.")
        recommended_actions.append(
            _settings_action(
                "review_model_config_coverage_drop",
                "Review projected conversion drop",
                benefit="Prevent avoidable coverage regressions before activation",
            )
        )
        status = "warning"
        score = 52.0
    elif warnings:
        reasons.append("Preview completed, but cautionary warnings remain.")
        recommended_actions.append(
            _settings_action(
                "review_model_config_preview",
                "Review preview warnings",
                benefit="Confirm the draft behaves as expected before activation",
            )
        )
        status = "warning"
        score = 68.0
    else:
        reasons.append("Preview completed without material risk signals.")
        status = "ready"
        score = 88.0

    return {
        "status": status,
        "confidence": _confidence(score),
        "blockers": [],
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": recommended_actions,
    }


def build_model_config_activation_decision(
    *,
    validation: Optional[Dict[str, Any]],
    preview: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    validation_status = (validation or {}).get("status")
    preview_status = (preview or {}).get("status")
    blockers: List[str] = []
    warnings: List[str] = []
    reasons: List[str] = []
    recommended_actions: List[Dict[str, Any]] = []

    if validation_status == "blocked":
        blockers.extend((validation or {}).get("blockers", []))
        reasons.append("Activation is blocked until validation issues are resolved.")
        recommended_actions.extend((validation or {}).get("recommended_actions", []))
    elif preview_status == "blocked":
        blockers.extend((preview or {}).get("blockers", []))
        reasons.append("Activation is blocked until a usable impact preview is available.")
        recommended_actions.extend((preview or {}).get("recommended_actions", []))
    else:
        if validation_status == "warning":
            warnings.extend((validation or {}).get("warnings", []))
            reasons.append("Validation passed with warnings.")
            recommended_actions.extend((validation or {}).get("recommended_actions", []))
        if preview_status == "warning":
            warnings.extend((preview or {}).get("warnings", []))
            reasons.append("Preview indicates caution before activation.")
            recommended_actions.extend((preview or {}).get("recommended_actions", []))

    if blockers:
        status = "blocked"
        score = 24.0
    elif warnings:
        status = "warning"
        score = 58.0
    else:
        status = "ready"
        score = 90.0
        reasons.append("Validation and preview checks support activation.")

    deduped_actions: List[Dict[str, Any]] = []
    seen_action_ids = set()
    for action in recommended_actions:
        action_id = action.get("id")
        if not action_id or action_id in seen_action_ids:
            continue
        seen_action_ids.add(action_id)
        deduped_actions.append(action)

    return {
        "status": status,
        "confidence": _confidence(score),
        "blockers": blockers,
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": deduped_actions,
    }
