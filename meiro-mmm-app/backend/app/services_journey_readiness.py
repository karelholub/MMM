from __future__ import annotations

from typing import Any, Dict, List

from app.services_journeys_health import build_journeys_summary
from app.services_kpi_decisions import build_kpi_overview
from app.services_taxonomy_decisions import build_taxonomy_overview


def _status_rank(status: str) -> int:
    return {"blocked": 3, "warning": 2, "stale": 2, "ready": 1}.get(status, 0)


def _extract_latest_event_replay_context(get_import_runs_fn: Any) -> Dict[str, Any] | None:
    try:
        runs = get_import_runs_fn(status="success", limit=25) or []
    except Exception:
        return None
    for run in runs:
        if not isinstance(run, dict):
            continue
        validation_summary = run.get("validation_summary") or {}
        diagnostics = validation_summary.get("event_reconstruction_diagnostics")
        if isinstance(diagnostics, dict):
            return {
                "run_id": run.get("id"),
                "source": run.get("source"),
                "started_at": run.get("started_at") or run.get("at"),
                "import_note": run.get("import_note"),
                "diagnostics": diagnostics,
            }
    return None


def build_journey_readiness(
    *,
    journeys: List[Dict[str, Any]],
    kpi_config: Any,
    get_import_runs_fn: Any,
    active_settings: Any,
    active_settings_preview: Dict[str, Any],
) -> Dict[str, Any]:
    journeys_summary = build_journeys_summary(
        journeys=journeys,
        kpi_config=kpi_config,
        get_import_runs_fn=get_import_runs_fn,
    )
    taxonomy_overview = build_taxonomy_overview(journeys, suggestion_count=0)
    kpi_overview = build_kpi_overview(journeys, kpi_config, suggestion_count=0)

    validation = journeys_summary.get("validation", {})
    settings_validation = (active_settings.validation_json or {}) if active_settings else {}
    latest_event_replay = _extract_latest_event_replay_context(get_import_runs_fn)
    settings_status = "ready"
    if settings_validation.get("errors"):
        settings_status = "blocked"
    elif settings_validation.get("warnings"):
        settings_status = "warning"

    readiness_status = "ready"
    candidates = [
        taxonomy_overview.get("status", "ready"),
        kpi_overview.get("status", "ready"),
        settings_status,
    ]
    if journeys_summary.get("system_state") in {"error", "empty"}:
        candidates.append("blocked")
    elif journeys_summary.get("system_state") in {"partial", "stale"}:
        candidates.append("warning")
    readiness_status = max(candidates, key=_status_rank)

    blockers: List[str] = []
    warnings: List[str] = []
    if journeys_summary.get("count", 0) == 0:
        blockers.append("No journeys are loaded.")
    if validation.get("error_count", 0) > 0:
        blockers.append(f"{validation.get('error_count', 0)} journey validation errors need attention.")
    if taxonomy_overview.get("status") == "blocked":
        blockers.append("Taxonomy coverage is too weak for reliable journeys analysis.")
    if kpi_overview.get("status") == "blocked":
        blockers.append("KPI configuration is not ready.")

    if journeys_summary.get("data_freshness_hours") is not None and journeys_summary.get("data_freshness_hours", 0) > 24:
        warnings.append(f"Journey data is {journeys_summary['data_freshness_hours']} hours old.")
    if validation.get("warn_count", 0) > 0:
        warnings.append(f"{validation.get('warn_count', 0)} journey validation warnings detected.")
    quality_summary = journeys_summary.get("quality", {})
    if quality_summary.get("low_share") is not None and quality_summary.get("low_share", 0) >= 0.2:
        warnings.append(
            f"Low-quality journey share is {quality_summary['low_share'] * 100:.1f}%."
        )
    if taxonomy_overview.get("summary", {}).get("unknown_share", 0) > 0:
        warnings.append(
            f"Unknown taxonomy share is {taxonomy_overview['summary']['unknown_share'] * 100:.1f}%."
        )
    if kpi_overview.get("summary", {}).get("primary_coverage", 0) < 0.2:
        warnings.append(
            f"Primary KPI coverage is {kpi_overview['summary']['primary_coverage'] * 100:.1f}%."
        )
    replay_diagnostics = (latest_event_replay or {}).get("diagnostics") or {}
    if replay_diagnostics:
        touchpoints_reconstructed = int(replay_diagnostics.get("touchpoints_reconstructed") or 0)
        conversions_reconstructed = int(replay_diagnostics.get("conversions_reconstructed") or 0)
        attributable_profiles = int(replay_diagnostics.get("attributable_profiles") or 0)
        journeys_persisted = int(replay_diagnostics.get("journeys_persisted") or 0)
        if touchpoints_reconstructed == 0:
            warnings.append("Latest raw-event replay reconstructed no touchpoints, so channel and campaign reporting will stay empty.")
        elif conversions_reconstructed == 0:
            warnings.append("Latest raw-event replay reconstructed no conversions, so attribution models have nothing to score.")
        elif attributable_profiles == 0:
            warnings.append("Latest raw-event replay did not produce profiles with both touchpoints and conversions.")
        elif journeys_persisted == 0:
            warnings.append("Latest raw-event replay found attributable profiles, but none survived import into persisted journeys.")
        warnings.extend((replay_diagnostics.get("warnings") or [])[:2])
    warnings.extend(active_settings_preview.get("warnings", [])[:3])

    recommended_actions: List[Dict[str, Any]] = []
    for item in taxonomy_overview.get("recommended_actions", []):
        recommended_actions.append({**item, "domain": "taxonomy"})
    for item in kpi_overview.get("recommended_actions", []):
        recommended_actions.append({**item, "domain": "kpi"})
    if settings_validation.get("errors"):
        recommended_actions.append(
            {
                "id": "fix_settings_validation",
                "label": "Resolve journey settings validation errors",
                "benefit": "Allow safe preview and activation",
                "requires_review": True,
                "domain": "journeys_settings",
                "target_page": "settings",
                "target_section": "journeys",
            }
        )
    elif active_settings_preview.get("warnings"):
        recommended_actions.append(
            {
                "id": "review_settings_preview",
                "label": "Review active journey settings impact warnings",
                "benefit": "Reduce noisy paths and expensive queries",
                "requires_review": True,
                "domain": "journeys_settings",
                "target_page": "settings",
                "target_section": "journeys",
            }
        )

    return {
        "status": readiness_status,
        "summary": {
            "journeys_loaded": journeys_summary.get("count", 0),
            "converted_journeys": journeys_summary.get("converted", 0),
            "primary_kpi_coverage": kpi_overview.get("summary", {}).get("primary_coverage", 0.0),
            "freshness_hours": journeys_summary.get("data_freshness_hours"),
            "taxonomy_unknown_share": taxonomy_overview.get("summary", {}).get("unknown_share", 0.0),
            "journey_validation_errors": validation.get("error_count", 0),
            "journey_validation_warnings": validation.get("warn_count", 0),
            "journey_quality_average": quality_summary.get("average_score"),
            "journey_quality_low_share": quality_summary.get("low_share"),
            "active_settings_version": getattr(active_settings, "version_label", None),
            "latest_event_replay": latest_event_replay,
        },
        "blockers": blockers,
        "warnings": warnings[:8],
        "recommended_actions": recommended_actions[:8],
        "details": {
            "journeys": journeys_summary,
            "taxonomy": taxonomy_overview,
            "kpi": kpi_overview,
            "settings_preview": active_settings_preview,
            "settings_validation": settings_validation,
            "latest_event_replay": latest_event_replay,
        },
    }
