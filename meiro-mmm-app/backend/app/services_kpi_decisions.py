from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.modules.settings.schemas import KpiConfigModel, KpiDefinitionModel
from app.models_config_dq import ConversionKpiSignalFact, ConversionPath


def _confidence_band(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def _definition_match_stats(
    journeys: List[Dict[str, Any]],
    definition: KpiDefinitionModel,
) -> Dict[str, Any]:
    target_event = (definition.event_name or definition.id or "").strip().lower()
    matched_events = 0
    journeys_matched = 0
    missing_value_checks = 0
    missing_value_count = 0
    fallback_used = False

    def _record_value(source: Dict[str, Any]) -> None:
        nonlocal missing_value_checks, missing_value_count
        if definition.value_field:
            missing_value_checks += 1
            value = source.get(definition.value_field)
            if value in (None, "", []):
                missing_value_count += 1

    for journey in journeys:
        journey_matches = False
        events = journey.get("events") or []
        if target_event:
            for event in events:
                name = str(event.get("name") or event.get("event_name") or "").strip().lower()
                if name and name == target_event:
                    matched_events += 1
                    journey_matches = True
                    _record_value(event)
        if not journey_matches:
            journey_type = str(journey.get("kpi_type") or "").strip().lower()
            if definition.id and journey_type == definition.id.strip().lower():
                matched_events += 1
                journey_matches = True
                _record_value(journey)
                fallback_used = True
            elif not target_event and definition.event_name == "" and journey.get("converted", False):
                matched_events += 1
                journey_matches = True
                _record_value(journey)
                fallback_used = True

        if journey_matches:
            journeys_matched += 1

    return {
        "definition_id": definition.id,
        "journeys_matched": journeys_matched,
        "events_matched": matched_events,
        "missing_value_checks": missing_value_checks,
        "missing_value_count": missing_value_count,
        "missing_value_pct": (missing_value_count / missing_value_checks) if missing_value_checks else None,
        "fallback_used": fallback_used,
    }


def _build_definition_stats(
    journeys: List[Dict[str, Any]],
    cfg: KpiConfigModel,
) -> List[Dict[str, Any]]:
    return [
        {
            **_definition_match_stats(journeys, definition),
            "label": definition.label,
            "type": definition.type,
            "event_name": definition.event_name,
            "value_field": definition.value_field,
            "weight": definition.weight,
        }
        for definition in cfg.definitions
    ]


def _recommended_actions(
    *,
    cfg: KpiConfigModel,
    total_journeys: int,
    primary_coverage: float,
    missing_primary: bool,
    suggestion_count: int,
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    if missing_primary:
        actions.append(
            {
                "id": "set_primary_kpi",
                "label": "Select a primary KPI",
                "benefit": "Unlock consistent attribution defaults and KPI summaries",
                "requires_review": True,
                "domain": "kpi",
                "target_page": "settings",
                "target_section": "kpi",
                "target_tab": "advanced",
            }
        )
    if suggestion_count > 0:
        actions.append(
            {
                "id": "review_kpi_suggestions",
                "label": f"Review {suggestion_count} KPI suggestions",
                "benefit": "Improve KPI coverage without editing raw JSON",
                "requires_review": True,
                "domain": "kpi",
                "target_page": "settings",
                "target_section": "kpi",
                "target_tab": "suggestions",
            }
        )
    if total_journeys > 0 and primary_coverage < 0.2 and cfg.primary_kpi_id:
        actions.append(
            {
                "id": "review_primary_coverage",
                "label": "Review primary KPI coverage",
                "benefit": "Raise trust in attribution and reporting outputs",
                "requires_review": True,
                "domain": "kpi",
                "target_page": "settings",
                "target_section": "kpi",
                "target_tab": "advanced",
            }
        )
    return actions


def build_kpi_overview(
    journeys: List[Dict[str, Any]],
    cfg: KpiConfigModel,
    *,
    suggestion_count: int = 0,
) -> Dict[str, Any]:
    total_journeys = len(journeys)
    definition_stats = _build_definition_stats(journeys, cfg)
    primary_stat = next((item for item in definition_stats if item["definition_id"] == cfg.primary_kpi_id), None)
    primary_definition = next((definition for definition in cfg.definitions if definition.id == cfg.primary_kpi_id), None)
    journeys_with_any_kpi = sum(1 for journey in journeys if journey.get("kpi_type"))
    journeys_with_primary = int(primary_stat["journeys_matched"]) if primary_stat else 0
    primary_coverage = (journeys_with_primary / total_journeys) if total_journeys else 0.0
    missing_primary = not cfg.primary_kpi_id or primary_definition is None
    confidence_score = max(
        0.0,
        min(
            1.0,
            1.0
            - (0.45 if missing_primary else 0.0)
            - (0.25 if not cfg.definitions else 0.0)
            - max(0.0, 0.25 - primary_coverage) * 1.6,
        ),
    )
    status = "ready"
    if missing_primary or not cfg.definitions:
        status = "blocked"
    elif primary_coverage < 0.2:
        status = "warning"

    warnings: List[str] = []
    if missing_primary:
        warnings.append("No valid primary KPI is configured.")
    if primary_stat and primary_stat["missing_value_pct"] is not None and primary_stat["missing_value_pct"] >= 0.2:
        warnings.append("Primary KPI value field is often missing; revenue-based views may be unstable.")

    return {
        "status": status,
        "confidence": {"score": round(confidence_score, 3), "band": _confidence_band(confidence_score)},
        "summary": {
            "definitions_count": len(cfg.definitions),
            "primary_kpi_id": cfg.primary_kpi_id,
            "primary_kpi_label": primary_definition.label if primary_definition else None,
            "journeys_total": total_journeys,
            "journeys_with_any_kpi": journeys_with_any_kpi,
            "journeys_with_primary_kpi": journeys_with_primary,
            "primary_coverage": primary_coverage,
        },
        "definition_stats": definition_stats,
        "warnings": warnings,
        "recommended_actions": _recommended_actions(
            cfg=cfg,
            total_journeys=total_journeys,
            primary_coverage=primary_coverage,
            missing_primary=missing_primary,
            suggestion_count=suggestion_count,
        ),
    }


def build_kpi_suggestions(
    journeys: List[Dict[str, Any]],
    cfg: KpiConfigModel,
    *,
    limit: int = 8,
) -> Dict[str, Any]:
    configured_ids = {definition.id.strip().lower() for definition in cfg.definitions}
    configured_events = {definition.event_name.strip().lower() for definition in cfg.definitions if definition.event_name}
    event_counts: Counter[str] = Counter()
    kpi_type_counts: Counter[str] = Counter()
    conversion_name_counts: Counter[str] = Counter()
    generic_conversion_fallback_count = 0

    for journey in journeys:
        kpi_type = str(journey.get("kpi_type") or "").strip().lower()
        if kpi_type:
            kpi_type_counts[kpi_type] += 1
        for event in journey.get("events") or []:
            name = str(event.get("name") or event.get("event_name") or "").strip().lower()
            if name:
                event_counts[name] += 1
        for conversion in journey.get("conversions") or []:
            name = str(
                conversion.get("name")
                or conversion.get("event_name")
                or conversion.get("conversion_key")
                or ""
            ).strip().lower()
            if name:
                conversion_name_counts[name] += 1
        if kpi_type == "conversion":
            generic_conversion_fallback_count += 1

    suggestions: List[Dict[str, Any]] = []

    if cfg.definitions and (not cfg.primary_kpi_id or cfg.primary_kpi_id.strip().lower() not in configured_ids):
        best = max(
            cfg.definitions,
            key=lambda definition: kpi_type_counts.get(definition.id.strip().lower(), 0),
            default=None,
        )
        if best is not None:
            suggestions.append(
                {
                    "id": f"set_primary:{best.id}",
                    "type": "set_primary",
                    "title": f"Set '{best.label}' as the primary KPI",
                    "description": "A primary KPI is missing or inconsistent with the configured definitions.",
                    "confidence": {"score": 0.92, "band": "high"},
                    "impact_count": int(kpi_type_counts.get(best.id.strip().lower(), 0)),
                    "reasons": ["missing_primary_kpi", "existing_definition_available"],
                    "recommended_action": "Promote this KPI to primary before trusting KPI-driven summaries.",
                    "payload": {"primary_kpi_id": best.id},
                }
            )

    observed_definition_candidates: Counter[str] = Counter()
    observed_definition_candidates.update(event_counts)
    observed_definition_candidates.update(conversion_name_counts)
    for kpi_name, count in kpi_type_counts.items():
        observed_definition_candidates[kpi_name] += count

    for event_name, count in observed_definition_candidates.most_common(limit * 3):
        if event_name in configured_events or event_name in configured_ids:
            continue
        suggestion_id = f"definition:{event_name}"
        suggestions.append(
            {
                "id": suggestion_id,
                "type": "add_definition",
                "title": f"Add KPI definition for event '{event_name}'",
                "description": "This event appears in imported journeys and is not yet part of the KPI config.",
                "confidence": {"score": 0.72, "band": "medium"},
                "impact_count": int(count),
                "reasons": ["observed_event_frequency", "not_configured"],
                "recommended_action": "Add it as a KPI definition, then test coverage before saving.",
                "payload": {
                    "definition": {
                        "id": event_name,
                        "label": event_name.replace("_", " ").title(),
                        "type": "micro",
                        "event_name": event_name,
                        "value_field": None,
                        "weight": 0.5,
                        "lookback_days": 14,
                    }
                },
            }
        )
        if len(suggestions) >= limit:
            break

    if (
        generic_conversion_fallback_count > 0
        and len(suggestions) < limit
    ):
        suggestions.append(
            {
                "id": "review_generic_conversion_mapping",
                "type": "review_mapping",
                "title": "Review generic 'conversion' KPI mapping",
                "description": "Some journeys still rely on the generic fallback conversion key instead of a specific KPI event name.",
                "confidence": {"score": 0.68, "band": "medium"},
                "impact_count": int(generic_conversion_fallback_count),
                "reasons": ["generic_conversion_fallback", "raw_event_mapping_gap"],
                "recommended_action": "Tighten conversion event naming or aliases so these journeys resolve to a specific KPI like purchase or form_submit.",
                "payload": {"focus_conversion_key": "conversion"},
            }
        )

    if cfg.definitions:
        best_existing = max(
            _build_definition_stats(journeys, cfg),
            key=lambda item: item["journeys_matched"],
            default=None,
        )
        if best_existing and cfg.primary_kpi_id and best_existing["definition_id"] != cfg.primary_kpi_id:
            current_primary = next((item for item in _build_definition_stats(journeys, cfg) if item["definition_id"] == cfg.primary_kpi_id), None)
            if current_primary and best_existing["journeys_matched"] > current_primary["journeys_matched"] * 2:
                suggestions.append(
                    {
                        "id": f"swap_primary:{best_existing['definition_id']}",
                        "type": "set_primary",
                        "title": f"Review '{best_existing['label']}' as the primary KPI",
                        "description": "Another configured KPI is observed much more often than the current primary KPI.",
                        "confidence": {"score": 0.66, "band": "medium"},
                        "impact_count": int(best_existing["journeys_matched"]),
                        "reasons": ["coverage_gap", "existing_definition_outperforms_primary"],
                        "recommended_action": "Preview the downstream impact before changing the primary KPI.",
                        "payload": {"primary_kpi_id": best_existing["definition_id"]},
                    }
                )

    return {"suggestions": suggestions[:limit]}


def build_kpi_suggestions_from_db(
    db: Session,
    cfg: KpiConfigModel,
    *,
    limit: int = 8,
    sample_limit: int = 50000,
) -> Optional[Dict[str, Any]]:
    rows = (
        db.query(ConversionKpiSignalFact)
        .order_by(ConversionKpiSignalFact.conversion_ts.desc())
        .limit(sample_limit)
        .all()
    )
    if not rows:
        return None
    raw_count = (
        db.query(ConversionPath.id)
        .order_by(ConversionPath.conversion_ts.desc())
        .limit(sample_limit)
        .count()
    )
    if raw_count > len(rows):
        return None

    journeys: List[Dict[str, Any]] = []
    for row in rows:
        journeys.append(
            {
                "kpi_type": row.kpi_type,
                "events": [{"name": name} for name in (row.event_names_json or []) if isinstance(name, str)],
                "conversions": [{"name": name} for name in (row.conversion_names_json or []) if isinstance(name, str)],
            }
        )
    return build_kpi_suggestions(journeys, cfg, limit=limit)


def build_kpi_overview_from_db(
    db: Session,
    cfg: KpiConfigModel,
    *,
    suggestion_count: int = 0,
    sample_limit: int = 50000,
) -> Optional[Dict[str, Any]]:
    rows = (
        db.query(ConversionKpiSignalFact)
        .order_by(ConversionKpiSignalFact.conversion_ts.desc())
        .limit(sample_limit)
        .all()
    )
    if not rows:
        return None
    raw_count = (
        db.query(ConversionPath.id)
        .order_by(ConversionPath.conversion_ts.desc())
        .limit(sample_limit)
        .count()
    )
    if raw_count > len(rows):
        return None

    journeys: List[Dict[str, Any]] = []
    for row in rows:
        journeys.append(
            {
                "kpi_type": row.kpi_type,
                "events": [{"name": name} for name in (row.event_names_json or []) if isinstance(name, str)],
                "conversions": [{"name": name} for name in (row.conversion_names_json or []) if isinstance(name, str)],
                "converted": bool(row.kpi_type),
            }
        )
    return build_kpi_overview(journeys, cfg, suggestion_count=suggestion_count)


def build_kpi_preview(
    journeys: List[Dict[str, Any]],
    *,
    current_cfg: KpiConfigModel,
    draft_cfg: KpiConfigModel,
) -> Dict[str, Any]:
    before = build_kpi_overview(journeys, current_cfg, suggestion_count=0)
    after = build_kpi_overview(journeys, draft_cfg, suggestion_count=0)
    before_summary = before["summary"]
    after_summary = after["summary"]
    current_primary = before_summary.get("primary_kpi_id")
    draft_primary = after_summary.get("primary_kpi_id")

    warnings: List[str] = []
    if current_primary != draft_primary:
        warnings.append("Changing the primary KPI will affect attribution defaults and KPI-led summaries.")
    if after["status"] == "blocked":
        warnings.append("Draft KPI config is not ready for activation.")

    return {
        "before": before_summary,
        "after": after_summary,
        "delta": {
            "definitions_count": int(after_summary["definitions_count"]) - int(before_summary["definitions_count"]),
            "journeys_with_primary_kpi": int(after_summary["journeys_with_primary_kpi"]) - int(before_summary["journeys_with_primary_kpi"]),
            "journeys_with_any_kpi": int(after_summary["journeys_with_any_kpi"]) - int(before_summary["journeys_with_any_kpi"]),
            "primary_coverage": round(float(after_summary["primary_coverage"]) - float(before_summary["primary_coverage"]), 6),
        },
        "primary_change": {
            "before": current_primary,
            "after": draft_primary,
        },
        "warnings": warnings,
        "recommended_actions": after.get("recommended_actions", []),
    }
