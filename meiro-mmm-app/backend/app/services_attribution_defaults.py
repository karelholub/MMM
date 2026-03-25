from __future__ import annotations

from typing import Any, Dict, Mapping

from app.models_config_dq import ModelConfig as ORMModelConfig
from app.models_config_dq import ModelConfigStatus
from app.services_kpi_decisions import build_kpi_overview
from app.services_model_config import get_default_config_id, validate_model_config
from app.services_settings_decisions import build_attribution_defaults_overview_decision
from app.services_taxonomy_decisions import build_taxonomy_overview
from app.utils.taxonomy import load_taxonomy


def _as_mapping(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        return dict(value.model_dump())
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def build_attribution_defaults_overview(
    *,
    db: Any,
    journeys: list[dict[str, Any]],
    attribution_defaults: Any,
    kpi_config: Any,
) -> Dict[str, Any]:
    kpi_overview = build_kpi_overview(journeys, kpi_config, suggestion_count=0)
    taxonomy_overview = build_taxonomy_overview(
        journeys,
        taxonomy=load_taxonomy(),
        suggestion_count=0,
    )

    default_model_id = get_default_config_id()
    active_model = (
        db.query(ORMModelConfig)
        .filter(ORMModelConfig.status == ModelConfigStatus.ACTIVE)
        .order_by(ORMModelConfig.activated_at.desc(), ORMModelConfig.updated_at.desc())
        .first()
    )
    selected_model = db.get(ORMModelConfig, default_model_id) if default_model_id else None
    if selected_model is None:
        selected_model = active_model

    model_status = "blocked"
    model_validation_ok = False
    model_validation_message = "No active model config is available."
    selected_model_status = str(getattr(selected_model, "status", "") or "").lower() if selected_model else None
    if selected_model:
        model_validation_ok, model_validation_message = validate_model_config(selected_model.config_json or {})
        if selected_model_status != ModelConfigStatus.ACTIVE:
            model_status = "warning"
            model_validation_ok = False
            model_validation_message = "Selected default model config is not active."
        elif model_validation_ok:
            model_status = "ready"
        else:
            model_status = "warning"

    taxonomy_summary = taxonomy_overview.get("summary", {})
    kpi_summary = kpi_overview.get("summary", {})

    decision = build_attribution_defaults_overview_decision(
        kpi_status=str(kpi_overview.get("status") or "blocked"),
        taxonomy_status=str(taxonomy_overview.get("status") or "blocked"),
        model_ready=model_status == "ready",
        model_validation_ok=model_validation_ok,
        journeys_loaded=len(journeys),
        taxonomy_unknown_share=float(taxonomy_summary.get("unknown_share") or 0.0),
        kpi_primary_coverage=float(kpi_summary.get("primary_coverage") or 0.0),
    )

    dependencies = {
        "kpi": {
            "status": kpi_overview.get("status"),
            "primary_kpi_id": kpi_summary.get("primary_kpi_id"),
            "primary_kpi_label": kpi_summary.get("primary_kpi_label"),
            "primary_coverage": kpi_summary.get("primary_coverage"),
            "definitions_count": kpi_summary.get("definitions_count"),
        },
        "taxonomy": {
            "status": taxonomy_overview.get("status"),
            "unknown_share": taxonomy_summary.get("unknown_share"),
            "low_confidence_share": taxonomy_summary.get("low_confidence_share"),
            "active_rules": taxonomy_summary.get("active_rules"),
        },
        "measurement_model": {
            "status": model_status,
            "id": getattr(selected_model, "id", None),
            "name": getattr(selected_model, "name", None),
            "version": getattr(selected_model, "version", None),
            "model_status": selected_model_status,
            "is_default_selected": bool(default_model_id and selected_model and selected_model.id == default_model_id),
            "validation_ok": model_validation_ok,
            "validation_message": model_validation_message,
        },
    }

    resolved_inputs = {
        "journeys_loaded": len(journeys),
        "attribution_defaults": _as_mapping(attribution_defaults),
        "primary_kpi_id": kpi_summary.get("primary_kpi_id"),
        "primary_kpi_label": kpi_summary.get("primary_kpi_label"),
        "primary_kpi_coverage": kpi_summary.get("primary_coverage"),
        "taxonomy_unknown_share": taxonomy_summary.get("unknown_share"),
        "taxonomy_low_confidence_share": taxonomy_summary.get("low_confidence_share"),
        "measurement_model_id": getattr(selected_model, "id", None),
        "measurement_model_name": getattr(selected_model, "name", None),
        "measurement_model_version": getattr(selected_model, "version", None),
        "measurement_model_status": selected_model_status,
    }

    return {
        "dependencies": dependencies,
        "resolved_inputs": resolved_inputs,
        "decision": decision,
    }
