from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from app.modules.config_management.schemas import (
    JourneySettingsActivatePayload,
    JourneySettingsPreviewPayload,
    JourneySettingsValidatePayload,
    JourneySettingsVersionCreatePayload,
    JourneySettingsVersionUpdatePayload,
    ModelConfigActivatePayload,
    ModelConfigPayload,
    ModelConfigPreviewPayload,
    ModelConfigSuggestPayload,
    ModelConfigUpdatePayload,
    ModelConfigValidatePayload,
)
from app.services_model_config_decisions import (
    build_model_config_activation_decision,
    build_model_config_preview_decision,
    build_model_config_validation_decision,
)


def _compute_journey_metrics(journeys: List[Dict[str, Any]]) -> Dict[str, Any]:
    touchpoints = sum(len(j.get("touchpoints") or []) for j in journeys)
    conversions = sum(1 for j in journeys if j.get("converted", True))
    return {"journeys": len(journeys), "touchpoints": touchpoints, "conversions": conversions}


def _changed_top_level_keys(old_cfg: Optional[Dict[str, Any]], new_cfg: Dict[str, Any]) -> List[str]:
    old_map = old_cfg if isinstance(old_cfg, dict) else {}
    keys = set(old_map.keys()) | set(new_cfg.keys())
    changed: List[str] = []
    for key in sorted(keys):
        if old_map.get(key) != new_cfg.get(key):
            changed.append(key)
    return changed


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    model_config_cls: Any,
    model_config_audit_cls: Any,
    model_config_status_obj: Any,
    journey_settings_version_cls: Any,
    create_draft_config_fn: Callable[..., Any],
    clone_config_fn: Callable[..., Any],
    update_draft_config_fn: Callable[..., Any],
    activate_config_fn: Callable[..., Any],
    archive_config_fn: Callable[..., Any],
    validate_model_config_fn: Callable[[Dict[str, Any]], tuple[bool, str]],
    get_kpi_config_fn: Callable[[], Any],
    get_import_runs_fn: Callable[..., List[Dict[str, Any]]],
    get_journeys_fn: Callable[[Any], List[Dict[str, Any]]],
    apply_model_config_fn: Callable[..., List[Dict[str, Any]]],
    suggest_model_config_from_journeys_fn: Callable[..., Dict[str, Any]],
    list_journey_settings_versions_fn: Callable[..., List[Any]],
    create_journey_settings_draft_fn: Callable[..., Any],
    get_journey_settings_version_fn: Callable[..., Any],
    update_journey_settings_draft_fn: Callable[..., Any],
    archive_journey_settings_version_fn: Callable[..., Any],
    ensure_active_journey_settings_fn: Callable[..., Any],
    validate_journey_settings_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    build_journey_settings_impact_preview_fn: Callable[..., Dict[str, Any]],
    build_journey_settings_context_fn: Callable[..., Dict[str, Any]],
    activate_journey_settings_version_fn: Callable[..., Any],
) -> APIRouter:
    router = APIRouter(tags=["config_management"])

    def _serialize_journey_settings_version(item: Any) -> Dict[str, Any]:
        return {
            "id": item.id,
            "status": item.status,
            "version_label": item.version_label,
            "description": item.description,
            "created_at": item.created_at,
            "updated_at": item.updated_at,
            "created_by": item.created_by,
            "activated_at": item.activated_at,
            "activated_by": item.activated_by,
            "settings_json": item.settings_json,
            "validation_json": item.validation_json,
            "diff_json": item.diff_json,
        }

    @router.get("/api/model-configs")
    def list_model_configs(db=Depends(get_db_dependency)):
        rows = db.query(model_config_cls).order_by(model_config_cls.name.asc(), model_config_cls.version.desc()).all()
        return [
            {
                "id": r.id,
                "name": r.name,
                "status": r.status,
                "version": r.version,
                "parent_id": r.parent_id,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
                "created_by": r.created_by,
                "change_note": r.change_note,
                "activated_at": r.activated_at,
            }
            for r in rows
        ]

    @router.post("/api/model-configs")
    def create_model_config(payload: ModelConfigPayload, db=Depends(get_db_dependency)):
        cfg = create_draft_config_fn(
            db=db,
            name=payload.name,
            config_json=payload.config_json,
            created_by=payload.created_by,
            change_note=payload.change_note,
        )
        return {"id": cfg.id, "status": cfg.status, "version": cfg.version}

    @router.post("/api/model-configs/{cfg_id}/clone")
    def clone_model_config(cfg_id: str, actor: str = "system", db=Depends(get_db_dependency)):
        try:
            cfg = clone_config_fn(db=db, source_id=cfg_id, actor=actor)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"id": cfg.id, "status": cfg.status, "version": cfg.version, "parent_id": cfg.parent_id}

    @router.patch("/api/model-configs/{cfg_id}")
    def edit_model_config(cfg_id: str, payload: ModelConfigUpdatePayload, db=Depends(get_db_dependency)):
        try:
            cfg = update_draft_config_fn(
                db=db,
                cfg_id=cfg_id,
                new_config_json=payload.config_json,
                actor=payload.actor,
                change_note=payload.change_note,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"id": cfg.id, "status": cfg.status, "version": cfg.version}

    @router.post("/api/model-configs/{cfg_id}/activate")
    def activate_model_config(cfg_id: str, payload: ModelConfigActivatePayload, db=Depends(get_db_dependency)):
        validation_payload = validate_model_config_route(cfg_id, ModelConfigValidatePayload(), db)
        preview_payload = preview_model_config(cfg_id, ModelConfigPreviewPayload(), db)
        activation_decision = build_model_config_activation_decision(
            validation=validation_payload.get("decision"),
            preview=preview_payload.get("decision"),
        )
        try:
            cfg = activate_config_fn(
                db=db,
                cfg_id=cfg_id,
                actor=payload.actor,
                set_as_default=payload.set_as_default,
                activation_note=payload.activation_note,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {
            "id": cfg.id,
            "status": cfg.status,
            "version": cfg.version,
            "activated_at": cfg.activated_at,
            "decision": activation_decision,
        }

    @router.post("/api/model-configs/{cfg_id}/archive")
    def archive_model_config(cfg_id: str, actor: str = "system", db=Depends(get_db_dependency)):
        try:
            cfg = archive_config_fn(db=db, cfg_id=cfg_id, actor=actor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"id": cfg.id, "status": cfg.status, "version": cfg.version}

    @router.post("/api/model-configs/{cfg_id}/validate")
    def validate_model_config_route(cfg_id: str, payload: ModelConfigValidatePayload, db=Depends(get_db_dependency)):
        cfg = db.get(model_config_cls, cfg_id)
        if not cfg:
            raise HTTPException(status_code=404, detail="Config not found")

        cfg_json: Any = payload.config_json or cfg.config_json or {}
        schema_errors: List[str] = []
        errors: List[str] = []
        warnings: List[str] = []
        missing_conversions: List[str] = []

        if not isinstance(cfg_json, dict):
            schema_errors.append("Config JSON must be a top-level object.")
        else:
            ok, msg = validate_model_config_fn(cfg_json)
            if not ok:
                errors.append(msg)
            conv_section = cfg_json.get("conversions") or {}
            conversion_defs = conv_section.get("conversion_definitions") or []
            available_keys = {d.id for d in get_kpi_config_fn().definitions}
            missing_conversions = sorted({str(defn.get("key")) for defn in conversion_defs if defn.get("key") and defn.get("key") not in available_keys})
            if missing_conversions:
                errors.append("Conversion definitions reference unknown KPI keys: " + ", ".join(missing_conversions))
            for section in ["eligible_touchpoints", "windows", "conversions"]:
                if section not in cfg_json:
                    errors.append(f"Missing top-level section '{section}'")
            touchpoints = cfg_json.get("eligible_touchpoints") or {}
            for field in ["include_channels", "exclude_channels", "include_event_types", "exclude_event_types"]:
                value = touchpoints.get(field)
                if value is not None and not isinstance(value, list):
                    errors.append(f"'{field}' must be an array of strings")

        valid = not errors and not schema_errors
        decision = build_model_config_validation_decision(
            valid=valid,
            errors=errors,
            warnings=warnings,
            schema_errors=schema_errors,
            missing_conversions=missing_conversions,
        )
        return {
            "valid": valid,
            "errors": errors,
            "warnings": warnings,
            "missing_conversions": missing_conversions,
            "schema_errors": schema_errors,
            "decision": decision,
        }

    @router.post("/api/model-configs/{cfg_id}/preview")
    def preview_model_config(cfg_id: str, payload: ModelConfigPreviewPayload, db=Depends(get_db_dependency)):
        cfg = db.get(model_config_cls, cfg_id)
        if not cfg:
            raise HTTPException(status_code=404, detail="Config not found")
        cfg_json: Any = payload.config_json or cfg.config_json or {}
        if not isinstance(cfg_json, dict):
            reason = "Config JSON must be an object"
            return {
                "preview_available": False,
                "reason": reason,
                "decision": build_model_config_preview_decision(
                    preview_available=False,
                    reason=reason,
                    warnings=[],
                    coverage_warning=False,
                    changed_keys=[],
                ),
            }
        journeys = get_journeys_fn(db)
        if not journeys:
            reason = "No journeys loaded"
            return {
                "preview_available": False,
                "reason": reason,
                "decision": build_model_config_preview_decision(
                    preview_available=False,
                    reason=reason,
                    warnings=[],
                    coverage_warning=False,
                    changed_keys=[],
                ),
            }

        baseline_cfg = (
            db.query(model_config_cls)
            .filter(model_config_cls.name == cfg.name, model_config_cls.status == model_config_status_obj.ACTIVE)
            .order_by(model_config_cls.version.desc())
            .first()
        )
        baseline_json = baseline_cfg.config_json if baseline_cfg else {}
        baseline_journeys = apply_model_config_fn(journeys, baseline_json or {})
        draft_journeys = apply_model_config_fn(journeys, cfg_json)
        baseline_metrics = _compute_journey_metrics(baseline_journeys)
        draft_metrics = _compute_journey_metrics(draft_journeys)
        deltas = {key: float(draft_metrics.get(key, 0) - baseline_metrics.get(key, 0)) for key in draft_metrics.keys()}
        deltas_pct: Dict[str, Optional[float]] = {}
        for key, delta in deltas.items():
            baseline_value = baseline_metrics.get(key, 0)
            deltas_pct[key] = round((delta / baseline_value) * 100.0, 2) if baseline_value else None
        warnings: List[str] = []
        coverage_warning = False
        baseline_conversions = baseline_metrics.get("conversions", 0)
        draft_conversions = draft_metrics.get("conversions", 0)
        if baseline_conversions and draft_conversions / baseline_conversions < 0.9:
            coverage_warning = True
            warnings.append("Projected attributable conversions decrease by more than 10% versus the active config.")
        changed_keys = _changed_top_level_keys(baseline_json or {}, cfg_json)
        decision = build_model_config_preview_decision(
            preview_available=True,
            reason=None,
            warnings=warnings,
            coverage_warning=coverage_warning,
            changed_keys=changed_keys,
        )
        return {
            "preview_available": True,
            "baseline": baseline_metrics,
            "draft": draft_metrics,
            "deltas": deltas,
            "deltas_pct": deltas_pct,
            "warnings": warnings,
            "coverage_warning": coverage_warning,
            "changed_keys": changed_keys,
            "active_config_id": baseline_cfg.id if baseline_cfg else None,
            "active_version": baseline_cfg.version if baseline_cfg else None,
            "reason": None,
            "decision": decision,
        }

    @router.post("/api/model-configs/{cfg_id}/suggest")
    def suggest_model_config_route(cfg_id: str, payload: ModelConfigSuggestPayload, db=Depends(get_db_dependency)):
        cfg = db.get(model_config_cls, cfg_id)
        if not cfg:
            raise HTTPException(status_code=404, detail="Config not found")
        journeys = get_journeys_fn(db)
        return suggest_model_config_from_journeys_fn(
            journeys,
            kpi_definitions=[d.__dict__ for d in get_kpi_config_fn().definitions],
            strategy=payload.strategy,
        )

    @router.get("/api/model-configs/{cfg_id}")
    def get_model_config_detail(cfg_id: str, db=Depends(get_db_dependency)):
        cfg = db.get(model_config_cls, cfg_id)
        if not cfg:
            raise HTTPException(status_code=404, detail="Config not found")
        return {
            "id": cfg.id,
            "name": cfg.name,
            "status": cfg.status,
            "version": cfg.version,
            "parent_id": cfg.parent_id,
            "created_at": cfg.created_at,
            "updated_at": cfg.updated_at,
            "created_by": cfg.created_by,
            "change_note": cfg.change_note,
            "activated_at": cfg.activated_at,
            "config_json": cfg.config_json,
        }

    @router.get("/api/model-configs/{cfg_id}/audit")
    def get_model_config_audit(cfg_id: str, db=Depends(get_db_dependency)):
        cfg = db.get(model_config_cls, cfg_id)
        if not cfg:
            raise HTTPException(status_code=404, detail="Config not found")
        audits = (
            db.query(model_config_audit_cls)
            .filter(model_config_audit_cls.model_config_id == cfg_id)
            .order_by(model_config_audit_cls.created_at.desc())
            .all()
        )
        return [{"id": a.id, "actor": a.actor, "action": a.action, "diff_json": a.diff_json, "created_at": a.created_at} for a in audits]

    @router.get("/api/settings/journeys/versions")
    def list_journeys_settings_versions_route(
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        rows = list_journey_settings_versions_fn(db)
        return [_serialize_journey_settings_version(r) for r in rows]

    @router.post("/api/settings/journeys/versions")
    def create_journeys_settings_version_route(
        payload: JourneySettingsVersionCreatePayload,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        item = create_journey_settings_draft_fn(
            db,
            created_by=payload.created_by,
            version_label=payload.version_label,
            description=payload.description,
            settings_json=payload.settings_json,
        )
        return _serialize_journey_settings_version(item)

    @router.get("/api/settings/journeys/versions/{version_id}")
    def get_journeys_settings_version_route(
        version_id: str,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        item = get_journey_settings_version_fn(db, version_id)
        if not item:
            raise HTTPException(status_code=404, detail="Journey settings version not found")
        return _serialize_journey_settings_version(item)

    @router.patch("/api/settings/journeys/versions/{version_id}")
    def update_journeys_settings_version_route(
        version_id: str,
        payload: JourneySettingsVersionUpdatePayload,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        try:
            item = update_journey_settings_draft_fn(
                db,
                version_id=version_id,
                actor=payload.actor,
                settings_json=payload.settings_json,
                description=payload.description,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return _serialize_journey_settings_version(item)

    @router.post("/api/settings/journeys/versions/{version_id}/archive")
    def archive_journeys_settings_version_route(
        version_id: str,
        actor: str = "system",
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        try:
            item = archive_journey_settings_version_fn(db, version_id=version_id, actor=actor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return _serialize_journey_settings_version(item)

    @router.get("/api/settings/journeys/active")
    def get_active_journeys_settings_route(
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        active = ensure_active_journey_settings_fn(db, actor="system")
        return _serialize_journey_settings_version(active)

    @router.get("/api/settings/journeys/readiness")
    def get_journeys_readiness_route(
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        from app.services_journey_readiness import build_journey_readiness

        journeys = get_journeys_fn(db)
        active = ensure_active_journey_settings_fn(db, actor="system")
        active_preview = build_journey_settings_impact_preview_fn(db, draft_settings_json=active.settings_json or {})
        return build_journey_readiness(
            journeys=journeys,
            kpi_config=get_kpi_config_fn(),
            get_import_runs_fn=get_import_runs_fn,
            active_settings=active,
            active_settings_preview=active_preview,
        )

    @router.get("/api/settings/journeys/context")
    def get_journeys_settings_context_route(
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        return build_journey_settings_context_fn(
            db,
            kpi_config=get_kpi_config_fn(),
        )

    @router.post("/api/settings/journeys/validate")
    def validate_journeys_settings_route(
        payload: JourneySettingsValidatePayload,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        if payload.version_id:
            item = get_journey_settings_version_fn(db, payload.version_id)
            if not item:
                raise HTTPException(status_code=404, detail="Journey settings version not found")
            target = item.settings_json or {}
        else:
            target = payload.settings_json or {}
        result = validate_journey_settings_fn(target)
        return {"valid": result["valid"], "errors": result["errors"], "warnings": result["warnings"]}

    @router.post("/api/settings/journeys/preview")
    def preview_journeys_settings_route(
        payload: JourneySettingsPreviewPayload,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        if payload.version_id:
            item = get_journey_settings_version_fn(db, payload.version_id)
            if not item:
                raise HTTPException(status_code=404, detail="Journey settings version not found")
            target = item.settings_json or {}
        else:
            target = payload.settings_json or {}
        return build_journey_settings_impact_preview_fn(db, draft_settings_json=target)

    @router.post("/api/settings/journeys/activate")
    def activate_journeys_settings_route(
        payload: JourneySettingsActivatePayload,
        _ctx=Depends(require_permission_dependency("settings.manage")),
        db=Depends(get_db_dependency),
    ):
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="Activation requires explicit confirm=true")
        item = get_journey_settings_version_fn(db, payload.version_id)
        if not item:
            raise HTTPException(status_code=404, detail="Journey settings version not found")
        preview = build_journey_settings_impact_preview_fn(db, draft_settings_json=item.settings_json or {})
        if not preview.get("preview_available"):
            raise HTTPException(status_code=400, detail="Impact preview unavailable; resolve validation before activation")
        try:
            item = activate_journey_settings_version_fn(
                db,
                version_id=payload.version_id,
                actor=payload.actor,
                activation_note=payload.activation_note,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {**_serialize_journey_settings_version(item), "impact_preview": preview}

    return router
