import logging
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.modules.alerts_funnels.schemas import (
    AlertRuleCreate,
    AlertRuleUpdate,
    AlertSnoozeBody,
    FunnelCreatePayload,
    JourneyAlertCreatePayload,
    JourneyAlertPreviewPayload,
    JourneyAlertUpdatePayload,
)
from app.services_journey_alerts import ALERT_DOMAINS as JOURNEY_ALERT_DOMAINS
from app.services_journey_alerts import ALERT_TYPES as JOURNEY_ALERT_TYPES
from app.services_alerts import (
    ack_alert,
    create_alert_rule,
    delete_alert_rule,
    get_alert_by_id,
    get_alert_rule_by_id,
    list_alert_rules,
    list_alerts,
    resolve_alert,
    snooze_alert,
    update_alert_rule,
)
from app.services_funnels import (
    create_funnel,
    get_funnel,
    get_funnel_diagnostics as compute_funnel_diagnostics,
    get_funnel_results as compute_funnel_results,
    list_funnels,
)
from app.services_journey_alerts import (
    create_alert_definition as create_journey_alert_definition,
    list_alert_definitions as list_journey_alert_definitions,
    list_alert_events as list_journey_alert_events,
    preview_alert as preview_journey_alert,
    update_alert_definition as update_journey_alert_definition,
)

logger = logging.getLogger(__name__)


def _validate_journey_alert_payload(type: str, domain: str, metric: str, condition: Dict[str, Any]) -> None:
    if type not in JOURNEY_ALERT_TYPES:
        raise HTTPException(status_code=400, detail=f"type must be one of: {', '.join(sorted(JOURNEY_ALERT_TYPES))}")
    if domain not in JOURNEY_ALERT_DOMAINS:
        raise HTTPException(status_code=400, detail=f"domain must be one of: {', '.join(sorted(JOURNEY_ALERT_DOMAINS))}")
    metric_clean = (metric or "").strip()
    if not metric_clean:
        raise HTTPException(status_code=400, detail="metric is required")
    mode = str((condition or {}).get("comparison_mode") or "previous_period")
    if mode not in {"previous_period", "rolling_baseline"}:
        raise HTTPException(status_code=400, detail="condition.comparison_mode must be previous_period or rolling_baseline")


def _validate_journey_alert_scope_and_metric(type: str, scope: Dict[str, Any], metric: str) -> None:
    scope = scope or {}
    metric = (metric or "").strip()
    allowed_metrics = {
        "path_cr_drop": {"conversion_rate"},
        "path_volume_change": {
            "count_journeys",
            "gross_conversions_total",
            "net_conversions_total",
            "gross_revenue_total",
            "net_revenue_total",
        },
        "funnel_dropoff_spike": {"dropoff_rate"},
        "ttc_shift": {"p50_time_to_convert_sec"},
    }
    if metric not in allowed_metrics.get(type, set()):
        raise HTTPException(status_code=400, detail=f"metric '{metric}' is not valid for alert type '{type}'")

    if type in {"path_cr_drop", "path_volume_change", "ttc_shift"}:
        journey_definition_id = str(scope.get("journey_definition_id") or "").strip()
        if not journey_definition_id:
            raise HTTPException(status_code=400, detail="scope.journey_definition_id is required")

    if type == "funnel_dropoff_spike":
        funnel_id = str(scope.get("funnel_id") or "").strip()
        if not funnel_id:
            raise HTTPException(status_code=400, detail="scope.funnel_id is required")
        try:
            step_index = int(scope.get("step_index"))
        except Exception:
            raise HTTPException(status_code=400, detail="scope.step_index is required")
        if step_index < 0:
            raise HTTPException(status_code=400, detail="scope.step_index must be >= 0")


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    clamp_int_fn: Callable[..., int],
    resolve_per_page_fn: Callable[..., int],
    get_journey_definition_fn: Callable[..., Any],
) -> APIRouter:
    router = APIRouter(tags=["alerts_funnels"])

    @router.get("/api/funnels")
    def api_list_funnels(
        workspace_id: str = Query("default"),
        user_id: Optional[str] = Query(None),
        journey_definition_id: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("funnels.view")),
    ):
        return {
            "items": list_funnels(
                db,
                workspace_id=workspace_id,
                user_id=user_id,
                journey_definition_id=journey_definition_id,
            )
        }

    @router.post("/api/funnels")
    def api_create_funnel(
        body: FunnelCreatePayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("funnels.manage")),
    ):
        user_id = ctx.user_id
        jd = get_journey_definition_fn(db, body.journey_definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        steps = [str(s).strip() for s in (body.steps or []) if str(s).strip()]
        if len(steps) < 2:
            raise HTTPException(status_code=400, detail="steps must include at least 2 items")
        return create_funnel(
            db,
            journey_definition_id=body.journey_definition_id,
            workspace_id=body.workspace_id or "default",
            user_id=user_id,
            name=body.name,
            description=body.description,
            steps=steps,
            counting_method=body.counting_method,
            window_days=body.window_days,
            actor=user_id,
        )

    @router.get("/api/funnels/{funnel_id}/results")
    def api_get_funnel_results(
        funnel_id: str,
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        device: Optional[str] = Query(None),
        channel_group: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("funnels.view")),
    ):
        funnel = get_funnel(db, funnel_id)
        if not funnel or funnel.is_archived:
            raise HTTPException(status_code=404, detail="Funnel not found")
        jd = get_journey_definition_fn(db, funnel.journey_definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            d_from = datetime.fromisoformat(date_from).date()
            d_to = datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")
        return compute_funnel_results(
            db,
            funnel=funnel,
            journey_definition=jd,
            date_from=d_from,
            date_to=d_to,
            device=device,
            channel_group=channel_group,
            country=country,
            campaign_id=campaign_id,
        )

    @router.get("/api/funnels/{funnel_id}/diagnostics")
    def api_get_funnel_diagnostics(
        funnel_id: str,
        step: str = Query(..., description="Funnel step label to diagnose"),
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        device: Optional[str] = Query(None),
        channel_group: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("funnels.view")),
    ):
        funnel = get_funnel(db, funnel_id)
        if not funnel or funnel.is_archived:
            raise HTTPException(status_code=404, detail="Funnel not found")
        jd = get_journey_definition_fn(db, funnel.journey_definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            d_from = datetime.fromisoformat(date_from).date()
            d_to = datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")
        return compute_funnel_diagnostics(
            db,
            funnel=funnel,
            journey_definition=jd,
            step=step,
            date_from=d_from,
            date_to=d_to,
            device=device,
            channel_group=channel_group,
            country=country,
            campaign_id=campaign_id,
        )

    @router.get("/api/alerts")
    def api_list_alerts(
        domain: Optional[str] = Query(None, description="journeys | funnels (new journey/funnel alerts domain)"),
        status: str = Query("open", description="open | all"),
        severity: Optional[str] = Query(None, description="Filter by severity"),
        rule_type: Optional[str] = Query(None, description="Filter by rule type"),
        search: Optional[str] = Query(None, description="Search in title, message, rule name"),
        page: int = Query(1, ge=1, description="Page number"),
        per_page: Optional[int] = Query(None, description="Items per page"),
        page_size: Optional[int] = Query(None, description="Alias for per_page"),
        limit: Optional[int] = Query(None, description="Alias for per_page"),
        scope: Optional[str] = Query(None, description="Filter by scope (workspace/account)"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("alerts.view")),
    ):
        resolved_page = clamp_int_fn(page, default=1, minimum=1, maximum=1_000_000)
        resolved_per_page = resolve_per_page_fn(per_page=per_page, page_size=page_size, limit=limit, default=20, maximum=100)
        try:
            if domain in JOURNEY_ALERT_DOMAINS:
                return list_journey_alert_definitions(db=db, domain=domain, page=resolved_page, per_page=resolved_per_page)
            return list_alerts(
                db=db,
                status=status,
                severity=severity,
                rule_type=rule_type,
                search=search,
                page=resolved_page,
                per_page=resolved_per_page,
                scope=scope,
            )
        except Exception as exc:
            logger.warning("List alerts failed (tables or schema may be missing): %s", exc, exc_info=True)
            return {"items": [], "total": 0, "page": resolved_page, "per_page": resolved_per_page}

    @router.get("/api/alerts/events")
    def api_list_alert_events(
        domain: Optional[str] = Query(None, description="journeys | funnels"),
        page: int = Query(1, ge=1),
        per_page: Optional[int] = Query(None),
        page_size: Optional[int] = Query(None, description="Alias for per_page"),
        limit: Optional[int] = Query(None, description="Alias for per_page"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("alerts.view")),
    ):
        resolved_page = clamp_int_fn(page, default=1, minimum=1, maximum=1_000_000)
        resolved_per_page = resolve_per_page_fn(per_page=per_page, page_size=page_size, limit=limit, default=20, maximum=200)
        if domain not in JOURNEY_ALERT_DOMAINS:
            raise HTTPException(status_code=400, detail="domain must be journeys or funnels")
        try:
            return list_journey_alert_events(db=db, domain=domain, page=resolved_page, per_page=resolved_per_page)
        except Exception as exc:
            logger.warning("List journey alert events failed: %s", exc, exc_info=True)
            return {"items": [], "total": 0, "page": resolved_page, "per_page": resolved_per_page}

    @router.post("/api/alerts")
    def api_create_alert(
        body: JourneyAlertCreatePayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        user_id = ctx.user_id
        _validate_journey_alert_payload(body.type, body.domain, body.metric, body.condition)
        _validate_journey_alert_scope_and_metric(body.type, body.scope or {}, body.metric)
        return create_journey_alert_definition(
            db,
            name=body.name,
            type=body.type,
            domain=body.domain,
            scope=body.scope or {},
            metric=body.metric,
            condition=body.condition or {},
            schedule=body.schedule or {"cadence": "daily"},
            is_enabled=body.is_enabled,
            actor=user_id,
        )

    @router.put("/api/alerts/{alert_definition_id}")
    def api_update_alert(
        alert_definition_id: str,
        body: JourneyAlertUpdatePayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        out = update_journey_alert_definition(
            db,
            definition_id=alert_definition_id,
            actor=ctx.user_id,
            name=body.name,
            is_enabled=body.is_enabled,
            condition=body.condition,
            schedule=body.schedule,
        )
        if not out:
            raise HTTPException(status_code=404, detail="Alert definition not found")
        return out

    @router.post("/api/alerts/preview")
    def api_preview_alert(
        body: JourneyAlertPreviewPayload,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("alerts.view")),
    ):
        _validate_journey_alert_payload(body.type, "journeys" if body.type != "funnel_dropoff_spike" else "funnels", body.metric, body.condition)
        _validate_journey_alert_scope_and_metric(body.type, body.scope or {}, body.metric)
        return preview_journey_alert(db, type=body.type, scope=body.scope or {}, metric=body.metric, condition=body.condition or {})

    @router.get("/api/alerts/{alert_id}")
    def api_get_alert(
        alert_id: int,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("alerts.view")),
    ):
        out = get_alert_by_id(db=db, alert_id=alert_id)
        if not out:
            raise HTTPException(status_code=404, detail="Alert not found")
        return out

    @router.post("/api/alerts/{alert_id}/ack")
    def api_ack_alert(
        alert_id: int,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        ev = ack_alert(db=db, alert_id=alert_id, user_id=ctx.user_id)
        if not ev:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {"id": ev.id, "status": ev.status}

    @router.post("/api/alerts/{alert_id}/snooze")
    def api_snooze_alert(
        alert_id: int,
        body: AlertSnoozeBody,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        ev = snooze_alert(db=db, alert_id=alert_id, duration_minutes=body.duration_minutes, user_id=ctx.user_id)
        if not ev:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {"id": ev.id, "status": ev.status, "snooze_until": ev.snooze_until.isoformat() if getattr(ev, "snooze_until", None) and ev.snooze_until else None}

    @router.post("/api/alerts/{alert_id}/resolve")
    def api_resolve_alert(
        alert_id: int,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        ev = resolve_alert(db=db, alert_id=alert_id, user_id=ctx.user_id)
        if not ev:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {"id": ev.id, "status": ev.status}

    @router.get("/api/alert-rules")
    def api_list_alert_rules(
        scope: Optional[str] = Query(None),
        is_enabled: Optional[bool] = Query(None, description="Filter by enabled"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("alerts.view")),
    ):
        try:
            return list_alert_rules(db=db, scope=scope, is_enabled=is_enabled)
        except Exception as exc:
            logger.warning("List alert rules failed (tables or schema may be missing): %s", exc, exc_info=True)
            return []

    @router.post("/api/alert-rules")
    def api_create_alert_rule(
        body: AlertRuleCreate,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        try:
            rule = create_alert_rule(
                db=db,
                name=body.name,
                scope=body.scope,
                severity=body.severity,
                rule_type=body.rule_type,
                schedule=body.schedule,
                created_by=ctx.user_id,
                description=body.description,
                is_enabled=body.is_enabled,
                kpi_key=body.kpi_key,
                dimension_filters_json=body.dimension_filters_json,
                params_json=body.params_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return get_alert_rule_by_id(db=db, rule_id=rule.id)

    @router.get("/api/alert-rules/{rule_id}")
    def api_get_alert_rule(
        rule_id: int,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("alerts.view")),
    ):
        out = get_alert_rule_by_id(db=db, rule_id=rule_id)
        if not out:
            raise HTTPException(status_code=404, detail="Alert rule not found")
        return out

    @router.put("/api/alert-rules/{rule_id}")
    def api_update_alert_rule(
        rule_id: int,
        body: AlertRuleUpdate,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        try:
            rule = update_alert_rule(
                db=db,
                rule_id=rule_id,
                updated_by=ctx.user_id,
                name=body.name,
                description=body.description,
                is_enabled=body.is_enabled,
                severity=body.severity,
                kpi_key=body.kpi_key,
                dimension_filters_json=body.dimension_filters_json,
                params_json=body.params_json,
                schedule=body.schedule,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if not rule:
            raise HTTPException(status_code=404, detail="Alert rule not found")
        return get_alert_rule_by_id(db=db, rule_id=rule.id)

    @router.delete("/api/alert-rules/{rule_id}")
    def api_delete_alert_rule(
        rule_id: int,
        disable_only: bool = Query(True, description="If true, disable rule; if false, delete"),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("alerts.manage")),
    ):
        ok = delete_alert_rule(db=db, rule_id=rule_id, updated_by=ctx.user_id, disable_only=disable_only)
        if not ok:
            raise HTTPException(status_code=404, detail="Alert rule not found")
        return {"id": rule_id, "disabled": disable_only}

    return router
