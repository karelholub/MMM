from typing import Any, Callable, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.services_import_runs import get_last_successful_run
from app.services_overview import (
    get_overview_drivers,
    get_overview_funnels,
    get_overview_summary,
    get_overview_trend_insights,
)
from app.services_performance_diagnostics import build_scope_diagnostics
from app.services_performance_trends import (
    build_campaign_summary_response,
    build_campaign_trend_response,
    build_channel_summary_response,
    build_channel_trend_response,
)
from app.services_quality import load_config_and_meta


def _add_summary_derivatives(items: list[dict], scope_type: str, diagnostics: dict[str, Any]) -> None:
    for item in items:
        current = item.get("current") or {}
        previous = item.get("previous") or {}
        visits = float(current.get("visits", 0.0) or 0.0)
        conversions = float(current.get("conversions", 0.0) or 0.0)
        revenue = float(current.get("revenue", 0.0) or 0.0)
        spend = float(current.get("spend", 0.0) or 0.0)
        derived = item.setdefault("derived", {})
        derived["cvr"] = round((conversions / visits), 4) if visits > 0 else None
        derived["cost_per_visit"] = round((spend / visits), 4) if visits > 0 else None
        derived["revenue_per_visit"] = round((revenue / visits), 4) if visits > 0 else None

        prev_visits = float(previous.get("visits", 0.0) or 0.0)
        prev_conversions = float(previous.get("conversions", 0.0) or 0.0)
        prev_revenue = float(previous.get("revenue", 0.0) or 0.0)
        prev_spend = float(previous.get("spend", 0.0) or 0.0)
        item["previous_derived"] = {
            "cvr": round((prev_conversions / prev_visits), 4) if prev_visits > 0 else None,
            "cost_per_visit": round((prev_spend / prev_visits), 4) if prev_visits > 0 else None,
            "revenue_per_visit": round((prev_revenue / prev_visits), 4) if prev_visits > 0 else None,
        } if previous else None

        if scope_type == "channel":
            diag_key = str(item.get("channel"))
        else:
            diag_key = str(item.get("campaign_id") or item.get("channel"))
        item["diagnostics"] = diagnostics.get(diag_key, {})


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    ensure_journeys_loaded_fn: Callable[[Any], list[dict]],
    build_query_context_fn: Callable[..., Any],
    build_meta_fn: Callable[..., dict[str, Any]],
    attach_scope_confidence_fn: Callable[..., None],
    build_mapping_coverage_fn: Callable[..., dict[str, Any]],
    summarize_mapped_current_fn: Callable[[list[dict]], dict[str, float]],
    expenses_obj: dict[str, Any],
) -> APIRouter:
    router = APIRouter(tags=["performance"])

    @router.get("/api/overview/summary")
    def overview_summary(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="Timezone for display"),
        currency: Optional[str] = Query(None, description="Filter/display currency"),
        workspace: Optional[str] = Query(None, description="Workspace filter"),
        account: Optional[str] = Query(None, description="Account filter"),
        model_id: Optional[str] = Query(None, description="Optional model config id (for metadata only)"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_summary(
            db=db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            expenses=expenses_obj,
            import_runs_get_last_successful=get_last_successful_run,
        )

    @router.get("/api/overview/drivers")
    def overview_drivers(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        top_campaigns_n: int = Query(10, ge=1, le=50, description="Top N campaigns"),
        conversion_key: Optional[str] = Query(None, description="Filter by conversion key"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_drivers(
            db=db,
            date_from=date_from,
            date_to=date_to,
            expenses=expenses_obj,
            top_campaigns_n=top_campaigns_n,
            conversion_key=conversion_key,
        )

    @router.get("/api/overview/funnels")
    def overview_funnels(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        limit: int = Query(5, ge=1, le=10, description="Rows per tab"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_funnels(
            db=db,
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            limit=limit,
        )

    @router.get("/api/overview/trends")
    def overview_trends(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_trend_insights(
            db=db,
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
        )

    @router.get("/api/performance/channel/trend")
    def performance_channel_trend(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        kpi_key: str = Query("revenue", description="spend|visits|conversions|revenue|cpa|roas"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        grain: str = Query("auto", description="auto|daily|weekly"),
        compare: bool = Query(True, description="Include previous-period series"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys = ensure_journeys_loaded_fn(db)
        try:
            query_ctx = build_query_context_fn(
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                currency=currency,
                workspace=workspace,
                account=account,
                model_id=model_id,
                kpi_key=kpi_key,
                grain=grain,
                compare=compare,
                channels=channels,
                conversion_key=conversion_key,
            )
            out = build_channel_trend_response(
                journeys=journeys,
                expenses=expenses_obj,
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                kpi_key=query_ctx.kpi_key,
                grain=query_ctx.grain,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=query_ctx.conversion_key,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        out["meta"] = build_meta_fn(ctx=query_ctx, include_kpi=True)
        return out

    @router.get("/api/performance/channel/summary")
    def performance_channel_summary(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        compare: bool = Query(True, description="Include previous-period summary"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys = ensure_journeys_loaded_fn(db)
        query_ctx = build_query_context_fn(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key="revenue",
            grain="daily",
            compare=compare,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = load_config_and_meta(db, query_ctx.model_id)
        effective_conversion_key = query_ctx.conversion_key or (config_meta.get("conversion_key") if config_meta else None)
        out = build_channel_summary_response(
            journeys=journeys,
            expenses=expenses_obj,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        diagnostics = build_scope_diagnostics(
            db=db,
            scope_type="channel",
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            conversion_key=effective_conversion_key,
            channels=query_ctx.channels,
        )
        _add_summary_derivatives(out.get("items", []), "channel", diagnostics)
        attach_scope_confidence_fn(
            db=db,
            items=out.get("items", []),
            scope_type="channel",
            id_field="channel",
            conversion_key=effective_conversion_key,
        )
        mapped = summarize_mapped_current_fn(out.get("items", []))
        out["config"] = config_meta
        out["mapping_coverage"] = build_mapping_coverage_fn(
            mapped_spend=mapped["mapped_spend"],
            mapped_value=mapped["mapped_value"],
            expenses=expenses_obj,
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone_name=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        return out

    @router.get("/api/performance/campaign/trend")
    def performance_campaign_trend(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        kpi_key: str = Query("revenue", description="spend|visits|conversions|revenue|cpa|roas"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        grain: str = Query("auto", description="auto|daily|weekly"),
        compare: bool = Query(True, description="Include previous-period series"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys = ensure_journeys_loaded_fn(db)
        try:
            query_ctx = build_query_context_fn(
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                currency=currency,
                workspace=workspace,
                account=account,
                model_id=model_id,
                kpi_key=kpi_key,
                grain=grain,
                compare=compare,
                channels=channels,
                conversion_key=conversion_key,
            )
            out = build_campaign_trend_response(
                journeys=journeys,
                expenses=expenses_obj,
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                kpi_key=query_ctx.kpi_key,
                grain=query_ctx.grain,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=query_ctx.conversion_key,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        out["meta"] = build_meta_fn(ctx=query_ctx, include_kpi=True)
        return out

    @router.get("/api/performance/campaign/summary")
    def performance_campaign_summary(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        compare: bool = Query(True, description="Include previous-period summary"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys = ensure_journeys_loaded_fn(db)
        query_ctx = build_query_context_fn(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key="revenue",
            grain="daily",
            compare=compare,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = load_config_and_meta(db, query_ctx.model_id)
        effective_conversion_key = query_ctx.conversion_key or (config_meta.get("conversion_key") if config_meta else None)
        out = build_campaign_summary_response(
            journeys=journeys,
            expenses=expenses_obj,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        diagnostics = build_scope_diagnostics(
            db=db,
            scope_type="campaign",
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            conversion_key=effective_conversion_key,
            channels=query_ctx.channels,
        )
        _add_summary_derivatives(out.get("items", []), "campaign", diagnostics)
        attach_scope_confidence_fn(
            db=db,
            items=out.get("items", []),
            scope_type="campaign",
            id_field="campaign_id",
            conversion_key=effective_conversion_key,
        )
        mapped = summarize_mapped_current_fn(out.get("items", []))
        out["config"] = config_meta
        out["mapping_coverage"] = build_mapping_coverage_fn(
            mapped_spend=mapped["mapped_spend"],
            mapped_value=mapped["mapped_value"],
            expenses=expenses_obj,
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone_name=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        return out

    return router
