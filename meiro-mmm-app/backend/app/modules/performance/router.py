import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.attribution_engine import compute_next_best_action, has_any_campaign
from app.modules.settings.schemas import NBASettings, Settings
from app.services_canonical_facts import iter_canonical_conversion_rows
from app.services_import_runs import get_last_successful_run, get_runs as get_import_runs
from app.services_nba_defaults import filter_nba_recommendations
from app.services_overview import (
    get_overview_drivers,
    get_overview_funnels,
    get_overview_summary,
    get_overview_trend_insights,
)
from app.services_performance_lag import build_scope_lag_summary
from app.services_performance_diagnostics import build_scope_diagnostics
from app.services_performance_helpers import _local_date_from_ts
from app.services_performance_trends import (
    build_campaign_aggregate_overlay,
    build_campaign_summary_response,
    build_campaign_trend_response,
    build_channel_aggregate_overlay,
    build_channel_summary_response,
    build_channel_trend_response,
)
from app.services_quality import load_config_and_meta


def _selected_period_bounds(date_from: str, date_to: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(str(date_from)[:10]).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromisoformat(str(date_to)[:10]).replace(hour=23, minute=59, second=59, microsecond=999999)
    if end < start:
        start, end = end.replace(hour=0, minute=0, second=0, microsecond=0), start.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
        )
    return start, end


def _has_canonical_conversions(
    db: Any,
    *,
    date_from: str,
    date_to: str,
    conversion_key: Optional[str],
) -> bool:
    start, end = _selected_period_bounds(date_from, date_to)
    for _row in iter_canonical_conversion_rows(
        db,
        date_from=start,
        date_to=end,
        conversion_key=conversion_key,
    ):
        return True
    return False


def _resolve_effective_conversion_key(
    db: Any,
    *,
    requested_conversion_key: Optional[str],
    configured_conversion_key: Optional[str],
    date_from: str,
    date_to: str,
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    if requested_conversion_key is not None:
        return requested_conversion_key, None
    configured_key = (configured_conversion_key or "").strip() or None
    if not configured_key:
        return None, None
    if _has_canonical_conversions(
        db,
        date_from=date_from,
        date_to=date_to,
        conversion_key=configured_key,
    ):
        return configured_key, None
    if _has_canonical_conversions(
        db,
        date_from=date_from,
        date_to=date_to,
        conversion_key=None,
    ):
        return None, {
            "requested_conversion_key": None,
            "configured_conversion_key": configured_key,
            "applied_conversion_key": None,
            "reason": "configured_conversion_key_has_no_data_in_selected_period",
        }
    return configured_key, None


def _load_selected_config_meta(db: Any, model_id: Optional[str]) -> tuple[Any, Optional[dict[str, Any]]]:
    if not model_id:
        return None, None
    return load_config_and_meta(db, model_id)


_SETTINGS_PATH = Path(__file__).resolve().parents[2] / "data" / "settings.json"


def _load_runtime_nba_settings() -> NBASettings:
    if _SETTINGS_PATH.exists():
        try:
            payload = json.loads(_SETTINGS_PATH.read_text())
            return Settings(**payload).nba
        except Exception:
            pass
    return NBASettings()


def _filter_journeys_for_campaign_suggestions(
    *,
    journeys: list[dict[str, Any]],
    date_from: str,
    date_to: str,
    timezone: str,
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> list[dict[str, Any]]:
    start_d = datetime.fromisoformat(str(date_from)[:10]).date()
    end_d = datetime.fromisoformat(str(date_to)[:10]).date()
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    selected: list[dict[str, Any]] = []

    for journey in journeys or []:
        if conversion_key:
            journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
            if journey_key != conversion_key:
                continue
        touchpoints = journey.get("touchpoints") or []
        if not touchpoints:
            continue
        last_tp = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
        channel = str(last_tp.get("channel") or "unknown")
        if filter_channels and channel not in allowed_channels:
            continue
        day = _local_date_from_ts(last_tp.get("timestamp") or last_tp.get("ts"), timezone)
        if day is None or day < start_d or day > end_d:
            continue
        selected.append(journey)
    return selected


def _build_campaign_suggestions_payload(
    *,
    journeys: list[dict[str, Any]],
    settings: NBASettings,
) -> Dict[str, Any]:
    if not journeys:
        return {"items": {}, "level": "campaign", "eligible_journeys": 0}
    if not has_any_campaign(journeys):
        return {
            "items": {},
            "level": "campaign",
            "eligible_journeys": len(journeys),
            "reason": "Campaign suggestions unavailable because journeys lack campaign data.",
        }

    nba_campaign_raw = compute_next_best_action(journeys, level="campaign")
    nba_campaign, _stats = filter_nba_recommendations(nba_campaign_raw, settings)
    return {
        "items": {prefix: recs[0] for prefix, recs in nba_campaign.items() if recs},
        "level": "campaign",
        "eligible_journeys": len(journeys),
    }


def _build_channel_suggestions_payload(
    *,
    journeys: list[dict[str, Any]],
    settings: NBASettings,
) -> Dict[str, Any]:
    if not journeys:
        return {"items": {}, "level": "channel", "eligible_journeys": 0}

    nba_channel_raw = compute_next_best_action(journeys, level="channel")
    nba_channel, _stats = filter_nba_recommendations(nba_channel_raw, settings)
    return {
        "items": {
            prefix: recs[0]
            for prefix, recs in nba_channel.items()
            if prefix and recs
        },
        "level": "channel",
        "eligible_journeys": len(journeys),
    }


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


def _build_consistency_payload(db: Any, journeys: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        from app.services_journey_readiness import build_journey_readiness
        from app.services_journey_settings import (
            build_journey_settings_impact_preview,
            ensure_active_journey_settings,
        )
        from app.utils.kpi_config import load_kpi_config

        active_settings = ensure_active_journey_settings(db, actor="system")
        active_preview = build_journey_settings_impact_preview(
            db,
            draft_settings_json=active_settings.settings_json or {},
        )
        readiness = build_journey_readiness(
            journeys=journeys,
            kpi_config=load_kpi_config(),
            get_import_runs_fn=get_import_runs,
            active_settings=active_settings,
            active_settings_preview=active_preview,
        )
        warnings = [
            *readiness.get("blockers", []),
            *readiness.get("warnings", []),
        ]
        return readiness, warnings
    except Exception:
        return None, []


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    ensure_journeys_loaded_fn: Callable[[Any], list[dict]],
    get_overview_attention_queue_fn: Callable[[Any], list[dict[str, Any]]],
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
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        payload = get_overview_summary(
            db=db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            channel_group=channel_group,
            expenses=expenses_obj,
            import_runs_get_last_successful=get_last_successful_run,
        )
        payload["attention_queue"] = get_overview_attention_queue_fn(db)
        return payload

    @router.get("/api/overview/drivers")
    def overview_drivers(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        top_campaigns_n: int = Query(10, ge=1, le=50, description="Top N campaigns"),
        conversion_key: Optional[str] = Query(None, description="Filter by conversion key"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
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
            channel_group=channel_group,
        )

    @router.get("/api/overview/funnels")
    def overview_funnels(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        limit: int = Query(5, ge=1, le=10, description="Rows per tab"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_funnels(
            db=db,
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            limit=limit,
            channel_group=channel_group,
        )

    @router.get("/api/overview/trends")
    def overview_trends(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_trend_insights(
            db=db,
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            channel_group=channel_group,
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
            _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
            effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
                db,
                requested_conversion_key=query_ctx.conversion_key,
                configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
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
                conversion_key=effective_conversion_key,
                aggregate_overlay=build_channel_aggregate_overlay(
                    db,
                    date_from=query_ctx.date_from,
                    date_to=query_ctx.date_to,
                    timezone=query_ctx.timezone,
                    compare=query_ctx.compare,
                    channels=query_ctx.channels,
                    conversion_key=effective_conversion_key,
                    grain=query_ctx.grain,
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key, include_kpi=True)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
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
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        out = build_channel_summary_response(
            journeys=journeys,
            expenses=expenses_obj,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
            aggregate_overlay=build_channel_aggregate_overlay(
                db,
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=effective_conversion_key,
                grain="daily",
            ),
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
        readiness, consistency_warnings = _build_consistency_payload(db, journeys)
        out["readiness"] = readiness
        out["consistency_warnings"] = consistency_warnings
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/channel/lag")
    def performance_channel_lag(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return build_scope_lag_summary(
            db,
            scope_type="channel",
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            channels=channels,
        )

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
            _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
            effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
                db,
                requested_conversion_key=query_ctx.conversion_key,
                configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
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
                conversion_key=effective_conversion_key,
                aggregate_overlay=build_campaign_aggregate_overlay(
                    db,
                    date_from=query_ctx.date_from,
                    date_to=query_ctx.date_to,
                    timezone=query_ctx.timezone,
                    compare=query_ctx.compare,
                    channels=query_ctx.channels,
                    conversion_key=effective_conversion_key,
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key, include_kpi=True)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/campaign/lag")
    def performance_campaign_lag(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return build_scope_lag_summary(
            db,
            scope_type="campaign",
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            channels=channels,
        )

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
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        out = build_campaign_summary_response(
            journeys=journeys,
            expenses=expenses_obj,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
            aggregate_overlay=build_campaign_aggregate_overlay(
                db,
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=effective_conversion_key,
            ),
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
        readiness, consistency_warnings = _build_consistency_payload(db, journeys)
        out["readiness"] = readiness
        out["consistency_warnings"] = consistency_warnings
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/campaign/suggestions")
    def performance_campaign_suggestions(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
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
            compare=False,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        suggestion_journeys = _filter_journeys_for_campaign_suggestions(
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        out = _build_campaign_suggestions_payload(
            journeys=suggestion_journeys,
            settings=_load_runtime_nba_settings(),
        )
        out["config"] = config_meta
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/channel/suggestions")
    def performance_channel_suggestions(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
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
            compare=False,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        suggestion_journeys = _filter_journeys_for_campaign_suggestions(
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        out = _build_channel_suggestions_payload(
            journeys=suggestion_journeys,
            settings=_load_runtime_nba_settings(),
        )
        out["config"] = config_meta
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    return router
