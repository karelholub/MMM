from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import func

from app.attribution_engine import run_attribution, run_attribution_campaign
from app.main import EXPENSES, SETTINGS, SessionLocal
from app.models_config_dq import ConversionPath, JourneyDefinition, JourneyPathDaily
from app.services_conversions import (
    apply_model_config_to_journeys,
    conversion_path_is_converted,
    conversion_path_outcome_summary,
    filter_journeys_by_quality,
    load_journeys_from_db,
)
from app.services_overview import (
    _single_active_overview_definition_id,
    _normalize_period_bounds,
    get_overview_drivers,
    get_overview_funnels,
    get_overview_summary,
    get_overview_trend_insights,
)
from app.services_conversion_paths_adapter import build_conversion_paths_analysis_from_daily
from app.services_performance_diagnostics import build_scope_diagnostics
from app.services_performance_lag import build_scope_lag_summary
from app.services_performance_trends import (
    build_campaign_aggregate_overlay,
    build_campaign_summary_response,
    build_channel_aggregate_overlay,
    build_channel_summary_response,
)
from app.services_quality import load_config_and_meta


def _default_date_bounds() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=30)).date().isoformat()
    end = now.date().isoformat()
    return start, end


def _converted_journey_count(journeys: List[Dict[str, Any]]) -> int:
    return sum(1 for journey in journeys if (journey.get("conversions") or journey.get("converted")))


def _performance_kwargs(model: str) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    return kwargs


def _filter_journeys_to_window(
    journeys: List[Dict[str, Any]],
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not date_from and not date_to:
        return journeys
    start = pd.to_datetime(date_from).to_pydatetime() if date_from else None
    end = pd.to_datetime(date_to).to_pydatetime() if date_to else None
    filtered: List[Dict[str, Any]] = []
    for journey in journeys or []:
        touchpoints = journey.get("touchpoints") or []
        conversions = journey.get("conversions") or []
        conv_ts = None
        if conversions and isinstance(conversions[0], dict):
            conv_raw = conversions[0].get("ts") or conversions[0].get("timestamp")
            if conv_raw:
                parsed = pd.to_datetime(conv_raw, utc=True, errors="coerce")
                if not pd.isna(parsed):
                    conv_ts = parsed.to_pydatetime()
        if conv_ts is None:
            tp_times = []
            for tp in touchpoints:
                if not isinstance(tp, dict):
                    continue
                raw = tp.get("ts") or tp.get("timestamp")
                if not raw:
                    continue
                parsed = pd.to_datetime(raw, utc=True, errors="coerce")
                if pd.isna(parsed):
                    continue
                tp_times.append(parsed.to_pydatetime())
            conv_ts = max(tp_times) if tp_times else None
        if conv_ts is None:
            continue
        conv_day = conv_ts.date()
        if start and conv_day < start.date():
            continue
        if end and conv_day > end.date():
            continue
        filtered.append(journey)
    return filtered


def _date_scoped_conversion_path_totals(
    db: Any,
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
) -> Dict[str, float]:
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to, timezone)
    rows = (
        db.query(ConversionPath)
        .filter(ConversionPath.conversion_ts >= dt_from, ConversionPath.conversion_ts <= dt_to)
        .all()
    )
    totals = {
        "count_conversions": 0.0,
        "gross_conversions": 0.0,
        "net_conversions": 0.0,
        "gross_revenue": 0.0,
        "net_revenue": 0.0,
    }
    for row in rows:
        if not conversion_path_is_converted(row):
            continue
        outcome = conversion_path_outcome_summary(row)
        totals["count_conversions"] += 1.0
        totals["gross_conversions"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
        totals["net_conversions"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        totals["gross_revenue"] += float(outcome.get("gross_value", 0.0) or 0.0)
        totals["net_revenue"] += float(outcome.get("net_value", 0.0) or 0.0)
    return totals


def _surface_basis_registry(
    *,
    config_id: Optional[str],
    conversion_key: Optional[str],
    active_definition_id: Optional[str],
    conversion_paths_source: Optional[str],
    live_path_journeys: int,
) -> Dict[str, Dict[str, Any]]:
    config_label = config_id or "default active"
    return {
        "overview": {
            "basis_type": "workspace_facts",
            "config_behavior": "selected config is context only; not applied retroactively",
            "primary_source": "canonical workspace facts",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "attribution_comparison": {
            "basis_type": "live_attribution_config_aware",
            "config_behavior": "selected config applied to live attribution journeys",
            "primary_source": "live attribution results",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "attribution_roles": {
            "basis_type": "live_attribution_config_aware",
            "config_behavior": "selected config applied to live attribution role entities",
            "primary_source": "live attribution journeys and derived role entities",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "attribution_trust": {
            "basis_type": "mixed_live_and_materialized",
            "config_behavior": "selected config affects live attribution diagnostics but not stored path outputs",
            "primary_source": "live attribution diagnostics + materialized path outputs",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "channel_performance": {
            "basis_type": "mixed_config_aware_and_workspace_diagnostics",
            "config_behavior": "selected config applies to summary and trend panels; lag remains workspace diagnostic facts",
            "primary_source": "config-aware performance summaries + workspace lag diagnostics",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "campaign_performance": {
            "basis_type": "mixed_config_aware_and_workspace_diagnostics",
            "config_behavior": "selected config applies to summary and trend panels; lag remains workspace diagnostic facts",
            "primary_source": "config-aware performance summaries + workspace lag diagnostics",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "incrementality": {
            "basis_type": "workspace_facts_with_config_provenance",
            "config_behavior": "selected config is stored on new experiments for provenance, but planner setup context remains workspace-observed",
            "primary_source": "observed journeys + KPI settings",
            "selected_config": config_label,
            "conversion_key": conversion_key,
        },
        "journeys": {
            "basis_type": "materialized_definition_outputs",
            "config_behavior": "selected config is workspace context only",
            "primary_source": "stored journey-definition outputs",
            "selected_config": config_label,
            "journey_definition_id": active_definition_id,
        },
        "conversion_paths": {
            "basis_type": "materialized_definition_outputs",
            "config_behavior": "selected config is workspace context only",
            "primary_source": conversion_paths_source or "journey_paths_daily",
            "selected_config": config_label,
            "journey_definition_id": active_definition_id,
        },
        "path_archetypes": {
            "basis_type": "live_attribution_config_aware",
            "config_behavior": "selected config applied before live archetype clustering",
            "primary_source": "live attribution journeys",
            "selected_config": config_label,
            "conversion_key": conversion_key,
            "live_journeys_in_window": live_path_journeys,
        },
        "mmm_dashboard": {
            "basis_type": "mmm_model_run",
            "config_behavior": "uses saved MMM run inputs, not live attribution config directly",
            "primary_source": "saved MMM model outputs",
            "selected_config": config_label,
        },
    }


def _audience_mode_registry() -> Dict[str, Dict[str, Any]]:
    return {
        "overview": {
            "exact_filter_mode": "directly comparable to workspace baseline within the same page scope",
            "analytical_lens_mode": "directional comparison based on matched journey-instance rows",
        },
        "attribution_comparison": {
            "exact_filter_mode": "directly comparable within visible live-attribution rows",
            "analytical_lens_mode": "directional comparison based on matched journey-instance rows; not a literal page filter",
        },
        "attribution_roles": {
            "exact_filter_mode": "directly comparable within visible live role entities",
            "analytical_lens_mode": "directional comparison based on matched journey-instance rows and derived role entities",
        },
        "attribution_trust": {
            "exact_filter_mode": "directly comparable only for cards backed by focused materialized path rows",
            "analytical_lens_mode": "directional comparison based on matched journey-instance rows alongside workspace reconciliation",
        },
        "journeys": {
            "exact_filter_mode": "exact page filtering when the segment is definition-compatible",
            "analytical_lens_mode": "directional comparison only; stored outputs are not fully re-scoped",
        },
        "conversion_paths": {
            "exact_filter_mode": "directly comparable within materialized journey-definition outputs",
            "analytical_lens_mode": "not supported as a direct page filter",
        },
        "path_archetypes": {
            "exact_filter_mode": "directly comparable within live clustered journeys",
            "analytical_lens_mode": "not supported as a direct page filter",
        },
    }


def _date_scoped_journey_path_daily_totals(
    db: Any,
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    journey_definition_id: Optional[str] = None,
) -> Dict[str, float]:
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to, timezone)
    query = db.query(
        func.sum(JourneyPathDaily.count_conversions),
        func.sum(JourneyPathDaily.gross_conversions_total),
        func.sum(JourneyPathDaily.net_conversions_total),
        func.sum(JourneyPathDaily.gross_revenue_total),
        func.sum(JourneyPathDaily.net_revenue_total),
    ).filter(
        JourneyPathDaily.date >= dt_from.date(),
        JourneyPathDaily.date <= dt_to.date(),
    )
    if journey_definition_id:
        query = query.filter(JourneyPathDaily.journey_definition_id == journey_definition_id)
    row = query.first()
    return {
        "count_conversions": float((row[0] if row else 0.0) or 0.0),
        "gross_conversions": float((row[1] if row else 0.0) or 0.0),
        "net_conversions": float((row[2] if row else 0.0) or 0.0),
        "gross_revenue": float((row[3] if row else 0.0) or 0.0),
        "net_revenue": float((row[4] if row else 0.0) or 0.0),
    }


def _role_totals_from_diagnostics(diags: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    totals = {
        "first_touch_conversions": 0,
        "assist_conversions": 0,
        "last_touch_conversions": 0,
    }
    for diag in (diags or {}).values():
        roles = diag.get("roles") or {}
        totals["first_touch_conversions"] += int(roles.get("first_touch_conversions") or 0)
        totals["assist_conversions"] += int(roles.get("assist_conversions") or 0)
        totals["last_touch_conversions"] += int(roles.get("last_touch_conversions") or 0)
    return totals


def build_consistency_audit(
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    model: str = "linear",
    config_id: Optional[str] = None,
) -> Dict[str, Any]:
    with SessionLocal() as db:
        journeys = load_journeys_from_db(db)
        resolved_cfg, meta = load_config_and_meta(db, config_id)
        journeys_for_model = apply_model_config_to_journeys(
            journeys,
            resolved_cfg.config_json or {},
        ) if resolved_cfg else journeys
        journeys_for_model = filter_journeys_by_quality(
            journeys_for_model,
            int(getattr(SETTINGS.attribution, "min_journey_quality_score", 0) or 0),
        )
        conversion_key = meta.get("conversion_key") if meta else None
        if conversion_key:
            journeys_for_model = [journey for journey in journeys_for_model if journey.get("kpi_type") == conversion_key]
        active_definition_id = _single_active_overview_definition_id(db, conversion_key=conversion_key)

        attribution_result = run_attribution(
            journeys_for_model,
            model=model,
            **_performance_kwargs(model),
        )
        campaign_result = run_attribution_campaign(
            journeys_for_model,
            model=model,
            **_performance_kwargs(model),
        )

        overview_summary = get_overview_summary(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            expenses=EXPENSES,
        )
        overview_drivers = get_overview_drivers(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            expenses=EXPENSES,
        )
        overview_funnels = get_overview_funnels(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
        )
        overview_trends = get_overview_trend_insights(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
        )
        channel_summary = build_channel_summary_response(
            journeys=journeys,
            expenses=EXPENSES,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            compare=False,
            aggregate_overlay=build_channel_aggregate_overlay(
                db,
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                compare=False,
            ),
        )
        campaign_summary = build_campaign_summary_response(
            journeys=journeys,
            expenses=EXPENSES,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            compare=False,
            aggregate_overlay=build_campaign_aggregate_overlay(
                db,
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                compare=False,
            ),
        )
        channel_lag_summary = build_scope_lag_summary(
            db,
            scope_type="channel",
            date_from=date_from,
            date_to=date_to,
            conversion_key=None,
        )
        campaign_lag_summary = build_scope_lag_summary(
            db,
            scope_type="campaign",
            date_from=date_from,
            date_to=date_to,
            conversion_key=None,
        )
        channel_scope_diagnostics = build_scope_diagnostics(
            db=db,
            scope_type="channel",
            date_from=date_from,
            date_to=date_to,
            conversion_key=None,
        )
        campaign_scope_diagnostics = build_scope_diagnostics(
            db=db,
            scope_type="campaign",
            date_from=date_from,
            date_to=date_to,
            conversion_key=None,
        )
        conversion_paths_analysis = build_conversion_paths_analysis_from_daily(
            db,
            definition_id=active_definition_id,
            date_from=datetime.fromisoformat(date_from).date() if date_from else None,
            date_to=datetime.fromisoformat(date_to).date() if date_to else None,
            direct_mode="include",
            path_scope="converted",
        )
        live_journeys_for_archetypes = _filter_journeys_to_window(
            journeys_for_model,
            date_from=date_from,
            date_to=date_to,
        )

        overview_conversions = next(
            (tile["value"] for tile in overview_summary.get("kpi_tiles", []) if tile.get("kpi_key") == "conversions"),
            0,
        )
        drivers_conversions = sum(int(item.get("conversions") or 0) for item in overview_drivers.get("by_channel", []))
        funnels_conversions = int((overview_funnels.get("summary") or {}).get("total_conversions") or 0)
        trends_conversions = float((((overview_trends.get("decomposition") or {}).get("current") or {}).get("conversions") or 0.0))
        overview_revenue = float(
            next(
                (tile["value"] for tile in overview_summary.get("kpi_tiles", []) if tile.get("kpi_key") == "revenue"),
                0.0,
            )
            or 0.0
        )
        overview_net_revenue = float(
            next(
                (tile["value"] for tile in overview_summary.get("kpi_tiles", []) if tile.get("kpi_key") == "net_revenue"),
                0.0,
            )
            or 0.0
        )
        funnels_gross_revenue = float((overview_funnels.get("summary") or {}).get("gross_revenue") or 0.0)
        funnels_net_revenue = float((overview_funnels.get("summary") or {}).get("net_revenue") or 0.0)
        trends_revenue = float((((overview_trends.get("decomposition") or {}).get("current") or {}).get("revenue") or 0.0))
        overview_spend = float(
            next(
                (tile["value"] for tile in overview_summary.get("kpi_tiles", []) if tile.get("kpi_key") == "spend"),
                0.0,
            )
            or 0.0
        )
        overview_visits = int(
            next(
                (tile["value"] for tile in overview_summary.get("kpi_tiles", []) if tile.get("kpi_key") == "visits"),
                0,
            )
            or 0
        )
        channel_totals = ((channel_summary.get("totals") or {}).get("current") or {})
        campaign_totals = ((campaign_summary.get("totals") or {}).get("current") or {})
        channel_lag_role_totals = {
            "first_touch_conversions": sum(int(item.get("role_mix", {}).get("first_touch_conversions") or 0) for item in channel_lag_summary.get("items", [])),
            "assist_conversions": sum(int(item.get("role_mix", {}).get("assist_conversions") or 0) for item in channel_lag_summary.get("items", [])),
            "last_touch_conversions": sum(int(item.get("role_mix", {}).get("last_touch_conversions") or 0) for item in channel_lag_summary.get("items", [])),
        }
        campaign_lag_role_totals = {
            "first_touch_conversions": sum(int(item.get("role_mix", {}).get("first_touch_conversions") or 0) for item in campaign_lag_summary.get("items", [])),
            "assist_conversions": sum(int(item.get("role_mix", {}).get("assist_conversions") or 0) for item in campaign_lag_summary.get("items", [])),
            "last_touch_conversions": sum(int(item.get("role_mix", {}).get("last_touch_conversions") or 0) for item in campaign_lag_summary.get("items", [])),
        }
        channel_diagnostic_role_totals = _role_totals_from_diagnostics(channel_scope_diagnostics)
        campaign_diagnostic_role_totals = _role_totals_from_diagnostics(campaign_scope_diagnostics)

        conversion_path_scoped = _date_scoped_conversion_path_totals(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
        )
        journey_path_daily_scoped = _date_scoped_journey_path_daily_totals(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
        )
        journey_path_daily_scoped_active_definition = _date_scoped_journey_path_daily_totals(
            db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            journey_definition_id=active_definition_id,
        )

        conversion_path_total = db.query(func.count()).select_from(ConversionPath).scalar() or 0
        conversion_path_converted = (
            db.query(func.count())
            .select_from(ConversionPath)
            .filter(ConversionPath.conversion_key.isnot(None))
            .scalar()
            or 0
        )
        journey_path_daily_total = db.query(func.sum(JourneyPathDaily.count_conversions)).scalar() or 0
        journey_path_daily_by_definition = []
        for definition_id, name, definition_conversion_key in db.query(
            JourneyDefinition.id,
            JourneyDefinition.name,
            JourneyDefinition.conversion_kpi_id,
        ).all():
            total = (
                db.query(func.sum(JourneyPathDaily.count_conversions))
                .filter(JourneyPathDaily.journey_definition_id == definition_id)
                .scalar()
                or 0
            )
            journey_path_daily_by_definition.append(
                {
                    "journey_definition_id": definition_id,
                    "journey_definition_name": name,
                    "count_conversions": int(total),
                    "gross_revenue": round(
                        float(
                            db.query(func.sum(JourneyPathDaily.gross_revenue_total))
                            .filter(JourneyPathDaily.journey_definition_id == definition_id)
                            .scalar()
                            or 0.0
                        ),
                        2,
                    ),
                    "net_revenue": round(
                        float(
                            db.query(func.sum(JourneyPathDaily.net_revenue_total))
                            .filter(JourneyPathDaily.journey_definition_id == definition_id)
                            .scalar()
                            or 0.0
                        ),
                        2,
                    ),
                    "conversion_kpi_id": definition_conversion_key,
                }
            )
        excluded_non_primary_definitions = [
            item
            for item in journey_path_daily_by_definition
            if item["count_conversions"] > 0
            and item["journey_definition_id"] != active_definition_id
            and (conversion_key is None or item.get("conversion_kpi_id") != conversion_key)
        ]

        converted_journeys = _converted_journey_count(journeys)
        converted_journeys_for_model = _converted_journey_count(journeys_for_model)
        report = {
            "scope": {
                "date_from": date_from,
                "date_to": date_to,
                "timezone": timezone,
                "model": model,
                "config_id": meta.get("config_id") if meta else None,
                "conversion_key": conversion_key,
                "min_journey_quality_score": int(getattr(SETTINGS.attribution, "min_journey_quality_score", 0) or 0),
            },
            "surfaces": _surface_basis_registry(
                config_id=meta.get("config_id") if meta else config_id,
                conversion_key=conversion_key,
                active_definition_id=active_definition_id,
                conversion_paths_source=conversion_paths_analysis.get("source"),
                live_path_journeys=len(live_journeys_for_archetypes),
            ),
            "audience_modes": _audience_mode_registry(),
            "counts": {
                "journeys_loaded": len(journeys),
                "journeys_converted": converted_journeys,
                "journeys_for_model": len(journeys_for_model),
                "journeys_for_model_converted": converted_journeys_for_model,
                "attribution_total_conversions": int(attribution_result.get("total_conversions", 0) or 0),
                "attribution_effective_conversions": float(attribution_result.get("effective_conversions", 0.0) or 0.0),
                "campaign_total_conversions": int(campaign_result.get("total_conversions", 0) or 0),
                "overview_conversions_tile": int(overview_conversions or 0),
                "overview_spend_tile": round(overview_spend, 2),
                "overview_visits_tile": overview_visits,
                "overview_drivers_conversions": int(drivers_conversions),
                "overview_funnels_conversions": funnels_conversions,
                "overview_trends_conversions": trends_conversions,
                "overview_revenue_tile": round(overview_revenue, 2),
                "overview_net_revenue_tile": round(overview_net_revenue, 2),
                "overview_funnels_gross_revenue": round(funnels_gross_revenue, 2),
                "overview_funnels_net_revenue": round(funnels_net_revenue, 2),
                "overview_trends_revenue": round(trends_revenue, 2),
                "channel_summary_spend": round(float(channel_totals.get("spend", 0.0) or 0.0), 2),
                "channel_summary_visits": int(float(channel_totals.get("visits", 0.0) or 0.0)),
                "channel_summary_conversions": int(float(channel_totals.get("conversions", 0.0) or 0.0)),
                "channel_summary_revenue": round(float(channel_totals.get("revenue", 0.0) or 0.0), 2),
                "campaign_summary_spend": round(float(campaign_totals.get("spend", 0.0) or 0.0), 2),
                "campaign_summary_visits": int(float(campaign_totals.get("visits", 0.0) or 0.0)),
                "campaign_summary_conversions": int(float(campaign_totals.get("conversions", 0.0) or 0.0)),
                "campaign_summary_revenue": round(float(campaign_totals.get("revenue", 0.0) or 0.0), 2),
                "channel_lag_scope_rows": int(channel_lag_summary.get("summary", {}).get("conversions") or 0),
                "channel_lag_median_days_from_first_touch": channel_lag_summary.get("summary", {}).get("median_days_from_first_touch"),
                "channel_lag_p90_days_from_first_touch": channel_lag_summary.get("summary", {}).get("p90_days_from_first_touch"),
                "channel_lag_long_lag_share_over_7d": channel_lag_summary.get("summary", {}).get("long_lag_share_over_7d"),
                "channel_lag_first_touch_conversions": channel_lag_role_totals["first_touch_conversions"],
                "channel_lag_assist_conversions": channel_lag_role_totals["assist_conversions"],
                "channel_lag_last_touch_conversions": channel_lag_role_totals["last_touch_conversions"],
                "campaign_lag_scope_rows": int(campaign_lag_summary.get("summary", {}).get("conversions") or 0),
                "campaign_lag_median_days_from_first_touch": campaign_lag_summary.get("summary", {}).get("median_days_from_first_touch"),
                "campaign_lag_p90_days_from_first_touch": campaign_lag_summary.get("summary", {}).get("p90_days_from_first_touch"),
                "campaign_lag_long_lag_share_over_7d": campaign_lag_summary.get("summary", {}).get("long_lag_share_over_7d"),
                "campaign_lag_first_touch_conversions": campaign_lag_role_totals["first_touch_conversions"],
                "campaign_lag_assist_conversions": campaign_lag_role_totals["assist_conversions"],
                "campaign_lag_last_touch_conversions": campaign_lag_role_totals["last_touch_conversions"],
                "channel_scope_diagnostics_first_touch_conversions": channel_diagnostic_role_totals["first_touch_conversions"],
                "channel_scope_diagnostics_assist_conversions": channel_diagnostic_role_totals["assist_conversions"],
                "channel_scope_diagnostics_last_touch_conversions": channel_diagnostic_role_totals["last_touch_conversions"],
                "campaign_scope_diagnostics_first_touch_conversions": campaign_diagnostic_role_totals["first_touch_conversions"],
                "campaign_scope_diagnostics_assist_conversions": campaign_diagnostic_role_totals["assist_conversions"],
                "campaign_scope_diagnostics_last_touch_conversions": campaign_diagnostic_role_totals["last_touch_conversions"],
                "conversion_paths_total_rows": int(conversion_path_total),
                "conversion_paths_rows_with_conversion_key": int(conversion_path_converted),
                "journey_path_daily_total_conversions": int(journey_path_daily_total),
                "conversion_paths_scoped_count_conversions": int(conversion_path_scoped["count_conversions"]),
                "conversion_paths_scoped_gross_conversions": round(conversion_path_scoped["gross_conversions"], 2),
                "conversion_paths_scoped_net_conversions": round(conversion_path_scoped["net_conversions"], 2),
                "conversion_paths_scoped_gross_revenue": round(conversion_path_scoped["gross_revenue"], 2),
                "conversion_paths_scoped_net_revenue": round(conversion_path_scoped["net_revenue"], 2),
                "journey_path_daily_scoped_count_conversions": int(journey_path_daily_scoped["count_conversions"]),
                "journey_path_daily_scoped_gross_conversions": round(journey_path_daily_scoped["gross_conversions"], 2),
                "journey_path_daily_scoped_net_conversions": round(journey_path_daily_scoped["net_conversions"], 2),
                "journey_path_daily_scoped_gross_revenue": round(journey_path_daily_scoped["gross_revenue"], 2),
                "journey_path_daily_scoped_net_revenue": round(journey_path_daily_scoped["net_revenue"], 2),
                "active_overview_journey_definition_id": active_definition_id,
                "journey_path_daily_active_definition_scoped_count_conversions": int(
                    journey_path_daily_scoped_active_definition["count_conversions"]
                ),
                "journey_path_daily_active_definition_scoped_gross_conversions": round(
                    journey_path_daily_scoped_active_definition["gross_conversions"], 2
                ),
                "journey_path_daily_active_definition_scoped_net_conversions": round(
                    journey_path_daily_scoped_active_definition["net_conversions"], 2
                ),
                "journey_path_daily_active_definition_scoped_gross_revenue": round(
                    journey_path_daily_scoped_active_definition["gross_revenue"], 2
                ),
                "journey_path_daily_active_definition_scoped_net_revenue": round(
                    journey_path_daily_scoped_active_definition["net_revenue"], 2
                ),
                "conversion_paths_analysis_source": conversion_paths_analysis.get("source"),
                "conversion_paths_analysis_definition_id": conversion_paths_analysis.get("journey_definition_id"),
                "conversion_paths_analysis_total_journeys": int(conversion_paths_analysis.get("total_journeys") or 0),
                "path_archetypes_live_journeys_in_window": len(live_journeys_for_archetypes),
            },
            "journey_path_daily_by_definition": journey_path_daily_by_definition,
            "notes": [],
        }
        scoped_conversion_baseline = int(conversion_path_scoped["count_conversions"])
        checks = {
            "overview_tile_matches_scoped_conversion_paths": int(overview_conversions or 0) == scoped_conversion_baseline,
            "overview_drivers_match_scoped_conversion_paths": int(drivers_conversions) == scoped_conversion_baseline,
            "overview_funnels_match_scoped_conversion_paths": funnels_conversions == scoped_conversion_baseline,
            "overview_trends_match_scoped_conversion_paths": int(trends_conversions) == scoped_conversion_baseline,
            "attribution_matches_model_set": int(attribution_result.get("total_conversions", 0) or 0) == converted_journeys_for_model,
            "campaign_matches_model_set": int(campaign_result.get("total_conversions", 0) or 0) == converted_journeys_for_model,
            "active_definition_path_daily_count_matches_scoped_conversion_paths": (
                None
                if not active_definition_id
                else int(journey_path_daily_scoped_active_definition["count_conversions"])
                == int(conversion_path_scoped["count_conversions"])
            ),
            "active_definition_path_daily_gross_revenue_matches_scoped_conversion_paths": (
                None
                if not active_definition_id
                else round(journey_path_daily_scoped_active_definition["gross_revenue"], 2)
                == round(conversion_path_scoped["gross_revenue"], 2)
            ),
            "active_definition_path_daily_net_revenue_matches_scoped_conversion_paths": (
                None
                if not active_definition_id
                else round(journey_path_daily_scoped_active_definition["net_revenue"], 2)
                == round(conversion_path_scoped["net_revenue"], 2)
            ),
            "overview_revenue_matches_scoped_conversion_paths": round(overview_revenue, 2) == round(conversion_path_scoped["gross_revenue"], 2),
            "overview_net_revenue_matches_scoped_conversion_paths": round(overview_net_revenue, 2) == round(conversion_path_scoped["net_revenue"], 2),
            "overview_funnels_revenue_matches_scoped_conversion_paths": round(funnels_gross_revenue, 2) == round(conversion_path_scoped["gross_revenue"], 2),
            "overview_funnels_net_revenue_matches_scoped_conversion_paths": round(funnels_net_revenue, 2) == round(conversion_path_scoped["net_revenue"], 2),
            "overview_trends_revenue_matches_scoped_conversion_paths": round(trends_revenue, 2) == round(conversion_path_scoped["gross_revenue"], 2),
            "channel_summary_spend_matches_overview": round(float(channel_totals.get("spend", 0.0) or 0.0), 2) == round(overview_spend, 2),
            "channel_summary_visits_matches_overview": int(float(channel_totals.get("visits", 0.0) or 0.0)) == int(overview_visits),
            "channel_summary_conversions_matches_overview": int(float(channel_totals.get("conversions", 0.0) or 0.0)) == int(overview_conversions or 0),
            "channel_summary_revenue_matches_overview": round(float(channel_totals.get("revenue", 0.0) or 0.0), 2) == round(overview_revenue, 2),
            "campaign_summary_spend_matches_channel_summary": round(float(campaign_totals.get("spend", 0.0) or 0.0), 2) == round(float(channel_totals.get("spend", 0.0) or 0.0), 2),
            "campaign_summary_visits_matches_channel_summary": int(float(campaign_totals.get("visits", 0.0) or 0.0)) == int(float(channel_totals.get("visits", 0.0) or 0.0)),
            "campaign_summary_conversions_matches_channel_summary": int(float(campaign_totals.get("conversions", 0.0) or 0.0)) == int(float(channel_totals.get("conversions", 0.0) or 0.0)),
            "campaign_summary_revenue_matches_channel_summary": round(float(campaign_totals.get("revenue", 0.0) or 0.0), 2) == round(float(channel_totals.get("revenue", 0.0) or 0.0), 2),
            "channel_lag_scope_rows_match_campaign_lag": int(channel_lag_summary.get("summary", {}).get("conversions") or 0) == int(campaign_lag_summary.get("summary", {}).get("conversions") or 0),
            "channel_lag_median_matches_campaign_lag": channel_lag_summary.get("summary", {}).get("median_days_from_first_touch") == campaign_lag_summary.get("summary", {}).get("median_days_from_first_touch"),
            "channel_lag_p90_matches_campaign_lag": channel_lag_summary.get("summary", {}).get("p90_days_from_first_touch") == campaign_lag_summary.get("summary", {}).get("p90_days_from_first_touch"),
            "channel_lag_long_share_matches_campaign_lag": channel_lag_summary.get("summary", {}).get("long_lag_share_over_7d") == campaign_lag_summary.get("summary", {}).get("long_lag_share_over_7d"),
            "channel_lag_first_touch_totals_match_campaign_lag": channel_lag_role_totals["first_touch_conversions"] == campaign_lag_role_totals["first_touch_conversions"],
            "channel_lag_assist_totals_match_campaign_lag": channel_lag_role_totals["assist_conversions"] == campaign_lag_role_totals["assist_conversions"],
            "channel_lag_last_touch_totals_match_campaign_lag": channel_lag_role_totals["last_touch_conversions"] == campaign_lag_role_totals["last_touch_conversions"],
            "channel_scope_diagnostics_role_parity_applicable": None,
            "campaign_scope_diagnostics_role_parity_applicable": None,
            "conversion_paths_analysis_source_is_supported": conversion_paths_analysis.get("source") in {"journey_paths_daily", "journey_definition_facts"},
            "path_analysis_cross_page_count_parity_applicable": None,
        }
        report["checks"] = checks
        if not active_definition_id and excluded_non_primary_definitions:
            report["notes"].append(
                "No single active overview journey definition matched the scoped conversion key; definition-level path checks were skipped."
            )
        if active_definition_id and excluded_non_primary_definitions:
            formatted = ", ".join(
                f"{item['journey_definition_name']} ({item['count_conversions']} conversions)"
                for item in excluded_non_primary_definitions
            )
            report["notes"].append(
                "Non-primary journey definitions with conversions remain in path-daily storage and are excluded from the scoped overview baseline: "
                + formatted
                + "."
            )
        elif not active_definition_id and journey_path_daily_by_definition:
            formatted = ", ".join(
                f"{item['journey_definition_name']} ({item['count_conversions']} conversions)"
                for item in journey_path_daily_by_definition
                if item["count_conversions"] > 0
            )
            report["notes"].append(
                "Journey path-daily storage contains multiple active definition scopes, so scoped overview reconciliation falls back to conversion-path rows instead of a single journey-definition baseline: "
                + formatted
                + "."
            )
        if meta and meta.get("config_id") and converted_journeys_for_model == 0 and int(overview_conversions or 0) > 0:
            report["notes"].append(
                "The selected model config currently filters all live attribution journeys out of the tested scope, while workspace fact-based pages still show observed conversions. Treat live config-aware pages and workspace fact-based pages as different comparison groups until that contract is reconciled or labeled in the UI."
            )
        report["notes"].append(
            "Lag parity checks are workspace-wide diagnostic-fact checks and compare channel vs campaign lag summaries on the same raw scope-diagnostic basis."
        )
        report["notes"].append(
            "Channel Performance and Campaign Performance are mixed-basis pages: summary and trend panels now follow the selected model config, while lag panels still read workspace diagnostic facts because lag storage is not config-scoped."
        )
        report["notes"].append(
            "Incrementality is a workspace-observed planning surface: the selected config is recorded on new experiments for provenance, but the planner channel and KPI baseline remain derived from workspace journey observations."
        )
        report["notes"].append(
            "Scope diagnostics and lag summaries intentionally use different row-window contracts: scope diagnostics includes journeys overlapping the selected window, while lag summaries include conversions whose conversion_ts falls inside the window. Their role totals are reported for context but not enforced as hard parity checks."
        )
        report["notes"].append(
            "Conversion Paths and Path Archetypes intentionally use different bases: Conversion Paths reads selected journey-definition outputs "
            f"({conversion_paths_analysis.get('source') or 'unknown source'}) while Path Archetypes clusters live attribution journeys "
            f"({len(live_journeys_for_archetypes)} journeys in the selected window after model-config filtering). Cross-page path count parity is therefore documented, not enforced as a hard equality check."
        )
        if conversion_paths_analysis.get("source") == "journey_definition_facts":
            report["notes"].append(
                "Conversion Paths is currently falling back to journey_definition_facts instead of journey_paths_daily for the selected period, so materialized-path parity should be treated as lower-confidence until daily outputs are present."
            )
        report["status"] = "ok" if all(value is not False for value in checks.values()) else "warning"
        return report


def main() -> int:
    default_from, default_to = _default_date_bounds()
    parser = argparse.ArgumentParser(description="Audit metric consistency across overview, attribution, and path stores.")
    parser.add_argument("--date-from", default=default_from)
    parser.add_argument("--date-to", default=default_to)
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--model", default="linear")
    parser.add_argument("--config-id", default=None)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    report = build_consistency_audit(
        date_from=args.date_from,
        date_to=args.date_to,
        timezone=args.timezone,
        model=args.model,
        config_id=args.config_id,
    )
    if args.as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"status: {report['status']}")
        print(f"scope: {report['scope']}")
        print("counts:")
        for key, value in report["counts"].items():
            print(f"  {key}: {value}")
        print("checks:")
        for key, value in report["checks"].items():
            print(f"  {key}: {'PASS' if value else 'FAIL'}")
        print("surfaces:")
        for key, value in report["surfaces"].items():
            print(f"  {key}: {value['basis_type']} ({value['config_behavior']})")
        print("audience_modes:")
        for key, value in report["audience_modes"].items():
            print(f"  {key}: exact={value['exact_filter_mode']} | lens={value['analytical_lens_mode']}")
        if report["journey_path_daily_by_definition"]:
            print("journey_path_daily_by_definition:")
            for item in report["journey_path_daily_by_definition"]:
                print(f"  {item['journey_definition_name']}: {item['count_conversions']}")
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
