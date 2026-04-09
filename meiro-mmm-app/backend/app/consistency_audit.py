from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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
        if report["journey_path_daily_by_definition"]:
            print("journey_path_daily_by_definition:")
            for item in report["journey_path_daily_by_definition"]:
                print(f"  {item['journey_definition_name']}: {item['count_conversions']}")
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
