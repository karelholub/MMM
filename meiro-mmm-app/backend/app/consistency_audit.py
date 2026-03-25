from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import func

from app.attribution_engine import run_attribution, run_attribution_campaign
from app.main import SETTINGS, SessionLocal
from app.models_config_dq import ConversionPath, JourneyDefinition, JourneyPathDaily
from app.services_conversions import (
    apply_model_config_to_journeys,
    filter_journeys_by_quality,
    load_journeys_from_db,
)
from app.services_overview import (
    get_overview_drivers,
    get_overview_funnels,
    get_overview_summary,
    get_overview_trend_insights,
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


def build_consistency_audit(
    *,
    date_from: str,
    date_to: str,
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

        overview_summary = get_overview_summary(db, date_from=date_from, date_to=date_to)
        overview_drivers = get_overview_drivers(db, date_from=date_from, date_to=date_to)
        overview_funnels = get_overview_funnels(db, date_from=date_from, date_to=date_to)
        overview_trends = get_overview_trend_insights(db, date_from=date_from, date_to=date_to)

        overview_conversions = next(
            (tile["value"] for tile in overview_summary.get("kpi_tiles", []) if tile.get("kpi_key") == "conversions"),
            0,
        )
        drivers_conversions = sum(int(item.get("conversions") or 0) for item in overview_drivers.get("by_channel", []))
        funnels_conversions = int((overview_funnels.get("summary") or {}).get("total_conversions") or 0)
        trends_conversions = float((((overview_trends.get("decomposition") or {}).get("current") or {}).get("conversions") or 0.0))

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
        for definition_id, name in db.query(JourneyDefinition.id, JourneyDefinition.name).all():
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
                }
            )

        converted_journeys = _converted_journey_count(journeys)
        converted_journeys_for_model = _converted_journey_count(journeys_for_model)
        report = {
            "scope": {
                "date_from": date_from,
                "date_to": date_to,
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
                "overview_drivers_conversions": int(drivers_conversions),
                "overview_funnels_conversions": funnels_conversions,
                "overview_trends_conversions": trends_conversions,
                "conversion_paths_total_rows": int(conversion_path_total),
                "conversion_paths_rows_with_conversion_key": int(conversion_path_converted),
                "journey_path_daily_total_conversions": int(journey_path_daily_total),
            },
            "journey_path_daily_by_definition": journey_path_daily_by_definition,
        }
        baseline = converted_journeys
        checks = {
            "overview_tile_matches_converted_journeys": int(overview_conversions or 0) == baseline,
            "overview_drivers_match_converted_journeys": int(drivers_conversions) == baseline,
            "overview_funnels_match_converted_journeys": funnels_conversions == baseline,
            "overview_trends_match_converted_journeys": int(trends_conversions) == baseline,
            "attribution_matches_model_set": int(attribution_result.get("total_conversions", 0) or 0) == converted_journeys_for_model,
            "campaign_matches_model_set": int(campaign_result.get("total_conversions", 0) or 0) == converted_journeys_for_model,
            "path_daily_matches_converted_conversion_paths": int(journey_path_daily_total) == int(conversion_path_converted),
        }
        report["checks"] = checks
        report["status"] = "ok" if all(checks.values()) else "warning"
        return report


def main() -> int:
    default_from, default_to = _default_date_bounds()
    parser = argparse.ArgumentParser(description="Audit metric consistency across overview, attribution, and path stores.")
    parser.add_argument("--date-from", default=default_from)
    parser.add_argument("--date-to", default=default_to)
    parser.add_argument("--model", default="linear")
    parser.add_argument("--config-id", default=None)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    report = build_consistency_audit(
        date_from=args.date_from,
        date_to=args.date_to,
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
