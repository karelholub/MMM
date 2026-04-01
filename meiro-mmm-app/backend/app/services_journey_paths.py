"""Read APIs for journey path aggregates."""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import JourneyPathDaily
from .services_journey_definition_facts import has_definition_instance_facts, list_paths_from_definition_facts


def list_paths_for_journey_definition(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    mode: str = "conversion_only",
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
) -> dict:
    q = db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == journey_definition_id,
        JourneyPathDaily.date >= date_from,
        JourneyPathDaily.date <= date_to,
    )
    if channel_group:
        q = q.filter(JourneyPathDaily.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyPathDaily.campaign_id == campaign_id)
    if device:
        q = q.filter(JourneyPathDaily.device == device)
    if country:
        q = q.filter(JourneyPathDaily.country == country)
    if mode == "conversion_only":
        q = q.filter(JourneyPathDaily.count_conversions > 0)

    if q.limit(1).first() is None and has_definition_instance_facts(
        db,
        journey_definition_id=journey_definition_id,
        date_from=date_from,
        date_to=date_to,
    ):
        return list_paths_from_definition_facts(
            db,
            journey_definition_id=journey_definition_id,
            date_from=date_from,
            date_to=date_to,
            mode=mode,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            page=page,
            limit=limit,
        )

    summary_row = q.with_entities(
        func.sum(JourneyPathDaily.count_journeys),
        func.sum(JourneyPathDaily.count_conversions),
        func.sum(JourneyPathDaily.gross_revenue_total),
        func.sum(JourneyPathDaily.net_revenue_total),
    ).first()
    total_journeys = int(summary_row[0] or 0)
    total_conversions = int(summary_row[1] or 0)
    gross_revenue_total = float(summary_row[2] or 0.0)
    net_revenue_total = float(summary_row[3] or 0.0)

    total = q.count()
    page = max(1, int(page))
    limit = max(1, min(int(limit), 200))
    rows = (
        q.order_by(JourneyPathDaily.count_journeys.desc(), JourneyPathDaily.path_hash.asc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    items = []
    for row in rows:
        count_journeys = int(row.count_journeys or 0)
        count_conversions = int(row.count_conversions or 0)
        gross_revenue = float(row.gross_revenue_total or 0.0)
        net_revenue = float(row.net_revenue_total or 0.0)
        conversion_rate = (count_conversions / count_journeys) if count_journeys > 0 else 0.0
        items.append(
            {
                "path_hash": row.path_hash,
                "path_steps": row.path_steps,
                "path_length": int(row.path_length or 0),
                "count_journeys": count_journeys,
                "count_conversions": count_conversions,
                "conversion_rate": conversion_rate,
                "gross_revenue": round(gross_revenue, 2),
                "net_revenue": round(net_revenue, 2),
                "gross_revenue_per_conversion": round((gross_revenue / count_conversions), 2) if count_conversions > 0 else 0.0,
                "net_revenue_per_conversion": round((net_revenue / count_conversions), 2) if count_conversions > 0 else 0.0,
                "avg_time_to_convert_sec": row.avg_time_to_convert_sec,
                "p50_time_to_convert_sec": row.p50_time_to_convert_sec,
                "p90_time_to_convert_sec": row.p90_time_to_convert_sec,
                "channel_group": row.channel_group,
                "campaign_id": row.campaign_id,
                "device": row.device,
                "country": row.country,
            }
        )

    return {
        "items": items,
        "total": total,
        "summary": {
            "count_journeys": total_journeys,
            "count_conversions": total_conversions,
            "conversion_rate": (total_conversions / total_journeys) if total_journeys > 0 else 0.0,
            "gross_revenue": round(gross_revenue_total, 2),
            "net_revenue": round(net_revenue_total, 2),
            "gross_revenue_per_conversion": round((gross_revenue_total / total_conversions), 2) if total_conversions > 0 else 0.0,
            "net_revenue_per_conversion": round((net_revenue_total / total_conversions), 2) if total_conversions > 0 else 0.0,
        },
        "page": page,
        "limit": limit,
        "mode": mode,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
    }
