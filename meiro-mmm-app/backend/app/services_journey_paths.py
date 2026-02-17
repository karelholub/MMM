"""Read APIs for journey path aggregates."""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from .models_config_dq import JourneyPathDaily


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
        conversion_rate = (count_conversions / count_journeys) if count_journeys > 0 else 0.0
        items.append(
            {
                "path_hash": row.path_hash,
                "path_steps": row.path_steps,
                "path_length": int(row.path_length or 0),
                "count_journeys": count_journeys,
                "count_conversions": count_conversions,
                "conversion_rate": conversion_rate,
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
        "page": page,
        "limit": limit,
        "mode": mode,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
    }
