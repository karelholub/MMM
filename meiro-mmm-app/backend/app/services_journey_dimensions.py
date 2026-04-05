from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Query, Session

from .models_config_dq import JourneyDefinitionInstanceFact


def _apply_dimension_filters(
    query: Query,
    *,
    definition_id: str,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    exclude_dimension: Optional[str] = None,
) -> Query:
    query = query.filter(
        JourneyDefinitionInstanceFact.journey_definition_id == definition_id,
        JourneyDefinitionInstanceFact.date >= date_from,
        JourneyDefinitionInstanceFact.date <= date_to,
    )
    if channel_group and exclude_dimension != "channel_group":
        query = query.filter(JourneyDefinitionInstanceFact.channel_group == channel_group)
    if campaign_id and exclude_dimension != "campaign_id":
        query = query.filter(JourneyDefinitionInstanceFact.campaign_id == campaign_id)
    if device and exclude_dimension != "device":
        query = query.filter(JourneyDefinitionInstanceFact.device == device)
    if country and exclude_dimension != "country":
        query = query.filter(func.lower(JourneyDefinitionInstanceFact.country) == str(country).strip().lower())
    return query


def _top_dimension_values(
    db: Session,
    column: Any,
    *,
    definition_id: str,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    exclude_dimension: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = db.query(column, func.count(JourneyDefinitionInstanceFact.id).label("count"))
    query = _apply_dimension_filters(
        query,
        definition_id=definition_id,
        date_from=date_from,
        date_to=date_to,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        exclude_dimension=exclude_dimension,
    )
    rows = (
        query.filter(column.isnot(None))
        .filter(column != "")
        .group_by(column)
        .order_by(func.count(JourneyDefinitionInstanceFact.id).desc(), column.asc())
        .limit(max(1, min(limit, 500)))
        .all()
    )
    return [{"value": str(value), "count": int(count or 0)} for value, count in rows if str(value or "").strip()]


def build_journey_filter_dimensions(
    db: Session,
    *,
    definition_id: str,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Dict[str, Any]:
    total_rows = (
        _apply_dimension_filters(
            db.query(func.count(JourneyDefinitionInstanceFact.id)),
            definition_id=definition_id,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
        ).scalar()
        or 0
    )

    return {
        "summary": {
            "journey_rows": int(total_rows),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "segment_supported": False,
        },
        "channels": _top_dimension_values(
            db,
            JourneyDefinitionInstanceFact.channel_group,
            definition_id=definition_id,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            exclude_dimension="channel_group",
        ),
        "campaigns": _top_dimension_values(
            db,
            JourneyDefinitionInstanceFact.campaign_id,
            definition_id=definition_id,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            exclude_dimension="campaign_id",
        ),
        "devices": _top_dimension_values(
            db,
            JourneyDefinitionInstanceFact.device,
            definition_id=definition_id,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            exclude_dimension="device",
        ),
        "countries": _top_dimension_values(
            db,
            JourneyDefinitionInstanceFact.country,
            definition_id=definition_id,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            exclude_dimension="country",
        ),
        "segments": [],
    }
