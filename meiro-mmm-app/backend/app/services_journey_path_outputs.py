from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Query, Session

from .models_config_dq import JourneyDefinitionInstanceFact, JourneyPathDaily
from .services_journey_definition_facts import (
    compute_path_metric_from_definition_facts,
    has_definition_instance_facts,
    list_paths_from_definition_facts,
)


def query_path_daily_outputs(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    mode: str = "conversion_only",
    path_hash: Optional[str] = None,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Query:
    q = db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == journey_definition_id,
        JourneyPathDaily.date >= date_from,
        JourneyPathDaily.date <= date_to,
    )
    if path_hash:
        q = q.filter(JourneyPathDaily.path_hash == path_hash)
    if channel_group:
        q = q.filter(JourneyPathDaily.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyPathDaily.campaign_id == campaign_id)
    if device:
        q = q.filter(JourneyPathDaily.device == device)
    if country:
        q = q.filter(func.lower(JourneyPathDaily.country) == country.lower())
    if mode == "conversion_only":
        q = q.filter(JourneyPathDaily.count_conversions > 0)
    return q


def has_path_daily_outputs(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    mode: str = "conversion_only",
    path_hash: Optional[str] = None,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> bool:
    return (
        query_path_daily_outputs(
            db,
            journey_definition_id=journey_definition_id,
            date_from=date_from,
            date_to=date_to,
            mode=mode,
            path_hash=path_hash,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
        )
        .limit(1)
        .first()
        is not None
    )


def compute_path_metric_from_outputs(
    db: Session,
    *,
    journey_definition_id: str,
    metric: str,
    date_from: date,
    date_to: date,
    mode: str = "conversion_only",
    path_hash: Optional[str] = None,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Optional[float]:
    q = query_path_daily_outputs(
        db,
        journey_definition_id=journey_definition_id,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        path_hash=path_hash,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    )
    if q.limit(1).first() is None and has_definition_instance_facts(
        db,
        journey_definition_id=journey_definition_id,
        date_from=date_from,
        date_to=date_to,
    ):
        return compute_path_metric_from_definition_facts(
            db,
            journey_definition_id=journey_definition_id,
            metric=metric,
            date_from=date_from,
            date_to=date_to,
            path_hash=path_hash,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
        )

    sums = q.with_entities(
        func.sum(JourneyPathDaily.count_journeys),
        func.sum(JourneyPathDaily.count_conversions),
        func.sum(JourneyPathDaily.gross_conversions_total),
        func.sum(JourneyPathDaily.net_conversions_total),
        func.sum(JourneyPathDaily.gross_revenue_total),
        func.sum(JourneyPathDaily.net_revenue_total),
        func.sum((JourneyPathDaily.p50_time_to_convert_sec) * (JourneyPathDaily.count_conversions)),
    ).first()
    journeys = int(sums[0] or 0)
    conversions = int(sums[1] or 0)
    gross_conversions = float(sums[2] or 0.0)
    net_conversions = float(sums[3] or 0.0)
    gross_revenue = float(sums[4] or 0.0)
    net_revenue = float(sums[5] or 0.0)
    p50_weighted_sum = float(sums[6] or 0.0)
    if metric == "conversion_rate":
        return (float(conversions) / float(journeys)) if journeys > 0 else None
    if metric == "count_journeys":
        return float(journeys)
    if metric == "gross_conversions_total":
        return gross_conversions
    if metric == "net_conversions_total":
        return net_conversions
    if metric == "gross_revenue_total":
        return gross_revenue
    if metric == "net_revenue_total":
        return net_revenue
    if metric == "p50_time_to_convert_sec":
        return (p50_weighted_sum / float(conversions)) if conversions > 0 else None
    return None


def list_paths_from_outputs(
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
) -> Optional[Dict[str, Any]]:
    if has_path_daily_outputs(
        db,
        journey_definition_id=journey_definition_id,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    ):
        return None
    if not has_definition_instance_facts(
        db,
        journey_definition_id=journey_definition_id,
        date_from=date_from,
        date_to=date_to,
    ):
        return None
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


def count_recent_path_outputs(
    db: Session,
    *,
    date_from: date,
    date_to: date,
) -> int:
    daily_count = int(
        db.query(func.count(JourneyPathDaily.id))
        .filter(JourneyPathDaily.date >= date_from, JourneyPathDaily.date <= date_to)
        .scalar()
        or 0
    )
    if daily_count > 0:
        return daily_count
    return int(
        db.query(func.count(JourneyDefinitionInstanceFact.id))
        .filter(
            JourneyDefinitionInstanceFact.date >= date_from,
            JourneyDefinitionInstanceFact.date <= date_to,
        )
        .scalar()
        or 0
    )
