from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from .models_config_dq import JourneyDefinition, JourneyTransitionDaily, JourneyTransitionFact
from .services_journey_transition_facts import iter_journey_transition_rows
from .utils.meiro_config import campaign_label_matches_target_site_scope, get_out_of_scope_campaign_labels, site_scope_is_strict


def _exclude_out_of_scope_campaigns(q, column):
    if not site_scope_is_strict():
        return q
    labels = get_out_of_scope_campaign_labels()
    if not labels:
        return q
    return q.filter(or_(column.is_(None), column == "", func.lower(func.trim(column)).notin_(labels)))


def _row_campaign_in_scope(row) -> bool:
    return campaign_label_matches_target_site_scope(getattr(row, "campaign_id", None), allow_unknown=True)


def can_use_transition_fact_fallback(db: Session, *, journey_definition: JourneyDefinition) -> bool:
    conversion_key = str(journey_definition.conversion_kpi_id or "").strip()
    if not conversion_key:
        return False
    matching_definition_ids = [
        str(definition_id or "")
        for (definition_id,) in db.query(JourneyDefinition.id)
        .filter(
            JourneyDefinition.is_archived == False,  # noqa: E712
            JourneyDefinition.conversion_kpi_id == conversion_key,
        )
        .all()
    ]
    return matching_definition_ids == [journey_definition.id]


def _daily_transition_query(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    from_step: Optional[str] = None,
    to_step: Optional[str] = None,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
):
    q = db.query(JourneyTransitionDaily).filter(
        JourneyTransitionDaily.journey_definition_id == journey_definition_id,
        JourneyTransitionDaily.date >= date_from,
        JourneyTransitionDaily.date <= date_to,
    )
    if from_step:
        q = q.filter(JourneyTransitionDaily.from_step == from_step)
    if to_step:
        q = q.filter(JourneyTransitionDaily.to_step == to_step)
    if channel_group:
        q = q.filter(JourneyTransitionDaily.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyTransitionDaily.campaign_id == campaign_id)
    q = _exclude_out_of_scope_campaigns(q, JourneyTransitionDaily.campaign_id)
    if device:
        q = q.filter(JourneyTransitionDaily.device == device)
    if country:
        q = q.filter(JourneyTransitionDaily.country == country)
    return q


def list_transition_edges_from_outputs(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    q = (
        db.query(
            JourneyTransitionDaily.from_step.label("from_step"),
            JourneyTransitionDaily.to_step.label("to_step"),
            func.sum(JourneyTransitionDaily.count_transitions).label("count_transitions"),
            func.sum(JourneyTransitionDaily.count_profiles).label("count_profiles"),
            func.sum(JourneyTransitionDaily.avg_time_between_sec * JourneyTransitionDaily.count_transitions).label("weighted_avg_time_between_sec"),
            func.sum(JourneyTransitionDaily.p50_time_between_sec * JourneyTransitionDaily.count_transitions).label("weighted_p50_time_between_sec"),
            func.sum(JourneyTransitionDaily.p90_time_between_sec * JourneyTransitionDaily.count_transitions).label("weighted_p90_time_between_sec"),
        )
        .filter(
            JourneyTransitionDaily.journey_definition_id == journey_definition.id,
            JourneyTransitionDaily.date >= date_from,
            JourneyTransitionDaily.date <= date_to,
        )
        .group_by(JourneyTransitionDaily.from_step, JourneyTransitionDaily.to_step)
    )
    if channel_group:
        q = q.filter(JourneyTransitionDaily.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyTransitionDaily.campaign_id == campaign_id)
    q = _exclude_out_of_scope_campaigns(q, JourneyTransitionDaily.campaign_id)
    if device:
        q = q.filter(JourneyTransitionDaily.device == device)
    if country:
        q = q.filter(JourneyTransitionDaily.country == country)
    rows = q.all()
    if rows:
        return (
            [
                {
                    "from_step": str(row.from_step),
                    "to_step": str(row.to_step),
                    "count_transitions": int(row.count_transitions or 0),
                    "count_profiles": int(row.count_profiles or 0),
                    "avg_time_between_sec": (
                        round(float(row.weighted_avg_time_between_sec or 0.0) / float(row.count_transitions or 1), 2)
                        if int(row.count_transitions or 0) > 0 and row.weighted_avg_time_between_sec is not None
                        else None
                    ),
                    "p50_time_between_sec": (
                        round(float(row.weighted_p50_time_between_sec or 0.0) / float(row.count_transitions or 1), 2)
                        if int(row.count_transitions or 0) > 0 and row.weighted_p50_time_between_sec is not None
                        else None
                    ),
                    "p90_time_between_sec": (
                        round(float(row.weighted_p90_time_between_sec or 0.0) / float(row.count_transitions or 1), 2)
                        if int(row.count_transitions or 0) > 0 and row.weighted_p90_time_between_sec is not None
                        else None
                    ),
                }
                for row in rows
                if int(row.count_transitions or 0) > 0
            ],
            "aggregates",
        )

    if not can_use_transition_fact_fallback(db, journey_definition=journey_definition):
        return [], "none"

    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    buckets: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in iter_journey_transition_rows(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=journey_definition.conversion_kpi_id,
    ):
        if not _row_campaign_in_scope(row):
            continue
        if channel_group and str(row.channel_group or "") != channel_group:
            continue
        if campaign_id and str(row.campaign_id or "") != campaign_id:
            continue
        if device and str(row.device or "") != device:
            continue
        if country and str(row.country or "") != country:
            continue
        key = (str(row.from_step or ""), str(row.to_step or ""))
        bucket = buckets.setdefault(
            key,
            {
                "from_step": key[0],
                "to_step": key[1],
                "count_transitions": 0,
                "profiles": set(),
                "time_values": [],
            },
        )
        bucket["count_transitions"] += 1
        if row.profile_id:
            bucket["profiles"].add(str(row.profile_id))
        if isinstance(row.delta_sec, (int, float)) and float(row.delta_sec) >= 0:
            bucket["time_values"].append(float(row.delta_sec))

    return (
        [
            {
                "from_step": payload["from_step"],
                "to_step": payload["to_step"],
                "count_transitions": int(payload["count_transitions"]),
                "count_profiles": len(payload["profiles"]),
                "avg_time_between_sec": round(sum(payload["time_values"]) / len(payload["time_values"]), 2) if payload["time_values"] else None,
                "p50_time_between_sec": None,
                "p90_time_between_sec": None,
            }
            for payload in buckets.values()
            if int(payload["count_transitions"] or 0) > 0
        ],
        "transition_facts" if buckets else "none",
    )


def compute_transition_pair_counts_from_outputs(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    from_step: str,
    to_step: str,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Tuple[float, float]:
    base_q = _daily_transition_query(
        db,
        journey_definition_id=journey_definition.id,
        date_from=date_from,
        date_to=date_to,
        from_step=from_step,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    )
    denom = float(base_q.with_entities(func.sum(JourneyTransitionDaily.count_transitions)).scalar() or 0.0)
    numer = float(
        base_q.filter(JourneyTransitionDaily.to_step == to_step)
        .with_entities(func.sum(JourneyTransitionDaily.count_transitions))
        .scalar()
        or 0.0
    )
    if denom > 0:
        return denom, numer

    if not can_use_transition_fact_fallback(db, journey_definition=journey_definition):
        return 0.0, 0.0

    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    denom = 0.0
    numer = 0.0
    for row in iter_journey_transition_rows(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=journey_definition.conversion_kpi_id,
    ):
        if not _row_campaign_in_scope(row):
            continue
        if str(row.from_step or "") != from_step:
            continue
        if channel_group and str(row.channel_group or "") != channel_group:
            continue
        if campaign_id and str(row.campaign_id or "") != campaign_id:
            continue
        if device and str(row.device or "") != device:
            continue
        if country and str(row.country or "").lower() != country.lower():
            continue
        denom += 1.0
        if str(row.to_step or "") == to_step:
            numer += 1.0
    return denom, numer


def list_transition_breakdowns_from_outputs(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    from_step: str,
    to_step: str,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    base_q = _daily_transition_query(
        db,
        journey_definition_id=journey_definition.id,
        date_from=date_from,
        date_to=date_to,
        from_step=from_step,
        to_step=to_step,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    )
    if base_q.limit(1).first() is not None:
        device_rows = (
            base_q.with_entities(
                JourneyTransitionDaily.device,
                func.sum(JourneyTransitionDaily.count_profiles),
            )
            .group_by(JourneyTransitionDaily.device)
            .all()
        )
        channel_rows = (
            base_q.with_entities(
                JourneyTransitionDaily.channel_group,
                func.sum(JourneyTransitionDaily.count_profiles),
            )
            .group_by(JourneyTransitionDaily.channel_group)
            .all()
        )
        return (
            [{"key": str(key or "unknown"), "count": int(value or 0)} for key, value in device_rows if int(value or 0) > 0][:5],
            [{"key": str(key or "unknown"), "count": int(value or 0)} for key, value in channel_rows if int(value or 0) > 0][:5],
            "aggregates",
        )

    if not can_use_transition_fact_fallback(db, journey_definition=journey_definition):
        return [], [], "none"

    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    by_device: Dict[str, int] = {}
    by_channel: Dict[str, int] = {}
    for row in iter_journey_transition_rows(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=journey_definition.conversion_kpi_id,
    ):
        if not _row_campaign_in_scope(row):
            continue
        if str(row.from_step or "") != from_step or str(row.to_step or "") != to_step:
            continue
        if channel_group and str(row.channel_group or "") != channel_group:
            continue
        if campaign_id and str(row.campaign_id or "") != campaign_id:
            continue
        if device and str(row.device or "") != device:
            continue
        if country and str(row.country or "").lower() != country.lower():
            continue
        by_device[str(row.device or "unknown")] = by_device.get(str(row.device or "unknown"), 0) + 1
        by_channel[str(row.channel_group or "unknown")] = by_channel.get(str(row.channel_group or "unknown"), 0) + 1
    return (
        [{"key": key, "count": value} for key, value in sorted(by_device.items(), key=lambda item: -item[1])[:5]],
        [{"key": key, "count": value} for key, value in sorted(by_channel.items(), key=lambda item: -item[1])[:5]],
        "transition_facts" if by_device or by_channel else "none",
    )


def count_recent_transition_outputs(
    db: Session,
    *,
    date_from: date,
    date_to: date,
) -> int:
    daily_count = int(
        db.query(func.count(JourneyTransitionDaily.id))
        .filter(JourneyTransitionDaily.date >= date_from, JourneyTransitionDaily.date <= date_to)
        .scalar()
        or 0
    )
    if daily_count > 0:
        return daily_count
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    return int(
        db.query(func.count(JourneyTransitionFact.id))
        .filter(JourneyTransitionFact.conversion_ts >= start_dt, JourneyTransitionFact.conversion_ts < end_dt)
        .scalar()
        or 0
    )


def count_recent_transition_from_steps(
    db: Session,
    *,
    date_from: date,
    date_to: date,
) -> int:
    daily_count = int(
        db.query(func.count(func.distinct(JourneyTransitionDaily.from_step)))
        .filter(JourneyTransitionDaily.date >= date_from, JourneyTransitionDaily.date <= date_to)
        .scalar()
        or 0
    )
    if daily_count > 0:
        return daily_count
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    return int(
        db.query(func.count(func.distinct(JourneyTransitionFact.from_step)))
        .filter(JourneyTransitionFact.conversion_ts >= start_dt, JourneyTransitionFact.conversion_ts < end_dt)
        .scalar()
        or 0
    )
