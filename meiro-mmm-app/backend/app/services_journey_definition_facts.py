from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from .models_config_dq import JourneyDefinitionInstanceFact
from .utils.meiro_config import get_out_of_scope_campaign_labels, site_scope_is_strict


def _exclude_out_of_scope_campaigns(q):
    if not site_scope_is_strict():
        return q
    labels = get_out_of_scope_campaign_labels()
    if not labels:
        return q
    column = JourneyDefinitionInstanceFact.campaign_id
    return q.filter(or_(column.is_(None), column == "", func.lower(func.trim(column)).notin_(labels)))


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    q = max(0.0, min(1.0, q))
    idx = (len(ordered) - 1) * q
    lo = int(idx)
    hi = min(len(ordered) - 1, lo + 1)
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def build_journey_definition_instance_fact(
    *,
    journey_definition_id: str,
    conversion_id: str,
    profile_id: Optional[str],
    conversion_key: Optional[str],
    conversion_ts: datetime,
    path_hash: str,
    steps: List[str],
    channel_group: Optional[str],
    last_touch_channel: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
    interaction_path_type: Optional[str],
    time_to_convert_sec: Optional[float],
    gross_conversions_total: float,
    net_conversions_total: float,
    gross_revenue_total: float,
    net_revenue_total: float,
    created_at: datetime,
) -> JourneyDefinitionInstanceFact:
    return JourneyDefinitionInstanceFact(
        date=conversion_ts.date(),
        journey_definition_id=journey_definition_id,
        conversion_id=conversion_id,
        profile_id=profile_id,
        conversion_key=conversion_key,
        conversion_ts=conversion_ts,
        path_hash=path_hash,
        steps_json=list(steps),
        path_length=len(steps),
        channel_group=channel_group,
        last_touch_channel=last_touch_channel,
        campaign_id=campaign_id,
        device=device,
        country=country,
        interaction_path_type=interaction_path_type,
        time_to_convert_sec=time_to_convert_sec,
        gross_conversions_total=float(gross_conversions_total or 0.0),
        net_conversions_total=float(net_conversions_total or 0.0),
        gross_revenue_total=float(gross_revenue_total or 0.0),
        net_revenue_total=float(net_revenue_total or 0.0),
        created_at=created_at,
        updated_at=created_at,
    )


def _query_definition_instance_facts(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    path_hash: Optional[str] = None,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
):
    q = db.query(JourneyDefinitionInstanceFact).filter(
        JourneyDefinitionInstanceFact.journey_definition_id == journey_definition_id,
        JourneyDefinitionInstanceFact.date >= date_from,
        JourneyDefinitionInstanceFact.date <= date_to,
    )
    if path_hash:
        q = q.filter(JourneyDefinitionInstanceFact.path_hash == path_hash)
    if channel_group:
        q = q.filter(JourneyDefinitionInstanceFact.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyDefinitionInstanceFact.campaign_id == campaign_id)
    q = _exclude_out_of_scope_campaigns(q)
    if device:
        q = q.filter(JourneyDefinitionInstanceFact.device == device)
    if country:
        q = q.filter(func.lower(JourneyDefinitionInstanceFact.country) == country.lower())
    return q


def has_definition_instance_facts(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
) -> bool:
    return (
        _query_definition_instance_facts(
            db,
            journey_definition_id=journey_definition_id,
            date_from=date_from,
            date_to=date_to,
        )
        .limit(1)
        .first()
        is not None
    )


def compute_path_metric_from_definition_facts(
    db: Session,
    *,
    journey_definition_id: str,
    metric: str,
    date_from: date,
    date_to: date,
    path_hash: Optional[str] = None,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
) -> Optional[float]:
    rows = (
        _query_definition_instance_facts(
            db,
            journey_definition_id=journey_definition_id,
            date_from=date_from,
            date_to=date_to,
            path_hash=path_hash,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
        )
        .all()
    )
    if not rows:
        return None
    journeys = len(rows)
    conversions = len(rows)
    gross_conversions = sum(float(row.gross_conversions_total or 0.0) for row in rows)
    net_conversions = sum(float(row.net_conversions_total or 0.0) for row in rows)
    gross_revenue = sum(float(row.gross_revenue_total or 0.0) for row in rows)
    net_revenue = sum(float(row.net_revenue_total or 0.0) for row in rows)
    ttc_values = [float(row.time_to_convert_sec) for row in rows if isinstance(row.time_to_convert_sec, (int, float)) and float(row.time_to_convert_sec) >= 0]

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
        return _percentile(ttc_values, 0.5)
    return None


def list_paths_from_definition_facts(
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
) -> Dict[str, Any]:
    rows = (
        _query_definition_instance_facts(
            db,
            journey_definition_id=journey_definition_id,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
        )
        .all()
    )
    if not rows:
        return {
            "items": [],
            "total": 0,
            "summary": {
                "count_journeys": 0,
                "count_conversions": 0,
                "conversion_rate": 0.0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
                "gross_revenue_per_conversion": 0.0,
                "net_revenue_per_conversion": 0.0,
            },
            "page": max(1, int(page)),
            "limit": max(1, min(int(limit), 200)),
            "mode": mode,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        }

    buckets: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.path_hash or "")
        bucket = buckets.setdefault(
            key,
            {
                "path_hash": key,
                "path_steps": list(row.steps_json or []),
                "path_length": int(row.path_length or 0),
                "count_journeys": 0,
                "count_conversions": 0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
                "gross_conversions_total": 0.0,
                "net_conversions_total": 0.0,
                "ttc_values": [],
                "channel_group": row.channel_group,
                "campaign_id": row.campaign_id,
                "device": row.device,
                "country": row.country,
            },
        )
        bucket["count_journeys"] += 1
        bucket["count_conversions"] += 1
        bucket["gross_revenue"] += float(row.gross_revenue_total or 0.0)
        bucket["net_revenue"] += float(row.net_revenue_total or 0.0)
        bucket["gross_conversions_total"] += float(row.gross_conversions_total or 0.0)
        bucket["net_conversions_total"] += float(row.net_conversions_total or 0.0)
        if isinstance(row.time_to_convert_sec, (int, float)) and float(row.time_to_convert_sec) >= 0:
            bucket["ttc_values"].append(float(row.time_to_convert_sec))

    items = []
    for bucket in buckets.values():
        count_journeys = int(bucket["count_journeys"])
        count_conversions = int(bucket["count_conversions"])
        gross_revenue = float(bucket["gross_revenue"])
        net_revenue = float(bucket["net_revenue"])
        ttc_values = list(bucket["ttc_values"])
        if mode == "conversion_only" and count_conversions <= 0:
            continue
        items.append(
            {
                "path_hash": bucket["path_hash"],
                "path_steps": bucket["path_steps"],
                "path_length": bucket["path_length"],
                "count_journeys": count_journeys,
                "count_conversions": count_conversions,
                "conversion_rate": (count_conversions / count_journeys) if count_journeys > 0 else 0.0,
                "gross_revenue": round(gross_revenue, 2),
                "net_revenue": round(net_revenue, 2),
                "gross_revenue_per_conversion": round((gross_revenue / count_conversions), 2) if count_conversions > 0 else 0.0,
                "net_revenue_per_conversion": round((net_revenue / count_conversions), 2) if count_conversions > 0 else 0.0,
                "avg_time_to_convert_sec": round(sum(ttc_values) / len(ttc_values), 2) if ttc_values else None,
                "p50_time_to_convert_sec": _percentile(ttc_values, 0.5),
                "p90_time_to_convert_sec": _percentile(ttc_values, 0.9),
                "channel_group": bucket["channel_group"],
                "campaign_id": bucket["campaign_id"],
                "device": bucket["device"],
                "country": bucket["country"],
            }
        )

    items.sort(key=lambda item: (-int(item["count_journeys"]), str(item["path_hash"])))
    total = len(items)
    page = max(1, int(page))
    limit = max(1, min(int(limit), 200))
    paged_items = items[(page - 1) * limit : page * limit]

    total_journeys = sum(int(bucket["count_journeys"]) for bucket in buckets.values())
    total_conversions = sum(int(bucket["count_conversions"]) for bucket in buckets.values())
    gross_revenue_total = sum(float(bucket["gross_revenue"]) for bucket in buckets.values())
    net_revenue_total = sum(float(bucket["net_revenue"]) for bucket in buckets.values())

    return {
        "items": paged_items,
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


def iter_definition_instance_rows(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
):
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    q = db.query(JourneyDefinitionInstanceFact).filter(
        JourneyDefinitionInstanceFact.journey_definition_id == journey_definition_id,
        JourneyDefinitionInstanceFact.conversion_ts >= start_dt,
        JourneyDefinitionInstanceFact.conversion_ts < end_dt,
    )
    q = _exclude_out_of_scope_campaigns(q)
    for row in q.order_by(JourneyDefinitionInstanceFact.conversion_ts.asc()).yield_per(1000):
        yield row
