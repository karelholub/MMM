"""
Overview (Cover) Dashboard: summary KPIs, drivers, alerts, freshness.

- Does NOT block on MMM/Incrementality; shows "not available" when absent.
- Pre-aggregates where possible; robust to missing data; consistent response shapes.
- Includes confidence flags when data quality/freshness is poor.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import (
    ChannelPerformanceDaily,
    ConversionPath,
    JourneyDefinition,
    JourneyInstanceFact,
    JourneyPathDaily,
)
from .models_overview_alerts import AlertEvent, AlertRule
from .services_canonical_facts import iter_canonical_conversion_rows
from .services_conversions import (
    conversion_path_is_converted as _conversion_path_is_converted,
    conversion_path_payload,
    conversion_path_revenue_value,
    conversion_path_touchpoints,
)
from .services_journey_path_outputs import list_paths_from_outputs
from .services_visit_facts import iter_touchpoint_visit_rows
from .services_metrics import delta_pct, journey_outcome_summary


# ---------------------------------------------------------------------------
# Types and helpers
# ---------------------------------------------------------------------------

def _parse_dt(s: Optional[str]):
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _coerce_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_tz(timezone_name: Optional[str]) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _overview_path_type(payload: Dict[str, Any]) -> str:
    summary = ((payload.get("meta") or {}).get("interaction_summary") or {}) if isinstance(payload.get("meta"), dict) else {}
    path_type = summary.get("path_type")
    if isinstance(path_type, str) and path_type:
        return path_type
    touchpoints = payload.get("touchpoints") or []
    has_impression = False
    has_click_like = False
    for tp in touchpoints:
        if not isinstance(tp, dict):
            continue
        interaction = str(tp.get("interaction_type") or "").strip().lower()
        if interaction == "impression":
            has_impression = True
        elif interaction in {"click", "visit", "direct"}:
            has_click_like = True
    if has_impression and has_click_like:
        return "mixed_path"
    if has_click_like:
        return "click_through"
    if has_impression:
        return "view_through"
    return "unknown"


def _iter_conversion_path_rows(
    db: Session,
    *,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    first_touch_from: Optional[datetime] = None,
    last_touch_to: Optional[datetime] = None,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
):
    q = db.query(
        ConversionPath.conversion_id,
        ConversionPath.conversion_key,
        ConversionPath.conversion_ts,
        ConversionPath.first_touch_ts,
        ConversionPath.last_touch_ts,
        ConversionPath.path_json,
    )
    if date_from is not None:
        q = q.filter(ConversionPath.conversion_ts >= date_from)
    if date_to is not None:
        q = q.filter(ConversionPath.conversion_ts <= date_to)
    if first_touch_from is not None:
        q = q.filter(ConversionPath.first_touch_ts <= first_touch_from)
    if last_touch_to is not None:
        q = q.filter(ConversionPath.last_touch_ts >= last_touch_to)
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    if conversion_ids is not None:
        normalized_ids = [str(value) for value in conversion_ids if str(value or "").strip()]
        if not normalized_ids:
            return
        q = q.filter(ConversionPath.conversion_id.in_(normalized_ids))
    for row in q.order_by(ConversionPath.conversion_ts.desc()).yield_per(1000):
        yield SimpleNamespace(
            conversion_id=row[0],
            conversion_key=row[1],
            conversion_ts=row[2],
            first_touch_ts=row[3],
            last_touch_ts=row[4],
            path_json=row[5] if isinstance(row[5], dict) else {},
        )


def _iter_silver_conversion_rows(
    db: Session,
    *,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
):
    allowed_ids = set(str(value) for value in (conversion_ids or []) if str(value))
    use_id_filter = conversion_ids is not None
    if use_id_filter and not allowed_ids:
        return
    for row in iter_canonical_conversion_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        conversion_key=conversion_key,
    ):
        if use_id_filter and str(getattr(row, "conversion_id", "") or "") not in allowed_ids:
            continue
        yield row


def _iter_visit_rows(
    db: Session,
    *,
    conversion_ids: Optional[List[str]] = None,
    touchpoint_from: Optional[datetime] = None,
    touchpoint_to: Optional[datetime] = None,
):
    if conversion_ids is not None and not [str(value) for value in conversion_ids if str(value or "").strip()]:
        return
    yield from iter_touchpoint_visit_rows(
        db,
        conversion_ids=conversion_ids,
        touchpoint_from=touchpoint_from,
        touchpoint_to=touchpoint_to,
    )


def _empty_outcomes() -> Dict[str, float]:
    return {
        "gross_conversions": 0.0,
        "net_conversions": 0.0,
        "gross_value": 0.0,
        "net_value": 0.0,
        "refunded_value": 0.0,
        "cancelled_value": 0.0,
        "invalid_leads": 0.0,
        "valid_leads": 0.0,
        "click_through_conversions": 0.0,
        "view_through_conversions": 0.0,
        "mixed_path_conversions": 0.0,
    }


def _merge_outcomes(target: Dict[str, float], payload: Dict[str, Any]) -> None:
    outcome = journey_outcome_summary(payload)
    target["gross_conversions"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
    target["net_conversions"] += float(outcome.get("net_conversions", 0.0) or 0.0)
    target["gross_value"] += float(outcome.get("gross_value", 0.0) or 0.0)
    target["net_value"] += float(outcome.get("net_value", 0.0) or 0.0)
    target["refunded_value"] += float(outcome.get("refunded_value", 0.0) or 0.0)
    target["cancelled_value"] += float(outcome.get("cancelled_value", 0.0) or 0.0)
    target["invalid_leads"] += float(outcome.get("invalid_leads", 0.0) or 0.0)
    target["valid_leads"] += float(outcome.get("valid_leads", 0.0) or 0.0)
    path_type = _overview_path_type(payload)
    count = float(outcome.get("net_conversions", 0.0) or 0.0)
    if path_type == "click_through":
        target["click_through_conversions"] += count
    elif path_type == "view_through":
        target["view_through_conversions"] += count
    elif path_type == "mixed_path":
        target["mixed_path_conversions"] += count


def _merge_silver_outcomes(target: Dict[str, float], row: Any) -> None:
    target["gross_conversions"] += float(getattr(row, "gross_conversions_total", 0.0) or 0.0)
    target["net_conversions"] += float(getattr(row, "net_conversions_total", 0.0) or 0.0)
    target["gross_value"] += float(getattr(row, "gross_revenue_total", 0.0) or 0.0)
    target["net_value"] += float(getattr(row, "net_revenue_total", 0.0) or 0.0)
    target["refunded_value"] += float(getattr(row, "refunded_value", 0.0) or 0.0)
    target["cancelled_value"] += float(getattr(row, "cancelled_value", 0.0) or 0.0)
    target["invalid_leads"] += float(getattr(row, "invalid_leads", 0.0) or 0.0)
    target["valid_leads"] += float(getattr(row, "valid_leads", 0.0) or 0.0)
    path_type = str(getattr(row, "interaction_path_type", "") or "").strip().lower()
    count = float(getattr(row, "net_conversions_total", 0.0) or 0.0)
    if path_type == "click_through":
        target["click_through_conversions"] += count
    elif path_type == "view_through":
        target["view_through_conversions"] += count
    elif path_type == "mixed_path":
        target["mixed_path_conversions"] += count


def _expense_by_channel(
    expenses: Any,
    date_from: Optional[str],
    date_to: Optional[str],
    allowed_channels: Optional[List[str]] = None,
) -> Dict[str, float]:
    """Aggregate expenses by channel. expenses: dict[id -> ExpenseEntry] or list of dicts."""
    by_ch: Dict[str, float] = {}
    allowed = {str(value) for value in (allowed_channels or []) if str(value)}
    use_allowed = bool(allowed)
    items = expenses.values() if isinstance(expenses, dict) else (expenses or [])
    for exp in items:
        entry = exp if isinstance(exp, dict) else getattr(exp, "__dict__", exp)
        if isinstance(entry, dict):
            status = entry.get("status", "active")
            ch = entry.get("channel")
            amount = entry.get("converted_amount") or entry.get("amount") or 0
            start = entry.get("service_period_start")
        else:
            status = getattr(exp, "status", "active")
            ch = getattr(exp, "channel", None)
            amount = getattr(exp, "converted_amount", None) or getattr(exp, "amount", 0) or 0
            start = getattr(exp, "service_period_start", None)
        if status == "deleted" or not ch:
            continue
        if use_allowed and str(ch) not in allowed:
            continue
        if date_from and start and start < date_from:
            continue
        if date_to and start:
            end = getattr(exp, "service_period_end", None) if not isinstance(entry, dict) else entry.get("service_period_end")
            if end and end > date_to:
                continue
        by_ch[ch] = by_ch.get(ch, 0.0) + float(amount)
    return by_ch


def _filtered_conversion_ids_for_channel_group(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str],
    channel_group: Optional[str],
) -> Optional[List[str]]:
    channel = str(channel_group or "").strip().lower()
    if not channel:
        return None
    q = db.query(JourneyInstanceFact.conversion_id).filter(
        JourneyInstanceFact.conversion_ts >= dt_from,
        JourneyInstanceFact.conversion_ts <= dt_to,
        JourneyInstanceFact.channel_group == channel,
    )
    if conversion_key:
        q = q.filter(JourneyInstanceFact.conversion_key == conversion_key)
    return [str(row[0]) for row in q.all() if str(row[0] or "")]


def _to_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _is_full_utc_day_window(date_from: Optional[datetime], date_to: Optional[datetime]) -> bool:
    start = _to_utc_naive(date_from)
    end = _to_utc_naive(date_to)
    if start is None or end is None or end < start:
        return False
    if start.time() != datetime.min.time():
        return False
    return end.time() == datetime.max.time()


def _conversions_and_revenue_from_channel_facts(
    db: Session,
    date_from: datetime,
    date_to: datetime,
    conversion_key: Optional[str],
) -> Optional[Tuple[int, float, List[Dict[str, Any]]]]:
    if not _is_full_utc_day_window(date_from, date_to):
        return None
    rows = (
        db.query(ChannelPerformanceDaily)
        .filter(
            ChannelPerformanceDaily.date >= _to_utc_naive(date_from).date(),
            ChannelPerformanceDaily.date <= _to_utc_naive(date_to).date(),
        )
        .order_by(ChannelPerformanceDaily.date.asc())
        .all()
    )
    if not rows:
        return None
    daily: Dict[str, Dict[str, Any]] = {}
    total_value = 0.0
    total_conversions = 0
    for row in rows:
        if conversion_key is None:
            if row.conversion_key is not None:
                continue
        elif str(row.conversion_key or "") != conversion_key:
            continue
        day = row.date.isoformat()
        conversions = int(round(float(row.count_conversions or 0.0)))
        revenue = float(row.gross_revenue_total or 0.0)
        if day not in daily:
            daily[day] = {"date": day, "conversions": 0, "revenue": 0.0}
        daily[day]["conversions"] += conversions
        daily[day]["revenue"] += revenue
        total_conversions += conversions
        total_value += revenue
    if not daily:
        return None
    series = sorted(daily.values(), key=lambda x: x["date"])
    return total_conversions, total_value, series


def _conversions_and_revenue_from_paths(
    db: Session,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    conversion_key: Optional[str],
    conversion_ids: Optional[List[str]] = None,
) -> Tuple[int, float, List[Dict[str, Any]]]:
    """Returns (conversions_count, total_revenue, daily_series for sparkline)."""
    aggregate = None if conversion_ids else _conversions_and_revenue_from_channel_facts(db, date_from, date_to, conversion_key)
    if aggregate is not None:
        return aggregate
    series_from_silver = _series_from_silver_conversion_facts(db, date_from, date_to, "daily", conversion_key, conversion_ids=conversion_ids)
    if series_from_silver is not None:
        daily = [
            {"date": bucket, "conversions": int(round(value or 0.0)), "revenue": float(series_from_silver["revenue_map"].get(bucket, 0.0) or 0.0)}
            for bucket, value in sorted(series_from_silver["conversions_map"].items())
        ]
        return (
            int(series_from_silver["conversions_total"]),
            float(series_from_silver["revenue_total"]),
            daily,
        )
    dedupe_seen = set()
    total_value = 0.0
    daily: Dict[str, Dict[str, Any]] = {}
    for r in _iter_conversion_path_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        if not _conversion_path_is_converted(r):
            continue
        val = conversion_path_revenue_value(r, dedupe_seen=dedupe_seen)
        total_value += val
        d = r.conversion_ts.date().isoformat() if hasattr(r.conversion_ts, "date") else str(r.conversion_ts)[:10]
        if d not in daily:
            daily[d] = {"date": d, "conversions": 0, "revenue": 0.0}
        daily[d]["conversions"] += 1
        daily[d]["revenue"] += val
    series = sorted(daily.values(), key=lambda x: x["date"])
    return sum(int(item.get("conversions", 0)) for item in series), total_value, series


def _aggregate_outcomes_from_paths(
    db: Session,
    date_from: datetime,
    date_to: datetime,
    conversion_key: Optional[str],
    conversion_ids: Optional[List[str]] = None,
) -> Dict[str, float]:
    silver = _aggregate_outcomes_from_silver_facts(db, date_from, date_to, conversion_key, conversion_ids=conversion_ids)
    if silver is not None:
        return silver
    totals = _empty_outcomes()
    for row in _iter_conversion_path_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        if not _conversion_path_is_converted(row):
            continue
        _merge_outcomes(totals, conversion_path_payload(row))
    return totals


def _bucket_key_for_date(day: Any, grain: str) -> str:
    if isinstance(day, datetime):
        return _bucket_key(day, grain)
    if grain == "hourly":
        return f"{str(day)[:10]}T00:00:00"
    if hasattr(day, "weekday"):
        return (day - timedelta(days=day.weekday())).isoformat()
    return str(day)[:10]


def _series_from_channel_facts(
    db: Session,
    start: datetime,
    end: datetime,
    grain: str,
    conversion_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if grain == "hourly":
        return None
    rows = (
        db.query(ChannelPerformanceDaily)
        .filter(
            ChannelPerformanceDaily.date >= start.date(),
            ChannelPerformanceDaily.date <= end.date(),
        )
        .all()
    )
    if not rows:
        return None

    conv_map: Dict[str, float] = {}
    rev_map: Dict[str, float] = {}
    visit_map: Dict[str, float] = {}
    total_revenue = 0.0
    total_conversions = 0
    total_visits = 0.0
    observed_keys: Set[str] = set()

    for row in rows:
        bucket = _bucket_key_for_date(row.date, grain)
        if row.conversion_key is None:
            visits = float(row.visits_total or 0.0)
            if visits:
                visit_map[bucket] = visit_map.get(bucket, 0.0) + visits
                total_visits += visits
                observed_keys.add(bucket)
            if conversion_key is not None:
                continue
        elif str(row.conversion_key or "") != conversion_key:
            continue

        conversions = float(row.count_conversions or 0.0)
        revenue = float(row.gross_revenue_total or 0.0)
        if conversions or revenue:
            conv_map[bucket] = conv_map.get(bucket, 0.0) + conversions
            rev_map[bucket] = rev_map.get(bucket, 0.0) + revenue
            total_revenue += revenue
            total_conversions += int(round(conversions))
            observed_keys.add(bucket)

    return {
        "conversions_total": total_conversions,
        "revenue_total": round(total_revenue, 2),
        "visits_total": round(total_visits, 2),
        "conversions_map": conv_map,
        "revenue_map": rev_map,
        "visits_map": visit_map,
        "observed_points": len(observed_keys),
    }


def _aggregate_outcomes_from_channel_facts(
    db: Session,
    start: datetime,
    end: datetime,
    conversion_key: Optional[str] = None,
) -> Optional[Dict[str, float]]:
    rows = (
        db.query(ChannelPerformanceDaily)
        .filter(
            ChannelPerformanceDaily.date >= start.date(),
            ChannelPerformanceDaily.date <= end.date(),
        )
        .all()
    )
    if not rows:
        return None
    totals = _empty_outcomes()
    for row in rows:
        if conversion_key is None:
            if row.conversion_key is not None:
                continue
        elif str(row.conversion_key or "") != conversion_key:
            continue
        totals["gross_conversions"] += float(row.gross_conversions_total or 0.0)
        totals["net_conversions"] += float(row.net_conversions_total or 0.0)
        totals["gross_value"] += float(row.gross_revenue_total or 0.0)
        totals["net_value"] += float(row.net_revenue_total or 0.0)
        totals["view_through_conversions"] += float(row.view_through_conversions_total or 0.0)
        totals["click_through_conversions"] += float(row.click_through_conversions_total or 0.0)
        totals["mixed_path_conversions"] += float(row.mixed_path_conversions_total or 0.0)
    return totals


def _aggregate_outcomes_from_silver_facts(
    db: Session,
    start: datetime,
    end: datetime,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, float]]:
    totals = _empty_outcomes()
    seen = False
    for row in _iter_silver_conversion_rows(
        db,
        date_from=start,
        date_to=end,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        seen = True
        totals["gross_conversions"] += row.gross_conversions_total
        totals["net_conversions"] += row.net_conversions_total
        totals["gross_value"] += row.gross_revenue_total
        totals["net_value"] += row.net_revenue_total
        totals["refunded_value"] += row.refunded_value
        totals["cancelled_value"] += row.cancelled_value
        totals["invalid_leads"] += row.invalid_leads
        totals["valid_leads"] += row.valid_leads
        if row.interaction_path_type == "click_through":
            totals["click_through_conversions"] += row.net_conversions_total
        elif row.interaction_path_type == "view_through":
            totals["view_through_conversions"] += row.net_conversions_total
        elif row.interaction_path_type == "mixed_path":
            totals["mixed_path_conversions"] += row.net_conversions_total
    return totals if seen else None


def _aggregate_channel_metrics_from_facts(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
) -> Optional[Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]]:
    rows = (
        db.query(ChannelPerformanceDaily)
        .filter(
            ChannelPerformanceDaily.date >= dt_from.date(),
            ChannelPerformanceDaily.date <= dt_to.date(),
        )
        .all()
    )
    if not rows:
        return None
    metrics: Dict[str, Dict[str, float]] = {}
    outcomes: Dict[str, Dict[str, float]] = {}
    for row in rows:
        channel = str(row.channel or "unknown")
        entry = metrics.setdefault(channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
        if row.conversion_key is None:
            entry["visits"] += float(row.visits_total or 0.0)
            if conversion_key is not None:
                continue
        elif str(row.conversion_key or "") != conversion_key:
            continue
        entry["conversions"] += float(row.count_conversions or 0.0)
        entry["revenue"] += float(row.gross_revenue_total or 0.0)
        outcome = outcomes.setdefault(channel, _empty_outcomes())
        outcome["gross_conversions"] += float(row.gross_conversions_total or 0.0)
        outcome["net_conversions"] += float(row.net_conversions_total or 0.0)
        outcome["gross_value"] += float(row.gross_revenue_total or 0.0)
        outcome["net_value"] += float(row.net_revenue_total or 0.0)
        outcome["view_through_conversions"] += float(row.view_through_conversions_total or 0.0)
        outcome["click_through_conversions"] += float(row.click_through_conversions_total or 0.0)
        outcome["mixed_path_conversions"] += float(row.mixed_path_conversions_total or 0.0)
    return metrics, outcomes


def _aggregate_daily_channel_revenue_from_facts(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
) -> Optional[Dict[str, Dict[str, float]]]:
    rows = (
        db.query(ChannelPerformanceDaily)
        .filter(
            ChannelPerformanceDaily.date >= dt_from.date(),
            ChannelPerformanceDaily.date <= dt_to.date(),
        )
        .all()
    )
    if not rows:
        return None
    out: Dict[str, Dict[str, float]] = {}
    for row in rows:
        if conversion_key is None:
            if row.conversion_key is not None:
                continue
        elif str(row.conversion_key or "") != conversion_key:
            continue
        channel = str(row.channel or "unknown")
        day = row.date.isoformat()
        out.setdefault(channel, {})
        out[channel][day] = out[channel].get(day, 0.0) + float(row.gross_revenue_total or 0.0)
    return out


def _single_active_overview_definition_id(
    db: Session,
    *,
    conversion_key: Optional[str] = None,
) -> Optional[str]:
    definition_query = (
        db.query(JourneyDefinition)
        .filter(JourneyDefinition.is_archived == False)  # noqa: E712
        .order_by(JourneyDefinition.updated_at.desc())
    )
    if conversion_key:
        definition_query = definition_query.filter(JourneyDefinition.conversion_kpi_id == conversion_key)
    definitions = definition_query.limit(2).all()
    if len(definitions) != 1:
        return None
    return str(definitions[0].id)


def _series_from_daily_path_aggregates(
    db: Session,
    *,
    journey_definition_id: str,
    start: datetime,
    end: datetime,
) -> Dict[str, Any]:
    rows = (
        db.query(
            JourneyPathDaily.date.label("day"),
            func.sum(JourneyPathDaily.count_conversions).label("conversions_total"),
            func.sum(JourneyPathDaily.gross_revenue_total).label("gross_revenue_total"),
        )
        .filter(
            JourneyPathDaily.journey_definition_id == journey_definition_id,
            JourneyPathDaily.date >= start.date(),
            JourneyPathDaily.date <= end.date(),
        )
        .group_by(JourneyPathDaily.date)
        .order_by(JourneyPathDaily.date.asc())
        .all()
    )

    conv_map: Dict[str, float] = {}
    rev_map: Dict[str, float] = {}
    total_revenue = 0.0
    total_conversions = 0
    for row in rows:
        day = row.day.isoformat()
        conversions = float(row.conversions_total or 0.0)
        revenue = float(row.gross_revenue_total or 0.0)
        conv_map[day] = conv_map.get(day, 0.0) + conversions
        rev_map[day] = rev_map.get(day, 0.0) + revenue
        total_revenue += revenue
        total_conversions += int(round(conversions))

    return {
        "conversions_total": total_conversions,
        "revenue_total": round(total_revenue, 2),
        "conversions_map": conv_map,
        "revenue_map": rev_map,
        "observed_points": len(conv_map),
    }


def _aggregate_outcomes_from_daily_path_aggregates(
    db: Session,
    *,
    journey_definition_id: str,
    start: datetime,
    end: datetime,
) -> Dict[str, float]:
    row = (
        db.query(
            func.sum(JourneyPathDaily.gross_conversions_total),
            func.sum(JourneyPathDaily.net_conversions_total),
            func.sum(JourneyPathDaily.gross_revenue_total),
            func.sum(JourneyPathDaily.net_revenue_total),
            func.sum(JourneyPathDaily.view_through_conversions_total),
            func.sum(JourneyPathDaily.click_through_conversions_total),
            func.sum(JourneyPathDaily.mixed_path_conversions_total),
        )
        .filter(
            JourneyPathDaily.journey_definition_id == journey_definition_id,
            JourneyPathDaily.date >= start.date(),
            JourneyPathDaily.date <= end.date(),
        )
        .first()
    )
    totals = _empty_outcomes()
    if not row:
        return totals
    totals["gross_conversions"] = float(row[0] or 0.0)
    totals["net_conversions"] = float(row[1] or 0.0)
    totals["gross_value"] = float(row[2] or 0.0)
    totals["net_value"] = float(row[3] or 0.0)
    totals["view_through_conversions"] = float(row[4] or 0.0)
    totals["click_through_conversions"] = float(row[5] or 0.0)
    totals["mixed_path_conversions"] = float(row[6] or 0.0)
    return totals


def _sparkline_from_series(series: List[Dict[str, Any]], key: str, num_points: int = 14) -> List[float]:
    """Extract numeric series for sparkline (e.g. last N days)."""
    if not series:
        return []
    vals = [s.get(key, 0) for s in series]
    if len(vals) <= num_points:
        return vals
    return vals[-num_points:]


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _normalize_period_bounds(date_from: str, date_to: str, timezone_name: str = "UTC") -> Tuple[datetime, datetime]:
    local_tz = _safe_tz(timezone_name)
    dt_from = _parse_dt(date_from) or (datetime.utcnow() - timedelta(days=30))
    dt_to = _parse_dt(date_to) or datetime.utcnow()
    if isinstance(date_from, str) and len(date_from) == 10:
        dt_from = datetime.fromisoformat(date_from[:10]).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
            tzinfo=local_tz,
        ).astimezone(timezone.utc)
    if isinstance(date_to, str) and len(date_to) == 10:
        dt_to = datetime.fromisoformat(date_to[:10]).replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
            tzinfo=local_tz,
        ).astimezone(timezone.utc)
    if dt_to < dt_from:
        dt_from, dt_to = dt_to, dt_from
    return dt_from, dt_to


def _bucket_key(ts: datetime, grain: str) -> str:
    ts = _coerce_utc_datetime(ts)
    if grain == "hourly":
        return ts.replace(minute=0, second=0, microsecond=0).isoformat()
    return ts.date().isoformat()


def _bucket_step(grain: str) -> timedelta:
    return timedelta(hours=1) if grain == "hourly" else timedelta(days=1)


def _bucket_keys_in_range(start: datetime, end: datetime, grain: str) -> List[str]:
    keys: List[str] = []
    cursor = start.replace(minute=0, second=0, microsecond=0) if grain == "hourly" else start.replace(hour=0, minute=0, second=0, microsecond=0)
    step = _bucket_step(grain)
    while cursor <= end:
        keys.append(_bucket_key(cursor, grain))
        cursor += step
    return keys


def _series_from_map(values: Dict[str, float], bucket_keys: List[str], *, observed_points: int) -> List[Dict[str, Any]]:
    if observed_points <= 0:
        return []
    out: List[Dict[str, Any]] = []
    for key in bucket_keys:
        out.append({"ts": key, "value": float(values[key]) if key in values else None})
    return out


def _series_from_conversion_paths(
    db: Session,
    start: datetime,
    end: datetime,
    grain: str,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    silver = _series_from_silver_conversion_facts(db, start, end, grain, conversion_key, conversion_ids=conversion_ids)
    if silver is not None:
        return silver
    dedupe_seen = set()
    conv_map: Dict[str, float] = {}
    rev_map: Dict[str, float] = {}
    total_revenue = 0.0
    conversion_count = 0
    for row in _iter_conversion_path_rows(
        db,
        date_from=start,
        date_to=end,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        if not _conversion_path_is_converted(row):
            continue
        value = conversion_path_revenue_value(row, dedupe_seen=dedupe_seen)
        total_revenue += value
        conversion_count += 1
        key = _bucket_key(row.conversion_ts, grain)
        conv_map[key] = conv_map.get(key, 0.0) + 1.0
        rev_map[key] = rev_map.get(key, 0.0) + value
    return {
        "conversions_total": conversion_count,
        "revenue_total": round(total_revenue, 2),
        "conversions_map": conv_map,
        "revenue_map": rev_map,
        "observed_points": conversion_count,
    }


def _series_from_silver_conversion_facts(
    db: Session,
    start: datetime,
    end: datetime,
    grain: str,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    conv_map: Dict[str, float] = {}
    rev_map: Dict[str, float] = {}
    total_revenue = 0.0
    conversion_count = 0
    for row in _iter_silver_conversion_rows(
        db,
        date_from=start,
        date_to=end,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        conversion_count += 1
        total_revenue += row.gross_revenue_total
        key = _bucket_key(row.conversion_ts, grain)
        conv_map[key] = conv_map.get(key, 0.0) + 1.0
        rev_map[key] = rev_map.get(key, 0.0) + row.gross_revenue_total
    if conversion_count <= 0:
        return None
    return {
        "conversions_total": conversion_count,
        "revenue_total": round(total_revenue, 2),
        "conversions_map": conv_map,
        "revenue_map": rev_map,
        "observed_points": conversion_count,
    }


def _series_from_visits(
    db: Session,
    start: datetime,
    end: datetime,
    grain: str,
    conversion_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    silver = _series_from_silver_touchpoint_facts(db, start, end, grain, conversion_ids=conversion_ids)
    if silver is not None:
        return silver
    visit_map: Dict[str, float] = {}
    observed_points = 0
    for row in _iter_conversion_path_rows(
        db,
        first_touch_from=end,
        last_touch_to=start,
        conversion_ids=conversion_ids,
    ):
        for tp in conversion_path_touchpoints(row):
            ts = _as_datetime(tp.get("timestamp") or tp.get("ts"))
            if ts is None or ts < start or ts > end:
                continue
            key = _bucket_key(ts, grain)
            visit_map[key] = visit_map.get(key, 0.0) + 1.0
            observed_points += 1
    return {
        "visits_total": round(sum(visit_map.values()), 2),
        "visits_map": visit_map,
        "observed_points": observed_points,
    }


def _series_from_silver_touchpoint_facts(
    db: Session,
    start: datetime,
    end: datetime,
    grain: str,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    visit_map: Dict[str, float] = {}
    observed_points = 0
    for row in _iter_visit_rows(
        db,
        conversion_ids=conversion_ids,
        touchpoint_from=start,
        touchpoint_to=end,
    ):
        ts = row.touchpoint_ts
        if ts is None:
            continue
        key = _bucket_key(ts, grain)
        visit_map[key] = visit_map.get(key, 0.0) + 1.0
        observed_points += 1
    if observed_points <= 0:
        return None
    return {
        "visits_total": round(sum(visit_map.values()), 2),
        "visits_map": visit_map,
        "observed_points": observed_points,
    }


def _series_from_expenses(
    expenses: Any,
    start: datetime,
    end: datetime,
    grain: str,
    allowed_channels: Optional[List[str]] = None,
) -> Dict[str, Any]:
    amount_map: Dict[str, float] = {}
    observed_points = 0
    allowed = {str(value) for value in (allowed_channels or []) if str(value)}
    use_allowed = bool(allowed)
    items = expenses.values() if isinstance(expenses, dict) else (expenses or [])
    for exp in items:
        entry = exp if isinstance(exp, dict) else getattr(exp, "__dict__", exp)
        if isinstance(entry, dict):
            status = entry.get("status", "active")
            amount_raw = entry.get("converted_amount") or entry.get("amount") or 0
            start_raw = entry.get("service_period_start")
        else:
            status = getattr(exp, "status", "active")
            amount_raw = getattr(exp, "converted_amount", None) or getattr(exp, "amount", 0) or 0
            start_raw = getattr(exp, "service_period_start", None)
        if status == "deleted":
            continue
        channel = str(entry.get("channel") if isinstance(entry, dict) else getattr(exp, "channel", None) or "")
        if use_allowed and channel not in allowed:
            continue
        ts = _as_datetime(start_raw)
        if ts is None or ts < start or ts > end:
            continue
        key = _bucket_key(ts, grain)
        amount = float(amount_raw)
        amount_map[key] = amount_map.get(key, 0.0) + amount
        observed_points += 1
    return {
        "total": round(sum(amount_map.values()), 2),
        "map": amount_map,
        "observed_points": observed_points,
    }


def _normalize_channel_token(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        value = value.get("name") or value.get("id") or value.get("platform") or value.get("label")
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _display_channel_token(token: str) -> str:
    raw = token.strip()
    if not raw:
        return "Unknown"
    lower = raw.lower()
    if lower in {"cpc", "ppc", "seo", "crm"}:
        return lower.upper()
    return " ".join(part.capitalize() for part in raw.replace("_", " ").replace("-", " ").split())


def _touchpoint_channel(tp: Dict[str, Any]) -> str:
    for candidate in (
        tp.get("channel"),
        tp.get("source"),
        tp.get("medium"),
        (tp.get("utm") or {}).get("source") if isinstance(tp.get("utm"), dict) else None,
        (tp.get("utm") or {}).get("medium") if isinstance(tp.get("utm"), dict) else None,
    ):
        token = _normalize_channel_token(candidate)
        if token:
            return token
    return "unknown"


def _path_steps_from_payload(payload: Dict[str, Any]) -> List[str]:
    touchpoints = payload.get("touchpoints") or []
    if not isinstance(touchpoints, list):
        return ["unknown"]
    steps: List[str] = []
    prev_key: Optional[str] = None
    for tp in touchpoints:
        if not isinstance(tp, dict):
            continue
        channel = _touchpoint_channel(tp)
        key = channel.strip().lower() or "unknown"
        if key == prev_key:
            continue
        steps.append(channel)
        prev_key = key
    return steps or ["unknown"]


def _weighted_median(counts: Dict[int, int]) -> float:
    if not counts:
        return 0.0
    total = sum(max(0, count) for count in counts.values())
    if total <= 0:
        return 0.0
    threshold = total / 2.0
    running = 0
    for value in sorted(counts):
        running += max(0, counts[value])
        if running >= threshold:
            return float(value)
    return float(max(counts))


def _path_steps_from_daily_value(path_steps: Any) -> List[str]:
    if isinstance(path_steps, list):
        return [str(step).strip() for step in path_steps if str(step).strip()]
    if isinstance(path_steps, str):
        return [step.strip() for step in path_steps.split(">") if step.strip()]
    return []


def _overview_funnels_from_daily_aggregates(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str],
    limit: int,
) -> Optional[Dict[str, Any]]:
    definition_query = (
        db.query(JourneyDefinition)
        .filter(JourneyDefinition.is_archived == False)  # noqa: E712
        .order_by(JourneyDefinition.updated_at.desc())
    )
    if conversion_key:
        definition_query = definition_query.filter(JourneyDefinition.conversion_kpi_id == conversion_key)
    definitions = definition_query.limit(2).all()
    if len(definitions) != 1:
        return None

    rows = (
        db.query(JourneyPathDaily)
        .filter(JourneyPathDaily.journey_definition_id == definitions[0].id)
        .filter(JourneyPathDaily.date >= dt_from.date(), JourneyPathDaily.date <= dt_to.date())
        .all()
    )
    aggs: Dict[str, Dict[str, Any]] = {}
    outcomes = _empty_outcomes()
    total_conversions = 0
    path_length_counts: Dict[int, int] = defaultdict(int)

    if rows:
        for row in rows:
            steps = _path_steps_from_daily_value(row.path_steps)
            if not steps:
                continue
            conversions = int(row.count_conversions or 0)
            revenue = float(row.gross_revenue_total or 0.0)
            if conversions <= 0 and abs(revenue) <= 1e-9:
                continue

            path_key = " > ".join(steps)
            entry = aggs.setdefault(
                path_key,
                {
                    "path": path_key,
                    "steps": steps,
                    "conversions": 0,
                    "revenue": 0.0,
                    "median_weighted_sec": 0.0,
                    "median_weight": 0.0,
                    "path_length": len(steps),
                    "ends_with_direct": steps[-1].strip().lower() == "direct",
                },
            )
            entry["conversions"] += conversions
            entry["revenue"] += revenue
            latency_sec = float(row.p50_time_to_convert_sec or row.avg_time_to_convert_sec or 0.0)
            if latency_sec > 0 and conversions > 0:
                entry["median_weighted_sec"] += latency_sec * conversions
                entry["median_weight"] += conversions

            total_conversions += conversions
            path_length_counts[len(steps)] += conversions
            outcomes["gross_conversions"] += float(row.gross_conversions_total or 0.0)
            outcomes["net_conversions"] += float(row.net_conversions_total or 0.0)
            outcomes["gross_value"] += float(row.gross_revenue_total or 0.0)
            outcomes["net_value"] += float(row.net_revenue_total or 0.0)
            outcomes["view_through_conversions"] += float(row.view_through_conversions_total or 0.0)
            outcomes["click_through_conversions"] += float(row.click_through_conversions_total or 0.0)
            outcomes["mixed_path_conversions"] += float(row.mixed_path_conversions_total or 0.0)
    else:
        fallback = list_paths_from_outputs(
            db,
            journey_definition_id=definitions[0].id,
            date_from=dt_from.date(),
            date_to=dt_to.date(),
            mode="conversion_only",
            page=1,
            limit=max(50, limit * 4),
        )
        if not fallback:
            return None
        for row in fallback.get("items") or []:
            steps = _path_steps_from_daily_value(row.get("path_steps"))
            if not steps:
                continue
            conversions = int(row.get("count_conversions") or 0)
            revenue = float(row.get("gross_revenue") or 0.0)
            if conversions <= 0 and abs(revenue) <= 1e-9:
                continue
            path_key = " > ".join(steps)
            entry = aggs.setdefault(
                path_key,
                {
                    "path": path_key,
                    "steps": steps,
                    "conversions": 0,
                    "revenue": 0.0,
                    "median_weighted_sec": 0.0,
                    "median_weight": 0.0,
                    "path_length": len(steps),
                    "ends_with_direct": steps[-1].strip().lower() == "direct",
                },
            )
            entry["conversions"] += conversions
            entry["revenue"] += revenue
            latency_sec = float(row.get("p50_time_to_convert_sec") or row.get("avg_time_to_convert_sec") or 0.0)
            if latency_sec > 0 and conversions > 0:
                entry["median_weighted_sec"] += latency_sec * conversions
                entry["median_weight"] += conversions

            total_conversions += conversions
            path_length_counts[len(steps)] += conversions
            outcomes["gross_conversions"] += float(row.get("gross_conversions_total") or conversions)
            outcomes["net_conversions"] += float(row.get("net_conversions_total") or conversions)
            outcomes["gross_value"] += revenue
            outcomes["net_value"] += float(row.get("net_revenue") or 0.0)

    if total_conversions <= 0 or not aggs:
        return None

    items: List[Dict[str, Any]] = []
    for entry in aggs.values():
        conversions = int(entry.get("conversions") or 0)
        revenue = float(entry.get("revenue") or 0.0)
        median_days = None
        if float(entry.get("median_weight") or 0.0) > 0:
            median_days = (float(entry["median_weighted_sec"]) / float(entry["median_weight"])) / 86400.0
        items.append(
            {
                "path": entry["path"],
                "path_display": " -> ".join(_display_channel_token(step) for step in entry.get("steps") or []),
                "steps": entry.get("steps") or [],
                "conversions": conversions,
                "share": round((conversions / total_conversions), 6) if total_conversions > 0 else 0.0,
                "revenue": round(revenue, 2),
                "revenue_per_conversion": round((revenue / conversions), 2) if conversions > 0 else 0.0,
                "median_days_to_convert": round(median_days, 2) if median_days is not None else None,
                "path_length": int(entry.get("path_length") or 0),
                "ends_with_direct": bool(entry.get("ends_with_direct")),
            }
        )

    by_conversions = sorted(items, key=lambda item: (item["conversions"], item["revenue"]), reverse=True)
    by_revenue = sorted(items, key=lambda item: (item["revenue"], item["conversions"]), reverse=True)
    speed_candidates = [item for item in items if item["median_days_to_convert"] is not None]
    direct_speed_candidates = [item for item in speed_candidates if item.get("ends_with_direct")]
    if direct_speed_candidates:
        speed_candidates = direct_speed_candidates
    by_speed = sorted(
        speed_candidates,
        key=lambda item: (item["median_days_to_convert"], -item["conversions"], -item["revenue"]),
    )

    top_converting = by_conversions[: max(1, limit)]
    return {
        "date_from": dt_from.date().isoformat(),
        "date_to": dt_to.date().isoformat(),
        "summary": {
            "total_conversions": total_conversions,
            "net_conversions": round(outcomes["net_conversions"], 2),
            "gross_conversions": round(outcomes["gross_conversions"], 2),
            "net_revenue": round(outcomes["net_value"], 2),
            "gross_revenue": round(outcomes["gross_value"], 2),
            "view_through_conversions": round(outcomes["view_through_conversions"], 2),
            "click_through_conversions": round(outcomes["click_through_conversions"], 2),
            "mixed_path_conversions": round(outcomes["mixed_path_conversions"], 2),
            "distinct_paths": len(items),
            "top_paths_conversion_share": round(sum(item["conversions"] for item in top_converting) / total_conversions, 6) if total_conversions > 0 else 0.0,
            "median_path_length": _weighted_median(path_length_counts),
        },
        "tabs": {
            "conversions": top_converting,
            "revenue": by_revenue[: max(1, limit)],
            "speed": by_speed[: max(1, limit)],
        },
    }


def _campaign_rollups_from_daily_path_aggregates(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    prev_from: datetime,
    prev_to: datetime,
    conversion_key: Optional[str],
    channel_group: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    definition_id = _single_active_overview_definition_id(db, conversion_key=conversion_key)
    if not definition_id:
        return None

    rows_query = (
        db.query(JourneyPathDaily)
        .filter(
            JourneyPathDaily.journey_definition_id == definition_id,
            JourneyPathDaily.date >= prev_from.date(),
            JourneyPathDaily.date <= dt_to.date(),
        )
    )
    if channel_group:
        rows_query = rows_query.filter(JourneyPathDaily.channel_group == channel_group)
    rows = rows_query.all()
    if not rows:
        return None

    current_revenue: Dict[str, float] = {}
    current_conversions: Dict[str, int] = {}
    current_outcomes: Dict[str, Dict[str, float]] = {}
    previous_revenue: Dict[str, float] = {}

    for row in rows:
        campaign = str(row.campaign_id or "").strip() or "unknown"
        if dt_from.date() <= row.date <= dt_to.date():
            current_revenue[campaign] = current_revenue.get(campaign, 0.0) + float(row.gross_revenue_total or 0.0)
            current_conversions[campaign] = current_conversions.get(campaign, 0) + int(row.count_conversions or 0)
            outcome = current_outcomes.setdefault(campaign, _empty_outcomes())
            outcome["gross_conversions"] += float(row.gross_conversions_total or 0.0)
            outcome["net_conversions"] += float(row.net_conversions_total or 0.0)
            outcome["gross_value"] += float(row.gross_revenue_total or 0.0)
            outcome["net_value"] += float(row.net_revenue_total or 0.0)
            outcome["view_through_conversions"] += float(row.view_through_conversions_total or 0.0)
            outcome["click_through_conversions"] += float(row.click_through_conversions_total or 0.0)
            outcome["mixed_path_conversions"] += float(row.mixed_path_conversions_total or 0.0)
        elif prev_from.date() <= row.date <= prev_to.date():
            previous_revenue[campaign] = previous_revenue.get(campaign, 0.0) + float(row.gross_revenue_total or 0.0)

    if not current_revenue and not previous_revenue:
        return None
    return {
        "current_revenue": current_revenue,
        "current_conversions": current_conversions,
        "current_outcomes": current_outcomes,
        "previous_revenue": previous_revenue,
    }


def _confidence_from_quality(
    *,
    freshness_lag_min: Optional[float],
    expected_points: int,
    observed_points: int,
    missing_spend_share: float,
    missing_conversion_share: float,
) -> Dict[str, Any]:
    if observed_points <= 0:
        reasons = [
            {
                "key": "coverage",
                "label": "Coverage",
                "score": 0,
                "weight": 0.4,
                "detail": "No spend or conversion datapoints in selected period.",
            }
        ]
        return {"score": 0, "level": "low", "reasons": reasons}

    if freshness_lag_min is None:
        freshness_score = 60
        freshness_detail = "Ingest lag unavailable."
    elif freshness_lag_min <= 60 * 4:
        freshness_score = 100
        freshness_detail = f"Latest ingest lag {freshness_lag_min:.0f} min."
    elif freshness_lag_min <= 60 * 24:
        freshness_score = 70
        freshness_detail = f"Latest ingest lag {freshness_lag_min:.0f} min."
    else:
        freshness_score = 35
        freshness_detail = f"Latest ingest lag {freshness_lag_min:.0f} min (stale)."

    coverage_ratio = min(1.0, observed_points / max(expected_points, 1))
    coverage_score = round(coverage_ratio * 100)
    spend_score = round(max(0.0, 100.0 - missing_spend_share * 100.0))
    conv_score = round(max(0.0, 100.0 - missing_conversion_share * 100.0))
    stability_score = 70
    score = round(
        freshness_score * 0.30
        + coverage_score * 0.30
        + spend_score * 0.20
        + conv_score * 0.10
        + stability_score * 0.10
    )
    if score >= 80:
        level = "high"
    elif score >= 55:
        level = "medium"
    else:
        level = "low"
    reasons = [
        {
            "key": "freshness",
            "label": "Freshness",
            "score": freshness_score,
            "weight": 0.30,
            "detail": freshness_detail,
        },
        {
            "key": "coverage",
            "label": "Coverage",
            "score": coverage_score,
            "weight": 0.30,
            "detail": f"{observed_points}/{max(expected_points, 1)} trend points available.",
        },
        {
            "key": "missing_spend",
            "label": "Missing spend",
            "score": spend_score,
            "weight": 0.20,
            "detail": f"{missing_spend_share * 100:.1f}% of buckets have conversions but no spend.",
        },
        {
            "key": "missing_conversions",
            "label": "Missing conversions",
            "score": conv_score,
            "weight": 0.10,
            "detail": f"{missing_conversion_share * 100:.1f}% of buckets have spend but no conversions.",
        },
        {
            "key": "model_stability",
            "label": "Model stability",
            "score": stability_score,
            "weight": 0.10,
            "detail": "Model stability is not computed on Cover; neutral score applied.",
        },
    ]
    return {"score": score, "level": level, "reasons": reasons}


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------

def get_freshness(
    db: Session,
    import_runs_get_last_successful: Any,
) -> Dict[str, Any]:
    """
    last_touchpoint_ts, last_conversion_ts from ConversionPath; ingest_lag_minutes from last import.
    """
    last_touch_ts = None
    last_conv_ts = None
    try:
        row = (
            db.query(
                func.max(ConversionPath.last_touch_ts).label("last_touch"),
                func.max(ConversionPath.conversion_ts).label("last_conv"),
            )
        ).first()
        if row:
            last_touch_ts = row[0].isoformat() if row[0] else None
            last_conv_ts = row[1].isoformat() if row[1] else None
    except Exception:
        pass

    ingest_lag_minutes: Optional[float] = None
    if import_runs_get_last_successful:
        last_run = import_runs_get_last_successful()
        if last_run:
            finished = last_run.get("finished_at") or last_run.get("started_at")
            if finished:
                try:
                    ft = datetime.fromisoformat(finished.replace("Z", "+00:00"))
                    ingest_lag_minutes = (datetime.now(ft.tzinfo) - ft).total_seconds() / 60.0
                except Exception:
                    pass

    return {
        "last_touchpoint_ts": last_touch_ts,
        "last_conversion_ts": last_conv_ts,
        "ingest_lag_minutes": round(ingest_lag_minutes, 1) if ingest_lag_minutes is not None else None,
    }


# ---------------------------------------------------------------------------
# Summary: KPI tiles, highlights, freshness
# ---------------------------------------------------------------------------

def get_overview_summary(
    db: Session,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    currency: Optional[str] = None,
    workspace: Optional[str] = None,
    account: Optional[str] = None,
    model_id: Optional[str] = None,
    channel_group: Optional[str] = None,
    expenses: Any = None,
    import_runs_get_last_successful: Any = None,
) -> Dict[str, Any]:
    """
    Returns kpi_tiles, highlights, freshness.
    Does not require MMM/Incrementality; uses raw paths + expenses.
    """
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to, timezone)
    period_span = dt_to - dt_from
    prev_to = dt_from - timedelta(microseconds=1)
    prev_from = prev_to - period_span
    use_channel_group_filter = bool(str(channel_group or "").strip())
    use_utc_daily_aggregates = (timezone or "UTC").upper() == "UTC"
    current_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=None,
        channel_group=channel_group,
    )
    previous_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_key=None,
        channel_group=channel_group,
    )
    grain = "hourly" if period_span <= timedelta(days=2) else "daily"
    bucket_keys_current = _bucket_keys_in_range(dt_from, dt_to, grain)
    bucket_keys_prev = _bucket_keys_in_range(prev_from, prev_to, grain)
    expected_points = len(bucket_keys_current)

    fact_current = None if use_channel_group_filter or not use_utc_daily_aggregates else _series_from_channel_facts(db, dt_from, dt_to, grain)
    fact_prev = None if use_channel_group_filter or not use_utc_daily_aggregates else _series_from_channel_facts(db, prev_from, prev_to, grain)
    fact_current_outcomes = None if use_channel_group_filter or not use_utc_daily_aggregates else _aggregate_outcomes_from_channel_facts(db, dt_from, dt_to)
    fact_prev_outcomes = None if use_channel_group_filter or not use_utc_daily_aggregates else _aggregate_outcomes_from_channel_facts(db, prev_from, prev_to)
    if (
        fact_current is not None
        and fact_prev is not None
        and fact_current_outcomes is not None
        and fact_prev_outcomes is not None
    ):
        current_paths = fact_current
        prev_paths = fact_prev
        current_outcomes = fact_current_outcomes
        prev_outcomes = fact_prev_outcomes
        current_visits = fact_current
        prev_visits = fact_prev
    else:
        aggregate_definition_id = _single_active_overview_definition_id(db) if grain == "daily" and not use_channel_group_filter and use_utc_daily_aggregates else None
        if aggregate_definition_id:
            current_paths = _series_from_daily_path_aggregates(
                db,
                journey_definition_id=aggregate_definition_id,
                start=dt_from,
                end=dt_to,
            )
            prev_paths = _series_from_daily_path_aggregates(
                db,
                journey_definition_id=aggregate_definition_id,
                start=prev_from,
                end=prev_to,
            )
            current_outcomes = _aggregate_outcomes_from_daily_path_aggregates(
                db,
                journey_definition_id=aggregate_definition_id,
                start=dt_from,
                end=dt_to,
            )
            prev_outcomes = _aggregate_outcomes_from_daily_path_aggregates(
                db,
                journey_definition_id=aggregate_definition_id,
                start=prev_from,
                end=prev_to,
            )
        else:
            current_paths = _series_from_conversion_paths(
                db,
                dt_from,
                dt_to,
                grain,
                conversion_ids=current_conversion_ids if use_channel_group_filter else None,
            )
            prev_paths = _series_from_conversion_paths(
                db,
                prev_from,
                prev_to,
                grain,
                conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
            )
            current_outcomes = _aggregate_outcomes_from_paths(
                db,
                dt_from,
                dt_to,
                None,
                conversion_ids=current_conversion_ids if use_channel_group_filter else None,
            )
            prev_outcomes = _aggregate_outcomes_from_paths(
                db,
                prev_from,
                prev_to,
                None,
                conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
            )
        current_visits = _series_from_visits(
            db,
            dt_from,
            dt_to,
            grain,
            conversion_ids=current_conversion_ids if use_channel_group_filter else None,
        )
        prev_visits = _series_from_visits(
            db,
            prev_from,
            prev_to,
            grain,
            conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
        )
    current_expenses = _series_from_expenses(
        expenses or {},
        dt_from,
        dt_to,
        grain,
        allowed_channels=[channel_group] if use_channel_group_filter else None,
    )
    prev_expenses = _series_from_expenses(
        expenses or {},
        prev_from,
        prev_to,
        grain,
        allowed_channels=[channel_group] if use_channel_group_filter else None,
    )

    total_spend = current_expenses["total"]
    prev_spend = prev_expenses["total"]
    total_visits = current_visits["visits_total"]
    prev_visits_total = prev_visits["visits_total"]
    conv_count = current_paths["conversions_total"]
    prev_conv = prev_paths["conversions_total"]
    total_revenue = current_paths["revenue_total"]
    prev_revenue = prev_paths["revenue_total"]

    def _delta_pct(curr: float, prev: float) -> Optional[float]:
        if prev == 0:
            return 100.0 if curr > 0 else None
        return round((curr - prev) / prev * 100.0, 1)

    freshness = get_freshness(db, import_runs_get_last_successful)
    lag_min = freshness.get("ingest_lag_minutes")
    conv_map_current = current_paths["conversions_map"]
    spend_map_current = current_expenses["map"]
    buckets_with_conversions = 0
    missing_spend_buckets = 0
    buckets_with_spend = 0
    missing_conversion_buckets = 0
    for key in bucket_keys_current:
        conv_v = conv_map_current.get(key, 0.0)
        spend_v = spend_map_current.get(key, 0.0)
        if conv_v > 0:
            buckets_with_conversions += 1
            if spend_v <= 0:
                missing_spend_buckets += 1
        if spend_v > 0:
            buckets_with_spend += 1
            if conv_v <= 0:
                missing_conversion_buckets += 1
    missing_spend_share = (
        missing_spend_buckets / buckets_with_conversions if buckets_with_conversions > 0 else 0.0
    )
    missing_conversion_share = (
        missing_conversion_buckets / buckets_with_spend if buckets_with_spend > 0 else 0.0
    )

    observed_points = max(
        current_expenses.get("observed_points", 0),
        current_visits.get("observed_points", 0),
        current_paths.get("observed_points", 0),
    )
    confidence = _confidence_from_quality(
        freshness_lag_min=lag_min,
        expected_points=expected_points,
        observed_points=observed_points,
        missing_spend_share=missing_spend_share,
        missing_conversion_share=missing_conversion_share,
    )

    current_spend_series = _series_from_map(
        current_expenses["map"], bucket_keys_current, observed_points=current_expenses["observed_points"]
    )
    prev_spend_series = _series_from_map(
        prev_expenses["map"], bucket_keys_prev, observed_points=prev_expenses["observed_points"]
    )
    current_visit_series = _series_from_map(
        current_visits["visits_map"], bucket_keys_current, observed_points=current_visits["observed_points"]
    )
    prev_visit_series = _series_from_map(
        prev_visits["visits_map"], bucket_keys_prev, observed_points=prev_visits["observed_points"]
    )
    current_conv_series = _series_from_map(
        current_paths["conversions_map"], bucket_keys_current, observed_points=current_paths["observed_points"]
    )
    prev_conv_series = _series_from_map(
        prev_paths["conversions_map"], bucket_keys_prev, observed_points=prev_paths["observed_points"]
    )
    current_rev_series = _series_from_map(
        current_paths["revenue_map"], bucket_keys_current, observed_points=current_paths["observed_points"]
    )
    prev_rev_series = _series_from_map(
        prev_paths["revenue_map"], bucket_keys_prev, observed_points=prev_paths["observed_points"]
    )

    current_period = {
        "date_from": dt_from.isoformat(),
        "date_to": dt_to.isoformat(),
        "grain": grain,
    }
    previous_period = {
        "date_from": prev_from.isoformat(),
        "date_to": prev_to.isoformat(),
    }

    def _tile_payload(
        *,
        kpi_key: str,
        value: float,
        prev_value: float,
        series: List[Dict[str, Any]],
        series_prev: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        delta_abs = round(value - prev_value, 2)
        delta_pct = _delta_pct(float(value), float(prev_value))
        return {
            "kpi_key": kpi_key,
            "value": round(value, 2) if kpi_key not in {"conversions", "visits"} else int(value),
            "delta_pct": delta_pct,
            "delta_abs": delta_abs,
            "current_period": current_period,
            "previous_period": previous_period,
            "series": series,
            "series_prev": series_prev,
            "sparkline": [float(p["value"]) if isinstance(p.get("value"), (int, float)) else 0.0 for p in series],
            "confidence": confidence["level"],
            "confidence_score": confidence["score"],
            "confidence_level": confidence["level"],
            "confidence_reasons": confidence["reasons"],
        }

    kpi_tiles = [
        _tile_payload(
            kpi_key="spend",
            value=float(total_spend),
            prev_value=float(prev_spend),
            series=current_spend_series,
            series_prev=prev_spend_series,
        ),
        _tile_payload(
            kpi_key="visits",
            value=float(total_visits),
            prev_value=float(prev_visits_total),
            series=current_visit_series,
            series_prev=prev_visit_series,
        ),
        _tile_payload(
            kpi_key="conversions",
            value=float(conv_count),
            prev_value=float(prev_conv),
            series=current_conv_series,
            series_prev=prev_conv_series,
        ),
        _tile_payload(
            kpi_key="revenue",
            value=float(total_revenue),
            prev_value=float(prev_revenue),
            series=current_rev_series,
            series_prev=prev_rev_series,
        ),
    ]
    kpi_tiles.extend(
        [
            _tile_payload(
                kpi_key="net_conversions",
                value=float(current_outcomes["net_conversions"]),
                prev_value=float(prev_outcomes["net_conversions"]),
                series=current_conv_series,
                series_prev=prev_conv_series,
            ),
            _tile_payload(
                kpi_key="net_revenue",
                value=float(current_outcomes["net_value"]),
                prev_value=float(prev_outcomes["net_value"]),
                series=current_rev_series,
                series_prev=prev_rev_series,
            ),
        ]
    )

    # Highlights: from alerts + KPI deltas
    highlights: List[Dict[str, Any]] = []
    for t in kpi_tiles:
        dp = t.get("delta_pct")
        if dp is not None and abs(dp) >= 5:
            direction = "up" if dp > 0 else "down"
            highlights.append({
                "type": "kpi_delta",
                "kpi_key": t["kpi_key"],
                "message": f"{t['kpi_key'].title()} {direction} {abs(dp):.1f}% vs previous period",
                "delta_pct": dp,
            })
    # Add open alerts as highlights
    try:
        alert_events = (
            db.query(AlertEvent)
            .filter(AlertEvent.status.in_(["open", "ack"]))
            .order_by(AlertEvent.ts_detected.desc())
            .limit(5)
            .all()
        )
        for ev in alert_events:
            highlights.append({
                "type": "alert",
                "alert_id": ev.id,
                "severity": ev.severity,
                "message": ev.title or ev.message,
                "ts_detected": ev.ts_detected.isoformat() if hasattr(ev.ts_detected, "isoformat") else str(ev.ts_detected),
            })
    except Exception:
        pass
    highlights = highlights[:10]

    readiness = None
    consistency_warnings: List[str] = []
    try:
        from app.services_conversions import load_journeys_from_db
        from app.services_journey_readiness import build_journey_readiness
        from app.services_journey_settings import build_journey_settings_impact_preview, ensure_active_journey_settings
        from app.utils.kpi_config import load_kpi_config

        journeys = load_journeys_from_db(db, limit=50000)
        kpi_config = load_kpi_config()
        active_settings = ensure_active_journey_settings(db, actor="system")
        active_preview = build_journey_settings_impact_preview(db, draft_settings_json=active_settings.settings_json or {})
        readiness = build_journey_readiness(
            journeys=journeys,
            kpi_config=kpi_config,
            get_import_runs_fn=lambda limit=50: [],
            active_settings=active_settings,
            active_settings_preview=active_preview,
        )
        consistency_warnings = [
            *readiness.get("blockers", []),
            *readiness.get("warnings", []),
        ][:5]
        for warning in consistency_warnings[:3]:
            highlights.append(
                {
                    "type": "consistency_warning",
                    "message": warning,
                }
            )
    except Exception:
        readiness = None
        consistency_warnings = []

    return {
        "kpi_tiles": kpi_tiles,
        "outcomes": {
            "current": current_outcomes,
            "previous": prev_outcomes,
        },
        "highlights": highlights,
        "freshness": freshness,
        "readiness": readiness,
        "consistency_warnings": consistency_warnings,
        "current_period": current_period,
        "previous_period": previous_period,
        "model_id": model_id,
        "channel_group": channel_group,
        "date_from": date_from,
        "date_to": date_to,
        "timezone": timezone,
        "currency": currency,
    }


# ---------------------------------------------------------------------------
# Drivers: by_channel, by_campaign, biggest_movers
# ---------------------------------------------------------------------------

def get_overview_drivers(
    db: Session,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    expenses: Any = None,
    top_campaigns_n: int = 10,
    conversion_key: Optional[str] = None,
    channel_group: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Top drivers: by_channel (spend, conversions, revenue, delta), by_campaign (top N), biggest_movers.
    Uses ConversionPath + expenses only; does not block on MMM.
    """
    dedupe_seen_current = set()
    dedupe_seen_prev = set()

    dt_from, dt_to = _normalize_period_bounds(date_from, date_to, timezone)
    period_span = dt_to - dt_from
    prev_to = dt_from - timedelta(microseconds=1)
    prev_from = prev_to - period_span
    use_utc_daily_aggregates = (timezone or "UTC").upper() == "UTC"
    use_channel_group_filter = bool(str(channel_group or "").strip())
    current_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        channel_group=channel_group,
    )
    previous_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_key=conversion_key,
        channel_group=channel_group,
    )
    aggregate_campaign_rollups = (
        _campaign_rollups_from_daily_path_aggregates(
            db,
            dt_from=dt_from,
            dt_to=dt_to,
            prev_from=prev_from,
            prev_to=prev_to,
            conversion_key=conversion_key,
            channel_group=channel_group,
        )
        if use_utc_daily_aggregates
        else None
    )

    expense_by_channel = _expense_by_channel(
        expenses or {},
        date_from,
        date_to,
        allowed_channels=[channel_group] if use_channel_group_filter else None,
    )
    prev_expense = _expense_by_channel(
        expenses or {},
        prev_from.strftime("%Y-%m-%d"),
        prev_to.strftime("%Y-%m-%d"),
        allowed_channels=[channel_group] if use_channel_group_filter else None,
    )

    fact_current_channel_rollups = None if use_channel_group_filter or not use_utc_daily_aggregates else _aggregate_channel_metrics_from_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
    )
    fact_prev_channel_rollups = None if use_channel_group_filter or not use_utc_daily_aggregates else _aggregate_channel_metrics_from_facts(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_key=conversion_key,
    )
    silver_current_channel_rollups = None if fact_current_channel_rollups is not None else _channel_driver_rollups_from_silver_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        conversion_ids=current_conversion_ids if use_channel_group_filter else None,
    )
    silver_prev_channel_rollups = None if fact_prev_channel_rollups is not None else _channel_driver_rollups_from_silver_facts(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_key=conversion_key,
        conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
    )
    silver_current_channel_visits = None if fact_current_channel_rollups is not None else _channel_visits_from_silver_touchpoints(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_ids=current_conversion_ids if use_channel_group_filter else None,
    )
    silver_prev_channel_visits = None if fact_prev_channel_rollups is not None else _channel_visits_from_silver_touchpoints(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
    )
    silver_current_campaign_rollups = None if aggregate_campaign_rollups is not None else _campaign_driver_rollups_from_silver_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        conversion_ids=current_conversion_ids if use_channel_group_filter else None,
    )
    silver_prev_campaign_rollups = None if aggregate_campaign_rollups is not None else _campaign_driver_rollups_from_silver_facts(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_key=conversion_key,
        conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
    )

    # Aggregate by channel from paths (revenue/conversions)
    ch_rev: Dict[str, float] = {}
    ch_conv: Dict[str, int] = {}
    ch_outcomes: Dict[str, Dict[str, float]] = {}
    camp_rev: Dict[str, float] = {}
    camp_conv: Dict[str, int] = {}
    camp_outcomes: Dict[str, Dict[str, float]] = {}
    need_current_channel_paths = fact_current_channel_rollups is None and silver_current_channel_rollups is None
    need_previous_channel_paths = fact_prev_channel_rollups is None and silver_prev_channel_rollups is None
    need_current_campaign_paths = aggregate_campaign_rollups is None and silver_current_campaign_rollups is None
    need_previous_campaign_paths = aggregate_campaign_rollups is None and silver_prev_campaign_rollups is None

    if need_current_channel_paths or need_current_campaign_paths:
        for r in _iter_conversion_path_rows(
            db,
            date_from=dt_from,
            date_to=dt_to,
            conversion_key=conversion_key,
            conversion_ids=current_conversion_ids if use_channel_group_filter else None,
        ):
            if not _conversion_path_is_converted(r):
                continue
            payload = conversion_path_payload(r)
            val = conversion_path_revenue_value(r, dedupe_seen=dedupe_seen_current)
            tps = conversion_path_touchpoints(r)
            if need_current_channel_paths:
                for idx, tp in enumerate(tps):
                    ch = tp.get("channel", "unknown") if isinstance(tp, dict) else "unknown"
                    ch_rev[ch] = ch_rev.get(ch, 0) + val / max(len(tps), 1)
                    ch_conv[ch] = ch_conv.get(ch, 0) + (1 if idx == len(tps) - 1 else 0)  # last-touch count
                    metrics = ch_outcomes.setdefault(ch, _empty_outcomes())
                    _merge_outcomes(metrics, payload)
            if need_current_campaign_paths and tps:
                last = tps[-1] if isinstance(tps[-1], dict) else {}
                camp = last.get("campaign") or last.get("campaign_name") if isinstance(last, dict) else "unknown"
                if isinstance(camp, dict):
                    camp = camp.get("name", "unknown")
                camp = camp or "unknown"
                camp_rev[camp] = camp_rev.get(camp, 0) + val
                camp_conv[camp] = camp_conv.get(camp, 0) + 1
                metrics = camp_outcomes.setdefault(camp, _empty_outcomes())
                _merge_outcomes(metrics, payload)

    prev_ch_rev: Dict[str, float] = {}
    prev_ch_conv: Dict[str, int] = {}
    prev_camp_rev: Dict[str, float] = {}
    if need_previous_channel_paths or need_previous_campaign_paths:
        for r in _iter_conversion_path_rows(
            db,
            date_from=prev_from,
            date_to=prev_to,
            conversion_key=conversion_key,
            conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
        ):
            if not _conversion_path_is_converted(r):
                continue
            tps = conversion_path_touchpoints(r)
            val = conversion_path_revenue_value(r, dedupe_seen=dedupe_seen_prev)
            if need_previous_channel_paths:
                for tp in tps:
                    ch = tp.get("channel", "unknown") if isinstance(tp, dict) else "unknown"
                    prev_ch_rev[ch] = prev_ch_rev.get(ch, 0) + val / max(len(tps), 1)
                if tps:
                    ch_last = tps[-1].get("channel", "unknown") if isinstance(tps[-1], dict) else "unknown"
                    prev_ch_conv[ch_last] = prev_ch_conv.get(ch_last, 0) + 1
            if need_previous_campaign_paths and tps:
                last = tps[-1] if isinstance(tps[-1], dict) else {}
                camp = last.get("campaign") or last.get("campaign_name") or "unknown"
                if isinstance(camp, dict):
                    camp = camp.get("name", "unknown")
                prev_camp_rev[camp] = prev_camp_rev.get(camp, 0) + val

    if fact_current_channel_rollups is not None:
        current_metrics, current_fact_outcomes = fact_current_channel_rollups
        previous_metrics = (fact_prev_channel_rollups or ({}, {}))[0]
        ch_rev = {key: float(value.get("revenue", 0.0) or 0.0) for key, value in current_metrics.items()}
        ch_conv = {key: int(value.get("conversions", 0.0) or 0.0) for key, value in current_metrics.items()}
        ch_outcomes = current_fact_outcomes
        prev_ch_rev = {key: float(value.get("revenue", 0.0) or 0.0) for key, value in previous_metrics.items()}
        prev_ch_conv = {key: int(value.get("conversions", 0.0) or 0.0) for key, value in previous_metrics.items()}
        ch_visits = {key: int(value.get("visits", 0.0) or 0.0) for key, value in current_metrics.items()}
        prev_ch_visits = {key: int(value.get("visits", 0.0) or 0.0) for key, value in previous_metrics.items()}
    elif silver_current_channel_rollups is not None:
        ch_rev = {key: float(value or 0.0) for key, value in silver_current_channel_rollups["revenue"].items()}
        ch_conv = {key: int(value or 0) for key, value in silver_current_channel_rollups["conversions"].items()}
        ch_outcomes = dict(silver_current_channel_rollups["outcomes"])
        prev_ch_rev = {
            key: float(value or 0.0)
            for key, value in ((silver_prev_channel_rollups or {}).get("revenue") or {}).items()
        }
        prev_ch_conv = {
            key: int(value or 0)
            for key, value in ((silver_prev_channel_rollups or {}).get("conversions") or {}).items()
        }
        ch_visits = dict(silver_current_channel_visits or {})
        prev_ch_visits = dict(silver_prev_channel_visits or {})
    else:
        ch_visits: Dict[str, int] = {}
        prev_ch_visits: Dict[str, int] = {}
        for r in _iter_conversion_path_rows(
            db,
            first_touch_from=dt_to,
            last_touch_to=dt_from,
            conversion_ids=current_conversion_ids if use_channel_group_filter else None,
        ):
            for tp in conversion_path_touchpoints(r):
                ts = _parse_dt(tp.get("timestamp") or tp.get("ts"))
                if not ts or ts < dt_from or ts > dt_to:
                    continue
                ch = tp.get("channel", "unknown")
                ch_visits[ch] = ch_visits.get(ch, 0) + 1

        for r in _iter_conversion_path_rows(
            db,
            first_touch_from=prev_to,
            last_touch_to=prev_from,
            conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
        ):
            for tp in conversion_path_touchpoints(r):
                ts = _parse_dt(tp.get("timestamp") or tp.get("ts"))
                if not ts or ts < prev_from or ts > prev_to:
                    continue
                ch = tp.get("channel", "unknown")
                prev_ch_visits[ch] = prev_ch_visits.get(ch, 0) + 1

    channels = sorted(set(expense_by_channel.keys()) | set(ch_rev.keys()) | set(ch_visits.keys()))
    by_channel = []
    for ch in channels:
        spend = expense_by_channel.get(ch, 0)
        visits = ch_visits.get(ch, 0)
        rev = ch_rev.get(ch, 0)
        conv = ch_conv.get(ch, 0)
        prev_spend = prev_expense.get(ch, 0)
        prev_visits = prev_ch_visits.get(ch, 0)
        prev_rev = prev_ch_rev.get(ch, 0)
        prev_conv = prev_ch_conv.get(ch, 0)
        by_channel.append({
            "channel": ch,
            "spend": round(spend, 2),
            "visits": visits,
            "conversions": conv,
            "revenue": round(rev, 2),
            "delta_spend_pct": delta_pct(spend, prev_spend),
            "delta_visits_pct": delta_pct(float(visits), float(prev_visits)),
            "delta_conversions_pct": delta_pct(float(conv), float(prev_conv)),
            "delta_revenue_pct": delta_pct(rev, prev_rev),
            "outcomes": ch_outcomes.get(ch, _empty_outcomes()),
        })
    by_channel.sort(key=lambda x: -x["revenue"])

    if aggregate_campaign_rollups is not None:
        camp_rev = dict(aggregate_campaign_rollups["current_revenue"])
        camp_conv = dict(aggregate_campaign_rollups["current_conversions"])
        camp_outcomes = dict(aggregate_campaign_rollups["current_outcomes"])
        prev_camp_rev = dict(aggregate_campaign_rollups["previous_revenue"])
    elif silver_current_campaign_rollups is not None:
        camp_rev = {key: float(value or 0.0) for key, value in silver_current_campaign_rollups["revenue"].items()}
        camp_conv = {key: int(value or 0) for key, value in silver_current_campaign_rollups["conversions"].items()}
        camp_outcomes = dict(silver_current_campaign_rollups["outcomes"])
        prev_camp_rev = {
            key: float(value or 0.0)
            for key, value in ((silver_prev_campaign_rollups or {}).get("revenue") or {}).items()
        }
    campaigns_sorted = sorted(camp_rev.keys(), key=lambda c: -camp_rev[c])[:top_campaigns_n]
    by_campaign = [
        {
            "campaign": c,
            "revenue": round(camp_rev[c], 2),
            "conversions": camp_conv.get(c, 0),
            "delta_revenue_pct": delta_pct(camp_rev.get(c, 0), prev_camp_rev.get(c, 0)),
            "outcomes": camp_outcomes.get(c, _empty_outcomes()),
        }
        for c in campaigns_sorted
    ]

    # Biggest movers: largest |delta| across channels
    movers = []
    for x in by_channel:
        dp = x.get("delta_revenue_pct") or x.get("delta_spend_pct")
        if dp is not None:
            movers.append({"channel": x["channel"], "delta_pct": dp, "metric": "revenue" if abs(x.get("delta_revenue_pct") or 0) >= abs(x.get("delta_spend_pct") or 0) else "spend"})
    movers.sort(key=lambda m: -abs(m["delta_pct"]))
    biggest_movers = movers[:5]

    return {
        "by_channel": by_channel,
        "by_campaign": by_campaign,
        "biggest_movers": biggest_movers,
        "channel_group": channel_group,
        "date_from": date_from,
        "date_to": date_to,
    }


def _safe_div(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-9:
        return 0.0
    return numerator / denominator


def _aggregate_channel_metrics(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    timezone: str = "UTC",
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Dict[str, Dict[str, float]]:
    use_utc_daily_aggregates = (timezone or "UTC").upper() == "UTC"
    fact_rollups = None if conversion_ids is not None or not use_utc_daily_aggregates else _aggregate_channel_metrics_from_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
    )
    if fact_rollups is not None:
        return fact_rollups[0]
    silver_rollups = _aggregate_channel_metrics_from_silver_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    )
    if silver_rollups is not None:
        return silver_rollups
    dedupe_seen = set()
    metrics: Dict[str, Dict[str, float]] = {}
    query_from = dt_from.replace(tzinfo=None) if dt_from.tzinfo is not None else dt_from
    query_to = dt_to.replace(tzinfo=None) if dt_to.tzinfo is not None else dt_to
    for row in _iter_conversion_path_rows(
        db,
        date_from=query_from,
        date_to=query_to,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        touchpoints = conversion_path_touchpoints(row)
        value = conversion_path_revenue_value(row, dedupe_seen=dedupe_seen)
        for idx, tp in enumerate(touchpoints):
            channel = _touchpoint_channel(tp)
            entry = metrics.setdefault(channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
            entry["visits"] += 1.0
            if idx == len(touchpoints) - 1 and _conversion_path_is_converted(row):
                entry["conversions"] += 1.0
                entry["revenue"] += float(value or 0.0)
    return metrics


def _aggregate_daily_channel_revenue(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    timezone: str = "UTC",
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Dict[str, Dict[str, float]]:
    use_utc_daily_aggregates = (timezone or "UTC").upper() == "UTC"
    fact_rollups = None if conversion_ids is not None or not use_utc_daily_aggregates else _aggregate_daily_channel_revenue_from_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
    )
    if fact_rollups is not None:
        return fact_rollups
    silver_rollups = _aggregate_daily_channel_revenue_from_silver_facts(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    )
    if silver_rollups is not None:
        return silver_rollups
    dedupe_seen = set()
    out: Dict[str, Dict[str, float]] = {}
    query_from = dt_from.replace(tzinfo=None) if dt_from.tzinfo is not None else dt_from
    query_to = dt_to.replace(tzinfo=None) if dt_to.tzinfo is not None else dt_to
    for row in _iter_conversion_path_rows(
        db,
        date_from=query_from,
        date_to=query_to,
        conversion_key=conversion_key,
        conversion_ids=conversion_ids,
    ):
        if not _conversion_path_is_converted(row):
            continue
        touchpoints = conversion_path_touchpoints(row)
        if not touchpoints:
            continue
        channel = _touchpoint_channel(touchpoints[-1])
        ts = _as_datetime(getattr(row, "conversion_ts", None))
        if ts is None:
            continue
        day = ts.date().isoformat()
        entry = out.setdefault(channel, {})
        entry[day] = entry.get(day, 0.0) + conversion_path_revenue_value(row, dedupe_seen=dedupe_seen)
    return out


def _aggregate_channel_metrics_from_silver_facts(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Dict[str, float]]]:
    conversions = list(
        _iter_silver_conversion_rows(
            db,
            date_from=dt_from,
            date_to=dt_to,
            conversion_key=conversion_key,
            conversion_ids=conversion_ids,
        )
    )
    if not conversions:
        return None
    conversion_ids = [str(row.conversion_id or "") for row in conversions if str(row.conversion_id or "")]
    if not conversion_ids:
        return None
    revenue_by_conversion = {
        str(row.conversion_id): float(row.gross_revenue_total or 0.0)
        for row in conversions
        if str(row.conversion_id or "")
    }
    touchpoints_by_conversion: Dict[str, List[str]] = defaultdict(list)
    for row in _iter_visit_rows(db, conversion_ids=conversion_ids):
        conversion_id = str(row.conversion_id or "")
        if not conversion_id:
            continue
        touchpoints_by_conversion[conversion_id].append(str(row.channel or "unknown"))
    metrics: Dict[str, Dict[str, float]] = {}
    for conversion_id, touchpoints in touchpoints_by_conversion.items():
        if not touchpoints:
            continue
        for channel in touchpoints:
            entry = metrics.setdefault(channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
            entry["visits"] += 1.0
        last_channel = touchpoints[-1]
        entry = metrics.setdefault(last_channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
        entry["conversions"] += 1.0
        entry["revenue"] += float(revenue_by_conversion.get(conversion_id, 0.0) or 0.0)
    return metrics or None


def _channel_driver_rollups_from_silver_facts(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    conversions = list(
        _iter_silver_conversion_rows(
            db,
            date_from=dt_from,
            date_to=dt_to,
            conversion_key=conversion_key,
            conversion_ids=conversion_ids,
        )
    )
    if not conversions:
        return None
    conversion_ids = [str(row.conversion_id or "") for row in conversions if str(row.conversion_id or "")]
    if not conversion_ids:
        return None
    touchpoints_by_conversion: Dict[str, List[SimpleNamespace]] = defaultdict(list)
    for row in _iter_visit_rows(db, conversion_ids=conversion_ids):
        conversion_id = str(row.conversion_id or "")
        if not conversion_id:
            continue
        touchpoints_by_conversion[conversion_id].append(row)

    revenue: Dict[str, float] = {}
    conversions_by_channel: Dict[str, int] = {}
    outcomes: Dict[str, Dict[str, float]] = {}
    for row in conversions:
        conversion_id = str(row.conversion_id or "")
        touchpoints = touchpoints_by_conversion.get(conversion_id) or []
        if not touchpoints:
            continue
        share = float(row.gross_revenue_total or 0.0) / max(len(touchpoints), 1)
        for idx, tp in enumerate(touchpoints):
            channel = str(tp.channel or "unknown")
            revenue[channel] = revenue.get(channel, 0.0) + share
            if idx == len(touchpoints) - 1:
                conversions_by_channel[channel] = conversions_by_channel.get(channel, 0) + 1
            metric = outcomes.setdefault(channel, _empty_outcomes())
            _merge_silver_outcomes(metric, row)
    if not revenue and not conversions_by_channel and not outcomes:
        return None
    return {
        "revenue": revenue,
        "conversions": conversions_by_channel,
        "outcomes": outcomes,
    }


def _channel_visits_from_silver_touchpoints(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, int]]:
    visits: Dict[str, int] = {}
    seen = False
    for row in _iter_visit_rows(
        db,
        conversion_ids=conversion_ids,
        touchpoint_from=dt_from,
        touchpoint_to=dt_to,
    ):
        seen = True
        channel = str(row.channel or "unknown")
        visits[channel] = visits.get(channel, 0) + 1
    return visits if seen else None


def _campaign_driver_rollups_from_silver_facts(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    conversions = list(
        _iter_silver_conversion_rows(
            db,
            date_from=dt_from,
            date_to=dt_to,
            conversion_key=conversion_key,
            conversion_ids=conversion_ids,
        )
    )
    if not conversions:
        return None
    conversion_ids = [str(row.conversion_id or "") for row in conversions if str(row.conversion_id or "")]
    if not conversion_ids:
        return None
    last_touch_campaign: Dict[str, str] = {}
    for row in _iter_visit_rows(db, conversion_ids=conversion_ids):
        conversion_id = str(row.conversion_id or "")
        if not conversion_id:
            continue
        campaign = row.campaign
        last_touch_campaign[conversion_id] = str(campaign or "unknown")

    revenue: Dict[str, float] = {}
    conversions_by_campaign: Dict[str, int] = {}
    outcomes: Dict[str, Dict[str, float]] = {}
    for row in conversions:
        conversion_id = str(row.conversion_id or "")
        campaign = last_touch_campaign.get(conversion_id)
        if not campaign:
            continue
        revenue[campaign] = revenue.get(campaign, 0.0) + float(row.gross_revenue_total or 0.0)
        conversions_by_campaign[campaign] = conversions_by_campaign.get(campaign, 0) + 1
        metric = outcomes.setdefault(campaign, _empty_outcomes())
        _merge_silver_outcomes(metric, row)
    if not revenue and not conversions_by_campaign and not outcomes:
        return None
    return {
        "revenue": revenue,
        "conversions": conversions_by_campaign,
        "outcomes": outcomes,
    }


def _aggregate_daily_channel_revenue_from_silver_facts(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
    conversion_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Dict[str, float]]]:
    conversions = list(
        _iter_silver_conversion_rows(
            db,
            date_from=dt_from,
            date_to=dt_to,
            conversion_key=conversion_key,
            conversion_ids=conversion_ids,
        )
    )
    if not conversions:
        return None
    conversion_ids = [str(row.conversion_id or "") for row in conversions if str(row.conversion_id or "")]
    if not conversion_ids:
        return None
    last_touch_channel: Dict[str, str] = {}
    for row in _iter_visit_rows(db, conversion_ids=conversion_ids):
        conversion_id = str(row.conversion_id or "")
        if not conversion_id:
            continue
        last_touch_channel[conversion_id] = str(row.channel or "unknown")
    out: Dict[str, Dict[str, float]] = {}
    for row in conversions:
        conversion_id = str(row.conversion_id or "")
        channel = last_touch_channel.get(conversion_id)
        if not channel:
            continue
        ts = _as_datetime(row.conversion_ts)
        if ts is None:
            continue
        day = ts.date().isoformat()
        entry = out.setdefault(channel, {})
        entry[day] = entry.get(day, 0.0) + float(row.gross_revenue_total or 0.0)
    return out if out else None


def get_overview_trend_insights(
    db: Session,
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    conversion_key: Optional[str] = None,
    channel_group: Optional[str] = None,
) -> Dict[str, Any]:
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to, timezone)
    period_span = dt_to - dt_from
    prev_to = dt_from - timedelta(microseconds=1)
    prev_from = prev_to - period_span
    use_channel_group_filter = bool(str(channel_group or "").strip())
    current_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        channel_group=channel_group,
    )
    previous_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        conversion_key=conversion_key,
        channel_group=channel_group,
    )

    current = _aggregate_channel_metrics(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        timezone=timezone,
        conversion_key=conversion_key,
        conversion_ids=current_conversion_ids if use_channel_group_filter else None,
    )
    previous = _aggregate_channel_metrics(
        db,
        dt_from=prev_from,
        dt_to=prev_to,
        timezone=timezone,
        conversion_key=conversion_key,
        conversion_ids=previous_conversion_ids if use_channel_group_filter else None,
    )

    curr_visits = sum(item["visits"] for item in current.values())
    prev_visits = sum(item["visits"] for item in previous.values())
    curr_conversions = sum(item["conversions"] for item in current.values())
    prev_conversions = sum(item["conversions"] for item in previous.values())
    curr_revenue = sum(item["revenue"] for item in current.values())
    prev_revenue = sum(item["revenue"] for item in previous.values())

    prev_cvr = _safe_div(prev_conversions, prev_visits)
    curr_cvr = _safe_div(curr_conversions, curr_visits)
    prev_rpc = _safe_div(prev_revenue, prev_conversions)
    curr_rpc = _safe_div(curr_revenue, curr_conversions)
    revenue_delta = curr_revenue - prev_revenue
    traffic_effect = (curr_visits - prev_visits) * prev_cvr * prev_rpc
    cvr_effect = curr_visits * (curr_cvr - prev_cvr) * prev_rpc
    value_effect = curr_visits * curr_cvr * (curr_rpc - prev_rpc)
    mix_effect = revenue_delta - traffic_effect - cvr_effect - value_effect

    decomposition = {
        "current": {
            "visits": round(curr_visits, 2),
            "conversions": round(curr_conversions, 2),
            "revenue": round(curr_revenue, 2),
            "cvr": round(curr_cvr, 6),
            "revenue_per_conversion": round(curr_rpc, 2),
        },
        "previous": {
            "visits": round(prev_visits, 2),
            "conversions": round(prev_conversions, 2),
            "revenue": round(prev_revenue, 2),
            "cvr": round(prev_cvr, 6),
            "revenue_per_conversion": round(prev_rpc, 2),
        },
        "revenue_delta": round(revenue_delta, 2),
        "factors": [
            {"key": "traffic", "label": "Traffic effect", "value": round(traffic_effect, 2)},
            {"key": "cvr", "label": "Conversion-rate effect", "value": round(cvr_effect, 2)},
            {"key": "value", "label": "Value-per-conversion effect", "value": round(value_effect, 2)},
            {"key": "mix", "label": "Mix effect", "value": round(mix_effect, 2)},
        ],
    }

    momentum_window_days = min(7, max(3, ((dt_to.date() - dt_from.date()).days + 1) // 2 or 3))
    momentum_current_from = dt_to - timedelta(days=momentum_window_days - 1)
    momentum_prev_to = momentum_current_from - timedelta(microseconds=1)
    momentum_prev_from = momentum_prev_to - timedelta(days=momentum_window_days - 1)
    momentum_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=momentum_prev_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        channel_group=channel_group,
    )
    daily_revenue = _aggregate_daily_channel_revenue(
        db,
        dt_from=momentum_prev_from,
        dt_to=dt_to,
        timezone=timezone,
        conversion_key=conversion_key,
        conversion_ids=momentum_conversion_ids if use_channel_group_filter else None,
    )
    spark_days = [(dt_to.date() - timedelta(days=idx)).isoformat() for idx in range(momentum_window_days * 2 - 1, -1, -1)]
    momentum_rows: List[Dict[str, Any]] = []
    for channel, series in daily_revenue.items():
        current_total = 0.0
        previous_total = 0.0
        sparkline: List[float] = []
        for day in spark_days:
            value = float(series.get(day, 0.0))
            sparkline.append(round(value, 2))
            day_dt = _parse_dt(day)
            if day_dt is None:
                continue
            if momentum_current_from.date() <= day_dt.date() <= dt_to.date():
                current_total += value
            elif momentum_prev_from.date() <= day_dt.date() <= momentum_prev_to.date():
                previous_total += value
        delta_value = current_total - previous_total
        momentum_rows.append(
            {
                "channel": channel,
                "current_revenue": round(current_total, 2),
                "previous_revenue": round(previous_total, 2),
                "delta_revenue": round(delta_value, 2),
                "delta_revenue_pct": round(delta_pct(current_total, previous_total), 1) if delta_pct(current_total, previous_total) is not None else None,
                "sparkline": sparkline,
            }
        )
    rising = sorted(momentum_rows, key=lambda row: (row["delta_revenue"], row["current_revenue"]), reverse=True)[:5]
    falling = sorted(momentum_rows, key=lambda row: (row["delta_revenue"], -row["current_revenue"]))[:5]

    mix_rows: List[Dict[str, Any]] = []
    all_channels = set(current.keys()) | set(previous.keys())
    for channel in all_channels:
        curr_metrics = current.get(channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
        prev_metrics = previous.get(channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
        curr_rev_share = _safe_div(curr_metrics["revenue"], curr_revenue)
        prev_rev_share = _safe_div(prev_metrics["revenue"], prev_revenue)
        curr_visit_share = _safe_div(curr_metrics["visits"], curr_visits)
        prev_visit_share = _safe_div(prev_metrics["visits"], prev_visits)
        curr_conv_share = _safe_div(curr_metrics["conversions"], curr_conversions)
        prev_conv_share = _safe_div(prev_metrics["conversions"], prev_conversions)
        mix_rows.append(
            {
                "channel": channel,
                "revenue_share_current": round(curr_rev_share, 6),
                "revenue_share_previous": round(prev_rev_share, 6),
                "revenue_share_delta_pp": round((curr_rev_share - prev_rev_share) * 100.0, 2),
                "visit_share_current": round(curr_visit_share, 6),
                "visit_share_previous": round(prev_visit_share, 6),
                "visit_share_delta_pp": round((curr_visit_share - prev_visit_share) * 100.0, 2),
                "conversion_share_current": round(curr_conv_share, 6),
                "conversion_share_previous": round(prev_conv_share, 6),
                "conversion_share_delta_pp": round((curr_conv_share - prev_conv_share) * 100.0, 2),
            }
        )
    mix_shift = sorted(
        mix_rows,
        key=lambda row: max(abs(row["revenue_share_delta_pp"]), abs(row["visit_share_delta_pp"]), abs(row["conversion_share_delta_pp"])),
        reverse=True,
    )[:6]

    return {
        "date_from": date_from,
        "date_to": date_to,
        "decomposition": decomposition,
        "momentum": {
            "window_days": momentum_window_days,
            "rising": rising,
            "falling": falling,
        },
        "mix_shift": mix_shift,
        "channel_group": channel_group,
    }


def get_overview_funnels(
    db: Session,
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    conversion_key: Optional[str] = None,
    limit: int = 5,
    channel_group: Optional[str] = None,
) -> Dict[str, Any]:
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to, timezone)
    use_channel_group_filter = bool(str(channel_group or "").strip())
    current_conversion_ids = _filtered_conversion_ids_for_channel_group(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        channel_group=channel_group,
    )
    aggregate_result = None if use_channel_group_filter else _overview_funnels_from_daily_aggregates(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        conversion_key=conversion_key,
        limit=limit,
    )
    if aggregate_result is not None:
        return aggregate_result

    dedupe_seen = set()

    aggs: Dict[str, Dict[str, Any]] = {}
    total_conversions = 0
    outcomes = _empty_outcomes()
    path_length_counts: Dict[int, int] = defaultdict(int)
    for row in _iter_conversion_path_rows(
        db,
        date_from=dt_from,
        date_to=dt_to,
        conversion_key=conversion_key,
        conversion_ids=current_conversion_ids if use_channel_group_filter else None,
    ):
        if not _conversion_path_is_converted(row):
            continue
        payload = conversion_path_payload(row)
        _merge_outcomes(outcomes, payload)
        steps = _path_steps_from_payload(payload)
        path_key = " > ".join(steps)
        revenue = conversion_path_revenue_value(row, dedupe_seen=dedupe_seen)
        latency_days = None
        first_ts = _as_datetime(getattr(row, "first_touch_ts", None))
        conversion_ts = _as_datetime(getattr(row, "conversion_ts", None))
        if first_ts is not None and conversion_ts is not None and conversion_ts >= first_ts:
            latency_days = (conversion_ts - first_ts).total_seconds() / 86400.0

        entry = aggs.setdefault(
            path_key,
            {
                "path": path_key,
                "steps": steps,
                "conversions": 0,
                "revenue": 0.0,
                "latencies": [],
                "path_length": len(steps),
                "ends_with_direct": steps[-1].strip().lower() == "direct",
            },
        )
        entry["conversions"] += 1
        entry["revenue"] += float(revenue or 0.0)
        if latency_days is not None:
            entry["latencies"].append(latency_days)
        total_conversions += 1
        path_length_counts[len(steps)] += 1

    def _format_item(entry: Dict[str, Any]) -> Dict[str, Any]:
        latencies = sorted(float(value) for value in entry.get("latencies") or [])
        median_days = None
        if latencies:
            mid = len(latencies) // 2
            if len(latencies) % 2 == 1:
                median_days = latencies[mid]
            else:
                median_days = (latencies[mid - 1] + latencies[mid]) / 2.0
        conversions = int(entry.get("conversions") or 0)
        revenue = float(entry.get("revenue") or 0.0)
        return {
            "path": entry["path"],
            "path_display": " -> ".join(_display_channel_token(step) for step in entry.get("steps") or []),
            "steps": entry.get("steps") or [],
            "conversions": conversions,
            "share": round((conversions / total_conversions), 6) if total_conversions > 0 else 0.0,
            "revenue": round(revenue, 2),
            "revenue_per_conversion": round((revenue / conversions), 2) if conversions > 0 else 0.0,
            "median_days_to_convert": round(median_days, 2) if median_days is not None else None,
            "path_length": int(entry.get("path_length") or 0),
            "ends_with_direct": bool(entry.get("ends_with_direct")),
        }

    items = [_format_item(entry) for entry in aggs.values()]
    by_conversions = sorted(items, key=lambda item: (item["conversions"], item["revenue"]), reverse=True)
    by_revenue = sorted(items, key=lambda item: (item["revenue"], item["conversions"]), reverse=True)
    speed_candidates = [item for item in items if item["median_days_to_convert"] is not None]
    direct_speed_candidates = [item for item in speed_candidates if item.get("ends_with_direct")]
    if direct_speed_candidates:
        speed_candidates = direct_speed_candidates
    by_speed = sorted(
        speed_candidates,
        key=lambda item: (item["median_days_to_convert"], -item["conversions"], -item["revenue"]),
    )

    top_converting = by_conversions[: max(1, limit)]
    top_revenue = by_revenue[: max(1, limit)]
    fastest = by_speed[: max(1, limit)]

    return {
        "date_from": date_from,
        "date_to": date_to,
        "channel_group": channel_group,
        "summary": {
            "total_conversions": total_conversions,
            "net_conversions": round(outcomes["net_conversions"], 2),
            "gross_conversions": round(outcomes["gross_conversions"], 2),
            "net_revenue": round(outcomes["net_value"], 2),
            "gross_revenue": round(outcomes["gross_value"], 2),
            "view_through_conversions": round(outcomes["view_through_conversions"], 2),
            "click_through_conversions": round(outcomes["click_through_conversions"], 2),
            "mixed_path_conversions": round(outcomes["mixed_path_conversions"], 2),
            "distinct_paths": len(items),
            "top_paths_conversion_share": round(sum(item["conversions"] for item in top_converting) / total_conversions, 6) if total_conversions > 0 else 0.0,
            "median_path_length": _weighted_median(path_length_counts),
        },
        "tabs": {
            "conversions": top_converting,
            "revenue": top_revenue,
            "speed": fastest,
        },
    }


# ---------------------------------------------------------------------------
# Alerts: open + recent resolved with deep links
# ---------------------------------------------------------------------------

def get_overview_alerts(
    db: Session,
    scope: Optional[str] = None,
    status_filter: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Latest alert_events (open + recent resolved). Deep links point to DQ drilldown or attribution.
    """
    q = db.query(AlertEvent).join(AlertRule, AlertEvent.rule_id == AlertRule.id)
    if scope:
        q = q.filter(AlertRule.scope == scope)
    if status_filter:
        q = q.filter(AlertEvent.status == status_filter)
    else:
        q = q.filter(AlertEvent.status.in_(["open", "ack", "resolved"]))
    events = q.order_by(AlertEvent.ts_detected.desc()).limit(limit).all()

    base_url = "/dashboard"  # or from config
    out = []
    for ev in events:
        rule = ev.rule
        ctx = ev.context_json or {}
        kpi_key = ctx.get("kpi_key") if isinstance(ctx, dict) else (rule.kpi_key if rule else None) or None
        link = f"{base_url}/data-quality?alert_id={ev.id}"
        if kpi_key:
            link += f"&kpi={kpi_key}"
        out.append({
            "id": ev.id,
            "rule_id": ev.rule_id,
            "rule_name": rule.name if rule else None,
            "ts_detected": ev.ts_detected.isoformat() if hasattr(ev.ts_detected, "isoformat") else str(ev.ts_detected),
            "severity": ev.severity,
            "title": ev.title,
            "message": ev.message,
            "status": ev.status,
            "context": ev.context_json,
            "related_entities": ev.related_entities_json,
            "deep_link": link,
        })
    return {"alerts": out, "total": len(out)}
