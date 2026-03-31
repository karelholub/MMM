from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from sqlalchemy.orm import Session
from .models_config_dq import (
    ChannelPerformanceDaily,
    JourneyDefinition,
    JourneyPathDaily,
    SilverConversionFact,
    SilverTouchpointFact,
)
from app.services_metrics import (
    SUPPORTED_KPIS,
    journey_outcome_summary,
    journey_revenue_value,
    metric_value,
    summarize_rows,
)



def _safe_tz(timezone_name: Optional[str]) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _parse_date(value: str) -> date:
    return datetime.fromisoformat(value[:10]).date()


def _local_date_from_ts(ts_value: Any, tz: ZoneInfo) -> Optional[date]:
    if not ts_value:
        return None
    try:
        raw = str(ts_value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(tz).date()


def _bucket_start(d: date, grain: str) -> date:
    if grain == "weekly":
        return d - timedelta(days=d.weekday())  # Monday start
    return d


def _bucket_keys_for_period(start_d: date, end_d: date, grain: str) -> List[str]:
    keys: List[str] = []
    seen = set()
    cursor = start_d
    while cursor <= end_d:
        key = _bucket_start(cursor, grain).isoformat()
        if key not in seen:
            seen.add(key)
            keys.append(key)
        cursor += timedelta(days=1)
    return keys


def resolve_period_windows(
    date_from: str,
    date_to: str,
    grain: str = "auto",
) -> Dict[str, Any]:
    if grain not in {"auto", "daily", "weekly"}:
        raise ValueError("grain must be one of: auto, daily, weekly")
    start_d = _parse_date(date_from)
    end_d = _parse_date(date_to)
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    length_days = (end_d - start_d).days + 1
    prev_end = start_d - timedelta(days=1)
    prev_start = prev_end - timedelta(days=length_days - 1)
    resolved_grain = "daily" if (grain == "auto" and length_days <= 45) else ("weekly" if grain == "auto" else grain)
    return {
        "current_period": {
            "date_from": start_d.isoformat(),
            "date_to": end_d.isoformat(),
            "grain": resolved_grain,
        },
        "previous_period": {
            "date_from": prev_start.isoformat(),
            "date_to": prev_end.isoformat(),
        },
        "length_days": length_days,
    }


def _add_metric_rollup(
    store: Dict[str, Dict[str, Dict[str, float]]],
    dim_key: str,
    bucket_key: str,
    *,
    spend: float = 0.0,
    visits: float = 0.0,
    conversions: float = 0.0,
    revenue: float = 0.0,
) -> None:
    dim_entry = store.setdefault(dim_key, {})
    bucket_entry = dim_entry.setdefault(bucket_key, {"spend": 0.0, "visits": 0.0, "conversions": 0.0, "revenue": 0.0})
    bucket_entry["spend"] += spend
    bucket_entry["visits"] += visits
    bucket_entry["conversions"] += conversions
    bucket_entry["revenue"] += revenue


def _empty_outcome_metrics() -> Dict[str, float]:
    return {
        "gross_conversions": 0.0,
        "net_conversions": 0.0,
        "gross_revenue": 0.0,
        "net_revenue": 0.0,
        "refunded_value": 0.0,
        "cancelled_value": 0.0,
        "invalid_leads": 0.0,
        "valid_leads": 0.0,
        "click_through_conversions": 0.0,
        "view_through_conversions": 0.0,
        "mixed_path_conversions": 0.0,
    }


def _path_type(journey: Dict[str, Any]) -> str:
    summary = ((journey.get("meta") or {}).get("interaction_summary") or {}) if isinstance(journey.get("meta"), dict) else {}
    path_type = summary.get("path_type") or journey.get("interaction_path_type")
    if isinstance(path_type, str) and path_type:
        return path_type
    touchpoints = journey.get("touchpoints") or []
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


def _merge_outcome_metrics(target: Dict[str, float], outcome: Dict[str, float], path_type: str) -> None:
    target["gross_conversions"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
    target["net_conversions"] += float(outcome.get("net_conversions", 0.0) or 0.0)
    target["gross_revenue"] += float(outcome.get("gross_value", 0.0) or 0.0)
    target["net_revenue"] += float(outcome.get("net_value", 0.0) or 0.0)
    target["refunded_value"] += float(outcome.get("refunded_value", 0.0) or 0.0)
    target["cancelled_value"] += float(outcome.get("cancelled_value", 0.0) or 0.0)
    target["invalid_leads"] += float(outcome.get("invalid_leads", 0.0) or 0.0)
    target["valid_leads"] += float(outcome.get("valid_leads", 0.0) or 0.0)
    selected_count = float(outcome.get("net_conversions", 0.0) or 0.0)
    if path_type == "click_through":
        target["click_through_conversions"] += selected_count
    elif path_type == "view_through":
        target["view_through_conversions"] += selected_count
    elif path_type == "mixed_path":
        target["mixed_path_conversions"] += selected_count



def _expense_records(expenses: Any) -> Iterable[Any]:
    if isinstance(expenses, dict):
        return expenses.values()
    return expenses or []


def _expense_fields(exp: Any) -> Tuple[Optional[str], Optional[str], float, str]:
    if isinstance(exp, dict):
        channel = exp.get("channel")
        start = exp.get("service_period_start")
        amount = float(exp.get("converted_amount") or exp.get("amount") or 0.0)
        status = str(exp.get("status", "active"))
        return channel, start, amount, status
    channel = getattr(exp, "channel", None)
    start = getattr(exp, "service_period_start", None)
    amount = float(getattr(exp, "converted_amount", None) or getattr(exp, "amount", 0.0) or 0.0)
    status = str(getattr(exp, "status", "active"))
    return channel, start, amount, status


def _single_active_journey_definition_id(db: Session, *, conversion_key: Optional[str] = None) -> Optional[str]:
    query = (
        db.query(JourneyDefinition)
        .filter(JourneyDefinition.is_archived == False)  # noqa: E712
        .order_by(JourneyDefinition.updated_at.desc())
    )
    if conversion_key:
        query = query.filter(JourneyDefinition.conversion_kpi_id == conversion_key)
    rows = query.limit(2).all()
    if len(rows) != 1:
        return None
    return str(rows[0].id)


def build_campaign_aggregate_overlay(
    db: Session,
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    compare: bool = True,
    channels: Optional[List[str]] = None,
    conversion_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if (timezone or "UTC").upper() != "UTC":
        return None
    windows = resolve_period_windows(date_from, date_to, "daily")
    resolved_grain = windows["current_period"]["grain"]
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    journey_definition_id = _single_active_journey_definition_id(db, conversion_key=conversion_key)
    if not journey_definition_id:
        return None

    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    rows = (
        db.query(JourneyPathDaily)
        .filter(
            JourneyPathDaily.journey_definition_id == journey_definition_id,
            JourneyPathDaily.date >= (prev_from if compare else curr_from),
            JourneyPathDaily.date <= curr_to,
        )
        .all()
    )
    if not rows:
        return _build_campaign_aggregate_overlay_from_silver(
            db,
            curr_from=curr_from,
            curr_to=curr_to,
            prev_from=prev_from,
            prev_to=prev_to,
            compare=compare,
            resolved_grain=resolved_grain,
            allowed_channels=allowed_channels,
            filter_channels=filter_channels,
            conversion_key=conversion_key,
        )

    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    curr_outcomes: Dict[str, Dict[str, float]] = {}
    prev_outcomes: Dict[str, Dict[str, float]] = {}
    meta: Dict[str, Dict[str, Any]] = {}

    def _campaign_key(channel: str, campaign: Optional[str]) -> str:
        return f"{channel}:{campaign}" if campaign else channel

    def _outcome_entry(target: Dict[str, Dict[str, float]], key: str) -> Dict[str, float]:
        return target.setdefault(key, _empty_outcome_metrics())

    for row in rows:
        channel = str(row.last_touch_channel or "unknown")
        if filter_channels and channel not in allowed_channels:
            continue
        campaign = str(row.campaign_id or "").strip() or None
        key = _campaign_key(channel, campaign)
        meta.setdefault(
            key,
            {
                "campaign_id": key,
                "campaign_name": campaign,
                "channel": channel,
                "platform": None,
            },
        )
        bucket = _bucket_start(row.date, resolved_grain).isoformat()
        target_store = curr_store if curr_from <= row.date <= curr_to else (prev_store if compare and prev_from <= row.date <= prev_to else None)
        target_outcomes = curr_outcomes if curr_from <= row.date <= curr_to else (prev_outcomes if compare and prev_from <= row.date <= prev_to else None)
        if target_store is None or target_outcomes is None:
            continue
        _add_metric_rollup(
            target_store,
            key,
            bucket,
            conversions=float(row.count_conversions or 0.0),
            revenue=float(row.gross_revenue_total or 0.0),
        )
        outcome = _outcome_entry(target_outcomes, key)
        outcome["gross_conversions"] += float(row.gross_conversions_total or 0.0)
        outcome["net_conversions"] += float(row.net_conversions_total or 0.0)
        outcome["gross_revenue"] += float(row.gross_revenue_total or 0.0)
        outcome["net_revenue"] += float(row.net_revenue_total or 0.0)
        outcome["click_through_conversions"] += float(row.click_through_conversions_total or 0.0)
        outcome["view_through_conversions"] += float(row.view_through_conversions_total or 0.0)
        outcome["mixed_path_conversions"] += float(row.mixed_path_conversions_total or 0.0)

    if not curr_store and not prev_store:
        return None
    return {
        "current_store": curr_store,
        "previous_store": prev_store,
        "current_outcomes": curr_outcomes,
        "previous_outcomes": prev_outcomes,
        "meta": meta,
    }


def _build_campaign_aggregate_overlay_from_silver(
    db: Session,
    *,
    curr_from: date,
    curr_to: date,
    prev_from: date,
    prev_to: date,
    compare: bool,
    resolved_grain: str,
    allowed_channels: Set[str],
    filter_channels: bool,
    conversion_key: Optional[str],
) -> Optional[Dict[str, Any]]:
    start_day = prev_from if compare else curr_from
    day_start = datetime.combine(start_day, datetime.min.time())
    day_end = datetime.combine(curr_to + timedelta(days=1), datetime.min.time())
    conversion_rows = (
        db.query(
            SilverConversionFact.conversion_id,
            SilverConversionFact.conversion_key,
            SilverConversionFact.conversion_ts,
            SilverConversionFact.interaction_path_type,
            SilverConversionFact.gross_conversions_total,
            SilverConversionFact.net_conversions_total,
            SilverConversionFact.gross_revenue_total,
            SilverConversionFact.net_revenue_total,
        )
        .filter(
            SilverConversionFact.conversion_ts >= day_start,
            SilverConversionFact.conversion_ts < day_end,
        )
        .all()
    )
    if not conversion_rows:
        return None

    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    curr_outcomes: Dict[str, Dict[str, float]] = {}
    prev_outcomes: Dict[str, Dict[str, float]] = {}
    meta: Dict[str, Dict[str, Any]] = {}

    conversion_ids = [str(row[0] or "") for row in conversion_rows if str(row[0] or "")]
    last_touch: Dict[str, Tuple[str, Optional[str]]] = {}
    if conversion_ids:
        for conversion_id, channel, campaign, _ordinal in (
            db.query(
                SilverTouchpointFact.conversion_id,
                SilverTouchpointFact.channel,
                SilverTouchpointFact.campaign,
                SilverTouchpointFact.ordinal,
            )
            .filter(SilverTouchpointFact.conversion_id.in_(conversion_ids))
            .order_by(SilverTouchpointFact.conversion_id.asc(), SilverTouchpointFact.ordinal.asc())
            .all()
        ):
            last_touch[str(conversion_id or "")] = (
                str(channel or "unknown"),
                str(campaign).strip() if campaign not in (None, "") else None,
            )

    def _campaign_key(channel: str, campaign: Optional[str]) -> str:
        return f"{channel}:{campaign}" if campaign else channel

    for row in conversion_rows:
        conversion_id = str(row[0] or "")
        row_conversion_key = str(row[1] or "").strip() or None
        conversion_ts = row[2]
        if not isinstance(conversion_ts, datetime):
            continue
        if conversion_key is not None and row_conversion_key != conversion_key:
            continue
        day = conversion_ts.date()
        channel, campaign = last_touch.get(conversion_id, ("unknown", None))
        if filter_channels and channel not in allowed_channels:
            continue
        key = _campaign_key(channel, campaign)
        meta.setdefault(
            key,
            {
                "campaign_id": key,
                "campaign_name": campaign,
                "channel": channel,
                "platform": None,
            },
        )
        bucket = _bucket_start(day, resolved_grain).isoformat()
        if curr_from <= day <= curr_to:
            target_store = curr_store
            target_outcomes = curr_outcomes
        elif compare and prev_from <= day <= prev_to:
            target_store = prev_store
            target_outcomes = prev_outcomes
        else:
            continue
        _add_metric_rollup(
            target_store,
            key,
            bucket,
            conversions=float(row[4] or 0.0),
            revenue=float(row[6] or 0.0),
        )
        outcome = target_outcomes.setdefault(key, _empty_outcome_metrics())
        outcome["gross_conversions"] += float(row[4] or 0.0)
        outcome["net_conversions"] += float(row[5] or 0.0)
        outcome["gross_revenue"] += float(row[6] or 0.0)
        outcome["net_revenue"] += float(row[7] or 0.0)
        path_type = str(row[3] or "").strip().lower()
        net_conversions = float(row[5] or 0.0)
        if path_type == "click_through":
            outcome["click_through_conversions"] += net_conversions
        elif path_type == "view_through":
            outcome["view_through_conversions"] += net_conversions
        elif path_type == "mixed_path":
            outcome["mixed_path_conversions"] += net_conversions

    if not curr_store and not prev_store:
        return None
    return {
        "current_store": curr_store,
        "previous_store": prev_store,
        "current_outcomes": curr_outcomes,
        "previous_outcomes": prev_outcomes,
        "meta": meta,
    }


def build_channel_aggregate_overlay(
    db: Session,
    *,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    compare: bool = True,
    channels: Optional[List[str]] = None,
    conversion_key: Optional[str] = None,
    grain: str = "daily",
) -> Optional[Dict[str, Any]]:
    if (timezone or "UTC").upper() != "UTC":
        return None
    windows = resolve_period_windows(date_from, date_to, grain)
    resolved_grain = windows["current_period"]["grain"]
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)

    rows = (
        db.query(ChannelPerformanceDaily)
        .filter(
            ChannelPerformanceDaily.date >= (prev_from if compare else curr_from),
            ChannelPerformanceDaily.date <= curr_to,
        )
        .all()
    )
    if not rows:
        return _build_channel_aggregate_overlay_from_silver(
            db,
            curr_from=curr_from,
            curr_to=curr_to,
            prev_from=prev_from,
            prev_to=prev_to,
            compare=compare,
            resolved_grain=resolved_grain,
            allowed_channels=allowed_channels,
            filter_channels=filter_channels,
            conversion_key=conversion_key,
        )

    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    curr_outcomes: Dict[str, Dict[str, float]] = {}
    prev_outcomes: Dict[str, Dict[str, float]] = {}

    for row in rows:
        channel = str(row.channel or "unknown")
        if filter_channels and channel not in allowed_channels:
            continue
        bucket = _bucket_start(row.date, resolved_grain).isoformat()
        in_current = curr_from <= row.date <= curr_to
        in_previous = compare and prev_from <= row.date <= prev_to
        if not in_current and not in_previous:
            continue
        target_store = curr_store if in_current else prev_store
        target_outcomes = curr_outcomes if in_current else prev_outcomes

        if row.conversion_key is None:
            _add_metric_rollup(
                target_store,
                channel,
                bucket,
                visits=float(row.visits_total or 0.0),
            )
            if conversion_key is not None:
                continue

        if conversion_key is None and row.conversion_key is not None:
            continue
        if conversion_key is not None and str(row.conversion_key or "") != conversion_key:
            continue

        _add_metric_rollup(
            target_store,
            channel,
            bucket,
            conversions=float(row.count_conversions or 0.0),
            revenue=float(row.gross_revenue_total or 0.0),
        )
        outcome = target_outcomes.setdefault(channel, _empty_outcome_metrics())
        outcome["gross_conversions"] += float(row.gross_conversions_total or 0.0)
        outcome["net_conversions"] += float(row.net_conversions_total or 0.0)
        outcome["gross_revenue"] += float(row.gross_revenue_total or 0.0)
        outcome["net_revenue"] += float(row.net_revenue_total or 0.0)
        outcome["view_through_conversions"] += float(row.view_through_conversions_total or 0.0)
        outcome["click_through_conversions"] += float(row.click_through_conversions_total or 0.0)
        outcome["mixed_path_conversions"] += float(row.mixed_path_conversions_total or 0.0)

    if not curr_store and not prev_store:
        return None
    return {
        "current_store": curr_store,
        "previous_store": prev_store,
        "current_outcomes": curr_outcomes,
        "previous_outcomes": prev_outcomes,
    }


def _build_channel_aggregate_overlay_from_silver(
    db: Session,
    *,
    curr_from: date,
    curr_to: date,
    prev_from: date,
    prev_to: date,
    compare: bool,
    resolved_grain: str,
    allowed_channels: Set[str],
    filter_channels: bool,
    conversion_key: Optional[str],
) -> Optional[Dict[str, Any]]:
    touchpoint_start = prev_from if compare else curr_from
    day_start = datetime.combine(touchpoint_start, datetime.min.time())
    day_end = datetime.combine(curr_to + timedelta(days=1), datetime.min.time())

    touchpoint_rows = (
        db.query(
            SilverTouchpointFact.touchpoint_ts,
            SilverTouchpointFact.channel,
        )
        .filter(
            SilverTouchpointFact.touchpoint_ts.isnot(None),
            SilverTouchpointFact.touchpoint_ts >= day_start,
            SilverTouchpointFact.touchpoint_ts < day_end,
        )
        .all()
    )
    conversion_rows = (
        db.query(
            SilverConversionFact.conversion_id,
            SilverConversionFact.conversion_key,
            SilverConversionFact.conversion_ts,
            SilverConversionFact.interaction_path_type,
            SilverConversionFact.gross_conversions_total,
            SilverConversionFact.net_conversions_total,
            SilverConversionFact.gross_revenue_total,
            SilverConversionFact.net_revenue_total,
        )
        .filter(
            SilverConversionFact.conversion_ts >= day_start,
            SilverConversionFact.conversion_ts < day_end,
        )
        .all()
    )
    if not touchpoint_rows and not conversion_rows:
        return None

    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    curr_outcomes: Dict[str, Dict[str, float]] = {}
    prev_outcomes: Dict[str, Dict[str, float]] = {}

    conversion_ids = [str(row[0] or "") for row in conversion_rows if str(row[0] or "")]
    last_touch_channel: Dict[str, str] = {}
    if conversion_ids:
        for conversion_id, channel, _ordinal in (
            db.query(
                SilverTouchpointFact.conversion_id,
                SilverTouchpointFact.channel,
                SilverTouchpointFact.ordinal,
            )
            .filter(SilverTouchpointFact.conversion_id.in_(conversion_ids))
            .order_by(SilverTouchpointFact.conversion_id.asc(), SilverTouchpointFact.ordinal.asc())
            .all()
        ):
            last_touch_channel[str(conversion_id or "")] = str(channel or "unknown")

    for touchpoint_ts, channel_raw in touchpoint_rows:
        if not isinstance(touchpoint_ts, datetime):
            continue
        day = touchpoint_ts.date()
        channel = str(channel_raw or "unknown")
        if filter_channels and channel not in allowed_channels:
            continue
        bucket = _bucket_start(day, resolved_grain).isoformat()
        if curr_from <= day <= curr_to:
            _add_metric_rollup(curr_store, channel, bucket, visits=1.0)
        elif compare and prev_from <= day <= prev_to:
            _add_metric_rollup(prev_store, channel, bucket, visits=1.0)

    for row in conversion_rows:
        conversion_id = str(row[0] or "")
        row_conversion_key = str(row[1] or "").strip() or None
        conversion_ts = row[2]
        if not isinstance(conversion_ts, datetime):
            continue
        if conversion_key is not None and row_conversion_key != conversion_key:
            continue
        day = conversion_ts.date()
        channel = last_touch_channel.get(conversion_id, "unknown")
        if filter_channels and channel not in allowed_channels:
            continue
        bucket = _bucket_start(day, resolved_grain).isoformat()
        if curr_from <= day <= curr_to:
            target_store = curr_store
            target_outcomes = curr_outcomes
        elif compare and prev_from <= day <= prev_to:
            target_store = prev_store
            target_outcomes = prev_outcomes
        else:
            continue
        _add_metric_rollup(
            target_store,
            channel,
            bucket,
            conversions=1.0,
            revenue=float(row[6] or 0.0),
        )
        outcome = target_outcomes.setdefault(channel, _empty_outcome_metrics())
        outcome["gross_conversions"] += float(row[4] or 0.0)
        outcome["net_conversions"] += float(row[5] or 0.0)
        outcome["gross_revenue"] += float(row[6] or 0.0)
        outcome["net_revenue"] += float(row[7] or 0.0)
        path_type = str(row[3] or "").strip().lower()
        net_conversions = float(row[5] or 0.0)
        if path_type == "click_through":
            outcome["click_through_conversions"] += net_conversions
        elif path_type == "view_through":
            outcome["view_through_conversions"] += net_conversions
        elif path_type == "mixed_path":
            outcome["mixed_path_conversions"] += net_conversions

    if not curr_store and not prev_store:
        return None
    return {
        "current_store": curr_store,
        "previous_store": prev_store,
        "current_outcomes": curr_outcomes,
        "previous_outcomes": prev_outcomes,
    }


def _collect_channel_rollups(
    *,
    journeys: List[Dict[str, Any]],
    expenses: Any,
    timezone: str,
    curr_from: date,
    curr_to: date,
    prev_from: date,
    prev_to: date,
    resolved_grain: str,
    compare: bool,
    channels: Optional[List[str]],
    conversion_key: Optional[str] = None,
    aggregate_overlay: Optional[Dict[str, Any]] = None,
) -> Tuple[
    Dict[str, Dict[str, Dict[str, float]]],
    Dict[str, Dict[str, Dict[str, float]]],
    Dict[str, Dict[str, float]],
    Dict[str, Dict[str, float]],
]:
    tz = _safe_tz(timezone)
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    curr_outcomes: Dict[str, Dict[str, float]] = {}
    prev_outcomes: Dict[str, Dict[str, float]] = {}
    dedupe_curr: set[str] = set()
    dedupe_prev: set[str] = set()

    if aggregate_overlay is None:
        for journey in journeys or []:
            matches_conversion_key = True
            if conversion_key:
                journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
                matches_conversion_key = journey_key == conversion_key
            touchpoints = journey.get("touchpoints") or []
            for tp in touchpoints:
                if not isinstance(tp, dict):
                    continue
                channel = str(tp.get("channel") or "unknown")
                if filter_channels and channel not in allowed_channels:
                    continue
                day = _local_date_from_ts(tp.get("timestamp") or tp.get("ts"), tz)
                if day is None:
                    continue
                bucket = _bucket_start(day, resolved_grain).isoformat()
                if curr_from <= day <= curr_to:
                    _add_metric_rollup(curr_store, channel, bucket, visits=1.0)
                elif compare and prev_from <= day <= prev_to:
                    _add_metric_rollup(prev_store, channel, bucket, visits=1.0)

            if not matches_conversion_key:
                continue
            if not ((journey.get("conversions") or []) or journey.get("converted", False)):
                continue
            if not touchpoints:
                continue
            last_tp = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
            channel = str(last_tp.get("channel") or "unknown")
            if filter_channels and channel not in allowed_channels:
                continue
            day = _local_date_from_ts(last_tp.get("timestamp"), tz)
            if day is None:
                continue
            bucket = _bucket_start(day, resolved_grain).isoformat()
            outcome = journey_outcome_summary(journey)
            path_type = _path_type(journey)
            if curr_from <= day <= curr_to:
                revenue = journey_revenue_value(journey, dedupe_seen=dedupe_curr)
                _add_metric_rollup(curr_store, channel, bucket, conversions=1.0, revenue=revenue)
                metrics = curr_outcomes.setdefault(channel, _empty_outcome_metrics())
                _merge_outcome_metrics(metrics, outcome, path_type)
            elif compare and prev_from <= day <= prev_to:
                revenue = journey_revenue_value(journey, dedupe_seen=dedupe_prev)
                _add_metric_rollup(prev_store, channel, bucket, conversions=1.0, revenue=revenue)
                metrics = prev_outcomes.setdefault(channel, _empty_outcome_metrics())
                _merge_outcome_metrics(metrics, outcome, path_type)
    else:
        for channel, buckets in (aggregate_overlay.get("current_store") or {}).items():
            for bucket, row in buckets.items():
                _add_metric_rollup(
                    curr_store,
                    channel,
                    bucket,
                    spend=float((row or {}).get("spend", 0.0) or 0.0),
                    visits=float((row or {}).get("visits", 0.0) or 0.0),
                    conversions=float((row or {}).get("conversions", 0.0) or 0.0),
                    revenue=float((row or {}).get("revenue", 0.0) or 0.0),
                )
        for channel, buckets in (aggregate_overlay.get("previous_store") or {}).items():
            for bucket, row in buckets.items():
                _add_metric_rollup(
                    prev_store,
                    channel,
                    bucket,
                    spend=float((row or {}).get("spend", 0.0) or 0.0),
                    visits=float((row or {}).get("visits", 0.0) or 0.0),
                    conversions=float((row or {}).get("conversions", 0.0) or 0.0),
                    revenue=float((row or {}).get("revenue", 0.0) or 0.0),
                )
        curr_outcomes = {
            key: {metric: float(value or 0.0) for metric, value in metrics.items()}
            for key, metrics in (aggregate_overlay.get("current_outcomes") or {}).items()
        }
        prev_outcomes = {
            key: {metric: float(value or 0.0) for metric, value in metrics.items()}
            for key, metrics in (aggregate_overlay.get("previous_outcomes") or {}).items()
        }

    for exp in _expense_records(expenses):
        channel, start_raw, amount, status = _expense_fields(exp)
        if status == "deleted" or not channel:
            continue
        channel = str(channel)
        if filter_channels and channel not in allowed_channels:
            continue
        day = _local_date_from_ts(start_raw, tz)
        if day is None:
            continue
        bucket = _bucket_start(day, resolved_grain).isoformat()
        if curr_from <= day <= curr_to:
            _add_metric_rollup(curr_store, channel, bucket, spend=amount)
        elif compare and prev_from <= day <= prev_to:
            _add_metric_rollup(prev_store, channel, bucket, spend=amount)
    return curr_store, prev_store, curr_outcomes, prev_outcomes


def _collect_campaign_rollups(
    *,
    journeys: List[Dict[str, Any]],
    expenses: Any,
    timezone: str,
    curr_from: date,
    curr_to: date,
    prev_from: date,
    prev_to: date,
    resolved_grain: str,
    compare: bool,
    channels: Optional[List[str]],
    conversion_key: Optional[str] = None,
    aggregate_overlay: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Dict[str, Dict[str, float]]], Dict[str, Dict[str, Dict[str, float]]], Dict[str, Dict[str, Any]]]:
    tz = _safe_tz(timezone)
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    curr_outcomes: Dict[str, Dict[str, float]] = {}
    prev_outcomes: Dict[str, Dict[str, float]] = {}
    meta: Dict[str, Dict[str, Any]] = {}
    dedupe_curr: set[str] = set()
    dedupe_prev: set[str] = set()

    def campaign_key(channel: str, campaign: Optional[str]) -> str:
        return f"{channel}:{campaign}" if campaign else channel

    for journey in journeys or []:
        matches_conversion_key = True
        if conversion_key:
            journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
            matches_conversion_key = journey_key == conversion_key
        touchpoints = journey.get("touchpoints") or []
        for tp in touchpoints:
            if not isinstance(tp, dict):
                continue
            channel = str(tp.get("channel") or "unknown")
            if filter_channels and channel not in allowed_channels:
                continue
            campaign_name = tp.get("campaign")
            if isinstance(campaign_name, dict):
                campaign_name = campaign_name.get("name")
            c_key = campaign_key(channel, campaign_name if campaign_name else None)
            meta.setdefault(
                c_key,
                {
                    "campaign_id": c_key,
                    "campaign_name": campaign_name,
                    "channel": channel,
                    "platform": tp.get("platform"),
                },
            )
            day = _local_date_from_ts(tp.get("timestamp") or tp.get("ts"), tz)
            if day is None:
                continue
            bucket = _bucket_start(day, resolved_grain).isoformat()
            if curr_from <= day <= curr_to:
                _add_metric_rollup(curr_store, c_key, bucket, visits=1.0)
            elif compare and prev_from <= day <= prev_to:
                _add_metric_rollup(prev_store, c_key, bucket, visits=1.0)

        if aggregate_overlay is None:
            if not matches_conversion_key:
                continue
            if not ((journey.get("conversions") or []) or journey.get("converted", False)):
                continue
            if not touchpoints:
                continue
            last_tp = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
            channel = str(last_tp.get("channel") or "unknown")
            if filter_channels and channel not in allowed_channels:
                continue
            campaign_name = last_tp.get("campaign")
            c_key = campaign_key(channel, campaign_name if campaign_name else None)
            meta[c_key] = {
                "campaign_id": c_key,
                "campaign_name": campaign_name,
                "channel": channel,
                "platform": last_tp.get("platform"),
            }
            day = _local_date_from_ts(last_tp.get("timestamp"), tz)
            if day is None:
                continue
            bucket = _bucket_start(day, resolved_grain).isoformat()
            outcome = journey_outcome_summary(journey)
            path_type = _path_type(journey)
            if curr_from <= day <= curr_to:
                revenue = journey_revenue_value(journey, dedupe_seen=dedupe_curr)
                _add_metric_rollup(curr_store, c_key, bucket, conversions=1.0, revenue=revenue)
                metrics = curr_outcomes.setdefault(c_key, _empty_outcome_metrics())
                _merge_outcome_metrics(metrics, outcome, path_type)
            elif compare and prev_from <= day <= prev_to:
                revenue = journey_revenue_value(journey, dedupe_seen=dedupe_prev)
                _add_metric_rollup(prev_store, c_key, bucket, conversions=1.0, revenue=revenue)
                metrics = prev_outcomes.setdefault(c_key, _empty_outcome_metrics())
                _merge_outcome_metrics(metrics, outcome, path_type)

    if aggregate_overlay is not None:
        for c_key, details in (aggregate_overlay.get("meta") or {}).items():
            meta.setdefault(c_key, details)
        for c_key, buckets in (aggregate_overlay.get("current_store") or {}).items():
            for bucket, row in buckets.items():
                _add_metric_rollup(
                    curr_store,
                    c_key,
                    bucket,
                    conversions=float((row or {}).get("conversions", 0.0) or 0.0),
                    revenue=float((row or {}).get("revenue", 0.0) or 0.0),
                )
        for c_key, buckets in (aggregate_overlay.get("previous_store") or {}).items():
            for bucket, row in buckets.items():
                _add_metric_rollup(
                    prev_store,
                    c_key,
                    bucket,
                    conversions=float((row or {}).get("conversions", 0.0) or 0.0),
                    revenue=float((row or {}).get("revenue", 0.0) or 0.0),
                )
        curr_outcomes = {
            key: {metric: float(value or 0.0) for metric, value in metrics.items()}
            for key, metrics in (aggregate_overlay.get("current_outcomes") or {}).items()
        }
        prev_outcomes = {
            key: {metric: float(value or 0.0) for metric, value in metrics.items()}
            for key, metrics in (aggregate_overlay.get("previous_outcomes") or {}).items()
        }

    # Spend remains channel-level in source data. Allocate channel spend to campaign keys
    # per bucket to avoid double-counting when campaign totals are aggregated:
    # - proportional to campaign revenue within the same channel+bucket
    # - equal split when channel bucket has spend but no campaign revenue
    channel_spend_curr: Dict[str, Dict[str, float]] = {}
    channel_spend_prev: Dict[str, Dict[str, float]] = {}
    for exp in _expense_records(expenses):
        channel, start_raw, amount, status = _expense_fields(exp)
        if status == "deleted" or not channel:
            continue
        channel = str(channel)
        if filter_channels and channel not in allowed_channels:
            continue
        day = _local_date_from_ts(start_raw, tz)
        if day is None:
            continue
        bucket = _bucket_start(day, resolved_grain).isoformat()
        if curr_from <= day <= curr_to:
            by_bucket = channel_spend_curr.setdefault(channel, {})
            by_bucket[bucket] = by_bucket.get(bucket, 0.0) + amount
        elif compare and prev_from <= day <= prev_to:
            by_bucket = channel_spend_prev.setdefault(channel, {})
            by_bucket[bucket] = by_bucket.get(bucket, 0.0) + amount

    campaigns_by_channel: Dict[str, List[str]] = {}
    for c_key, details in meta.items():
        channel = str(details["channel"])
        campaigns_by_channel.setdefault(channel, []).append(c_key)

    def _allocate_spend(
        spend_by_channel_bucket: Dict[str, Dict[str, float]],
        target_store: Dict[str, Dict[str, Dict[str, float]]],
    ) -> None:
        for channel, bucket_amounts in spend_by_channel_bucket.items():
            campaign_keys = campaigns_by_channel.get(channel, [])
            if not campaign_keys:
                continue
            for bucket, amount in bucket_amounts.items():
                if amount == 0:
                    continue
                rev_by_campaign: Dict[str, float] = {
                    c_key: target_store.get(c_key, {}).get(bucket, {}).get("revenue", 0.0)
                    for c_key in campaign_keys
                }
                total_rev = sum(max(0.0, v) for v in rev_by_campaign.values())
                if total_rev > 0:
                    for c_key, rev in rev_by_campaign.items():
                        if rev <= 0:
                            continue
                        _add_metric_rollup(target_store, c_key, bucket, spend=amount * (rev / total_rev))
                else:
                    equal_share = amount / float(len(campaign_keys))
                    for c_key in campaign_keys:
                        _add_metric_rollup(target_store, c_key, bucket, spend=equal_share)

    _allocate_spend(channel_spend_curr, curr_store)
    if compare:
        _allocate_spend(channel_spend_prev, prev_store)
    meta["__current_outcomes__"] = curr_outcomes
    meta["__previous_outcomes__"] = prev_outcomes
    return curr_store, prev_store, meta


def build_channel_trend_response(
    *,
    journeys: List[Dict[str, Any]],
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    kpi_key: str = "revenue",
    grain: str = "auto",
    compare: bool = True,
    channels: Optional[List[str]] = None,
    conversion_key: Optional[str] = None,
    aggregate_overlay: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if kpi_key not in SUPPORTED_KPIS:
        raise ValueError(f"Unsupported kpi_key '{kpi_key}'")
    windows = resolve_period_windows(date_from, date_to, grain)
    resolved_grain = windows["current_period"]["grain"]
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    current_keys = _bucket_keys_for_period(curr_from, curr_to, resolved_grain)
    prev_keys = _bucket_keys_for_period(prev_from, prev_to, resolved_grain)
    curr_store, prev_store, _curr_outcomes, _prev_outcomes = _collect_channel_rollups(
        journeys=journeys,
        expenses=expenses,
        timezone=timezone,
        curr_from=curr_from,
        curr_to=curr_to,
        prev_from=prev_from,
        prev_to=prev_to,
        resolved_grain=resolved_grain,
        compare=compare,
        channels=channels,
        conversion_key=conversion_key,
        aggregate_overlay=aggregate_overlay,
    )

    dims = sorted(set(curr_store.keys()) | (set(prev_store.keys()) if compare else set()))
    series: List[Dict[str, Any]] = []
    for ch in dims:
        for key in current_keys:
            metric_row = curr_store.get(ch, {}).get(key, {})
            val = metric_value(metric_row, kpi_key) if metric_row else None
            series.append({"ts": key, "channel": ch, "value": val})

    out: Dict[str, Any] = {
        "current_period": windows["current_period"],
        "previous_period": windows["previous_period"],
        "series": series,
    }
    if compare:
        series_prev: List[Dict[str, Any]] = []
        for ch in dims:
            for key in prev_keys:
                metric_row = prev_store.get(ch, {}).get(key, {})
                val = metric_value(metric_row, kpi_key) if metric_row else None
                series_prev.append({"ts": key, "channel": ch, "value": val})
        out["series_prev"] = series_prev
    return out


def build_campaign_trend_response(
    *,
    journeys: List[Dict[str, Any]],
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    kpi_key: str = "revenue",
    grain: str = "auto",
    compare: bool = True,
    channels: Optional[List[str]] = None,
    conversion_key: Optional[str] = None,
    aggregate_overlay: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if kpi_key not in SUPPORTED_KPIS:
        raise ValueError(f"Unsupported kpi_key '{kpi_key}'")
    windows = resolve_period_windows(date_from, date_to, grain)
    resolved_grain = windows["current_period"]["grain"]
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    current_keys = _bucket_keys_for_period(curr_from, curr_to, resolved_grain)
    prev_keys = _bucket_keys_for_period(prev_from, prev_to, resolved_grain)
    curr_store, prev_store, meta = _collect_campaign_rollups(
        journeys=journeys,
        expenses=expenses,
        timezone=timezone,
        curr_from=curr_from,
        curr_to=curr_to,
        prev_from=prev_from,
        prev_to=prev_to,
        resolved_grain=resolved_grain,
        compare=compare,
        channels=channels,
        conversion_key=conversion_key,
        aggregate_overlay=aggregate_overlay,
    )
    if isinstance(meta.get("__current_outcomes__"), dict):
        meta = {key: value for key, value in meta.items() if not str(key).startswith("__")}

    dims = sorted(set(meta.keys()) | set(curr_store.keys()) | (set(prev_store.keys()) if compare else set()))
    series: List[Dict[str, Any]] = []
    for dim in dims:
        dim_meta = meta.get(dim, {"campaign_id": dim, "campaign_name": None, "channel": dim.split(":", 1)[0], "platform": None})
        for key in current_keys:
            metric_row = curr_store.get(dim, {}).get(key, {})
            val = metric_value(metric_row, kpi_key) if metric_row else None
            series.append(
                {
                    "ts": key,
                    "campaign_id": dim_meta["campaign_id"],
                    "campaign_name": dim_meta["campaign_name"],
                    "channel": dim_meta["channel"],
                    "platform": dim_meta.get("platform"),
                    "value": val,
                }
            )

    out: Dict[str, Any] = {
        "current_period": windows["current_period"],
        "previous_period": windows["previous_period"],
        "series": series,
    }
    if compare:
        series_prev: List[Dict[str, Any]] = []
        for dim in dims:
            dim_meta = meta.get(dim, {"campaign_id": dim, "campaign_name": None, "channel": dim.split(":", 1)[0], "platform": None})
            for key in prev_keys:
                metric_row = prev_store.get(dim, {}).get(key, {})
                val = metric_value(metric_row, kpi_key) if metric_row else None
                series_prev.append(
                    {
                        "ts": key,
                        "campaign_id": dim_meta["campaign_id"],
                        "campaign_name": dim_meta["campaign_name"],
                        "channel": dim_meta["channel"],
                        "platform": dim_meta.get("platform"),
                        "value": val,
                    }
                )
        out["series_prev"] = series_prev
    return out


def build_channel_summary_response(
    *,
    journeys: List[Dict[str, Any]],
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    compare: bool = True,
    channels: Optional[List[str]] = None,
    conversion_key: Optional[str] = None,
    aggregate_overlay: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    windows = resolve_period_windows(date_from, date_to, "daily")
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    curr_store, prev_store, curr_outcomes, prev_outcomes = _collect_channel_rollups(
        journeys=journeys,
        expenses=expenses,
        timezone=timezone,
        curr_from=curr_from,
        curr_to=curr_to,
        prev_from=prev_from,
        prev_to=prev_to,
        resolved_grain="daily",
        compare=compare,
        channels=channels,
        conversion_key=conversion_key,
        aggregate_overlay=aggregate_overlay,
    )
    dims = sorted(set(curr_store.keys()) | (set(prev_store.keys()) if compare else set()))
    items: List[Dict[str, Any]] = []
    for dim in dims:
        curr_totals, curr_derived = summarize_rows(curr_store.get(dim, {}))
        prev_totals, _ = summarize_rows(prev_store.get(dim, {}))
        items.append(
            {
                "channel": dim,
                "current": curr_totals,
                "previous": prev_totals if compare else None,
                "derived": {"roas": curr_derived["roas"], "cpa": curr_derived["cpa"]},
                "outcomes": {
                    "current": curr_outcomes.get(dim, _empty_outcome_metrics()),
                    "previous": prev_outcomes.get(dim, _empty_outcome_metrics()) if compare else None,
                },
            }
        )
    totals_current = {"spend": 0.0, "visits": 0.0, "conversions": 0.0, "revenue": 0.0}
    totals_previous = {"spend": 0.0, "visits": 0.0, "conversions": 0.0, "revenue": 0.0}
    for item in items:
        totals_current["spend"] += float(item["current"].get("spend", 0.0))
        totals_current["visits"] += float(item["current"].get("visits", 0.0))
        totals_current["conversions"] += float(item["current"].get("conversions", 0.0))
        totals_current["revenue"] += float(item["current"].get("revenue", 0.0))
        prev_row = item.get("previous") or {}
        totals_previous["spend"] += float(prev_row.get("spend", 0.0))
        totals_previous["visits"] += float(prev_row.get("visits", 0.0))
        totals_previous["conversions"] += float(prev_row.get("conversions", 0.0))
        totals_previous["revenue"] += float(prev_row.get("revenue", 0.0))
    totals_outcomes_current = _empty_outcome_metrics()
    totals_outcomes_previous = _empty_outcome_metrics()
    for item in items:
        for key, value in (item.get("outcomes") or {}).get("current", {}).items():
            totals_outcomes_current[key] = totals_outcomes_current.get(key, 0.0) + float(value or 0.0)
        for key, value in ((item.get("outcomes") or {}).get("previous") or {}).items():
            totals_outcomes_previous[key] = totals_outcomes_previous.get(key, 0.0) + float(value or 0.0)
    notes: List[str] = []
    if totals_outcomes_current["view_through_conversions"] > totals_outcomes_current["click_through_conversions"]:
        notes.append("View-through conversions exceed click-through conversions in the selected period.")
    if totals_outcomes_current["invalid_leads"] > 0:
        notes.append("Invalid or disqualified leads are present; compare gross vs net conversion totals.")
    if totals_outcomes_current["refunded_value"] > 0 or totals_outcomes_current["cancelled_value"] > 0:
        notes.append("Refunded or cancelled value is present; net revenue is lower than gross revenue.")
    return {
        "current_period": windows["current_period"],
        "previous_period": windows["previous_period"],
        "items": items,
        "totals": {
            "current": totals_current,
            "previous": totals_previous if compare else None,
            "outcomes_current": totals_outcomes_current,
            "outcomes_previous": totals_outcomes_previous if compare else None,
        },
        "notes": notes,
    }


def build_campaign_summary_response(
    *,
    journeys: List[Dict[str, Any]],
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone: str = "UTC",
    compare: bool = True,
    channels: Optional[List[str]] = None,
    conversion_key: Optional[str] = None,
    aggregate_overlay: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    windows = resolve_period_windows(date_from, date_to, "daily")
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    curr_store, prev_store, meta = _collect_campaign_rollups(
        journeys=journeys,
        expenses=expenses,
        timezone=timezone,
        curr_from=curr_from,
        curr_to=curr_to,
        prev_from=prev_from,
        prev_to=prev_to,
        resolved_grain="daily",
        compare=compare,
        channels=channels,
        conversion_key=conversion_key,
        aggregate_overlay=aggregate_overlay,
    )
    curr_outcomes = meta.pop("__current_outcomes__", {}) if isinstance(meta.get("__current_outcomes__"), dict) else {}
    prev_outcomes = meta.pop("__previous_outcomes__", {}) if isinstance(meta.get("__previous_outcomes__"), dict) else {}
    dims = sorted(set(curr_store.keys()) | (set(prev_store.keys()) if compare else set()))
    items: List[Dict[str, Any]] = []
    for dim in dims:
        curr_totals, curr_derived = summarize_rows(curr_store.get(dim, {}))
        prev_totals, _ = summarize_rows(prev_store.get(dim, {}))
        m = meta.get(dim, {"campaign_id": dim, "campaign_name": None, "channel": dim.split(":", 1)[0], "platform": None})
        items.append(
            {
                "campaign_id": m["campaign_id"],
                "campaign_name": m["campaign_name"],
                "channel": m["channel"],
                "platform": m.get("platform"),
                "current": curr_totals,
                "previous": prev_totals if compare else None,
                "derived": {"roas": curr_derived["roas"], "cpa": curr_derived["cpa"]},
                "outcomes": {
                    "current": curr_outcomes.get(dim, _empty_outcome_metrics()),
                    "previous": prev_outcomes.get(dim, _empty_outcome_metrics()) if compare else None,
                },
            }
        )
    totals_current = {"spend": 0.0, "visits": 0.0, "conversions": 0.0, "revenue": 0.0}
    totals_previous = {"spend": 0.0, "visits": 0.0, "conversions": 0.0, "revenue": 0.0}
    for item in items:
        totals_current["spend"] += float(item["current"].get("spend", 0.0))
        totals_current["visits"] += float(item["current"].get("visits", 0.0))
        totals_current["conversions"] += float(item["current"].get("conversions", 0.0))
        totals_current["revenue"] += float(item["current"].get("revenue", 0.0))
        prev_row = item.get("previous") or {}
        totals_previous["spend"] += float(prev_row.get("spend", 0.0))
        totals_previous["visits"] += float(prev_row.get("visits", 0.0))
        totals_previous["conversions"] += float(prev_row.get("conversions", 0.0))
        totals_previous["revenue"] += float(prev_row.get("revenue", 0.0))
    totals_outcomes_current = _empty_outcome_metrics()
    totals_outcomes_previous = _empty_outcome_metrics()
    for item in items:
        for key, value in (item.get("outcomes") or {}).get("current", {}).items():
            totals_outcomes_current[key] = totals_outcomes_current.get(key, 0.0) + float(value or 0.0)
        for key, value in ((item.get("outcomes") or {}).get("previous") or {}).items():
            totals_outcomes_previous[key] = totals_outcomes_previous.get(key, 0.0) + float(value or 0.0)
    notes: List[str] = []
    if totals_outcomes_current["view_through_conversions"] > totals_outcomes_current["click_through_conversions"]:
        notes.append("View-through conversions exceed click-through conversions in the selected period.")
    if totals_outcomes_current["invalid_leads"] > 0:
        notes.append("Invalid or disqualified leads are present; compare gross vs net conversion totals.")
    if totals_outcomes_current["refunded_value"] > 0 or totals_outcomes_current["cancelled_value"] > 0:
        notes.append("Refunded or cancelled value is present; net revenue is lower than gross revenue.")
    return {
        "current_period": windows["current_period"],
        "previous_period": windows["previous_period"],
        "items": items,
        "totals": {
            "current": totals_current,
            "previous": totals_previous if compare else None,
            "outcomes_current": totals_outcomes_current,
            "outcomes_previous": totals_outcomes_previous if compare else None,
        },
        "notes": [
            "Channel-level spend is allocated across campaign keys per bucket (revenue-weighted, equal-split fallback).",
            *notes,
        ],
    }
