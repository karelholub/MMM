from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


SUPPORTED_KPIS = {"spend", "conversions", "revenue", "cpa", "roas"}


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
    conversions: float = 0.0,
    revenue: float = 0.0,
) -> None:
    dim_entry = store.setdefault(dim_key, {})
    bucket_entry = dim_entry.setdefault(bucket_key, {"spend": 0.0, "conversions": 0.0, "revenue": 0.0})
    bucket_entry["spend"] += spend
    bucket_entry["conversions"] += conversions
    bucket_entry["revenue"] += revenue


def _metric_value(metric_row: Dict[str, float], kpi_key: str) -> Optional[float]:
    spend = metric_row.get("spend", 0.0)
    conversions = metric_row.get("conversions", 0.0)
    revenue = metric_row.get("revenue", 0.0)
    if kpi_key == "spend":
        return spend
    if kpi_key == "conversions":
        return conversions
    if kpi_key == "revenue":
        return revenue
    if kpi_key == "cpa":
        return (spend / conversions) if conversions > 0 else None
    if kpi_key == "roas":
        return (revenue / spend) if spend > 0 else None
    return None


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
) -> Dict[str, Any]:
    if kpi_key not in SUPPORTED_KPIS:
        raise ValueError(f"Unsupported kpi_key '{kpi_key}'")
    windows = resolve_period_windows(date_from, date_to, grain)
    resolved_grain = windows["current_period"]["grain"]
    tz = _safe_tz(timezone)
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    current_keys = _bucket_keys_for_period(curr_from, curr_to, resolved_grain)
    prev_keys = _bucket_keys_for_period(prev_from, prev_to, resolved_grain)

    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}

    for journey in journeys or []:
        if not journey.get("converted", True):
            continue
        touchpoints = journey.get("touchpoints") or []
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
        revenue = float(journey.get("conversion_value") or 0.0)
        if curr_from <= day <= curr_to:
            _add_metric_rollup(curr_store, channel, bucket, conversions=1.0, revenue=revenue)
        elif compare and prev_from <= day <= prev_to:
            _add_metric_rollup(prev_store, channel, bucket, conversions=1.0, revenue=revenue)

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

    dims = sorted(set(curr_store.keys()) | (set(prev_store.keys()) if compare else set()))
    series: List[Dict[str, Any]] = []
    for ch in dims:
        for key in current_keys:
            metric_row = curr_store.get(ch, {}).get(key, {})
            val = _metric_value(metric_row, kpi_key) if metric_row else None
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
                val = _metric_value(metric_row, kpi_key) if metric_row else None
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
) -> Dict[str, Any]:
    if kpi_key not in SUPPORTED_KPIS:
        raise ValueError(f"Unsupported kpi_key '{kpi_key}'")
    windows = resolve_period_windows(date_from, date_to, grain)
    resolved_grain = windows["current_period"]["grain"]
    tz = _safe_tz(timezone)
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    current_keys = _bucket_keys_for_period(curr_from, curr_to, resolved_grain)
    prev_keys = _bucket_keys_for_period(prev_from, prev_to, resolved_grain)

    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    meta: Dict[str, Dict[str, Any]] = {}

    def campaign_key(channel: str, campaign: Optional[str]) -> str:
        return f"{channel}:{campaign}" if campaign else channel

    for journey in journeys or []:
        if not journey.get("converted", True):
            continue
        touchpoints = journey.get("touchpoints") or []
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
        revenue = float(journey.get("conversion_value") or 0.0)
        if curr_from <= day <= curr_to:
            _add_metric_rollup(curr_store, c_key, bucket, conversions=1.0, revenue=revenue)
        elif compare and prev_from <= day <= prev_to:
            _add_metric_rollup(prev_store, c_key, bucket, conversions=1.0, revenue=revenue)

    # Spend remains channel-level in current data model; attach spend to all campaigns of that channel.
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

    for c_key, details in meta.items():
        channel = str(details["channel"])
        for bucket, amount in channel_spend_curr.get(channel, {}).items():
            _add_metric_rollup(curr_store, c_key, bucket, spend=amount)
        if compare:
            for bucket, amount in channel_spend_prev.get(channel, {}).items():
                _add_metric_rollup(prev_store, c_key, bucket, spend=amount)

    dims = sorted(set(meta.keys()) | set(curr_store.keys()) | (set(prev_store.keys()) if compare else set()))
    series: List[Dict[str, Any]] = []
    for dim in dims:
        dim_meta = meta.get(dim, {"campaign_id": dim, "campaign_name": None, "channel": dim.split(":", 1)[0], "platform": None})
        for key in current_keys:
            metric_row = curr_store.get(dim, {}).get(key, {})
            val = _metric_value(metric_row, kpi_key) if metric_row else None
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
                val = _metric_value(metric_row, kpi_key) if metric_row else None
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
