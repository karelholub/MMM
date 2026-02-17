from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from app.services_metrics import (
    SUPPORTED_KPIS,
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
    conversions: float = 0.0,
    revenue: float = 0.0,
) -> None:
    dim_entry = store.setdefault(dim_key, {})
    bucket_entry = dim_entry.setdefault(bucket_key, {"spend": 0.0, "conversions": 0.0, "revenue": 0.0})
    bucket_entry["spend"] += spend
    bucket_entry["conversions"] += conversions
    bucket_entry["revenue"] += revenue



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
) -> Tuple[Dict[str, Dict[str, Dict[str, float]]], Dict[str, Dict[str, Dict[str, float]]]]:
    tz = _safe_tz(timezone)
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    dedupe_curr: set[str] = set()
    dedupe_prev: set[str] = set()

    for journey in journeys or []:
        if not journey.get("converted", True):
            continue
        if conversion_key:
            journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
            if journey_key != conversion_key:
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
        if curr_from <= day <= curr_to:
            revenue = journey_revenue_value(journey, dedupe_seen=dedupe_curr)
            _add_metric_rollup(curr_store, channel, bucket, conversions=1.0, revenue=revenue)
        elif compare and prev_from <= day <= prev_to:
            revenue = journey_revenue_value(journey, dedupe_seen=dedupe_prev)
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
    return curr_store, prev_store


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
) -> Tuple[Dict[str, Dict[str, Dict[str, float]]], Dict[str, Dict[str, Dict[str, float]]], Dict[str, Dict[str, Any]]]:
    tz = _safe_tz(timezone)
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    curr_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    prev_store: Dict[str, Dict[str, Dict[str, float]]] = {}
    meta: Dict[str, Dict[str, Any]] = {}
    dedupe_curr: set[str] = set()
    dedupe_prev: set[str] = set()

    def campaign_key(channel: str, campaign: Optional[str]) -> str:
        return f"{channel}:{campaign}" if campaign else channel

    for journey in journeys or []:
        if not journey.get("converted", True):
            continue
        if conversion_key:
            journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
            if journey_key != conversion_key:
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
        if curr_from <= day <= curr_to:
            revenue = journey_revenue_value(journey, dedupe_seen=dedupe_curr)
            _add_metric_rollup(curr_store, c_key, bucket, conversions=1.0, revenue=revenue)
        elif compare and prev_from <= day <= prev_to:
            revenue = journey_revenue_value(journey, dedupe_seen=dedupe_prev)
            _add_metric_rollup(prev_store, c_key, bucket, conversions=1.0, revenue=revenue)

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
    curr_store, prev_store = _collect_channel_rollups(
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
    )

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
) -> Dict[str, Any]:
    windows = resolve_period_windows(date_from, date_to, "daily")
    curr_from = _parse_date(windows["current_period"]["date_from"])
    curr_to = _parse_date(windows["current_period"]["date_to"])
    prev_from = _parse_date(windows["previous_period"]["date_from"])
    prev_to = _parse_date(windows["previous_period"]["date_to"])
    curr_store, prev_store = _collect_channel_rollups(
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
            }
        )
    totals_current = {"spend": 0.0, "conversions": 0.0, "revenue": 0.0}
    totals_previous = {"spend": 0.0, "conversions": 0.0, "revenue": 0.0}
    for item in items:
        totals_current["spend"] += float(item["current"].get("spend", 0.0))
        totals_current["conversions"] += float(item["current"].get("conversions", 0.0))
        totals_current["revenue"] += float(item["current"].get("revenue", 0.0))
        prev_row = item.get("previous") or {}
        totals_previous["spend"] += float(prev_row.get("spend", 0.0))
        totals_previous["conversions"] += float(prev_row.get("conversions", 0.0))
        totals_previous["revenue"] += float(prev_row.get("revenue", 0.0))
    return {
        "current_period": windows["current_period"],
        "previous_period": windows["previous_period"],
        "items": items,
        "totals": {
            "current": totals_current,
            "previous": totals_previous if compare else None,
        },
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
    )
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
            }
        )
    totals_current = {"spend": 0.0, "conversions": 0.0, "revenue": 0.0}
    totals_previous = {"spend": 0.0, "conversions": 0.0, "revenue": 0.0}
    for item in items:
        totals_current["spend"] += float(item["current"].get("spend", 0.0))
        totals_current["conversions"] += float(item["current"].get("conversions", 0.0))
        totals_current["revenue"] += float(item["current"].get("revenue", 0.0))
        prev_row = item.get("previous") or {}
        totals_previous["spend"] += float(prev_row.get("spend", 0.0))
        totals_previous["conversions"] += float(prev_row.get("conversions", 0.0))
        totals_previous["revenue"] += float(prev_row.get("revenue", 0.0))
    return {
        "current_period": windows["current_period"],
        "previous_period": windows["previous_period"],
        "items": items,
        "totals": {
            "current": totals_current,
            "previous": totals_previous if compare else None,
        },
        "notes": [
            "Channel-level spend is allocated across campaign keys per bucket (revenue-weighted, equal-split fallback).",
        ],
    }
