from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.services_metrics import journey_revenue_value
from app.services_performance_trends import resolve_period_windows


@dataclass
class PerformanceQueryContext:
    date_from: str
    date_to: str
    timezone: str
    currency: Optional[str] = None
    workspace: Optional[str] = None
    account: Optional[str] = None
    model_id: Optional[str] = None
    kpi_key: str = "revenue"
    grain: str = "auto"
    compare: bool = True
    channels: Optional[List[str]] = None
    conversion_key: Optional[str] = None
    current_period: Optional[Dict[str, Any]] = None
    previous_period: Optional[Dict[str, Any]] = None


def compute_campaign_trends(journeys: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build time series per campaign step (channel:campaign or channel):
      - transactions: number of converted journeys attributed to that campaign (last touch)
      - revenue: sum of configured revenue for those journeys
    """
    if not journeys:
        return {"campaigns": [], "dates": [], "series": {}}

    series: Dict[str, Dict[str, Dict[str, float]]] = {}
    all_dates: set[str] = set()
    dedupe_seen: set[str] = set()

    for journey in journeys:
        if not journey.get("converted", True):
            continue
        touchpoints = journey.get("touchpoints", [])
        if not touchpoints:
            continue
        last_tp = touchpoints[-1]
        ts = last_tp.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
            date_key = dt.date().isoformat()
        except Exception:
            date_key = str(ts)

        channel = last_tp.get("channel", "unknown")
        campaign = last_tp.get("campaign")
        step = f"{channel}:{campaign}" if campaign else channel

        all_dates.add(date_key)
        step_series = series.setdefault(step, {})
        entry = step_series.setdefault(date_key, {"transactions": 0.0, "revenue": 0.0})
        entry["transactions"] += 1.0
        entry["revenue"] += journey_revenue_value(journey, dedupe_seen=dedupe_seen)

    sorted_dates = sorted(all_dates)
    campaigns = sorted(series.keys())

    out_series: Dict[str, List[Dict[str, Any]]] = {}
    for step, date_map in series.items():
        points = []
        for day in sorted_dates:
            val = date_map.get(day, {"transactions": 0.0, "revenue": 0.0})
            points.append({"date": day, "transactions": int(val["transactions"]), "revenue": val["revenue"]})
        out_series[step] = points

    return {"campaigns": campaigns, "dates": sorted_dates, "series": out_series}


def normalize_channel_filter(channels: Optional[List[str]]) -> Optional[List[str]]:
    if not channels:
        return None
    normalized: List[str] = []
    for raw in channels:
        if raw is None:
            continue
        parts = [p.strip() for p in str(raw).split(",")]
        for part in parts:
            if not part:
                continue
            if part.lower() == "all":
                return None
            normalized.append(part)
    uniq_sorted = sorted(set(normalized))
    return uniq_sorted or None


def build_performance_query_context(
    *,
    date_from: str,
    date_to: str,
    timezone: str,
    currency: Optional[str],
    workspace: Optional[str],
    account: Optional[str],
    model_id: Optional[str],
    kpi_key: str,
    grain: str,
    compare: bool,
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> PerformanceQueryContext:
    windows = resolve_period_windows(date_from=date_from, date_to=date_to, grain=grain)
    return PerformanceQueryContext(
        date_from=windows["current_period"]["date_from"],
        date_to=windows["current_period"]["date_to"],
        timezone=(timezone or "UTC").strip() or "UTC",
        currency=currency,
        workspace=workspace,
        account=account,
        model_id=model_id,
        kpi_key=(kpi_key or "revenue").strip().lower(),
        grain=grain,
        compare=bool(compare),
        channels=normalize_channel_filter(channels),
        conversion_key=(conversion_key.strip() if conversion_key else None),
        current_period=windows.get("current_period"),
        previous_period=windows.get("previous_period"),
    )


def _safe_zoneinfo(timezone_name: Optional[str]) -> ZoneInfo:
    try:
        return ZoneInfo((timezone_name or "UTC").strip() or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _local_date_from_ts(ts_value: Any, timezone_name: Optional[str]) -> Optional[date]:
    if not ts_value:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_value).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_safe_zoneinfo(timezone_name)).date()


def compute_total_spend_for_period(
    *,
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
) -> float:
    start_d = datetime.fromisoformat(date_from[:10]).date()
    end_d = datetime.fromisoformat(date_to[:10]).date()
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    allowed = set(channels or [])
    total = 0.0
    records = expenses.values() if isinstance(expenses, dict) else (expenses or [])
    for exp in records:
        if isinstance(exp, dict):
            status = str(exp.get("status", "active"))
            channel = exp.get("channel")
            ts_raw = exp.get("service_period_start")
            amount = float(exp.get("converted_amount") or exp.get("amount") or 0.0)
        else:
            status = str(getattr(exp, "status", "active"))
            channel = getattr(exp, "channel", None)
            ts_raw = getattr(exp, "service_period_start", None)
            amount = float(getattr(exp, "converted_amount", None) or getattr(exp, "amount", 0.0) or 0.0)
        if status == "deleted" or not channel:
            continue
        channel = str(channel)
        if allowed and channel not in allowed:
            continue
        day = _local_date_from_ts(ts_raw, timezone_name)
        if day is None:
            continue
        if start_d <= day <= end_d:
            total += amount
    return total


def compute_total_converted_value_for_period(
    *,
    journeys: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> float:
    start_d = datetime.fromisoformat(date_from[:10]).date()
    end_d = datetime.fromisoformat(date_to[:10]).date()
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    allowed = set(channels or [])
    total = 0.0
    dedupe_seen: set[str] = set()
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
        if allowed and channel not in allowed:
            continue
        day = _local_date_from_ts(last_tp.get("timestamp"), timezone_name)
        if day is None:
            continue
        if start_d <= day <= end_d:
            total += journey_revenue_value(journey, dedupe_seen=dedupe_seen)
    return total


def build_mapping_coverage(
    *,
    mapped_spend: float,
    mapped_value: float,
    expenses: Any,
    journeys: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> Dict[str, float]:
    spend_total = compute_total_spend_for_period(
        expenses=expenses,
        date_from=date_from,
        date_to=date_to,
        timezone_name=timezone_name,
        channels=channels,
    )
    value_total = compute_total_converted_value_for_period(
        journeys=journeys,
        date_from=date_from,
        date_to=date_to,
        timezone_name=timezone_name,
        channels=channels,
        conversion_key=conversion_key,
    )
    return {
        "spend_mapped_pct": (mapped_spend / spend_total * 100.0) if spend_total > 0 else 0.0,
        "value_mapped_pct": (mapped_value / value_total * 100.0) if value_total > 0 else 0.0,
        "spend_mapped": mapped_spend,
        "spend_total": spend_total,
        "value_mapped": mapped_value,
        "value_total": value_total,
    }


def build_performance_meta(
    *,
    ctx: PerformanceQueryContext,
    conversion_key: Optional[str] = None,
    include_kpi: bool = False,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "workspace": ctx.workspace,
        "account": ctx.account,
        "model_id": ctx.model_id,
        "currency": ctx.currency,
        "timezone": ctx.timezone,
        "channels": ctx.channels or [],
        "conversion_key": conversion_key if conversion_key is not None else ctx.conversion_key,
        "query_context": {
            "current_period": ctx.current_period,
            "previous_period": ctx.previous_period,
            "compare": ctx.compare,
        },
    }
    if include_kpi:
        meta["kpi_key"] = ctx.kpi_key
        meta["query_context"]["grain"] = ctx.grain
    return meta


def summarize_mapped_current(items: List[Dict[str, Any]]) -> Dict[str, float]:
    mapped_spend = sum(float((row.get("current") or {}).get("spend", 0.0)) for row in items)
    mapped_value = sum(float((row.get("current") or {}).get("revenue", 0.0)) for row in items)
    return {"mapped_spend": mapped_spend, "mapped_value": mapped_value}
