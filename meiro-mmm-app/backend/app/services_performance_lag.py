from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import ConversionScopeDiagnosticFact


def _selected_period_bounds(date_from: str, date_to: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(str(date_from)[:10]).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromisoformat(str(date_to)[:10]).replace(hour=23, minute=59, second=59, microsecond=999999)
    if end < start:
        start, end = end.replace(hour=0, minute=0, second=0, microsecond=0), start.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
        )
    return start, end


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(value) for value in values)
    index = max(0.0, min(float(len(ordered) - 1), float(q) * float(len(ordered) - 1)))
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return float(ordered[lower])
    fraction = index - lower
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction)


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def _lag_bucket_counts(values: List[float]) -> Dict[str, int]:
    buckets = {
        "within_1d": 0,
        "days_1_to_3": 0,
        "days_3_to_7": 0,
        "over_7d": 0,
    }
    for value in values:
        if value <= 1.0:
            buckets["within_1d"] += 1
        elif value <= 3.0:
            buckets["days_1_to_3"] += 1
        elif value <= 7.0:
            buckets["days_3_to_7"] += 1
        else:
            buckets["over_7d"] += 1
    return buckets


def _round(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def build_scope_lag_summary(
    db: Session,
    *,
    scope_type: str,
    date_from: str,
    date_to: str,
    conversion_key: Optional[str] = None,
    channels: Optional[List[str]] = None,
) -> Dict[str, Any]:
    start, end = _selected_period_bounds(date_from, date_to)
    query = db.query(ConversionScopeDiagnosticFact).filter(
        ConversionScopeDiagnosticFact.scope_type == scope_type,
        ConversionScopeDiagnosticFact.conversion_ts >= start,
        ConversionScopeDiagnosticFact.conversion_ts <= end,
    )
    if conversion_key:
        query = query.filter(ConversionScopeDiagnosticFact.conversion_key == conversion_key)
    normalized_channels = [str(channel).strip() for channel in (channels or []) if str(channel).strip()]
    if normalized_channels:
        query = query.filter(ConversionScopeDiagnosticFact.scope_channel.in_(normalized_channels))

    rows = query.all()
    grouped: Dict[str, Dict[str, Any]] = {}
    overall_first_touch_days: List[float] = []
    overall_last_touch_days: List[float] = []

    for row in rows:
        first_touch_days: Optional[float] = None
        last_touch_days: Optional[float] = None
        if row.first_touch_ts and row.conversion_ts and row.conversion_ts >= row.first_touch_ts:
            first_touch_days = float((row.conversion_ts - row.first_touch_ts).total_seconds() / 86400.0)
            overall_first_touch_days.append(first_touch_days)
        if row.last_touch_ts and row.conversion_ts and row.conversion_ts >= row.last_touch_ts:
            last_touch_days = float((row.conversion_ts - row.last_touch_ts).total_seconds() / 86400.0)
            overall_last_touch_days.append(last_touch_days)

        scope_key = str(
            (
                row.scope_campaign
                if scope_type == "campaign" and row.scope_campaign
                else row.scope_key or row.scope_channel or "unknown"
            )
        )
        item = grouped.setdefault(
            scope_key,
            {
                "key": scope_key,
                "label": scope_key,
                "channel": str(row.scope_channel or ""),
                "campaign": str(row.scope_campaign or "") if row.scope_campaign else None,
                "conversions": 0,
                "first_touch_days": [],
                "last_touch_days": [],
                "role_mix": {
                    "first_touch_conversions": 0,
                    "assist_conversions": 0,
                    "last_touch_conversions": 0,
                },
            },
        )
        item["conversions"] += 1
        if first_touch_days is not None:
            item["first_touch_days"].append(first_touch_days)
        if last_touch_days is not None:
            item["last_touch_days"].append(last_touch_days)
        item["role_mix"]["first_touch_conversions"] += int(row.first_touch_conversions or 0)
        item["role_mix"]["assist_conversions"] += int(row.assist_conversions or 0)
        item["role_mix"]["last_touch_conversions"] += int(row.last_touch_conversions or 0)

    total_conversions = sum(int(item["conversions"]) for item in grouped.values())
    items: List[Dict[str, Any]] = []
    for item in grouped.values():
        first_days = list(item["first_touch_days"])
        last_days = list(item["last_touch_days"])
        first_buckets = _lag_bucket_counts(first_days)
        items.append(
            {
                "key": item["key"],
                "label": item["label"],
                "channel": item["channel"],
                "campaign": item["campaign"],
                "conversions": int(item["conversions"]),
                "share_of_conversions": round(
                    float(item["conversions"]) / float(total_conversions),
                    4,
                )
                if total_conversions > 0
                else 0.0,
                "avg_days_from_first_touch": _round(_mean(first_days)),
                "p50_days_from_first_touch": _round(_percentile(first_days, 0.5)),
                "p90_days_from_first_touch": _round(_percentile(first_days, 0.9)),
                "avg_days_from_last_touch": _round(_mean(last_days)),
                "p50_days_from_last_touch": _round(_percentile(last_days, 0.5)),
                "p90_days_from_last_touch": _round(_percentile(last_days, 0.9)),
                "lag_buckets": first_buckets,
                "role_mix": item["role_mix"],
            }
        )

    items.sort(
        key=lambda item: (
            -int(item.get("conversions") or 0),
            -(float(item.get("p50_days_from_first_touch") or 0.0)),
            str(item.get("label") or ""),
        )
    )

    over_7d = sum(int(item["lag_buckets"]["over_7d"]) for item in items)
    return {
        "scope_type": scope_type,
        "current_period": {"date_from": date_from, "date_to": date_to},
        "conversion_key": conversion_key,
        "items": items,
        "summary": {
            "conversions": total_conversions,
            "median_days_from_first_touch": _round(_percentile(overall_first_touch_days, 0.5)),
            "p90_days_from_first_touch": _round(_percentile(overall_first_touch_days, 0.9)),
            "median_days_from_last_touch": _round(_percentile(overall_last_touch_days, 0.5)),
            "p90_days_from_last_touch": _round(_percentile(overall_last_touch_days, 0.9)),
            "long_lag_share_over_7d": round(float(over_7d) / float(total_conversions), 4) if total_conversions > 0 else 0.0,
        },
    }
