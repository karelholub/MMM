"""
Overview (Cover) Dashboard: summary KPIs, drivers, alerts, freshness.

- Does NOT block on MMM/Incrementality; shows "not available" when absent.
- Pre-aggregates where possible; robust to missing data; consistent response shapes.
- Includes confidence flags when data quality/freshness is poor.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath
from .models_overview_alerts import AlertEvent, AlertRule
from .services_metrics import delta_pct
from .services_revenue_config import compute_payload_revenue_value, get_revenue_config


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


def _expense_by_channel(
    expenses: Any,
    date_from: Optional[str],
    date_to: Optional[str],
    currency_filter: Optional[str],
) -> Dict[str, float]:
    """Aggregate expenses by channel. expenses: dict[id -> ExpenseEntry] or list of dicts."""
    by_ch: Dict[str, float] = {}
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
        if date_from and start and start < date_from:
            continue
        if date_to and start:
            end = getattr(exp, "service_period_end", None) if not isinstance(entry, dict) else entry.get("service_period_end")
            if end and end > date_to:
                continue
        by_ch[ch] = by_ch.get(ch, 0.0) + float(amount)
    return by_ch


def _conversions_and_revenue_from_paths(
    db: Session,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    conversion_key: Optional[str],
) -> Tuple[int, float, List[Dict[str, Any]]]:
    """Returns (conversions_count, total_revenue, daily_series for sparkline)."""
    revenue_config = get_revenue_config()
    dedupe_seen = set()
    q = db.query(ConversionPath).filter(ConversionPath.conversion_ts >= date_from, ConversionPath.conversion_ts <= date_to)
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).all()
    total_value = 0.0
    daily: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        payload = r.path_json or {}
        val = compute_payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen,
            fallback_conversion_id=str(getattr(r, "conversion_id", "") or ""),
        )
        total_value += val
        d = r.conversion_ts.date().isoformat() if hasattr(r.conversion_ts, "date") else str(r.conversion_ts)[:10]
        if d not in daily:
            daily[d] = {"date": d, "conversions": 0, "revenue": 0.0}
        daily[d]["conversions"] += 1
        daily[d]["revenue"] += val
    series = sorted(daily.values(), key=lambda x: x["date"])
    return len(rows), total_value, series


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


def _normalize_period_bounds(date_from: str, date_to: str) -> Tuple[datetime, datetime]:
    dt_from = _parse_dt(date_from) or (datetime.utcnow() - timedelta(days=30))
    dt_to = _parse_dt(date_to) or datetime.utcnow()
    if isinstance(date_from, str) and len(date_from) == 10:
        dt_from = dt_from.replace(hour=0, minute=0, second=0, microsecond=0)
    if isinstance(date_to, str) and len(date_to) == 10:
        dt_to = dt_to.replace(hour=23, minute=59, second=59, microsecond=999999)
    if dt_to < dt_from:
        dt_from, dt_to = dt_to, dt_from
    return dt_from, dt_to


def _bucket_key(ts: datetime, grain: str) -> str:
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
) -> Dict[str, Any]:
    revenue_config = get_revenue_config()
    dedupe_seen = set()
    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= start,
        ConversionPath.conversion_ts <= end,
    )
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).all()
    conv_map: Dict[str, float] = {}
    rev_map: Dict[str, float] = {}
    total_revenue = 0.0
    for row in rows:
        payload = row.path_json or {}
        value = _payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen,
            fallback_conversion_id=str(getattr(row, "conversion_id", "") or ""),
        )
        total_revenue += value
        key = _bucket_key(row.conversion_ts, grain)
        conv_map[key] = conv_map.get(key, 0.0) + 1.0
        rev_map[key] = rev_map.get(key, 0.0) + value
    return {
        "conversions_total": len(rows),
        "revenue_total": round(total_revenue, 2),
        "conversions_map": conv_map,
        "revenue_map": rev_map,
        "observed_points": len(rows),
    }


def _series_from_visits(
    db: Session,
    start: datetime,
    end: datetime,
    grain: str,
) -> Dict[str, Any]:
    visit_map: Dict[str, float] = {}
    observed_points = 0
    q = db.query(ConversionPath).filter(
        ConversionPath.last_touch_ts >= start,
        ConversionPath.first_touch_ts <= end,
    )
    rows = q.all()
    for row in rows:
        payload = row.path_json or {}
        for tp in payload.get("touchpoints") or []:
            if not isinstance(tp, dict):
                continue
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


def _series_from_expenses(
    expenses: Any,
    start: datetime,
    end: datetime,
    grain: str,
) -> Dict[str, Any]:
    amount_map: Dict[str, float] = {}
    observed_points = 0
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
    expenses: Any = None,
    import_runs_get_last_successful: Any = None,
) -> Dict[str, Any]:
    """
    Returns kpi_tiles, highlights, freshness.
    Does not require MMM/Incrementality; uses raw paths + expenses.
    """
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to)
    period_span = dt_to - dt_from
    prev_to = dt_from - timedelta(microseconds=1)
    prev_from = prev_to - period_span
    grain = "hourly" if period_span <= timedelta(days=2) else "daily"
    bucket_keys_current = _bucket_keys_in_range(dt_from, dt_to, grain)
    bucket_keys_prev = _bucket_keys_in_range(prev_from, prev_to, grain)
    expected_points = len(bucket_keys_current)

    current_paths = _series_from_conversion_paths(db, dt_from, dt_to, grain)
    prev_paths = _series_from_conversion_paths(db, prev_from, prev_to, grain)
    current_visits = _series_from_visits(db, dt_from, dt_to, grain)
    prev_visits = _series_from_visits(db, prev_from, prev_to, grain)
    current_expenses = _series_from_expenses(expenses or {}, dt_from, dt_to, grain)
    prev_expenses = _series_from_expenses(expenses or {}, prev_from, prev_to, grain)

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

    return {
        "kpi_tiles": kpi_tiles,
        "highlights": highlights,
        "freshness": freshness,
        "current_period": current_period,
        "previous_period": previous_period,
        "model_id": model_id,
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
    expenses: Any = None,
    top_campaigns_n: int = 10,
    conversion_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Top drivers: by_channel (spend, conversions, revenue, delta), by_campaign (top N), biggest_movers.
    Uses ConversionPath + expenses only; does not block on MMM.
    """
    revenue_config = get_revenue_config()
    dedupe_seen_current = set()
    dedupe_seen_prev = set()

    dt_from = _parse_dt(date_from)
    dt_to = _parse_dt(date_to)
    if not dt_from or not dt_to:
        dt_from = datetime.now(timezone.utc) - timedelta(days=30)
        dt_to = datetime.now(timezone.utc)
    delta_days = max((dt_to - dt_from).days, 1)
    prev_to = dt_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=delta_days)

    expense_by_channel = _expense_by_channel(expenses or {}, date_from, date_to, None)
    prev_expense = _expense_by_channel(expenses or {}, prev_from.strftime("%Y-%m-%d"), prev_to.strftime("%Y-%m-%d"), None)

    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= dt_from,
        ConversionPath.conversion_ts <= dt_to,
    )
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).all()

    # Aggregate by channel from paths (revenue/conversions)
    ch_rev: Dict[str, float] = {}
    ch_conv: Dict[str, int] = {}
    camp_rev: Dict[str, float] = {}
    camp_conv: Dict[str, int] = {}
    for r in rows:
        payload = r.path_json or {}
        val = compute_payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen_current,
            fallback_conversion_id=str(getattr(r, "conversion_id", "") or ""),
        )
        tps = payload.get("touchpoints") or []
        for idx, tp in enumerate(tps):
            ch = tp.get("channel", "unknown") if isinstance(tp, dict) else "unknown"
            ch_rev[ch] = ch_rev.get(ch, 0) + val / max(len(tps), 1)
            ch_conv[ch] = ch_conv.get(ch, 0) + (1 if idx == len(tps) - 1 else 0)  # last-touch count
        if tps:
            last = tps[-1] if isinstance(tps[-1], dict) else {}
            camp = last.get("campaign") or last.get("campaign_name") if isinstance(last, dict) else "unknown"
            if isinstance(camp, dict):
                camp = camp.get("name", "unknown")
            camp = camp or "unknown"
            camp_rev[camp] = camp_rev.get(camp, 0) + val
            camp_conv[camp] = camp_conv.get(camp, 0) + 1

    prev_rows = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= prev_from,
        ConversionPath.conversion_ts <= prev_to,
    ).order_by(ConversionPath.conversion_ts.desc()).all()
    if conversion_key:
        prev_rows = [r for r in prev_rows if r.conversion_key == conversion_key]
    prev_ch_rev: Dict[str, float] = {}
    prev_ch_conv: Dict[str, int] = {}
    prev_camp_rev: Dict[str, float] = {}
    for r in prev_rows:
        payload = r.path_json or {}
        val = compute_payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen_prev,
            fallback_conversion_id=str(getattr(r, "conversion_id", "") or ""),
        )
        tps = payload.get("touchpoints") or []
        for tp in tps:
            ch = tp.get("channel", "unknown") if isinstance(tp, dict) else "unknown"
            prev_ch_rev[ch] = prev_ch_rev.get(ch, 0) + val / max(len(tps), 1)
        if tps:
            last = tps[-1] if isinstance(tps[-1], dict) else {}
            ch_last = last.get("channel", "unknown") if isinstance(last, dict) else "unknown"
            prev_ch_conv[ch_last] = prev_ch_conv.get(ch_last, 0) + 1
            camp = last.get("campaign") or last.get("campaign_name") or "unknown"
            if isinstance(camp, dict):
                camp = camp.get("name", "unknown")
            prev_camp_rev[camp] = prev_camp_rev.get(camp, 0) + val

    ch_visits: Dict[str, int] = {}
    prev_ch_visits: Dict[str, int] = {}
    visit_rows = db.query(ConversionPath).filter(
        ConversionPath.last_touch_ts >= dt_from,
        ConversionPath.first_touch_ts <= dt_to,
    ).all()
    for r in visit_rows:
        for tp in (r.path_json or {}).get("touchpoints") or []:
            if not isinstance(tp, dict):
                continue
            ts = _parse_dt(tp.get("timestamp") or tp.get("ts"))
            if not ts or ts < dt_from or ts > dt_to:
                continue
            ch = tp.get("channel", "unknown")
            ch_visits[ch] = ch_visits.get(ch, 0) + 1

    prev_visit_rows = db.query(ConversionPath).filter(
        ConversionPath.last_touch_ts >= prev_from,
        ConversionPath.first_touch_ts <= prev_to,
    ).all()
    for r in prev_visit_rows:
        for tp in (r.path_json or {}).get("touchpoints") or []:
            if not isinstance(tp, dict):
                continue
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
        })
    by_channel.sort(key=lambda x: -x["revenue"])

    campaigns_sorted = sorted(camp_rev.keys(), key=lambda c: -camp_rev[c])[:top_campaigns_n]
    by_campaign = [
        {
            "campaign": c,
            "revenue": round(camp_rev[c], 2),
            "conversions": camp_conv.get(c, 0),
            "delta_revenue_pct": delta_pct(camp_rev.get(c, 0), prev_camp_rev.get(c, 0)),
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
        "date_from": date_from,
        "date_to": date_to,
    }


def _safe_div(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-9:
        return 0.0
    return numerator / denominator


def _payload_revenue_value(
    payload: Dict[str, Any],
    revenue_config: Dict[str, Any],
    *,
    dedupe_seen: Optional[set[str]] = None,
    fallback_conversion_id: Optional[str] = None,
) -> float:
    value = compute_payload_revenue_value(
        payload,
        revenue_config,
        dedupe_seen=dedupe_seen,
        fallback_conversion_id=fallback_conversion_id,
    )
    if abs(value) > 1e-9:
        return float(value)
    fallback_raw = payload.get("value")
    if fallback_raw in (None, "", []):
        fallback_raw = payload.get("conversion_value")
    try:
        return float(fallback_raw or 0.0)
    except Exception:
        return 0.0


def _aggregate_channel_metrics(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
) -> Dict[str, Dict[str, float]]:
    revenue_config = get_revenue_config()
    dedupe_seen = set()
    metrics: Dict[str, Dict[str, float]] = {}
    query_from = dt_from.replace(tzinfo=None) if dt_from.tzinfo is not None else dt_from
    query_to = dt_to.replace(tzinfo=None) if dt_to.tzinfo is not None else dt_to

    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= query_from,
        ConversionPath.conversion_ts <= query_to,
    )
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).all()
    for row in rows:
        payload = row.path_json or {}
        if not isinstance(payload, dict):
            payload = {}
        touchpoints = payload.get("touchpoints") or []
        if not isinstance(touchpoints, list):
            touchpoints = []
        value = compute_payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen,
            fallback_conversion_id=str(getattr(row, "conversion_id", "") or ""),
        )
        for idx, tp in enumerate(touchpoints):
            if not isinstance(tp, dict):
                continue
            channel = _touchpoint_channel(tp)
            entry = metrics.setdefault(channel, {"visits": 0.0, "conversions": 0.0, "revenue": 0.0})
            entry["visits"] += 1.0
            if idx == len(touchpoints) - 1:
                entry["conversions"] += 1.0
                entry["revenue"] += float(value or 0.0)
    return metrics


def _aggregate_daily_channel_revenue(
    db: Session,
    *,
    dt_from: datetime,
    dt_to: datetime,
    conversion_key: Optional[str] = None,
) -> Dict[str, Dict[str, float]]:
    revenue_config = get_revenue_config()
    dedupe_seen = set()
    out: Dict[str, Dict[str, float]] = {}
    query_from = dt_from.replace(tzinfo=None) if dt_from.tzinfo is not None else dt_from
    query_to = dt_to.replace(tzinfo=None) if dt_to.tzinfo is not None else dt_to
    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= query_from,
        ConversionPath.conversion_ts <= query_to,
    )
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).all()
    for row in rows:
        payload = row.path_json or {}
        if not isinstance(payload, dict):
            payload = {}
        touchpoints = payload.get("touchpoints") or []
        if not isinstance(touchpoints, list) or not touchpoints:
            continue
        last_tp = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
        channel = _touchpoint_channel(last_tp if isinstance(last_tp, dict) else {})
        ts = _as_datetime(getattr(row, "conversion_ts", None))
        if ts is None:
            continue
        day = ts.date().isoformat()
        entry = out.setdefault(channel, {})
        entry[day] = entry.get(day, 0.0) + _payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen,
            fallback_conversion_id=str(getattr(row, "conversion_id", "") or ""),
        )
    return out


def get_overview_trend_insights(
    db: Session,
    *,
    date_from: str,
    date_to: str,
    conversion_key: Optional[str] = None,
) -> Dict[str, Any]:
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to)
    period_span = dt_to - dt_from
    prev_to = dt_from - timedelta(microseconds=1)
    prev_from = prev_to - period_span

    current = _aggregate_channel_metrics(db, dt_from=dt_from, dt_to=dt_to, conversion_key=conversion_key)
    previous = _aggregate_channel_metrics(db, dt_from=prev_from, dt_to=prev_to, conversion_key=conversion_key)

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
    daily_revenue = _aggregate_daily_channel_revenue(db, dt_from=momentum_prev_from, dt_to=dt_to, conversion_key=conversion_key)
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
    }


def get_overview_funnels(
    db: Session,
    *,
    date_from: str,
    date_to: str,
    conversion_key: Optional[str] = None,
    limit: int = 5,
) -> Dict[str, Any]:
    dt_from, dt_to = _normalize_period_bounds(date_from, date_to)
    revenue_config = get_revenue_config()
    dedupe_seen = set()

    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= dt_from,
        ConversionPath.conversion_ts <= dt_to,
    )
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).all()

    aggs: Dict[str, Dict[str, Any]] = {}
    total_conversions = 0
    path_length_counts: Dict[int, int] = defaultdict(int)
    for row in rows:
        payload = row.path_json or {}
        if not isinstance(payload, dict):
            payload = {}
        steps = _path_steps_from_payload(payload)
        path_key = " > ".join(steps)
        revenue = _payload_revenue_value(
            payload,
            revenue_config,
            dedupe_seen=dedupe_seen,
            fallback_conversion_id=str(getattr(row, "conversion_id", "") or ""),
        )
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
        "summary": {
            "total_conversions": total_conversions,
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
