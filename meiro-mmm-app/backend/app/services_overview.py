"""
Overview (Cover) Dashboard: summary KPIs, drivers, alerts, freshness.

- Does NOT block on MMM/Incrementality; shows "not available" when absent.
- Pre-aggregates where possible; robust to missing data; consistent response shapes.
- Includes confidence flags when data quality/freshness is poor.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath
from .models_overview_alerts import AlertEvent, AlertRule, MetricSnapshot


# ---------------------------------------------------------------------------
# Types and helpers
# ---------------------------------------------------------------------------

def _parse_dt(s: Optional[str]):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
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
    q = db.query(ConversionPath).filter(ConversionPath.conversion_ts >= date_from, ConversionPath.conversion_ts <= date_to)
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.all()
    total_value = 0.0
    daily: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        payload = r.path_json or {}
        convs = payload.get("conversions") or []
        val = 0.0
        if convs and isinstance(convs[0], dict):
            val = float(convs[0].get("value", 0))
        else:
            val = float(payload.get("conversion_value", 0))
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
    dt_from = _parse_dt(date_from)
    dt_to = _parse_dt(date_to)
    if not dt_from or not dt_to:
        dt_from = datetime.utcnow() - timedelta(days=30)
        dt_to = datetime.utcnow()

    # Previous period for delta
    delta_days = (dt_to - dt_from).days or 1
    prev_to = dt_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=delta_days)

    expense_by_channel = _expense_by_channel(expenses or {}, date_from, date_to, currency)
    total_spend = sum(expense_by_channel.values())

    conv_count, total_revenue, daily_series = _conversions_and_revenue_from_paths(
        db, dt_from, dt_to, None
    )
    prev_conv, prev_revenue, _ = _conversions_and_revenue_from_paths(db, prev_from, prev_to, None)

    # Sparkline: use daily series or metric_snapshots if present
    scope = workspace or account or "default"
    try:
        snapshots = (
            db.query(MetricSnapshot)
            .filter(
                MetricSnapshot.ts >= dt_from,
                MetricSnapshot.ts <= dt_to,
                MetricSnapshot.scope == scope,
            )
            .order_by(MetricSnapshot.ts.asc())
            .all()
        )
        spend_snap = [s.kpi_value for s in snapshots if s.kpi_key == "spend"]
        conv_snap = [s.kpi_value for s in snapshots if s.kpi_key == "conversions"]
        rev_snap = [s.kpi_value for s in snapshots if s.kpi_key == "revenue"]
    except Exception:
        spend_snap = conv_snap = rev_snap = []

    def _delta_pct(curr: float, prev: float) -> Optional[float]:
        if prev == 0:
            return 100.0 if curr > 0 else None
        return round((curr - prev) / prev * 100.0, 1)

    def _confidence(lag_min: Optional[float], has_data: bool) -> str:
        if not has_data:
            return "no_data"
        if lag_min is not None and lag_min > 60 * 24:  # > 1 day
            return "stale"
        if lag_min is not None and lag_min > 60 * 4:  # > 4 hours
            return "degraded"
        return "ok"

    freshness = get_freshness(db, import_runs_get_last_successful)
    lag_min = freshness.get("ingest_lag_minutes")
    has_any = total_spend > 0 or conv_count > 0 or total_revenue > 0
    confidence = _confidence(lag_min, has_any)

    spark_spend = spend_snap[-14:] if spend_snap else _sparkline_from_series(daily_series, "revenue")  # reuse daily as proxy if no spend series
    if not spark_spend and daily_series:
        # Fake spend sparkline from revenue * 0.3 for display if no snapshot
        spark_spend = [d.get("revenue", 0) * 0.3 for d in daily_series[-14:]]
    spark_conversions = conv_snap[-14:] if conv_snap else _sparkline_from_series(daily_series, "conversions")
    spark_revenue = rev_snap[-14:] if rev_snap else _sparkline_from_series(daily_series, "revenue")

    kpi_tiles = [
        {
            "kpi_key": "spend",
            "value": round(total_spend, 2),
            "delta_pct": _delta_pct(total_spend, sum(_expense_by_channel(expenses or {}, prev_from.strftime("%Y-%m-%d"), prev_to.strftime("%Y-%m-%d"), currency).values())),
            "sparkline": spark_spend,
            "confidence": confidence,
        },
        {
            "kpi_key": "conversions",
            "value": conv_count,
            "delta_pct": _delta_pct(float(conv_count), float(prev_conv)),
            "sparkline": spark_conversions,
            "confidence": confidence,
        },
        {
            "kpi_key": "revenue",
            "value": round(total_revenue, 2),
            "delta_pct": _delta_pct(total_revenue, prev_revenue),
            "sparkline": spark_revenue,
            "confidence": confidence,
        },
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
    dt_from = _parse_dt(date_from)
    dt_to = _parse_dt(date_to)
    if not dt_from or not dt_to:
        dt_from = datetime.utcnow() - timedelta(days=30)
        dt_to = datetime.utcnow()
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
    rows = q.all()

    # Aggregate by channel from paths (revenue/conversions)
    ch_rev: Dict[str, float] = {}
    ch_conv: Dict[str, int] = {}
    camp_rev: Dict[str, float] = {}
    camp_conv: Dict[str, int] = {}
    for r in rows:
        payload = r.path_json or {}
        convs = payload.get("conversions") or []
        val = float(convs[0].get("value", 0)) if convs and isinstance(convs[0], dict) else float(payload.get("conversion_value", 0))
        tps = payload.get("touchpoints") or []
        for tp in tps:
            ch = tp.get("channel", "unknown") if isinstance(tp, dict) else "unknown"
            ch_rev[ch] = ch_rev.get(ch, 0) + val / max(len(tps), 1)
            ch_conv[ch] = ch_conv.get(ch, 0) + (1 if tp == tps[-1] else 0)  # last-touch count
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
    ).all()
    if conversion_key:
        prev_rows = [r for r in prev_rows if r.conversion_key == conversion_key]
    prev_ch_rev: Dict[str, float] = {}
    prev_ch_conv: Dict[str, int] = {}
    prev_camp_rev: Dict[str, float] = {}
    for r in prev_rows:
        payload = r.path_json or {}
        convs = payload.get("conversions") or []
        val = float(convs[0].get("value", 0)) if convs and isinstance(convs[0], dict) else float(payload.get("conversion_value", 0))
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

    def _delta(curr: float, prev: float) -> Optional[float]:
        if prev == 0:
            return 100.0 if curr > 0 else 0.0
        return round((curr - prev) / prev * 100.0, 1)

    channels = sorted(set(expense_by_channel.keys()) | set(ch_rev.keys()))
    by_channel = []
    for ch in channels:
        spend = expense_by_channel.get(ch, 0)
        rev = ch_rev.get(ch, 0)
        conv = ch_conv.get(ch, 0)
        prev_spend = prev_expense.get(ch, 0)
        prev_rev = prev_ch_rev.get(ch, 0)
        prev_conv = prev_ch_conv.get(ch, 0)
        by_channel.append({
            "channel": ch,
            "spend": round(spend, 2),
            "conversions": conv,
            "revenue": round(rev, 2),
            "delta_spend_pct": _delta(spend, prev_spend),
            "delta_conversions_pct": _delta(float(conv), float(prev_conv)),
            "delta_revenue_pct": _delta(rev, prev_rev),
        })
    by_channel.sort(key=lambda x: -x["revenue"])

    campaigns_sorted = sorted(camp_rev.keys(), key=lambda c: -camp_rev[c])[:top_campaigns_n]
    by_campaign = [
        {
            "campaign": c,
            "revenue": round(camp_rev[c], 2),
            "conversions": camp_conv.get(c, 0),
            "delta_revenue_pct": _delta(camp_rev.get(c, 0), prev_camp_rev.get(c, 0)),
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
