"""
Alerts engine: evaluates alert_rules and produces alert_events.

Detection types (MVP):
- anomaly_kpi: z-score vs 28-period baseline (same weekday daily / same hour hourly), min_volume guardrail.
- threshold: kpi crosses threshold (>, <).
- data_freshness: last_touchpoint_ts older than X => warn/critical.
- pipeline_health: spend present but conversions missing (or vice versa) beyond tolerance.

Behavior:
- De-duplicate by fingerprint (rule_id + dims + period).
- Update existing open alert if condition persists; resolve when back to normal.
- Status lifecycle: open -> ack/snoozed -> resolved.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath
from .models_overview_alerts import AlertEvent, AlertRule, MetricSnapshot
from .services_overview import get_freshness as overview_get_freshness

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fingerprint and period
# ---------------------------------------------------------------------------


def make_fingerprint(rule_id: int, dimensions: Dict[str, Any], period_id: str) -> str:
    """Deterministic fingerprint for de-duplication: rule_id + dims + period."""
    dims_str = json.dumps(dimensions, sort_keys=True) if dimensions else ""
    payload = f"{rule_id}|{dims_str}|{period_id}"
    return hashlib.sha256(payload.encode()).hexdigest()[:64]


def period_id_for_schedule(now: datetime, schedule: str) -> str:
    """Canonical period identifier: daily => YYYY-MM-DD, hourly => YYYY-MM-DDTHH."""
    if schedule == "hourly":
        return now.strftime("%Y-%m-%dT%H")
    return now.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# KPI series from MetricSnapshot (baseline for anomaly)
# ---------------------------------------------------------------------------


def get_kpi_series_for_baseline(
    db: Session,
    scope: str,
    kpi_key: str,
    dimensions: Optional[Dict[str, Any]],
    schedule: str,
    now: datetime,
    lookback_periods: int = 28,
) -> List[float]:
    """
    Last N periods with same weekday (daily) or same hour (hourly).
    Returns list of kpi_value in chronological order (oldest first).
    """
    if schedule == "hourly":
        since = now - timedelta(hours=28 * 24 + 24)
    else:
        since = now - timedelta(days=28 * 7 + 7)

    q = (
        db.query(MetricSnapshot)
        .filter(
            MetricSnapshot.scope == scope,
            MetricSnapshot.kpi_key == kpi_key,
            MetricSnapshot.ts >= since,
            MetricSnapshot.ts < now,
        )
        .order_by(MetricSnapshot.ts.asc())
    )
    if dimensions:
        rows = q.all()
        filtered = [r for r in rows if (r.dimensions_json or {}) == dimensions]
        rows = filtered
    else:
        rows = q.all()

    if not rows:
        return []

    buckets: Dict[str, List[float]] = {}
    for r in rows:
        ts = r.ts
        key = ts.strftime("%Y-%m-%dT%H") if schedule == "hourly" else ts.strftime("%Y-%m-%d")
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(float(r.kpi_value))

    if schedule == "daily":
        target_weekday = now.weekday()
        period_keys = [
            k for k in sorted(buckets.keys())
            if datetime.strptime(k[:10], "%Y-%m-%d").weekday() == target_weekday
        ]
    else:
        target_hour = now.strftime("%H")
        period_keys = [k for k in sorted(buckets.keys()) if "T" in k and k.endswith("T" + target_hour)]

    values = []
    for k in sorted(period_keys)[-lookback_periods:]:
        vals = buckets.get(k, [])
        if vals:
            values.append(sum(vals) / len(vals))
    return values[-lookback_periods:]


def get_current_kpi_from_snapshots(
    db: Session,
    scope: str,
    kpi_key: str,
    dimensions: Optional[Dict[str, Any]],
    now: datetime,
    window_hours: int = 48,
) -> Optional[float]:
    """Latest period aggregate for this kpi_key + dimensions in the last window."""
    since = now - timedelta(hours=window_hours)
    q = (
        db.query(MetricSnapshot)
        .filter(
            MetricSnapshot.scope == scope,
            MetricSnapshot.kpi_key == kpi_key,
            MetricSnapshot.ts >= since,
            MetricSnapshot.ts <= now,
        )
        .order_by(MetricSnapshot.ts.desc())
    )
    rows = q.all()
    if dimensions:
        rows = [r for r in rows if (r.dimensions_json or {}) == dimensions]
    if not rows:
        return None
    return float(rows[0].kpi_value)


def get_current_kpi_from_paths_and_expenses(
    db: Session,
    scope: str,
    kpi_key: str,
    now: datetime,
    expenses: Any = None,
) -> Optional[float]:
    """Fallback: derive current period value from ConversionPath and expenses (no dimensions)."""
    from .services_overview import _conversions_and_revenue_from_paths, _expense_by_channel

    today = now.date()
    dt_from = datetime.combine(today, datetime.min.time())
    dt_to = now
    date_from = dt_from.strftime("%Y-%m-%d")
    date_to = dt_to.strftime("%Y-%m-%d")

    if kpi_key == "conversions":
        count, _, _ = _conversions_and_revenue_from_paths(db, dt_from, dt_to, None)
        return float(count)
    if kpi_key == "revenue":
        _, rev, _ = _conversions_and_revenue_from_paths(db, dt_from, dt_to, None)
        return rev
    if kpi_key == "spend" and expenses is not None:
        by_ch = _expense_by_channel(expenses, date_from, date_to, None)
        return sum(by_ch.values())
    return None


# ---------------------------------------------------------------------------
# Anomaly evaluator (z-score)
# ---------------------------------------------------------------------------


def _zscore(value: float, baseline_values: List[float]) -> Tuple[float, float, float]:
    """Returns (expected_mean, observed, zscore). If std=0, zscore=0."""
    if not baseline_values:
        return 0.0, value, 0.0
    mean = sum(baseline_values) / len(baseline_values)
    n = len(baseline_values)
    variance = sum((x - mean) ** 2 for x in baseline_values) / n if n else 0
    std = variance ** 0.5
    if std == 0:
        return mean, value, 0.0
    return mean, value, (value - mean) / std


def evaluate_anomaly_kpi(
    db: Session,
    rule: AlertRule,
    scope: str,
    now: datetime,
    expenses: Any = None,
) -> List[Tuple[bool, Dict[str, Any], str, str]]:
    """
    For (kpi_key, dimension scope), compare latest value vs 28-period baseline.
    Returns list of (triggered, context_json, fingerprint, period_id).
    """
    results: List[Tuple[bool, Dict[str, Any], str, str]] = []
    kpi_key = rule.kpi_key or "conversions"
    params = rule.params_json or {}
    zscore_threshold = params.get("zscore_threshold", 2.5)
    min_volume = params.get("min_volume")
    lookback_periods = params.get("lookback_periods", 28)
    schedule = rule.schedule or "daily"
    dimensions = rule.dimension_filters_json

    period_id = period_id_for_schedule(now, schedule)

    observed_val = get_current_kpi_from_snapshots(db, scope, kpi_key, dimensions, now)
    if observed_val is None:
        observed_val = get_current_kpi_from_paths_and_expenses(db, scope, kpi_key, now, expenses)
    if observed_val is None:
        logger.debug("No current value for kpi_key=%s scope=%s", kpi_key, scope)
        return []

    if min_volume is not None:
        mv = min_volume if isinstance(min_volume, (int, float)) else (min_volume.get(kpi_key) if isinstance(min_volume, dict) else None)
        if mv is not None and observed_val < mv:
            logger.debug("Below min_volume for %s: %.2f < %s", kpi_key, observed_val, mv)
            return []

    baseline = get_kpi_series_for_baseline(
        db, scope, kpi_key, dimensions, schedule, now, lookback_periods
    )
    if len(baseline) < 3:
        logger.debug("Insufficient baseline points for anomaly (%s): %d", kpi_key, len(baseline))
        return []

    expected, observed, zscore = _zscore(observed_val, baseline)
    context = {
        "kpi_key": kpi_key,
        "observed": observed,
        "expected": round(expected, 4),
        "zscore": round(zscore, 4),
        "baseline_n": len(baseline),
        "period_id": period_id,
        "dimensions": dimensions,
    }
    triggered = abs(zscore) >= zscore_threshold
    fp = make_fingerprint(rule.id, dimensions or {}, period_id)
    results.append((triggered, context, fp, period_id))
    return results


# ---------------------------------------------------------------------------
# Threshold evaluator
# ---------------------------------------------------------------------------


def evaluate_threshold(
    db: Session,
    rule: AlertRule,
    scope: str,
    now: datetime,
    expenses: Any = None,
) -> List[Tuple[bool, Dict[str, Any], str, str]]:
    """If kpi crosses threshold (>, <), create event."""
    results: List[Tuple[bool, Dict[str, Any], str, str]] = []
    kpi_key = rule.kpi_key or "conversions"
    params = rule.params_json or {}
    threshold_value = params.get("threshold_value")
    operator = params.get("operator", ">")
    dimensions = rule.dimension_filters_json
    schedule = rule.schedule or "daily"
    period_id = period_id_for_schedule(now, schedule)

    if threshold_value is None:
        return []

    current = get_current_kpi_from_snapshots(db, scope, kpi_key, dimensions, now)
    if current is None:
        current = get_current_kpi_from_paths_and_expenses(db, scope, kpi_key, now, expenses)
    if current is None:
        return []

    triggered = False
    if operator == ">":
        triggered = current > threshold_value
    elif operator == "<":
        triggered = current < threshold_value
    elif operator == ">=":
        triggered = current >= threshold_value
    elif operator == "<=":
        triggered = current <= threshold_value

    context = {
        "kpi_key": kpi_key,
        "observed": current,
        "threshold_value": threshold_value,
        "operator": operator,
        "period_id": period_id,
        "dimensions": dimensions,
    }
    fp = make_fingerprint(rule.id, dimensions or {}, period_id)
    results.append((triggered, context, fp, period_id))
    return results


# ---------------------------------------------------------------------------
# Data freshness evaluator
# ---------------------------------------------------------------------------


def _parse_ts(ts_value: Any) -> Optional[datetime]:
    if ts_value is None:
        return None
    if isinstance(ts_value, datetime):
        return ts_value
    try:
        s = str(ts_value).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def evaluate_data_freshness(
    db: Session,
    rule: AlertRule,
    scope: str,
    now: datetime,
    get_freshness_fn: Optional[Callable[[], Dict[str, Any]]] = None,
) -> List[Tuple[bool, Dict[str, Any], str, str]]:
    """
    If last_touchpoint_ts older than X minutes/hours => warn/critical.
    """
    results: List[Tuple[bool, Dict[str, Any], str, str]] = []
    params = rule.params_json or {}
    max_age_minutes = params.get("max_age_minutes")
    max_age_hours = params.get("max_age_hours")
    if max_age_minutes is None and max_age_hours is None:
        max_age_minutes = 60 * 24
    elif max_age_hours is not None:
        max_age_minutes = max_age_hours * 60

    if get_freshness_fn is None:
        def _freshness():
            return overview_get_freshness(db, None)
        get_freshness_fn = _freshness

    freshness = get_freshness_fn()
    last_touch_ts = _parse_ts(freshness.get("last_touchpoint_ts"))
    last_conv_ts = _parse_ts(freshness.get("last_conversion_ts"))
    last_ts = max(last_touch_ts, last_conv_ts) if (last_touch_ts and last_conv_ts) else (last_touch_ts or last_conv_ts)

    period_id = period_id_for_schedule(now, rule.schedule or "daily")
    dims = rule.dimension_filters_json or {}
    fp = make_fingerprint(rule.id, dims, period_id)

    if last_ts is None:
        context = {
            "last_touchpoint_ts": None,
            "last_conversion_ts": None,
            "max_age_minutes": max_age_minutes,
            "age_minutes": None,
            "message": "No touchpoint or conversion data",
        }
        results.append((True, context, fp, period_id))
        return results

    age_minutes = (now - last_ts).total_seconds() / 60.0
    triggered = age_minutes > max_age_minutes
    context = {
        "last_touchpoint_ts": freshness.get("last_touchpoint_ts"),
        "last_conversion_ts": freshness.get("last_conversion_ts"),
        "max_age_minutes": max_age_minutes,
        "age_minutes": round(age_minutes, 1),
    }
    results.append((triggered, context, fp, period_id))
    return results


# ---------------------------------------------------------------------------
# Pipeline health evaluator
# ---------------------------------------------------------------------------


def evaluate_pipeline_health(
    db: Session,
    rule: AlertRule,
    scope: str,
    now: datetime,
    get_freshness_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    expenses: Any = None,
) -> List[Tuple[bool, Dict[str, Any], str, str]]:
    """If spend present but conversions missing (or vice versa) beyond tolerance => warn."""
    results: List[Tuple[bool, Dict[str, Any], str, str]] = []
    params = rule.params_json or {}
    tolerance_hours = params.get("tolerance_hours", 24)

    from .services_overview import _conversions_and_revenue_from_paths, _expense_by_channel

    since = now - timedelta(hours=tolerance_hours)
    date_from = since.strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    dt_from = since
    dt_to = now

    conv_count, total_revenue, _ = _conversions_and_revenue_from_paths(db, dt_from, dt_to, None)
    spend = 0.0
    if expenses is not None:
        by_ch = _expense_by_channel(expenses, date_from, date_to, None)
        spend = sum(by_ch.values())

    period_id = period_id_for_schedule(now, rule.schedule or "daily")
    dims = rule.dimension_filters_json or {}
    fp = make_fingerprint(rule.id, dims, period_id)

    spend_no_conv = spend > 0 and conv_count == 0
    conv_no_spend = conv_count > 0 and spend == 0
    triggered = spend_no_conv or conv_no_spend

    context = {
        "spend": round(spend, 2),
        "conversions": conv_count,
        "revenue": round(total_revenue, 2),
        "tolerance_hours": tolerance_hours,
        "spend_no_conversions": spend_no_conv,
        "conversions_no_spend": conv_no_spend,
    }
    results.append((triggered, context, fp, period_id))
    return results


# ---------------------------------------------------------------------------
# Engine: run rules, dedupe, create/update/resolve
# ---------------------------------------------------------------------------

EVALUATORS = {
    "anomaly_kpi": evaluate_anomaly_kpi,
    "threshold": evaluate_threshold,
    "data_freshness": evaluate_data_freshness,
    "pipeline_health": evaluate_pipeline_health,
}


def _find_open_event_by_fingerprint(db: Session, fingerprint: str) -> Optional[AlertEvent]:
    return (
        db.query(AlertEvent)
        .filter(AlertEvent.fingerprint == fingerprint, AlertEvent.status.in_(["open", "ack", "snoozed"]))
        .first()
    )


def _resolve_open_events_for_rule(
    db: Session,
    rule_id: int,
    current_fingerprints: List[str],
) -> int:
    """Resolve open events for this rule whose fingerprint is no longer in current_fingerprints."""
    open_events = (
        db.query(AlertEvent)
        .filter(
            AlertEvent.rule_id == rule_id,
            AlertEvent.status.in_(["open", "ack", "snoozed"]),
            AlertEvent.fingerprint.isnot(None),
        )
        .all()
    )
    resolved = 0
    current_set = set(current_fingerprints)
    for ev in open_events:
        if ev.fingerprint and ev.fingerprint not in current_set:
            ev.status = "resolved"
            ev.updated_at = datetime.utcnow()
            resolved += 1
    return resolved


def run_alerts_engine(
    db: Session,
    scope: str,
    now: Optional[datetime] = None,
    *,
    import_runs_get_last_successful: Optional[Callable] = None,
    expenses: Any = None,
) -> Dict[str, Any]:
    """
    Evaluate all enabled rules for scope at `now`; create/update/resolve alert_events.
    Returns metrics: rules_evaluated, events_created, events_updated, events_resolved.
    """
    now = now or datetime.utcnow()
    metrics = {
        "scope": scope,
        "run_at": now.isoformat(),
        "rules_evaluated": 0,
        "events_created": 0,
        "events_updated": 0,
        "events_resolved": 0,
        "created_event_ids": [],
    }

    rules = (
        db.query(AlertRule)
        .filter(AlertRule.scope == scope, AlertRule.is_enabled == True)
        .all()
    )

    def get_freshness():
        return overview_get_freshness(db, import_runs_get_last_successful)

    current_fingerprints_by_rule: Dict[int, List[str]] = {}

    for rule in rules:
        evaluator = EVALUATORS.get(rule.rule_type)
        if not evaluator:
            logger.warning("Unknown rule_type=%s for rule id=%s", rule.rule_type, rule.id)
            continue

        metrics["rules_evaluated"] += 1
        try:
            if rule.rule_type == "anomaly_kpi":
                outcomes = evaluator(db, rule, scope, now, expenses=expenses)
            elif rule.rule_type == "threshold":
                outcomes = evaluator(db, rule, scope, now, expenses=expenses)
            elif rule.rule_type == "data_freshness":
                outcomes = evaluator(db, rule, scope, now, get_freshness_fn=get_freshness)
            elif rule.rule_type == "pipeline_health":
                outcomes = evaluator(db, rule, scope, now, get_freshness_fn=get_freshness, expenses=expenses)
            else:
                outcomes = []
        except Exception as e:
            logger.exception("Rule %s (%s) failed: %s", rule.id, rule.name, e)
            continue

        rule_fingerprints: List[str] = []
        for triggered, context, fingerprint, period_id in outcomes:
            rule_fingerprints.append(fingerprint)
            existing = _find_open_event_by_fingerprint(db, fingerprint)

            if triggered:
                title = rule.name
                message = _format_message(rule.rule_type, context)
                if existing:
                    existing.ts_detected = now
                    existing.message = message
                    existing.context_json = context
                    existing.updated_at = now
                    metrics["events_updated"] += 1
                else:
                    ev = AlertEvent(
                        rule_id=rule.id,
                        ts_detected=now,
                        severity=rule.severity,
                        title=title,
                        message=message,
                        context_json=context,
                        status="open",
                        fingerprint=fingerprint,
                    )
                    db.add(ev)
                    db.flush()  # so ev.id is set for notification delivery
                    metrics["events_created"] += 1
                    metrics["created_event_ids"].append(ev.id)
            else:
                if existing:
                    existing.status = "resolved"
                    existing.updated_at = now
                    metrics["events_resolved"] += 1

        current_fingerprints_by_rule[rule.id] = rule_fingerprints

    for rule in rules:
        if rule.rule_type not in EVALUATORS:
            continue
        fps = current_fingerprints_by_rule.get(rule.id, [])
        metrics["events_resolved"] += _resolve_open_events_for_rule(db, rule.id, fps)

    try:
        db.commit()
    except Exception as e:
        logger.exception("Alerts engine commit failed: %s", e)
        db.rollback()
        raise

    logger.info(
        "Alerts engine run: scope=%s rules_evaluated=%d events_created=%d events_updated=%d events_resolved=%d",
        scope,
        metrics["rules_evaluated"],
        metrics["events_created"],
        metrics["events_updated"],
        metrics["events_resolved"],
    )
    return metrics


def _format_message(rule_type: str, context: Dict[str, Any]) -> str:
    if rule_type == "anomaly_kpi":
        return (
            f"KPI {context.get('kpi_key', '')} anomaly: observed={context.get('observed')} "
            f"expected≈{context.get('expected')} z-score={context.get('zscore')}"
        )
    if rule_type == "threshold":
        return (
            f"KPI {context.get('kpi_key', '')} {context.get('operator')} {context.get('threshold_value')}: "
            f"observed={context.get('observed')}"
        )
    if rule_type == "data_freshness":
        age = context.get("age_minutes")
        if age is not None:
            return f"Data freshness: last touchpoint {age:.0f} min ago (max {context.get('max_age_minutes')} min)"
        return context.get("message", "Data freshness: no touchpoint/conversion data")
    if rule_type == "pipeline_health":
        if context.get("spend_no_conversions"):
            return "Pipeline health: spend present but no conversions in tolerance window"
        if context.get("conversions_no_spend"):
            return "Pipeline health: conversions present but no spend in tolerance window"
        return "Pipeline health alert"
    return str(context)
