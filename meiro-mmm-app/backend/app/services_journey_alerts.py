"""Journeys/Funnels alerts: definitions CRUD, evaluator, and events listing."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple
import uuid

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import (
    FunnelDefinition,
    JourneyAlertDefinition,
    JourneyAlertEvent,
    JourneyPathDaily,
    JourneyTransitionDaily,
)

ALERT_TYPES = {
    "path_cr_drop",
    "path_volume_change",
    "funnel_dropoff_spike",
    "ttc_shift",
}
ALERT_DOMAINS = {"journeys", "funnels"}
SEVERITIES = {"info", "warn", "critical"}
BASELINE_MODES = {"previous_period", "rolling_baseline"}


def _normalize_filters(scope: Dict[str, Any]) -> Dict[str, Any]:
    raw = scope.get("filters")
    if not isinstance(raw, dict):
        raw = {}
    return {
        "channel_group": raw.get("channel_group") or None,
        "campaign_id": raw.get("campaign_id") or None,
        "device": raw.get("device") or None,
        "country": raw.get("country") or None,
    }


def _parse_date(value: Any, fallback: date) -> date:
    if isinstance(value, date):
        return value
    if not value:
        return fallback
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return fallback


def _window_dates(now: date, condition: Dict[str, Any]) -> Tuple[date, date, date, date]:
    mode = str(condition.get("comparison_mode") or "previous_period")
    if mode not in BASELINE_MODES:
        mode = "previous_period"

    if mode == "rolling_baseline":
        current_days = max(1, min(30, int(condition.get("current_days") or 7)))
        baseline_days = max(7, min(90, int(condition.get("baseline_days") or 28)))
        curr_end = now
        curr_start = curr_end - timedelta(days=current_days - 1)
        base_end = curr_start - timedelta(days=1)
        base_start = base_end - timedelta(days=baseline_days - 1)
        return curr_start, curr_end, base_start, base_end

    period_days = max(1, min(90, int(condition.get("window_days") or 7)))
    curr_end = now
    curr_start = curr_end - timedelta(days=period_days - 1)
    base_end = curr_start - timedelta(days=1)
    base_start = base_end - timedelta(days=period_days - 1)
    return curr_start, curr_end, base_start, base_end


def _serialize_definition(item: JourneyAlertDefinition) -> Dict[str, Any]:
    return {
        "id": item.id,
        "name": item.name,
        "type": item.type,
        "domain": item.domain,
        "scope": item.scope_json or {},
        "metric": item.metric,
        "condition": item.condition_json or {},
        "schedule": item.schedule_json or {"cadence": "daily"},
        "is_enabled": bool(item.is_enabled),
        "created_by": item.created_by,
        "updated_by": item.updated_by,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


def _serialize_event(item: JourneyAlertEvent) -> Dict[str, Any]:
    details = item.details_json or {}
    return {
        "id": item.id,
        "alert_definition_id": item.alert_definition_id,
        "domain": item.domain,
        "triggered_at": item.triggered_at.isoformat() if item.triggered_at else None,
        "severity": item.severity,
        "summary": item.summary,
        "details": details,
        "deep_link": details.get("deep_link"),
    }


def list_alert_definitions(
    db: Session,
    *,
    domain: Optional[str] = None,
    page: int = 1,
    per_page: int = 20,
    is_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    q = db.query(JourneyAlertDefinition)
    if domain in ALERT_DOMAINS:
        q = q.filter(JourneyAlertDefinition.domain == domain)
    if is_enabled is not None:
        q = q.filter(JourneyAlertDefinition.is_enabled == is_enabled)
    total = q.count()
    rows = (
        q.order_by(JourneyAlertDefinition.updated_at.desc())
        .offset(max(0, page - 1) * per_page)
        .limit(max(1, min(100, per_page)))
        .all()
    )
    return {
        "items": [_serialize_definition(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


def list_alert_events(
    db: Session,
    *,
    domain: Optional[str] = None,
    page: int = 1,
    per_page: int = 20,
) -> Dict[str, Any]:
    q = db.query(JourneyAlertEvent)
    if domain in ALERT_DOMAINS:
        q = q.filter(JourneyAlertEvent.domain == domain)
    total = q.count()
    rows = (
        q.order_by(JourneyAlertEvent.triggered_at.desc())
        .offset(max(0, page - 1) * per_page)
        .limit(max(1, min(200, per_page)))
        .all()
    )
    return {
        "items": [_serialize_event(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


def create_alert_definition(
    db: Session,
    *,
    name: str,
    type: str,
    domain: str,
    scope: Dict[str, Any],
    metric: str,
    condition: Dict[str, Any],
    schedule: Optional[Dict[str, Any]],
    is_enabled: bool,
    actor: str,
) -> Dict[str, Any]:
    item = JourneyAlertDefinition(
        id=str(uuid.uuid4()),
        name=name.strip(),
        type=type,
        domain=domain,
        scope_json=scope,
        metric=metric,
        condition_json=condition,
        schedule_json=schedule or {"cadence": "daily"},
        is_enabled=bool(is_enabled),
        created_by=actor,
        updated_by=actor,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return _serialize_definition(item)


def update_alert_definition(
    db: Session,
    *,
    definition_id: str,
    actor: str,
    name: Optional[str] = None,
    is_enabled: Optional[bool] = None,
    condition: Optional[Dict[str, Any]] = None,
    schedule: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    item = db.get(JourneyAlertDefinition, definition_id)
    if not item:
        return None
    if name is not None:
        item.name = name.strip()
    if is_enabled is not None:
        item.is_enabled = bool(is_enabled)
    if condition is not None:
        item.condition_json = condition
    if schedule is not None:
        item.schedule_json = schedule
    item.updated_by = actor
    item.updated_at = datetime.utcnow()
    db.add(item)
    db.commit()
    db.refresh(item)
    return _serialize_definition(item)


def _apply_path_scope_filters(q, filters: Dict[str, Any]):
    if filters.get("channel_group"):
        q = q.filter(JourneyPathDaily.channel_group == str(filters["channel_group"]))
    if filters.get("campaign_id"):
        q = q.filter(JourneyPathDaily.campaign_id == str(filters["campaign_id"]))
    if filters.get("device"):
        q = q.filter(JourneyPathDaily.device == str(filters["device"]))
    if filters.get("country"):
        q = q.filter(func.lower(JourneyPathDaily.country) == str(filters["country"]).lower())
    return q


def _path_metric_for_period(
    db: Session,
    *,
    scope: Dict[str, Any],
    metric: str,
    date_from: date,
    date_to: date,
) -> Optional[float]:
    journey_definition_id = str(scope.get("journey_definition_id") or "").strip()
    if not journey_definition_id:
        return None

    q = db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == journey_definition_id,
        JourneyPathDaily.date >= date_from,
        JourneyPathDaily.date <= date_to,
    )
    path_hash = (scope.get("path_hash") or "").strip()
    if path_hash:
        q = q.filter(JourneyPathDaily.path_hash == path_hash)
    q = _apply_path_scope_filters(q, _normalize_filters(scope))

    sums = q.with_entities(
        func.sum(JourneyPathDaily.count_journeys),
        func.sum(JourneyPathDaily.count_conversions),
        func.sum((JourneyPathDaily.p50_time_to_convert_sec) * (JourneyPathDaily.count_conversions)),
    ).first()
    journeys = int(sums[0] or 0)
    conversions = int(sums[1] or 0)
    p50_weighted_sum = float(sums[2] or 0.0)
    if metric == "conversion_rate":
        return (float(conversions) / float(journeys)) if journeys > 0 else None
    if metric == "count_journeys":
        return float(journeys)
    if metric == "p50_time_to_convert_sec":
        return (p50_weighted_sum / float(conversions)) if conversions > 0 else None
    return None


def _funnel_step_dropoff_for_period(
    db: Session,
    *,
    scope: Dict[str, Any],
    date_from: date,
    date_to: date,
) -> Optional[float]:
    funnel_id = str(scope.get("funnel_id") or "").strip()
    step_index = int(scope.get("step_index") or 0)
    if not funnel_id:
        return None
    funnel = db.get(FunnelDefinition, funnel_id)
    if not funnel or funnel.is_archived:
        return None
    steps = [str(s).strip() for s in (funnel.steps_json or []) if str(s).strip()]
    if step_index < 0 or step_index >= len(steps) - 1:
        return None
    from_step = steps[step_index]
    to_step = steps[step_index + 1]
    filters = _normalize_filters(scope)

    base_q = db.query(JourneyTransitionDaily).filter(
        JourneyTransitionDaily.journey_definition_id == funnel.journey_definition_id,
        JourneyTransitionDaily.date >= date_from,
        JourneyTransitionDaily.date <= date_to,
        JourneyTransitionDaily.from_step == from_step,
    )
    if filters.get("channel_group"):
        base_q = base_q.filter(JourneyTransitionDaily.channel_group == str(filters["channel_group"]))
    if filters.get("campaign_id"):
        base_q = base_q.filter(JourneyTransitionDaily.campaign_id == str(filters["campaign_id"]))
    if filters.get("device"):
        base_q = base_q.filter(JourneyTransitionDaily.device == str(filters["device"]))
    if filters.get("country"):
        base_q = base_q.filter(func.lower(JourneyTransitionDaily.country) == str(filters["country"]).lower())

    denom = float(base_q.with_entities(func.sum(JourneyTransitionDaily.count_transitions)).scalar() or 0.0)
    numer = float(
        base_q.filter(JourneyTransitionDaily.to_step == to_step)
        .with_entities(func.sum(JourneyTransitionDaily.count_transitions))
        .scalar()
        or 0.0
    )
    if denom <= 0:
        return None
    conversion_rate = numer / denom
    return max(0.0, min(1.0, 1.0 - conversion_rate))


def _pct_delta(current: float, baseline: float) -> Optional[float]:
    if baseline == 0:
        return None
    return ((current - baseline) / baseline) * 100.0


def _build_deep_link(defn: JourneyAlertDefinition, details: Dict[str, Any]) -> Dict[str, Any]:
    scope = defn.scope_json or {}
    params: Dict[str, Any] = {"journey_id": scope.get("journey_definition_id")}
    window = details.get("window") or {}
    if window.get("current_from"):
        params["date_from"] = window["current_from"]
    if window.get("current_to"):
        params["date_to"] = window["current_to"]
    filters = _normalize_filters(scope)
    if filters.get("channel_group"):
        params["channel"] = filters["channel_group"]
    if filters.get("campaign_id"):
        params["campaign"] = filters["campaign_id"]
    if filters.get("device"):
        params["device"] = filters["device"]
    if filters.get("country"):
        params["geo"] = str(filters["country"]).lower()
    if scope.get("funnel_id"):
        params["funnel_id"] = scope.get("funnel_id")
    if scope.get("path_hash"):
        params["path_hash"] = scope.get("path_hash")
    query = "&".join([f"{k}={v}" for k, v in params.items() if v not in (None, "", "all")])
    return {
        "page": "analytics_journeys",
        "path": f"/analytics/journeys?{query}" if query else "/analytics/journeys",
        "params": params,
    }


def preview_alert(
    db: Session,
    *,
    type: str,
    scope: Dict[str, Any],
    metric: str,
    condition: Dict[str, Any],
    now: Optional[date] = None,
) -> Dict[str, Any]:
    today = now or datetime.utcnow().date()
    curr_from, curr_to, base_from, base_to = _window_dates(today, condition)
    if type == "funnel_dropoff_spike":
        current_value = _funnel_step_dropoff_for_period(db, scope=scope, date_from=curr_from, date_to=curr_to)
        baseline_value = _funnel_step_dropoff_for_period(db, scope=scope, date_from=base_from, date_to=base_to)
    else:
        current_value = _path_metric_for_period(db, scope=scope, metric=metric, date_from=curr_from, date_to=curr_to)
        baseline_value = _path_metric_for_period(db, scope=scope, metric=metric, date_from=base_from, date_to=base_to)
    delta_pct = None
    if current_value is not None and baseline_value is not None:
        delta_pct = _pct_delta(float(current_value), float(baseline_value))
    return {
        "current_value": current_value,
        "baseline_value": baseline_value,
        "delta_pct": delta_pct,
        "window": {
            "current_from": curr_from.isoformat(),
            "current_to": curr_to.isoformat(),
            "baseline_from": base_from.isoformat(),
            "baseline_to": base_to.isoformat(),
        },
    }


def _threshold_triggered(type: str, current: float, baseline: float, condition: Dict[str, Any]) -> bool:
    threshold_pct = float(condition.get("threshold_pct") or 20.0)
    absolute_threshold = condition.get("absolute_threshold")
    if type == "path_cr_drop":
        if absolute_threshold is not None and current < float(absolute_threshold):
            return True
        return baseline > 0 and current <= baseline * (1.0 - threshold_pct / 100.0)
    if type == "path_volume_change":
        delta_pct = _pct_delta(current, baseline)
        return delta_pct is not None and abs(delta_pct) >= threshold_pct
    if type == "funnel_dropoff_spike":
        return baseline > 0 and current >= baseline * (1.0 + threshold_pct / 100.0)
    if type == "ttc_shift":
        if absolute_threshold is not None and current > float(absolute_threshold):
            return True
        return baseline > 0 and current >= baseline * (1.0 + threshold_pct / 100.0)
    return False


def evaluate_alert_definitions(
    db: Session,
    *,
    domain: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    started = datetime.utcnow()
    now_dt = now or datetime.utcnow()
    today = now_dt.date()
    q = db.query(JourneyAlertDefinition).filter(JourneyAlertDefinition.is_enabled == True)  # noqa: E712
    if domain in ALERT_DOMAINS:
        q = q.filter(JourneyAlertDefinition.domain == domain)
    defs = q.all()

    evaluated = 0
    fired = 0
    skipped_cooldown = 0
    errors = 0

    for definition in defs:
        evaluated += 1
        try:
            condition = definition.condition_json or {}
            preview = preview_alert(
                db,
                type=definition.type,
                scope=definition.scope_json or {},
                metric=definition.metric,
                condition=condition,
                now=today,
            )
            current = preview.get("current_value")
            baseline = preview.get("baseline_value")
            if current is None or baseline is None:
                continue

            if not _threshold_triggered(definition.type, float(current), float(baseline), condition):
                continue

            cooldown_days = max(1, min(30, int(condition.get("cooldown_days") or 2)))
            last_event = (
                db.query(JourneyAlertEvent)
                .filter(JourneyAlertEvent.alert_definition_id == definition.id)
                .order_by(JourneyAlertEvent.triggered_at.desc())
                .first()
            )
            if last_event and last_event.triggered_at.date() == today:
                skipped_cooldown += 1
                continue
            if last_event and (today - last_event.triggered_at.date()).days < cooldown_days:
                skipped_cooldown += 1
                continue

            delta_pct = _pct_delta(float(current), float(baseline))
            metric_label = definition.metric
            summary = f"{definition.name}: {metric_label} moved materially ({delta_pct:.1f}% vs baseline)." if delta_pct is not None else f"{definition.name}: threshold triggered."
            details = {
                "computed": {
                    "current": current,
                    "baseline": baseline,
                    "delta_pct": delta_pct,
                    "metric": definition.metric,
                },
                "window": preview["window"],
                "scope": definition.scope_json or {},
                "filters": _normalize_filters(definition.scope_json or {}),
                "condition": condition,
            }
            details["deep_link"] = _build_deep_link(definition, details)
            severity = str(condition.get("severity") or "warn").lower()
            if severity not in SEVERITIES:
                severity = "warn"

            ev = JourneyAlertEvent(
                id=str(uuid.uuid4()),
                alert_definition_id=definition.id,
                domain=definition.domain,
                triggered_at=now_dt,
                severity=severity,
                summary=summary,
                details_json=details,
                dedupe_key=f"{definition.id}:{today.isoformat()}",
                created_at=now_dt,
            )
            db.add(ev)
            fired += 1
        except Exception:
            errors += 1
    db.commit()
    return {
        "evaluated": evaluated,
        "fired": fired,
        "skipped_cooldown": skipped_cooldown,
        "errors": errors,
        "duration_ms": int((datetime.utcnow() - started).total_seconds() * 1000),
    }
