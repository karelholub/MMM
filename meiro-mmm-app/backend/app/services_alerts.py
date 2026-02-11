"""
Alerts API: alert events (list, detail, ack, snooze, resolve) and alert rules CRUD.
- Pagination and filtering for list endpoints.
- Safe params_json validation for rules.
- Deep link fields: entity_type, entity_id, url for UI navigation.
- Audit: created_by, updated_by filled where applicable.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .models_overview_alerts import AlertEvent, AlertRule


# Allowed rule types and severities for validation
RULE_TYPES = ("anomaly_kpi", "threshold", "data_freshness", "pipeline_health")
SEVERITIES = ("info", "warn", "critical")
STATUSES = ("open", "ack", "snoozed", "resolved")
SCHEDULES = ("hourly", "daily")

# Safe keys for params_json per rule_type (alerts engine + API)
PARAMS_SAFE_KEYS = {
    "anomaly_kpi": {"zscore_threshold", "lookback_days", "lookback_periods", "min_volume"},
    "threshold": {"threshold_value", "threshold_direction", "operator", "lookback_days", "min_volume"},
    "data_freshness": {"max_lag_hours", "max_age_minutes", "max_age_hours", "lookback_days"},
    "pipeline_health": {"max_failure_count", "window_hours", "tolerance_hours"},
}


def _validate_params_json(rule_type: str, params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Validate params_json: only allow known keys and safe value types (str, int, float, bool)."""
    if params is None:
        return None
    if not isinstance(params, dict):
        return None
    allowed = PARAMS_SAFE_KEYS.get(rule_type, set())
    out = {}
    for k, v in params.items():
        if not isinstance(k, str) or len(k) > 128:
            continue
        if allowed and k not in allowed:
            continue
        if v is None:
            out[k] = None
        elif isinstance(v, (str, int, float, bool)):
            if isinstance(v, str) and len(v) > 1024:
                continue
            out[k] = v
        elif isinstance(v, list) and all(isinstance(x, (str, int, float, bool)) for x in v):
            out[k] = v[:100]  # cap list size
        else:
            continue
    return out if out else None


def _build_deep_link(ev: AlertEvent, base_url: str = "/dashboard") -> Dict[str, Any]:
    """Build deep link: entity_type, entity_id, url for UI navigation."""
    related = ev.related_entities_json or {}
    if not isinstance(related, dict):
        related = {}
    ctx = ev.context_json or {}
    if not isinstance(ctx, dict):
        ctx = {}
    entity_type = "data_quality"
    entity_id = None
    if related.get("channel"):
        entity_type = "channel"
        entity_id = related.get("channel")
    elif related.get("campaign_id"):
        entity_type = "campaign"
        entity_id = related.get("campaign_id")
    elif related.get("pipeline"):
        entity_type = "pipeline"
        entity_id = related.get("pipeline")
    kpi_key = ctx.get("kpi_key") or (ev.rule.kpi_key if ev.rule else None)
    url = f"{base_url}/data-quality?alert_id={ev.id}"
    if kpi_key:
        url += f"&kpi={kpi_key}"
    if entity_type == "channel" and entity_id:
        url = f"{base_url}/attribution/performance?channel={entity_id}"
    elif entity_type == "campaign" and entity_id:
        url = f"{base_url}/attribution/campaign-performance?campaign={entity_id}"
    return {"entity_type": entity_type, "entity_id": entity_id, "url": url}


# ---------------------------------------------------------------------------
# Alerts (events)
# ---------------------------------------------------------------------------


def list_alerts(
    db: Session,
    status: str = "open",
    severity: Optional[str] = None,
    rule_type: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    per_page: int = 20,
    scope: Optional[str] = None,
) -> Dict[str, Any]:
    """List alert events with pagination and filters. status=open|all."""
    q = db.query(AlertEvent).join(AlertRule, AlertEvent.rule_id == AlertRule.id)
    if scope:
        q = q.filter(AlertRule.scope == scope)
    if status and status != "all":
        q = q.filter(AlertEvent.status == status)
    if severity:
        q = q.filter(AlertEvent.severity == severity)
    if rule_type:
        q = q.filter(AlertRule.rule_type == rule_type)
    if search:
        term = f"%{search}%"
        q = q.filter(
            or_(
                AlertEvent.title.ilike(term),
                AlertEvent.message.ilike(term),
                AlertRule.name.ilike(term),
            )
        )
    total = q.count()
    q = q.order_by(AlertEvent.ts_detected.desc())
    offset = max(0, page - 1) * per_page
    events = q.offset(offset).limit(max(1, min(per_page, 100))).all()
    base_url = "/dashboard"
    items = []
    for ev in events:
        items.append({
            "id": ev.id,
            "rule_id": ev.rule_id,
            "rule_name": ev.rule.name if ev.rule else None,
            "rule_type": ev.rule.rule_type if ev.rule else None,
            "ts_detected": ev.ts_detected.isoformat() if ev.ts_detected else None,
            "severity": ev.severity,
            "title": ev.title,
            "message": ev.message,
            "status": ev.status,
            "snooze_until": ev.snooze_until.isoformat() if getattr(ev, "snooze_until", None) and ev.snooze_until else None,
            "deep_link": _build_deep_link(ev, base_url),
            "created_at": ev.created_at.isoformat() if ev.created_at else None,
        })
    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
    }


def get_alert_by_id(db: Session, alert_id: int) -> Optional[Dict[str, Any]]:
    """Get single alert with context_json, related_entities, deep_link."""
    ev = db.query(AlertEvent).filter(AlertEvent.id == alert_id).first()
    if not ev:
        return None
    base_url = "/dashboard"
    return {
        "id": ev.id,
        "rule_id": ev.rule_id,
        "rule_name": ev.rule.name if ev.rule else None,
        "rule_type": ev.rule.rule_type if ev.rule else None,
        "ts_detected": ev.ts_detected.isoformat() if ev.ts_detected else None,
        "severity": ev.severity,
        "title": ev.title,
        "message": ev.message,
        "status": ev.status,
        "context_json": ev.context_json,
        "related_entities": ev.related_entities_json,
        "deep_link": _build_deep_link(ev, base_url),
        "assignee_user_id": ev.assignee_user_id,
        "snooze_until": ev.snooze_until.isoformat() if getattr(ev, "snooze_until", None) and ev.snooze_until else None,
        "created_at": ev.created_at.isoformat() if ev.created_at else None,
        "updated_at": ev.updated_at.isoformat() if ev.updated_at else None,
        "updated_by": getattr(ev, "updated_by", None),
    }


def ack_alert(db: Session, alert_id: int, user_id: str) -> Optional[AlertEvent]:
    """Set alert status to ack. Returns event or None."""
    ev = db.query(AlertEvent).filter(AlertEvent.id == alert_id).first()
    if not ev:
        return None
    ev.status = "ack"
    ev.updated_at = datetime.utcnow()
    ev.updated_by = user_id
    db.commit()
    db.refresh(ev)
    return ev


def snooze_alert(db: Session, alert_id: int, duration_minutes: int, user_id: str) -> Optional[AlertEvent]:
    """Snooze alert for duration_minutes. Sets status to snoozed and snooze_until."""
    ev = db.query(AlertEvent).filter(AlertEvent.id == alert_id).first()
    if not ev:
        return None
    until = datetime.utcnow() + timedelta(minutes=min(max(1, duration_minutes), 10080))  # cap 7 days
    ev.status = "snoozed"
    ev.snooze_until = until
    ev.updated_at = datetime.utcnow()
    ev.updated_by = user_id
    db.commit()
    db.refresh(ev)
    return ev


def resolve_alert(db: Session, alert_id: int, user_id: str) -> Optional[AlertEvent]:
    """Manually resolve alert."""
    ev = db.query(AlertEvent).filter(AlertEvent.id == alert_id).first()
    if not ev:
        return None
    ev.status = "resolved"
    ev.snooze_until = None
    ev.updated_at = datetime.utcnow()
    ev.updated_by = user_id
    db.commit()
    db.refresh(ev)
    return ev


# ---------------------------------------------------------------------------
# Alert rules
# ---------------------------------------------------------------------------


def list_alert_rules(
    db: Session,
    scope: Optional[str] = None,
    is_enabled: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """List alert rules (no pagination by default; small set)."""
    q = db.query(AlertRule).order_by(AlertRule.id.asc())
    if scope:
        q = q.filter(AlertRule.scope == scope)
    if is_enabled is not None:
        q = q.filter(AlertRule.is_enabled == is_enabled)
    rules = q.all()
    return [
        {
            "id": r.id,
            "name": r.name,
            "description": r.description,
            "is_enabled": r.is_enabled,
            "scope": r.scope,
            "severity": r.severity,
            "rule_type": r.rule_type,
            "kpi_key": r.kpi_key,
            "dimension_filters_json": r.dimension_filters_json,
            "params_json": r.params_json,
            "schedule": r.schedule,
            "created_by": r.created_by,
            "updated_by": getattr(r, "updated_by", None),
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in rules
    ]


def get_alert_rule_by_id(db: Session, rule_id: int) -> Optional[Dict[str, Any]]:
    """Get single alert rule."""
    r = db.query(AlertRule).filter(AlertRule.id == rule_id).first()
    if not r:
        return None
    return {
        "id": r.id,
        "name": r.name,
        "description": r.description,
        "is_enabled": r.is_enabled,
        "scope": r.scope,
        "severity": r.severity,
        "rule_type": r.rule_type,
        "kpi_key": r.kpi_key,
        "dimension_filters_json": r.dimension_filters_json,
        "params_json": r.params_json,
        "schedule": r.schedule,
        "created_by": r.created_by,
        "updated_by": getattr(r, "updated_by", None),
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def create_alert_rule(
    db: Session,
    name: str,
    scope: str,
    severity: str,
    rule_type: str,
    schedule: str,
    created_by: str,
    description: Optional[str] = None,
    is_enabled: bool = True,
    kpi_key: Optional[str] = None,
    dimension_filters_json: Optional[Dict[str, Any]] = None,
    params_json: Optional[Dict[str, Any]] = None,
) -> AlertRule:
    """Create alert rule with validated params_json."""
    if rule_type not in RULE_TYPES:
        raise ValueError(f"rule_type must be one of {RULE_TYPES}")
    if severity not in SEVERITIES:
        raise ValueError(f"severity must be one of {SEVERITIES}")
    if schedule not in SCHEDULES:
        raise ValueError(f"schedule must be one of {SCHEDULES}")
    params = _validate_params_json(rule_type, params_json)
    r = AlertRule(
        name=name[:255],
        description=description,
        is_enabled=is_enabled,
        scope=scope[:64],
        severity=severity,
        rule_type=rule_type,
        kpi_key=kpi_key[:128] if kpi_key else None,
        dimension_filters_json=dimension_filters_json if isinstance(dimension_filters_json, dict) else None,
        params_json=params,
        schedule=schedule,
        created_by=created_by,
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def update_alert_rule(
    db: Session,
    rule_id: int,
    updated_by: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    is_enabled: Optional[bool] = None,
    severity: Optional[str] = None,
    kpi_key: Optional[str] = None,
    dimension_filters_json: Optional[Dict[str, Any]] = None,
    params_json: Optional[Dict[str, Any]] = None,
    schedule: Optional[str] = None,
) -> Optional[AlertRule]:
    """Update alert rule; validate params_json if provided."""
    r = db.query(AlertRule).filter(AlertRule.id == rule_id).first()
    if not r:
        return None
    if name is not None:
        r.name = name[:255]
    if description is not None:
        r.description = description
    if is_enabled is not None:
        r.is_enabled = is_enabled
    if severity is not None:
        if severity not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}")
        r.severity = severity
    if kpi_key is not None:
        r.kpi_key = kpi_key[:128] if kpi_key else None
    if dimension_filters_json is not None:
        r.dimension_filters_json = dimension_filters_json if isinstance(dimension_filters_json, dict) else None
    if params_json is not None:
        r.params_json = _validate_params_json(r.rule_type, params_json)
    if schedule is not None:
        if schedule not in SCHEDULES:
            raise ValueError(f"schedule must be one of {SCHEDULES}")
        r.schedule = schedule
    r.updated_by = updated_by
    r.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(r)
    return r


def delete_alert_rule(db: Session, rule_id: int, updated_by: str, disable_only: bool = True) -> bool:
    """Delete rule or disable it. disable_only=True sets is_enabled=False."""
    r = db.query(AlertRule).filter(AlertRule.id == rule_id).first()
    if not r:
        return False
    if disable_only:
        r.is_enabled = False
        r.updated_by = updated_by
        r.updated_at = datetime.utcnow()
        db.commit()
    else:
        db.delete(r)
        db.commit()
    return True
