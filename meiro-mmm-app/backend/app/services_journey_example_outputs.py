from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .models_config_dq import (
    ConversionPath,
    JourneyDefinition,
    JourneyDefinitionInstanceFact,
    JourneyExampleFact,
    JourneyInstanceFact,
)
from .utils.meiro_config import campaign_label_matches_target_site_scope


def _campaign_in_scope(value: Optional[str]) -> bool:
    return campaign_label_matches_target_site_scope(value, allow_unknown=True)


def _query_example_outputs(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    conversion_key: Optional[str] = None,
):
    q = (
        db.query(JourneyExampleFact)
        .filter(
            JourneyExampleFact.journey_definition_id == journey_definition_id,
            JourneyExampleFact.date >= date_from,
            JourneyExampleFact.date <= date_to,
        )
        .order_by(JourneyExampleFact.conversion_ts.desc())
    )
    if conversion_key:
        q = q.filter(JourneyExampleFact.conversion_key == conversion_key)
    return q


def _count_example_source_rows(
    db: Session,
    *,
    definition: JourneyDefinition,
    start_dt: datetime,
    end_dt: datetime,
    date_from: date,
    date_to: date,
) -> int:
    instance_count_query = (
        db.query(JourneyInstanceFact.conversion_id)
        .filter(JourneyInstanceFact.conversion_ts >= start_dt, JourneyInstanceFact.conversion_ts < end_dt)
    )
    if definition.conversion_kpi_id:
        instance_count_query = instance_count_query.filter(JourneyInstanceFact.conversion_key == definition.conversion_kpi_id)
    source_count = int(instance_count_query.count() or 0)

    if source_count <= 0:
        raw_count_query = (
            db.query(ConversionPath.conversion_id)
            .filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
        )
        if definition.conversion_kpi_id:
            raw_count_query = raw_count_query.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
        source_count = int(raw_count_query.count() or 0)

    definition_fact_count = int(
        db.query(JourneyDefinitionInstanceFact.conversion_id)
        .filter(
            JourneyDefinitionInstanceFact.journey_definition_id == definition.id,
            JourneyDefinitionInstanceFact.date >= date_from,
            JourneyDefinitionInstanceFact.date <= date_to,
        )
        .count()
        or 0
    )
    return max(source_count, definition_fact_count)


def _definition_fact_rows(
    db: Session,
    *,
    definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    limit: int,
):
    q = (
        db.query(JourneyDefinitionInstanceFact)
        .filter(
            JourneyDefinitionInstanceFact.journey_definition_id == definition.id,
            JourneyDefinitionInstanceFact.date >= date_from,
            JourneyDefinitionInstanceFact.date <= date_to,
        )
        .order_by(JourneyDefinitionInstanceFact.conversion_ts.desc())
    )
    if definition.conversion_kpi_id:
        q = q.filter(JourneyDefinitionInstanceFact.conversion_key == definition.conversion_kpi_id)
    return q.limit(limit).all()


def list_examples_from_outputs(
    db: Session,
    *,
    definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    path_hash: Optional[str] = None,
    contains_step: Optional[str] = None,
    limit: int = 20,
) -> Optional[Dict[str, Any]]:
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    fact_query = _query_example_outputs(
        db,
        journey_definition_id=definition.id,
        date_from=date_from,
        date_to=date_to,
        conversion_key=definition.conversion_kpi_id,
    )
    fact_count = int(fact_query.count() or 0)
    source_count = _count_example_source_rows(
        db,
        definition=definition,
        start_dt=start_dt,
        end_dt=end_dt,
        date_from=date_from,
        date_to=date_to,
    )
    step_token = str(contains_step or "").strip().lower()

    if fact_count >= source_count and fact_count > 0:
        items: List[Dict[str, Any]] = []
        for row in fact_query.limit(max(50, int(limit) * 8)).all():
            if not _campaign_in_scope(row.campaign_id):
                continue
            steps = [str(step) for step in (row.steps_json or [])]
            if path_hash and str(row.path_hash or "") != path_hash:
                continue
            if step_token and not any(step_token in step.lower() for step in steps):
                continue
            if channel_group and (str(row.channel_group or "").lower() != channel_group.lower()):
                continue
            if campaign_id and str(row.campaign_id or "") != campaign_id:
                continue
            if device and str(row.device or "").lower() != device.lower():
                continue
            if country and str(row.country or "").lower() != country.lower():
                continue
            items.append(
                {
                    "conversion_id": row.conversion_id,
                    "profile_id": row.profile_id,
                    "conversion_key": row.conversion_key,
                    "conversion_ts": row.conversion_ts.isoformat() if row.conversion_ts else None,
                    "path_hash": row.path_hash,
                    "steps": steps,
                    "touchpoints_count": int(row.touchpoints_count or 0),
                    "conversion_value": round(float(row.conversion_value or 0.0), 2),
                    "dimensions": {
                        "channel_group": row.channel_group,
                        "campaign_id": row.campaign_id,
                        "device": row.device,
                        "country": row.country,
                    },
                    "touchpoints_preview": row.touchpoints_preview_json or [],
                }
            )
            if len(items) >= limit:
                break
        return {
            "items": items,
            "total": len(items),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "path_hash": path_hash,
            "contains_step": contains_step,
        }

    definition_fact_rows = _definition_fact_rows(
        db,
        definition=definition,
        date_from=date_from,
        date_to=date_to,
        limit=max(50, int(limit) * 8),
    )
    if not definition_fact_rows:
        return None

    items: List[Dict[str, Any]] = []
    for row in definition_fact_rows:
        if not _campaign_in_scope(row.campaign_id):
            continue
        steps = [str(step) for step in (row.steps_json or []) if str(step)]
        resolved_hash = str(row.path_hash or "")
        if path_hash and resolved_hash != path_hash:
            continue
        if step_token and not any(step_token in step.lower() for step in steps):
            continue
        if channel_group and (str(row.channel_group or "").lower() != channel_group.lower()):
            continue
        if campaign_id and str(row.campaign_id or "") != campaign_id:
            continue
        if device and str(row.device or "").lower() != device.lower():
            continue
        if country and str(row.country or "").lower() != country.lower():
            continue

        preview_touchpoints = [
            {"ts": None, "channel": None, "event": step_name, "campaign": row.campaign_id}
            for step_name in steps[:5]
        ]
        items.append(
            {
                "conversion_id": row.conversion_id,
                "profile_id": row.profile_id,
                "conversion_key": row.conversion_key,
                "conversion_ts": row.conversion_ts.isoformat() if row.conversion_ts else None,
                "path_hash": resolved_hash,
                "steps": steps,
                "touchpoints_count": max(0, len(steps) - 1),
                "conversion_value": round(float(row.gross_revenue_total or 0.0), 2),
                "dimensions": {
                    "channel_group": row.channel_group,
                    "campaign_id": row.campaign_id,
                    "device": row.device,
                    "country": row.country,
                },
                "touchpoints_preview": preview_touchpoints,
            }
        )
        if len(items) >= limit:
            break

    return {
        "items": items,
        "total": len(items),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "path_hash": path_hash,
        "contains_step": contains_step,
    }
