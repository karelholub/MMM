"""Read API helpers for journey examples sampled from conversion_paths."""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath, JourneyDefinition, JourneyExampleFact
from .services_conversions import conversion_path_payload, conversion_path_revenue_value, conversion_path_touchpoints
from .services_journey_aggregates import _build_journey_steps, _path_hash


def _iter_raw_examples(
    db: Session,
    *,
    definition: JourneyDefinition,
    start_dt: datetime,
    end_dt: datetime,
    limit: int,
):
    raw_query = (
        db.query(
            ConversionPath.conversion_id,
            ConversionPath.profile_id,
            ConversionPath.conversion_key,
            ConversionPath.conversion_ts,
            ConversionPath.path_json,
        )
        .filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
        .order_by(ConversionPath.conversion_ts.desc())
    )
    if definition.conversion_kpi_id:
        raw_query = raw_query.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)

    for row in raw_query.limit(limit).all():
        yield SimpleNamespace(
            conversion_id=row[0],
            profile_id=row[1],
            conversion_key=row[2],
            conversion_ts=row[3],
            path_json=row[4] if isinstance(row[4], dict) else {},
        )


def list_examples_for_journey_definition(
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
) -> Dict[str, Any]:
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    fact_query = (
        db.query(JourneyExampleFact)
        .filter(
            JourneyExampleFact.journey_definition_id == definition.id,
            JourneyExampleFact.date >= date_from,
            JourneyExampleFact.date <= date_to,
        )
        .order_by(JourneyExampleFact.conversion_ts.desc())
    )
    if definition.conversion_kpi_id:
        fact_query = fact_query.filter(JourneyExampleFact.conversion_key == definition.conversion_kpi_id)

    fact_count = fact_query.count()
    raw_count_query = (
        db.query(ConversionPath.conversion_id)
        .filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
    )
    if definition.conversion_kpi_id:
        raw_count_query = raw_count_query.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
    raw_count = raw_count_query.count()
    if fact_count >= raw_count and fact_count > 0:
        items: List[Dict[str, Any]] = []
        step_token = str(contains_step or "").strip().lower()
        for row in fact_query.limit(max(50, int(limit) * 8)).all():
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

    items: List[Dict[str, Any]] = []
    step_token = str(contains_step or "").strip().lower()

    for row in _iter_raw_examples(
        db,
        definition=definition,
        start_dt=start_dt,
        end_dt=end_dt,
        limit=max(50, int(limit) * 8),
    ):
        payload = conversion_path_payload(row)
        conversion_ts = row.conversion_ts
        if conversion_ts.tzinfo is None:
            conversion_ts = conversion_ts.replace(tzinfo=timezone.utc)

        steps, _, dims = _build_journey_steps(
            payload,
            conversion_ts=conversion_ts,
            lookback_window_days=definition.lookback_window_days,
        )
        if not steps:
            continue

        resolved_hash = _path_hash(steps)
        if path_hash and resolved_hash != path_hash:
            continue
        if step_token and not any(step_token in step.lower() for step in steps):
            continue
        if channel_group and (dims.get("channel_group") or "").lower() != channel_group.lower():
            continue
        if campaign_id and (dims.get("campaign_id") or "") != campaign_id:
            continue
        if device and (dims.get("device") or "").lower() != device.lower():
            continue
        if country and (dims.get("country") or "").lower() != country.lower():
            continue

        touchpoints = conversion_path_touchpoints(row)
        preview_touchpoints = []
        for tp in touchpoints[:5]:
            campaign = tp.get("campaign")
            campaign_label = None
            if isinstance(campaign, dict):
                campaign_label = campaign.get("name") or campaign.get("id")
            elif campaign is not None:
                campaign_label = str(campaign)
            preview_touchpoints.append(
                {
                    "ts": tp.get("timestamp") or tp.get("ts") or tp.get("event_ts"),
                    "channel": tp.get("channel"),
                    "event": tp.get("event") or tp.get("event_name") or tp.get("name"),
                    "campaign": campaign_label,
                }
            )

        items.append(
            {
                "conversion_id": row.conversion_id,
                "profile_id": row.profile_id,
                "conversion_key": row.conversion_key,
                "conversion_ts": row.conversion_ts.isoformat() if row.conversion_ts else None,
                "path_hash": resolved_hash,
                "steps": steps,
                "touchpoints_count": len(touchpoints),
                "conversion_value": round(float(conversion_path_revenue_value(row)), 2),
                "dimensions": {
                    "channel_group": dims.get("channel_group"),
                    "campaign_id": dims.get("campaign_id"),
                    "device": dims.get("device"),
                    "country": dims.get("country"),
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
