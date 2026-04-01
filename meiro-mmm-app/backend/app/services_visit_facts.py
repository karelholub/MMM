from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .models_config_dq import TouchpointVisitFact
from .services_conversion_silver_facts import _campaign_token, _normalized_token
from .services_journey_steps import to_utc_dt


def build_touchpoint_visit_facts(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    import_batch_id: Optional[str],
    import_source: Optional[str],
    source_snapshot_id: Optional[str],
    created_at: datetime,
) -> List[TouchpointVisitFact]:
    rows: List[TouchpointVisitFact] = []
    for ordinal, tp in enumerate(journey.get("touchpoints") or []):
        if not isinstance(tp, dict):
            continue
        rows.append(
            TouchpointVisitFact(
                conversion_id=conversion_id,
                profile_id=_normalized_token(profile_id),
                conversion_key=_normalized_token(conversion_key),
                ordinal=ordinal,
                touchpoint_ts=to_utc_dt(tp.get("ts") or tp.get("timestamp") or tp.get("event_ts")),
                channel=_normalized_token(tp.get("channel")),
                campaign=_campaign_token(tp),
                import_batch_id=_normalized_token(import_batch_id),
                import_source=_normalized_token(import_source),
                source_snapshot_id=_normalized_token(source_snapshot_id),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return rows


def iter_touchpoint_visit_rows(
    db: Session,
    *,
    conversion_ids: Optional[List[str]] = None,
    touchpoint_from: Optional[datetime] = None,
    touchpoint_to: Optional[datetime] = None,
):
    q = db.query(
        TouchpointVisitFact.conversion_id,
        TouchpointVisitFact.touchpoint_ts,
        TouchpointVisitFact.channel,
        TouchpointVisitFact.campaign,
        TouchpointVisitFact.ordinal,
    )
    if conversion_ids:
        q = q.filter(TouchpointVisitFact.conversion_id.in_(conversion_ids))
    if touchpoint_from is not None:
        q = q.filter(TouchpointVisitFact.touchpoint_ts >= touchpoint_from)
    if touchpoint_to is not None:
        q = q.filter(TouchpointVisitFact.touchpoint_ts <= touchpoint_to)
    for row in q.order_by(TouchpointVisitFact.conversion_id.asc(), TouchpointVisitFact.ordinal.asc()).yield_per(1000):
        yield SimpleNamespace(
            conversion_id=row[0],
            touchpoint_ts=row[1],
            channel=row[2],
            campaign=row[3],
            ordinal=row[4],
        )
