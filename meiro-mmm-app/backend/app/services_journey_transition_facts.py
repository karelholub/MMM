from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional

from sqlalchemy.orm import Session

from .models_config_dq import JourneyInstanceFact, JourneyStepFact, JourneyTransitionFact


def build_journey_transition_facts(
    *,
    instance_row: JourneyInstanceFact,
    step_rows: List[JourneyStepFact],
    created_at: datetime,
) -> List[JourneyTransitionFact]:
    rows: List[JourneyTransitionFact] = []
    ordered_steps = [
        row
        for row in sorted(step_rows, key=lambda item: int(item.ordinal or 0))
        if str(row.step_name or "").strip()
    ]
    for idx, (from_row, to_row) in enumerate(zip(ordered_steps, ordered_steps[1:])):
        delta_sec = None
        if isinstance(from_row.step_ts, datetime) and isinstance(to_row.step_ts, datetime):
            candidate = (to_row.step_ts - from_row.step_ts).total_seconds()
            if candidate >= 0:
                delta_sec = float(candidate)
        rows.append(
            JourneyTransitionFact(
                conversion_id=instance_row.conversion_id,
                profile_id=instance_row.profile_id,
                conversion_key=instance_row.conversion_key,
                conversion_ts=instance_row.conversion_ts,
                ordinal=idx,
                from_step=from_row.step_name,
                to_step=to_row.step_name,
                from_step_ts=from_row.step_ts,
                to_step_ts=to_row.step_ts,
                delta_sec=delta_sec,
                channel_group=instance_row.channel_group,
                campaign_id=instance_row.campaign_id,
                device=instance_row.device,
                country=instance_row.country,
                import_batch_id=instance_row.import_batch_id,
                import_source=instance_row.import_source,
                source_snapshot_id=instance_row.source_snapshot_id,
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return rows


def iter_journey_transition_rows(
    db: Session,
    *,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    conversion_key: Optional[str] = None,
):
    q = db.query(
        JourneyTransitionFact.conversion_id,
        JourneyTransitionFact.profile_id,
        JourneyTransitionFact.conversion_key,
        JourneyTransitionFact.conversion_ts,
        JourneyTransitionFact.ordinal,
        JourneyTransitionFact.from_step,
        JourneyTransitionFact.to_step,
        JourneyTransitionFact.from_step_ts,
        JourneyTransitionFact.to_step_ts,
        JourneyTransitionFact.delta_sec,
        JourneyTransitionFact.channel_group,
        JourneyTransitionFact.campaign_id,
        JourneyTransitionFact.device,
        JourneyTransitionFact.country,
    )
    if start_dt is not None:
        q = q.filter(JourneyTransitionFact.conversion_ts >= start_dt)
    if end_dt is not None:
        q = q.filter(JourneyTransitionFact.conversion_ts < end_dt)
    if conversion_key:
        q = q.filter(JourneyTransitionFact.conversion_key == conversion_key)
    for row in q.order_by(JourneyTransitionFact.conversion_id.asc(), JourneyTransitionFact.ordinal.asc()).yield_per(1000):
        yield SimpleNamespace(
            conversion_id=row[0],
            profile_id=row[1],
            conversion_key=row[2],
            conversion_ts=row[3],
            ordinal=row[4],
            from_step=row[5],
            to_step=row[6],
            from_step_ts=row[7],
            to_step_ts=row[8],
            delta_sec=row[9],
            channel_group=row[10],
            campaign_id=row[11],
            device=row[12],
            country=row[13],
        )
