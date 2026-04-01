from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .models_config_dq import JourneyInstanceFact, JourneyStepFact
from .services_conversion_silver_facts import (
    _browser_token,
    _consent_opt_out,
    _has_error_event,
    _interaction_path_type,
    _landing_page_group,
    _normalized_token,
    _silver_outcome_summary,
)
from .services_journey_steps import build_journey_steps_with_timestamps


def _path_hash(steps: List[str]) -> str:
    joined = " > ".join(steps)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def build_journey_instance_and_step_facts(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    conversion_ts: datetime,
    lookback_window_days: int,
    import_batch_id: Optional[str],
    import_source: Optional[str],
    source_snapshot_id: Optional[str],
    created_at: datetime,
) -> Tuple[JourneyInstanceFact, List[JourneyStepFact]]:
    steps, step_timestamps, ttc_sec, dims = build_journey_steps_with_timestamps(
        journey,
        conversion_ts=conversion_ts,
        lookback_window_days=lookback_window_days,
    )
    outcome = _silver_outcome_summary(journey, conversion_id=conversion_id)
    interaction_path_type = _interaction_path_type(journey)
    instance = JourneyInstanceFact(
        conversion_id=conversion_id,
        profile_id=_normalized_token(profile_id),
        conversion_key=_normalized_token(conversion_key),
        conversion_ts=conversion_ts,
        import_batch_id=_normalized_token(import_batch_id),
        import_source=_normalized_token(import_source),
        source_snapshot_id=_normalized_token(source_snapshot_id),
        path_hash=_path_hash(steps) if steps else None,
        path_length=len(steps),
        steps_json=list(steps),
        channel_group=dims.get("channel_group"),
        last_touch_channel=dims.get("last_touch_channel"),
        campaign_id=dims.get("campaign_id"),
        device=dims.get("device"),
        country=dims.get("country"),
        browser=_browser_token(journey),
        consent_opt_out=_consent_opt_out(journey),
        landing_page_group=_landing_page_group(journey),
        has_error_event=_has_error_event(journey),
        interaction_path_type=interaction_path_type,
        time_to_convert_sec=ttc_sec,
        gross_conversions_total=float(outcome.get("gross_conversions", 0.0) or 0.0),
        net_conversions_total=float(outcome.get("net_conversions", 0.0) or 0.0),
        gross_revenue_total=float(outcome.get("gross_value", 0.0) or 0.0),
        net_revenue_total=float(outcome.get("net_value", 0.0) or 0.0),
        created_at=created_at,
        updated_at=created_at,
    )
    touchpoints = journey.get("touchpoints") or []
    if not isinstance(touchpoints, list):
        touchpoints = []
    step_rows: List[JourneyStepFact] = []
    last_touchpoint_for_step: List[Dict[str, Any]] = []
    last_step_name: Optional[str] = None
    for idx, (step_name, step_ts) in enumerate(zip(steps[:-1], step_timestamps[:-1])):
        matching_tp: Optional[Dict[str, Any]] = None
        for tp in touchpoints:
            if not isinstance(tp, dict):
                continue
            candidate_ts = tp.get("timestamp") or tp.get("ts") or tp.get("event_ts")
            try:
                same_ts = datetime.fromisoformat(str(candidate_ts).replace("Z", "+00:00")) == step_ts
            except Exception:
                same_ts = False
            if same_ts:
                matching_tp = tp
                break
        if step_name == last_step_name:
            continue
        last_step_name = step_name
        last_touchpoint_for_step.append(matching_tp or {})
        step_rows.append(
            JourneyStepFact(
                conversion_id=conversion_id,
                profile_id=_normalized_token(profile_id),
                conversion_key=_normalized_token(conversion_key),
                ordinal=idx,
                step_name=step_name,
                step_ts=step_ts,
                channel=_normalized_token((matching_tp or {}).get("channel")),
                campaign=_normalized_token((matching_tp or {}).get("campaign")),
                event_name=_normalized_token((matching_tp or {}).get("event_name") or (matching_tp or {}).get("event") or (matching_tp or {}).get("name")),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    step_rows.append(
        JourneyStepFact(
            conversion_id=conversion_id,
            profile_id=_normalized_token(profile_id),
            conversion_key=_normalized_token(conversion_key),
            ordinal=len(step_rows),
            step_name=steps[-1] if steps else "Purchase / Lead Won (conversion)",
            step_ts=conversion_ts,
            channel=_normalized_token(dims.get("last_touch_channel")),
            campaign=_normalized_token(dims.get("campaign_id")),
            event_name=None,
            created_at=created_at,
            updated_at=created_at,
        )
    )
    return instance, step_rows


def load_journey_instance_sequences(
    db: Session,
    *,
    start_dt: datetime,
    end_dt: datetime,
    conversion_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    q = db.query(
        JourneyInstanceFact.conversion_id,
        JourneyInstanceFact.profile_id,
        JourneyInstanceFact.conversion_key,
        JourneyInstanceFact.conversion_ts,
        JourneyInstanceFact.path_hash,
        JourneyInstanceFact.steps_json,
        JourneyInstanceFact.channel_group,
        JourneyInstanceFact.last_touch_channel,
        JourneyInstanceFact.campaign_id,
        JourneyInstanceFact.device,
        JourneyInstanceFact.country,
        JourneyInstanceFact.browser,
        JourneyInstanceFact.consent_opt_out,
        JourneyInstanceFact.landing_page_group,
        JourneyInstanceFact.has_error_event,
        JourneyInstanceFact.interaction_path_type,
        JourneyInstanceFact.time_to_convert_sec,
        JourneyInstanceFact.gross_conversions_total,
        JourneyInstanceFact.net_conversions_total,
        JourneyInstanceFact.gross_revenue_total,
        JourneyInstanceFact.net_revenue_total,
    ).filter(
        JourneyInstanceFact.conversion_ts >= start_dt,
        JourneyInstanceFact.conversion_ts < end_dt,
    )
    if conversion_key:
        q = q.filter(JourneyInstanceFact.conversion_key == conversion_key)
    rows = q.all()
    if not rows:
        return []
    conversion_ids = [str(row[0] or "") for row in rows if str(row[0] or "")]
    steps_by_conversion: Dict[str, List[Dict[str, Any]]] = {}
    if conversion_ids:
        for conversion_id, ordinal, step_name, step_ts, channel, campaign, event_name in (
            db.query(
                JourneyStepFact.conversion_id,
                JourneyStepFact.ordinal,
                JourneyStepFact.step_name,
                JourneyStepFact.step_ts,
                JourneyStepFact.channel,
                JourneyStepFact.campaign,
                JourneyStepFact.event_name,
            )
            .filter(JourneyStepFact.conversion_id.in_(conversion_ids))
            .order_by(JourneyStepFact.conversion_id.asc(), JourneyStepFact.ordinal.asc())
            .all()
        ):
            steps_by_conversion.setdefault(str(conversion_id or ""), []).append(
                {
                    "ordinal": int(ordinal or 0),
                    "step_name": str(step_name or ""),
                    "step_ts": step_ts,
                    "channel": channel,
                    "campaign": campaign,
                    "event_name": event_name,
                }
            )
    out: List[Dict[str, Any]] = []
    for row in rows:
        conversion_id = str(row[0] or "")
        out.append(
            {
                "conversion_id": conversion_id,
                "profile_id": row[1],
                "conversion_key": row[2],
                "conversion_ts": row[3],
                "path_hash": row[4],
                "steps_json": list(row[5] or []),
                "channel_group": row[6],
                "last_touch_channel": row[7],
                "campaign_id": row[8],
                "device": row[9],
                "country": row[10],
                "browser": row[11],
                "consent_opt_out": row[12],
                "landing_page_group": row[13],
                "has_error_event": row[14],
                "interaction_path_type": row[15],
                "time_to_convert_sec": row[16],
                "gross_conversions_total": row[17],
                "net_conversions_total": row[18],
                "gross_revenue_total": row[19],
                "net_revenue_total": row[20],
                "steps": list(steps_by_conversion.get(conversion_id) or []),
            }
        )
    return out
