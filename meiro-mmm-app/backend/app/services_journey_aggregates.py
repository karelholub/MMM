"""Daily journey aggregate ETL for journey_paths_daily and journey_transitions_daily."""

from __future__ import annotations

import hashlib
import logging
import math
import time
from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from .models_config_dq import (
    ChannelPerformanceDaily,
    ConversionPath,
    JourneyDefinition,
    JourneyExampleFact,
    JourneyInstanceFact,
    JourneyPathDaily,
    JourneyTransitionDaily,
    SilverConversionFact,
    SilverTouchpointFact,
)
from .services_conversions import (
    classify_journey_interaction,
    conversion_path_is_converted,
    conversion_path_outcome_summary,
    conversion_path_payload,
    conversion_path_revenue_value,
    conversion_path_touchpoints,
)
from .services_journey_instance_facts import load_journey_instance_sequences
from .services_silver_journeys import load_silver_journeys

logger = logging.getLogger(__name__)

STEP_PAID_LANDING = "Paid Landing"
STEP_ORGANIC_LANDING = "Organic Landing"
STEP_CONTENT_VIEW = "Product View / Content View"
STEP_ADD_TO_CART = "Add to Cart / Form Start"
STEP_CHECKOUT = "Checkout / Form Submit"
STEP_CONVERSION = "Purchase / Lead Won (conversion)"

MAX_STEPS = 20
DEFAULT_REPROCESS_DAYS = 3

_PAID_CHANNEL_TOKENS = {
    "google_ads",
    "meta_ads",
    "linkedin_ads",
    "bing_ads",
    "tiktok_ads",
    "snapchat_ads",
    "paid",
    "cpc",
    "ppc",
    "paid_social",
    "display",
    "affiliate",
}
_ADD_TO_CART_TOKENS = {"add_to_cart", "cart_add", "form_start", "lead_start", "start_form", "begin_form"}
_CHECKOUT_TOKENS = {
    "checkout",
    "form_submit",
    "submit_form",
    "payment",
    "place_order",
    "begin_checkout",
    "checkout_start",
}
_CONTENT_VIEW_TOKENS = {
    "product_view",
    "view_item",
    "content_view",
    "page_view",
    "view_content",
    "article_view",
}


def _to_utc_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _conversion_path_is_converted(row: ConversionPath) -> bool:
    return conversion_path_is_converted(row)


def _touchpoint_ts(tp: Dict[str, Any]) -> Optional[datetime]:
    for key in ("ts", "timestamp", "event_ts", "occurred_at", "time"):
        out = _to_utc_dt(tp.get(key))
        if out is not None:
            return out
    return None


def _to_token_set(tp: Dict[str, Any]) -> Set[str]:
    toks: Set[str] = set()
    candidates = [
        tp.get("event"),
        tp.get("event_name"),
        tp.get("name"),
        tp.get("type"),
        tp.get("action"),
        tp.get("channel"),
        tp.get("medium"),
        (tp.get("utm") or {}).get("medium") if isinstance(tp.get("utm"), dict) else None,
        (tp.get("source") or {}).get("platform") if isinstance(tp.get("source"), dict) else None,
    ]
    for item in candidates:
        if item is None:
            continue
        raw = str(item).strip().lower().replace("-", "_").replace(" ", "_")
        if not raw:
            continue
        toks.add(raw)
        toks.update(part for part in raw.split("_") if part)
    return toks


def map_touchpoint_step(tp: Dict[str, Any], index: int) -> str:
    toks = _to_token_set(tp)
    if index == 0:
        if toks & _PAID_CHANNEL_TOKENS:
            return STEP_PAID_LANDING
        return STEP_ORGANIC_LANDING
    if toks & _ADD_TO_CART_TOKENS:
        return STEP_ADD_TO_CART
    if toks & _CHECKOUT_TOKENS:
        return STEP_CHECKOUT
    if toks & _CONTENT_VIEW_TOKENS:
        return STEP_CONTENT_VIEW
    return STEP_CONTENT_VIEW


def dedup_steps(steps: Sequence[str], max_steps: int = MAX_STEPS) -> List[str]:
    out: List[str] = []
    prev: Optional[str] = None
    for step in steps:
        if step == prev:
            continue
        out.append(step)
        prev = step
        if len(out) >= max_steps:
            break
    return out


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    q = max(0.0, min(1.0, q))
    idx = (len(ordered) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _build_journey_steps_with_timestamps(
    payload: Dict[str, Any],
    *,
    conversion_ts: datetime,
    lookback_window_days: int,
) -> Tuple[List[str], List[datetime], Optional[float], Dict[str, Optional[str]]]:
    tps = payload.get("touchpoints") or []
    if not isinstance(tps, list):
        tps = []
    lower_bound = conversion_ts - timedelta(days=max(1, int(lookback_window_days)))
    selected: List[Tuple[datetime, Dict[str, Any]]] = []
    for tp in tps:
        if not isinstance(tp, dict):
            continue
        ts = _touchpoint_ts(tp)
        if ts is None:
            continue
        if lower_bound <= ts <= conversion_ts:
            selected.append((ts, tp))
    selected.sort(key=lambda row: row[0])
    raw_steps: List[Tuple[str, datetime]] = [(map_touchpoint_step(tp, idx), ts) for idx, (ts, tp) in enumerate(selected)]
    raw_steps.append((STEP_CONVERSION, conversion_ts))
    compact_steps: List[str] = []
    compact_timestamps: List[datetime] = []
    prev_step: Optional[str] = None
    for step, ts in raw_steps:
        if step == prev_step:
            continue
        compact_steps.append(step)
        compact_timestamps.append(ts)
        prev_step = step
        if len(compact_steps) >= MAX_STEPS:
            break
    first_step_ts = selected[0][0] if selected else None
    ttc = (conversion_ts - first_step_ts).total_seconds() if first_step_ts else None
    last_tp = selected[-1][1] if selected else {}
    campaign_val = last_tp.get("campaign")
    if isinstance(campaign_val, dict):
        campaign_id = str(campaign_val.get("id") or campaign_val.get("name") or "")
    elif campaign_val:
        campaign_id = str(campaign_val)
    else:
        campaign_id = None
    dims = {
        "channel_group": None,
        "last_touch_channel": str(last_tp.get("channel")) if last_tp.get("channel") else None,
        "campaign_id": campaign_id,
        "device": str(payload.get("device")) if payload.get("device") else None,
        "country": str(payload.get("country")) if payload.get("country") else None,
    }
    if compact_steps:
        dims["channel_group"] = "paid" if compact_steps[0] == STEP_PAID_LANDING else ("organic" if compact_steps[0] == STEP_ORGANIC_LANDING else None)
    return compact_steps, compact_timestamps, ttc, dims


def _build_journey_steps(
    payload: Dict[str, Any],
    *,
    conversion_ts: datetime,
    lookback_window_days: int,
) -> Tuple[List[str], Optional[float], Dict[str, Optional[str]]]:
    steps, _timestamps, ttc, dims = _build_journey_steps_with_timestamps(
        payload,
        conversion_ts=conversion_ts,
        lookback_window_days=lookback_window_days,
    )
    return steps, ttc, dims


def _path_hash(steps: Sequence[str]) -> str:
    joined = " > ".join(steps)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _get_source_days(
    db: Session,
    *,
    definition: JourneyDefinition,
    end_day: date,
) -> Set[date]:
    instance_days = _get_source_days_from_instance_facts(db, definition=definition, end_day=end_day)
    if instance_days:
        return instance_days
    silver_days = _get_source_days_from_silver(db, definition=definition, end_day=end_day)
    if silver_days:
        return silver_days
    q = db.query(func.date(ConversionPath.conversion_ts)).filter(
        ConversionPath.conversion_ts < datetime.combine(end_day + timedelta(days=1), dt_time.min)
    )
    if definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
    out: Set[date] = set()
    for (raw_day,) in q.distinct().all():
        if raw_day is None:
            continue
        if isinstance(raw_day, date):
            out.add(raw_day)
        else:
            try:
                out.add(datetime.fromisoformat(str(raw_day)[:10]).date())
            except Exception:
                continue
    return out


def _get_source_days_from_instance_facts(
    db: Session,
    *,
    definition: JourneyDefinition,
    end_day: date,
) -> Set[date]:
    upper_bound = datetime.combine(end_day + timedelta(days=1), dt_time.min).replace(tzinfo=timezone.utc)
    q = db.query(JourneyInstanceFact.conversion_ts).filter(JourneyInstanceFact.conversion_ts < upper_bound)
    if definition.conversion_kpi_id:
        q = q.filter(JourneyInstanceFact.conversion_key == definition.conversion_kpi_id)
    out: Set[date] = set()
    for (raw_ts,) in q.all():
        ts = _to_utc_dt(raw_ts)
        if ts is not None and ts.date() <= end_day:
            out.add(ts.date())
    return out


def _get_source_days_from_silver(
    db: Session,
    *,
    definition: JourneyDefinition,
    end_day: date,
) -> Set[date]:
    upper_bound = datetime.combine(end_day + timedelta(days=1), dt_time.min).replace(tzinfo=timezone.utc)
    q = db.query(SilverConversionFact.conversion_ts).filter(SilverConversionFact.conversion_ts < upper_bound)
    if definition.conversion_kpi_id:
        q = q.filter(SilverConversionFact.conversion_key == definition.conversion_kpi_id)
    out: Set[date] = set()
    for (raw_ts,) in q.all():
        ts = _to_utc_dt(raw_ts)
        if ts is not None and ts.date() <= end_day:
            out.add(ts.date())
    return out


def _get_channel_source_days(
    db: Session,
    *,
    end_day: date,
) -> Set[date]:
    silver_days = _get_channel_source_days_from_silver(db, end_day=end_day)
    if silver_days:
        return silver_days
    upper_bound = datetime.combine(end_day + timedelta(days=1), dt_time.min)
    rows = (
        db.query(ConversionPath)
        .filter(
            or_(
                ConversionPath.first_touch_ts < upper_bound,
                ConversionPath.conversion_ts < upper_bound,
            )
        )
        .all()
    )
    out: Set[date] = set()
    for row in rows:
        conversion_ts = _to_utc_dt(getattr(row, "conversion_ts", None))
        if conversion_ts is not None and conversion_ts.date() <= end_day:
            out.add(conversion_ts.date())
        payload = conversion_path_payload(row)
        touchpoints = payload.get("touchpoints") or []
        if not isinstance(touchpoints, list):
            continue
        for tp in touchpoints:
            if not isinstance(tp, dict):
                continue
            ts = _touchpoint_ts(tp)
            if ts is not None and ts.date() <= end_day:
                out.add(ts.date())
    return out


def _get_channel_source_days_from_silver(
    db: Session,
    *,
    end_day: date,
) -> Set[date]:
    upper_bound = datetime.combine(end_day + timedelta(days=1), dt_time.min).replace(tzinfo=timezone.utc)
    out: Set[date] = set()
    for (raw_ts,) in db.query(SilverConversionFact.conversion_ts).filter(SilverConversionFact.conversion_ts < upper_bound).all():
        ts = _to_utc_dt(raw_ts)
        if ts is not None and ts.date() <= end_day:
            out.add(ts.date())
    for (raw_ts,) in db.query(SilverTouchpointFact.touchpoint_ts).filter(
        SilverTouchpointFact.touchpoint_ts.isnot(None),
        SilverTouchpointFact.touchpoint_ts < upper_bound,
    ).all():
        ts = _to_utc_dt(raw_ts)
        if ts is not None and ts.date() <= end_day:
            out.add(ts.date())
    return out


def _aggregate_for_day_definition(
    db: Session,
    *,
    day: date,
    definition: JourneyDefinition,
) -> Dict[str, int]:
    silver_stats = _aggregate_for_day_definition_from_silver(db, day=day, definition=definition)
    if silver_stats is not None:
        return silver_stats
    day_start = datetime.combine(day, dt_time.min)
    day_end = datetime.combine(day + timedelta(days=1), dt_time.min)
    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= day_start,
        ConversionPath.conversion_ts < day_end,
    )
    if definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
    rows = q.all()

    path_aggs: Dict[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    trans_aggs: Dict[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    example_rows: List[JourneyExampleFact] = []

    for row in rows:
        if not _conversion_path_is_converted(row):
            continue
        conversion_ts = _to_utc_dt(row.conversion_ts)
        if conversion_ts is None:
            continue
        payload = conversion_path_payload(row)
        steps, step_timestamps, ttc_sec, dims = _build_journey_steps_with_timestamps(
            payload,
            conversion_ts=conversion_ts,
            lookback_window_days=definition.lookback_window_days,
        )
        if not steps:
            continue
        phash = _path_hash(steps)
        outcome = conversion_path_outcome_summary(row)
        path_type = classify_journey_interaction(payload)
        path_key = (
            phash,
            dims.get("channel_group"),
            dims.get("campaign_id"),
            dims.get("device"),
            dims.get("country"),
        )
        bucket = path_aggs.setdefault(
            path_key,
            {
                "path_hash": phash,
                "path_steps": steps,
                "path_length": len(steps),
                "count_journeys": 0,
                "count_conversions": 0,
                "gross_conversions_total": 0.0,
                "net_conversions_total": 0.0,
                "gross_revenue_total": 0.0,
                "net_revenue_total": 0.0,
                "view_through_conversions_total": 0.0,
                "click_through_conversions_total": 0.0,
                "mixed_path_conversions_total": 0.0,
                "ttc_values": [],
                "channel_group": dims.get("channel_group"),
                "last_touch_channel": dims.get("last_touch_channel"),
                "campaign_id": dims.get("campaign_id"),
                "device": dims.get("device"),
                "country": dims.get("country"),
            },
        )
        bucket["count_journeys"] += 1
        bucket["count_conversions"] += 1
        bucket["gross_conversions_total"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
        bucket["net_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        bucket["gross_revenue_total"] += float(outcome.get("gross_value", 0.0) or 0.0)
        bucket["net_revenue_total"] += float(outcome.get("net_value", 0.0) or 0.0)
        if path_type == "view_through":
            bucket["view_through_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        elif path_type == "click_through":
            bucket["click_through_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        elif path_type == "mixed_path":
            bucket["mixed_path_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        if ttc_sec is not None and ttc_sec >= 0:
            bucket["ttc_values"].append(float(ttc_sec))

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
        example_rows.append(
            JourneyExampleFact(
                date=day,
                journey_definition_id=definition.id,
                conversion_id=str(row.conversion_id or ""),
                profile_id=str(row.profile_id or ""),
                conversion_key=row.conversion_key,
                conversion_ts=row.conversion_ts,
                path_hash=phash,
                steps_json=steps,
                touchpoints_count=len(touchpoints),
                conversion_value=round(float(conversion_path_revenue_value(row)), 2),
                channel_group=dims.get("channel_group"),
                campaign_id=dims.get("campaign_id"),
                device=dims.get("device"),
                country=dims.get("country"),
                touchpoints_preview_json=preview_touchpoints,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )

        profile_id = str(row.profile_id or "")
        for idx, (from_step, to_step) in enumerate(zip(steps, steps[1:])):
            transition_key = (
                from_step,
                to_step,
                dims.get("channel_group"),
                dims.get("campaign_id"),
                dims.get("device"),
                dims.get("country"),
            )
            t_bucket = trans_aggs.setdefault(
                transition_key,
                {
                    "count_transitions": 0,
                    "profiles": set(),
                    "time_values": [],
                    "channel_group": dims.get("channel_group"),
                    "campaign_id": dims.get("campaign_id"),
                    "device": dims.get("device"),
                    "country": dims.get("country"),
                },
            )
            t_bucket["count_transitions"] += 1
            if profile_id:
                t_bucket["profiles"].add(profile_id)
            if idx + 1 < len(step_timestamps):
                delta_sec = (step_timestamps[idx + 1] - step_timestamps[idx]).total_seconds()
                if delta_sec >= 0:
                    t_bucket["time_values"].append(float(delta_sec))

    db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == definition.id,
        JourneyPathDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyTransitionDaily).filter(
        JourneyTransitionDaily.journey_definition_id == definition.id,
        JourneyTransitionDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyExampleFact).filter(
        JourneyExampleFact.journey_definition_id == definition.id,
        JourneyExampleFact.date == day,
    ).delete(synchronize_session=False)

    now = datetime.now(timezone.utc)
    for _path_key, payload in path_aggs.items():
        ttc_values = payload["ttc_values"]
        db.add(
            JourneyPathDaily(
                date=day,
                journey_definition_id=definition.id,
                path_hash=payload["path_hash"],
                path_steps=payload["path_steps"],
                path_length=payload["path_length"],
                count_journeys=payload["count_journeys"],
                count_conversions=payload["count_conversions"],
                gross_conversions_total=payload["gross_conversions_total"],
                net_conversions_total=payload["net_conversions_total"],
                gross_revenue_total=payload["gross_revenue_total"],
                net_revenue_total=payload["net_revenue_total"],
                view_through_conversions_total=payload["view_through_conversions_total"],
                click_through_conversions_total=payload["click_through_conversions_total"],
                mixed_path_conversions_total=payload["mixed_path_conversions_total"],
                avg_time_to_convert_sec=(sum(ttc_values) / len(ttc_values)) if ttc_values else None,
                p50_time_to_convert_sec=_percentile(ttc_values, 0.5),
                p90_time_to_convert_sec=_percentile(ttc_values, 0.9),
                channel_group=payload["channel_group"],
                last_touch_channel=payload["last_touch_channel"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for (from_step, to_step, _channel_group, _campaign_id, _device, _country), payload in trans_aggs.items():
        db.add(
            JourneyTransitionDaily(
                date=day,
                journey_definition_id=definition.id,
                from_step=from_step,
                to_step=to_step,
                count_transitions=payload["count_transitions"],
                count_profiles=len(payload["profiles"]),
                avg_time_between_sec=(sum(payload["time_values"]) / len(payload["time_values"])) if payload["time_values"] else None,
                p50_time_between_sec=_percentile(payload["time_values"], 0.5),
                p90_time_between_sec=_percentile(payload["time_values"], 0.9),
                channel_group=payload["channel_group"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for example_row in example_rows:
        db.add(example_row)

    db.commit()
    return {
        "source_rows": len(rows),
        "path_rows_written": len(path_aggs),
        "transition_rows_written": len(trans_aggs),
        "example_rows_written": len(example_rows),
    }


def _aggregate_for_day_definition_from_silver(
    db: Session,
    *,
    day: date,
    definition: JourneyDefinition,
) -> Optional[Dict[str, int]]:
    day_start = datetime.combine(day, dt_time.min).replace(tzinfo=timezone.utc)
    day_end = datetime.combine(day + timedelta(days=1), dt_time.min).replace(tzinfo=timezone.utc)
    journeys = load_journey_instance_sequences(
        db,
        start_dt=day_start,
        end_dt=day_end,
        conversion_key=definition.conversion_kpi_id,
    )
    if journeys:
        return _aggregate_for_day_definition_from_instance_rows(db, day=day, definition=definition, journeys=journeys)
    journeys = load_silver_journeys(
        db,
        start_dt=day_start,
        end_dt=day_end,
        conversion_key=definition.conversion_kpi_id,
    )
    if not journeys:
        return None

    path_aggs: Dict[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    trans_aggs: Dict[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    example_rows: List[JourneyExampleFact] = []

    for journey in journeys:
        conversion_id = journey["conversion_id"]
        profile_id = journey["profile_id"]
        conversion_key = journey["conversion_key"]
        conversion_ts = _to_utc_dt(journey["conversion_ts"])
        if conversion_ts is None:
            continue
        payload = {
            "touchpoints": list((journey["payload"] or {}).get("touchpoints") or []),
            "device": journey["device"],
            "country": journey["country"],
            "interaction_path_type": journey["interaction_path_type"],
        }
        touchpoints = payload["touchpoints"]
        steps, step_timestamps, ttc_sec, dims = _build_journey_steps_with_timestamps(
            payload,
            conversion_ts=conversion_ts,
            lookback_window_days=definition.lookback_window_days,
        )
        if not steps:
            continue
        phash = _path_hash(steps)
        outcome = {
            "gross_conversions": float(journey["gross_conversions_total"] or 0.0),
            "net_conversions": float(journey["net_conversions_total"] or 0.0),
            "gross_value": float(journey["gross_revenue_total"] or 0.0),
            "net_value": float(journey["net_revenue_total"] or 0.0),
        }
        path_type = str(journey["interaction_path_type"] or "").strip().lower()
        path_key = (
            phash,
            dims.get("channel_group"),
            dims.get("campaign_id"),
            dims.get("device"),
            dims.get("country"),
        )
        bucket = path_aggs.setdefault(
            path_key,
            {
                "path_hash": phash,
                "path_steps": steps,
                "path_length": len(steps),
                "count_journeys": 0,
                "count_conversions": 0,
                "gross_conversions_total": 0.0,
                "net_conversions_total": 0.0,
                "gross_revenue_total": 0.0,
                "net_revenue_total": 0.0,
                "view_through_conversions_total": 0.0,
                "click_through_conversions_total": 0.0,
                "mixed_path_conversions_total": 0.0,
                "ttc_values": [],
                "channel_group": dims.get("channel_group"),
                "last_touch_channel": dims.get("last_touch_channel"),
                "campaign_id": dims.get("campaign_id"),
                "device": dims.get("device"),
                "country": dims.get("country"),
            },
        )
        bucket["count_journeys"] += 1
        bucket["count_conversions"] += 1
        bucket["gross_conversions_total"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
        bucket["net_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        bucket["gross_revenue_total"] += float(outcome.get("gross_value", 0.0) or 0.0)
        bucket["net_revenue_total"] += float(outcome.get("net_value", 0.0) or 0.0)
        if path_type == "view_through":
            bucket["view_through_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        elif path_type == "click_through":
            bucket["click_through_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        elif path_type == "mixed_path":
            bucket["mixed_path_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        if ttc_sec is not None and ttc_sec >= 0:
            bucket["ttc_values"].append(float(ttc_sec))

        preview_touchpoints = []
        for tp in touchpoints[:5]:
            preview_ts = tp.get("timestamp") or tp.get("ts") or tp.get("event_ts")
            if isinstance(preview_ts, datetime):
                preview_ts = preview_ts.isoformat()
            preview_touchpoints.append(
                {
                    "ts": preview_ts,
                    "channel": tp.get("channel"),
                    "event": tp.get("event") or tp.get("event_name") or tp.get("name"),
                    "campaign": tp.get("campaign"),
                }
            )
        example_rows.append(
            JourneyExampleFact(
                date=day,
                journey_definition_id=definition.id,
                conversion_id=conversion_id,
                profile_id=profile_id,
                conversion_key=conversion_key,
                conversion_ts=conversion_ts,
                path_hash=phash,
                steps_json=steps,
                touchpoints_count=len(touchpoints),
                conversion_value=round(float(journey["gross_revenue_total"] or 0.0), 2),
                channel_group=dims.get("channel_group"),
                campaign_id=dims.get("campaign_id"),
                device=dims.get("device"),
                country=dims.get("country"),
                touchpoints_preview_json=preview_touchpoints,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )

        for idx, (from_step, to_step) in enumerate(zip(steps, steps[1:])):
            transition_key = (
                from_step,
                to_step,
                dims.get("channel_group"),
                dims.get("campaign_id"),
                dims.get("device"),
                dims.get("country"),
            )
            t_bucket = trans_aggs.setdefault(
                transition_key,
                {
                    "count_transitions": 0,
                    "profiles": set(),
                    "time_values": [],
                    "channel_group": dims.get("channel_group"),
                    "campaign_id": dims.get("campaign_id"),
                    "device": dims.get("device"),
                    "country": dims.get("country"),
                },
            )
            t_bucket["count_transitions"] += 1
            if profile_id:
                t_bucket["profiles"].add(profile_id)
            if idx + 1 < len(step_timestamps):
                delta_sec = (step_timestamps[idx + 1] - step_timestamps[idx]).total_seconds()
                if delta_sec >= 0:
                    t_bucket["time_values"].append(float(delta_sec))

    db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == definition.id,
        JourneyPathDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyTransitionDaily).filter(
        JourneyTransitionDaily.journey_definition_id == definition.id,
        JourneyTransitionDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyExampleFact).filter(
        JourneyExampleFact.journey_definition_id == definition.id,
        JourneyExampleFact.date == day,
    ).delete(synchronize_session=False)

    now = datetime.now(timezone.utc)
    for payload in path_aggs.values():
        ttc_values = payload["ttc_values"]
        db.add(
            JourneyPathDaily(
                date=day,
                journey_definition_id=definition.id,
                path_hash=payload["path_hash"],
                path_steps=payload["path_steps"],
                path_length=payload["path_length"],
                count_journeys=payload["count_journeys"],
                count_conversions=payload["count_conversions"],
                gross_conversions_total=payload["gross_conversions_total"],
                net_conversions_total=payload["net_conversions_total"],
                gross_revenue_total=payload["gross_revenue_total"],
                net_revenue_total=payload["net_revenue_total"],
                view_through_conversions_total=payload["view_through_conversions_total"],
                click_through_conversions_total=payload["click_through_conversions_total"],
                mixed_path_conversions_total=payload["mixed_path_conversions_total"],
                avg_time_to_convert_sec=(sum(ttc_values) / len(ttc_values)) if ttc_values else None,
                p50_time_to_convert_sec=_percentile(ttc_values, 0.5),
                p90_time_to_convert_sec=_percentile(ttc_values, 0.9),
                channel_group=payload["channel_group"],
                last_touch_channel=payload["last_touch_channel"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for (from_step, to_step, _channel_group, _campaign_id, _device, _country), payload in trans_aggs.items():
        db.add(
            JourneyTransitionDaily(
                date=day,
                journey_definition_id=definition.id,
                from_step=from_step,
                to_step=to_step,
                count_transitions=payload["count_transitions"],
                count_profiles=len(payload["profiles"]),
                avg_time_between_sec=(sum(payload["time_values"]) / len(payload["time_values"])) if payload["time_values"] else None,
                p50_time_between_sec=_percentile(payload["time_values"], 0.5),
                p90_time_between_sec=_percentile(payload["time_values"], 0.9),
                channel_group=payload["channel_group"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for example_row in example_rows:
        db.add(example_row)

    db.commit()
    return {
        "source_rows": len(journeys),
        "path_rows_written": len(path_aggs),
        "transition_rows_written": len(trans_aggs),
        "example_rows_written": len(example_rows),
    }


def _aggregate_for_day_definition_from_instance_rows(
    db: Session,
    *,
    day: date,
    definition: JourneyDefinition,
    journeys: List[Dict[str, Any]],
) -> Dict[str, int]:
    path_aggs: Dict[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    trans_aggs: Dict[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    example_rows: List[JourneyExampleFact] = []

    for journey in journeys:
        conversion_id = str(journey["conversion_id"] or "")
        profile_id = str(journey["profile_id"] or "")
        conversion_key = str(journey["conversion_key"] or "").strip() or None
        conversion_ts = _to_utc_dt(journey["conversion_ts"])
        if conversion_ts is None:
            continue
        steps = [str(step.get("step_name") or "") for step in (journey.get("steps") or []) if str(step.get("step_name") or "")]
        step_timestamps = [step.get("step_ts") for step in (journey.get("steps") or []) if isinstance(step.get("step_ts"), datetime)]
        if not steps or len(steps) != len(step_timestamps):
            continue
        phash = str(journey.get("path_hash") or _path_hash(steps))
        outcome = {
            "gross_conversions": float(journey.get("gross_conversions_total") or 0.0),
            "net_conversions": float(journey.get("net_conversions_total") or 0.0),
            "gross_value": float(journey.get("gross_revenue_total") or 0.0),
            "net_value": float(journey.get("net_revenue_total") or 0.0),
        }
        path_type = str(journey.get("interaction_path_type") or "").strip().lower()
        dims = {
            "channel_group": journey.get("channel_group"),
            "last_touch_channel": journey.get("last_touch_channel"),
            "campaign_id": journey.get("campaign_id"),
            "device": journey.get("device"),
            "country": journey.get("country"),
        }
        path_key = (
            phash,
            dims.get("channel_group"),
            dims.get("campaign_id"),
            dims.get("device"),
            dims.get("country"),
        )
        bucket = path_aggs.setdefault(
            path_key,
            {
                "path_hash": phash,
                "path_steps": steps,
                "path_length": len(steps),
                "count_journeys": 0,
                "count_conversions": 0,
                "gross_conversions_total": 0.0,
                "net_conversions_total": 0.0,
                "gross_revenue_total": 0.0,
                "net_revenue_total": 0.0,
                "view_through_conversions_total": 0.0,
                "click_through_conversions_total": 0.0,
                "mixed_path_conversions_total": 0.0,
                "ttc_values": [],
                "channel_group": dims.get("channel_group"),
                "last_touch_channel": dims.get("last_touch_channel"),
                "campaign_id": dims.get("campaign_id"),
                "device": dims.get("device"),
                "country": dims.get("country"),
            },
        )
        bucket["count_journeys"] += 1
        bucket["count_conversions"] += 1
        bucket["gross_conversions_total"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
        bucket["net_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        bucket["gross_revenue_total"] += float(outcome.get("gross_value", 0.0) or 0.0)
        bucket["net_revenue_total"] += float(outcome.get("net_value", 0.0) or 0.0)
        if path_type == "view_through":
            bucket["view_through_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        elif path_type == "click_through":
            bucket["click_through_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        elif path_type == "mixed_path":
            bucket["mixed_path_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
        ttc_sec = journey.get("time_to_convert_sec")
        if isinstance(ttc_sec, (int, float)) and ttc_sec >= 0:
            bucket["ttc_values"].append(float(ttc_sec))

        preview_touchpoints = []
        for step in (journey.get("steps") or [])[:5]:
            preview_ts = step.get("step_ts")
            if isinstance(preview_ts, datetime):
                preview_ts = preview_ts.isoformat()
            preview_touchpoints.append(
                {
                    "ts": preview_ts,
                    "channel": step.get("channel"),
                    "event": step.get("event_name") or step.get("step_name"),
                    "campaign": step.get("campaign"),
                }
            )
        example_rows.append(
            JourneyExampleFact(
                date=day,
                journey_definition_id=definition.id,
                conversion_id=conversion_id,
                profile_id=profile_id,
                conversion_key=conversion_key,
                conversion_ts=conversion_ts,
                path_hash=phash,
                steps_json=steps,
                touchpoints_count=max(0, len(steps) - 1),
                conversion_value=round(float(journey.get("gross_revenue_total") or 0.0), 2),
                channel_group=dims.get("channel_group"),
                campaign_id=dims.get("campaign_id"),
                device=dims.get("device"),
                country=dims.get("country"),
                touchpoints_preview_json=preview_touchpoints,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )

        for idx, (from_step, to_step) in enumerate(zip(steps, steps[1:])):
            transition_key = (
                from_step,
                to_step,
                dims.get("channel_group"),
                dims.get("campaign_id"),
                dims.get("device"),
                dims.get("country"),
            )
            t_bucket = trans_aggs.setdefault(
                transition_key,
                {
                    "count_transitions": 0,
                    "profiles": set(),
                    "time_values": [],
                    "channel_group": dims.get("channel_group"),
                    "campaign_id": dims.get("campaign_id"),
                    "device": dims.get("device"),
                    "country": dims.get("country"),
                },
            )
            t_bucket["count_transitions"] += 1
            if profile_id:
                t_bucket["profiles"].add(profile_id)
            if idx + 1 < len(step_timestamps):
                delta_sec = (step_timestamps[idx + 1] - step_timestamps[idx]).total_seconds()
                if delta_sec >= 0:
                    t_bucket["time_values"].append(float(delta_sec))

    db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == definition.id,
        JourneyPathDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyTransitionDaily).filter(
        JourneyTransitionDaily.journey_definition_id == definition.id,
        JourneyTransitionDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyExampleFact).filter(
        JourneyExampleFact.journey_definition_id == definition.id,
        JourneyExampleFact.date == day,
    ).delete(synchronize_session=False)

    now = datetime.now(timezone.utc)
    for payload in path_aggs.values():
        ttc_values = payload["ttc_values"]
        db.add(
            JourneyPathDaily(
                date=day,
                journey_definition_id=definition.id,
                path_hash=payload["path_hash"],
                path_steps=payload["path_steps"],
                path_length=payload["path_length"],
                count_journeys=payload["count_journeys"],
                count_conversions=payload["count_conversions"],
                gross_conversions_total=payload["gross_conversions_total"],
                net_conversions_total=payload["net_conversions_total"],
                gross_revenue_total=payload["gross_revenue_total"],
                net_revenue_total=payload["net_revenue_total"],
                view_through_conversions_total=payload["view_through_conversions_total"],
                click_through_conversions_total=payload["click_through_conversions_total"],
                mixed_path_conversions_total=payload["mixed_path_conversions_total"],
                avg_time_to_convert_sec=(sum(ttc_values) / len(ttc_values)) if ttc_values else None,
                p50_time_to_convert_sec=_percentile(ttc_values, 0.5),
                p90_time_to_convert_sec=_percentile(ttc_values, 0.9),
                channel_group=payload["channel_group"],
                last_touch_channel=payload["last_touch_channel"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for (from_step, to_step, _channel_group, _campaign_id, _device, _country), payload in trans_aggs.items():
        db.add(
            JourneyTransitionDaily(
                date=day,
                journey_definition_id=definition.id,
                from_step=from_step,
                to_step=to_step,
                count_transitions=payload["count_transitions"],
                count_profiles=len(payload["profiles"]),
                avg_time_between_sec=(sum(payload["time_values"]) / len(payload["time_values"])) if payload["time_values"] else None,
                p50_time_between_sec=_percentile(payload["time_values"], 0.5),
                p90_time_between_sec=_percentile(payload["time_values"], 0.9),
                channel_group=payload["channel_group"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for example_row in example_rows:
        db.add(example_row)

    db.commit()
    return {
        "source_rows": len(journeys),
        "path_rows_written": len(path_aggs),
        "transition_rows_written": len(trans_aggs),
        "example_rows_written": len(example_rows),
    }


def _aggregate_channel_facts_for_day(
    db: Session,
    *,
    day: date,
) -> Dict[str, int]:
    silver_stats = _aggregate_channel_facts_for_day_from_silver(db, day=day)
    if silver_stats is not None:
        return silver_stats
    day_start = datetime.combine(day, dt_time.min).replace(tzinfo=timezone.utc)
    day_end = datetime.combine(day + timedelta(days=1), dt_time.min).replace(tzinfo=timezone.utc)
    rows = (
        db.query(ConversionPath)
        .filter(
            or_(
                func.coalesce(ConversionPath.first_touch_ts, ConversionPath.conversion_ts) < day_end,
                ConversionPath.conversion_ts < day_end,
            ),
            or_(
                func.coalesce(ConversionPath.last_touch_ts, ConversionPath.conversion_ts) >= day_start,
                ConversionPath.conversion_ts >= day_start,
            ),
        )
        .all()
    )

    aggs: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}

    def _bucket(channel: str, conversion_key: Optional[str]) -> Dict[str, Any]:
        key = (channel, conversion_key)
        return aggs.setdefault(
            key,
            {
                "channel": channel,
                "conversion_key": conversion_key,
                "visits_total": 0,
                "count_conversions": 0,
                "gross_conversions_total": 0.0,
                "net_conversions_total": 0.0,
                "gross_revenue_total": 0.0,
                "net_revenue_total": 0.0,
                "view_through_conversions_total": 0.0,
                "click_through_conversions_total": 0.0,
                "mixed_path_conversions_total": 0.0,
            },
        )

    for row in rows:
        payload = conversion_path_payload(row)
        touchpoints = payload.get("touchpoints") or []
        if isinstance(touchpoints, list):
            for tp in touchpoints:
                if not isinstance(tp, dict):
                    continue
                ts = _touchpoint_ts(tp)
                if ts is None or ts < day_start or ts >= day_end:
                    continue
                channel = str(tp.get("channel") or "unknown")
                _bucket(channel, None)["visits_total"] += 1

        conversion_ts = _to_utc_dt(getattr(row, "conversion_ts", None))
        if conversion_ts is None or conversion_ts < day_start or conversion_ts >= day_end:
            continue
        if not _conversion_path_is_converted(row):
            continue
        last_touchpoints = [tp for tp in touchpoints if isinstance(tp, dict)] if isinstance(touchpoints, list) else []
        last_tp = last_touchpoints[-1] if last_touchpoints else {}
        channel = str(last_tp.get("channel") or "unknown")
        conversion_key = str(getattr(row, "conversion_key", None) or "").strip() or None
        outcome = conversion_path_outcome_summary(row)
        path_type = classify_journey_interaction(payload)
        keys_to_update: List[Optional[str]] = [None]
        if conversion_key:
            keys_to_update.append(conversion_key)
        for key in keys_to_update:
            bucket = _bucket(channel, key)
            bucket["count_conversions"] += 1
            bucket["gross_conversions_total"] += float(outcome.get("gross_conversions", 0.0) or 0.0)
            bucket["net_conversions_total"] += float(outcome.get("net_conversions", 0.0) or 0.0)
            bucket["gross_revenue_total"] += float(outcome.get("gross_value", 0.0) or 0.0)
            bucket["net_revenue_total"] += float(outcome.get("net_value", 0.0) or 0.0)
            net_conversions = float(outcome.get("net_conversions", 0.0) or 0.0)
            if path_type == "view_through":
                bucket["view_through_conversions_total"] += net_conversions
            elif path_type == "click_through":
                bucket["click_through_conversions_total"] += net_conversions
            elif path_type == "mixed_path":
                bucket["mixed_path_conversions_total"] += net_conversions

    db.query(ChannelPerformanceDaily).filter(ChannelPerformanceDaily.date == day).delete(synchronize_session=False)
    now = datetime.now(timezone.utc)
    for payload in aggs.values():
        db.add(
            ChannelPerformanceDaily(
                date=day,
                channel=payload["channel"],
                conversion_key=payload["conversion_key"],
                visits_total=payload["visits_total"],
                count_conversions=payload["count_conversions"],
                gross_conversions_total=payload["gross_conversions_total"],
                net_conversions_total=payload["net_conversions_total"],
                gross_revenue_total=payload["gross_revenue_total"],
                net_revenue_total=payload["net_revenue_total"],
                view_through_conversions_total=payload["view_through_conversions_total"],
                click_through_conversions_total=payload["click_through_conversions_total"],
                mixed_path_conversions_total=payload["mixed_path_conversions_total"],
                created_at=now,
                updated_at=now,
            )
        )
    db.commit()
    return {
        "source_rows": len(rows),
        "channel_rows_written": len(aggs),
    }


def _aggregate_channel_facts_for_day_from_silver(
    db: Session,
    *,
    day: date,
) -> Optional[Dict[str, int]]:
    day_start = datetime.combine(day, dt_time.min).replace(tzinfo=timezone.utc)
    day_end = datetime.combine(day + timedelta(days=1), dt_time.min).replace(tzinfo=timezone.utc)
    touchpoint_rows = (
        db.query(
            SilverTouchpointFact.conversion_id,
            SilverTouchpointFact.touchpoint_ts,
            SilverTouchpointFact.channel,
            SilverTouchpointFact.ordinal,
        )
        .filter(
            SilverTouchpointFact.touchpoint_ts.isnot(None),
            SilverTouchpointFact.touchpoint_ts >= day_start,
            SilverTouchpointFact.touchpoint_ts < day_end,
        )
        .order_by(SilverTouchpointFact.conversion_id.asc(), SilverTouchpointFact.ordinal.asc())
        .all()
    )
    conversion_rows = (
        db.query(
            SilverConversionFact.conversion_id,
            SilverConversionFact.conversion_key,
            SilverConversionFact.conversion_ts,
            SilverConversionFact.interaction_path_type,
            SilverConversionFact.gross_conversions_total,
            SilverConversionFact.net_conversions_total,
            SilverConversionFact.gross_revenue_total,
            SilverConversionFact.net_revenue_total,
        )
        .filter(
            SilverConversionFact.conversion_ts >= day_start,
            SilverConversionFact.conversion_ts < day_end,
        )
        .order_by(SilverConversionFact.conversion_id.asc())
        .all()
    )
    if not touchpoint_rows and not conversion_rows:
        return None

    aggs: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}

    def _bucket(channel: str, conversion_key: Optional[str]) -> Dict[str, Any]:
        key = (channel, conversion_key)
        return aggs.setdefault(
            key,
            {
                "channel": channel,
                "conversion_key": conversion_key,
                "visits_total": 0,
                "count_conversions": 0,
                "gross_conversions_total": 0.0,
                "net_conversions_total": 0.0,
                "gross_revenue_total": 0.0,
                "net_revenue_total": 0.0,
                "view_through_conversions_total": 0.0,
                "click_through_conversions_total": 0.0,
                "mixed_path_conversions_total": 0.0,
            },
        )

    last_touch_channel: Dict[str, str] = {}
    for conversion_id, _touchpoint_ts, channel, ordinal in touchpoint_rows:
        ch = str(channel or "unknown")
        _bucket(ch, None)["visits_total"] += 1
        if conversion_id:
            last_touch_channel[str(conversion_id)] = ch

    for row in conversion_rows:
        conversion_id = str(row[0] or "")
        conversion_key = str(row[1] or "").strip() or None
        channel = last_touch_channel.get(conversion_id, "unknown")
        interaction_path_type = str(row[3] or "").strip().lower()
        gross_conversions = float(row[4] or 0.0)
        net_conversions = float(row[5] or 0.0)
        gross_revenue = float(row[6] or 0.0)
        net_revenue = float(row[7] or 0.0)
        keys_to_update: List[Optional[str]] = [None]
        if conversion_key:
            keys_to_update.append(conversion_key)
        for key in keys_to_update:
            bucket = _bucket(channel, key)
            bucket["count_conversions"] += 1
            bucket["gross_conversions_total"] += gross_conversions
            bucket["net_conversions_total"] += net_conversions
            bucket["gross_revenue_total"] += gross_revenue
            bucket["net_revenue_total"] += net_revenue
            if interaction_path_type == "view_through":
                bucket["view_through_conversions_total"] += net_conversions
            elif interaction_path_type == "click_through":
                bucket["click_through_conversions_total"] += net_conversions
            elif interaction_path_type == "mixed_path":
                bucket["mixed_path_conversions_total"] += net_conversions

    db.query(ChannelPerformanceDaily).filter(ChannelPerformanceDaily.date == day).delete(synchronize_session=False)
    now = datetime.now(timezone.utc)
    for payload in aggs.values():
        db.add(
            ChannelPerformanceDaily(
                date=day,
                channel=payload["channel"],
                conversion_key=payload["conversion_key"],
                visits_total=payload["visits_total"],
                count_conversions=payload["count_conversions"],
                gross_conversions_total=payload["gross_conversions_total"],
                net_conversions_total=payload["net_conversions_total"],
                gross_revenue_total=payload["gross_revenue_total"],
                net_revenue_total=payload["net_revenue_total"],
                view_through_conversions_total=payload["view_through_conversions_total"],
                click_through_conversions_total=payload["click_through_conversions_total"],
                mixed_path_conversions_total=payload["mixed_path_conversions_total"],
                created_at=now,
                updated_at=now,
            )
        )
    db.commit()
    return {
        "source_rows": len(touchpoint_rows) + len(conversion_rows),
        "channel_rows_written": len(aggs),
    }


def run_daily_journey_aggregates(
    db: Session,
    *,
    as_of_date: Optional[date] = None,
    reprocess_days: int = DEFAULT_REPROCESS_DAYS,
) -> Dict[str, Any]:
    started = time.perf_counter()
    now_utc = datetime.now(timezone.utc)
    run_day = as_of_date or now_utc.date()
    latest_complete_day = run_day - timedelta(days=1)
    if reprocess_days < 1:
        reprocess_days = 1

    defs = (
        db.query(JourneyDefinition)
        .filter(JourneyDefinition.is_archived == False)  # noqa: E712
        .order_by(JourneyDefinition.created_at.asc())
        .all()
    )
    if not defs or latest_complete_day < date(1970, 1, 2):
        return {
            "definitions": 0,
            "days_processed": 0,
            "source_rows_processed": 0,
            "path_rows_written": 0,
            "transition_rows_written": 0,
            "lag_minutes": None,
            "duration_ms": 0,
        }

    total_days = 0
    total_source_rows = 0
    total_paths = 0
    total_transitions = 0
    total_channel_rows = 0
    total_examples = 0
    max_source_ts: Optional[datetime] = None

    for definition in defs:
        source_days = _get_source_days(db, definition=definition, end_day=latest_complete_day)
        existing_path_days = {
            d
            for (d,) in db.query(JourneyPathDaily.date)
            .filter(
                JourneyPathDaily.journey_definition_id == definition.id,
                JourneyPathDaily.date <= latest_complete_day,
            )
            .all()
        }
        existing_transition_days = {
            d
            for (d,) in db.query(JourneyTransitionDaily.date)
            .filter(
                JourneyTransitionDaily.journey_definition_id == definition.id,
                JourneyTransitionDaily.date <= latest_complete_day,
            )
            .all()
        }
        obsolete_path_days = sorted(d for d in existing_path_days if d not in source_days)
        obsolete_transition_days = sorted(d for d in existing_transition_days if d not in source_days)
        if obsolete_path_days:
            db.query(JourneyPathDaily).filter(
                JourneyPathDaily.journey_definition_id == definition.id,
                JourneyPathDaily.date.in_(obsolete_path_days),
            ).delete(synchronize_session=False)
        if obsolete_transition_days:
            db.query(JourneyTransitionDaily).filter(
                JourneyTransitionDaily.journey_definition_id == definition.id,
                JourneyTransitionDaily.date.in_(obsolete_transition_days),
            ).delete(synchronize_session=False)
        if obsolete_path_days or obsolete_transition_days:
            db.commit()
        if not source_days:
            continue
        missing_days = {
            d for d in source_days if d not in existing_path_days or d not in existing_transition_days
        }
        reprocess_set = {
            latest_complete_day - timedelta(days=offset)
            for offset in range(reprocess_days)
        }
        days_to_process = sorted(d for d in (missing_days | reprocess_set) if d <= latest_complete_day and d in source_days)
        for day in days_to_process:
            stats = _aggregate_for_day_definition(db, day=day, definition=definition)
            total_days += 1
            total_source_rows += stats["source_rows"]
            total_paths += stats["path_rows_written"]
            total_transitions += stats["transition_rows_written"]
            total_examples += stats.get("example_rows_written", 0)

        max_ts_q = db.query(func.max(ConversionPath.conversion_ts))
        if definition.conversion_kpi_id:
            max_ts_q = max_ts_q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
        max_ts = _to_utc_dt(max_ts_q.scalar())
        if max_ts and (max_source_ts is None or max_ts > max_source_ts):
            max_source_ts = max_ts

    channel_source_days = _get_channel_source_days(db, end_day=latest_complete_day)
    existing_channel_days = {
        d
        for (d,) in db.query(ChannelPerformanceDaily.date)
        .filter(ChannelPerformanceDaily.date <= latest_complete_day)
        .all()
    }
    obsolete_channel_days = sorted(d for d in existing_channel_days if d not in channel_source_days)
    if obsolete_channel_days:
        db.query(ChannelPerformanceDaily).filter(ChannelPerformanceDaily.date.in_(obsolete_channel_days)).delete(synchronize_session=False)
        db.commit()
    missing_channel_days = {d for d in channel_source_days if d not in existing_channel_days}
    reprocess_set = {
        latest_complete_day - timedelta(days=offset)
        for offset in range(reprocess_days)
    }
    channel_days_to_process = sorted(d for d in (missing_channel_days | reprocess_set) if d <= latest_complete_day and d in channel_source_days)
    for day in channel_days_to_process:
        stats = _aggregate_channel_facts_for_day(db, day=day)
        total_days += 1
        total_source_rows += stats["source_rows"]
        total_channel_rows += stats["channel_rows_written"]

    duration_ms = int((time.perf_counter() - started) * 1000.0)
    lag_minutes = (
        round((now_utc - max_source_ts).total_seconds() / 60.0, 2)
        if max_source_ts is not None
        else None
    )
    metrics = {
        "definitions": len(defs),
        "days_processed": total_days,
        "source_rows_processed": total_source_rows,
        "path_rows_written": total_paths,
        "transition_rows_written": total_transitions,
        "channel_rows_written": total_channel_rows,
        "example_rows_written": total_examples,
        "lag_minutes": lag_minutes,
        "duration_ms": duration_ms,
    }
    logger.info(
        "Journey aggregates completed: definitions=%s days=%s source_rows=%s path_rows=%s transition_rows=%s lag_minutes=%s duration_ms=%s",
        metrics["definitions"],
        metrics["days_processed"],
        metrics["source_rows_processed"],
        metrics["path_rows_written"],
        metrics["transition_rows_written"],
        metrics["lag_minutes"],
        metrics["duration_ms"],
    )
    return metrics
