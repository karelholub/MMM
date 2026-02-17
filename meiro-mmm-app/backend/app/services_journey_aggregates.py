"""Daily journey aggregate ETL for journey_paths_daily and journey_transitions_daily."""

from __future__ import annotations

import hashlib
import logging
import math
import time
from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import (
    ConversionPath,
    JourneyDefinition,
    JourneyPathDaily,
    JourneyTransitionDaily,
)

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


def _payload_dict(path_json: Any) -> Dict[str, Any]:
    if isinstance(path_json, dict):
        return path_json
    return {}


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


def _build_journey_steps(
    payload: Dict[str, Any],
    *,
    conversion_ts: datetime,
    lookback_window_days: int,
) -> Tuple[List[str], Optional[float], Dict[str, Optional[str]]]:
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
    steps = [map_touchpoint_step(tp, idx) for idx, (_, tp) in enumerate(selected)]
    steps.append(STEP_CONVERSION)
    compact = dedup_steps(steps, max_steps=MAX_STEPS)
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
        "campaign_id": campaign_id,
        "device": str(payload.get("device")) if payload.get("device") else None,
        "country": str(payload.get("country")) if payload.get("country") else None,
    }
    if compact:
        dims["channel_group"] = "paid" if compact[0] == STEP_PAID_LANDING else ("organic" if compact[0] == STEP_ORGANIC_LANDING else None)
    return compact, ttc, dims


def _path_hash(steps: Sequence[str]) -> str:
    joined = " > ".join(steps)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _get_source_days(
    db: Session,
    *,
    definition: JourneyDefinition,
    end_day: date,
) -> Set[date]:
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


def _aggregate_for_day_definition(
    db: Session,
    *,
    day: date,
    definition: JourneyDefinition,
) -> Dict[str, int]:
    day_start = datetime.combine(day, dt_time.min)
    day_end = datetime.combine(day + timedelta(days=1), dt_time.min)
    q = db.query(ConversionPath).filter(
        ConversionPath.conversion_ts >= day_start,
        ConversionPath.conversion_ts < day_end,
    )
    if definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
    rows = q.all()

    path_aggs: Dict[str, Dict[str, Any]] = {}
    trans_aggs: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in rows:
        conversion_ts = _to_utc_dt(row.conversion_ts)
        if conversion_ts is None:
            continue
        payload = _payload_dict(row.path_json)
        steps, ttc_sec, dims = _build_journey_steps(
            payload,
            conversion_ts=conversion_ts,
            lookback_window_days=definition.lookback_window_days,
        )
        if not steps:
            continue
        phash = _path_hash(steps)
        bucket = path_aggs.setdefault(
            phash,
            {
                "path_steps": steps,
                "path_length": len(steps),
                "count_journeys": 0,
                "count_conversions": 0,
                "ttc_values": [],
                "channel_group": dims.get("channel_group"),
                "device": dims.get("device"),
                "country": dims.get("country"),
            },
        )
        bucket["count_journeys"] += 1
        bucket["count_conversions"] += 1
        if ttc_sec is not None and ttc_sec >= 0:
            bucket["ttc_values"].append(float(ttc_sec))

        profile_id = str(row.profile_id or "")
        for from_step, to_step in zip(steps, steps[1:]):
            t_bucket = trans_aggs.setdefault(
                (from_step, to_step),
                {
                    "count_transitions": 0,
                    "profiles": set(),
                    "channel_group": dims.get("channel_group"),
                    "campaign_id": dims.get("campaign_id"),
                    "device": dims.get("device"),
                    "country": dims.get("country"),
                },
            )
            t_bucket["count_transitions"] += 1
            if profile_id:
                t_bucket["profiles"].add(profile_id)

    db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == definition.id,
        JourneyPathDaily.date == day,
    ).delete(synchronize_session=False)
    db.query(JourneyTransitionDaily).filter(
        JourneyTransitionDaily.journey_definition_id == definition.id,
        JourneyTransitionDaily.date == day,
    ).delete(synchronize_session=False)

    now = datetime.now(timezone.utc)
    for phash, payload in path_aggs.items():
        ttc_values = payload["ttc_values"]
        db.add(
            JourneyPathDaily(
                date=day,
                journey_definition_id=definition.id,
                path_hash=phash,
                path_steps=payload["path_steps"],
                path_length=payload["path_length"],
                count_journeys=payload["count_journeys"],
                count_conversions=payload["count_conversions"],
                avg_time_to_convert_sec=(sum(ttc_values) / len(ttc_values)) if ttc_values else None,
                p50_time_to_convert_sec=_percentile(ttc_values, 0.5),
                p90_time_to_convert_sec=_percentile(ttc_values, 0.9),
                channel_group=payload["channel_group"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    for (from_step, to_step), payload in trans_aggs.items():
        db.add(
            JourneyTransitionDaily(
                date=day,
                journey_definition_id=definition.id,
                from_step=from_step,
                to_step=to_step,
                count_transitions=payload["count_transitions"],
                count_profiles=len(payload["profiles"]),
                channel_group=payload["channel_group"],
                campaign_id=payload["campaign_id"],
                device=payload["device"],
                country=payload["country"],
                created_at=now,
                updated_at=now,
            )
        )

    db.commit()
    return {
        "source_rows": len(rows),
        "path_rows_written": len(path_aggs),
        "transition_rows_written": len(trans_aggs),
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
    max_source_ts: Optional[datetime] = None

    for definition in defs:
        source_days = _get_source_days(db, definition=definition, end_day=latest_complete_day)
        if not source_days:
            continue
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

        max_ts_q = db.query(func.max(ConversionPath.conversion_ts))
        if definition.conversion_kpi_id:
            max_ts_q = max_ts_q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
        max_ts = _to_utc_dt(max_ts_q.scalar())
        if max_ts and (max_source_ts is None or max_ts > max_source_ts):
            max_source_ts = max_ts

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
