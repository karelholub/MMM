from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


STEP_PAID_LANDING = "Paid Landing"
STEP_ORGANIC_LANDING = "Organic Landing"
STEP_CONTENT_VIEW = "Product View / Content View"
STEP_ADD_TO_CART = "Add to Cart / Form Start"
STEP_CHECKOUT = "Checkout / Form Submit"
STEP_CONVERSION = "Purchase / Lead Won (conversion)"

MAX_STEPS = 20

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


def to_utc_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def touchpoint_ts(tp: Dict[str, Any]) -> Optional[datetime]:
    for key in ("ts", "timestamp", "event_ts", "occurred_at", "time"):
        out = to_utc_dt(tp.get(key))
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


def build_journey_steps_with_timestamps(
    payload: Dict[str, Any],
    *,
    conversion_ts: datetime,
    lookback_window_days: int,
) -> Tuple[List[str], List[datetime], Optional[float], Dict[str, Optional[str]]]:
    normalized_conversion_ts = to_utc_dt(conversion_ts)
    if normalized_conversion_ts is None:
        normalized_conversion_ts = datetime.now(timezone.utc)
    tps = payload.get("touchpoints") or []
    if not isinstance(tps, list):
        tps = []
    lower_bound = normalized_conversion_ts - timedelta(days=max(1, int(lookback_window_days)))
    selected: List[Tuple[datetime, Dict[str, Any]]] = []
    for tp in tps:
        if not isinstance(tp, dict):
            continue
        ts = touchpoint_ts(tp)
        if ts is None:
            continue
        if lower_bound <= ts <= normalized_conversion_ts:
            selected.append((ts, tp))
    selected.sort(key=lambda row: row[0])
    raw_steps: List[Tuple[str, datetime]] = [(map_touchpoint_step(tp, idx), ts) for idx, (ts, tp) in enumerate(selected)]
    raw_steps.append((STEP_CONVERSION, normalized_conversion_ts))
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
    ttc = (normalized_conversion_ts - first_step_ts).total_seconds() if first_step_ts else None
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


def build_journey_steps(
    payload: Dict[str, Any],
    *,
    conversion_ts: datetime,
    lookback_window_days: int,
) -> Tuple[List[str], Optional[float], Dict[str, Optional[str]]]:
    steps, _timestamps, ttc, dims = build_journey_steps_with_timestamps(
        payload,
        conversion_ts=conversion_ts,
        lookback_window_days=lookback_window_days,
    )
    return steps, ttc, dims
