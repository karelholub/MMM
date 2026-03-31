from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from .models_config_dq import ConversionScopeDiagnosticFact
from .services_metrics import journey_outcome_summary
from .services_revenue_config import compute_payload_revenue_value, get_revenue_config

STEP_CONTENT_VIEW = "Product View / Content View"
STEP_ADD_TO_CART = "Add to Cart / Form Start"
STEP_CHECKOUT = "Checkout / Form Submit"

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


def _touchpoint_channel(tp: Dict[str, Any]) -> str:
    return str(tp.get("channel") or "unknown")


def _campaign_name(tp: Dict[str, Any]) -> Optional[str]:
    campaign = tp.get("campaign")
    if isinstance(campaign, dict):
        return str(campaign.get("name") or campaign.get("id") or "").strip() or None
    if campaign not in (None, "", []):
        return str(campaign).strip() or None
    if tp.get("campaign_name") not in (None, "", []):
        return str(tp.get("campaign_name")).strip() or None
    return None


def _dimension_key(tp: Dict[str, Any], scope_type: str) -> str:
    channel = _touchpoint_channel(tp)
    if scope_type == "campaign":
        campaign_name = _campaign_name(tp)
        return f"{channel}:{campaign_name}" if campaign_name else channel
    return channel


def _tp_interaction_type(tp: Dict[str, Any]) -> str:
    raw = str(tp.get("interaction_type") or "").strip().lower()
    if raw in {"impression", "click", "visit", "direct", "unknown"}:
        return raw
    if _touchpoint_channel(tp).strip().lower() == "direct":
        return "direct"
    return "unknown"


def _classify_journey_interaction(journey: Dict[str, Any]) -> str:
    touchpoints = journey.get("touchpoints") or []
    has_click_like = any(_tp_interaction_type(tp) in {"click", "visit", "direct"} for tp in touchpoints if isinstance(tp, dict))
    has_impression = any(_tp_interaction_type(tp) == "impression" for tp in touchpoints if isinstance(tp, dict))
    if has_click_like and has_impression:
        return "mixed_path"
    if has_click_like:
        return "click_through"
    if has_impression:
        return "view_through"
    return "unknown"


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


def _map_touchpoint_step(tp: Dict[str, Any], index: int) -> str:
    toks = _to_token_set(tp)
    if index == 0:
        if toks & _PAID_CHANNEL_TOKENS:
            return "Paid Landing"
        return "Organic Landing"
    if toks & _ADD_TO_CART_TOKENS:
        return STEP_ADD_TO_CART
    if toks & _CHECKOUT_TOKENS:
        return STEP_CHECKOUT
    if toks & _CONTENT_VIEW_TOKENS:
        return STEP_CONTENT_VIEW
    return STEP_CONTENT_VIEW


def _empty_fact() -> Dict[str, float]:
    return {
        "touch_journeys": 1.0,
        "content_journeys": 0.0,
        "checkout_journeys": 0.0,
        "converted_journeys": 0.0,
        "first_touch_conversions": 0.0,
        "last_touch_conversions": 0.0,
        "assist_conversions": 0.0,
        "first_touch_revenue": 0.0,
        "last_touch_revenue": 0.0,
        "assist_revenue": 0.0,
    }


def build_scope_diagnostic_fact_rows(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    first_touch_ts: datetime,
    last_touch_ts: datetime,
    conversion_ts: datetime,
    created_at: datetime,
) -> List[ConversionScopeDiagnosticFact]:
    touchpoints = [tp for tp in (journey.get("touchpoints") or []) if isinstance(tp, dict)]
    if not touchpoints:
        return []

    revenue_value = compute_payload_revenue_value(
        journey,
        get_revenue_config(),
        fallback_conversion_id=conversion_id,
    )
    if revenue_value <= 0.0:
        conversions = journey.get("conversions") or []
        if conversions and isinstance(conversions[0], dict):
            revenue_value = float(conversions[0].get("value") or 0.0)
        else:
            revenue_value = float(journey.get("conversion_value") or 0.0)

    converted = bool(journey.get("converted")) or bool(journey.get("conversions"))
    interaction_type = _classify_journey_interaction(journey)
    outcome = journey_outcome_summary(journey)
    if not converted and float(outcome.get("gross_conversions", 0.0) or 0.0) > 0.0:
        converted = True
    if not converted and float(outcome.get("net_conversions", 0.0) or 0.0) > 0.0:
        converted = True

    by_scope: Dict[tuple[str, str], Dict[str, Any]] = {}
    for scope_type in ("channel", "campaign"):
        dims_any: Set[str] = set()
        dims_content: Set[str] = set()
        dims_checkout: Set[str] = set()
        ordered_dims: List[str] = []
        dim_meta: Dict[str, Dict[str, Optional[str]]] = {}

        for idx, tp in enumerate(touchpoints):
            dim = _dimension_key(tp, scope_type)
            dims_any.add(dim)
            ordered_dims.append(dim)
            dim_meta.setdefault(
                dim,
                {
                    "scope_channel": _touchpoint_channel(tp),
                    "scope_campaign": _campaign_name(tp),
                },
            )
            step = _map_touchpoint_step(tp, idx)
            if step in {STEP_CONTENT_VIEW, STEP_ADD_TO_CART}:
                dims_content.add(dim)
            if step == STEP_CHECKOUT:
                dims_checkout.add(dim)

        for dim in dims_any:
            key = (scope_type, dim)
            entry = by_scope.setdefault(key, _empty_fact())
            entry.update(dim_meta.get(dim) or {})
            if dim in dims_content:
                entry["content_journeys"] = 1.0
            if dim in dims_checkout:
                entry["checkout_journeys"] = 1.0

        if converted:
            unique_assists = list(dict.fromkeys(ordered_dims[1:-1]))
            assist_share = revenue_value / float(len(unique_assists)) if unique_assists else 0.0
            for dim in dims_any:
                by_scope[(scope_type, dim)]["converted_journeys"] = 1.0
            if ordered_dims:
                by_scope[(scope_type, ordered_dims[0])]["first_touch_conversions"] = 1.0
                by_scope[(scope_type, ordered_dims[0])]["first_touch_revenue"] = revenue_value
                by_scope[(scope_type, ordered_dims[-1])]["last_touch_conversions"] = 1.0
                by_scope[(scope_type, ordered_dims[-1])]["last_touch_revenue"] = revenue_value
            for dim in unique_assists:
                by_scope[(scope_type, dim)]["assist_conversions"] = 1.0
                by_scope[(scope_type, dim)]["assist_revenue"] = assist_share

    rows: List[ConversionScopeDiagnosticFact] = []
    for (scope_type, scope_key), metrics in by_scope.items():
        rows.append(
            ConversionScopeDiagnosticFact(
                conversion_id=conversion_id,
                profile_id=profile_id,
                conversion_key=conversion_key,
                scope_type=scope_type,
                scope_key=scope_key,
                scope_channel=str(metrics.get("scope_channel") or "unknown"),
                scope_campaign=metrics.get("scope_campaign"),
                first_touch_ts=first_touch_ts,
                last_touch_ts=last_touch_ts,
                conversion_ts=conversion_ts,
                touch_journeys=int(metrics["touch_journeys"]),
                content_journeys=int(metrics["content_journeys"]),
                checkout_journeys=int(metrics["checkout_journeys"]),
                converted_journeys=int(metrics["converted_journeys"]),
                first_touch_conversions=int(metrics["first_touch_conversions"]),
                last_touch_conversions=int(metrics["last_touch_conversions"]),
                assist_conversions=int(metrics["assist_conversions"]),
                first_touch_revenue=round(float(metrics["first_touch_revenue"]), 2),
                last_touch_revenue=round(float(metrics["last_touch_revenue"]), 2),
                assist_revenue=round(float(metrics["assist_revenue"]), 2),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return rows
