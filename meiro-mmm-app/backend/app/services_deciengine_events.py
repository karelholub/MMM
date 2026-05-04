"""Convert deciEngine activation events into MMM attribution journeys."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, List, Optional


def _as_text(value: Any) -> Optional[str]:
    if value in (None, "", []):
        return None
    text = str(value).strip()
    return text or None


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        source = payload
    elif isinstance(payload, dict):
        source = payload.get("items") or payload.get("events") or payload.get("data") or []
    else:
        source = []
    return [item for item in source if isinstance(item, dict)]


def _interaction_type(event_type: Any) -> str:
    normalized = str(event_type or "").strip().upper()
    if normalized == "IMPRESSION":
        return "impression"
    if normalized == "CLICK":
        return "click"
    if normalized == "DISMISS":
        return "visit"
    return "unknown"


def _profile_key(event: Dict[str, Any], index: int) -> str:
    profile_id = _as_text(event.get("profileId") or event.get("profile_id"))
    if profile_id:
        return profile_id
    lookup_hash = _as_text(event.get("lookupValueHash") or event.get("lookup_value_hash"))
    lookup_attr = _as_text(event.get("lookupAttribute") or event.get("lookup_attribute"))
    if lookup_hash:
        return f"lookup:{lookup_attr or 'unknown'}:{lookup_hash}"
    message_id = _as_text(event.get("messageId") or event.get("message_id"))
    if message_id:
        return f"message:{message_id}"
    return f"anonymous:{index}"


def _stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _activation_context(event: Dict[str, Any]) -> Dict[str, Any]:
    context = _as_dict(event.get("context") or event.get("contextJson") or event.get("context_json"))
    activation = _as_dict(context.get("activationMeasurement") or context.get("activation") or {})
    return {key: value for key, value in activation.items() if value not in (None, "", [])}


def _touchpoint_from_event(event: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    timestamp = _as_text(event.get("ts") or event.get("timestamp"))
    if not timestamp:
        return None

    campaign_key = _as_text(event.get("campaignKey") or event.get("campaign_id") or event.get("campaignId"))
    variant_key = _as_text(event.get("variantKey") or event.get("variant_id") or event.get("variantId"))
    activation = _activation_context(event)
    channel = _as_text(activation.get("channel") or event.get("channel") or event.get("appKey")) or "inapp"
    message_id = _as_text(event.get("messageId") or event.get("message_id"))
    source_id = _as_text(event.get("sourceStreamId") or event.get("source_stream_id") or event.get("id")) or f"event-{index}"

    meta: Dict[str, Any] = {
        "source": "deciengine_inapp_events",
        "source_event_id": source_id,
    }
    if activation:
        meta["activation"] = activation

    touchpoint: Dict[str, Any] = {
        "id": source_id,
        "ts": timestamp,
        "channel": channel,
        "interaction_type": _interaction_type(event.get("eventType") or event.get("event_type")),
        "meta": meta,
    }
    if campaign_key:
        touchpoint["campaign_id"] = campaign_key
        touchpoint["campaign"] = {"name": campaign_key}
    if variant_key:
        touchpoint["variant_id"] = variant_key
    placement = _as_text(event.get("placement"))
    if placement:
        touchpoint["placement_id"] = placement
    return touchpoint


def deciengine_inapp_events_to_v2_journeys(payload: Any) -> Dict[str, Any]:
    """Return a v2 journey envelope from deciEngine `/v1/inapp/events` data."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for index, event in enumerate(_extract_items(payload)):
        touchpoint = _touchpoint_from_event(event, index)
        if not touchpoint:
            continue
        grouped.setdefault(_profile_key(event, index), []).append(touchpoint)

    journeys: List[Dict[str, Any]] = []
    for profile_id, touchpoints in sorted(grouped.items()):
        touchpoints.sort(key=lambda item: str(item.get("ts") or ""))
        journeys.append(
            {
                "journey_id": _stable_id("deciengine", profile_id),
                "customer": {"id": profile_id, "type": "deciengine_profile"},
                "touchpoints": touchpoints,
                "conversions": [],
                "meta": {
                    "source": "deciengine_inapp_events",
                    "event_count": len(touchpoints),
                },
            }
        )

    return {
        "schema_version": "2.0",
        "defaults": {"timezone": "UTC", "currency": "EUR"},
        "journeys": journeys,
    }
