from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse


TARGET_SITES = ("meiro.io", "meir.store")


def _event_payload(item: Any) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    nested = item.get("event_payload")
    if isinstance(nested, dict):
        merged = dict(nested)
        for key, value in item.items():
            if key != "event_payload" and key not in merged:
                merged[key] = value
        return merged
    return item


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_present(*values: Any) -> Optional[Any]:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _nested_dict(value: Any, key: str) -> Dict[str, Any]:
    if isinstance(value, dict) and isinstance(value.get(key), dict):
        return value.get(key) or {}
    return {}


def _activation_meta(event: Dict[str, Any]) -> Dict[str, Any]:
    context = _nested_dict(event, "context")
    meta = _nested_dict(event, "meta")
    activation = {}
    if isinstance(meta.get("activation"), dict):
        activation.update(meta.get("activation") or {})
    if isinstance(context.get("activationMeasurement"), dict):
        activation.update(context.get("activationMeasurement") or {})
    if isinstance(context.get("activation"), dict):
        activation.update(context.get("activation") or {})
    if isinstance(event.get("activationMeasurement"), dict):
        activation.update(event.get("activationMeasurement") or {})
    for key in (
        "activation_campaign_id",
        "native_meiro_campaign_id",
        "creative_asset_id",
        "native_meiro_asset_id",
        "offer_catalog_id",
        "native_meiro_catalog_id",
        "decision_key",
        "decision_stack_key",
        "variant_key",
        "placement_key",
        "template_key",
        "content_block_id",
        "offer_id",
        "bundle_id",
        "experiment_key",
        "is_holdout",
    ):
        if event.get(key) not in (None, "", []):
            activation.setdefault(key, event.get(key))
    return {key: value for key, value in activation.items() if value not in (None, "", [])}


def _site_for_event(event: Dict[str, Any]) -> str:
    explicit = _text(_first_present(event.get("site"), event.get("hostname"), event.get("domain"))).lower()
    candidates = [explicit]
    for key in ("page_url", "url", "location", "href", "referrer"):
        raw = _text(event.get(key))
        if not raw:
            continue
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        if parsed.netloc:
            candidates.append(parsed.netloc.lower())
    for candidate in candidates:
        normalized = candidate.replace("www.", "")
        for site in TARGET_SITES:
            if normalized == site or normalized.endswith(f".{site}"):
                return site
    return explicit.replace("www.", "") or "unknown"


def _has_identity(event: Dict[str, Any]) -> bool:
    customer = event.get("customer") if isinstance(event.get("customer"), dict) else {}
    profile = event.get("profile") if isinstance(event.get("profile"), dict) else {}
    return _first_present(
        event.get("customer_id"),
        event.get("profile_id"),
        event.get("user_id"),
        event.get("anonymous_id"),
        event.get("session_id"),
        customer.get("id"),
        profile.get("id"),
    ) is not None


def _has_attribution(event: Dict[str, Any]) -> bool:
    return _first_present(
        event.get("channel"),
        event.get("source"),
        event.get("medium"),
        event.get("campaign"),
        event.get("utm_source"),
        event.get("utm_medium"),
        event.get("utm_campaign"),
        event.get("campaign_id"),
        event.get("creative_id"),
        event.get("placement_id"),
    ) is not None


def _has_conversion_linkage(event: Dict[str, Any]) -> bool:
    name = _text(_first_present(event.get("event_name"), event.get("event_type"), event.get("name"), event.get("type"))).lower()
    looks_conversion = any(token in name for token in ("purchase", "checkout", "order", "lead", "signup", "conversion", "submit"))
    if not looks_conversion:
        return False
    return _first_present(event.get("conversion_id"), event.get("order_id"), event.get("lead_id"), event.get("event_id"), event.get("id")) is not None


def _has_segments(event: Dict[str, Any]) -> bool:
    for key in ("segments", "audiences", "segment_ids", "audience_ids"):
        value = event.get(key)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, str) and value.strip():
            return True
    attrs = event.get("attributes") if isinstance(event.get("attributes"), dict) else {}
    return isinstance(attrs.get("segments"), list) and bool(attrs.get("segments"))


def _flatten_events(archive_entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for entry in archive_entries:
        raw_events = entry.get("events")
        if isinstance(raw_events, list):
            events.extend(_event_payload(item) for item in raw_events)
    return [event for event in events if event]


def _pct(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def build_event_contract_readiness(
    archive_entries: Iterable[Dict[str, Any]],
    *,
    target_sites: Iterable[str] = TARGET_SITES,
) -> Dict[str, Any]:
    events = _flatten_events(archive_entries)
    target_site_set = {str(site).strip().lower() for site in target_sites if str(site).strip()}
    site_counts = Counter(_site_for_event(event) for event in events)
    target_events = [event for event in events if _site_for_event(event) in target_site_set]
    total = len(target_events)
    identity_count = sum(1 for event in target_events if _has_identity(event))
    attribution_count = sum(1 for event in target_events if _has_attribution(event))
    conversion_linkage_count = sum(1 for event in target_events if _has_conversion_linkage(event))
    activation_count = sum(1 for event in target_events if _activation_meta(event))
    segment_count = sum(1 for event in target_events if _has_segments(event))
    named_count = sum(1 for event in target_events if _first_present(event.get("event_name"), event.get("event_type"), event.get("name"), event.get("type")) is not None)
    warnings: List[str] = []
    blockers: List[str] = []
    missing_sites = sorted(site for site in target_site_set if site_counts.get(site, 0) <= 0)
    if not total:
        blockers.append("No raw events from meiro.io or meir.store were found in the recent archive window.")
    if missing_sites:
        warnings.append(f"Missing target site traffic: {', '.join(missing_sites)}.")
    if total and _pct(identity_count, total) < 0.9:
        warnings.append("Identity coverage is below 90%; journeys may fragment by anonymous fallback IDs.")
    if total and _pct(attribution_count, total) < 0.6:
        warnings.append("Attribution coverage is below 60%; touchpoints may fall back to unknown channels.")
    if total and conversion_linkage_count <= 0:
        warnings.append("No conversion-like events with stable conversion/order/lead linkage were found.")
    if total and activation_count <= 0:
        warnings.append("No activation metadata was found; campaign/decision/asset measurement will not match governed objects yet.")
    if total and segment_count <= 0:
        warnings.append("No segment or audience memberships were found; MMM operational audience selectors will stay sparse.")
    status = "blocked" if blockers else "warning" if warnings else "ready"
    return {
        "status": status,
        "target_sites": sorted(target_site_set),
        "events_analyzed": len(events),
        "target_events": total,
        "site_counts": dict(sorted(site_counts.items())),
        "coverage": {
            "named_event": _pct(named_count, total),
            "identity": _pct(identity_count, total),
            "attribution": _pct(attribution_count, total),
            "conversion_linkage": _pct(conversion_linkage_count, total),
            "activation_metadata": _pct(activation_count, total),
            "segments": _pct(segment_count, total),
        },
        "counts": {
            "named_event": named_count,
            "identity": identity_count,
            "attribution": attribution_count,
            "conversion_linkage": conversion_linkage_count,
            "activation_metadata": activation_count,
            "segments": segment_count,
        },
        "warnings": warnings,
        "blockers": blockers,
        "samples": [
            {
                "event_name": _first_present(event.get("event_name"), event.get("event_type"), event.get("name"), event.get("type")),
                "site": _site_for_event(event),
                "timestamp": _first_present(event.get("timestamp"), event.get("ts"), event.get("occurred_at"), event.get("created_at")),
                "identity": _first_present(event.get("customer_id"), event.get("profile_id"), event.get("anonymous_id"), event.get("session_id")),
                "campaign": _first_present(event.get("campaign"), event.get("utm_campaign"), event.get("campaign_id")),
                "activation_keys": sorted(_activation_meta(event).keys()),
            }
            for event in target_events[:5]
        ],
    }
