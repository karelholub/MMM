from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from app.utils.meiro_config import event_site_host, get_target_site_domains, site_domain_matches

TARGET_SITES = ("meiro.io", "meir.store")


def build_sample_contract_events(*, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    suffix = _text(batch_id) or now.replace(":", "").replace("-", "")
    profile_id = f"contract-profile-{suffix}"
    session_id = f"contract-session-{suffix}"
    campaign_id = f"contract_campaign_{suffix}"
    return [
        {
            "event_id": f"contract-meiro-click-{suffix}",
            "customer_id": profile_id,
            "anonymous_id": f"contract-anon-{suffix}",
            "session_id": session_id,
            "timestamp": now,
            "event_name": "campaign_click",
            "event_type": "click",
            "interaction_type": "click",
            "site": "meiro.io",
            "page_url": "https://meiro.io/product",
            "page_path": "/product",
            "referrer": "https://google.com/search?q=meiro",
            "channel": "paid_search",
            "source": "google",
            "medium": "cpc",
            "campaign": campaign_id,
            "utm_source": "google",
            "utm_medium": "cpc",
            "utm_campaign": campaign_id,
            "utm_content": "contract_sample",
            "campaign_id": campaign_id,
            "creative_id": f"contract_asset_{suffix}",
            "placement_id": "meiro_io_product_hero",
            "segments": [{"id": "contract_high_intent", "name": "Contract high intent"}],
            "activationMeasurement": {
                "schema_version": "activation_measurement.v1",
                "source_system": "meiro_pipes",
                "activation_campaign_id": campaign_id,
                "native_meiro_campaign_id": f"meiro_native_{campaign_id}",
                "creative_asset_id": f"contract_asset_{suffix}",
                "native_meiro_asset_id": f"meiro_asset_{suffix}",
                "decision_key": "contract_homepage_offer_decision",
                "variant_key": "variant_contract",
                "placement_key": "meiro_io_product_hero",
                "experiment_key": "contract_hero_offer_test",
                "is_holdout": False,
            },
        },
        {
            "event_id": f"contract-store-purchase-{suffix}",
            "customer_id": profile_id,
            "anonymous_id": f"contract-anon-{suffix}",
            "session_id": session_id,
            "timestamp": now,
            "event_name": "purchase",
            "event_type": "conversion",
            "site": "meir.store",
            "page_url": "https://meir.store/checkout/success",
            "page_path": "/checkout/success",
            "channel": "paid_search",
            "source": "google",
            "medium": "cpc",
            "campaign": campaign_id,
            "utm_source": "google",
            "utm_medium": "cpc",
            "utm_campaign": campaign_id,
            "conversion_id": f"contract-conversion-{suffix}",
            "order_id": f"contract-order-{suffix}",
            "value": 99.0,
            "currency": "EUR",
            "segments": [{"id": "contract_high_intent", "name": "Contract high intent"}],
            "activationMeasurement": {
                "schema_version": "activation_measurement.v1",
                "source_system": "meiro_pipes",
                "activation_campaign_id": campaign_id,
                "native_meiro_campaign_id": f"meiro_native_{campaign_id}",
                "decision_key": "contract_homepage_offer_decision",
                "variant_key": "variant_contract",
                "placement_key": "meir_store_checkout",
                "experiment_key": "contract_hero_offer_test",
                "is_holdout": False,
            },
        },
    ]


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
    host = event_site_host(event)
    if not host:
        return "unknown"
    for site in get_target_site_domains():
        if site_domain_matches(host, [site]):
            return site
    return host.replace("www.", "")


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
    target_sites: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    events = _flatten_events(archive_entries)
    target_site_set = {str(site).strip().lower() for site in (target_sites or get_target_site_domains()) if str(site).strip()}
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
        blockers.append(f"No raw events from {', '.join(sorted(target_site_set))} were found in the recent archive window.")
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
